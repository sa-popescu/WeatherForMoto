"""Errors from the app on riders' phones, into the API log and by email.

The app sends what broke (POST /client-errors): the message, a few stack
lines, the app version, the tab and the browser. Never coordinates, an
account or an email address. Every accepted report goes to the log at ERROR
level; the first time an error is seen in a day, an email goes to
ERROR_ALERT_EMAIL, at most DAILY_EMAILS a day.

The database is touched as little as possible: Turso's free plan blocks the
whole database for the rest of the month once its quota runs out. Each
instance remembers the errors it has seen today in memory, and only an error
new to the instance costs one read (and, when it is new for the day, two
writes) in app_state, so other instances do not mail it again. When the
database does not answer, memory alone decides, with a smaller daily cap.

Configuration: ERROR_ALERT_EMAIL, where the emails go. Without it, reports
are only logged.

Everything here is blocking: run it from a thread, never on the event loop.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable

logger = logging.getLogger("motonor.client_errors")

# Largest body accepted. A report is a few hundred bytes; the stack is cut
# well before this in the app too.
MAX_BODY_BYTES = 16_384
MAX_MESSAGE = 300
MAX_STACK = 1_500
MAX_STACK_LINES = 8
MAX_FIELD = 200

KINDS = ("error", "rejection", "render")
_TAB_RE = re.compile(r"^[a-z-]{1,24}$")
_VERSION_RE = re.compile(r"^[0-9A-Za-z.+-]{1,40}$")
# Query strings and fragments can carry coordinates or tokens: cut them.
_URL_TAIL_RE = re.compile(r"(https?://[^\s?#)]+)[?#][^\s)]*")
_DIGITS_RE = re.compile(r"\d+")

# Per client address: this many reports per window, in memory per instance.
CLIENT_MAX = 20
CLIENT_WINDOW_S = 3600.0
# Per instance and day: reports taken at all, and emails sent when the
# database cannot share the count between instances.
INSTANCE_DAILY_REPORTS = 2_000
FALLBACK_DAILY_EMAILS = 3
# Across instances, through app_state.
DAILY_EMAILS = 10

SEEN_KEY_PREFIX = "client_error:"
MAILED_KEY = "client_error:mailed"  # "<YYYY-MM-DD>:<count>"


@dataclass(frozen=True)
class Report:
    kind: str
    message: str
    stack: str
    version: str
    tab: str
    browser: str

    @property
    def fingerprint(self) -> str:
        """Same error, same fingerprint: the kind, the message without its
        numbers (ids, sizes) and the first stack line."""
        first_frame = self.stack.splitlines()[0] if self.stack else ""
        basis = "|".join((self.kind, _DIGITS_RE.sub("#", self.message), first_frame))
        return hashlib.sha256(basis.encode("utf-8")).hexdigest()[:12]


def _clean(value: Any, limit: int) -> str:
    if not isinstance(value, str):
        return ""
    text = _URL_TAIL_RE.sub(r"\1", value).replace("\x00", "").strip()
    return text[:limit]


def parse(body: bytes) -> Report:
    """The app's JSON report as a Report, cut to size; ValueError when it is not one."""
    if len(body) > MAX_BODY_BYTES:
        raise ValueError("report too large")
    try:
        data = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("not JSON") from exc
    if not isinstance(data, dict):
        raise ValueError("not an object")
    kind = data.get("kind")
    if kind not in KINDS:
        raise ValueError("unknown kind")
    message = _clean(data.get("message"), MAX_MESSAGE)
    if not message:
        raise ValueError("no message")
    stack_lines = _clean(data.get("stack"), MAX_STACK * 2).splitlines()
    stack = "\n".join(line.strip() for line in stack_lines[:MAX_STACK_LINES] if line.strip())[:MAX_STACK]
    version = _clean(data.get("version"), 40)
    tab = _clean(data.get("tab"), 24)
    return Report(
        kind=kind,
        message=message,
        stack=stack,
        version=version if _VERSION_RE.match(version) else "",
        tab=tab if _TAB_RE.match(tab) else "",
        browser=_clean(data.get("browser"), MAX_FIELD),
    )


class _Memory:
    """What one instance remembers for the day: the clients' recent reports,
    the errors already seen and the emails sent on its own."""

    def __init__(self) -> None:
        self.day = ""
        self.reports = 0
        self.fallback_emails = 0
        self.seen: set[str] = set()
        self.clients: dict[str, deque[float]] = {}

    def roll(self, day: str, now: float) -> None:
        if day == self.day:
            return
        self.day, self.reports, self.fallback_emails = day, 0, 0
        self.seen.clear()
        self.clients = {k: v for k, v in self.clients.items() if v and now - v[-1] < CLIENT_WINDOW_S}

    def allow(self, client: str, now: float) -> bool:
        if self.reports >= INSTANCE_DAILY_REPORTS:
            return False
        hits = self.clients.setdefault(client, deque())
        while hits and now - hits[0] >= CLIENT_WINDOW_S:
            hits.popleft()
        if len(hits) >= CLIENT_MAX:
            return False
        hits.append(now)
        self.reports += 1
        return True


_memory = _Memory()


def _reset_memory_for_tests() -> None:
    global _memory
    _memory = _Memory()


def email_text(report: Report, day: str) -> tuple[str, str]:
    subject = f"Motonor: eroare nouă în aplicație ({report.tab or 'fără tab'})"
    text = (
        f"Aplicația a raportat azi ({day}) o eroare pe care n-am mai văzut-o.\n\n"
        f"Mesaj: {report.message}\n"
        f"Tip: {report.kind}\n"
        f"Tab: {report.tab or '-'}\n"
        f"Versiune: {report.version or '-'}\n"
        f"Browser: {report.browser or '-'}\n"
        f"Amprentă: {report.fingerprint}\n\n"
        f"Stack:\n{report.stack or '-'}\n\n"
        "Toate aparițiile sunt în logurile Cloud Run, căutând amprenta de mai sus. "
        "Aceeași eroare nu mai primește email azi."
    )
    return subject, text


def _count_from(stored: str | None, day: str) -> int:
    if not stored or ":" not in stored:
        return 0
    stored_day, _, count = stored.rpartition(":")
    try:
        return int(count) if stored_day == day else 0
    except ValueError:
        return 0


def _should_mail(
    report: Report,
    day: str,
    get_state: Callable[[str], str | None],
    set_state: Callable[[str, str], None],
) -> tuple[bool, str]:
    """Whether this error gets today's email, sharing the answer between
    instances through app_state, or deciding alone when that fails."""
    key = f"{SEEN_KEY_PREFIX}{report.fingerprint}"
    try:
        if get_state(key) == day:
            return False, "seen today"
        mailed = _count_from(get_state(MAILED_KEY), day)
        if mailed >= DAILY_EMAILS:
            set_state(key, day)
            return False, "daily cap"
        set_state(key, day)
        set_state(MAILED_KEY, f"{day}:{mailed + 1}")
        return True, "new today"
    except Exception as exc:
        logger.warning("client error dedupe without the database: %s: %s", type(exc).__name__, exc)
    if _memory.fallback_emails >= FALLBACK_DAILY_EMAILS:
        return False, "daily cap (no database)"
    _memory.fallback_emails += 1
    return True, "new today (no database)"


def handle(
    report: Report,
    client: str,
    get_state: Callable[[str], str | None],
    set_state: Callable[[str, str], None],
    send_email: Callable[[str, str, str], None],
    now: float | None = None,
) -> str:
    """Log the report and mail it when it is new for the day. Returns what
    happened, for the tests and the log; never raises."""
    now = time.time() if now is None else now
    day = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%d")
    _memory.roll(day, now)
    if not _memory.allow(client, now):
        return "limited"

    fingerprint = report.fingerprint
    first_frame = report.stack.splitlines()[0] if report.stack else "-"
    logger.error(
        "client error %s [%s] tab=%s version=%s: %s | %s | %s",
        fingerprint, report.kind, report.tab or "-", report.version or "-",
        report.message, first_frame, report.browser or "-",
    )
    if fingerprint in _memory.seen:
        return "logged"
    _memory.seen.add(fingerprint)

    recipient = os.getenv("ERROR_ALERT_EMAIL", "").strip()
    if not recipient:
        return "logged"
    mail, reason = _should_mail(report, day, get_state, set_state)
    if not mail:
        return f"logged ({reason})"
    subject, text = email_text(report, day)
    try:
        send_email(recipient, subject, text)
    except Exception as exc:
        logger.warning("client error %s could not be mailed: %s", fingerprint, exc)
        return "logged (mail failed)"
    return "mailed"
