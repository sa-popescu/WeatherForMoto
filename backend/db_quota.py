"""An email before Turso blocks the database for the rest of the month.

The free Turso plan allows a fixed number of rows read and rows written per
calendar month. Past that, every query fails with BLOCKED until the month
turns, with no warning beforehand (November 2026). Once a day, riding on the
hourly alert dispatch, this module asks the Turso Platform API how much of the
month the organization has used and emails QUOTA_ALERT_EMAIL when reads or
writes cross 80%, and again at 95%. Each level mails at most once a month.

Configuration, all optional. Without the token or the address it does nothing:
- TURSO_PLATFORM_TOKEN: a Platform API token (not the database token)
- QUOTA_ALERT_EMAIL: where the warning goes
- TURSO_ORG: the organization slug; default, the personal one the token sees
- TURSO_ROWS_READ_LIMIT, TURSO_ROWS_WRITTEN_LIMIT: the plan's monthly limits

Everything here is blocking: run it from a thread, never on the event loop.
A failure is logged and reported in the result, never raised, so the dispatch
that carries it is not affected.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timezone
from typing import Any, Callable

import httpx

logger = logging.getLogger("weatherformoto.db_quota")

API_BASE = "https://api.turso.tech/v1"
# The free (Starter) plan, as of November 2026.
DEFAULT_ROWS_READ_LIMIT = 500_000_000
DEFAULT_ROWS_WRITTEN_LIMIT = 10_000_000
# Warning levels, highest first.
LEVELS = (95, 80)
# The dispatch runs hourly; the quota does not need looking at that often.
CHECK_INTERVAL_S = 20 * 3600
HTTP_TIMEOUT_S = 10

CHECKED_KEY = "db_quota:checked_at"
WARNED_KEY = "db_quota:warned"  # "<YYYY-MM>:<level>", the last level mailed

TURSO_CONSOLE_URL = "https://app.turso.tech"


def _limit(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, "") or default)
    except ValueError:
        logger.warning("%s is not a number, using %s", name, default)
        return default
    return value if value > 0 else default


def _get_json(client: httpx.Client, path: str) -> Any:
    response = client.get(f"{API_BASE}{path}")
    if response.status_code >= 400:
        raise RuntimeError(f"Turso API {path}: HTTP {response.status_code} - {response.text[:160]}")
    return response.json()


def _organization_slug(client: httpx.Client) -> str:
    configured = os.getenv("TURSO_ORG", "").strip()
    if configured:
        return configured
    organizations = _get_json(client, "/organizations")
    if not isinstance(organizations, list) or not organizations:
        raise RuntimeError("Turso API /organizations: no organization for this token")
    personal = [o for o in organizations if isinstance(o, dict) and o.get("type") == "personal"]
    slug = (personal or organizations)[0].get("slug")
    if not slug:
        raise RuntimeError("Turso API /organizations: organization without a slug")
    return str(slug)


def month_usage(client: httpx.Client) -> dict[str, int]:
    """Rows read and written this billing month, summed over every database
    of the organization: the quota is the organization's, not a database's."""
    slug = _organization_slug(client)
    body = _get_json(client, f"/organizations/{slug}/usage")
    databases = body.get("databases") if isinstance(body, dict) else None
    if not isinstance(databases, list):
        keys = sorted(body) if isinstance(body, dict) else type(body).__name__
        raise RuntimeError(f"Turso API usage: no database list in the answer ({keys})")
    totals = {"rows_read": 0, "rows_written": 0}
    for database in databases:
        total = database.get("total") if isinstance(database, dict) else None
        for key in totals:
            totals[key] += int((total or {}).get(key) or 0)
    return totals


def level_reached(usage: dict[str, int], read_limit: int, write_limit: int) -> tuple[int | None, dict[str, float]]:
    """The highest warning level crossed by reads or writes, and both shares in %."""
    shares = {
        "rows_read": 100.0 * usage["rows_read"] / read_limit,
        "rows_written": 100.0 * usage["rows_written"] / write_limit,
    }
    worst = max(shares.values())
    return next((level for level in LEVELS if worst >= level), None), shares


def already_warned(stored: str | None, month: str, level: int) -> bool:
    """True when this month's mail already went out at this level or a higher one."""
    if not stored or ":" not in stored:
        return False
    stored_month, _, stored_level = stored.partition(":")
    try:
        return stored_month == month and int(stored_level) >= level
    except ValueError:
        return False


def _format_count(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def warning_text(level: int, usage: dict[str, int], shares: dict[str, float],
                 read_limit: int, write_limit: int) -> tuple[str, str]:
    subject = f"Motonor: baza de date a folosit {level}% din cota lunii"
    text = (
        f"Turso a numărat luna aceasta:\n"
        f"- rânduri citite: {_format_count(usage['rows_read'])} din {_format_count(read_limit)} "
        f"({shares['rows_read']:.0f}%)\n"
        f"- rânduri scrise: {_format_count(usage['rows_written'])} din {_format_count(write_limit)} "
        f"({shares['rows_written']:.0f}%)\n\n"
        "La 100% Turso blochează baza până la începutul lunii următoare: conturile, "
        "traseele salvate, alertele și pericolele raportate nu mai merg.\n\n"
        "Ce poți face: vezi în consola Turso ce interogări consumă (Usage și Insights), "
        "iar dacă nu ajunge luna, treci temporar pe un plan plătit.\n\n"
        f"Consola: {TURSO_CONSOLE_URL}"
    )
    return subject, text


def check(
    get_state: Callable[[str], str | None],
    set_state: Callable[[str, str], None],
    send_email: Callable[[str, str, str], None],
    now: float | None = None,
) -> dict[str, Any]:
    """Look at the month's usage if a day has passed, and mail a warning when a
    new level is crossed. The state and mail functions come from auth_alerts."""
    token = os.getenv("TURSO_PLATFORM_TOKEN", "").strip()
    recipient = os.getenv("QUOTA_ALERT_EMAIL", "").strip()
    if not token or not recipient:
        return {"checked": False, "reason": "not configured"}

    now = time.time() if now is None else now
    try:
        last = float(get_state(CHECKED_KEY) or 0)
    except ValueError:
        last = 0.0
    if now - last < CHECK_INTERVAL_S:
        return {"checked": False, "reason": "checked recently"}

    read_limit = _limit("TURSO_ROWS_READ_LIMIT", DEFAULT_ROWS_READ_LIMIT)
    write_limit = _limit("TURSO_ROWS_WRITTEN_LIMIT", DEFAULT_ROWS_WRITTEN_LIMIT)
    try:
        with httpx.Client(timeout=HTTP_TIMEOUT_S, headers={"Authorization": f"Bearer {token}"}) as client:
            usage = month_usage(client)
    except Exception as exc:
        logger.warning("Turso quota check failed: %s", exc)
        return {"checked": False, "reason": "turso api error"}

    set_state(CHECKED_KEY, str(now))
    level, shares = level_reached(usage, read_limit, write_limit)
    result: dict[str, Any] = {
        "checked": True,
        "rows_read_pct": round(shares["rows_read"], 1),
        "rows_written_pct": round(shares["rows_written"], 1),
        "warned": None,
    }
    logger.info("Turso quota this month: reads %.1f%%, writes %.1f%%", shares["rows_read"], shares["rows_written"])
    if level is None:
        return result

    month = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m")
    if already_warned(get_state(WARNED_KEY), month, level):
        return result
    subject, text = warning_text(level, usage, shares, read_limit, write_limit)
    try:
        send_email(recipient, subject, text)
    except Exception as exc:
        logger.warning("Turso quota warning could not be mailed: %s", exc)
        return result
    set_state(WARNED_KEY, f"{month}:{level}")
    result["warned"] = level
    return result
