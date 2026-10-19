"""The last good weather answer, kept where a cold start cannot lose it.

weather_service holds the last good answer per cell in memory, so an upstream
that refuses us becomes old weather on screen instead of an error card. That
memory is per process, and Cloud Run scales to zero: by the time the rider
opens the app, the instance that stored the answer is usually gone and the net
is empty exactly when it is needed. The answer is therefore also written to
Turso, gzipped, and read back when this process has nothing of its own.

Writes happen off the event loop and are throttled per cell, so the forecast
being served never waits for the database. A failing database costs the safety
net and nothing else: every call here returns quietly on error.

The table is created by init_db (RUN_DB_MIGRATIONS=true); until then reads
find nothing and writes skip.
"""

from __future__ import annotations

import asyncio
import base64
import gzip
import json
import logging
import os
import time
from typing import Any

logger = logging.getLogger("weatherformoto.last_good")

# Set LAST_GOOD_STORE=false to stop using the database without a code change.
ENABLED = os.getenv("LAST_GOOD_STORE", "true").lower() != "false"
# One row per cell is rewritten at most this often: the answer only has to be
# recent enough to be worth serving, not current.
MIN_WRITE_INTERVAL_S = 10 * 60
# A read happens on a path that has already spent the whole time budget, and
# the app gives up at 12s. What is left over is all this gets.
READ_BUDGET_S = 1.5
# Rows past the caller's limit are dead weight; sweep them now and then.
PRUNE_INTERVAL_S = 3600
# A failing database is reported at most this often.
_ERROR_LOG_INTERVAL_S = 600

TABLES: tuple[str, ...] = (
    """CREATE TABLE IF NOT EXISTS last_good_weather (
        cell TEXT NOT NULL,
        days INTEGER NOT NULL,
        stored_at INTEGER NOT NULL,
        payload TEXT NOT NULL,
        PRIMARY KEY (cell, days)
    )""",
)
INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_last_good_stored ON last_good_weather(stored_at)",
)

_written_at: dict[tuple[str, int], float] = {}
_last_prune = 0.0
_last_error_log = 0.0


def _configured() -> bool:
    """No database means no store: say so before spending a thread on it."""
    return bool(os.getenv("TURSO_DATABASE_URL") and os.getenv("TURSO_AUTH_TOKEN"))


def _log_failure(what: str, exc: BaseException) -> None:
    global _last_error_log
    now = time.monotonic()
    if now - _last_error_log >= _ERROR_LOG_INTERVAL_S:
        _last_error_log = now
        logger.warning("last good %s failed: %s", what, type(exc).__name__)


def _pack(payload: dict[str, Any]) -> str:
    """JSON, gzipped, base64 — a forecast is mostly repeated keys and numbers."""
    raw = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    return base64.b64encode(gzip.compress(raw, 6)).decode("ascii")


def _unpack(stored: str) -> dict[str, Any]:
    return json.loads(gzip.decompress(base64.b64decode(stored)).decode("utf-8"))


def _write(cell: str, days: int, stored_at: float, payload: dict[str, Any]) -> None:
    """Blocking: replace this cell's row, and sweep expired rows now and then."""
    global _last_prune
    from auth_alerts import _connect, _has_column  # imported late: it imports weather_service

    conn = _connect()
    try:
        if not _has_column(conn, "last_good_weather", "payload"):
            return  # the migration has not run yet
        conn.execute(
            "INSERT OR REPLACE INTO last_good_weather (cell, days, stored_at, payload) VALUES (?, ?, ?, ?)",
            (cell, days, int(stored_at), _pack(payload)),
        )
        now = time.monotonic()
        if now - _last_prune >= PRUNE_INTERVAL_S:
            _last_prune = now
            conn.execute(
                "DELETE FROM last_good_weather WHERE stored_at < ?",
                (int(stored_at - 24 * 3600),),
            )
        conn.commit()
    finally:
        conn.close()


def _read(cell: str, days: int) -> tuple[float, dict[str, Any]] | None:
    """Blocking: this cell's stored answer and when it was stored."""
    from auth_alerts import _connect, _has_column

    conn = _connect()
    try:
        if not _has_column(conn, "last_good_weather", "payload"):
            return None
        row = conn.execute(
            "SELECT stored_at, payload FROM last_good_weather WHERE cell = ? AND days = ?",
            (cell, days),
        ).fetchone()
        if row is None:
            return None
        return float(row["stored_at"]), _unpack(row["payload"])
    finally:
        conn.close()


def _done(task: asyncio.Future) -> None:
    if not task.cancelled() and task.exception() is not None:
        _log_failure("write", task.exception())  # type: ignore[arg-type]


def remember(cell: str, days: int, stored_at: float, payload: dict[str, Any]) -> None:
    """Queue this answer for writing; returns at once. Called from the event loop."""
    if not ENABLED or not _configured():
        return
    key = (cell, days)
    previous = _written_at.get(key)
    if previous is not None and stored_at - previous < MIN_WRITE_INTERVAL_S:
        return
    # Marked as written before the write runs, on purpose: a database that is
    # down would otherwise get a thread from every single request.
    try:
        task = asyncio.get_running_loop().create_task(
            asyncio.to_thread(_write, cell, days, stored_at, payload)
        )
    except RuntimeError:
        return  # no running loop (a script or a test): nothing to write with
    _written_at[key] = stored_at
    task.add_done_callback(_done)


async def load(cell: str, days: int) -> tuple[float, dict[str, Any]] | None:
    """This cell's stored answer, or None when there is none to be had.

    Called only after every source has already failed, so it is bounded: a slow
    database must not add to a wait the rider has already spent.
    """
    if not ENABLED or not _configured():
        return None
    try:
        return await asyncio.wait_for(asyncio.to_thread(_read, cell, days), timeout=READ_BUDGET_S)
    except Exception as exc:
        _log_failure("read", exc)
        return None


def reset_for_tests() -> None:
    global _last_prune
    _written_at.clear()
    _last_prune = 0.0
