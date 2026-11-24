"""
Forecast verification log: what each source forecast, and what was measured.

Every weights and limits in weather_service and rain_fusion started as a
judgement call. To tune them on facts, the backend keeps a small log in Turso:

- forecast_log: for a ~5 km cell, at a few lead times (1 to 48 hours ahead),
  the final values the app showed plus each source's own chance of rain and
  Open-Meteo's raw temperature, so the corrections can be judged too.
- observation_log: what the official station, the airport, WeatherXM or
  Netatmo measured in that cell and hour.

Joined on cell and hour, the two answer "which source gets rain right here"
(verify_report.py prints it). Nothing about the user is stored: only the
rounded cell, which is also the forecast cache key, and UTC hours. At most one
snapshot per cell and hour is written, off the event loop, and a failed write
never touches the weather answer.

The join is done here in Python (rain_pairs), not in SQL, and rows older than
RETENTION_DAYS are dropped (prune): Turso bills every row a query steps over,
and this log is the only table in the app that grows without being asked to.

The tables are created by init_db (RUN_DB_MIGRATIONS=true); until then the
writes see the tables missing and skip.
"""

from __future__ import annotations

import asyncio
import logging
import os
import time
from collections import OrderedDict
from collections.abc import Sequence
from datetime import datetime, timedelta
from typing import Any

logger = logging.getLogger("motonor.verification")

# Hours ahead worth checking: the next ride, later today, tomorrow, the day after.
LEAD_HOURS: tuple[int, ...] = (1, 3, 6, 12, 24, 48)
# The rain sources whose own chance is logged, as named in rain_fusion.WEIGHTS.
RAIN_SOURCES: tuple[str, ...] = ("ensemble", "models", "open-meteo", "pirate-weather", "openweathermap", "met-norway")
# Set VERIFICATION_LOG=false to stop writing without a code change.
ENABLED = os.getenv("VERIFICATION_LOG", "true").lower() != "false"
# How long a row is kept. Twice the window calibration.py learns from, so
# verify_report.py can still look a season back; nothing reads further than that.
RETENTION_DAYS = 90
# Hours are stored as UTC text, "2026-11-19T03", which sorts like time does.
HOUR_FORMAT = "%Y-%m-%dT%H"
# In-process memory of the cells already written this hour (the table's key
# also stops duplicates across instances).
_SEEN_MAX = 5000
# A failing database is reported at most this often.
_ERROR_LOG_INTERVAL_S = 600

_seen: OrderedDict[tuple[str, str], None] = OrderedDict()
_last_error_log = 0.0

TABLES: tuple[str, ...] = (
    """CREATE TABLE IF NOT EXISTS forecast_log (
        cell TEXT NOT NULL,
        issued_hour TEXT NOT NULL,
        valid_hour TEXT NOT NULL,
        lead_h INTEGER NOT NULL,
        temp REAL,
        om_temp REAL,
        gusts REAL,
        precip_mm REAL,
        precip_prob REAL,
        prob_ensemble REAL,
        prob_models REAL,
        prob_open_meteo REAL,
        prob_pirate_weather REAL,
        prob_openweathermap REAL,
        prob_met_norway REAL,
        PRIMARY KEY (cell, issued_hour, valid_hour)
    )""",
    """CREATE TABLE IF NOT EXISTS observation_log (
        cell TEXT NOT NULL,
        hour TEXT NOT NULL,
        source TEXT NOT NULL,
        station TEXT,
        distance_km REAL,
        temp REAL,
        gusts REAL,
        precip_mm REAL,
        raining INTEGER,
        PRIMARY KEY (cell, hour, source)
    )""",
)
INDEXES: tuple[str, ...] = (
    "CREATE INDEX IF NOT EXISTS idx_forecast_log_valid ON forecast_log(valid_hour, cell)",
)

_FORECAST_COLUMNS = (
    "cell", "issued_hour", "valid_hour", "lead_h", "temp", "om_temp", "gusts", "precip_mm", "precip_prob",
    *(f"prob_{name.replace('-', '_')}" for name in RAIN_SOURCES),
)
_OBSERVATION_COLUMNS = ("cell", "hour", "source", "station", "distance_km", "temp", "gusts", "precip_mm", "raining")


def cell_key(cell: tuple[float, float]) -> str:
    """The rounded cell as stored: "44.45,26.10"."""
    return f"{cell[0]:.2f},{cell[1]:.2f}"


def _first_time(key: tuple[str, str]) -> bool:
    if key in _seen:
        return False
    _seen[key] = None
    while len(_seen) > _SEEN_MAX:
        _seen.popitem(last=False)
    return True


def _log_failure(exc: BaseException) -> None:
    global _last_error_log
    now = time.monotonic()
    if now - _last_error_log >= _ERROR_LOG_INTERVAL_S:
        _last_error_log = now
        logger.warning("verification log write failed: %s", type(exc).__name__)


def _write(snapshot: dict[str, Any]) -> None:
    """Blocking: insert the snapshot's rows, skipping what is already there."""
    from auth_alerts import _connect, _has_column  # imported late: auth_alerts imports this module

    conn = _connect()
    try:
        if not _has_column(conn, "forecast_log", "prob_met_norway") or not _has_column(conn, "observation_log", "raining"):
            return  # the migration has not run yet
        if snapshot["forecasts"]:
            conn.executemany(
                f"INSERT OR IGNORE INTO forecast_log ({', '.join(_FORECAST_COLUMNS)}) "
                f"VALUES ({', '.join('?' for _ in _FORECAST_COLUMNS)})",
                [tuple(row.get(column) for column in _FORECAST_COLUMNS) for row in snapshot["forecasts"]],
            )
        if snapshot["observations"]:
            conn.executemany(
                f"INSERT OR IGNORE INTO observation_log ({', '.join(_OBSERVATION_COLUMNS)}) "
                f"VALUES ({', '.join('?' for _ in _OBSERVATION_COLUMNS)})",
                [tuple(row.get(column) for column in _OBSERVATION_COLUMNS) for row in snapshot["observations"]],
            )
        conn.commit()
    finally:
        conn.close()


def rain_pairs(conn: Any, since: str, columns: Sequence[str]) -> list[dict[str, Any]]:
    """Blocking: forecasts issued since ``since``, each with whether it then rained.

    Every returned row carries the asked forecast_log ``columns`` (internal
    constants, never user input) plus ``raining``: 1 when any source saw rain
    in that cell and hour, 0 when none did. Forecasts without a measurement
    are left out.

    Two plain scans, matched here. The same thing written as one SQL join
    looked harmless and was not: SQLite walked a cell's whole forecast history
    again for every observed hour, so the rows read grew with the square of
    the log. With about 10,000 logged forecasts one read stepped over some 11
    million rows (measured on SQLite 3.45), which is the likely reason the
    database's monthly read quota ran out in November 2026.
    """
    observed: dict[tuple[str, str], int] = {}
    for row in conn.execute(
        "SELECT cell, hour, raining FROM observation_log WHERE hour >= ? AND raining IS NOT NULL",
        (since,),
    ).fetchall():
        key = (row["cell"], row["hour"])
        observed[key] = max(observed.get(key, 0), int(row["raining"]))

    pairs: list[dict[str, Any]] = []
    for row in conn.execute(
        f"SELECT cell, valid_hour, {', '.join(columns)} FROM forecast_log WHERE issued_hour >= ?",
        (since,),
    ).fetchall():
        raining = observed.get((row["cell"], row["valid_hour"]))
        if raining is None:
            continue
        pair = {column: row[column] for column in columns}
        pair["raining"] = raining
        pairs.append(pair)
    return pairs


def prune(conn: Any, now_utc: datetime) -> None:
    """Blocking: drop log rows older than RETENTION_DAYS.

    The forecast side goes by valid_hour because that is the column with an
    index, so the delete only touches the rows it removes. A forecast is valid
    at most 48 hours after it was issued, which is all the extra it keeps.
    """
    cutoff = (now_utc - timedelta(days=RETENTION_DAYS)).strftime(HOUR_FORMAT)
    conn.execute("DELETE FROM forecast_log WHERE valid_hour < ?", (cutoff,))
    conn.execute("DELETE FROM observation_log WHERE hour < ?", (cutoff,))
    conn.commit()


def _done(task: asyncio.Future) -> None:
    if not task.cancelled() and task.exception() is not None:
        _log_failure(task.exception())  # type: ignore[arg-type]


def record(snapshot: dict[str, Any]) -> None:
    """Queue a snapshot for writing; returns at once. Called from the event loop."""
    if not ENABLED:
        return
    if not _first_time((snapshot["cell"], snapshot["issued_hour"])):
        return
    try:
        task = asyncio.get_running_loop().create_task(asyncio.to_thread(_write, snapshot))
    except RuntimeError:
        return  # no running loop (a script or a test): nothing to write with
    task.add_done_callback(_done)
