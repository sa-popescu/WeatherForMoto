"""
What the verification log says, fed back into the next forecast.

verification.py has been writing down, for every cell and hour, what each
source said the chance of rain was and whether a station or an airport then
saw rain. verify_report.py prints that for a human to read and hand-edit the
weights. This module closes the loop instead, and learns two things:

- WEIGHTS for rain_fusion, in proportion to each source's skill against the
  local base rate. A source that keeps being right here counts for more here.
- a reliability curve for the fused chance itself: if the hours this app called
  "40%" rained one time in six, then 40% is not 40%, and the number shown is
  moved towards what actually happened.

Both are deliberately timid. Nothing is learned from fewer pairs than the
minimums below, the learned values are blended halfway with the hand-set ones
rather than replacing them, and a database that is missing, empty or broken
simply leaves the defaults in place. The maths here is pure; weather_service
schedules the refresh and applies the result.

The log is read once a day, by whichever instance gets there first. What it
learned is kept in app_state, so every other instance and every cold start
picks up the answer with a single row instead of reading the log again.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

import verification

logger = logging.getLogger("weatherformoto.calibration")

# How far back to read. Long enough for a season to show, short enough that a
# model upgrade upstream is not judged on how it behaved months ago.
WINDOW_DAYS = 45
# A source needs this many forecast/observation pairs before its skill counts.
MIN_SOURCE_PAIRS = 100
# The reliability curve needs this many in total, and this many in a bin.
MIN_TOTAL_PAIRS = 200
MIN_BIN_PAIRS = 40
# How far the learned values are allowed to pull the hand-set ones. Half: the
# log is a few weeks of one city, not a verification study.
LEARNED_SHARE = 0.5
# Probability bins for the reliability curve, as fractions.
BIN_EDGES: tuple[float, ...] = (0.0, 0.2, 0.4, 0.6, 0.8, 1.0)
# How often the snapshot is recomputed, and how long a failure is held off.
# Daily is plenty: a day adds a few hundred pairs to a window of thousands.
REFRESH_S = 24 * 3600
RETRY_AFTER_FAILURE_S = 30 * 60
# Where the snapshot is kept between processes (the app_state table).
STATE_KEY = "calibration_snapshot"

Pair = tuple[float, int]  # (forecast probability 0-1, observed 0 or 1)


def brier(pairs: Sequence[Pair]) -> float | None:
    """Mean squared error of a probability forecast; 0 is perfect."""
    if not pairs:
        return None
    return sum((p - o) ** 2 for p, o in pairs) / len(pairs)


def skill(pairs: Sequence[Pair]) -> float | None:
    """Brier skill against always forecasting the local base rate.

    Above 0 beats "it rains here x% of the time"; at or below 0 the source is
    not adding anything, and gets no weight from us.
    """
    score = brier(pairs)
    if score is None:
        return None
    base = sum(o for _, o in pairs) / len(pairs)
    reference = sum((base - o) ** 2 for _, o in pairs) / len(pairs)
    if reference <= 0:
        return None  # it always rained, or never did: nothing to beat
    return 1 - score / reference


def learned_weights(
    by_source: dict[str, Sequence[Pair]], defaults: dict[str, float]
) -> dict[str, float]:
    """Blend the hand-set weights towards each source's measured skill.

    The skills are scaled so the set keeps the same total weight as the
    defaults: this changes who is trusted, not how loud the whole chorus is.
    Sources without enough pairs keep their default exactly.
    """
    skills: dict[str, float] = {}
    for name, default in defaults.items():
        pairs = by_source.get(name) or []
        if len(pairs) < MIN_SOURCE_PAIRS:
            continue
        value = skill(pairs)
        if value is not None:
            skills[name] = max(0.0, value)
    if not skills or sum(skills.values()) <= 0:
        return dict(defaults)

    scale = sum(defaults[name] for name in skills) / sum(skills.values())
    out = dict(defaults)
    for name, value in skills.items():
        learned = value * scale
        out[name] = round(defaults[name] * (1 - LEARNED_SHARE) + learned * LEARNED_SHARE, 3)
    return out


def reliability_table(pairs: Sequence[Pair]) -> tuple[tuple[float, float], ...]:
    """(mean forecast, observed rate) per bin, for bins with enough pairs.

    Empty when there is not enough to say anything, which is the signal to
    leave the probabilities alone.
    """
    if len(pairs) < MIN_TOTAL_PAIRS:
        return ()
    table: list[tuple[float, float]] = []
    for index in range(len(BIN_EDGES) - 1):
        low, high = BIN_EDGES[index], BIN_EDGES[index + 1]
        # The last bin keeps its upper edge so a forecast of 1.0 lands somewhere.
        inside = [
            (p, o) for p, o in pairs
            if low <= p < high or (index == len(BIN_EDGES) - 2 and p == high)
        ]
        if len(inside) < MIN_BIN_PAIRS:
            continue
        table.append((
            sum(p for p, _ in inside) / len(inside),
            sum(o for _, o in inside) / len(inside),
        ))
    return tuple(table)


def calibrated(probability_pct: float | None, table: Sequence[tuple[float, float]]) -> float | None:
    """Move a percentage towards what hours like it actually did.

    Piecewise linear between the bins that had enough pairs, flat outside them,
    and only LEARNED_SHARE of the way: a curve from a few weeks of one city
    corrects a bias, it does not get to overrule the models.
    """
    if probability_pct is None:
        return None
    if not table:
        return probability_pct
    p = max(0.0, min(1.0, probability_pct / 100))
    points = sorted(table)
    if p <= points[0][0]:
        observed = points[0][1]
    elif p >= points[-1][0]:
        observed = points[-1][1]
    else:
        observed = points[-1][1]
        for (x0, y0), (x1, y1) in zip(points, points[1:]):
            if x0 <= p <= x1:
                span = x1 - x0
                observed = y0 if span <= 0 else y0 + (y1 - y0) * (p - x0) / span
                break
    moved = p * (1 - LEARNED_SHARE) + observed * LEARNED_SHARE
    return round(max(0.0, min(100.0, moved * 100)), 1)


class Snapshot:
    """What was learned, plus how it was learned, for /meta/scoring to show."""

    __slots__ = ("weights", "reliability", "pairs", "computed_at")

    def __init__(
        self,
        weights: dict[str, float],
        reliability: tuple[tuple[float, float], ...] = (),
        pairs: int = 0,
        computed_at: float | None = None,
    ) -> None:
        self.weights = weights
        self.reliability = reliability
        self.pairs = pairs
        self.computed_at = computed_at

    @property
    def learned(self) -> bool:
        return self.computed_at is not None and (self.pairs > 0 or bool(self.reliability))

    def as_meta(self) -> dict[str, Any]:
        return {
            "learned": self.learned,
            "pairs": self.pairs,
            "window_days": WINDOW_DAYS,
            "learned_share": LEARNED_SHARE,
            "weights": dict(self.weights),
            "reliability": [
                {"forecast": round(f, 3), "observed": round(o, 3)} for f, o in self.reliability
            ],
        }


def compute(rows: Iterable[dict[str, Any]], sources: Sequence[str], defaults: dict[str, float]) -> Snapshot:
    """Build a snapshot from the joined forecast/observation rows."""
    by_source: dict[str, list[Pair]] = {name: [] for name in sources}
    final: list[Pair] = []
    for row in rows:
        observed = row.get("raining")
        if observed is None:
            continue
        observed = int(observed)
        if row.get("precip_prob") is not None:
            final.append((max(0.0, min(1.0, float(row["precip_prob"]) / 100)), observed))
        for name in sources:
            value = row.get(f"prob_{name.replace('-', '_')}")
            if value is not None:
                by_source.setdefault(name, []).append((max(0.0, min(1.0, float(value))), observed))
    return Snapshot(
        weights=learned_weights(by_source, defaults),
        reliability=reliability_table(final),
        pairs=len(final),
        computed_at=time.time(),
    )


# --- the stored snapshot ----------------------------------------------------

def _settings(defaults: dict[str, float]) -> dict[str, Any]:
    """What a stored snapshot was computed with. A stored one made under other
    settings (a deploy changed the hand-set weights, say) is not reused."""
    return {"defaults": dict(defaults), "window_days": WINDOW_DAYS, "learned_share": LEARNED_SHARE}


def to_stored(snapshot: Snapshot, defaults: dict[str, float]) -> str:
    """The snapshot as the text kept in app_state."""
    return json.dumps({
        "computed_at": snapshot.computed_at,
        "pairs": snapshot.pairs,
        "weights": snapshot.weights,
        "reliability": [[forecast, observed] for forecast, observed in snapshot.reliability],
        "settings": _settings(defaults),
    })


def from_stored(text: str | None, defaults: dict[str, float], now: float) -> Snapshot | None:
    """The stored snapshot, when it is still good to use; otherwise None.

    None for nothing stored, something unreadable, other settings, or a
    snapshot older than REFRESH_S. All of those mean "read the log again".
    ``now`` is wall-clock seconds, like Snapshot.computed_at.
    """
    if not text:
        return None
    try:
        data = json.loads(text)
        if data["settings"] != _settings(defaults):
            return None
        computed_at = float(data["computed_at"])
        if not 0 <= now - computed_at < REFRESH_S:
            return None
        return Snapshot(
            weights={str(name): float(value) for name, value in data["weights"].items()},
            reliability=tuple((float(forecast), float(observed)) for forecast, observed in data["reliability"]),
            pairs=int(data["pairs"]),
            computed_at=computed_at,
        )
    except (ValueError, TypeError, KeyError, AttributeError):
        return None


# --- the live snapshot ------------------------------------------------------

_snapshot = Snapshot(weights={})
_next_refresh = 0.0
_refreshing = False


def current(defaults: dict[str, float]) -> Snapshot:
    """The snapshot in memory, or the hand-set weights until one is computed."""
    if not _snapshot.weights:
        return Snapshot(weights=dict(defaults))
    return _snapshot


def _load_stored(defaults: dict[str, float]) -> Snapshot | None:
    """Blocking: the snapshot another process left in app_state, if still fresh."""
    from auth_alerts import get_app_state  # imported late: auth_alerts imports weather_service

    return from_stored(get_app_state(STATE_KEY), defaults, time.time())


def _store(snapshot: Snapshot, defaults: dict[str, float]) -> None:
    """Blocking: leave the snapshot where the next process will find it."""
    from auth_alerts import set_app_state

    set_app_state(STATE_KEY, to_stored(snapshot, defaults))


def _read(since: str, sources: Sequence[str]) -> list[dict[str, Any]] | None:
    """Blocking: the forecast/observation pairs of the window.

    None when the log tables are not there yet (the migration has not run).
    """
    from auth_alerts import _connect, _has_column

    conn = _connect()
    try:
        if not _has_column(conn, "observation_log", "raining"):
            return None
        columns = ("precip_prob", *(f"prob_{name.replace('-', '_')}" for name in sources))
        return verification.rain_pairs(conn, since, columns)
    finally:
        conn.close()


def _prune_log() -> None:
    """Blocking: drop log rows nothing reads any more (verification.prune)."""
    from auth_alerts import _connect

    conn = _connect()
    try:
        verification.prune(conn, datetime.now(timezone.utc))
    finally:
        conn.close()


def _after_reading(snapshot: Snapshot, defaults: dict[str, float]) -> None:
    """Blocking: the two writes that follow a read of the log.

    Neither is allowed to cost the snapshot that was just computed. If storing
    fails, the next process reads the log itself; if pruning fails, the log is
    a day longer.
    """
    for what, job in (("store", lambda: _store(snapshot, defaults)), ("log prune", _prune_log)):
        try:
            job()
        except Exception as exc:
            logger.warning("calibration %s failed: %s", what, type(exc).__name__)


def _refresh(since: str, sources: Sequence[str], defaults: dict[str, float]) -> None:
    """Blocking: bring the snapshot up to date. Runs in a thread.

    A fresh snapshot in app_state is taken as it is. Only when there is none
    does this process read the log, and then it stores the result for the rest.
    """
    global _snapshot, _next_refresh, _refreshing
    try:
        stored = _load_stored(defaults)
        if stored is not None:
            _snapshot = stored
            age_s = max(0.0, time.time() - (stored.computed_at or 0.0))
            _next_refresh = time.monotonic() + max(60.0, REFRESH_S - age_s)
            return
        rows = _read(since, sources)
        if rows is not None:
            _snapshot = compute(rows, sources, defaults)
            logger.info("calibration refreshed from %d pairs; weights=%s",
                        _snapshot.pairs, _snapshot.weights)
            _after_reading(_snapshot, defaults)
        _next_refresh = time.monotonic() + REFRESH_S
    except Exception as exc:  # a missing table, no credentials, a network blip
        logger.info("calibration refresh skipped: %s", type(exc).__name__)
        _next_refresh = time.monotonic() + RETRY_AFTER_FAILURE_S
    finally:
        _refreshing = False


def refresh_soon(since: str, sources: Sequence[str], defaults: dict[str, float]) -> None:
    """Start a refresh if the snapshot is due; returns at once, never raises."""
    global _refreshing
    if _refreshing or time.monotonic() < _next_refresh:
        return
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return  # no loop (a script or a test): nothing to schedule with
    _refreshing = True
    loop.create_task(asyncio.to_thread(_refresh, since, sources, defaults))


def reset_for_tests() -> None:
    global _snapshot, _next_refresh, _refreshing
    _snapshot = Snapshot(weights={})
    _next_refresh = 0.0
    _refreshing = False
