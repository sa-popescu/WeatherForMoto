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

It also learns the numbers (learn_values): how far each station network reads
from the official ANM station next to it (a bias and a spread), how far
Open-Meteo's temperature an hour ahead lands from that station (globally, and
per cell: the models that run cold in a valley), and the same for gusts. That
goes to value_arbiter as uncertainties and biases, and to the hourly forecast
as a per-cell temperature correction.

All of it is deliberately timid. Nothing is learned from fewer pairs than the
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
import math
import time
from datetime import datetime, timezone
from typing import Any, Iterable, Sequence

import value_arbiter
import verification

logger = logging.getLogger("motonor.calibration")

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


# --- the numbers --------------------------------------------------------------

# A station counts as the reference for a cell when it is at most this far.
REFERENCE_MAX_KM = 15.0
# Pairs needed before a spread or bias is learned, globally and for one cell.
MIN_VALUE_PAIRS = 60
MIN_CELL_PAIRS = 24
# A per-cell correction is never more than this, in °C (before LEARNED_SHARE).
CELL_BIAS_MAX_C = 3.0
# How many cells are kept (the most observed first): the snapshot stays small.
MAX_CELLS = 2000
# Lead times the models are judged at. Temperature uses Open-Meteo's raw value
# an hour ahead; gusts only the final value, at leads the station's own
# correction no longer reaches (BIAS_FADE_HOURS in weather_service).
TEMP_LEADS = (1,)
GUST_LEADS = (6, 12)
_VALUE_FIELDS = {"temperature": "temp", "gusts": "gusts"}


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def _spread_and_bias(diffs: Sequence[float], reference_sigma: float) -> tuple[float, float]:
    """(standard deviation without the reference's own error, mean difference)."""
    bias = _mean(diffs)
    variance = _mean([(d - bias) ** 2 for d in diffs]) - reference_sigma ** 2
    return math.sqrt(max(variance, 0.01)), bias


def learn_values(observations: Iterable[dict[str, Any]], forecasts: Iterable[dict[str, Any]]) -> dict[str, Any]:
    """Uncertainties and biases for value_arbiter, from the measured numbers.

    Returns {"sigma": {variable: {source: σ}}, "bias": {variable: {source: b}},
    "cell_bias": {"temperature": {cell: b}}, "pairs": {...}}, already pulled
    LEARNED_SHARE of the way from the hand-set values, so they can be used as
    they are. Anything without enough pairs is left out (the default stands).
    """
    reference: dict[tuple[str, str], dict[str, Any]] = {}
    others: list[dict[str, Any]] = []
    for row in observations:
        if row.get("source") == "official":
            distance = row.get("distance_km")
            if distance is None or float(distance) <= REFERENCE_MAX_KM:
                reference[(str(row["cell"]), str(row["hour"]))] = row
        else:
            others.append(row)

    sigma: dict[str, dict[str, float]] = {}
    bias: dict[str, dict[str, float]] = {}
    pairs: dict[str, int] = {}

    def keep(variable: str, source: str, diffs: list[float]) -> None:
        pairs[f"{variable}:{source}"] = len(diffs)
        if len(diffs) < MIN_VALUE_PAIRS:
            return
        default = value_arbiter.SIGMA[variable][source]
        spread, mean_diff = _spread_and_bias(diffs, value_arbiter.SIGMA[variable]["official"])
        sigma.setdefault(variable, {})[source] = round(
            default * (1 - LEARNED_SHARE) + spread * LEARNED_SHARE, 3)
        bias.setdefault(variable, {})[source] = round(mean_diff * LEARNED_SHARE, 3)

    # Station networks against the official station of the same cell and hour.
    for variable, field in _VALUE_FIELDS.items():
        for source in ("weatherxm", "netatmo"):
            diffs = []
            for row in others:
                if row.get("source") != source or row.get(field) is None:
                    continue
                ref = reference.get((str(row["cell"]), str(row["hour"])))
                if ref and ref.get(field) is not None:
                    diffs.append(float(row[field]) - float(ref[field]))
            keep(variable, source, diffs)

    # The models against the same station, globally and per cell.
    temp_by_cell: dict[str, list[float]] = {}
    model_diffs: dict[str, list[float]] = {"temperature": [], "gusts": []}
    for row in forecasts:
        ref = reference.get((str(row["cell"]), str(row["valid_hour"])))
        if not ref:
            continue
        lead = row.get("lead_h")
        if lead in TEMP_LEADS and row.get("om_temp") is not None and ref.get("temp") is not None:
            diff = float(row["om_temp"]) - float(ref["temp"])
            model_diffs["temperature"].append(diff)
            temp_by_cell.setdefault(str(row["cell"]), []).append(diff)
        if lead in GUST_LEADS and row.get("gusts") is not None and ref.get("gusts") is not None:
            model_diffs["gusts"].append(float(row["gusts"]) - float(ref["gusts"]))
    for variable, diffs in model_diffs.items():
        keep(variable, "models", diffs)

    cells = sorted(((cell, diffs) for cell, diffs in temp_by_cell.items() if len(diffs) >= MIN_CELL_PAIRS),
                   key=lambda item: -len(item[1]))[:MAX_CELLS]
    cell_bias = {
        cell: round(max(-CELL_BIAS_MAX_C, min(CELL_BIAS_MAX_C, _mean(diffs))) * LEARNED_SHARE, 3)
        for cell, diffs in cells
    }
    out: dict[str, Any] = {"sigma": sigma, "bias": bias, "pairs": pairs}
    if cell_bias:
        out["cell_bias"] = {"temperature": cell_bias}
    return out


def values_for_cell(values: dict[str, Any] | None, cell: str) -> dict[str, Any] | None:
    """The learned numbers as value_arbiter takes them, with this cell's own model bias."""
    if not values:
        return None
    bias = {variable: dict(by_source) for variable, by_source in (values.get("bias") or {}).items()}
    cell_temp = ((values.get("cell_bias") or {}).get("temperature") or {}).get(cell)
    if cell_temp is not None:
        bias.setdefault("temperature", {})["models"] = cell_temp
    return {"sigma": values.get("sigma") or {}, "bias": bias}


def cell_temperature_bias(values: dict[str, Any] | None, cell: str) -> float | None:
    """How much warmer than the truth the models run in this cell, as learned (°C)."""
    return ((((values or {}).get("cell_bias") or {}).get("temperature")) or {}).get(cell)


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

    __slots__ = ("weights", "reliability", "pairs", "computed_at", "values")

    def __init__(
        self,
        weights: dict[str, float],
        reliability: tuple[tuple[float, float], ...] = (),
        pairs: int = 0,
        computed_at: float | None = None,
        values: dict[str, Any] | None = None,
    ) -> None:
        self.weights = weights
        self.reliability = reliability
        self.pairs = pairs
        self.computed_at = computed_at
        # What learn_values found for the numbers; {} until enough is logged.
        self.values = values or {}

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
            "values": {
                "sigma": self.values.get("sigma") or {},
                "bias": self.values.get("bias") or {},
                "pairs": self.values.get("pairs") or {},
                "cells_corrected": len(((self.values.get("cell_bias") or {}).get("temperature")) or {}),
            },
        }


def compute(
    rows: Iterable[dict[str, Any]],
    sources: Sequence[str],
    defaults: dict[str, float],
    *,
    observations: Iterable[dict[str, Any]] = (),
    forecasts: Iterable[dict[str, Any]] = (),
) -> Snapshot:
    """Build a snapshot from the joined forecast/observation rows (and the numbers' rows)."""
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
        values=learn_values(observations, forecasts),
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
        "values": snapshot.values,
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
            values=data.get("values") if isinstance(data.get("values"), dict) else None,
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


def _read(since: str, sources: Sequence[str]) -> dict[str, Any] | None:
    """Blocking: the forecast/observation rows of the window (verification.read_window).

    None when the log tables are not there yet (the migration has not run).
    """
    from auth_alerts import _connect, _has_column

    conn = _connect()
    try:
        if not _has_column(conn, "observation_log", "raining"):
            return None
        columns = ("precip_prob", *(f"prob_{name.replace('-', '_')}" for name in sources))
        return verification.read_window(conn, since, columns)
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
        window = _read(since, sources)
        if window is not None:
            _snapshot = compute(window["rain"], sources, defaults,
                                observations=window["observations"], forecasts=window["forecasts"])
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
