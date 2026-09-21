"""
Weather data aggregation service.

Fetches data from:
  1. OpenWeatherMap (paid key, detailed forecast & air quality)
  2. Open-Meteo (free, no key, high-resolution European model)
  3. MET Norway / Yr (free, no key, AROME/MetCoOp model, best for Europe)
  4. Pirate Weather (free key, Dark Sky-compatible, NOAA GFS/HRRR)

All sources are normalised to the same schema and then merged so that
each numeric field is the weighted average of the available values,
giving more accurate results than any single source alone.
"""

import asyncio
import logging
import math
import os
import time
import unicodedata
from collections import OrderedDict
from collections.abc import AsyncIterator, Awaitable, Callable, Hashable, Iterable
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx

import anm_nowcast
import meteoalarm
import metar
import official_stations

logger = logging.getLogger("weatherformoto.weather")

# ---------------------------------------------------------------------------
# Source URLs
# ---------------------------------------------------------------------------
OWM_BASE = "https://api.openweathermap.org"
OPENMETEO_BASE = "https://api.open-meteo.com/v1/forecast"
GEOCODING_OPENMETEO = "https://geocoding-api.open-meteo.com/v1/search"
OPENMETEO_AIR_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"
OWM_GEO_URL = f"{OWM_BASE}/geo/1.0/direct"
MET_NO_BASE = "https://api.met.no/weatherapi/locationforecast/2.0/complete"
PIRATE_WEATHER_BASE = "https://api.pirateweather.net/forecast"
WEATHERXM_PRO_BASE = "https://pro.weatherxm.com/api/v1"
NETATMO_TOKEN_URL = "https://api.netatmo.com/oauth2/token"
NETATMO_PUBLICDATA_URL = "https://api.netatmo.com/api/getpublicdata"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _weighted_avg(values: list[float | None], weights: list[float]) -> float | None:
    """Return a weighted average, ignoring None values."""
    total_w = 0.0
    total_v = 0.0
    for v, w in zip(values, weights):
        if v is not None:
            total_v += v * w
            total_w += w
    return round(total_v / total_w, 2) if total_w else None


def _wind_direction_label(degrees: float | None) -> str:
    # Romanian cardinal directions: N=Nord, NE=Nord-Est, E=Est, SE=Sud-Est,
    # S=Sud, SV=Sud-Vest, V=Vest, NV=Nord-Vest
    if degrees is None:
        return "—"
    dirs = ["N", "NE", "E", "SE", "S", "SV", "V", "NV"]
    return dirs[round(degrees / 45) % 8]


def _beaufort(speed_kmh: float | None) -> str:
    if speed_kmh is None:
        return "—"
    mps = speed_kmh / 3.6
    scale = [0.3, 1.5, 3.4, 5.4, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6]
    for i, threshold in enumerate(scale):
        if mps < threshold:
            return str(i)
    return "12"


# ---------------------------------------------------------------------------
# Moto score model
# ---------------------------------------------------------------------------
# Every number the score depends on lives in the constants below, so the
# /meta/scoring endpoint publishes exactly what the backend uses and the
# frontend never has to keep a hand-maintained copy in sync.

SCORING_MODEL_VERSION = 2

# Label bands, checked top-down: the first threshold the score reaches wins.
MOTO_LABEL_THRESHOLDS: tuple[tuple[str, int], ...] = (
    ("IDEAL", 85),
    ("OK", 60),
    ("ATENȚIE", 40),
    ("EVITĂ", 0),
)

# WMO weather-code families.
LIGHT_RAIN_CODES = frozenset({51, 53, 55, 61, 80})
HEAVY_RAIN_CODES = frozenset({63, 65, 81, 82})
STORM_CODES = frozenset({95, 96, 99})
SNOW_CODES = frozenset({71, 73, 75, 77, 85, 86})
ICE_CODES = frozenset({56, 57, 66, 67})  # freezing drizzle / freezing rain
FOG_CODES = frozenset({45, 48})

# A precipitation-type code (>= 51) only counts when the forecast carries some
# rain risk; with a known low probability and no amount it is a stale code.
STALE_CODE_MIN = 51
CODE_ACTIVE_MIN_PROBABILITY = 20
CODE_ACTIVE_MIN_AMOUNT_MM = 0.1

# --- Rain: probability x intensity -----------------------------------------
# Hourly amounts (mm/h) below this count as "none".
RAIN_NEGLIGIBLE_MM_H = 0.05
# Intensity bands: (name, lower bound in mm/h, inclusive).
RAIN_INTENSITY_BANDS: tuple[tuple[str, float], ...] = (
    ("urme", RAIN_NEGLIGIBLE_MM_H),
    ("slaba", 0.5),
    ("moderata", 2.5),
    ("puternica", 7.5),
)
RAIN_INTENSITY_ORDER: tuple[str, ...] = ("none", "urme", "slaba", "moderata", "puternica")
# Probability rows of the impact matrix: below the first bound, between the
# two bounds (both inclusive), above the second bound.
RAIN_PROBABILITY_ROW_BOUNDS: tuple[int, int] = (30, 60)
# Rows = probability (< 30 %, 30-60 %, > 60 %); columns = urme, slaba, moderata, puternica.
RAIN_IMPACT_MATRIX: tuple[tuple[str, str, str, str], ...] = (
    ("none", "none", "low", "medium"),
    ("none", "low", "medium", "high"),
    ("low", "medium", "high", "high"),
)
RAIN_IMPACT_CAPS: dict[str, int | None] = {"none": None, "low": 84, "medium": 59, "high": 39}
# Continuous penalty inside the cap: 10 x log2(1 + mm/h) capped at 35 points,
# plus 0.1 points per percent of probability (10 points at 100 %).
RAIN_AMOUNT_PENALTY_SCALE = 10.0
RAIN_AMOUNT_PENALTY_MAX = 35.0
RAIN_PROBABILITY_PENALTY_PER_PCT = 0.1
# When the model expects no measurable amount but the probability is in the
# top row, at least traces are likely: that hour is scored as "urme".
RAIN_HIGH_PROBABILITY_IMPLIES_TRACES = True
# Without any amount, an active rain code implies this intensity.
RAIN_CODE_IMPLIED_INTENSITY: dict[str, str] = {"light": "slaba", "heavy": "moderata"}

# --- Weather-code hazards: (penalty, cap) ----------------------------------
STORM_PENALTY, STORM_CAP = 40, 39
SNOW_PENALTY, SNOW_CAP = 45, 30
ICE_PENALTY, ICE_CAP = 50, 25

# --- Fog / visibility ---------------------------------------------------------
# (visibility upper bound in metres, exclusive; penalty; cap)
VISIBILITY_TIERS: tuple[tuple[int, int, int], ...] = (
    (200, 40, 39),
    (500, 30, 59),
    (1000, 20, 74),
)
# A fog code with good or unknown visibility still costs this much.
FOG_CODE_PENALTY, FOG_CODE_CAP = 15, 84

# --- Wind gusts: (gust above km/h, penalty) --------------------------------
WIND_GUST_PENALTY_TIERS: tuple[tuple[float, int], ...] = ((70, 30), (50, 15), (35, 8))

# --- Temperature (feels-like, °C), tiers ported from the frontend engine ----
# Cold tiers: (feels below, penalty), checked top-down.
COLD_PENALTY_TIERS: tuple[tuple[float, int], ...] = (
    (-5, 50), (0, 40), (5, 28), (10, 15), (14, 5),
)
# Extra penalty below 0 °C feels-like (numb hands, road close to freezing).
SUBZERO_EXTRA_PENALTY = 15
# Heat tiers: (feels above, penalty), checked top-down.
HEAT_PENALTY_TIERS: tuple[tuple[float, int], ...] = ((40, 25), (38, 18), (35, 10))
# Cold and wet together: feels below 5 °C with a rain probability >= 30 %.
COLD_WET_MAX_FEELS, COLD_WET_MIN_PROBABILITY, COLD_WET_PENALTY = 5, 30, 12

# --- Frost / black ice ------------------------------------------------------
# The road (or the air, when the road is unknown) must be at or below this.
FROST_SURFACE_MAX_C = 1.0
# Surface within this many degrees of the dew point: deposition / hoar frost.
FROST_DEWPOINT_MARGIN_C = 1.0
FROST_RISK_CAP = 59

# --- Daily score --------------------------------------------------------------
# Daily score = weighted mix of the mean of the worst quarter of the riding
# hours and the mean of all riding hours, then limited by the rain / storm /
# snow / ice cap that a sustained part (at least a quarter) of those hours
# reach, so one wet hour in an otherwise dry day does not cap the whole day.
DAILY_WORST_HOURS_FRACTION = 0.25
DAILY_WORST_HOURS_WEIGHT = 0.5
DAILY_SUSTAINED_CAP_FRACTION = 0.25
DAILY_HAZARD_FACTORS: tuple[str, ...] = ("rain", "storm", "snow", "ice")
# Fallback for a day without hourly data: assume the daily sum falls within
# this many hours to estimate a peak hourly intensity.
DAILY_SUM_PEAK_HOURS = 4.0
# Daylight fallback when neither is_day nor sunrise/sunset are available.
DEFAULT_DAY_START_HOUR, DEFAULT_DAY_END_HOUR = 7, 20

# WMO codes ordered from least to most severe for riding; used to pick the
# most severe condition of a day across sources.
_CODE_SEVERITY_ORDER: tuple[int, ...] = (
    0, 1, 2, 3, 45, 48, 51, 53, 55, 61, 80, 63, 81, 65, 82,
    71, 77, 85, 73, 75, 86, 56, 57, 66, 67, 95, 96, 99,
)
_CODE_SEVERITY: dict[int, int] = {code: rank for rank, code in enumerate(_CODE_SEVERITY_ORDER)}

_RAIN_IMPACT_RO: dict[str, str] = {
    "none": "neglijabil", "low": "scăzut", "medium": "mediu", "high": "ridicat",
}


def _factor(name: str, penalty: float, cap: int | None, detail: str) -> dict[str, Any]:
    """Build one score_breakdown entry (penalty rounded to whole points)."""
    return {"factor": name, "penalty": int(round(penalty)), "cap": cap, "detail": detail}


def _code_is_stale(
    code: int | None, amount_mm: float | None, probability: float | None
) -> bool:
    """True when a precipitation-type code (>= 51) has no rain risk behind it.

    Models sometimes keep a rain/storm code for an hour that is essentially
    dry. Only a KNOWN low probability together with no measured amount marks
    the code as stale; an unknown probability trusts the code (safety first).
    """
    if code is None or code < STALE_CODE_MIN or probability is None:
        return False
    return (
        probability < CODE_ACTIVE_MIN_PROBABILITY
        and (amount_mm or 0) < CODE_ACTIVE_MIN_AMOUNT_MM
    )


def _rain_intensity(amount_mm_h: float | None) -> str:
    """Classify an hourly amount as none / urme / slaba / moderata / puternica."""
    label = "none"
    if amount_mm_h is None:
        return label
    for name, lower_bound in RAIN_INTENSITY_BANDS:
        if amount_mm_h >= lower_bound:
            label = name
    return label


def _probability_row(probability: float) -> int:
    """Row of the impact matrix: 0 below 30 %, 1 for 30-60 %, 2 above 60 %."""
    low, high = RAIN_PROBABILITY_ROW_BOUNDS
    if probability < low:
        return 0
    if probability <= high:
        return 1
    return 2


def _rain_impact(probability: float, intensity: str) -> str:
    """Look up the impact level (none / low / medium / high) in the matrix."""
    if intensity == "none":
        return "none"
    column = RAIN_INTENSITY_ORDER.index(intensity) - 1
    return RAIN_IMPACT_MATRIX[_probability_row(probability)][column]


def _rain_factor(
    amount_mm: float | None, probability: float | None, code: int | None
) -> dict[str, Any] | None:
    """Rain: continuous penalty, capped by the probability x intensity matrix.

    An unknown probability together with a measurable amount (or an active
    rain code) means it is raining, so it is scored as a certainty.
    """
    intensity = _rain_intensity(amount_mm)
    if amount_mm is None and not _code_is_stale(code, amount_mm, probability):
        if code in LIGHT_RAIN_CODES:
            intensity = RAIN_CODE_IMPLIED_INTENSITY["light"]
        elif code in HEAVY_RAIN_CODES:
            intensity = RAIN_CODE_IMPLIED_INTENSITY["heavy"]

    if probability is None:
        if intensity == "none":
            return None
        prob = 100.0
    else:
        prob = float(max(0.0, min(100.0, probability)))

    scored_intensity = intensity
    if (
        intensity == "none"
        and RAIN_HIGH_PROBABILITY_IMPLIES_TRACES
        and _probability_row(prob) == 2
    ):
        scored_intensity = "urme"

    amount = max(0.0, amount_mm or 0.0)
    penalty = min(RAIN_AMOUNT_PENALTY_MAX, RAIN_AMOUNT_PENALTY_SCALE * math.log2(1 + amount))
    penalty += prob * RAIN_PROBABILITY_PENALTY_PER_PCT
    impact = _rain_impact(prob, scored_intensity)
    amount_text = f"{amount:.1f} mm/h" if amount_mm is not None else "cantitate necunoscută"
    detail = f"probabilitate {prob:.0f}%, {amount_text} ({intensity}) → impact {_RAIN_IMPACT_RO[impact]}"
    return _factor("rain", penalty, RAIN_IMPACT_CAPS[impact], detail)


def _code_hazard_factor(
    code: int | None, amount_mm: float | None, probability: float | None
) -> dict[str, Any] | None:
    """Freezing rain, snow and thunderstorm codes: fixed penalty plus a hard cap."""
    if code is None or _code_is_stale(code, amount_mm, probability):
        return None
    if code in ICE_CODES:
        return _factor("ice", ICE_PENALTY, ICE_CAP, f"polei / precipitații înghețate (cod WMO {code})")
    if code in SNOW_CODES:
        return _factor("snow", SNOW_PENALTY, SNOW_CAP, f"ninsoare (cod WMO {code})")
    if code in STORM_CODES:
        return _factor("storm", STORM_PENALTY, STORM_CAP, f"furtună (cod WMO {code})")
    return None


def _visibility_factor(code: int | None, visibility_m: float | None) -> dict[str, Any] | None:
    """Fog and low visibility. A fog code is never ignored, even when dry."""
    if visibility_m is not None:
        for upper_bound, penalty, cap in VISIBILITY_TIERS:
            if visibility_m < upper_bound:
                return _factor("fog", penalty, cap, f"vizibilitate {visibility_m:.0f} m")
    if code in FOG_CODES:
        seen = f", vizibilitate {visibility_m / 1000:.1f} km" if visibility_m is not None else ""
        return _factor("fog", FOG_CODE_PENALTY, FOG_CODE_CAP, f"ceață (cod WMO {code}){seen}")
    return None


def _wind_penalty(wind_gusts_kmh: float | None) -> int:
    """Penalty for wind gusts (km/h); missing data costs nothing."""
    if wind_gusts_kmh is None:
        return 0
    for threshold, penalty in WIND_GUST_PENALTY_TIERS:
        if wind_gusts_kmh > threshold:
            return penalty
    return 0


def _wind_factor(wind_gusts_kmh: float | None) -> dict[str, Any] | None:
    penalty = _wind_penalty(wind_gusts_kmh)
    if not penalty or wind_gusts_kmh is None:
        return None
    return _factor("wind", penalty, None, f"rafale {wind_gusts_kmh:.0f} km/h")


def _hourly_temperature_penalty(feels_like: float | None) -> int:
    """Cold / heat penalty for a feels-like temperature, sub-zero steps included."""
    if feels_like is None:
        return 0
    for threshold, penalty in COLD_PENALTY_TIERS:
        if feels_like < threshold:
            return penalty + (SUBZERO_EXTRA_PENALTY if feels_like < 0 else 0)
    for threshold, penalty in HEAT_PENALTY_TIERS:
        if feels_like > threshold:
            return penalty
    return 0


def _temperature_factors(
    feels_like: float | None, probability: float | None
) -> list[dict[str, Any]]:
    """Cold or heat factor, plus the cold-and-wet compound factor."""
    factors: list[dict[str, Any]] = []
    if feels_like is None:
        return factors
    penalty = _hourly_temperature_penalty(feels_like)
    if penalty:
        is_cold = feels_like < COLD_PENALTY_TIERS[-1][0]
        detail = f"resimțit {feels_like:.0f} °C" + (" (sub 0 °C)" if feels_like < 0 else "")
        factors.append(_factor("cold" if is_cold else "heat", penalty, None, detail))
    if (
        feels_like < COLD_WET_MAX_FEELS
        and probability is not None
        and probability >= COLD_WET_MIN_PROBABILITY
    ):
        factors.append(_factor(
            "cold_wet", COLD_WET_PENALTY, None,
            f"frig și probabilitate de ploaie {probability:.0f}%",
        ))
    return factors


def _frost_factor(frost_risk: bool) -> dict[str, Any] | None:
    if not frost_risk:
        return None
    return _factor("frost", 0, FROST_RISK_CAP, "risc de gheață / polei pe carosabil")


def _score_with_breakdown(
    feels_like: float | None,
    wind_gusts_kmh: float | None,
    precipitation_mm: float | None,
    weather_code: int | None,
    precipitation_probability: float | None = None,
    *,
    visibility_m: float | None = None,
    frost_risk: bool = False,
) -> tuple[int | None, list[dict[str, Any]]]:
    """Compute the 0-100 moto score and the factors that explain it.

    Score = 100 minus the sum of the factor penalties, then limited by the
    lowest factor cap. Returns (None, []) when there is nothing to score.
    """
    inputs = (
        feels_like, wind_gusts_kmh, precipitation_mm, weather_code,
        precipitation_probability, visibility_m,
    )
    if all(value is None for value in inputs):
        return None, []

    rain = _rain_factor(precipitation_mm, precipitation_probability, weather_code)
    hazard = _code_hazard_factor(weather_code, precipitation_mm, precipitation_probability)
    if rain is not None and hazard is not None:
        # Storm / snow / ice precipitation is the same water the rain factor
        # already counts: the hazard only adds what exceeds the rain penalty
        # (its cap still applies in full).
        hazard["penalty"] = max(0, hazard["penalty"] - rain["penalty"])
    candidates = [
        rain,
        hazard,
        _visibility_factor(weather_code, visibility_m),
        _wind_factor(wind_gusts_kmh),
        *_temperature_factors(feels_like, precipitation_probability),
        _frost_factor(frost_risk),
    ]
    factors = [
        f for f in candidates
        if f is not None and (f["penalty"] > 0 or f["cap"] is not None)
    ]
    score = 100 - sum(f["penalty"] for f in factors)
    caps = [f["cap"] for f in factors if f["cap"] is not None]
    if caps:
        score = min(score, min(caps))
    return max(0, min(100, score)), factors


def _moto_score(
    feels_like: float | None,
    wind_gusts_kmh: float | None,
    precipitation_mm: float | None,
    weather_code: int | None,
    precipitation_probability: float | None = None,
    *,
    visibility_m: float | None = None,
    frost_risk: bool = False,
) -> int | None:
    """
    Compute a 0-100 'moto suitability' score (higher = better riding).
    Returns None when no input at all is available.
    """
    score, _ = _score_with_breakdown(
        feels_like, wind_gusts_kmh, precipitation_mm, weather_code,
        precipitation_probability, visibility_m=visibility_m, frost_risk=frost_risk,
    )
    return score


def _effective_display_code(
    code: int | None,
    precipitation_mm: float | None,
    precipitation_probability: float | None = 0,
) -> int | None:
    """Keep the displayed weather code consistent with the score's de-weight.

    A precipitation/storm code that isn't actually precipitating (known
    probability below 20% AND no measured precip) is downgraded to overcast
    (3), so the icon and description never show a storm next to a high
    (green) score. Uses the same rule as the score (``_code_is_stale``).
    """
    if code is None:
        return code
    return 3 if _code_is_stale(code, precipitation_mm, precipitation_probability) else code


def _most_severe_code(codes: Iterable[int | None]) -> int | None:
    """Return the most severe WMO code of a set (unknown codes rank lowest)."""
    known = [c for c in codes if c is not None]
    if not known:
        return None
    return max(known, key=lambda c: _CODE_SEVERITY.get(c, -1))


def _hour_hazard_cap(hour: dict[str, Any]) -> int | None:
    """Lowest rain / storm / snow / ice cap of one hourly item (None = no cap)."""
    _, factors = _score_with_breakdown(
        hour.get("feels_like"),
        hour.get("wind_gusts_kmh"),
        hour.get("precipitation_mm"),
        hour.get("weather_code"),
        hour.get("precipitation_probability"),
        visibility_m=hour.get("visibility"),
        frost_risk=bool(hour.get("frost_risk")),
    )
    caps = [
        f["cap"] for f in factors
        if f["factor"] in DAILY_HAZARD_FACTORS and f["cap"] is not None
    ]
    return min(caps) if caps else None


def _sustained_hazard_cap(scored_hours: list[dict[str, Any]]) -> int | None:
    """Hazard cap reached by a sustained part of the scored hours (None = no cap).

    The hours' rain / storm / snow / ice caps are sorted from worst to none
    and the cap at index ceil(0.25 x n) - 1 is used: the worst cap that at
    least a quarter of the hours share. With 1-4 hours that is the worst hour.
    """
    if not scored_hours:
        return None
    caps = sorted(
        (_hour_hazard_cap(h) for h in scored_hours),
        key=lambda cap: math.inf if cap is None else cap,
    )
    index = max(1, math.ceil(len(caps) * DAILY_SUSTAINED_CAP_FRACTION)) - 1
    return caps[index]


def _daily_score_from_hours(hours: list[dict[str, Any]]) -> int | None:
    """Daily score from the riding hours: weight the worst hours, cap by sustained hazards."""
    scored = [h for h in hours if h.get("moto_score") is not None]
    if not scored:
        return None
    scores = sorted(h["moto_score"] for h in scored)
    worst_count = max(1, math.ceil(len(scores) * DAILY_WORST_HOURS_FRACTION))
    worst_mean = sum(scores[:worst_count]) / worst_count
    all_mean = sum(scores) / len(scores)
    score = DAILY_WORST_HOURS_WEIGHT * worst_mean + (1 - DAILY_WORST_HOURS_WEIGHT) * all_mean
    cap = _sustained_hazard_cap(scored)
    if cap is not None:
        score = min(score, cap)
    return max(0, min(100, round(score)))


def _daylight_display_code(hours: list[dict[str, Any]]) -> int | None:
    """Most severe display code of the scored daylight hours (None if there are none).

    Uses the same stale-code rule as the score, so a dry hour's leftover rain
    code does not drive the day's icon.
    """
    return _most_severe_code(
        _effective_display_code(
            h.get("weather_code"), h.get("precipitation_mm"), h.get("precipitation_probability")
        )
        for h in hours
        if h.get("is_day") and h.get("moto_score") is not None
    )


def _riding_hours(day_hours: list[dict[str, Any]], now_local: str | None) -> list[dict[str, Any]]:
    """Hours a daily score describes: daylight ones, and for today only those ahead.

    ``now_local`` ("YYYY-MM-DDTHH:MM", location time) is given only for today.
    Falls back to the remaining (or all) hours when no daylight hour is left.
    """
    candidates = day_hours
    if now_local is not None:
        ahead = [h for h in day_hours if str(h.get("time", "")) >= now_local[:13]]
        candidates = ahead or day_hours
    daylight = [h for h in candidates if h.get("is_day")]
    return daylight or candidates


def _moto_score_daily(
    feels_min: float | None,
    feels_max: float | None,
    wind_gusts_kmh: float | None,
    precipitation_mm_day: float | None,
    weather_code: int | None,
    precipitation_probability: float | None = None,
) -> int | None:
    """
    Daily 0-100 moto score from daily aggregates only.

    Fallback for days without hourly data: the daily sum is turned into an
    estimated peak hourly intensity and scored with the hourly model, using
    the average feels-like (or the maximum on hot days).
    """
    known = [v for v in (feels_min, feels_max) if v is not None]
    feels = sum(known) / len(known) if known else None
    if feels_max is not None and feels_max > HEAT_PENALTY_TIERS[-1][0]:
        feels = feels_max
    peak_mm_h = (
        precipitation_mm_day / DAILY_SUM_PEAK_HOURS
        if precipitation_mm_day is not None else None
    )
    return _moto_score(feels, wind_gusts_kmh, peak_mm_h, weather_code, precipitation_probability)


def _moto_label(score: int | None) -> str | None:
    """Map a score to IDEAL / OK / ATENȚIE / EVITĂ (None when not computable)."""
    if score is None:
        return None
    for label, threshold in MOTO_LABEL_THRESHOLDS:
        if score >= threshold:
            return label
    return MOTO_LABEL_THRESHOLDS[-1][0]


def scoring_metadata() -> dict[str, Any]:
    """Describe the score model for GET /meta/scoring (same numbers as above)."""
    low, high = RAIN_PROBABILITY_ROW_BOUNDS
    bands: list[dict[str, Any]] = [
        {"name": "none", "min_mm_h": 0.0, "max_mm_h": RAIN_INTENSITY_BANDS[0][1]},
    ]
    for index, (name, lower_bound) in enumerate(RAIN_INTENSITY_BANDS):
        upper_bound = (
            RAIN_INTENSITY_BANDS[index + 1][1] if index + 1 < len(RAIN_INTENSITY_BANDS) else None
        )
        bands.append({"name": name, "min_mm_h": lower_bound, "max_mm_h": upper_bound})

    def _hazard(codes: frozenset[int], penalty: int, cap: int) -> dict[str, Any]:
        return {"codes": sorted(codes), "penalty": penalty, "cap": cap}

    return {
        "version": SCORING_MODEL_VERSION,
        "labels": [
            {"label": label, "min_score": threshold}
            for label, threshold in MOTO_LABEL_THRESHOLDS
        ],
        "rain": {
            "intensity_bands": bands,  # min inclusive, max exclusive, null = open
            "probability_rows": [
                {"row": 0, "label": f"<{low}%", "min_pct": 0, "max_pct_exclusive": low},
                {"row": 1, "label": f"{low}-{high}%", "min_pct": low, "max_pct_inclusive": high},
                {"row": 2, "label": f">{high}%", "min_pct_exclusive": high, "max_pct": 100},
            ],
            "impact_matrix": {
                "columns": list(RAIN_INTENSITY_ORDER[1:]),
                "rows": [list(row) for row in RAIN_IMPACT_MATRIX],
            },
            "impact_caps": dict(RAIN_IMPACT_CAPS),
            "amount_penalty": {
                "formula": "min(max, scale * log2(1 + mm_h))",
                "scale": RAIN_AMOUNT_PENALTY_SCALE,
                "max": RAIN_AMOUNT_PENALTY_MAX,
            },
            "probability_penalty_per_pct": RAIN_PROBABILITY_PENALTY_PER_PCT,
            "high_probability_implies_traces": RAIN_HIGH_PROBABILITY_IMPLIES_TRACES,
            "unknown_probability_with_rain_pct": 100,
            "code_implied_intensity": dict(RAIN_CODE_IMPLIED_INTENSITY),
        },
        "stale_code_rule": {
            "codes_from": STALE_CODE_MIN,
            "min_probability_pct": CODE_ACTIVE_MIN_PROBABILITY,
            "min_amount_mm": CODE_ACTIVE_MIN_AMOUNT_MM,
        },
        "hazards": {
            "storm": _hazard(STORM_CODES, STORM_PENALTY, STORM_CAP),
            "snow": _hazard(SNOW_CODES, SNOW_PENALTY, SNOW_CAP),
            "ice": _hazard(ICE_CODES, ICE_PENALTY, ICE_CAP),
            "fog": {
                **_hazard(FOG_CODES, FOG_CODE_PENALTY, FOG_CODE_CAP),
                "visibility_tiers": [
                    {"below_m": bound, "penalty": penalty, "cap": cap}
                    for bound, penalty, cap in VISIBILITY_TIERS
                ],
            },
        },
        "wind_gust_tiers": [
            {"above_kmh": threshold, "penalty": penalty}
            for threshold, penalty in WIND_GUST_PENALTY_TIERS
        ],
        "temperature": {
            "cold_tiers": [
                {"feels_below_c": threshold, "penalty": penalty}
                for threshold, penalty in COLD_PENALTY_TIERS
            ],
            "subzero_extra_penalty": SUBZERO_EXTRA_PENALTY,
            "heat_tiers": [
                {"feels_above_c": threshold, "penalty": penalty}
                for threshold, penalty in HEAT_PENALTY_TIERS
            ],
            "cold_wet": {
                "feels_below_c": COLD_WET_MAX_FEELS,
                "min_probability_pct": COLD_WET_MIN_PROBABILITY,
                "penalty": COLD_WET_PENALTY,
            },
        },
        "frost": {
            "surface_max_c": FROST_SURFACE_MAX_C,
            "dewpoint_margin_c": FROST_DEWPOINT_MARGIN_C,
            "cap": FROST_RISK_CAP,
        },
        "daily": {
            "worst_hours_fraction": DAILY_WORST_HOURS_FRACTION,
            "worst_hours_weight": DAILY_WORST_HOURS_WEIGHT,
            "hazard_factors": list(DAILY_HAZARD_FACTORS),
            "sustained_cap_fraction": DAILY_SUSTAINED_CAP_FRACTION,
            "sustained_cap_rule": "caps sorted worst first; cap at index ceil(fraction * n) - 1",
            "hours": "daylight hours (sunrise to sunset); for today only the hours still ahead",
            "weather_code": "most severe display code of the scored daylight hours; "
                            "whole day across sources when no daylight hour is scored",
        },
        "confidence": {
            "models": list(ENSEMBLE_MODELS),
            "variables": list(ENSEMBLE_VARIABLES),
            "min_models": ENSEMBLE_MIN_MODELS,
            "blend": {
                "seamless_weight": ENSEMBLE_SEAMLESS_WEIGHT,
                "model_mean_weight": ENSEMBLE_MODEL_WEIGHT,
                "median_variables": sorted(ENSEMBLE_MEDIAN_VARIABLES),
                "weather_code": "severity-ranked vote of the models, the seamless blend "
                                "voting with its weight; the middle vote wins",
            },
            "measured_bias": {
                "fade_hours": BIAS_FADE_HOURS,
                "limits": {hour_field: limit for _, hour_field, limit in BIAS_FIELDS},
                "rule": "station minus forecast for the current hour, carried to the next "
                        "hours with a weight falling linearly from 1 to 0",
            },
            "station_altitude": {
                "lapse_rate_c_per_m": LAPSE_RATE_C_PER_M,
                "max_correction_c": MAX_ALTITUDE_CORRECTION_C,
            },
            "spread_max": {
                "high": dict(ENSEMBLE_HIGH_SPREAD),
                "medium": dict(ENSEMBLE_MEDIUM_SPREAD),
            },
            "rule": "spread is max - min across the models for that hour; "
                    "the worst variable decides the level, and anything above "
                    "the medium limits is low",
        },
    }


def _road_surface_temp(
    air_temp: float | None,
    humidity: float | None,
    weather_code: int | None,
    precipitation_mm: float | None,
    is_day: bool | None = None,
) -> float | None:
    """
    Estimate road surface temperature.
    Dark asphalt absorbs solar radiation — on sunny days it can be
    significantly warmer than the air. Wet roads approach air temp.
    At night there is no solar gain: under a clear sky the asphalt radiates
    heat away and ends up colder than the air, which is exactly when black
    ice forms. ``is_day=None`` (unknown) keeps the daytime estimate.
    """
    if air_temp is None:
        return None

    t = air_temp
    h = humidity or 60
    code = weather_code or 0
    p = precipitation_mm or 0
    night = is_day is False

    if p > 0.5 or code in (51, 53, 55, 61, 63, 65, 80, 81, 82, 95, 96, 99):
        # Wet road — evaporative cooling, close to air temp
        road_temp = t - 1.0
    elif code in SNOW_CODES:
        # Snow — insulating layer, road near air temp
        road_temp = t
    elif code in FOG_CODES:
        # Fog — high humidity, reduced solar
        road_temp = t - 0.5
    elif night:
        if code in (0, 1):
            # Clear night — strong radiative cooling of the asphalt
            road_temp = t - 1.5
        elif code == 2:
            road_temp = t - 0.5
        else:
            # Clouds keep some of the ground's stored heat
            road_temp = t + 0.5
    elif code in (0, 1):
        # Clear sky — dark asphalt strongly absorbs solar radiation
        road_temp = t + 9.0 if t > 15 else t + 4.0
    elif code == 2:
        # Partly cloudy
        road_temp = t + 4.0
    elif code == 3:
        # Overcast
        road_temp = t + 1.0
    else:
        road_temp = t + 1.0

    # High humidity reduces the solar heating effect (daytime only)
    if not night and h > 85 and p < 0.2:
        road_temp -= 2.0

    return round(road_temp, 1)


def _dew_point_c(temp_c: float | None, humidity_pct: float | None) -> float | None:
    """Dew point from air temperature and relative humidity (Magnus formula)."""
    if temp_c is None or humidity_pct is None or humidity_pct <= 0:
        return None
    a, b = 17.62, 243.12
    gamma = math.log(min(humidity_pct, 100.0) / 100.0) + a * temp_c / (b + temp_c)
    return round(b * gamma / (a - gamma), 1)


def _frost_risk(
    air_temp: float | None,
    road_temp: float | None,
    dew_point: float | None,
    precipitation_mm: float | None,
    weather_code: int | None,
) -> bool:
    """Risk of ice / hoar frost on the road surface.

    Needs a surface (road, else air) at or below ~1 °C plus a source of
    moisture: freezing / frozen precipitation codes, a wet road, or a surface
    at or below the dew point (+margin), where water vapour deposits as frost.
    Rain is NOT required: clear, humid nights are the classic black-ice case.
    """
    surface = road_temp if road_temp is not None else air_temp
    if surface is None or surface > FROST_SURFACE_MAX_C:
        return False
    if weather_code in ICE_CODES or weather_code in SNOW_CODES or weather_code == 48:
        return True
    if (precipitation_mm or 0) >= RAIN_NEGLIGIBLE_MM_H:
        return True
    if dew_point is not None:
        return surface <= dew_point + FROST_DEWPOINT_MARGIN_C
    # No humidity information: a surface below zero is treated as a risk.
    return surface <= 0.0


def _sun_times_by_date(om_data: dict) -> dict[str, tuple[str, str]]:
    """Map local date -> (sunrise, sunset) local ISO strings from Open-Meteo daily."""
    daily = om_data.get("daily") or {}
    result: dict[str, tuple[str, str]] = {}
    for date, sunrise, sunset in zip(
        daily.get("time") or [], daily.get("sunrise") or [], daily.get("sunset") or []
    ):
        if date and sunrise and sunset:
            result[date] = (sunrise[:16], sunset[:16])
    return result


def _hour_is_day(
    is_day_value: Any, time_iso: str, sun_by_date: dict[str, tuple[str, str]]
) -> bool:
    """Daylight flag for one local timestamp.

    Uses Open-Meteo's is_day when present, else sunrise/sunset of that date,
    else a fixed 07:00-20:00 window.
    """
    if is_day_value is not None:
        return bool(is_day_value)
    sun = sun_by_date.get(time_iso[:10])
    if sun is not None:
        return sun[0] <= time_iso[:16] < sun[1]
    try:
        hour = int(time_iso[11:13])
    except ValueError:
        return True
    return DEFAULT_DAY_START_HOUR <= hour < DEFAULT_DAY_END_HOUR


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance in km between two points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(a))


# ---------------------------------------------------------------------------
# Open-Meteo WMO weather-code → (description, emoji)
# ---------------------------------------------------------------------------
_WMO_MAP: dict[int, tuple[str, str]] = {
    0: ("Cer senin", "☀️"),
    1: ("Predominant senin", "🌤️"),
    2: ("Parțial înnorat", "⛅"),
    3: ("Înnorat", "☁️"),
    45: ("Ceață", "🌫️"),
    48: ("Ceață cu chiciură", "🌫️"),
    51: ("Burniță ușoară", "🌦️"),
    53: ("Burniță moderată", "🌦️"),
    55: ("Burniță densă", "🌧️"),
    56: ("Burniță înghețată ușoară", "🧊"),
    57: ("Burniță înghețată densă", "🧊"),
    61: ("Ploaie ușoară", "🌧️"),
    63: ("Ploaie moderată", "🌧️"),
    65: ("Ploaie torențială", "🌧️"),
    66: ("Ploaie înghețată ușoară (polei)", "🧊"),
    67: ("Ploaie înghețată puternică (polei)", "🧊"),
    71: ("Ninsoare ușoară", "🌨️"),
    73: ("Ninsoare moderată", "❄️"),
    75: ("Ninsoare puternică", "❄️"),
    77: ("Grăunțe de zăpadă", "🌨️"),
    80: ("Averse ușoare", "🌦️"),
    81: ("Averse moderate", "🌧️"),
    82: ("Averse violente", "⛈️"),
    85: ("Averse de ninsoare ușoare", "🌨️"),
    86: ("Averse de ninsoare puternice", "❄️"),
    95: ("Furtună", "⛈️"),
    96: ("Furtună cu grindină", "⛈️"),
    99: ("Furtună puternică cu grindină", "⛈️"),
}


def _wmo_desc(code: int | None) -> str:
    if code is None:
        return "—"
    return _WMO_MAP.get(code, ("—", "🌡️"))[0]


def _wmo_icon(code: int | None) -> str:
    if code is None:
        return "🌡️"
    return _WMO_MAP.get(code, ("—", "🌡️"))[1]


# ---------------------------------------------------------------------------
# OWM weather-condition id → WMO-like code (approximate mapping)
# ---------------------------------------------------------------------------
def _owm_id_to_wmo(owm_id: int) -> int:
    """Best-effort mapping of OWM condition IDs to WMO codes."""
    if 200 <= owm_id < 300:
        return 95
    if 300 <= owm_id < 400:
        return 53
    if owm_id in (500,):
        return 61
    if owm_id in (501,):
        return 63
    if 500 <= owm_id < 510:
        return 65
    if owm_id == 511:
        return 66  # freezing rain
    if 510 <= owm_id < 600:
        return 80
    if owm_id in (600,):
        return 71
    if owm_id in (601,):
        return 73
    if 600 <= owm_id < 700:
        return 75
    if owm_id in (741,):
        return 45
    if owm_id in (701, 711, 721, 731, 751, 761, 762, 771, 781):
        return 45
    if owm_id == 800:
        return 0
    if owm_id == 801:
        return 1
    if owm_id == 802:
        return 2
    if owm_id in (803, 804):
        return 3
    return 0


# MET Norway symbol (without _day/_night suffix) → WMO code. MET "sleet" is a
# rain and snow mix (Romanian "lapoviță"); it maps to the snow family so it is
# scored with the snow cap. The "lights..." spellings are MET's own typos.
_MET_SYMBOL_TO_WMO: dict[str, int] = {
    "clearsky": 0, "fair": 1, "partlycloudy": 2, "cloudy": 3,
    "fog": 45,
    "lightrain": 61, "rain": 63, "heavyrain": 65,
    "lightrainshowers": 80, "rainshowers": 81, "heavyrainshowers": 82,
    "lightsleet": 71, "sleet": 73, "heavysleet": 75,
    "lightsleetshowers": 85, "sleetshowers": 85, "heavysleetshowers": 86,
    "lightsnow": 71, "snow": 73, "heavysnow": 75,
    "lightsnowshowers": 85, "snowshowers": 85, "heavysnowshowers": 86,
    "lightrainandthunder": 95, "rainandthunder": 95, "heavyrainandthunder": 99,
    "lightrainshowersandthunder": 95, "rainshowersandthunder": 95,
    "heavyrainshowersandthunder": 99,
    "lightsleetandthunder": 95, "sleetandthunder": 95, "heavysleetandthunder": 99,
    "lightssleetshowersandthunder": 95, "sleetshowersandthunder": 95,
    "heavysleetshowersandthunder": 99,
    "lightsnowandthunder": 95, "snowandthunder": 95, "heavysnowandthunder": 99,
    "lightssnowshowersandthunder": 95, "snowshowersandthunder": 95,
    "heavysnowshowersandthunder": 99,
}


def _met_symbol_to_wmo(symbol: str | None) -> int | None:
    """Map MET Norway symbol_code strings to WMO weather codes (None if unknown)."""
    if not symbol:
        return None
    s = symbol
    for suffix in ("_day", "_night", "_polartwilight"):
        if symbol.endswith(suffix):
            s = symbol[: -len(suffix)]
            break
    code = _MET_SYMBOL_TO_WMO.get(s)
    if code is None:
        logger.info("Unknown MET Norway symbol_code %r ignored", symbol)
    return code


def _pw_icon_to_wmo(icon: str | None) -> int | None:
    """Map Pirate Weather (Dark Sky) icon strings to WMO weather codes (None if unknown).

    Dark Sky's "sleet" covers sleet, freezing rain and ice pellets, so it maps
    to light freezing rain (66) and gets the ice cap.
    """
    table = {
        "clear-day": 0, "clear-night": 0,
        "partly-cloudy-day": 2, "partly-cloudy-night": 2,
        "cloudy": 3, "wind": 3, "fog": 45,
        "rain": 63, "sleet": 66, "snow": 73,
    }
    return table.get(icon or "")


# ---------------------------------------------------------------------------
# Shared HTTP client, response cache and time budget
# ---------------------------------------------------------------------------
# MET Norway's terms require an identifying User-Agent (app + contact URL).
DEFAULT_MET_USER_AGENT = "WeatherForMoto/1.0 (+https://weatherformoto.bluemouse.cc)"
# The old placeholder default; MET Norway may throttle or block it.
_PLACEHOLDER_USER_AGENT_MARKER = "github.com/user/"

# Time budget (seconds). Optional providers get a short wait; a slower answer
# still completes in the background and fills the cache for the next request.
# Open-Meteo is required, so it gets longer attempts and one retry, and the
# whole aggregation is bounded so the frontend (12 s timeout) gets an answer.
OPTIONAL_PROVIDER_BUDGET_S = 3.5
OPTIONAL_PROVIDER_HTTP_TIMEOUT_S = 8.0
OPENMETEO_ATTEMPT_TIMEOUT_S = 6.0
OPENMETEO_ATTEMPTS = 2
OPENMETEO_RETRY_DELAY_S = 0.5
WEATHER_TOTAL_BUDGET_S = 8.0

# Cache: coordinates rounded to this grid (~5 km) share an entry.
CACHE_COORD_STEP_DEG = 0.05
TTL_OPENMETEO_S = 12 * 60
TTL_OWM_S = 12 * 60
TTL_AIR_QUALITY_S = 30 * 60
TTL_MET_DEFAULT_S = 30 * 60
TTL_MET_MIN_S = 60
TTL_MET_MAX_S = 2 * 3600
TTL_PIRATE_S = 30 * 60
TTL_ENSEMBLE_S = 30 * 60
TTL_METEOALARM_S = 10 * 60
# Stations report hourly and airports every 30 minutes; nowcasting warnings last under two hours.
TTL_STATIONS_S = 10 * 60
TTL_METAR_S = 10 * 60
TTL_ANM_NOWCAST_S = 5 * 60
# Weight of an official station's reading in the current-conditions blend
# (models weigh about 1, a Netatmo citizen station 1.3).
OFFICIAL_STATION_WEIGHT = 2.0
TTL_GEOCODE_S = 24 * 3600
FORECAST_CACHE_MAX_ENTRIES = 400
GEOCODE_CACHE_MAX_ENTRIES = 1000

_DEFAULT_HTTP_TIMEOUT = httpx.Timeout(10.0, connect=5.0)
_shared_client: httpx.AsyncClient | None = None
_shared_client_loop: asyncio.AbstractEventLoop | None = None


def set_http_client(client: httpx.AsyncClient | None) -> None:
    """Register the app-wide client created in main.py's lifespan (None clears it)."""
    global _shared_client, _shared_client_loop
    _shared_client = client
    try:
        _shared_client_loop = asyncio.get_running_loop() if client is not None else None
    except RuntimeError:
        _shared_client_loop = None


@asynccontextmanager
async def http_client_scope() -> AsyncIterator[httpx.AsyncClient]:
    """Yield the shared client when usable here, else a short-lived one.

    The fallback covers tests, scripts and callers running on another event
    loop (an AsyncClient must not be shared across event loops).
    """
    shared = _shared_client
    if (
        shared is not None
        and not shared.is_closed
        and _shared_client_loop in (None, asyncio.get_running_loop())
    ):
        yield shared
        return
    async with httpx.AsyncClient(timeout=_DEFAULT_HTTP_TIMEOUT) as client:
        yield client


class _TTLCache:
    """Bounded in-process LRU cache with per-entry TTL and single-flight fetches.

    Concurrent requests for the same key share one upstream call. Only
    non-None values are stored, so a failing provider is retried next time.
    """

    def __init__(self, name: str, max_entries: int) -> None:
        self.name = name
        self.max_entries = max_entries
        self._entries: OrderedDict[Hashable, tuple[float, Any]] = OrderedDict()
        self._inflight: dict[Hashable, asyncio.Task] = {}

    def __len__(self) -> int:
        return len(self._entries)

    def clear(self) -> None:
        self._entries.clear()
        self._inflight.clear()

    def get(self, key: Hashable) -> Any | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        expires_at, value = entry
        if time.monotonic() >= expires_at:
            del self._entries[key]
            return None
        self._entries.move_to_end(key)
        return value

    def put(self, key: Hashable, value: Any, ttl_s: float) -> None:
        if value is None or ttl_s <= 0:
            return
        self._entries[key] = (time.monotonic() + ttl_s, value)
        self._entries.move_to_end(key)
        while len(self._entries) > self.max_entries:
            self._entries.popitem(last=False)

    def _task_for(
        self, key: Hashable, fetch: Callable[[], Awaitable[tuple[Any, float]]]
    ) -> asyncio.Task:
        """Return the in-flight fetch for ``key`` or start one (single-flight)."""
        loop = asyncio.get_running_loop()
        task = self._inflight.get(key)
        if task is not None and not task.done() and task.get_loop() is loop:
            return task
        task = loop.create_task(self._run_fetch(key, fetch))
        task.add_done_callback(_consume_background_result)
        self._inflight[key] = task
        return task

    async def _run_fetch(
        self, key: Hashable, fetch: Callable[[], Awaitable[tuple[Any, float]]]
    ) -> Any:
        try:
            value, ttl_s = await fetch()
            self.put(key, value, ttl_s)
            return value
        finally:
            if self._inflight.get(key) is asyncio.current_task():
                del self._inflight[key]

    async def get_or_fetch(
        self,
        key: Hashable,
        fetch: Callable[[], Awaitable[tuple[Any, float]]],
        wait_s: float | None = None,
    ) -> Any | None:
        """Return the cached value, or fetch it once for all concurrent callers.

        ``fetch`` returns (value, ttl_seconds). With ``wait_s`` the caller stops
        waiting after that many seconds (TimeoutError) while the fetch keeps
        running and still fills the cache.
        """
        cached = self.get(key)
        if cached is not None:
            return cached
        task = self._task_for(key, fetch)
        if wait_s is None:
            return await asyncio.shield(task)
        return await asyncio.wait_for(asyncio.shield(task), timeout=wait_s)


_forecast_cache = _TTLCache("forecast", FORECAST_CACHE_MAX_ENTRIES)
_geocode_cache = _TTLCache("geocode", GEOCODE_CACHE_MAX_ENTRIES)


def clear_caches() -> None:
    """Drop every cached provider and geocoding response (used by tests)."""
    _forecast_cache.clear()
    _geocode_cache.clear()


def _coord_key(lat: float, lon: float) -> tuple[float, float]:
    """Round coordinates to the cache grid so nearby requests share entries."""
    step = CACHE_COORD_STEP_DEG
    return (round(round(lat / step) * step, 4), round(round(lon / step) * step, 4))


def _describe_error(exc: BaseException) -> str:
    """Short error description that never includes a URL (keys can live in URLs)."""
    if isinstance(exc, httpx.HTTPStatusError):
        return f"HTTP {exc.response.status_code}"
    return type(exc).__name__


def _consume_background_result(task: asyncio.Future) -> None:
    """Retrieve a finished task's outcome so a failure nobody awaited is not lost."""
    if task.cancelled():
        return
    exc = task.exception()
    if exc is not None:
        logger.debug("Background provider task failed: %s", _describe_error(exc))


async def _await_optional(name: str, awaitable: Awaitable[Any]) -> Any | None:
    """Wait up to the optional-provider budget; None on timeout or failure.

    The task is shielded, so a slow provider keeps running in the background
    (and fills its own cache) instead of being cancelled.
    """
    task = asyncio.ensure_future(awaitable)
    task.add_done_callback(_consume_background_result)
    try:
        return await asyncio.wait_for(asyncio.shield(task), timeout=OPTIONAL_PROVIDER_BUDGET_S)
    except TimeoutError:
        logger.info("%s did not answer within %.1fs; continuing without it",
                    name, OPTIONAL_PROVIDER_BUDGET_S)
    except Exception as exc:
        logger.warning("%s failed: %s", name, _describe_error(exc))
    return None


async def _cached_optional(
    name: str, key: Hashable, fetch: Callable[[], Awaitable[tuple[Any, float]]]
) -> Any | None:
    """Cached optional provider bounded by the optional-provider budget."""
    try:
        return await _forecast_cache.get_or_fetch(key, fetch, wait_s=OPTIONAL_PROVIDER_BUDGET_S)
    except TimeoutError:
        logger.info("%s did not answer within %.1fs; continuing without it",
                    name, OPTIONAL_PROVIDER_BUDGET_S)
    except Exception as exc:
        logger.warning("%s failed: %s", name, _describe_error(exc))
    return None


def _resolve_met_user_agent(explicit: str | None) -> str:
    """MET Norway User-Agent: explicit value, else MET_NORWAY_USER_AGENT, else default.

    A leftover placeholder identifier is replaced by the default.
    """
    for candidate in (explicit, os.getenv("MET_NORWAY_USER_AGENT")):
        if candidate and _PLACEHOLDER_USER_AGENT_MARKER not in candidate:
            return candidate
    return DEFAULT_MET_USER_AGENT


def _ttl_from_expires(expires_header: str | None) -> float:
    """Seconds until an HTTP Expires date (clamped); the default when absent/invalid."""
    if not expires_header:
        return TTL_MET_DEFAULT_S
    try:
        expires_at = parsedate_to_datetime(expires_header)
    except (TypeError, ValueError):
        return TTL_MET_DEFAULT_S
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    seconds = (expires_at - datetime.now(timezone.utc)).total_seconds()
    return min(max(seconds, TTL_MET_MIN_S), TTL_MET_MAX_S)


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_HEADERS = {"User-Agent": "WeatherForMoto/1.0 (weatherformoto@bluemouse.cc)"}
# Nominatim's usage policy allows at most one request per second.
NOMINATIM_MIN_INTERVAL_S = 1.1
_nominatim_lock: asyncio.Lock | None = None
_nominatim_lock_loop: asyncio.AbstractEventLoop | None = None
_nominatim_last_call: float = 0.0


def _ascii_fold(s: str) -> str:
    """Lower-case and strip diacritics ('Răsmirești' → 'rasmiresti')."""
    return unicodedata.normalize("NFD", s.lower()).encode("ascii", "ignore").decode()


def _get_nominatim_lock() -> asyncio.Lock:
    """Lock (one per event loop) that serialises Nominatim calls."""
    global _nominatim_lock, _nominatim_lock_loop
    loop = asyncio.get_running_loop()
    if _nominatim_lock is None or _nominatim_lock_loop is not loop:
        _nominatim_lock = asyncio.Lock()
        _nominatim_lock_loop = loop
    return _nominatim_lock


async def _nominatim_search(
    client: httpx.AsyncClient, params: dict[str, Any]
) -> list[dict[str, Any]]:
    """Query Nominatim, keeping NOMINATIM_MIN_INTERVAL_S between requests.

    Raises RuntimeError on transport errors and non-200 answers (429 too), so
    an outage is reported as such instead of "location not found".
    """
    global _nominatim_last_call
    async with _get_nominatim_lock():
        wait_s = _nominatim_last_call + NOMINATIM_MIN_INTERVAL_S - time.monotonic()
        if wait_s > 0:
            await asyncio.sleep(wait_s)
        try:
            resp = await client.get(NOMINATIM_URL, params=params,
                                    headers=NOMINATIM_HEADERS, timeout=8)
        except httpx.HTTPError as exc:
            raise RuntimeError(f"Nominatim unreachable: {type(exc).__name__}") from exc
        finally:
            _nominatim_last_call = time.monotonic()
    if resp.status_code != 200:
        raise RuntimeError(f"Nominatim HTTP {resp.status_code}")
    try:
        data = resp.json()
    except ValueError as exc:
        raise RuntimeError("Nominatim returned invalid JSON") from exc
    return data if isinstance(data, list) else []


async def _geocode_wikidata(village: str, county: str, client: httpx.AsyncClient) -> dict[str, Any] | None:
    """Last-resort geocoder using Wikidata's fuzzy entity search.

    Handles Romanian village names where OSM spelling differs from what the
    user typed (e.g. 'razmiresti' → 'Răsmirești'). Wikidata search tolerates
    diacritics and minor spelling variants. Returns None when nothing matches
    and raises RuntimeError when Wikidata cannot be reached.
    """
    try:
        resp = await client.get(
            WIKIDATA_API,
            params={"action": "wbsearchentities", "search": f"{village} {county}",
                    "language": "ro", "format": "json", "limit": 10},
            timeout=10,
        )
        resp.raise_for_status()
        items = resp.json().get("search", [])
    except (httpx.HTTPError, ValueError, AttributeError) as exc:
        logger.warning("geocode: Wikidata search failed: %s", _describe_error(exc))
        raise RuntimeError("Wikidata geocoding unavailable") from exc

    county_folded = _ascii_fold(county)
    for item in items:
        if county_folded not in _ascii_fold(item.get("description", "")):
            continue
        qid = item.get("id")
        if not qid:
            continue
        try:
            ent_resp = await client.get(
                WIKIDATA_API,
                params={"action": "wbgetentities", "ids": qid,
                        "props": "claims|labels", "format": "json"},
                timeout=10,
            )
            ent_resp.raise_for_status()
            ent = ent_resp.json()["entities"][qid]
        except (httpx.HTTPError, ValueError, KeyError, TypeError) as exc:
            logger.warning("geocode: Wikidata entity lookup failed: %s", _describe_error(exc))
            raise RuntimeError("Wikidata geocoding unavailable") from exc
        try:
            val = ent["claims"]["P625"][0]["mainsnak"]["datavalue"]["value"]
            lat, lon = float(val["latitude"]), float(val["longitude"])
        except (KeyError, IndexError, TypeError, ValueError):
            continue  # entity without usable coordinates
        name = ent.get("labels", {}).get("ro", {}).get("value", village)
        return {"lat": lat, "lon": lon, "name": name, "country": "RO", "timezone": "auto"}
    return None


async def geocode_city(city: str, client: httpx.AsyncClient) -> dict[str, Any]:
    """Return {lat, lon, name, country} for a city name, cached for 24 hours.

    Raises ValueError when the place is not found and RuntimeError when the
    geocoding services are unreachable. Concurrent lookups of the same name
    share one upstream call.
    """
    key = ("geocode", unicodedata.normalize("NFC", city.strip().casefold()))

    async def fetch() -> tuple[dict[str, Any], float]:
        return await _geocode_uncached(city, client), TTL_GEOCODE_S

    return await _geocode_cache.get_or_fetch(key, fetch)


async def _geocode_uncached(city: str, client: httpx.AsyncClient) -> dict[str, Any]:
    """Resolve a city name without the cache.

    Strategy:
    1. Open-Meteo geocoding (fast, good for cities)
    2. Nominatim free-text + county filter for "village, county" queries
    3. Wikidata fallback for fuzzy village name matching (county queries only)
    4. Nominatim free-text, Romania only
    5. Nominatim free-text, worldwide
    """
    # Track the last upstream transport error so a genuine service outage maps to
    # a 502 (via RuntimeError) instead of masquerading as a 404 "city not found".
    last_error: Exception | None = None

    # 1. Try Open-Meteo first (GeoNames — fast, good for known cities).
    # Skip when query has a comma (user specified county hint) — Open-Meteo
    # ignores county context and will return the first alphabetical match,
    # which may be in the wrong county.
    if "," not in city:
        try:
            resp = await client.get(
                GEOCODING_OPENMETEO,
                params={"name": city, "count": 1, "language": "ro"},
                timeout=8,
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("results"):
                r = data["results"][0]
                return {
                    "lat": r["latitude"],
                    "lon": r["longitude"],
                    "name": r.get("name", city),
                    "country": r.get("country_code", ""),
                    "timezone": r.get("timezone", "auto"),
                }
        except Exception as exc:
            last_error = exc
            logger.warning("geocode: Open-Meteo lookup failed: %s", _describe_error(exc))

    # 2. Nominatim free-text with county filter (for "village, county" queries).
    # Structured params (city=, state=, county=) don't reliably match Romanian
    # OSM admin levels, so we fetch broadly with addressdetails and filter.
    if "," in city:
        parts = [p.strip() for p in city.split(",", 1)]
        village_part, county_part = parts[0], parts[1]
        try:
            rows = await _nominatim_search(client, {
                "q": f"{village_part} {county_part}", "countrycodes": "ro",
                "format": "json", "limit": 20, "addressdetails": 1,
            })
            county_folded = _ascii_fold(county_part)
            matched = [
                r for r in rows
                if county_folded in _ascii_fold(r.get("address", {}).get("county", ""))
                or county_folded in _ascii_fold(r.get("address", {}).get("state", ""))
            ]
            if matched:
                r = matched[0]
                display = r.get("display_name", city).split(",")[0].strip()
                return {"lat": float(r["lat"]), "lon": float(r["lon"]),
                        "name": display, "country": "RO", "timezone": "auto"}
        except (RuntimeError, KeyError, TypeError, ValueError) as exc:
            last_error = exc
            logger.warning("geocode: Nominatim county lookup failed: %s", exc)

        # 3. Wikidata fallback — handles spelling variants OSM can't match
        # (e.g. 'razmiresti' → 'Răsmirești', diacritics, z/s differences)
        try:
            wd = await _geocode_wikidata(village_part, county_part, client)
        except RuntimeError as exc:
            last_error = exc
            wd = None
        if wd:
            return wd
        if last_error is not None:
            raise RuntimeError("Geocoding service temporarily unavailable") from last_error
        raise ValueError(
            f"Locația '{village_part}' nu a fost găsită în județul '{county_part}'."
        )

    # 4. Nominatim free-text, Romania only
    try:
        data = await _nominatim_search(client, {
            "q": city, "countrycodes": "ro", "format": "json", "limit": 1, "addressdetails": 0,
        })
        if data:
            r = data[0]
            display = r.get("display_name", city).split(",")[0].strip()
            return {"lat": float(r["lat"]), "lon": float(r["lon"]),
                    "name": display, "country": "RO", "timezone": "auto"}
    except (RuntimeError, KeyError, TypeError, ValueError) as exc:
        last_error = exc
        logger.warning("geocode: Nominatim RO lookup failed: %s", exc)

    # 5. Nominatim free-text, worldwide
    try:
        data = await _nominatim_search(client, {
            "q": city, "format": "json", "limit": 1, "addressdetails": 0,
        })
        if data:
            r = data[0]
            display = r.get("display_name", city).split(",")[0].strip()
            country = r.get("display_name", "").split(",")[-1].strip()
            return {"lat": float(r["lat"]), "lon": float(r["lon"]),
                    "name": display, "country": country, "timezone": "auto"}
    except (RuntimeError, KeyError, TypeError, ValueError) as exc:
        last_error = exc
        logger.warning("geocode: Nominatim worldwide lookup failed: %s", exc)

    if last_error is not None:
        raise RuntimeError("Geocoding service temporarily unavailable") from last_error
    raise ValueError(f"Locația '{city}' nu a fost găsită.")


# ---------------------------------------------------------------------------
# Fetch from Open-Meteo
# ---------------------------------------------------------------------------

async def _fetch_openmeteo(
    lat: float, lon: float, client: httpx.AsyncClient, forecast_days: int = 7
) -> dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": (
            "temperature_2m,apparent_temperature,relative_humidity_2m,"
            "wind_speed_10m,wind_gusts_10m,wind_direction_10m,"
            "precipitation,weather_code,surface_pressure,pressure_msl,visibility,is_day"
        ),
        "hourly": (
            "temperature_2m,apparent_temperature,precipitation_probability,"
            "precipitation,weather_code,wind_speed_10m,wind_gusts_10m,wind_direction_10m,"
            "uv_index,relative_humidity_2m,surface_pressure,pressure_msl,dew_point_2m,"
            "cloud_cover,visibility,is_day"
        ),
        "daily": (
            "weather_code,temperature_2m_max,temperature_2m_min,"
            "apparent_temperature_max,apparent_temperature_min,"
            "precipitation_sum,wind_speed_10m_max,wind_gusts_10m_max,"
            "precipitation_probability_max,sunrise,sunset"
        ),
        "timezone": "auto",
        "forecast_days": min(max(int(forecast_days), 1), 16),
        "wind_speed_unit": "kmh",
    }
    # Open-Meteo is the primary source — every other part of the response
    # (hourly, daily, the current block) is built on it. A transient timeout
    # or network blip would otherwise fail the whole /weather request, so
    # retry once before giving up. Each attempt has a hard deadline so the
    # whole request stays inside WEATHER_TOTAL_BUDGET_S. Real API errors
    # (4xx / error body) are raised immediately since a retry would not help.
    last_exc: Exception | None = None
    for attempt in range(OPENMETEO_ATTEMPTS):
        try:
            resp = await asyncio.wait_for(
                client.get(OPENMETEO_BASE, params=params, timeout=OPENMETEO_ATTEMPT_TIMEOUT_S),
                timeout=OPENMETEO_ATTEMPT_TIMEOUT_S,
            )
            if resp.status_code >= 500:
                raise httpx.HTTPStatusError(
                    f"Open-Meteo {resp.status_code}", request=resp.request, response=resp
                )
            if not resp.is_success:
                reason: str = resp.text[:200]
                try:
                    reason = resp.json().get("reason", reason)
                except (ValueError, AttributeError):
                    pass  # body is not a JSON object; keep the raw text
                raise ValueError(f"Open-Meteo error {resp.status_code}: {reason}")
            data = resp.json()
            if data.get("error"):
                raise ValueError(f"Open-Meteo error: {data.get('reason', 'unknown error')}")
            return data
        except (httpx.TransportError, httpx.HTTPStatusError, TimeoutError) as exc:
            last_exc = exc
            logger.warning("Open-Meteo attempt %d/%d failed: %s",
                           attempt + 1, OPENMETEO_ATTEMPTS, _describe_error(exc))
            if attempt < OPENMETEO_ATTEMPTS - 1:
                await asyncio.sleep(OPENMETEO_RETRY_DELAY_S)
    raise ValueError(
        f"Open-Meteo unreachable after {OPENMETEO_ATTEMPTS} attempts: "
        f"{_describe_error(last_exc) if last_exc else 'unknown error'}"
    ) from last_exc


# ---------------------------------------------------------------------------
# Model ensemble (Open-Meteo, several national models)
# ---------------------------------------------------------------------------
# Open-Meteo's default answer is its "seamless" blend, already curated per
# region. Asking the same endpoint for the individual national models costs one
# more request and no API key, and gives two things the blend alone cannot: a
# mean that does not depend on a single centre, and the spread between the
# models, which is the honest measure of how sure the forecast is.

ENSEMBLE_MODELS: tuple[str, ...] = (
    "ecmwf_ifs",              # ECMWF, Reading (HRES, 9 km, open data)
    "icon_seamless",          # DWD, Offenbach
    "gfs_seamless",           # NOAA, College Park
    "meteofrance_seamless",   # Météo-France, Toulouse
    "ukmo_seamless",          # Met Office, Exeter
)

ENSEMBLE_VARIABLES: tuple[str, ...] = (
    "temperature_2m",
    "apparent_temperature",
    "precipitation",
    "wind_speed_10m",
    "wind_gusts_10m",
    "weather_code",
)

# Rain is skewed: one model with a 4 mm/h shower and four dry ones average to a
# drizzle nobody forecast. The median keeps the answer most models give.
ENSEMBLE_MEDIAN_VARIABLES = frozenset({"precipitation"})
# Weather codes are categories; they are voted on (see _consensus_code), never averaged.
ENSEMBLE_CATEGORICAL_VARIABLES = frozenset({"weather_code"})
# A model "gives rain" for an hour from this amount on (the same bar a rain code needs).
ENSEMBLE_WET_MM = CODE_ACTIVE_MIN_AMOUNT_MM

# The seamless blend keeps twice the weight of the raw model mean: it is a
# curated product, not just another member.
ENSEMBLE_SEAMLESS_WEIGHT = 2.0
ENSEMBLE_MODEL_WEIGHT = 1.0

# An hour needs at least this many models before its spread means anything.
ENSEMBLE_MIN_MODELS = 2

# Spread (max - min across the models) up to which an hour still counts as
# agreed. The values are what matters on a bike: 2 °C does not change what you
# wear, 0.5 mm/h does not change whether the road is wet, 10 km/h of gust does
# not change how the bike behaves.
ENSEMBLE_HIGH_SPREAD: dict[str, float] = {
    "temperature_2m": 2.0,
    "precipitation": 0.5,
    "wind_gusts_10m": 10.0,
}
ENSEMBLE_MEDIUM_SPREAD: dict[str, float] = {
    "temperature_2m": 4.0,
    "precipitation": 1.5,
    "wind_gusts_10m": 20.0,
}


async def _fetch_openmeteo_ensemble(
    lat: float, lon: float, client: httpx.AsyncClient, forecast_days: int = 7
) -> dict[str, Any] | None:
    """The same hours from every model in ENSEMBLE_MODELS; None on failure."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ",".join(ENSEMBLE_VARIABLES),
        "models": ",".join(ENSEMBLE_MODELS),
        "timezone": "auto",
        "forecast_days": min(max(int(forecast_days), 1), 16),
        "wind_speed_unit": "kmh",
    }
    try:
        resp = await client.get(OPENMETEO_BASE, params=params,
                                timeout=OPTIONAL_PROVIDER_HTTP_TIMEOUT_S)
        resp.raise_for_status()
        data = resp.json()
        if not isinstance(data, dict) or data.get("error"):
            reason = data.get("reason", "unknown error") if isinstance(data, dict) else "not an object"
            logger.warning("Open-Meteo ensemble error: %s", reason)
            return None
        return data
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Open-Meteo ensemble fetch failed: %s", _describe_error(exc))
        return None


def _ensemble_series(hourly: dict[str, Any], variable: str) -> list[list[Any]]:
    """One list per model: Open-Meteo suffixes the key with the model name."""
    series: list[list[Any]] = []
    for key, values in hourly.items():
        if isinstance(values, list) and (key == variable or key.startswith(f"{variable}_")):
            series.append(values)
    return series


def _spread_confidence(spreads: dict[str, float]) -> str:
    """"high", "medium" or "low" — the worst of the variables decides."""
    level = "high"
    for name, spread in spreads.items():
        if name not in ENSEMBLE_HIGH_SPREAD:
            continue
        if spread > ENSEMBLE_MEDIUM_SPREAD[name]:
            return "low"
        if spread > ENSEMBLE_HIGH_SPREAD[name]:
            level = "medium"
    return level


def _median(values: list[float]) -> float:
    ordered = sorted(values)
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2


def _ensemble_by_time(ens_data: dict | None) -> dict[str, dict[str, Any]]:
    """Per local hour: the central value of the models, how many answered, and how much they agree.

    "means" holds the mean for smooth variables and the median for rain (see
    ENSEMBLE_MEDIAN_VARIABLES). "codes" lists each model's weather code, and
    "wet_models" / "rain_models" count how many models give rain out of how
    many answered for rain.
    """
    if not isinstance(ens_data, dict):
        return {}
    hourly = ens_data.get("hourly")
    if not isinstance(hourly, dict):
        return {}
    times = hourly.get("time")
    if not isinstance(times, list):
        return {}

    series = {variable: _ensemble_series(hourly, variable) for variable in ENSEMBLE_VARIABLES}
    out: dict[str, dict[str, Any]] = {}
    for i, time_value in enumerate(times):
        means: dict[str, float] = {}
        spreads: dict[str, float] = {}
        codes: list[int] = []
        wet_models = rain_models = 0
        models = 0
        for variable, lists in series.items():
            values = [v for v in (_to_float(_safe(lst, i)) for lst in lists) if v is not None]
            if not values:
                continue
            models = max(models, len(values))
            if variable in ENSEMBLE_CATEGORICAL_VARIABLES:
                codes = [int(v) for v in values]
                continue
            if variable in ENSEMBLE_MEDIAN_VARIABLES:
                means[variable] = _median(values)
            else:
                means[variable] = sum(values) / len(values)
            spreads[variable] = max(values) - min(values)
            if variable == "precipitation":
                rain_models = len(values)
                wet_models = sum(1 for v in values if v >= ENSEMBLE_WET_MM)
        if models < ENSEMBLE_MIN_MODELS:
            continue
        out[str(time_value)] = {
            "means": means,
            "spreads": spreads,
            "codes": codes,
            "wet_models": wet_models,
            "rain_models": rain_models,
            "models": models,
            "confidence": _spread_confidence(spreads),
        }
    return out


def _consensus_code(seamless_code: int | None, model_codes: list[int]) -> int | None:
    """The weather code most of the models back.

    Every model votes with its code and Open-Meteo's own blend votes with the
    seamless weight; the votes are ranked by severity and the middle one wins.
    A thunderstorm only one model draws stays a shower, and a dry blend is
    outvoted when most national models give rain. With fewer than
    ENSEMBLE_MIN_MODELS codes the blend's code stands.
    """
    if len(model_codes) < ENSEMBLE_MIN_MODELS:
        return seamless_code
    votes = list(model_codes)
    if seamless_code is not None:
        votes += [seamless_code] * int(ENSEMBLE_SEAMLESS_WEIGHT)
    votes.sort(key=lambda c: _CODE_SEVERITY.get(c, -1))
    return votes[len(votes) // 2]


def _blend_with_model_mean(seamless: float | None, model_mean: float | None) -> float | None:
    """Open-Meteo's own value, pulled toward the mean of the national models."""
    if model_mean is None:
        return seamless
    if seamless is None:
        return round(model_mean, 2)
    return _weighted_avg([seamless, model_mean], [ENSEMBLE_SEAMLESS_WEIGHT, ENSEMBLE_MODEL_WEIGHT])


# ---------------------------------------------------------------------------
# Fetch from OpenWeatherMap
# ---------------------------------------------------------------------------

async def _fetch_owm_json(
    path: str, params: dict[str, Any], client: httpx.AsyncClient, name: str
) -> dict[str, Any] | None:
    """GET an OpenWeatherMap endpoint; None on failure (logged without the URL,
    since the API key travels in the query string)."""
    try:
        resp = await client.get(f"{OWM_BASE}{path}", params=params,
                                timeout=OPTIONAL_PROVIDER_HTTP_TIMEOUT_S)
        resp.raise_for_status()
        return resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("%s fetch failed: %s", name, _describe_error(exc))
        return None


async def _fetch_owm_current(
    lat: float, lon: float, api_key: str, client: httpx.AsyncClient
) -> dict[str, Any] | None:
    if not api_key:
        return None
    return await _fetch_owm_json(
        "/data/2.5/weather",
        {"lat": lat, "lon": lon, "appid": api_key, "units": "metric", "lang": "ro"},
        client, "OWM current",
    )


async def _fetch_owm_forecast(
    lat: float, lon: float, api_key: str, client: httpx.AsyncClient
) -> dict[str, Any] | None:
    if not api_key:
        return None
    return await _fetch_owm_json(
        "/data/2.5/forecast",
        {"lat": lat, "lon": lon, "appid": api_key, "units": "metric", "lang": "ro", "cnt": 40},
        client, "OWM forecast",
    )


async def _fetch_owm_air(
    lat: float, lon: float, api_key: str, client: httpx.AsyncClient
) -> dict[str, Any] | None:
    if not api_key:
        return None
    return await _fetch_owm_json(
        "/data/2.5/air_pollution", {"lat": lat, "lon": lon, "appid": api_key},
        client, "OWM air pollution",
    )


async def _fetch_openmeteo_air_quality(
    lat: float, lon: float, client: httpx.AsyncClient
) -> dict[str, Any] | None:
    try:
        resp = await client.get(
            OPENMETEO_AIR_BASE,
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": (
                    "pm10,pm2_5,ozone,european_aqi,us_aqi,"
                    "alder_pollen,birch_pollen,grass_pollen,mugwort_pollen,ragweed_pollen"
                ),
                "timezone": "auto",
                "forecast_days": 1,
            },
            timeout=OPTIONAL_PROVIDER_HTTP_TIMEOUT_S,
        )
        if not resp.is_success:
            logger.warning("Open-Meteo air quality fetch failed: HTTP %s", resp.status_code)
            return None
        return resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Open-Meteo air quality fetch failed: %s", _describe_error(exc))
        return None


async def _fetch_met_norway(
    lat: float, lon: float, client: httpx.AsyncClient, user_agent: str
) -> tuple[dict | None, float]:
    """Fetch MET Norway LocationForecast 2.0 (free, no key, Europe-optimised).

    Returns (data, cache_ttl_seconds); the TTL honours the Expires header as
    MET's terms of service ask.
    """
    try:
        resp = await client.get(
            MET_NO_BASE,
            params={"lat": round(lat, 4), "lon": round(lon, 4)},
            headers={"User-Agent": user_agent},
            timeout=OPTIONAL_PROVIDER_HTTP_TIMEOUT_S,
        )
        if not resp.is_success:
            logger.warning("MET Norway fetch failed: HTTP %s", resp.status_code)
            return None, 0.0
        return resp.json(), _ttl_from_expires(resp.headers.get("Expires"))
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("MET Norway fetch failed: %s", _describe_error(exc))
        return None, 0.0


async def _fetch_pirate_weather(
    lat: float, lon: float, api_key: str, client: httpx.AsyncClient
) -> dict | None:
    """Fetch Pirate Weather forecast (Dark Sky-compatible, requires API key)."""
    if not api_key:
        return None
    try:
        resp = await client.get(
            f"{PIRATE_WEATHER_BASE}/{api_key}/{lat:.4f},{lon:.4f}",
            params={"units": "si", "exclude": "minutely,alerts,flags"},
            timeout=OPTIONAL_PROVIDER_HTTP_TIMEOUT_S,
        )
        if not resp.is_success:
            logger.warning("Pirate Weather fetch failed: HTTP %s", resp.status_code)
            return None
        return resp.json()
    except (httpx.HTTPError, ValueError) as exc:
        # Never log the URL: the API key is part of the path.
        logger.warning("Pirate Weather fetch failed: %s", _describe_error(exc))
        return None


# Cache for WeatherXM responses: key=(lat2, lon2), value=(timestamp, data)
# Rounds coordinates to 2 decimal places (~1 km grid) to merge nearby requests.
_wxm_cache: dict[tuple, tuple] = {}
_WXM_CACHE_TTL = 3600  # 1 hour — reduces daily API quota consumption
# A cached observation that has become too old to use (see
# WXM_MAX_OBSERVATION_AGE_S) is refetched, but never more often than this, so
# a station that stopped reporting cannot burn the daily quota.
_WXM_MIN_REFETCH_S = 600

# Global rate-limit backoff for external APIs (WeatherXM, Netatmo).
# Persisted in the Turso DB (app_state table) so it survives Cloud Run cold
# starts and is shared across instances — a per-instance /tmp file would not.
# Note: stored values are wall-clock Unix timestamps (time.time()), NOT
# monotonic ones, since monotonic clocks are not comparable across processes.
_WXM_BACKOFF_KEY = "wxm_backoff_until"
_WXM_BACKOFF_SECS = 86400  # 24 hours — WeatherXM rate limit is daily
_wxm_backoff_until: float = 0.0       # cached wall-clock expiry for this process
_wxm_backoff_loaded: bool = False     # whether persisted state was read this process


def _backoff_read(key: str) -> float:
    """Read a persisted backoff expiry (Unix timestamp) from Turso app_state."""
    try:
        from auth_alerts import get_app_state
        val = get_app_state(key)
        return float(val) if val else 0.0
    except Exception:
        return 0.0


def _backoff_write(key: str, until: float) -> None:
    """Persist a backoff expiry (Unix timestamp) to Turso app_state."""
    try:
        from auth_alerts import set_app_state
        set_app_state(key, str(until))
    except Exception:
        pass


async def _fetch_weatherxm(
    lat: float, lon: float, api_key: str, client: httpx.AsyncClient,
    radius_m: int = 20_000,
) -> dict | None:
    """Fetch nearest WeatherXM PRO station and its latest observation.

    Results are cached for up to an hour per ~1km grid cell to avoid 429s
    when multiple parallel requests hit the same area. The observation's
    own age is checked at use time: once it is too old to count as current
    it is refetched (at most every _WXM_MIN_REFETCH_S seconds).
    """
    if not api_key:
        return None

    import time
    global _wxm_backoff_until, _wxm_backoff_loaded
    cache_key = (round(lat, 2), round(lon, 2))
    now = time.monotonic()
    if cache_key in _wxm_cache:
        ts, cached = _wxm_cache[cache_key]
        entry_age = now - ts
        if entry_age < _WXM_MIN_REFETCH_S or (
            entry_age < _WXM_CACHE_TTL and not _wxm_observation_is_stale(cached)
        ):
            return cached

    # Respect global rate-limit backoff. Load the persisted expiry from Turso
    # once per process so a cold-started instance inherits an active backoff
    # instead of immediately re-hitting the (likely still rate-limited) API.
    if not _wxm_backoff_loaded:
        _wxm_backoff_until = await asyncio.to_thread(_backoff_read, _WXM_BACKOFF_KEY)
        _wxm_backoff_loaded = True
    if time.time() < _wxm_backoff_until:
        return None

    import logging
    _wxm_log = logging.getLogger("weatherformoto")
    try:
        headers = {"X-API-KEY": api_key}
        resp = await client.get(
            f"{WEATHERXM_PRO_BASE}/stations/near",
            params={"lat": round(lat, 4), "lon": round(lon, 4), "radius": radius_m},
            headers=headers,
            timeout=10,
        )
        _wxm_log.info("WeatherXM stations/near status=%s body=%s", resp.status_code, resp.text[:300])
        if resp.status_code == 429:
            _wxm_backoff_until = time.time() + _WXM_BACKOFF_SECS
            await asyncio.to_thread(_backoff_write, _WXM_BACKOFF_KEY, _wxm_backoff_until)
            _wxm_log.info("WeatherXM rate-limited — backing off for %dh", _WXM_BACKOFF_SECS // 3600)
            _wxm_cache[cache_key] = (now, None)
            return None
        if not resp.is_success:
            _wxm_cache[cache_key] = (now, None)
            return None
        payload = resp.json()
        stations = payload.get("stations", payload) if isinstance(payload, dict) else payload
        active = [s for s in stations if s.get("lastDayQod", 0) > 0]
        _wxm_log.info("WeatherXM stations total=%d active=%d", len(stations), len(active))
        if not active:
            _wxm_cache[cache_key] = (now, None)
            return None
        station_id = active[0].get("id")
        if not station_id:
            _wxm_cache[cache_key] = (now, None)
            return None
        obs_resp = await client.get(
            f"{WEATHERXM_PRO_BASE}/stations/{station_id}/latest",
            headers=headers,
            timeout=10,
        )
        _wxm_log.info("WeatherXM latest status=%s body=%s", obs_resp.status_code, obs_resp.text[:300])
        if obs_resp.status_code == 429:
            _wxm_backoff_until = time.time() + _WXM_BACKOFF_SECS
            await asyncio.to_thread(_backoff_write, _WXM_BACKOFF_KEY, _wxm_backoff_until)
            _wxm_log.info("WeatherXM rate-limited — backing off for %dh", _WXM_BACKOFF_SECS // 3600)
            _wxm_cache[cache_key] = (now, None)
            return None
        if not obs_resp.is_success:
            _wxm_cache[cache_key] = (now, None)
            return None
        result = obs_resp.json()
        _wxm_cache[cache_key] = (now, result)
        return result
    except Exception as exc:
        _wxm_log.warning("WeatherXM exception: %s", exc)
        return None


_WXM_ICON_TO_WMO: dict[str, int] = {
    "clear-day": 0, "clear-night": 0,
    "partly-cloudy-day": 2, "partly-cloudy-night": 2,
    "cloudy": 3, "wind": 3, "fog": 45,
    "rain": 63, "drizzle": 51, "sleet": 66, "snow": 73,
    "thunderstorm": 95, "hail": 96,
}

# A station reading older than this is not "current" conditions any more.
WXM_MAX_OBSERVATION_AGE_S = 30 * 60


def _to_float(value: Any) -> float | None:
    """Convert a sensor value to float, keeping missing / invalid values as None."""
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mps_to_kmh(value: Any) -> float | None:
    """m/s → km/h; a missing value stays None (never 0)."""
    speed = _to_float(value)
    return speed * 3.6 if speed is not None else None


def _parse_observation_time(value: Any) -> datetime | None:
    """Parse an ISO-8601 string or Unix seconds into an aware UTC datetime."""
    if value is None:
        return None
    try:
        if isinstance(value, (int, float)):
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError, OverflowError, OSError):
        return None
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=timezone.utc)


def _wxm_observation_is_stale(wxm_data: Any, now: datetime | None = None) -> bool:
    """True when a WeatherXM payload's observation is too old to use (or undated).

    A cached "no station" result (None) is not stale: it keeps its normal
    cache lifetime.
    """
    if not isinstance(wxm_data, dict):
        return False
    obs = wxm_data.get("observation", wxm_data)
    if not isinstance(obs, dict):
        return True
    observed_at = _parse_observation_time(obs.get("timestamp") or obs.get("ts"))
    if observed_at is None:
        return True
    age_s = ((now or datetime.now(timezone.utc)) - observed_at).total_seconds()
    return age_s > WXM_MAX_OBSERVATION_AGE_S


def _normalize_wxm_current(wxm_data: dict | None, now: datetime | None = None) -> dict | None:
    """Normalise a WeatherXM PRO latest-observation response.

    Missing sensor values stay None, so every field falls back to the model
    blend on its own instead of a 0 overriding all models. Observations older
    than WXM_MAX_OBSERVATION_AGE_S, or without a timestamp, are rejected.
    """
    if not isinstance(wxm_data, dict) or not wxm_data:
        return None
    # Response is wrapped: {"observation": {...}, "health": {...}, "location": {...}}
    obs = wxm_data.get("observation", wxm_data)
    if not isinstance(obs, dict):
        return None
    observed_at = _parse_observation_time(obs.get("timestamp") or obs.get("ts"))
    if observed_at is None:
        logger.info("WeatherXM observation without a usable timestamp ignored")
        return None
    age_s = ((now or datetime.now(timezone.utc)) - observed_at).total_seconds()
    if age_s > WXM_MAX_OBSERVATION_AGE_S:
        logger.info("WeatherXM observation is %.0f min old; ignored", age_s / 60)
        return None
    icon = obs.get("icon")
    return {
        "temp": _to_float(obs.get("temperature")),
        "feels_like": _to_float(obs.get("feels_like")),
        "humidity": _to_float(obs.get("humidity")),
        "wind_speed_kmh": _mps_to_kmh(obs.get("wind_speed")),
        "wind_gusts_kmh": _mps_to_kmh(obs.get("wind_gust")),
        "wind_dir": _to_float(obs.get("wind_direction")),
        "pressure": _to_float(obs.get("pressure")),
        "precipitation": _to_float(obs.get("precipitation_rate")),  # mm/h
        "wmo_code": _WXM_ICON_TO_WMO.get(icon) if icon else None,
        "observed_at": observed_at.isoformat(),
    }


# ---------------------------------------------------------------------------
# Netatmo — public personal-weather-station network (physical stations)
# ---------------------------------------------------------------------------
# Cache for Netatmo getpublicdata responses: key=(lat2, lon2) ~1km grid.
_netatmo_cache: dict[tuple, tuple] = {}
_NETATMO_CACHE_TTL = 3600  # 1 hour

# Rate-limit backoff (Netatmo allows ~500 req/hour — short window, so 1h backoff).
_NETATMO_BACKOFF_KEY = "netatmo_backoff_until"
_NETATMO_BACKOFF_SECS = 3600
_netatmo_backoff_until: float = 0.0
_netatmo_backoff_loaded: bool = False

# OAuth access token cache (Netatmo access tokens live ~3h). The refresh token
# is long-lived; the persisted copy in app_state covers the case where Netatmo
# ever rotates it (it currently returns the same one).
_netatmo_token: str | None = None
_netatmo_token_expiry: float = 0.0  # wall-clock
_NETATMO_REFRESH_KEY = "netatmo_refresh_token"

# Reject station readings older than this (seconds) — stale data is not "current".
_NETATMO_MAX_AGE = 3600


async def _netatmo_access_token(
    client_id: str, client_secret: str, refresh_token: str, client: httpx.AsyncClient,
) -> str | None:
    """Return a valid Netatmo OAuth access token, refreshing it when expired."""
    import time
    global _netatmo_token, _netatmo_token_expiry
    if _netatmo_token and time.time() < _netatmo_token_expiry - 120:
        return _netatmo_token

    import logging
    _log = logging.getLogger("weatherformoto")
    # Prefer a persisted refresh token (handles rotation); fall back to env seed.
    persisted = await asyncio.to_thread(_backoff_read_str, _NETATMO_REFRESH_KEY)
    rt = persisted or refresh_token
    try:
        resp = await client.post(
            NETATMO_TOKEN_URL,
            data={
                "grant_type": "refresh_token",
                "refresh_token": rt,
                "client_id": client_id,
                "client_secret": client_secret,
            },
            timeout=10,
        )
        if not resp.is_success:
            _log.warning("Netatmo token refresh status=%s body=%s", resp.status_code, resp.text[:200])
            return None
        data = resp.json()
        _netatmo_token = data.get("access_token")
        _netatmo_token_expiry = time.time() + float(data.get("expires_in", 10800))
        new_rt = data.get("refresh_token")
        if new_rt and new_rt != rt:
            await asyncio.to_thread(_backoff_write, _NETATMO_REFRESH_KEY, new_rt)
        return _netatmo_token
    except Exception as exc:
        _log.warning("Netatmo token exception: %s", exc)
        return None


def _backoff_read_str(key: str) -> str | None:
    """Read a raw string value from Turso app_state (for non-numeric state)."""
    try:
        from auth_alerts import get_app_state
        return get_app_state(key)
    except Exception:
        return None


async def _fetch_netatmo(
    lat: float, lon: float, client_id: str, client_secret: str, refresh_token: str,
    client: httpx.AsyncClient, radius_deg: float = 0.06,
) -> dict | None:
    """Fetch public Netatmo weather stations around (lat, lon).

    Returns {"stations": [...], "ref_lat": lat, "ref_lon": lon} or None.
    Cached per ~1km grid cell for an hour; backs off on HTTP 429.
    """
    if not (client_id and client_secret and refresh_token):
        return None

    import time
    global _netatmo_backoff_until, _netatmo_backoff_loaded
    cache_key = (round(lat, 2), round(lon, 2))
    mono = time.monotonic()
    if cache_key in _netatmo_cache:
        ts, cached = _netatmo_cache[cache_key]
        if mono - ts < _NETATMO_CACHE_TTL:
            return cached

    if not _netatmo_backoff_loaded:
        _netatmo_backoff_until = await asyncio.to_thread(_backoff_read, _NETATMO_BACKOFF_KEY)
        _netatmo_backoff_loaded = True
    if time.time() < _netatmo_backoff_until:
        return None

    import logging
    _log = logging.getLogger("weatherformoto")
    try:
        token = await _netatmo_access_token(client_id, client_secret, refresh_token, client)
        if not token:
            _netatmo_cache[cache_key] = (mono, None)
            return None
        resp = await client.get(
            NETATMO_PUBLICDATA_URL,
            params={
                "lat_ne": round(lat + radius_deg, 4),
                "lon_ne": round(lon + radius_deg, 4),
                "lat_sw": round(lat - radius_deg, 4),
                "lon_sw": round(lon - radius_deg, 4),
                "filter": "true",
            },
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        if resp.status_code == 429:
            _netatmo_backoff_until = time.time() + _NETATMO_BACKOFF_SECS
            await asyncio.to_thread(_backoff_write, _NETATMO_BACKOFF_KEY, _netatmo_backoff_until)
            _log.info("Netatmo rate-limited — backing off for %dm", _NETATMO_BACKOFF_SECS // 60)
            _netatmo_cache[cache_key] = (mono, None)
            return None
        if not resp.is_success:
            _log.warning("Netatmo getpublicdata status=%s body=%s", resp.status_code, resp.text[:200])
            _netatmo_cache[cache_key] = (mono, None)
            return None
        body = resp.json().get("body", [])
        _log.info("Netatmo getpublicdata stations=%d", len(body))
        result = {"stations": body, "ref_lat": lat, "ref_lon": lon} if body else None
        _netatmo_cache[cache_key] = (mono, result)
        return result
    except Exception as exc:
        _log.warning("Netatmo exception: %s", exc)
        return None


def _normalize_netatmo_current(payload: dict | None) -> dict | None:
    """Normalise Netatmo public-station data to the common current-conditions dict.

    Each Netatmo station carries separate modules (temp/humidity, pressure, rain,
    wind) and most stations lack rain/wind add-ons. For every measurement we keep
    the value from the *closest* station that reports it fresh — so temperature
    may come from one station and wind from another, both within the search box.
    """
    if not payload or not payload.get("stations"):
        return None
    import time
    ref_lat = payload["ref_lat"]
    ref_lon = payload["ref_lon"]
    now = time.time()

    # field -> (distance_km, value); keep the nearest fresh reading per field.
    best: dict[str, tuple[float, float]] = {}

    def consider(field: str, dist: float, value) -> None:
        if value is None:
            return
        if field not in best or dist < best[field][0]:
            best[field] = (dist, value)

    for st in payload["stations"]:
        loc = (st.get("place") or {}).get("location")
        if not loc or len(loc) != 2:
            continue
        dist = _haversine_km(ref_lat, ref_lon, loc[1], loc[0])
        for module in (st.get("measures") or {}).values():
            # temperature / humidity / pressure modules: timestamp-keyed `res`
            if "res" in module and "type" in module:
                res = module.get("res") or {}
                if not res:
                    continue
                latest_ts = max(res.keys(), key=lambda k: int(k))
                if now - int(latest_ts) > _NETATMO_MAX_AGE:
                    continue
                for typ, val in zip(module["type"], res[latest_ts]):
                    if typ == "temperature":
                        consider("temp", dist, val)
                    elif typ == "humidity":
                        consider("humidity", dist, val)
                    elif typ == "pressure":
                        consider("pressure", dist, val)
            # rain module
            if "rain_60min" in module:
                if now - module.get("rain_timeutc", 0) <= _NETATMO_MAX_AGE:
                    consider("rain", dist, module.get("rain_60min"))
            # wind module (wind_strength / gust_strength already in km/h)
            if "wind_strength" in module:
                if now - module.get("wind_timeutc", 0) <= _NETATMO_MAX_AGE:
                    consider("wind", dist, module.get("wind_strength"))
                    consider("gust", dist, module.get("gust_strength"))
                    consider("wind_dir", dist, module.get("wind_angle"))

    if "temp" not in best:
        return None  # without a temperature reading the station data is not useful

    def value(field: str):
        return best[field][1] if field in best else None

    return {
        "temp": value("temp"),
        "feels_like": None,            # Netatmo public data has no feels-like
        "humidity": value("humidity"),
        "wind_speed_kmh": value("wind"),
        "wind_gusts_kmh": value("gust"),
        "wind_dir": value("wind_dir"),
        "pressure": value("pressure"),
        "precipitation": value("rain"),
        "wmo_code": None,              # stations report no weather condition/icon
    }


def _normalize_met_current(met_data: dict | None) -> dict | None:
    """Extract current conditions from MET Norway timeseries."""
    if not met_data:
        return None
    try:
        ts = met_data["properties"]["timeseries"]
        if not ts:
            return None
        first = ts[0]
        inst = first["data"]["instant"]["details"]
        next_1h = first["data"].get("next_1_hours")
        next_6h = first["data"].get("next_6_hours")
        prec_block = next_1h or next_6h or {}
        symbol = prec_block.get("summary", {}).get("symbol_code")
        prec = _to_float(prec_block.get("details", {}).get("precipitation_amount"))
        if prec is not None and not next_1h and next_6h:
            prec = prec / 6  # a 6-hour total, turned into an hourly rate
        return {
            "temp": inst.get("air_temperature"),
            "humidity": inst.get("relative_humidity"),
            "wind_speed_kmh": _mps_to_kmh(inst.get("wind_speed")),
            # Only MET's gust field is a gust; sustained wind never stands in for it.
            "wind_gusts_kmh": _mps_to_kmh(inst.get("wind_speed_of_gust")),
            "wind_dir": inst.get("wind_from_direction"),
            "pressure": inst.get("air_pressure_at_sea_level"),
            "precipitation": prec,
            "wmo_code": _met_symbol_to_wmo(symbol),
        }
    except (KeyError, IndexError, TypeError):
        return None


def _normalize_pw_current(pw_data: dict | None) -> dict | None:
    """Extract current conditions from Pirate Weather response (units=si)."""
    if not isinstance(pw_data, dict):
        return None
    cur = pw_data.get("currently", {})
    if not cur:
        return None
    hum = cur.get("humidity")   # 0–1 fraction
    return {
        "temp": cur.get("temperature"),
        "feels_like": cur.get("apparentTemperature"),
        "humidity": (hum * 100) if hum is not None else None,
        "wind_speed_kmh": _mps_to_kmh(cur.get("windSpeed")),   # m/s with units=si
        "wind_gusts_kmh": _mps_to_kmh(cur.get("windGust")),
        "wind_dir": cur.get("windBearing"),
        "pressure": cur.get("pressure"),
        "precipitation": _to_float(cur.get("precipIntensity")),
        "wmo_code": _pw_icon_to_wmo(cur.get("icon")),
        "visibility_km": cur.get("visibility"),
    }


# A provider's day is only merged when it covers at least this many hours of
# the local day; partial days (today, the forecast edge) would skew max/min/sum.
MIN_DAY_COVERAGE_HOURS = 18


def _utc_to_local(utc_value: Any, utc_offset_seconds: int) -> datetime | None:
    """Shift a UTC timestamp (ISO string or Unix seconds) to local wall-clock time."""
    parsed = _parse_observation_time(utc_value)
    if parsed is None:
        return None
    return (parsed + timedelta(seconds=utc_offset_seconds)).replace(tzinfo=None)


def _aggregate_met_daily(
    met_data: dict | None, utc_offset_seconds: int = 0
) -> dict[str, dict] | None:
    """Aggregate MET Norway timeseries into per-LOCAL-date summaries.

    MET times are UTC; they are shifted by the location's UTC offset so the
    days line up with Open-Meteo's local daily rows. Days covering fewer than
    MIN_DAY_COVERAGE_HOURS are left out. Gusts come only from
    wind_speed_of_gust, and the day's code is the most severe one.
    """
    if not met_data:
        return None
    try:
        ts = met_data["properties"]["timeseries"]
    except (KeyError, TypeError):
        return None
    by_date: dict[str, list] = {}
    for entry in ts:
        local = _utc_to_local(entry.get("time"), utc_offset_seconds)
        if local is not None:
            by_date.setdefault(local.strftime("%Y-%m-%d"), []).append(entry)
    result: dict[str, dict] = {}
    for date, entries in by_date.items():
        temps, winds, gusts, precs, codes = [], [], [], [], []
        covered_hours = 0
        for entry in entries:
            data = entry.get("data") or {}
            inst = (data.get("instant") or {}).get("details") or {}
            if (t := inst.get("air_temperature")) is not None:
                temps.append(t)
            if (w := inst.get("wind_speed")) is not None:
                winds.append(w * 3.6)
            if (g := inst.get("wind_speed_of_gust")) is not None:
                gusts.append(g * 3.6)
            for blk_key, block_hours in (("next_1_hours", 1), ("next_6_hours", 6)):
                if blk := data.get(blk_key):
                    covered_hours += block_hours
                    amount = (blk.get("details") or {}).get("precipitation_amount")
                    if amount is not None:
                        precs.append(amount)
                    code = _met_symbol_to_wmo((blk.get("summary") or {}).get("symbol_code"))
                    if code is not None:
                        codes.append(code)
                    break
        if covered_hours < MIN_DAY_COVERAGE_HOURS:
            continue
        result[date] = {
            "t_max": max(temps) if temps else None,
            "t_min": min(temps) if temps else None,
            "wind_max_kmh": max(winds) if winds else None,
            "gust_max_kmh": max(gusts) if gusts else None,
            "precipitation_sum": sum(precs) if precs else None,
            "wmo_code": _most_severe_code(codes),
            "coverage_hours": min(covered_hours, 24),
        }
    return result


# ---------------------------------------------------------------------------
# Normalise & merge
# ---------------------------------------------------------------------------

def _local_now_iso(om_data: dict) -> str:
    """Current wall-clock time at the location as "YYYY-MM-DDTHH:MM"."""
    offset = int(om_data.get("utc_offset_seconds") or 0)
    local = datetime.now(timezone.utc) + timedelta(seconds=offset)
    return local.strftime("%Y-%m-%dT%H:%M")


def _current_hour_index(om_data: dict, hourly_times: list[str]) -> int | None:
    """Index of the hourly slot that CONTAINS the current local time.

    The slot stamped 22:00 is the hour you are living between 22:00 and 22:59,
    so the comparison is on the hour (first 13 characters), never on the
    minute: at 22:15 a minute comparison would skip 22:00 and describe 23:00
    instead. The frontend (currentHourIndex in app/src/lib/format.ts) uses the
    same rule, and the two must agree or the gauge and the timeline contradict
    each other. Falls back to the first slot after now when the current hour is
    missing from the series.
    """
    if not hourly_times:
        return None
    now_local = (om_data.get("current") or {}).get("time") or _local_now_iso(om_data)
    now_hour = now_local[:13]
    for index, slot in enumerate(hourly_times):
        if slot[:13] == now_hour:
            return index
    for index, slot in enumerate(hourly_times):
        if slot[:13] > now_hour:
            return index
    return None


# What the observed block replaces in the hour it belongs to. The amount and
# the probability travel with the score, so the bar's tooltip explains the
# number it shows instead of contradicting it.
_CURRENT_HOUR_FIELDS: tuple[str, ...] = (
    "moto_score", "moto_label", "precipitation_mm", "rain_intensity",
    "precipitation_probability", "weather_code", "icon", "description",
)


def _sync_current_hour(om_data: dict, hourly: list[dict], current: dict) -> None:
    """Give the hour being lived the measured conditions, in place.

    The gauge scores what the sources report right now; the hourly series is
    the models' average for each hour. For the hour you are in those two can
    disagree — a bar reading 90 above a gauge reading 59 — and the screen then
    contradicts itself. The measurement wins for that one hour; every hour
    ahead stays forecast.
    """
    index = _current_hour_index(om_data, [str(h.get("time", "")) for h in hourly])
    if index is None:
        return
    hour = hourly[index]
    for field in _CURRENT_HOUR_FIELDS:
        value = current.get(field)
        if value is not None:
            hour[field] = value


# ---------------------------------------------------------------------------
# Measured correction carried into the next hours
# ---------------------------------------------------------------------------
# When a station measures the air right now, the gap between it and the
# forecast for this hour is mostly the model's error at this spot (a valley
# fog, a city heat island, a sea breeze), and that error does not vanish at the
# next full hour. It is carried forward and faded out linearly: full weight on
# the hour being lived, nothing left after BIAS_FADE_HOURS.

BIAS_FADE_HOURS = 6

# (field in the current block, field in an hourly item, largest correction).
# A gap bigger than the limit is more likely a bad station than a model error.
BIAS_FIELDS: tuple[tuple[str, str, float], ...] = (
    ("temperature", "temperature", 6.0),
    ("feels_like", "feels_like", 6.0),
    ("humidity", "relative_humidity", 25.0),
    ("wind_speed_kmh", "wind_speed_kmh", 15.0),
    ("wind_gusts_kmh", "wind_gusts_kmh", 20.0),
)

# Which station field backs each current field (feels-like follows the temperature).
_BIAS_STATION_KEYS: dict[str, str] = {
    "temperature": "temp",
    "feels_like": "temp",
    "humidity": "humidity",
    "wind_speed_kmh": "wind_speed_kmh",
    "wind_gusts_kmh": "wind_gusts_kmh",
}


def _measured_fields(*stations: dict | None) -> set[str]:
    """Current-block fields that at least one station actually measured."""
    return {
        field for field, key in _BIAS_STATION_KEYS.items()
        if any(station and station.get(key) is not None for station in stations)
    }


def _rescore_hour(hour: dict[str, Any]) -> None:
    """Recompute an hourly item's road temperature, frost risk and score, in place."""
    road_temp = _road_surface_temp(hour.get("temperature"), hour.get("relative_humidity"),
                                   hour.get("weather_code"), hour.get("precipitation_mm"),
                                   is_day=hour.get("is_day"))
    dew_point = _first_not_none(hour.get("dew_point_2m"),
                                _dew_point_c(hour.get("temperature"), hour.get("relative_humidity")))
    frost_risk = _frost_risk(hour.get("temperature"), road_temp, dew_point,
                             hour.get("precipitation_mm"), hour.get("weather_code"))
    score = _moto_score(hour.get("feels_like"), hour.get("wind_gusts_kmh"), hour.get("precipitation_mm"),
                        hour.get("weather_code"), hour.get("precipitation_probability"),
                        visibility_m=hour.get("visibility"), frost_risk=frost_risk)
    hour.update({
        "road_surface_temp": road_temp,
        "frost_risk": frost_risk,
        "moto_score": score,
        "moto_label": _moto_label(score),
    })


def _carry_measured_bias(om_data: dict, hourly: list[dict], current: dict, measured: set[str]) -> None:
    """Shift the next hours by the measured-minus-forecast gap of this hour, in place."""
    index = _current_hour_index(om_data, [str(h.get("time", "")) for h in hourly])
    if index is None or not measured:
        return
    gaps: dict[str, float] = {}
    for current_field, hour_field, limit in BIAS_FIELDS:
        observed = current.get(current_field)
        forecast = hourly[index].get(hour_field)
        if current_field in measured and observed is not None and forecast is not None:
            gaps[hour_field] = max(-limit, min(limit, float(observed) - float(forecast)))
    if not gaps:
        return
    for offset in range(BIAS_FADE_HOURS):
        if index + offset >= len(hourly):
            break
        weight = 1 - offset / BIAS_FADE_HOURS
        hour = hourly[index + offset]
        for field, gap in gaps.items():
            value = hour.get(field)
            if value is None:
                continue
            shifted = float(value) + gap * weight
            if field == "relative_humidity":
                shifted = max(0.0, min(100.0, shifted))
            elif field in ("wind_speed_kmh", "wind_gusts_kmh"):
                shifted = max(0.0, shifted)
            hour[field] = round(shifted, 2)
        _rescore_hour(hour)


# ---------------------------------------------------------------------------
# Station altitude
# ---------------------------------------------------------------------------
# An official station up to 40 km away can sit hundreds of metres above or
# below the rider. Its temperature is brought to the rider's altitude with the
# standard lapse rate before it joins the blend. Station heights come from
# Open-Meteo's elevation API (Copernicus 90 m DEM) and never change, so each is
# asked once per process.

OPENMETEO_ELEVATION_URL = "https://api.open-meteo.com/v1/elevation"
LAPSE_RATE_C_PER_M = 0.0065
# Morning inversions break the lapse rate; beyond this the correction guesses.
MAX_ALTITUDE_CORRECTION_C = 4.0
ELEVATION_CACHE_MAX_ENTRIES = 2000
_elevation_cache: dict[tuple[float, float], float] = {}


async def _elevation_m(lat: float, lon: float, client: httpx.AsyncClient) -> float | None:
    """Ground elevation of a point, cached for the life of the process; None when the API fails."""
    key = (round(lat, 4), round(lon, 4))
    if key in _elevation_cache:
        return _elevation_cache[key]
    try:
        resp = await client.get(OPENMETEO_ELEVATION_URL, params={"latitude": key[0], "longitude": key[1]},
                                timeout=OPTIONAL_PROVIDER_HTTP_TIMEOUT_S)
        resp.raise_for_status()
        values = resp.json().get("elevation")
        elevation = _to_float(values[0]) if isinstance(values, list) and values else None
    except (httpx.HTTPError, ValueError, AttributeError) as exc:
        logger.warning("Open-Meteo elevation fetch failed: %s", _describe_error(exc))
        return None
    if elevation is None:
        return None
    if len(_elevation_cache) >= ELEVATION_CACHE_MAX_ENTRIES:
        _elevation_cache.clear()
    _elevation_cache[key] = elevation
    return elevation


def _altitude_corrected(station: dict, station_elevation: float | None, point_elevation: float | None) -> dict:
    """The station reading with its temperature moved to the rider's altitude."""
    temp = station.get("temp")
    if temp is None or station_elevation is None or point_elevation is None:
        return station
    correction = LAPSE_RATE_C_PER_M * (station_elevation - point_elevation)
    correction = max(-MAX_ALTITUDE_CORRECTION_C, min(MAX_ALTITUDE_CORRECTION_C, correction))
    return {
        **station,
        "temp": round(float(temp) + correction, 1),
        "measured_temp": temp,
        "elevation_m": round(station_elevation),
        "altitude_correction_c": round(correction, 1),
    }


def _first_not_none(*values: Any) -> Any:
    """First value that is not None (0 and 0.0 are valid values)."""
    for value in values:
        if value is not None:
            return value
    return None


def _merge_current(
    om_data: dict,
    owm_current: dict | None,
    owm_air: dict | None,
    om_air: dict | None = None,
    met_norm: dict | None = None,
    pw_norm: dict | None = None,
    wxm_norm: dict | None = None,
    netatmo_norm: dict | None = None,
    hourly: list[dict] | None = None,
    official_norm: dict | None = None,
    metar_obs: dict | None = None,
) -> dict:
    c = om_data.get("current", {})
    nta = netatmo_norm or {}
    off = official_norm or {}
    obs = metar_obs or {}
    if hourly is None:
        hourly = _build_hourly(om_data)
    hour_index = _current_hour_index(om_data, [str(h.get("time", "")) for h in hourly])
    current_hour = hourly[hour_index] if hour_index is not None else {}

    # --- temperature & feels-like
    om_temp = c.get("temperature_2m")
    om_feel = c.get("apparent_temperature")
    owm_temp = owm_current["main"]["temp"] if owm_current else None
    owm_feel = owm_current["main"]["feels_like"] if owm_current else None
    met_temp = met_norm.get("temp") if met_norm else None
    pw_temp = pw_norm.get("temp") if pw_norm else None
    pw_feel = pw_norm.get("feels_like") if pw_norm else None
    wxm_temp = wxm_norm.get("temp") if wxm_norm else None
    wxm_feel = wxm_norm.get("feels_like") if wxm_norm else None

    # When a physical WeatherXM station is available it is measuring the actual
    # air at that point — numerical models average over km² grid cells and can
    # be several degrees off for current conditions (urban heat island, terrain,
    # etc.).  Use the WeatherXM reading directly; otherwise blend the models, and
    # when a Netatmo public station is present add it to that blend with a high
    # weight.  Netatmo is *blended* rather than used as ground truth because
    # public stations vary in siting quality (rooftops, sun-exposed) — blending
    # keeps a poorly-sited station from dominating. An official synoptic
    # station (ANM, through MeteoGate) is sited to WMO rules but reports once an
    # hour, so it gets the largest weight in the blend without replacing it.
    # The weight fades with distance: a station 25 km away, often at another
    # altitude, says less about this point than one down the road.
    official_distance = off.get("distance_km")
    official_weight = OFFICIAL_STATION_WEIGHT * (
        max(0.3, 1 - float(official_distance) / 40) if official_distance is not None else 1.0
    )

    def _wxm_or_blend(wxm_val, model_vals, model_weights, nta_val=None, official_val=None):
        if wxm_val is not None:
            return round(float(wxm_val), 2)
        values, weights = list(model_vals), list(model_weights)
        if official_val is not None:
            values.append(official_val)
            weights.append(official_weight)
        if nta_val is not None:
            values.append(nta_val)
            weights.append(1.3)
        return _weighted_avg(values, weights)

    temp = _wxm_or_blend(wxm_temp, [om_temp, owm_temp, met_temp, pw_temp], [1.2, 1.0, 1.1, 0.8],
                         nta_val=nta.get("temp"), official_val=off.get("temp"))
    feels = _wxm_or_blend(wxm_feel, [om_feel, owm_feel, pw_feel], [1.0, 1.0, 0.8])

    # --- humidity
    om_hum = c.get("relative_humidity_2m")
    owm_hum = owm_current["main"]["humidity"] if owm_current else None
    met_hum = met_norm.get("humidity") if met_norm else None
    pw_hum = pw_norm.get("humidity") if pw_norm else None
    wxm_hum = wxm_norm.get("humidity") if wxm_norm else None
    humidity = _wxm_or_blend(wxm_hum, [om_hum, owm_hum, met_hum, pw_hum], [1.2, 1.0, 1.1, 0.8],
                             nta_val=nta.get("humidity"), official_val=off.get("humidity"))

    # --- wind speed (km/h) and gusts
    om_wind = c.get("wind_speed_10m")
    owm_wind = (owm_current["wind"]["speed"] * 3.6) if owm_current else None
    met_wind = met_norm.get("wind_speed_kmh") if met_norm else None
    pw_wind = pw_norm.get("wind_speed_kmh") if pw_norm else None
    wxm_wind = wxm_norm.get("wind_speed_kmh") if wxm_norm else None
    wind_speed = _wxm_or_blend(wxm_wind, [om_wind, owm_wind, met_wind, pw_wind], [1.2, 1.0, 1.1, 0.8],
                               nta_val=nta.get("wind_speed_kmh"), official_val=off.get("wind_speed_kmh"))

    om_gusts = c.get("wind_gusts_10m")
    # Only real gust values are blended; sustained wind never stands in for a gust.
    owm_gusts = _mps_to_kmh(owm_current["wind"].get("gust")) if owm_current else None
    met_gusts = met_norm.get("wind_gusts_kmh") if met_norm else None
    pw_gusts = pw_norm.get("wind_gusts_kmh") if pw_norm else None
    wxm_gusts = wxm_norm.get("wind_gusts_kmh") if wxm_norm else None
    wind_gusts = _wxm_or_blend(wxm_gusts, [om_gusts, owm_gusts, met_gusts, pw_gusts],
                               [1.2, 0.8, 1.1, 1.0], nta_val=nta.get("wind_gusts_kmh"),
                               official_val=off.get("wind_gusts_kmh"))

    # A measured direction when the official station has one, else the model's.
    wind_dir = _first_not_none(off.get("wind_dir"), c.get("wind_direction_10m"))

    # --- precipitation (physical station is ground truth here)
    om_prec = c.get("precipitation")
    owm_prec = (
        # OWM omits "rain" when it is dry, so absence really means 0 mm.
        owm_current.get("rain", {}).get("1h", 0.0)
        if owm_current
        else None
    )
    met_prec = met_norm.get("precipitation") if met_norm else None
    pw_prec = pw_norm.get("precipitation") if pw_norm else None
    wxm_prec = wxm_norm.get("precipitation") if wxm_norm else None
    # The official gauge's total covers the last full hour, not this moment: it
    # joins the blend, but does not count as the "station" reading below.
    precipitation = _wxm_or_blend(wxm_prec, [om_prec, owm_prec, met_prec, pw_prec], [1.0, 1.0, 1.1, 0.8],
                                  nta_val=nta.get("precipitation"), official_val=off.get("precipitation"))

    # Without a physical station the blend can read ~0 mm while the hour you are
    # actually in is forecast with real rain: the models' "current" block lags,
    # and OWM omits its "rain" field, which the blend takes as a measured zero.
    # Keep the wetter of the two so the gauge can never look better than the hour
    # its own timeline shows. A station reading stays ground truth.
    station_prec = _first_not_none(wxm_prec, nta.get("precipitation"))
    hour_prec = current_hour.get("precipitation_mm")
    if station_prec is None and hour_prec is not None:
        hour_prec = round(float(hour_prec), 2)
        precipitation = hour_prec if precipitation is None else max(precipitation, hour_prec)

    # --- rain probability of the current hour. A station that is measuring
    # rain right now makes it a certainty for the score.
    hour_probability = current_hour.get("precipitation_probability")
    score_probability = hour_probability
    if wxm_prec is not None and wxm_prec >= RAIN_NEGLIGIBLE_MM_H:
        score_probability = 100

    # --- weather code / description
    om_code = c.get("weather_code")
    owm_code_raw = owm_current["weather"][0]["id"] if owm_current else None
    owm_code = _owm_id_to_wmo(owm_code_raw) if owm_code_raw is not None else None
    wxm_code = wxm_norm.get("wmo_code") if wxm_norm else None
    # What an observer at a nearby airport reports right now (rain, storm, fog).
    metar_code = obs.get("wmo_code")

    # Weather code: when WXM station is active use its icon-derived code to
    # decide the icon (keeps visual in sync with measured conditions); then a
    # phenomenon observed at a nearby airport; then the providers.
    # Use OWM Romanian description for text since WXM has no localization.
    # OWM code 0 (clear sky) is a valid value, hence the explicit None checks.
    effective_code = _first_not_none(wxm_code, metar_code, owm_code, om_code)
    # Downgrade a precip/storm code to overcast for DISPLAY when it isn't actually
    # precipitating, so the icon and description stay consistent with the score.
    display_code = _effective_display_code(effective_code, precipitation, score_probability)
    _downgraded = display_code != effective_code
    # OWM's text describes its own code, so it is not used for an airport's.
    if owm_current and not _downgraded and (metar_code is None or wxm_code is not None):
        description = owm_current["weather"][0].get("description", _wmo_desc(display_code)).capitalize()
    else:
        description = _wmo_desc(display_code)
    icon_emoji = _wmo_icon(display_code)

    # --- pressure (sea level: stations, OWM and MET report it; Open-Meteo's
    # pressure_msl before the station-level surface_pressure), visibility
    pressure = _first_not_none(
        wxm_norm.get("pressure") if wxm_norm else None,
        off.get("pressure"),
        nta.get("pressure"),
        owm_current["main"].get("pressure") if owm_current else None,
        met_norm.get("pressure") if met_norm else None,
        c.get("pressure_msl"),
        c.get("surface_pressure"),
    )
    # Visibility measured at a nearby airport beats any model's.
    visibility_m = _first_not_none(
        obs.get("visibility_m"),
        owm_current.get("visibility") if owm_current else None,
        c.get("visibility"),
    )
    pw_vis = pw_norm.get("visibility_km") if pw_norm else None
    visibility_km = round(visibility_m / 1000, 1) if visibility_m is not None else pw_vis

    # --- air quality
    aqi = None
    if owm_air and owm_air.get("list"):
        aqi = owm_air["list"][0]["main"]["aqi"]

    pm10 = None
    pm2_5 = None
    ozone = None
    eu_aqi = None
    us_aqi = None
    pollen_index = None
    if om_air and om_air.get("hourly"):
        h = om_air["hourly"]

        def _first(name: str):
            vals = h.get(name) or []
            return vals[0] if vals else None

        pm10 = _first("pm10")
        pm2_5 = _first("pm2_5")
        ozone = _first("ozone")
        eu_aqi = _first("european_aqi")
        us_aqi = _first("us_aqi")

        pollen_vals = [
            _first("alder_pollen"),
            _first("birch_pollen"),
            _first("grass_pollen"),
            _first("mugwort_pollen"),
            _first("ragweed_pollen"),
        ]
        pollen_vals = [v for v in pollen_vals if v is not None]
        if pollen_vals:
            pollen_index = max(float(v) for v in pollen_vals)

    # --- day / night, road surface and frost risk
    is_day_raw = c.get("is_day")
    is_day = (
        bool(is_day_raw) if is_day_raw is not None
        else bool(current_hour.get("is_day", True))
    )
    dew_point = _dew_point_c(temp, humidity)
    road_temp = _road_surface_temp(temp, humidity, om_code, precipitation, is_day=is_day)
    frost_risk = _frost_risk(temp, road_temp, dew_point, precipitation, effective_code)

    # --- moto score
    # The score must use the same code that drives the displayed icon and
    # description (effective_code), and the rain probability of the current
    # hour (without it a storm code with 0 mm used to score 100).
    score, breakdown = _score_with_breakdown(
        feels, wind_gusts, precipitation, effective_code, score_probability,
        visibility_m=visibility_km * 1000 if visibility_km is not None else None,
        frost_risk=frost_risk,
    )

    return {
        "temperature": temp,
        "feels_like": feels,
        "humidity": humidity,
        "wind_speed_kmh": wind_speed,
        "wind_gusts_kmh": wind_gusts,
        "wind_direction_deg": wind_dir,
        "wind_direction": _wind_direction_label(wind_dir),
        "beaufort": _beaufort(wind_speed),
        "precipitation_mm": precipitation,
        "precipitation_probability": hour_probability,
        "rain_intensity": _rain_intensity(precipitation),
        "weather_code": display_code,
        "description": description,
        "icon": icon_emoji,
        "pressure_hpa": pressure,
        "visibility_km": visibility_km,
        "aqi": aqi,
        "pm10": pm10,
        "pm2_5": pm2_5,
        "ozone": ozone,
        "eu_aqi": eu_aqi,
        "us_aqi": us_aqi,
        "pollen_index": round(float(pollen_index), 1) if pollen_index is not None else None,
        "uv_index": current_hour.get("uv_index"),
        "is_day": is_day,
        "dew_point": dew_point,
        "frost_risk": frost_risk,
        "moto_score": score,
        "moto_label": _moto_label(score),
        "score_breakdown": breakdown,
        "road_surface_temp": road_temp,
        # How much the national models agree on this hour, and how many answered.
        "forecast_confidence": current_hour.get("forecast_confidence"),
        "model_count": current_hour.get("model_count"),
        "sources": (
            ["open-meteo"]
            + (["model-ensemble"] if current_hour.get("model_count") else [])
            + (["openweathermap"] if owm_current else [])
            + (["met-norway"] if met_norm else [])
            + (["pirate-weather"] if pw_norm else [])
            + (["weatherxm"] if wxm_norm else [])
            + (["netatmo"] if netatmo_norm else [])
            + (["official-stations"] if official_norm else [])
            + (["metar"] if metar_obs else [])
        ),
    }


def _owm_items_by_local_date(
    owm_forecast: dict | None, utc_offset_seconds: int
) -> dict[str, list[dict]]:
    """Group OWM 3-hour forecast items by LOCAL date.

    OWM timestamps are UTC (``dt`` / ``dt_txt``) while Open-Meteo's daily rows
    are local, so items are shifted by the location's UTC offset first. Days
    covering fewer than MIN_DAY_COVERAGE_HOURS are dropped.
    """
    by_date: dict[str, list[dict]] = {}
    if not owm_forecast:
        return by_date
    for item in owm_forecast.get("list", []):
        stamp = item.get("dt")
        if stamp is None:
            stamp = str(item.get("dt_txt", "")).replace(" ", "T")  # UTC wall clock
        local = _utc_to_local(stamp, utc_offset_seconds)
        if local is not None:
            by_date.setdefault(local.strftime("%Y-%m-%d"), []).append(item)
    return {
        date: items for date, items in by_date.items()
        if len(items) * 3 >= MIN_DAY_COVERAGE_HOURS
    }


def _merge_daily(
    om_data: dict,
    owm_forecast: dict | None,
    met_daily: dict | None = None,
    hourly: list[dict] | None = None,
    now_local: str | None = None,
) -> list[dict]:
    """Merge the daily rows of all sources.

    The score of each day comes from its riding hours (see _riding_hours and
    _daily_score_from_hours). ``now_local`` ("YYYY-MM-DDTHH:MM" at the
    location) defaults to the real current time; today's score only looks at
    the hours still ahead.
    """
    daily = om_data.get("daily", {})
    dates = daily.get("time", [])
    result = []
    utc_offset = int(om_data.get("utc_offset_seconds") or 0)
    if hourly is None:
        hourly = _build_hourly(om_data)
    if now_local is None:
        now_local = _local_now_iso(om_data)
    hours_by_date: dict[str, list[dict]] = {}
    for hour in hourly:
        hours_by_date.setdefault(str(hour.get("time", ""))[:10], []).append(hour)

    # Per-local-date OWM 3-hour items (only days with enough coverage)
    owm_by_date = _owm_items_by_local_date(owm_forecast, utc_offset)

    for i, date in enumerate(dates):
        om_code = _safe(daily.get("weather_code"), i)
        owm_items = owm_by_date.get(date, [])
        met_day = met_daily.get(date, {}) if met_daily else {}

        # temperatures
        t_max_om = _safe(daily.get("temperature_2m_max"), i)
        t_min_om = _safe(daily.get("temperature_2m_min"), i)
        fa_max_om = _safe(daily.get("apparent_temperature_max"), i)
        fa_min_om = _safe(daily.get("apparent_temperature_min"), i)

        if owm_items:
            owm_temps = [x["main"]["temp"] for x in owm_items]
            owm_feels = [x["main"]["feels_like"] for x in owm_items]
            t_max_owm = max(owm_temps)
            t_min_owm = min(owm_temps)
            fa_max_owm = max(owm_feels)
            fa_min_owm = min(owm_feels)
        else:
            t_max_owm = t_min_owm = fa_max_owm = fa_min_owm = None

        t_max_met = met_day.get("t_max")
        t_min_met = met_day.get("t_min")

        t_max = _weighted_avg([t_max_om, t_max_owm, t_max_met], [1.2, 1.0, 1.1])
        t_min = _weighted_avg([t_min_om, t_min_owm, t_min_met], [1.2, 1.0, 1.1])
        fa_max = _weighted_avg([fa_max_om, fa_max_owm], [1.0, 1.0])
        fa_min = _weighted_avg([fa_min_om, fa_min_owm], [1.0, 1.0])

        # precipitation (OWM/MET only for days they fully cover)
        prec_om = _safe(daily.get("precipitation_sum"), i)
        if owm_items:
            prec_owm = sum(
                x.get("rain", {}).get("3h", 0.0) + x.get("snow", {}).get("3h", 0.0)
                for x in owm_items
            )
        else:
            prec_owm = None
        prec_met = met_day.get("precipitation_sum")
        precipitation = _weighted_avg([prec_om, prec_owm, prec_met], [1.0, 1.0, 1.1])

        # wind gusts: only real gust values; sustained wind is never a gust
        gusts_om = _safe(daily.get("wind_gusts_10m_max"), i)
        owm_gust_values = [
            x["wind"]["gust"] * 3.6 for x in owm_items
            if (x.get("wind") or {}).get("gust") is not None
        ]
        gusts_owm = max(owm_gust_values) if owm_gust_values else None
        gusts_met = met_day.get("gust_max_kmh")
        wind_gusts = _weighted_avg([gusts_om, gusts_owm, gusts_met], [1.2, 0.8, 1.0])

        wind_max = _safe(daily.get("wind_speed_10m_max"), i)

        prec_prob_raw = _safe(daily.get("precipitation_probability_max"), i)
        prec_prob = prec_prob_raw or 0

        # score: from the day's riding hours (daylight; for today the hours
        # still ahead), weighting the worst hours; aggregate fallback otherwise.
        riding = _riding_hours(
            hours_by_date.get(date, []), now_local if date == now_local[:10] else None
        )

        # weather code: the most severe condition of the scored daylight hours,
        # so a night storm does not put a storm icon next to a good day score.
        # Only without scored daylight hours: the most severe code of the whole
        # day across sources (Open-Meteo's daily code is the day's most severe).
        owm_day_code = _most_severe_code(
            _owm_id_to_wmo(x["weather"][0]["id"]) for x in owm_items if x.get("weather")
        )
        whole_day_code = _most_severe_code([om_code, owm_day_code, met_day.get("wmo_code")])
        final_code = _first_not_none(_daylight_display_code(riding), whole_day_code)

        if riding:
            score = _daily_score_from_hours(riding)
            amounts = [h["precipitation_mm"] for h in riding if h.get("precipitation_mm") is not None]
            precipitation_max = round(max(amounts), 1) if amounts else None
            rain_intensity_max = _rain_intensity(precipitation_max)
        else:
            score = _moto_score_daily(fa_min, fa_max, wind_gusts, precipitation, final_code, prec_prob_raw)
            precipitation_max = None
            rain_intensity_max = _rain_intensity(
                precipitation / DAILY_SUM_PEAK_HOURS if precipitation is not None else None
            )

        result.append({
            "date": date,
            "weather_code": final_code,
            "icon": _wmo_icon(final_code),
            "description": _wmo_desc(final_code),
            "temp_max": t_max,
            "temp_min": t_min,
            "feels_max": fa_max,
            "feels_min": fa_min,
            "precipitation_mm": round(precipitation, 1) if precipitation is not None else 0.0,
            "precipitation_probability": prec_prob,
            "precipitation_max_mm_h": precipitation_max,
            "rain_intensity_max": rain_intensity_max,
            "wind_max_kmh": wind_max,
            "wind_gusts_kmh": wind_gusts,
            "moto_score": score,
            "moto_label": _moto_label(score),
            "sunrise": _safe(daily.get("sunrise"), i),
            "sunset": _safe(daily.get("sunset"), i),
        })

    return result


def _build_hourly(om_data: dict, ensemble: dict[str, dict[str, Any]] | None = None) -> list[dict]:
    """
    Hourly items, each with its own moto score and risk flags.

    Values come from Open-Meteo. When the model ensemble is available they are
    pulled toward the national models (their mean, the median for rain), the
    weather code is the one most models back, and the hour also carries how
    much those models agree (see ENSEMBLE_* above).
    """
    hourly = om_data.get("hourly", {})
    times = hourly.get("time", [])
    sun_by_date = _sun_times_by_date(om_data)
    result = []
    for i, t in enumerate(times):
        ens_hour = ensemble.get(str(t)) if ensemble else None
        means: dict[str, float] = ens_hour["means"] if ens_hour else {}
        code = _consensus_code(_safe(hourly.get("weather_code"), i), ens_hour.get("codes", []) if ens_hour else [])
        temp = _blend_with_model_mean(_safe(hourly.get("temperature_2m"), i),
                                      means.get("temperature_2m"))
        feels = _blend_with_model_mean(_safe(hourly.get("apparent_temperature"), i),
                                       means.get("apparent_temperature"))
        precipitation = _blend_with_model_mean(_safe(hourly.get("precipitation"), i),
                                               means.get("precipitation"))
        probability = _safe(hourly.get("precipitation_probability"), i)
        gusts = _blend_with_model_mean(_safe(hourly.get("wind_gusts_10m"), i),
                                       means.get("wind_gusts_10m"))
        humidity = _safe(hourly.get("relative_humidity_2m"), i)
        visibility = _safe(hourly.get("visibility"), i)
        dew_point_raw = _safe(hourly.get("dew_point_2m"), i)
        dew_point = dew_point_raw if dew_point_raw is not None else _dew_point_c(temp, humidity)
        is_day = _hour_is_day(_safe(hourly.get("is_day"), i), t, sun_by_date)
        road_temp = _road_surface_temp(temp, humidity, code, precipitation, is_day=is_day)
        frost_risk = _frost_risk(temp, road_temp, dew_point, precipitation, code)
        score = _moto_score(feels, gusts, precipitation, code, probability,
                            visibility_m=visibility, frost_risk=frost_risk)
        result.append({
            "time": t,
            "temperature": temp,
            "feels_like": feels,
            "precipitation_mm": precipitation,
            "precipitation_probability": probability,
            "rain_intensity": _rain_intensity(precipitation),
            "wind_speed_kmh": _blend_with_model_mean(_safe(hourly.get("wind_speed_10m"), i),
                                                     means.get("wind_speed_10m")),
            "wind_gusts_kmh": gusts,
            "weather_code": code,
            "icon": _wmo_icon(code),
            "description": _wmo_desc(code),
            "uv_index": _safe(hourly.get("uv_index"), i),
            "relative_humidity": humidity,
            "surface_pressure": _safe(hourly.get("surface_pressure"), i),
            "pressure_msl": _safe(hourly.get("pressure_msl"), i),
            "dew_point_2m": dew_point_raw,
            "cloud_cover": _safe(hourly.get("cloud_cover"), i),
            "visibility": visibility,
            "wind_direction_10m": _safe(hourly.get("wind_direction_10m"), i),
            "is_day": is_day,
            "road_surface_temp": road_temp,
            "frost_risk": frost_risk,
            "moto_score": score,
            "moto_label": _moto_label(score),
            "forecast_confidence": ens_hour["confidence"] if ens_hour else None,
            "model_count": ens_hour["models"] if ens_hour else None,
        })
    return result


def _safe(lst: list | None, i: int) -> Any:
    if lst is None or i >= len(lst):
        return None
    return lst[i]


# ---------------------------------------------------------------------------
# Route planner helpers
# ---------------------------------------------------------------------------

_ROUTE_HOURLY_VARIABLES = (
    "temperature_2m,apparent_temperature,precipitation_probability,"
    "precipitation,weather_code,wind_speed_10m,wind_gusts_10m,visibility,is_day"
)


async def _fetch_waypoint_forecast(
    lat: float, lon: float, client: httpx.AsyncClient
) -> dict[str, Any] | None:
    """Hourly Open-Meteo forecast for a route waypoint (cached per ~5 km cell)."""
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": _ROUTE_HOURLY_VARIABLES,
        "timezone": "auto",
        "forecast_days": 7,
        "wind_speed_unit": "kmh",
    }

    async def fetch() -> tuple[dict[str, Any] | None, float]:
        try:
            resp = await asyncio.wait_for(
                client.get(OPENMETEO_BASE, params=params, timeout=OPENMETEO_ATTEMPT_TIMEOUT_S),
                timeout=OPENMETEO_ATTEMPT_TIMEOUT_S,
            )
            if not resp.is_success:
                logger.warning("Route waypoint forecast failed: HTTP %s", resp.status_code)
                return None, 0.0
            data = resp.json()
        except (httpx.HTTPError, TimeoutError, ValueError) as exc:
            logger.warning("Route waypoint forecast failed: %s", _describe_error(exc))
            return None, 0.0
        if not isinstance(data, dict) or data.get("error"):
            logger.warning("Route waypoint forecast returned an error body")
            return None, 0.0
        return data, TTL_OPENMETEO_S

    return await _forecast_cache.get_or_fetch(("open-meteo-route", _coord_key(lat, lon)), fetch)


def _utc_offset_of(forecast: dict[str, Any] | None, default: int = 0) -> int:
    """UTC offset (seconds) reported by an Open-Meteo response, else ``default``."""
    if not forecast or forecast.get("utc_offset_seconds") is None:
        return default
    try:
        return int(forecast["utc_offset_seconds"])
    except (TypeError, ValueError):
        return default


def _validate_departure(departure_iso: str | None) -> None:
    """Raise ValueError when a departure is given but is not an ISO datetime."""
    if departure_iso is not None and departure_iso.strip():
        datetime.fromisoformat(departure_iso.strip())


def _resolve_departure(
    departure_iso: str | None, origin_utc_offset_s: int
) -> tuple[datetime, datetime]:
    """Departure as naive (origin local time, UTC).

    No value means "now at the origin": the server clock is UTC on Cloud Run
    while forecast times are local, so datetime.now() would be off by the
    offset. A value with a timezone is converted; a naive value is taken as
    origin local time. Raises ValueError for an unparseable value.
    """
    offset = timedelta(seconds=origin_utc_offset_s)
    if departure_iso is None or not departure_iso.strip():
        dep_utc = datetime.now(timezone.utc).replace(tzinfo=None)
        return dep_utc + offset, dep_utc
    parsed = datetime.fromisoformat(departure_iso.strip())
    if parsed.tzinfo is not None:
        dep_utc = parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return dep_utc + offset, dep_utc
    return parsed, parsed - offset


def _waypoint_snapshot(forecast: dict[str, Any] | None, eta_local: str) -> dict[str, Any]:
    """Weather at a waypoint for the first forecast hour at or after the ETA.

    ``eta_local`` is "YYYY-MM-DDTHH:MM" in the waypoint's own local time, the
    same frame as the forecast's hourly times.
    """
    if not forecast:
        return {}
    hourly = forecast.get("hourly", {})
    times = hourly.get("time", [])

    # Find the first hour >= ETA (truncated to the hour)
    eta_prefix = eta_local[:13]  # "YYYY-MM-DDTHH"
    best_idx = 0
    matched = False
    for j, t in enumerate(times):
        if t[:13] >= eta_prefix:
            best_idx = j
            matched = True
            break

    # ETA is past the forecast horizon: fall back to the furthest available
    # hour (not "now", index 0) and flag it so the UI can say "beyond forecast"
    # instead of silently showing current weather for a far-future waypoint.
    beyond_horizon = bool(times) and not matched
    if beyond_horizon:
        best_idx = len(times) - 1

    code = _safe(hourly.get("weather_code"), best_idx)
    feels = _safe(hourly.get("apparent_temperature"), best_idx)
    gusts = _safe(hourly.get("wind_gusts_10m"), best_idx)
    prec = _safe(hourly.get("precipitation"), best_idx)
    prec_prob = _safe(hourly.get("precipitation_probability"), best_idx)
    is_day_raw = _safe(hourly.get("is_day"), best_idx)
    score = _moto_score(feels, gusts, prec, code, prec_prob,
                        visibility_m=_safe(hourly.get("visibility"), best_idx))

    return {
        "time": times[best_idx] if best_idx < len(times) else eta_local,
        "temperature": _safe(hourly.get("temperature_2m"), best_idx),
        "feels_like": feels,
        "precipitation_mm": prec,
        "precipitation_probability": prec_prob,
        "rain_intensity": _rain_intensity(prec),
        "wind_speed_kmh": _safe(hourly.get("wind_speed_10m"), best_idx),
        "wind_gusts_kmh": gusts,
        "weather_code": code,
        "icon": _wmo_icon(code),
        "description": _wmo_desc(code),
        "is_day": bool(is_day_raw) if is_day_raw is not None else None,
        "moto_score": score,
        "moto_label": _moto_label(score),
        "beyond_forecast_horizon": beyond_horizon,
    }


def _eta_at_waypoint(
    dep_utc: datetime, hours_offset: float, forecast: dict[str, Any] | None, default_offset: int
) -> str:
    """ETA in the waypoint's local time (routes may cross a timezone border)."""
    eta = dep_utc + timedelta(hours=hours_offset, seconds=_utc_offset_of(forecast, default_offset))
    return eta.isoformat()[:16]


async def get_route_weather(
    origin_lat: float,
    origin_lon: float,
    origin_name: str,
    dest_lat: float,
    dest_lon: float,
    dest_name: str,
    departure_iso: str | None,
    avg_speed_kmh: float,
    owm_api_key: str,
    num_segments: int = 4,
) -> dict[str, Any]:
    """
    Compute weather along a motorcycle route from origin to destination.

    Uses linear (great-circle) interpolation for intermediate waypoints.
    Returns num_segments+1 waypoints with weather snapshots at estimated ETAs.
    ``departure_iso`` is local time at the origin (None = now at the origin);
    an invalid value raises ValueError. ``eta_iso`` is in origin local time.
    """
    _validate_departure(departure_iso)

    total_dist_km = _haversine_km(origin_lat, origin_lon, dest_lat, dest_lon)
    total_hours = total_dist_km / avg_speed_kmh if avg_speed_kmh > 0 else 0

    # Build waypoint metadata (start, N-1 intermediate, destination)
    waypoints_meta: list[dict[str, Any]] = []
    hours_offsets: list[float] = []
    for i in range(num_segments + 1):
        frac = i / num_segments
        wp_lat = origin_lat + frac * (dest_lat - origin_lat)
        wp_lon = origin_lon + frac * (dest_lon - origin_lon)
        hours_offsets.append(frac * total_hours)

        if i == 0:
            wp_name = origin_name
        elif i == num_segments:
            wp_name = dest_name
        else:
            wp_name = f"~{round(frac * total_dist_km)} km"

        waypoints_meta.append({
            "index": i,
            "name": wp_name,
            "lat": round(wp_lat, 4),
            "lon": round(wp_lon, 4),
            "distance_from_origin_km": round(frac * total_dist_km, 1),
        })

    # Fetch forecasts for all waypoints concurrently (cached per ~5 km cell)
    async with http_client_scope() as client:
        forecasts = await asyncio.gather(*[
            _fetch_waypoint_forecast(wp["lat"], wp["lon"], client) for wp in waypoints_meta
        ])

    origin_offset = next((_utc_offset_of(f) for f in forecasts if f), 0)
    dep_local, dep_utc = _resolve_departure(departure_iso, origin_offset)

    result_waypoints = []
    for wp, hours_offset, forecast in zip(waypoints_meta, hours_offsets, forecasts):
        eta_origin_local = dep_local + timedelta(hours=hours_offset)
        eta_local = _eta_at_waypoint(dep_utc, hours_offset, forecast, origin_offset)
        result_waypoints.append({
            **wp,
            "eta_iso": eta_origin_local.isoformat()[:16],
            "weather": _waypoint_snapshot(forecast, eta_local),
        })

    return {
        "origin": {"name": origin_name, "lat": origin_lat, "lon": origin_lon},
        "destination": {"name": dest_name, "lat": dest_lat, "lon": dest_lon},
        "departure": dep_local.isoformat()[:16],
        "avg_speed_kmh": avg_speed_kmh,
        "total_distance_km": round(total_dist_km, 1),
        "estimated_duration_h": round(total_hours, 2),
        "waypoints": result_waypoints,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def _with_ttl(
    fetch: Callable[[], Awaitable[Any]], ttl_s: float
) -> Callable[[], Awaitable[tuple[Any, float]]]:
    """Adapt a provider fetch that returns a value to the cache's (value, ttl) form."""
    async def fetch_with_ttl() -> tuple[Any, float]:
        return await fetch(), ttl_s
    return fetch_with_ttl


async def get_weather(
    lat: float,
    lon: float,
    city_name: str,
    owm_api_key: str,
    forecast_days: int = 7,
    pirate_weather_key: str = "",
    met_user_agent: str | None = None,
    weatherxm_api_key: str = "",
    netatmo_client_id: str = "",
    netatmo_client_secret: str = "",
    netatmo_refresh_token: str = "",
) -> dict[str, Any]:
    """
    Fetch and aggregate weather data from Open-Meteo (and its five-model
    ensemble), OpenWeatherMap, MET Norway, Pirate Weather, and measurements:
    official stations (MeteoGate), airports (METAR), WeatherXM and Netatmo;
    plus Meteoalarm and ANM nowcasting warnings.
    Returns a unified JSON-serialisable dict; current.source_status says what
    each source did for this answer.
    forecast_days: 7 (free) or 14 (premium — Open-Meteo supports up to 16).
    met_user_agent: optional; defaults to the MET_NORWAY_USER_AGENT env value.

    The whole call is bounded by WEATHER_TOTAL_BUDGET_S. Optional providers
    that miss OPTIONAL_PROVIDER_BUDGET_S are left out; Open-Meteo is required
    and its failure (or a blown budget) raises.
    """
    try:
        return await asyncio.wait_for(
            _collect_weather(
                lat, lon, city_name, owm_api_key, forecast_days, pirate_weather_key,
                _resolve_met_user_agent(met_user_agent), weatherxm_api_key,
                netatmo_client_id, netatmo_client_secret, netatmo_refresh_token,
            ),
            timeout=WEATHER_TOTAL_BUDGET_S,
        )
    except TimeoutError as exc:
        logger.warning("Weather aggregation exceeded the %.1fs budget without Open-Meteo data",
                       WEATHER_TOTAL_BUDGET_S)
        raise RuntimeError("Weather data not available within the time budget") from exc


def _source_status(
    *,
    current: dict[str, Any],
    keys: dict[str, bool],
    used: dict[str, bool],
    stations: tuple[Any, dict | None],
    airports: tuple[Any, dict | None],
    warnings: dict[str, tuple[Any, int]],
) -> list[dict[str, Any]]:
    """
    Every source and what it did for this answer, for the "data sources" view.

    status: "used" (it contributed), "off" (not configured on this server),
    "none-nearby" (answered, but nothing close enough or fresh enough),
    "no-data" (did not answer in time, failed, or had nothing to give).
    Warning feeds are "used" when they were read, with how many apply here.
    """
    out: list[dict[str, Any]] = [
        {"id": "open-meteo", "status": "used"},
        {"id": "model-ensemble", "status": "used" if current.get("model_count") else "no-data",
         "models": current.get("model_count")},
    ]
    for source in ("openweathermap", "met-norway", "pirate-weather"):
        configured = keys.get(source, True)
        out.append({"id": source, "status": "used" if used[source] else ("no-data" if configured else "off")})

    raw, norm = stations
    out.append({
        "id": "official-stations",
        "status": "used" if norm else ("none-nearby" if raw is not None else "no-data"),
        "station": norm.get("station") if norm else None,
        "distance_km": norm.get("distance_km") if norm else None,
        "observed_at": norm.get("observed_at") if norm else None,
    })
    raw, obs = airports
    out.append({
        "id": "metar",
        "status": "used" if obs else ("none-nearby" if raw is not None else "no-data"),
        "station": (obs.get("name") or obs.get("station")) if obs else None,
        "distance_km": obs.get("distance_km") if obs else None,
        "observed_at": obs.get("observed_at") if obs else None,
    })
    for source in ("weatherxm", "netatmo"):
        configured = keys.get(source, True)
        out.append({"id": source, "status": "used" if used[source] else ("no-data" if configured else "off")})

    for source, (feed, count) in warnings.items():
        out.append({"id": source, "status": "used" if feed is not None else "no-data", "count": count})
    return out


async def _collect_weather(
    lat: float,
    lon: float,
    city_name: str,
    owm_api_key: str,
    forecast_days: int,
    pirate_weather_key: str,
    met_user_agent: str,
    weatherxm_api_key: str,
    netatmo_client_id: str,
    netatmo_client_secret: str,
    netatmo_refresh_token: str,
) -> dict[str, Any]:
    """Fetch every provider concurrently (cached, time-bounded) and merge them."""
    days = min(max(int(forecast_days), 1), 16)
    cell = _coord_key(lat, lon)
    async with http_client_scope() as client:
        (om_data, ens_raw, owm_current, owm_forecast, owm_air, om_air, met_raw, pw_raw,
         wxm_raw, netatmo_raw, meteoalarm_feed, stations_raw, metar_raw, nowcast_feed) = await asyncio.gather(
            _forecast_cache.get_or_fetch(
                ("open-meteo", cell, days),
                _with_ttl(lambda: _fetch_openmeteo(lat, lon, client, forecast_days=days),
                          TTL_OPENMETEO_S),
            ),
            _cached_optional("Open-Meteo ensemble", ("om-ensemble", cell, days), _with_ttl(
                lambda: _fetch_openmeteo_ensemble(lat, lon, client, forecast_days=days),
                TTL_ENSEMBLE_S)),
            _cached_optional("OWM current", ("owm-current", cell), _with_ttl(
                lambda: _fetch_owm_current(lat, lon, owm_api_key, client), TTL_OWM_S)),
            _cached_optional("OWM forecast", ("owm-forecast", cell), _with_ttl(
                lambda: _fetch_owm_forecast(lat, lon, owm_api_key, client), TTL_OWM_S)),
            _cached_optional("OWM air", ("owm-air", cell), _with_ttl(
                lambda: _fetch_owm_air(lat, lon, owm_api_key, client), TTL_OWM_S)),
            _cached_optional("Open-Meteo air quality", ("om-air", cell), _with_ttl(
                lambda: _fetch_openmeteo_air_quality(lat, lon, client), TTL_AIR_QUALITY_S)),
            # MET's fetch already returns (data, ttl from the Expires header).
            _cached_optional("MET Norway", ("met-norway", cell),
                             lambda: _fetch_met_norway(lat, lon, client, met_user_agent)),
            _cached_optional("Pirate Weather", ("pirate-weather", cell), _with_ttl(
                lambda: _fetch_pirate_weather(lat, lon, pirate_weather_key, client), TTL_PIRATE_S)),
            # Station providers keep their own per-~1 km caches and backoff.
            _await_optional("WeatherXM", _fetch_weatherxm(lat, lon, weatherxm_api_key, client)),
            _await_optional("Netatmo", _fetch_netatmo(
                lat, lon, netatmo_client_id, netatmo_client_secret, netatmo_refresh_token, client)),
            # One feed covers the whole country, so every location shares the entry.
            _cached_optional("Meteoalarm", ("meteoalarm", meteoalarm.feed_url()), _with_ttl(
                lambda: meteoalarm.fetch_feed(client, met_user_agent), TTL_METEOALARM_S)),
            _cached_optional("Official stations", ("official-stations", cell), _with_ttl(
                lambda: official_stations.fetch(lat, lon, client, met_user_agent), TTL_STATIONS_S)),
            # Airports and nowcasting warnings: one answer for the whole country.
            _cached_optional("METAR", ("metar",), _with_ttl(
                lambda: metar.fetch(client, met_user_agent), TTL_METAR_S)),
            _cached_optional("ANM nowcasting", ("anm-nowcast",), _with_ttl(
                lambda: anm_nowcast.fetch_feed(client, met_user_agent), TTL_ANM_NOWCAST_S)),
        )
        official_norm = official_stations.normalize(stations_raw)
        if official_norm and official_norm.get("lat") is not None:
            station_elevation = await _await_optional("Open-Meteo elevation", _elevation_m(
                official_norm["lat"], official_norm["lon"], client))
            official_norm = _altitude_corrected(official_norm, station_elevation,
                                                _to_float(om_data.get("elevation")))

    utc_offset = int(om_data.get("utc_offset_seconds") or 0)
    met_norm = _normalize_met_current(met_raw)
    pw_norm = _normalize_pw_current(pw_raw)
    wxm_norm = _normalize_wxm_current(wxm_raw)
    netatmo_norm = _normalize_netatmo_current(netatmo_raw)
    metar_obs = metar.nearest(metar_raw, lat, lon)
    met_daily = _aggregate_met_daily(met_raw, utc_offset)

    hourly = _build_hourly(om_data, _ensemble_by_time(ens_raw))
    current = _merge_current(om_data, owm_current, owm_air, om_air, met_norm, pw_norm,
                             wxm_norm, netatmo_norm, hourly=hourly,
                             official_norm=official_norm, metar_obs=metar_obs)
    # Before the daily scores are built, so the day counts the hours as corrected.
    _carry_measured_bias(om_data, hourly, current, _measured_fields(wxm_norm, netatmo_norm, official_norm))
    _sync_current_hour(om_data, hourly, current)
    daily = _merge_daily(om_data, owm_forecast, met_daily, hourly=hourly)
    # Official warnings are advisory on top of our own score, never a source for it.
    county_alerts = meteoalarm.warnings_for(meteoalarm_feed, lat, lon, city_name) if meteoalarm_feed else []
    nowcast_alerts = anm_nowcast.warnings_for(nowcast_feed, lat, lon, city_name) if nowcast_feed else []
    alerts = sorted(county_alerts + nowcast_alerts,
                    key=lambda w: (meteoalarm.LEVEL_ORDER.get(w["level"], 9), w["onset"] or ""))
    current["source_status"] = _source_status(
        current=current,
        keys={
            "openweathermap": bool(owm_api_key),
            "pirate-weather": bool(pirate_weather_key),
            "weatherxm": bool(weatherxm_api_key),
            "netatmo": bool(netatmo_client_id and netatmo_client_secret),
        },
        used={
            "openweathermap": owm_current is not None,
            "met-norway": met_norm is not None,
            "pirate-weather": pw_norm is not None,
            "weatherxm": wxm_norm is not None,
            "netatmo": netatmo_norm is not None,
        },
        stations=(stations_raw, official_norm),
        airports=(metar_raw, metar_obs),
        warnings={
            "anm-nowcast": (nowcast_feed, len(nowcast_alerts)),
            "meteoalarm": (meteoalarm_feed, len(county_alerts)),
        },
    )

    return {
        "city": city_name,
        "latitude": lat,
        "longitude": lon,
        "timezone": om_data.get("timezone") or "UTC",
        "utc_offset_seconds": utc_offset,
        "current": current,
        "daily": daily,
        "hourly": hourly,
        "alerts": alerts,
    }


# ---------------------------------------------------------------------------
# Multi-stop route weather (premium)
# ---------------------------------------------------------------------------

async def get_multi_route_weather(
    stops: list[dict],
    departure_iso: str | None,
    avg_speed_kmh: float,
    owm_api_key: str,
) -> dict[str, Any]:
    """
    Compute weather along a multi-stop motorcycle route.

    Parameters
    ----------
    stops: list of dicts with keys ``name``, ``lat``, ``lon`` (2–5 items).
    departure_iso: ISO-8601 departure, local time at the first stop
        (None = now there). An invalid value raises ValueError.
    avg_speed_kmh: average speed in km/h.
    owm_api_key: OpenWeatherMap API key.

    Returns a dict with ``segments`` (one entry per consecutive stop pair),
    ``total_distance_km``, ``estimated_duration_h``, and ``stops`` metadata.
    """
    _validate_departure(departure_iso)

    segments: list[dict[str, Any]] = []
    elapsed_hours = 0.0
    total_dist_km = 0.0

    all_waypoints: list[dict[str, Any]] = []
    # Hours after departure for each waypoint dict (keyed by id: segment-end
    # waypoints are separate dicts sharing an index with the next start).
    hours_by_waypoint: dict[int, float] = {}

    for seg_idx in range(len(stops) - 1):
        origin = stops[seg_idx]
        dest = stops[seg_idx + 1]

        seg_dist_km = _haversine_km(origin["lat"], origin["lon"], dest["lat"], dest["lon"])
        seg_hours = seg_dist_km / avg_speed_kmh if avg_speed_kmh > 0 else 0

        num_seg = 3  # intermediate points per segment
        seg_waypoints: list[dict[str, Any]] = []
        for i in range(num_seg + 1):
            frac = i / num_seg
            wp_lat = origin["lat"] + frac * (dest["lat"] - origin["lat"])
            wp_lon = origin["lon"] + frac * (dest["lon"] - origin["lon"])

            if i == 0:
                wp_name = origin["name"]
            elif i == num_seg:
                wp_name = dest["name"]
            else:
                wp_name = f"~{round(frac * seg_dist_km + total_dist_km)} km"

            waypoint = {
                "index": len(all_waypoints) + i,
                "name": wp_name,
                "lat": round(wp_lat, 4),
                "lon": round(wp_lon, 4),
                "distance_from_start_km": round(total_dist_km + frac * seg_dist_km, 1),
            }
            hours_by_waypoint[id(waypoint)] = elapsed_hours + frac * seg_hours
            seg_waypoints.append(waypoint)

        all_waypoints.extend(seg_waypoints[:-1] if seg_idx < len(stops) - 2 else seg_waypoints)
        total_dist_km += seg_dist_km
        elapsed_hours += seg_hours

        segments.append({
            "from": origin["name"],
            "to": dest["name"],
            "distance_km": round(seg_dist_km, 1),
            "duration_h": round(seg_hours, 2),
            "waypoints": seg_waypoints,
        })

    # Fetch forecasts for unique waypoints concurrently (cached per ~5 km cell)
    async with http_client_scope() as client:
        forecasts = await asyncio.gather(*[
            _fetch_waypoint_forecast(wp["lat"], wp["lon"], client) for wp in all_waypoints
        ])

    origin_offset = next((_utc_offset_of(f) for f in forecasts if f), 0)
    dep_local, dep_utc = _resolve_departure(departure_iso, origin_offset)

    # Weather per unique waypoint, matched in that waypoint's local time
    weather_by_index = {
        wp["index"]: _waypoint_snapshot(
            forecast,
            _eta_at_waypoint(dep_utc, hours_by_waypoint[id(wp)], forecast, origin_offset),
        )
        for wp, forecast in zip(all_waypoints, forecasts)
    }

    # ETA (origin local time) and weather on every segment waypoint
    for seg in segments:
        for wp in seg["waypoints"]:
            hours_offset = hours_by_waypoint[id(wp)]
            wp["eta_iso"] = (dep_local + timedelta(hours=hours_offset)).isoformat()[:16]
            wp["weather"] = weather_by_index.get(wp["index"], {})

    return {
        "stops": [{"name": s["name"], "lat": s["lat"], "lon": s["lon"]} for s in stops],
        "departure": departure_iso,
        "avg_speed_kmh": avg_speed_kmh,
        "total_distance_km": round(total_dist_km, 1),
        "estimated_duration_h": round(elapsed_hours, 2),
        "segments": segments,
    }
