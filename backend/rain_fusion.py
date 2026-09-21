"""
Rain from every source at once.

Rain decides a ride more than anything else, and no single forecast gets it
right: one model puts the shower 20 km too far east, another an hour too
late. So every source that says something about rain for an hour gets a vote,
and the hour's chance of rain is the weighted mean of those votes:

- ensemble systems (ICON-EU EPS, ECMWF ENS, NCEP GEFS through Open-Meteo's
  ensemble API): the share of their 122 runs that give rain. This is the only
  real probability in the set, so it weighs the most.
- the national models (ECMWF, ICON, GFS, Météo-France, UKMO, GEM): the share
  of them that give rain.
- Open-Meteo's own precipitation_probability for the seamless blend.
- Pirate Weather and OpenWeatherMap, each with its own chance of rain.
- MET Norway: an amount only (no probability outside the Nordics), counted as
  a yes / no vote with a small weight, since it runs on ECMWF already counted.

Radar is not here: it sees only the next ~90 minutes and the app extrapolates
it itself (app/src/features/now/logic/radarRain.ts).

Every hour also gets a tally, "how many sources give rain out of how many",
which is what the app shows to explain the percentage. The functions are pure;
weather_service fetches the payloads and applies the result.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any

ENSEMBLE_API_URL = "https://ensemble-api.open-meteo.com/v1/ensemble"
# Open-Meteo's names; each answers with a control run plus its members.
ENSEMBLE_SYSTEMS: tuple[str, ...] = ("icon_eu", "ecmwf_ifs025", "gfs025")
# A run "gives rain" for an hour from this amount on (the bar a rain code needs).
WET_MM = 0.1
# Fewer runs than this say too little to count as a probability.
MIN_ENSEMBLE_MEMBERS = 10

# How much each kind of vote counts. The ensembles are real probabilities over
# many runs; the national models are few and share their physics; Open-Meteo's
# own probability comes from ensembles too, so it stays below them; the single
# commercial providers are one opinion each; MET repeats ECMWF.
WEIGHTS: dict[str, float] = {
    "ensemble": 3.0,
    "models": 2.0,
    "open-meteo": 1.5,
    "pirate-weather": 1.0,
    "openweathermap": 1.0,
    "met-norway": 0.5,
}

# A source with a probability "gives rain" in the tally from this chance on.
TALLY_WET_PROBABILITY = 0.5
# OpenWeatherMap forecasts 3-hour blocks; its chance is spread over the block's hours.
OWM_BLOCK_HOURS = 3


def _local_hour(moment: datetime, utc_offset_seconds: int) -> str:
    """UTC moment -> the local hourly slot it falls in, "YYYY-MM-DDTHH:00"."""
    local = moment.astimezone(timezone.utc).replace(tzinfo=None) + timedelta(seconds=utc_offset_seconds)
    return local.strftime("%Y-%m-%dT%H:00")


def _parse_utc(value: Any) -> datetime | None:
    if isinstance(value, (int, float)) and math.isfinite(value):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str) and value:
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    return None


def _number(value: Any) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    return float(value) if math.isfinite(value) else None


def _percentile(sorted_values: list[float], fraction: float) -> float:
    """Linear-interpolated percentile of an already sorted list."""
    position = (len(sorted_values) - 1) * fraction
    low = math.floor(position)
    high = math.ceil(position)
    if low == high:
        return sorted_values[low]
    return sorted_values[low] + (sorted_values[high] - sorted_values[low]) * (position - low)


# ---------------------------------------------------------------------------
# Per-source readers: payload -> {local hour: vote}
# ---------------------------------------------------------------------------
# A vote is {"probability": 0..1, "amount": mm/h or None}; the ensemble also
# carries "members" and the amount range.

def ensemble_votes(payload: dict | None) -> dict[str, dict[str, Any]]:
    """Share of all ensemble runs that give rain, per local hour (times are already local)."""
    if not isinstance(payload, dict):
        return {}
    hourly = payload.get("hourly")
    if not isinstance(hourly, dict) or not isinstance(hourly.get("time"), list):
        return {}
    # Every precipitation key is one run: "precipitation_icon_eu_eps" is a
    # control run, "precipitation_member07_ecmwf_ifs025_ensemble" a member.
    runs = [values for key, values in hourly.items()
            if key.startswith("precipitation") and isinstance(values, list)]
    votes: dict[str, dict[str, Any]] = {}
    for index, time_value in enumerate(hourly["time"]):
        amounts = sorted(v for v in (_number(run[index]) if index < len(run) else None for run in runs)
                         if v is not None)
        if len(amounts) < MIN_ENSEMBLE_MEMBERS:
            continue
        wet = sum(1 for v in amounts if v >= WET_MM)
        votes[str(time_value)[:13] + ":00"] = {
            "probability": wet / len(amounts),
            "amount": _percentile(amounts, 0.5),
            # Only worth showing when at least the wettest tenth of the runs gives rain.
            "range": ((round(_percentile(amounts, 0.25), 1), round(_percentile(amounts, 0.9), 1))
                      if _percentile(amounts, 0.9) >= WET_MM else None),
            "members": len(amounts),
        }
    return votes


def pirate_votes(payload: dict | None, utc_offset_seconds: int) -> dict[str, dict[str, Any]]:
    """Pirate Weather's hourly chance and intensity of rain (units=si, mm/h)."""
    if not isinstance(payload, dict):
        return {}
    rows = (payload.get("hourly") or {}).get("data")
    votes: dict[str, dict[str, Any]] = {}
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        moment = _parse_utc(row.get("time"))
        probability = _number(row.get("precipProbability"))
        if moment is None or probability is None:
            continue
        votes[_local_hour(moment, utc_offset_seconds)] = {
            "probability": max(0.0, min(1.0, probability)),
            "amount": _number(row.get("precipIntensity")),
        }
    return votes


def owm_votes(payload: dict | None, utc_offset_seconds: int) -> dict[str, dict[str, Any]]:
    """OpenWeatherMap's 3-hour chance of rain ("pop"), given to each hour of the block.

    The block is stamped with its middle-ish time, so the chance goes to the
    stamped hour and the hours on each side of it.
    """
    if not isinstance(payload, dict):
        return {}
    votes: dict[str, dict[str, Any]] = {}
    for item in payload.get("list") or []:
        if not isinstance(item, dict):
            continue
        moment = _parse_utc(item.get("dt"))
        probability = _number(item.get("pop"))
        if moment is None or probability is None:
            continue
        rain_3h = _number((item.get("rain") or {}).get("3h")) if isinstance(item.get("rain"), dict) else None
        amount = rain_3h / OWM_BLOCK_HOURS if rain_3h is not None else 0.0
        for shift in (-1, 0, 1):
            hour = _local_hour(moment + timedelta(hours=shift), utc_offset_seconds)
            votes.setdefault(hour, {"probability": max(0.0, min(1.0, probability)), "amount": amount})
    return votes


def met_votes(payload: dict | None, utc_offset_seconds: int) -> dict[str, dict[str, Any]]:
    """MET Norway's hourly amount, as a yes / no vote."""
    if not isinstance(payload, dict):
        return {}
    try:
        series = payload["properties"]["timeseries"]
    except (KeyError, TypeError):
        return {}
    votes: dict[str, dict[str, Any]] = {}
    for step in series if isinstance(series, list) else []:
        try:
            details = step["data"]["next_1_hours"]["details"]
        except (KeyError, TypeError):
            continue  # past the first days MET only has 6-hour blocks
        moment = _parse_utc(step.get("time"))
        amount = _number(details.get("precipitation_amount"))
        if moment is None or amount is None:
            continue
        probability = _number(details.get("probability_of_precipitation"))
        votes[_local_hour(moment, utc_offset_seconds)] = {
            "probability": probability / 100 if probability is not None else (1.0 if amount >= WET_MM else 0.0),
            "amount": amount,
        }
    return votes


# ---------------------------------------------------------------------------
# Fusion
# ---------------------------------------------------------------------------

def fuse_hour(
    *,
    open_meteo_probability: float | None,
    wet_models: int = 0,
    rain_models: int = 0,
    votes: dict[str, dict[str, Any] | None],
) -> dict[str, Any] | None:
    """One hour's chance of rain (0-100) and the tally of sources behind it.

    ``open_meteo_probability`` is in percent, as Open-Meteo gives it;
    ``votes`` maps a source name from WEIGHTS to that source's vote for the
    hour (None when it has nothing). Returns None when no source says anything.
    """
    weighted: list[tuple[float, float]] = []
    wet_sources = total_sources = 0

    def add(name: str, probability: float) -> None:
        nonlocal wet_sources, total_sources
        weighted.append((probability, WEIGHTS[name]))
        total_sources += 1
        wet_sources += probability >= TALLY_WET_PROBABILITY

    if open_meteo_probability is not None:
        add("open-meteo", max(0.0, min(1.0, open_meteo_probability / 100)))
    if rain_models:
        # The models tally one by one: "4 of 6 models give rain" reads better than a share.
        weighted.append((wet_models / rain_models, WEIGHTS["models"]))
        total_sources += rain_models
        wet_sources += wet_models
    for name, vote in votes.items():
        if vote is not None:
            add(name, float(vote["probability"]))
    if not weighted:
        return None
    probability = sum(p * w for p, w in weighted) / sum(w for _, w in weighted)
    ensemble = votes.get("ensemble")
    return {
        "probability": round(probability * 100),
        "wet_sources": wet_sources,
        "total_sources": total_sources,
        "range": ensemble["range"] if ensemble else None,
        "ensemble_members": ensemble["members"] if ensemble else None,
    }
