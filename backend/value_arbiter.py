"""
Temperature, humidity, wind and gusts right now, from every witness at once.

arbiter.py decides whether it rains. This module asks the same question of the
numbers: how warm is it, how damp, how windy. Before, a fixed-weight average
blended the providers, and a WeatherXM station, when there was one, replaced
the whole blend on its own. A single badly sited station could therefore
overrule everyone: the "one source says rain" problem again, with
temperatures.

Here every source is a witness with an uncertainty (a standard deviation, in
the units of the value):

- the providers' current blocks (Open-Meteo, OpenWeatherMap, MET Norway,
  Pirate Weather) share most of their physics, so they speak as one witness,
  "the models", with their usual error at a point;
- each station (the official ANM station, WeatherXM, Netatmo, the
  Sensor.Community median) is its own witness. A station is usually closer to
  the truth than a model grid cell, but its uncertainty grows with distance and
  with the age of its reading, and is larger for networks sited by their owners;
- the verification log can replace the hand-set uncertainties with measured
  ones, and teach a bias: a network that reads warm, models that run cold in
  this cell (calibration.py, ``learned``).

The value is the inverse-variance mean of the witnesses: whoever is surest
counts most. Before that, each station is checked against all the others
(leave one out): a station further than Z_REJECT combined deviations from
what everyone else says is set aside, and the sentence says so. Two stations
that agree with each other survive together even when the models disagree;
one station alone against everything does not.

The functions are pure. weather_service gathers the witnesses and applies the
verdict.
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any

VARIABLES = ("temperature", "humidity", "wind", "gusts")

# Hand-set uncertainty (one standard deviation) of each witness, per variable.
# Models: the usual error of a "current" block at a point. Stations: the error
# of the reading itself, siting included.
SIGMA: dict[str, dict[str, float]] = {
    "temperature": {"models": 1.6, "official": 0.5, "weatherxm": 0.8, "netatmo": 1.8, "sensor-community": 1.0},
    "humidity": {"models": 8.0, "official": 4.0, "weatherxm": 5.0, "netatmo": 7.0, "sensor-community": 6.0},
    "wind": {"models": 5.0, "official": 3.0, "weatherxm": 4.0, "netatmo": 7.0},
    "gusts": {"models": 8.0, "official": 5.0, "weatherxm": 6.0, "netatmo": 10.0},
}
# How much a station's uncertainty grows per km between it and the rider.
DISTANCE_SIGMA_PER_KM: dict[str, float] = {"temperature": 0.05, "humidity": 0.3, "wind": 0.2, "gusts": 0.3}
# And per hour of age of its reading (the official station reports hourly),
# counted up to AGE_MAX_H.
AGE_MAX_H = 2.0
AGE_SIGMA_PER_H: dict[str, float] = {"temperature": 0.6, "humidity": 4.0, "wind": 3.0, "gusts": 5.0}
# A station this many combined deviations away from all the others is set aside.
Z_REJECT = 2.5
# Learned uncertainties are kept within this factor of the hand-set ones.
SIGMA_LEARNED_MIN, SIGMA_LEARNED_MAX = 0.5, 2.0
# A learned bias is never applied beyond this many hand-set deviations.
BIAS_MAX_SIGMAS = 1.5
# The sentence is worth showing when the stations moved the value this much.
NOTABLE_SHIFT: dict[str, float] = {"temperature": 1.0, "humidity": 8.0, "wind": 5.0, "gusts": 8.0}
# Confidence from the final uncertainty: "high" under the first, "medium" under the second.
CONFIDENCE_SIGMA: dict[str, tuple[float, float]] = {
    "temperature": (0.8, 1.5),
    "humidity": (5.0, 9.0),
    "wind": (3.5, 6.0),
    "gusts": (5.0, 9.0),
}
BOUNDS: dict[str, tuple[float, float | None]] = {
    "temperature": (-60.0, 60.0),
    "humidity": (0.0, 100.0),
    "wind": (0.0, None),
    "gusts": (0.0, None),
}

STATION_NAMES = {
    "official": ("stația ANM", "the ANM station"),
    "weatherxm": ("stația WeatherXM", "the WeatherXM station"),
    "netatmo": ("o stație Netatmo", "a Netatmo station"),
    "sensor-community": ("senzorii din cartier", "the neighbourhood sensors"),
}
VARIABLE_NAMES = {
    "temperature": ("Temperatura", "The temperature"),
    "humidity": ("Umiditatea", "The humidity"),
    "wind": ("Vântul", "The wind"),
    "gusts": ("Rafalele", "The gusts"),
}


@dataclass
class ValueWitness:
    id: str
    value: float
    sigma: float
    distance_km: float | None = None
    station: str | None = None
    count: int = 1
    rejected: bool = False
    z: float | None = None


def _num(value: Any) -> float | None:
    try:
        out = None if value is None else float(value)
    except (TypeError, ValueError):
        return None
    return out if out is not None and math.isfinite(out) else None


def _learned_sigma(variable: str, source: str, learned: dict[str, Any] | None) -> float:
    default = SIGMA[variable][source]
    value = _num(((learned or {}).get("sigma") or {}).get(variable, {}).get(source))
    if value is None or value <= 0:
        return default
    return max(default * SIGMA_LEARNED_MIN, min(default * SIGMA_LEARNED_MAX, value))


def _learned_bias(variable: str, source: str, learned: dict[str, Any] | None) -> float:
    """What the source is known to add to the truth here (subtracted from its reading)."""
    bias = _num(((learned or {}).get("bias") or {}).get(variable, {}).get(source))
    if bias is None:
        return 0.0
    limit = BIAS_MAX_SIGMAS * SIGMA[variable][source]
    return max(-limit, min(limit, bias))


def _station_sigma(variable: str, source: str, distance_km: float | None, age_h: float | None,
                   learned: dict[str, Any] | None) -> float:
    base = _learned_sigma(variable, source, learned)
    far = DISTANCE_SIGMA_PER_KM[variable] * max(0.0, distance_km or 0.0)
    old = AGE_SIGMA_PER_H[variable] * min(AGE_MAX_H, max(0.0, age_h or 0.0))
    return math.sqrt(base ** 2 + far ** 2 + old ** 2)


def _pooled(witnesses: list[ValueWitness]) -> tuple[float, float] | None:
    weights = [1 / w.sigma ** 2 for w in witnesses]
    total = sum(weights)
    if total <= 0:
        return None
    return sum(w.value * k for w, k in zip(witnesses, weights)) / total, math.sqrt(1 / total)


def _clamp(variable: str, value: float) -> float:
    low, high = BOUNDS[variable]
    value = max(low, value)
    return min(high, value) if high is not None else value


def judge_value(
    variable: str,
    *,
    models: tuple[list[float | None], list[float]],
    stations: dict[str, dict[str, Any]],
    learned: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    """One value from every witness, with its uncertainty and its reasons.

    ``models`` is (values, relative weights) of the providers' current blocks.
    ``stations`` maps a station source (official, weatherxm, netatmo,
    sensor-community) to {"value", "distance_km", "age_h", "station"}.
    ``learned`` is calibration's {"sigma": {variable: {source: σ}},
    "bias": {variable: {source: b}}}, already narrowed to this cell.
    None when nobody said anything.
    """
    values, weights = models
    pairs = [(float(v), w) for v, w in zip(values, weights) if _num(v) is not None and w > 0]
    witnesses: list[ValueWitness] = []
    models_value: float | None = None
    if pairs:
        models_value = sum(v * w for v, w in pairs) / sum(w for _, w in pairs)
        corrected = models_value - _learned_bias(variable, "models", learned)
        witnesses.append(ValueWitness(
            id="models", value=corrected, sigma=_learned_sigma(variable, "models", learned), count=len(pairs)))
    for source, reading in stations.items():
        value = _num(reading.get("value"))
        if value is None or source not in SIGMA[variable]:
            continue
        witnesses.append(ValueWitness(
            id=source,
            value=value - _learned_bias(variable, source, learned),
            sigma=_station_sigma(variable, source, _num(reading.get("distance_km")),
                                 _num(reading.get("age_h")), learned),
            distance_km=_num(reading.get("distance_km")),
            station=reading.get("station"),
        ))
    if not witnesses:
        return None

    # Leave one out: the station furthest from everyone else goes first, then look again.
    while True:
        kept = [w for w in witnesses if not w.rejected]
        worst: ValueWitness | None = None
        for w in kept:
            if w.id == "models":
                continue
            others = _pooled([o for o in kept if o is not w])
            if others is None:
                continue
            w.z = abs(w.value - others[0]) / math.sqrt(w.sigma ** 2 + others[1] ** 2)
            if w.z > Z_REJECT and (worst is None or (w.z or 0) > (worst.z or 0)):
                worst = w
        if worst is None:
            break
        worst.rejected = True

    kept = [w for w in witnesses if not w.rejected]
    pooled = _pooled(kept)
    if pooled is None:
        return None
    value, sigma = pooled
    value = _clamp(variable, value)
    high, medium = CONFIDENCE_SIGMA[variable]
    confidence = "high" if sigma <= high else "medium" if sigma <= medium else "low"
    total = sum(1 / w.sigma ** 2 for w in kept)
    used = [w for w in kept if w.id != "models"]
    shift = abs(value - models_value) if models_value is not None else 0.0
    rejected = [w for w in witnesses if w.rejected]
    explanation, explanation_en = _explain(variable, value, models_value, used, rejected,
                                           next((w.count for w in witnesses if w.id == "models"), 0))
    return {
        "value": round(value, 2),
        "sigma": round(sigma, 2),
        "confidence": confidence,
        "models_value": round(models_value, 2) if models_value is not None else None,
        "witnesses": [
            {
                "id": w.id,
                "value": round(w.value, 2),
                "sigma": round(w.sigma, 2),
                "share": 0.0 if w.rejected else round((1 / w.sigma ** 2) / total, 3),
                **({"distance_km": round(w.distance_km, 1)} if w.distance_km is not None else {}),
                **({"station": w.station} if w.station else {}),
                **({"count": w.count} if w.count > 1 else {}),
                **({"set_aside": True} if w.rejected else {}),
            }
            for w in witnesses
        ],
        "notable": bool(rejected) or (bool(used) and shift >= NOTABLE_SHIFT[variable]),
        "explanation": explanation,
        "explanation_en": explanation_en,
    }


def _fmt(variable: str, value: float, lang: str) -> str:
    if variable == "temperature":
        text = f"{value:.1f}°"
    elif variable == "humidity":
        text = f"{value:.0f}%"
    else:
        text = f"{value:.0f} km/h"
    return text.replace(".", ",") if lang == "ro" else text


def _station_label(w: ValueWitness, lang: str) -> str:
    ro, en = STATION_NAMES.get(w.id, (w.id, w.id))
    if w.id == "official" and w.station:
        ro, en = f"stația ANM {w.station}", f"the ANM station {w.station}"
    label = ro if lang == "ro" else en
    if w.distance_km is not None and w.id != "sensor-community":
        label += f" ({w.distance_km:.0f} km)"
    return label


def _join(items: list[str], conj: str) -> str:
    return items[0] if len(items) == 1 else ", ".join(items[:-1]) + f" {conj} " + items[-1]


def _explain(variable: str, value: float, models_value: float | None, used: list[ValueWitness],
             rejected: list[ValueWitness], model_count: int) -> tuple[str, str]:
    what_ro, what_en = VARIABLE_NAMES[variable]
    if used:
        ro = f"{what_ro} {_fmt(variable, value, 'ro')} vine de la {_join([_station_label(w, 'ro') for w in used], 'și')}"
        en = f"{what_en} {_fmt(variable, value, 'en')} comes from {_join([_station_label(w, 'en') for w in used], 'and')}"
        if models_value is not None and abs(value - models_value) >= NOTABLE_SHIFT[variable] / 2:
            ro += f"; modelele dădeau {_fmt(variable, models_value, 'ro')}"
            en += f"; the models gave {_fmt(variable, models_value, 'en')}"
        ro += "."
        en += "."
    else:
        ro = f"{what_ro} {_fmt(variable, value, 'ro')} e estimarea modelelor"
        en = f"{what_en} {_fmt(variable, value, 'en')} is the models' estimate"
        if model_count > 1:
            ro += f" ({model_count} surse)"
            en += f" ({model_count} sources)"
        ro += ", fără nicio stație de încredere în apropiere."
        en += ", with no trusted station nearby."
    for w in rejected:
        label_ro, label_en = _station_label(w, "ro"), _station_label(w, "en")
        plural = w.id == "sensor-community"
        ro += (f" {label_ro[0].upper()}{label_ro[1:]} {'arătau' if plural else 'arăta'} "
               f"{_fmt(variable, w.value, 'ro')}, prea departe de ceilalți, și "
               f"{'n-au fost luați' if plural else 'n-a fost luată'} în calcul.")
        en += (f" {label_en[0].upper()}{label_en[1:]} read {_fmt(variable, w.value, 'en')}, "
               f"too far from the rest, and {'were' if plural else 'was'} set aside.")
    return ro, en
