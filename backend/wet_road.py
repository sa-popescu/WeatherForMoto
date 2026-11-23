"""Whether the road is still wet after rain, and whether rain is the first after dry days.

Two things every rider knows and an hourly forecast misses, because each hour
is scored on its own:

- after the rain stops the road stays wet for a while, longer when it is cold,
  damp, dark, overcast or still, shorter in sun and wind. The model here keeps
  a "wetness" that rain sets to full and every dry hour lowers by one share of
  that hour's drying time; while enough of it is left, the hour is "wet";
- the first rain after several dry days lifts the oil, rubber and dust the dry
  days left on the asphalt, so the first hours of it are slipperier than rain
  usually is: those hours are "first_rain".

Both are estimates from the forecast and from the rain of the past days, not
measurements, and the score says so in its detail. Pure functions: the inputs
are the hourly items weather_service builds and the past hours' rain.
"""

from __future__ import annotations

from typing import Any, Iterable

# An hour with at least this much rain (mm) wets the road.
WET_RAIN_MM = 0.2
# Hours a wet road takes to dry in mild, ordinary weather.
BASE_DRYING_HOURS = 2.0
# What makes it slower (> 1) or faster (< 1). Applied together, then kept in bounds.
COLD_BELOW_C, COLD_FACTOR = 5.0, 2.0
COOL_BELOW_C, COOL_FACTOR = 10.0, 1.5
DAMP_FROM_PCT, DAMP_FACTOR = 85.0, 1.5
NIGHT_FACTOR = 1.5
OVERCAST_FROM_PCT, OVERCAST_FACTOR = 80.0, 1.25
SUNNY_BELOW_PCT, SUNNY_FACTOR = 30.0, 0.6
WINDY_FROM_KMH, WINDY_FACTOR = 20.0, 0.75
MIN_DRYING_HOURS, MAX_DRYING_HOURS = 1.0, 8.0
# Below this much wetness left, the road counts as dry.
MIN_WETNESS = 0.2
# A dry spell this long (hours without rain) makes the next rain "first rain".
FIRST_RAIN_DRY_HOURS = 120
# The first rain is slippery for this many rainy hours.
FIRST_RAIN_HOURS = 2
# Rain from the past days this far back is asked for (weather_service), enough for a dry spell.
PAST_DAYS = 5

# What each costs on the moto score (no cap: they add to the rain, never replace it).
WET_ROAD_PENALTY = 8
FIRST_RAIN_PENALTY = 8

WET = "wet"
FIRST_RAIN = "first_rain"


def drying_hours(
    temperature: float | None,
    humidity: float | None,
    wind_kmh: float | None,
    cloud_cover: float | None,
    is_day: bool | None,
) -> float:
    """How many hours a wet road needs to dry in this hour's weather. Unknown values change nothing."""
    hours = BASE_DRYING_HOURS
    if temperature is not None:
        if temperature < COLD_BELOW_C:
            hours *= COLD_FACTOR
        elif temperature < COOL_BELOW_C:
            hours *= COOL_FACTOR
    if humidity is not None and humidity >= DAMP_FROM_PCT:
        hours *= DAMP_FACTOR
    if is_day is False:
        hours *= NIGHT_FACTOR
    elif cloud_cover is not None:
        if cloud_cover >= OVERCAST_FROM_PCT:
            hours *= OVERCAST_FACTOR
        elif cloud_cover < SUNNY_BELOW_PCT:
            hours *= SUNNY_FACTOR
    if wind_kmh is not None and wind_kmh >= WINDY_FROM_KMH:
        hours *= WINDY_FACTOR
    return max(MIN_DRYING_HOURS, min(MAX_DRYING_HOURS, hours))


def _rains(mm: float | None) -> bool:
    return mm is not None and mm >= WET_RAIN_MM


def road_states(hours: list[dict[str, Any]], past_rain: Iterable[float | None] | None) -> list[str | None]:
    """
    The state of the road for each hourly item, in order: "wet", "first_rain"
    or None. ``past_rain`` is the rain (mm) of the hours just before the
    first item, oldest first; None when it could not be fetched, and then a
    dry spell cannot be told, so no hour is called a first rain, and the
    road is taken as dry at the start.
    """
    wetness = 0.0
    known = past_rain is not None
    dry_run = 0
    for mm in past_rain or []:
        if _rains(mm):
            wetness, dry_run = 1.0, 0
        else:
            dry_run += 1
            wetness = max(0.0, wetness - 1.0 / BASE_DRYING_HOURS)
    first_left = 0
    states: list[str | None] = []
    for hour in hours:
        if _rains(hour.get("precipitation_mm")):
            if known and dry_run >= FIRST_RAIN_DRY_HOURS:
                first_left = FIRST_RAIN_HOURS
            states.append(FIRST_RAIN if first_left > 0 else None)
            first_left = max(0, first_left - 1)
            wetness, dry_run = 1.0, 0
            known = True
            continue
        first_left = 0
        dry_run += 1
        if wetness > 0:
            wetness = max(0.0, wetness - 1.0 / drying_hours(
                hour.get("temperature"), hour.get("relative_humidity"), hour.get("wind_speed_kmh"),
                hour.get("cloud_cover"), hour.get("is_day"),
            ))
        states.append(WET if wetness >= MIN_WETNESS else None)
    return states


def current_state(hour_state: str | None, measured_mm: float | None) -> str | None:
    """
    The state for the measured "now": a drying road that the sources say is
    being rained on now is just rain, and a first rain that is not falling
    now is not one yet.
    """
    if hour_state == WET and _rains(measured_mm):
        return None
    if hour_state == FIRST_RAIN and measured_mm is not None and not _rains(measured_mm):
        return None
    return hour_state


def penalty_of(state: str | None) -> int:
    return WET_ROAD_PENALTY if state == WET else FIRST_RAIN_PENALTY if state == FIRST_RAIN else 0


def metadata() -> dict[str, Any]:
    """The rules, for GET /meta/scoring."""
    return {
        "wet_rain_mm": WET_RAIN_MM,
        "base_drying_hours": BASE_DRYING_HOURS,
        "drying_hours_bounds": [MIN_DRYING_HOURS, MAX_DRYING_HOURS],
        "factors": {
            "cold": {"below_c": COLD_BELOW_C, "times": COLD_FACTOR},
            "cool": {"below_c": COOL_BELOW_C, "times": COOL_FACTOR},
            "damp": {"from_pct": DAMP_FROM_PCT, "times": DAMP_FACTOR},
            "night": {"times": NIGHT_FACTOR},
            "overcast": {"from_pct": OVERCAST_FROM_PCT, "times": OVERCAST_FACTOR},
            "sunny": {"below_pct": SUNNY_BELOW_PCT, "times": SUNNY_FACTOR},
            "windy": {"from_kmh": WINDY_FROM_KMH, "times": WINDY_FACTOR},
        },
        "min_wetness": MIN_WETNESS,
        "wet_penalty": WET_ROAD_PENALTY,
        "first_rain": {"dry_hours": FIRST_RAIN_DRY_HOURS, "rainy_hours": FIRST_RAIN_HOURS, "penalty": FIRST_RAIN_PENALTY},
    }
