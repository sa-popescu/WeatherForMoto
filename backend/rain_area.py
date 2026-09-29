"""
How much of the area around you actually gets the rain.

"Pe alocuri" is a statement about space, not about confidence, and until now
the app could only guess it from a probability. A 40% chance over a city can
mean one shower crossing a third of it, or a front soaking all of it: the
first you ride around, the second you wait out.

So the hours ahead are sampled on a ring around the location as well as at the
centre, in one Open-Meteo request, and each hour gets the share of those points
that see rain. The functions here are pure; weather_service does the fetching.
"""

from __future__ import annotations

import math
from typing import Any

# Far enough that a single shower does not cover the whole ring, close enough
# that the ring is still the ride you are about to take.
RING_KM = 15.0
RING_POINTS = 8
# The bar a point has to clear to count as wet (the same one rain_fusion uses).
WET_MM = 0.1

# Coverage bands. Below the first, the rain is somebody else's; above the
# second, it is everybody's.
ISOLATED_MAX = 0.25
SCATTERED_MAX = 0.60

EXTENT_ISOLATED = "isolated"
EXTENT_SCATTERED = "scattered"
EXTENT_WIDESPREAD = "widespread"

_KM_PER_DEGREE = 111.0


def ring_points(
    lat: float, lon: float, radius_km: float = RING_KM, count: int = RING_POINTS
) -> list[tuple[float, float]]:
    """The centre, then `count` points evenly around it at `radius_km`."""
    points = [(round(lat, 4), round(lon, 4))]
    # Longitude degrees shrink towards the poles; at 45° one is ~79 km.
    lon_km = _KM_PER_DEGREE * max(0.1, math.cos(math.radians(lat)))
    for index in range(count):
        bearing = 2 * math.pi * index / count
        points.append((
            round(lat + (radius_km / _KM_PER_DEGREE) * math.cos(bearing), 4),
            round(lon + (radius_km / lon_km) * math.sin(bearing), 4),
        ))
    return points


def _series(entry: Any) -> tuple[list[str], list[float | None]]:
    hourly = entry.get("hourly") if isinstance(entry, dict) else None
    if not isinstance(hourly, dict):
        return [], []
    times = [str(t) for t in hourly.get("time") or []]
    values = [
        float(v) if isinstance(v, (int, float)) else None
        for v in hourly.get("precipitation") or []
    ]
    return times, values


def coverage_by_time(raw: Any) -> dict[str, float]:
    """Share of the sampled points that see rain, per hour.

    Open-Meteo answers a list of coordinates with a list of objects (or a bare
    object for one). Points that came back short are simply not counted, so a
    partial answer gives a coverage over the points that did answer rather than
    a made-up zero.
    """
    entries = raw if isinstance(raw, list) else [raw] if isinstance(raw, dict) else []
    wet: dict[str, int] = {}
    seen: dict[str, int] = {}
    for entry in entries:
        times, values = _series(entry)
        for index, time in enumerate(times):
            value = values[index] if index < len(values) else None
            if value is None:
                continue
            seen[time] = seen.get(time, 0) + 1
            if value >= WET_MM:
                wet[time] = wet.get(time, 0) + 1
    return {
        time: round(wet.get(time, 0) / count, 3)
        for time, count in seen.items()
        if count > 0
    }


def extent(coverage: float | None) -> str | None:
    """Name the coverage: isolated, scattered or widespread."""
    if coverage is None:
        return None
    if coverage <= ISOLATED_MAX:
        return EXTENT_ISOLATED
    if coverage <= SCATTERED_MAX:
        return EXTENT_SCATTERED
    return EXTENT_WIDESPREAD
