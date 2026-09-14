"""
Official weather stations through EUMETNET MeteoGate (E-SOH, an OGC EDR API):
the national services' own synoptic stations (ANM for Romania), with hourly
temperature, humidity, wind, measured gusts, rain over the last hour and sea
level pressure. Free, CC BY 4.0, no key.

For a point we ask for every station in a box around it, keep each station's
latest value per measurement, and take every field from the nearest station
that reports it fresh. Stations are data, so everything here is tolerant: a
coverage we cannot read is skipped.
"""

from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger("weatherformoto.stations")

AREA_URL = "https://observations.meteogate.eu/collections/observations/area"
LOCATIONS_URL = "https://observations.meteogate.eu/collections/observations/locations"
ATTRIBUTION_URL = "https://eumetnet.github.io/meteogate-documentation/"

# The box asked for: about 33 km each way, so the nearest station is in it.
SEARCH_HALF_DEG_LAT = 0.3
# A station further than this does not describe the rider's weather.
MAX_DISTANCE_KM = 30.0
# Observations arrive about 10 minutes after the hour; older than this is not "now".
MAX_AGE_S = 90 * 60
FETCH_TIMEOUT_S = 6.0
MAX_BYTES = 4_000_000

TEMPERATURE = "air_temperature:2.0:point:PT0S"
HUMIDITY = "relative_humidity:2.0:point:PT0S"
WIND = "wind_speed:10.0:point:PT10M"
WIND_DIRECTION = "wind_from_direction:10.0:point:PT10M"
GUSTS = ("wind_speed_of_gust:10.0:point:PT10M", "wind_speed_of_gust:10.0:point:PT1H")
PRESSURE_MSL = "air_pressure_at_mean_sea_level:0.0:point:PT0S"
# Rain gauges stand at different heights (1.4 m, 1.5 m...), so only the rest of the name is matched.
RAIN_PREFIX = "precipitation_amount:"
RAIN_1H_SUFFIX = ":sum:PT1H"


def search_box(lat: float, lon: float) -> tuple[float, float, float, float]:
    """(south, west, north, east) around the point, the same distance each way."""
    dlat = SEARCH_HALF_DEG_LAT
    dlon = dlat / max(0.3, math.cos(math.radians(lat)))
    return lat - dlat, lon - dlon, lat + dlat, lon + dlon


def _polygon(box: tuple[float, float, float, float]) -> str:
    s, w, n, e = box
    return f"POLYGON(({w:.4f} {s:.4f},{e:.4f} {s:.4f},{e:.4f} {n:.4f},{w:.4f} {n:.4f},{w:.4f} {s:.4f}))"


async def _get_json(client: httpx.AsyncClient, url: str, params: dict[str, str], user_agent: str) -> Any:
    response = await client.get(url, params=params, timeout=FETCH_TIMEOUT_S,
                                headers={"User-Agent": user_agent, "Accept": "application/json"})
    # The API answers 404 when the box holds no data at all: that is an empty answer, not a failure.
    if response.status_code == 404:
        return {}
    response.raise_for_status()
    if len(response.content) > MAX_BYTES:
        raise ValueError(f"station answer is {len(response.content)} bytes")
    return response.json()


async def fetch(lat: float, lon: float, client: httpx.AsyncClient, user_agent: str) -> dict[str, Any] | None:
    """Observations and station names around the point; None when the service fails."""
    box = search_box(lat, lon)
    s, w, n, e = box
    try:
        area, places = await asyncio.gather(
            _get_json(client, AREA_URL, {"coords": _polygon(box)}, user_agent),
            _get_json(client, LOCATIONS_URL, {"bbox": f"{w:.4f},{s:.4f},{e:.4f},{n:.4f}"}, user_agent),
        )
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("MeteoGate stations fetch failed: %s", exc)
        return None
    if not isinstance(area, dict) or not isinstance(places, dict):
        return None
    return {
        "ref_lat": lat,
        "ref_lon": lon,
        "coverages": area.get("coverages") or [],
        "locations": places.get("features") or [],
    }


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    rad = math.pi / 180
    d_lat = (lat2 - lat1) * rad
    d_lon = (lon2 - lon1) * rad
    h = math.sin(d_lat / 2) ** 2 + math.cos(lat1 * rad) * math.cos(lat2 * rad) * math.sin(d_lon / 2) ** 2
    return 2 * 6371 * math.asin(min(1.0, math.sqrt(h)))


def _parse_time(value: Any) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _station_names(features: list[Any]) -> dict[str, str]:
    names: dict[str, str] = {}
    for feature in features:
        if not isinstance(feature, dict):
            continue
        station_id = feature.get("id")
        name = (feature.get("properties") or {}).get("name")
        if isinstance(station_id, str) and isinstance(name, str):
            names[station_id] = name
    return names


def pretty_name(raw: str | None) -> str | None:
    """"BUCURESTI_BANEASA" as "Bucuresti Baneasa"."""
    if not raw:
        return None
    return " ".join(part.capitalize() for part in raw.replace("_", " ").split())


def _latest_by_station(coverages: list[Any]) -> dict[str, dict[str, Any]]:
    """Per station: position and, per measurement, its newest (time, value)."""
    stations: dict[str, dict[str, Any]] = {}
    for coverage in coverages:
        if not isinstance(coverage, dict):
            continue
        axes = ((coverage.get("domain") or {}).get("axes")) or {}
        try:
            lon = float(axes["x"]["values"][0])
            lat = float(axes["y"]["values"][0])
            times = list(axes["t"]["values"])
        except (KeyError, IndexError, TypeError, ValueError):
            continue
        station_id = coverage.get("metocean:wigosId") or f"{lon:.4f},{lat:.4f}"
        station = stations.setdefault(station_id, {"lat": lat, "lon": lon, "values": {}})
        for parameter, series in (coverage.get("ranges") or {}).items():
            values = (series or {}).get("values") or []
            for time_text, raw in reversed(list(zip(times, values))):
                value = _number(raw)
                moment = _parse_time(time_text)
                if value is None or moment is None:
                    continue
                known = station["values"].get(parameter)
                if known is None or moment > known[0]:
                    station["values"][parameter] = (moment, value)
                break
    return stations


def normalize(payload: dict[str, Any] | None, now: datetime | None = None) -> dict[str, Any] | None:
    """
    Current conditions from the stations, in the shape the other station
    sources use, plus which station answered. None without a fresh temperature
    within MAX_DISTANCE_KM.
    """
    if not payload:
        return None
    moment = now or datetime.now(timezone.utc)
    ref_lat = float(payload["ref_lat"])
    ref_lon = float(payload["ref_lon"])
    names = _station_names(payload.get("locations") or [])
    stations = _latest_by_station(payload.get("coverages") or [])

    nearby = []
    for station_id, station in stations.items():
        distance = _distance_km(ref_lat, ref_lon, station["lat"], station["lon"])
        if distance <= MAX_DISTANCE_KM:
            nearby.append((distance, station_id, station))
    nearby.sort(key=lambda item: item[0])

    def fresh(station: dict[str, Any], parameter: str) -> tuple[datetime, float] | None:
        found = station["values"].get(parameter)
        if found and (moment - found[0]).total_seconds() <= MAX_AGE_S:
            return found
        return None

    def nearest(*parameters: str, match_rain: bool = False) -> tuple[datetime, float] | None:
        for _, _, station in nearby:
            if match_rain:
                for parameter in station["values"]:
                    if parameter.startswith(RAIN_PREFIX) and parameter.endswith(RAIN_1H_SUFFIX):
                        found = fresh(station, parameter)
                        if found:
                            return found
                continue
            for parameter in parameters:
                found = fresh(station, parameter)
                if found:
                    return found
        return None

    primary = next(((d, sid, st) for d, sid, st in nearby if fresh(st, TEMPERATURE)), None)
    if primary is None:
        return None
    distance, station_id, station = primary
    observed_at, temperature = fresh(station, TEMPERATURE)  # type: ignore[misc]

    def value(found: tuple[datetime, float] | None, scale: float = 1.0) -> float | None:
        return round(found[1] * scale, 1) if found else None

    return {
        "temp": round(temperature, 1),
        "feels_like": None,
        "humidity": value(nearest(HUMIDITY)),
        "wind_speed_kmh": value(nearest(WIND), 3.6),
        "wind_gusts_kmh": value(nearest(*GUSTS), 3.6),
        "wind_dir": value(nearest(WIND_DIRECTION)),
        "pressure": value(nearest(PRESSURE_MSL)),
        "precipitation": value(nearest(match_rain=True)),
        "wmo_code": None,
        "station": pretty_name(names.get(station_id)) or station_id,
        "station_id": station_id,
        "distance_km": round(distance, 1),
        "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
    }
