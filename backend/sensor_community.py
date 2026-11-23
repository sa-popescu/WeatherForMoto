"""Sensor.Community: the citizens' sensor network, for the air as measured now.

Thousands of home-built stations report every few minutes, many of them in
Romanian towns, with a temperature, humidity and pressure sensor (BME280,
DHT22 and the like) next to the particulate one. The data is open, needs no
key, and comes back for an area: every reading of the last five minutes.

The sensors are home made and placed where their owners could: some sit in
the sun, some by a warm wall, a few indoors (one read 20 °C on a 3 °C night
when this was written). So no single sensor is trusted: indoor ones are
dropped, the rest of the area is summed up by its median, readings far from
that median are left out, and an area with fewer than MIN_SENSORS sensors
gives nothing. What comes out joins the blend of the current conditions with
a weight below the official and the Netatmo stations (weather_service.py).
"""

from __future__ import annotations

import logging
import statistics
from datetime import datetime, timezone
from typing import Any, Iterable

import httpx

logger = logging.getLogger("weatherformoto.sensor_community")

AREA_URL = "https://data.sensor.community/airrohr/v1/filter/area={lat:.3f},{lon:.3f},{km:g}"
# The API asks clients to say who they are.
USER_AGENT = "Motonor/1.0 (+https://motonor.bluemouse.cc)"
TIMEOUT_S = 6.0

# How far around the point sensors count, km.
RADIUS_KM = 6
# Fewer sensors than this in the area is not enough to outvote a bad one.
MIN_SENSORS = 3
# A reading this far from the area's median is a sensor in the sun, by a wall or indoors.
TEMP_OUTLIER_C = 3.0
HUMIDITY_OUTLIER_PCT = 15.0
PRESSURE_OUTLIER_HPA = 4.0
# Readings older than this are not "now" (the API gives the last five minutes).
MAX_AGE_S = 15 * 60
# Values no sensor outdoors in Romania should report.
TEMP_RANGE_C = (-40.0, 50.0)
PRESSURE_RANGE_HPA = (900.0, 1080.0)


async def fetch_area(lat: float, lon: float, client: httpx.AsyncClient) -> list[dict[str, Any]] | None:
    """Every reading of the last minutes around the point; None when the service does not answer."""
    url = AREA_URL.format(lat=lat, lon=lon, km=RADIUS_KM)
    try:
        response = await client.get(url, headers={"User-Agent": USER_AGENT}, timeout=TIMEOUT_S)
        response.raise_for_status()
        body = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("Sensor.Community fetch failed: %s", type(exc).__name__)
        return None
    return body if isinstance(body, list) else None


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    from math import asin, cos, radians, sin, sqrt

    dlat, dlon = radians(lat2 - lat1), radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * 6371.0 * asin(sqrt(a))


def _number(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if number == number else None  # NaN is not a reading


def _parse_time(text: Any) -> datetime | None:
    """The API's "2026-11-23 21:30:12", which is UTC."""
    try:
        return datetime.strptime(str(text), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        return None


def _agreeing(values: list[float], spread: float) -> list[float]:
    """The values within ``spread`` of their median: the sensors that agree with the area."""
    middle = statistics.median(values)
    return [v for v in values if abs(v - middle) <= spread]


def normalize(
    records: Iterable[dict[str, Any]] | None, lat: float, lon: float, now: datetime | None = None
) -> dict[str, Any] | None:
    """
    The area's temperature, humidity and sea level pressure, each the median
    of the sensors that agree, with how many sensors that is, the nearest
    one's distance and the newest reading's time; None when too few sensors
    measure the temperature.
    """
    if not records:
        return None
    moment = now or datetime.now(timezone.utc)
    # The newest reading of each sensor.
    latest: dict[Any, dict[str, Any]] = {}
    for record in records:
        if not isinstance(record, dict):
            continue
        location = record.get("location") or {}
        if str(location.get("indoor", "0")) == "1":
            continue
        observed = _parse_time(record.get("timestamp"))
        if observed is None or (moment - observed).total_seconds() > MAX_AGE_S:
            continue
        sensor_id = (record.get("sensor") or {}).get("id")
        previous = latest.get(sensor_id)
        if previous is None or observed > previous["observed"]:
            latest[sensor_id] = {"record": record, "observed": observed, "location": location}

    temps: list[float] = []
    humidities: list[float] = []
    pressures: list[float] = []
    nearest: float | None = None
    newest: datetime | None = None
    for item in latest.values():
        values = {v.get("value_type"): _number(v.get("value")) for v in item["record"].get("sensordatavalues") or []}
        temp = values.get("temperature")
        if temp is None or not TEMP_RANGE_C[0] <= temp <= TEMP_RANGE_C[1]:
            continue
        temps.append(temp)
        humidity = values.get("humidity")
        if humidity is not None and 0 < humidity <= 100:
            humidities.append(humidity)
        sea_level = values.get("pressure_at_sealevel")
        if sea_level is not None and PRESSURE_RANGE_HPA[0] <= sea_level / 100 <= PRESSURE_RANGE_HPA[1]:
            pressures.append(sea_level / 100)
        s_lat, s_lon = _number(item["location"].get("latitude")), _number(item["location"].get("longitude"))
        if s_lat is not None and s_lon is not None:
            km = _distance_km(lat, lon, s_lat, s_lon)
            nearest = km if nearest is None else min(nearest, km)
        newest = item["observed"] if newest is None else max(newest, item["observed"])

    if len(temps) < MIN_SENSORS:
        return None
    agreeing = _agreeing(temps, TEMP_OUTLIER_C)
    if len(agreeing) < MIN_SENSORS:
        return None

    def summed(values: list[float], spread: float) -> float | None:
        if len(values) < MIN_SENSORS:
            return None
        kept = _agreeing(values, spread)
        return round(statistics.median(kept), 1) if len(kept) >= MIN_SENSORS else None

    return {
        "temp": round(statistics.median(agreeing), 1),
        "humidity": summed(humidities, HUMIDITY_OUTLIER_PCT),
        "pressure": summed(pressures, PRESSURE_OUTLIER_HPA),
        "sensors": len(agreeing),
        "distance_km": round(nearest, 1) if nearest is not None else None,
        "observed_at": newest.isoformat() if newest else None,
    }
