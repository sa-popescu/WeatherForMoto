"""
Airport observations (METAR) for Romania from NOAA's Aviation Weather Center
data API: public domain, no key. They are the only source that reports what
is happening right now (rain, thunderstorm, fog) and the visibility, measured
by an observer or an automatic station every 30 minutes.

One request covers every Romanian airport; the nearest report within reach
of the rider is used, and only for present weather and visibility (the
official stations already cover temperature and wind).
"""

from __future__ import annotations

import logging
import math
from datetime import datetime, timezone
from typing import Any

import httpx

logger = logging.getLogger("motonor.metar")

API_URL = "https://aviationweather.gov/api/data/metar"
ATTRIBUTION_URL = "https://aviationweather.gov/"
# Romanian airports that publish METAR.
STATIONS = (
    "LROP", "LRBS", "LRCL", "LRTR", "LRIA", "LRCK", "LRTC", "LROD", "LRSM",
    "LRCV", "LRSB", "LRTM", "LRBV", "LRSV", "LRBC", "LRBM", "LRCS", "LRAR",
)
FETCH_TIMEOUT_S = 6.0
# Showers and fog are local: an airport further than this says little about the rider.
MAX_DISTANCE_KM = 20.0
MAX_AGE_S = 75 * 60
STATUTE_MILE_M = 1609.34


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    rad = math.pi / 180
    d_lat = (lat2 - lat1) * rad
    d_lon = (lon2 - lon1) * rad
    h = math.sin(d_lat / 2) ** 2 + math.cos(lat1 * rad) * math.cos(lat2 * rad) * math.sin(d_lon / 2) ** 2
    return 2 * 6371 * math.asin(min(1.0, math.sqrt(h)))


def visibility_m(value: Any) -> float | None:
    """AWC writes visibility in statute miles, "6+" for 6 or more (10 km and beyond)."""
    if value is None:
        return None
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("+"):
            return 10_000.0
        try:
            miles = float(text)
        except ValueError:
            return None
    else:
        try:
            miles = float(value)
        except (TypeError, ValueError):
            return None
    return round(min(10_000.0, miles * STATUTE_MILE_M))


def _priority(code: int) -> tuple[int, int]:
    """Thunderstorms first, then freezing precipitation, then the rest by code."""
    if code in (95, 96, 99):
        return (3, code)
    if code in (56, 57, 66, 67):
        return (2, code)
    return (1, code)


def weather_code(wx: str | None) -> int | None:
    """
    WMO code for METAR present weather ("-RA", "TSRA", "+SHRA", "FG"...), or
    None when nothing worth an icon is going on (mist, haze, no weather).
    With several phenomena, thunderstorms beat freezing precipitation, which
    beats everything else.
    """
    if not wx:
        return None
    groups = wx.upper().split()
    codes: list[int] = []
    for group in groups:
        heavy = group.startswith("+")
        light = group.startswith("-")
        body = group.lstrip("+-")
        if body.startswith("VC"):
            continue  # in the vicinity, not at the airport
        if "TS" in body:
            codes.append(99 if "GR" in body or "GS" in body else 95)
        elif body.startswith("FZ") and ("RA" in body or "DZ" in body):
            codes.append(67 if heavy else 66 if "RA" in body else 56)
        elif "SN" in body or "SG" in body:
            codes.append(86 if body.startswith("SH") and heavy else 85 if body.startswith("SH") else 75 if heavy else 71 if light else 73)
        elif "RA" in body:
            if body.startswith("SH"):
                codes.append(82 if heavy else 80 if light else 81)
            else:
                codes.append(65 if heavy else 61 if light else 63)
        elif "DZ" in body:
            codes.append(55 if heavy else 51 if light else 53)
        elif "FG" in body and "BC" not in body and "MI" not in body:
            codes.append(48 if body.startswith("FZ") else 45)
    return max(codes, key=_priority) if codes else None


def phase(wx: str | None) -> str | None:
    """What falls, by its phase: "rain", "snow", "mixed" (lapoviță), "freezing", or None.

    Thunderstorm groups count by what falls in them (TSRA is rain, TSSN snow).
    """
    if not wx:
        return None
    rain = snow = freezing = False
    for group in wx.upper().split():
        body = group.lstrip("+-")
        if body.startswith("VC"):
            continue
        if body.startswith("FZ") and ("RA" in body or "DZ" in body):
            freezing = True
            continue
        if "RA" in body or "DZ" in body:
            rain = True
        if "SN" in body or "SG" in body:
            snow = True
    if freezing:
        return "freezing"
    if rain and snow:
        return "mixed"
    return "snow" if snow else "rain" if rain else None


async def fetch(client: httpx.AsyncClient, user_agent: str) -> list[dict[str, Any]] | None:
    """The latest report of every station, or None when the service fails."""
    try:
        response = await client.get(
            API_URL,
            params={"ids": ",".join(STATIONS), "format": "json"},
            timeout=FETCH_TIMEOUT_S,
            headers={"User-Agent": user_agent, "Accept": "application/json"},
        )
        # 204: no reports right now, a valid empty answer.
        if response.status_code == 204:
            return []
        response.raise_for_status()
        data = response.json()
    except (httpx.HTTPError, ValueError) as exc:
        logger.warning("METAR fetch failed: %s", exc)
        return None
    return data if isinstance(data, list) else None


def _report_time(report: dict[str, Any]) -> datetime | None:
    text = report.get("reportTime")
    if isinstance(text, str):
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            pass
    obs = report.get("obsTime")
    if isinstance(obs, (int, float)):
        return datetime.fromtimestamp(obs, tz=timezone.utc)
    return None


# Cloud amounts in oktas as a fraction of the sky, and the reports of a sky without them.
_COVER_FRACTION = {"FEW": 0.19, "SCT": 0.44, "BKN": 0.75, "OVC": 1.0, "OVX": 1.0, "VV": 1.0}
_CLEAR_REPORTS = frozenset({"CAVOK", "CLR", "SKC", "NSC", "NCD"})
FEET_M = 0.3048
# The infrared a cloud sends back falls with its height (road_surface.CLOUD_WEIGHT_*).
_LOW_CLOUD_TOP_M, _MID_CLOUD_TOP_M = 2000.0, 6000.0


def sky_cover(report: dict[str, Any]) -> dict[str, Any] | None:
    """The cloud the report sees, as the road feels it in infrared (0..1), and how it was reported.

    Layers combine as independent covers, each weighted by its height (low
    cloud 1, middle 0.85, high 0.35). CAVOK and the "no cloud" reports see the
    sky up to about 1500 m (CAVOK) or a few km (automatic sensors): above that
    the models' high cloud still counts (weather_service). None when the
    report says nothing about the sky.
    """
    layers = report.get("clouds")
    cover = str(report.get("cover") or "").upper()
    parts: list[tuple[float, float]] = []
    if isinstance(layers, list):
        for layer in layers:
            if not isinstance(layer, dict):
                continue
            fraction = _COVER_FRACTION.get(str(layer.get("cover") or "").upper())
            if fraction is None:
                continue
            try:
                base_m = float(layer.get("base")) * FEET_M
            except (TypeError, ValueError):
                base_m = 0.0
            weight = 1.0 if base_m < _LOW_CLOUD_TOP_M else 0.85 if base_m < _MID_CLOUD_TOP_M else 0.35
            parts.append((fraction, weight))
    if not parts:
        if cover in _CLEAR_REPORTS:
            return {"cloud": 0.0, "sky": cover, "sees_high": False}
        fraction = _COVER_FRACTION.get(cover)
        if fraction is None:
            return None
        parts.append((fraction, 1.0))
    clear = 1.0
    for fraction, weight in parts:
        clear *= 1 - fraction * weight
    return {"cloud": round(1 - clear, 3), "sky": cover or None, "sees_high": True}


def _shape(report: dict[str, Any], distance: float, observed: datetime) -> dict[str, Any]:
    # AWC names end in county and country codes ("Bucharest/Băneasa Intl, B, RO").
    name = report.get("name")
    return {
        "station": report.get("icaoId"),
        "name": name.split(",")[0].strip() if isinstance(name, str) and name.strip() else None,
        "distance_km": round(distance, 1),
        "observed_at": observed.isoformat().replace("+00:00", "Z"),
        "wmo_code": weather_code(report.get("wxString")),
        "phase": phase(report.get("wxString")),
        "visibility_m": visibility_m(report.get("visib")),
        "sky": sky_cover(report),
        "raw": report.get("rawOb"),
    }


def within(reports: list[dict[str, Any]] | None, lat: float, lon: float, max_km: float,
           now: datetime | None = None) -> list[dict[str, Any]]:
    """Every fresh report within ``max_km``, nearest first (for what is falling, which spans tens of km)."""
    if not reports:
        return []
    moment = now or datetime.now(timezone.utc)
    found: list[dict[str, Any]] = []
    for report in reports:
        if not isinstance(report, dict):
            continue
        try:
            distance = _distance_km(lat, lon, float(report["lat"]), float(report["lon"]))
        except (KeyError, TypeError, ValueError):
            continue
        observed = _report_time(report)
        if distance > max_km or observed is None or (moment - observed).total_seconds() > MAX_AGE_S:
            continue
        found.append(_shape(report, distance, observed))
    return sorted(found, key=lambda r: r["distance_km"])


def nearest(reports: list[dict[str, Any]] | None, lat: float, lon: float, now: datetime | None = None,
            max_km: float = MAX_DISTANCE_KM) -> dict[str, Any] | None:
    """Present weather, visibility and sky from the nearest fresh report within ``max_km``."""
    if not reports:
        return None
    moment = now or datetime.now(timezone.utc)
    best: tuple[float, dict[str, Any], datetime] | None = None
    for report in reports:
        if not isinstance(report, dict):
            continue
        try:
            distance = _distance_km(lat, lon, float(report["lat"]), float(report["lon"]))
        except (KeyError, TypeError, ValueError):
            continue
        observed = _report_time(report)
        if distance > max_km or observed is None or (moment - observed).total_seconds() > MAX_AGE_S:
            continue
        if best is None or distance < best[0]:
            best = (distance, report, observed)
    if best is None:
        return None
    distance, report, observed = best
    return _shape(report, distance, observed)
