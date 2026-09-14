"""
Official weather warnings from Meteoalarm, the European aggregator that
republishes what each national service (ANM for Romania) issues.

The feed is CAP (Common Alerting Protocol), either as a CAP document or as an
Atom feed whose entries carry the CAP fields. Everything here is tolerant: an
alert we cannot read is skipped rather than failing the request, because a
warning feed must never be able to take the weather down with it.

No API key and no account: the feeds are public.
"""

from __future__ import annotations

import logging
import math
import os
import unicodedata
from datetime import datetime, timezone
from typing import Any, Iterable
from xml.etree import ElementTree

import httpx

logger = logging.getLogger("weatherformoto.meteoalarm")

# Romania's feed by default; another country is one environment variable away.
DEFAULT_FEED_URL = "https://feeds.meteoalarm.org/feeds/meteoalarm-legacy-atom-romania"

# A warning feed is small. Anything larger is refused unread, so a broken or
# hostile response cannot be parsed into memory.
MAX_FEED_BYTES = 2_000_000

FETCH_TIMEOUT_S = 6.0

# Meteoalarm's colours, worst first. The number in awareness_level is what the
# feed actually carries ("2; yellow; Moderate").
LEVEL_BY_NUMBER: dict[str, str] = {"1": "green", "2": "yellow", "3": "orange", "4": "red"}
LEVEL_ORDER: dict[str, int] = {"red": 0, "orange": 1, "yellow": 2, "green": 3}

# CAP severity, used when the awareness level is missing.
SEVERITY_TO_LEVEL: dict[str, str] = {
    "Extreme": "red",
    "Severe": "orange",
    "Moderate": "yellow",
    "Minor": "green",
}

# awareness_type ("1; Wind") mapped to the words the app already uses.
AWARENESS_TYPES: dict[str, str] = {
    "1": "wind",
    "2": "snow-ice",
    "3": "thunderstorm",
    "4": "fog",
    "5": "high-temperature",
    "6": "low-temperature",
    "7": "coastal-event",
    "8": "forest-fire",
    "9": "avalanche",
    "10": "rain",
    "11": "flood",
    "12": "rain-flood",
    "13": "wildfire",
}

PREFERRED_LANGUAGES = ("ro", "en")


def _local(tag: str) -> str:
    """Tag name without its namespace, so CAP 1.1 and 1.2 read the same."""
    return tag.rsplit("}", 1)[-1] if "}" in tag else tag


def _children(node: ElementTree.Element, name: str) -> list[ElementTree.Element]:
    return [child for child in node if _local(child.tag) == name]


def _text(node: ElementTree.Element, name: str) -> str | None:
    for child in _children(node, name):
        if child.text and child.text.strip():
            return child.text.strip()
    return None


def fold(text: str | None) -> str:
    """Lowercase, without diacritics: 'Brașov' and 'Brasov' compare equal."""
    if not text:
        return ""
    stripped = unicodedata.normalize("NFKD", text)
    return "".join(c for c in stripped if not unicodedata.combining(c)).lower().strip()


def _parameters(info: ElementTree.Element) -> dict[str, str]:
    """CAP <parameter> pairs, keyed by valueName."""
    out: dict[str, str] = {}
    for parameter in _children(info, "parameter"):
        name = _text(parameter, "valueName")
        value = _text(parameter, "value")
        if name and value:
            out[name.strip()] = value.strip()
    return out


def _level_of(info: ElementTree.Element, params: dict[str, str]) -> str:
    """Meteoalarm's colour for this warning."""
    raw = params.get("awareness_level", "")
    for part in raw.split(";"):
        token = part.strip()
        if token in LEVEL_BY_NUMBER:
            return LEVEL_BY_NUMBER[token]
        if token.lower() in LEVEL_ORDER:
            return token.lower()
    return SEVERITY_TO_LEVEL.get(_text(info, "severity") or "", "yellow")


def _awareness_type(params: dict[str, str]) -> str | None:
    raw = params.get("awareness_type", "")
    head = raw.split(";", 1)[0].strip()
    if head in AWARENESS_TYPES:
        return AWARENESS_TYPES[head]
    label = fold(raw.split(";", 1)[-1])
    return label or None


def _parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _polygons(area: ElementTree.Element) -> list[list[tuple[float, float]]]:
    """CAP polygons: 'lat,lon lat,lon ...', first point repeated at the end."""
    polygons: list[list[tuple[float, float]]] = []
    for raw in _children(area, "polygon"):
        points: list[tuple[float, float]] = []
        for pair in (raw.text or "").split():
            lat_text, _, lon_text = pair.partition(",")
            try:
                points.append((float(lat_text), float(lon_text)))
            except ValueError:
                points = []
                break
        if len(points) >= 3:
            polygons.append(points)
    return polygons


def _circles(area: ElementTree.Element) -> list[tuple[float, float, float]]:
    """CAP circles: 'lat,lon radius-in-km'."""
    circles: list[tuple[float, float, float]] = []
    for raw in _children(area, "circle"):
        body = (raw.text or "").strip().split()
        if len(body) != 2:
            continue
        lat_text, _, lon_text = body[0].partition(",")
        try:
            circles.append((float(lat_text), float(lon_text), float(body[1])))
        except ValueError:
            continue
    return circles


def point_in_polygon(lat: float, lon: float, polygon: list[tuple[float, float]]) -> bool:
    """Ray casting; the polygon is a list of (lat, lon) points."""
    inside = False
    count = len(polygon)
    for i in range(count):
        lat_i, lon_i = polygon[i]
        lat_j, lon_j = polygon[(i - 1) % count]
        intersects = (lat_i > lat) != (lat_j > lat)
        if intersects and lon < (lon_j - lon_i) * (lat - lat_i) / (lat_j - lat_i) + lon_i:
            inside = not inside
    return inside


def _distance_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    rad = math.pi / 180
    d_lat = (lat2 - lat1) * rad
    d_lon = (lon2 - lon1) * rad
    h = math.sin(d_lat / 2) ** 2 + math.cos(lat1 * rad) * math.cos(lat2 * rad) * math.sin(d_lon / 2) ** 2
    return 2 * 6371 * math.asin(min(1.0, math.sqrt(h)))


def area_match(areas: list[dict[str, Any]], lat: float, lon: float, place_name: str) -> str | None:
    """
    How this warning reaches the rider: "geometry" when the feed drew the area
    and the point falls inside it, "name" when it only names places and one of
    them is where we are looking, None when it is for somewhere else.
    """
    place = fold(place_name)
    for area in areas:
        for polygon in area["polygons"]:
            if point_in_polygon(lat, lon, polygon):
                return "geometry"
        for c_lat, c_lon, radius in area["circles"]:
            if _distance_km(lat, lon, c_lat, c_lon) <= radius:
                return "geometry"
    if not place:
        return None
    for area in areas:
        described = fold(area["description"])
        if described and (place in described or described in place):
            return "name"
    return None


def _areas(info: ElementTree.Element) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for area in _children(info, "area"):
        out.append({
            "description": _text(area, "areaDesc") or "",
            "polygons": _polygons(area),
            "circles": _circles(area),
        })
    return out


def _infos(alert: ElementTree.Element) -> list[ElementTree.Element]:
    """Info blocks in the language we want, falling back to whatever is there."""
    infos = _children(alert, "info")
    for language in PREFERRED_LANGUAGES:
        picked = [info for info in infos if fold(_text(info, "language")).startswith(language)]
        if picked:
            return picked
    return infos


def _alert_elements(root: ElementTree.Element) -> Iterable[ElementTree.Element]:
    """The CAP alerts in the document, whether it is CAP or an Atom feed."""
    if _local(root.tag) == "alert":
        yield root
        return
    for entry in _children(root, "entry"):
        for alert in _children(entry, "alert"):
            yield alert
        # Legacy Meteoalarm entries carry the CAP fields directly on the entry.
        if _children(entry, "info"):
            yield entry
    for alert in _children(root, "alert"):
        yield alert


def parse_feed(xml_text: str) -> list[dict[str, Any]]:
    """Every readable warning in the document; unreadable ones are skipped."""
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        logger.warning("Meteoalarm feed is not valid XML: %s", exc)
        return []

    warnings: list[dict[str, Any]] = []
    for alert in _alert_elements(root):
        identifier = _text(alert, "identifier") or _text(alert, "id") or ""
        for info in _infos(alert):
            params = _parameters(info)
            areas = _areas(info)
            warnings.append({
                "id": f"{identifier}:{_text(info, 'event') or ''}:{_text(info, 'onset') or ''}",
                "event": _text(info, "event"),
                "headline": _text(info, "headline"),
                "description": _text(info, "description"),
                "instruction": _text(info, "instruction"),
                "level": _level_of(info, params),
                "awareness_type": _awareness_type(params),
                "sender": _text(info, "senderName") or _text(alert, "sender"),
                "onset": _text(info, "onset") or _text(info, "effective"),
                "expires": _text(info, "expires"),
                "areas": areas,
            })
    return warnings


def _is_current(warning: dict[str, Any], now: datetime) -> bool:
    """Expired warnings are dropped; ones that start later are kept."""
    expires = _parse_time(warning.get("expires"))
    return expires is None or expires > now


def _public(warning: dict[str, Any], match: str) -> dict[str, Any]:
    """The shape the API returns: no geometry, just what a rider needs to read."""
    return {
        "id": warning["id"],
        "event": warning["event"],
        "headline": warning["headline"],
        "description": warning["description"],
        "instruction": warning["instruction"],
        "level": warning["level"],
        "awareness_type": warning["awareness_type"],
        "sender": warning["sender"],
        "onset": warning["onset"],
        "expires": warning["expires"],
        "areas": [area["description"] for area in warning["areas"] if area["description"]],
        "match": match,
    }


def warnings_for(
    xml_text: str, lat: float, lon: float, place_name: str, now: datetime | None = None
) -> list[dict[str, Any]]:
    """Current warnings covering this point, worst colour first."""
    moment = now or datetime.now(timezone.utc)
    found: list[dict[str, Any]] = []
    seen: set[str] = set()
    for warning in parse_feed(xml_text):
        if not _is_current(warning, moment):
            continue
        match = area_match(warning["areas"], lat, lon, place_name)
        if match is None or warning["id"] in seen:
            continue
        seen.add(warning["id"])
        found.append(_public(warning, match))
    found.sort(key=lambda w: (LEVEL_ORDER.get(w["level"], 9), w["onset"] or ""))
    return found


def feed_url() -> str:
    return os.getenv("METEOALARM_FEED_URL", DEFAULT_FEED_URL).strip() or DEFAULT_FEED_URL


async def fetch_feed(client: httpx.AsyncClient, user_agent: str) -> str | None:
    """The raw feed, or None when it is unreachable, too big or an error."""
    url = feed_url()
    try:
        response = await client.get(
            url,
            timeout=FETCH_TIMEOUT_S,
            # The feed answers 406 to any narrower Accept, even application/atom+xml (September 2026).
            headers={"User-Agent": user_agent, "Accept": "*/*"},
        )
        response.raise_for_status()
        body = response.content
        if len(body) > MAX_FEED_BYTES:
            logger.warning("Meteoalarm feed is %d bytes, over the %d cap", len(body), MAX_FEED_BYTES)
            return None
        return body.decode(response.encoding or "utf-8", errors="replace")
    except (httpx.HTTPError, ValueError, UnicodeDecodeError) as exc:
        logger.warning("Meteoalarm fetch failed: %s", exc)
        return None
