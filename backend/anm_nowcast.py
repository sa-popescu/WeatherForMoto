"""
ANM nowcasting warnings: the short warnings (30 to 90 minutes) the Romanian
weather service issues for storms, downpours, hail and strong wind, with the
area drawn as a polygon. Meteoalarm carries the longer county warnings but not
these. Public XML on meteoromania.ro, no key; shown with credit to ANM.

The shape returned is the one meteoalarm.warnings_for returns, so the app
shows both the same way.
"""

from __future__ import annotations

import logging
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any
from xml.etree import ElementTree

import httpx

import meteoalarm

logger = logging.getLogger("weatherformoto.anm_nowcast")

FEED_URL = "https://www.meteoromania.ro/avertizari-nowcasting-xml-gis.php"
ATTRIBUTION_URL = "https://www.meteoromania.ro/"
FETCH_TIMEOUT_S = 6.0
MAX_FEED_BYTES = 2_000_000

LEVELS = {"galben": "yellow", "portocaliu": "orange", "rosu": "red"}

# Words in the warned phenomenon, in the order that decides the type.
AWARENESS_WORDS: tuple[tuple[str, str], ...] = (
    ("grindina", "thunderstorm"),
    ("descarcari electrice", "thunderstorm"),
    ("vijelie", "wind"),
    ("intensificari", "wind"),
    ("vant", "wind"),
    ("ceata", "fog"),
    ("polei", "snow-ice"),
    ("ninsoare", "snow-ice"),
    ("averse", "rain"),
    ("ploaie", "rain"),
    ("cantitati de apa", "rain"),
)

_HALF_WORLD_M = 20037508.342789244
_RING_RE = re.compile(r"\(\(\s*([^()]+?)\s*\)")


def _romania_offset(moment_utc: datetime) -> timedelta:
    """EET/EEST without a time zone database: summer time from the last Sunday
    of March to the last Sunday of October, both at 01:00 UTC."""

    def last_sunday(year: int, month: int) -> datetime:
        day = datetime(year, month + 1, 1, 1, tzinfo=timezone.utc) - timedelta(days=1)
        return day - timedelta(days=(day.weekday() + 1) % 7)

    start = last_sunday(moment_utc.year, 3)
    end = last_sunday(moment_utc.year, 10)
    return timedelta(hours=3) if start <= moment_utc < end else timedelta(hours=2)


def local_time(text: str | None) -> datetime | None:
    """"2026-09-14 10:35" in Romanian time, as an aware datetime."""
    if not text:
        return None
    try:
        naive = datetime.strptime(text.strip(), "%Y-%m-%d %H:%M")
    except ValueError:
        return None
    # The offset of the local time is the offset of (local - 2 h) in UTC, off by at most the hour of the change.
    guess = naive.replace(tzinfo=timezone.utc) - timedelta(hours=2)
    offset = _romania_offset(guess)
    return naive.replace(tzinfo=timezone(offset))


def _to_lat_lon(x: float, y: float) -> tuple[float, float]:
    lon = x / _HALF_WORLD_M * 180
    lat = math.degrees(2 * math.atan(math.exp(y / _HALF_WORLD_M * math.pi)) - math.pi / 2)
    return lat, lon


def polygons_from_wkt(wkt: str | None, srid: str | None) -> list[list[tuple[float, float]]]:
    """Outer rings of a WKT (MULTI)POLYGON as (lat, lon) lists; holes are ignored,
    which can only make a warning reach a little further, never miss."""
    if not wkt:
        return []
    mercator = (srid or "").strip() == "3857"
    polygons: list[list[tuple[float, float]]] = []
    for ring_text in _RING_RE.findall(wkt):
        points: list[tuple[float, float]] = []
        for pair in ring_text.split(","):
            parts = pair.split()
            if len(parts) < 2:
                continue
            try:
                x, y = float(parts[0]), float(parts[1])
            except ValueError:
                continue
            points.append(_to_lat_lon(x, y) if mercator else (y, x))
        if len(points) >= 3:
            polygons.append(points)
    return polygons


def _awareness(text: str) -> str | None:
    folded = meteoalarm.fold(text)
    for word, kind in AWARENESS_WORDS:
        if word in folded:
            return kind
    return None


def parse_feed(xml_text: str) -> list[dict[str, Any]]:
    """Every readable warning; the document holds one <avertizare> or a list of them."""
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        logger.warning("ANM nowcasting feed is not valid XML: %s", exc)
        return []
    elements = [root] if root.tag == "avertizare" else list(root.iter("avertizare"))
    warnings: list[dict[str, Any]] = []
    for element in elements:
        attrs = element.attrib
        onset = local_time(attrs.get("dataInceput"))
        expires = local_time(attrs.get("dataSfarsit"))
        phenomenon = (attrs.get("fenomenAvertizat") or "").strip()
        associated = (attrs.get("fenomeneAsociate") or "").strip()
        level = LEVELS.get(meteoalarm.fold(attrs.get("avertizareNivelDenumire")), "yellow")
        zones = [z.attrib.get("text", "").strip().rstrip(";") for z in element.iter("zona_afectata")]
        polygons = polygons_from_wkt(attrs.get("coordsGis"), attrs.get("srid")) if attrs.get("useCoordsGis") == "true" else []
        warnings.append({
            "id": f"anm-nowcast:{attrs.get('avertizareId') or attrs.get('numarAvertizare') or len(warnings)}",
            "event": "Avertizare nowcasting ANM",
            "headline": phenomenon or None,
            "description": associated or None,
            "instruction": None,
            "level": level,
            "awareness_type": _awareness(f"{phenomenon} {associated}"),
            "sender": attrs.get("entitateOrganizatorica") or "ANM",
            "onset": onset.isoformat() if onset else None,
            "expires": expires.isoformat() if expires else None,
            # The meteoalarm matcher reads areas as polygons, circles and a description.
            "areas": [{"description": zone, "polygons": polygons if i == 0 else [], "circles": []} for i, zone in enumerate(zones or [""])],
        })
    return warnings


def warnings_for(xml_text: str, lat: float, lon: float, place_name: str, now: datetime | None = None) -> list[dict[str, Any]]:
    """Current nowcasting warnings covering this point, in the meteoalarm shape."""
    moment = now or datetime.now(timezone.utc)
    found: list[dict[str, Any]] = []
    for warning in parse_feed(xml_text):
        expires = datetime.fromisoformat(warning["expires"]) if warning["expires"] else None
        if expires is not None and expires <= moment:
            continue
        drawn = [polygon for area in warning["areas"] for polygon in area["polygons"]]
        if drawn:
            # A drawn area is exact: a place that merely shares the county name is left out.
            match = "geometry" if any(meteoalarm.point_in_polygon(lat, lon, p) for p in drawn) else None
        else:
            match = meteoalarm.area_match(warning["areas"], lat, lon, place_name)
        if match is None:
            continue
        public = dict(warning)
        public["areas"] = [area["description"] for area in warning["areas"] if area["description"]]
        public["match"] = match
        found.append(public)
    return found


async def fetch_feed(client: httpx.AsyncClient, user_agent: str) -> str | None:
    """The raw XML, or None when unreachable, too big or an error."""
    try:
        response = await client.get(FEED_URL, timeout=FETCH_TIMEOUT_S,
                                    # A broad Accept: Meteoalarm showed a narrow one can be refused with 406.
                                    headers={"User-Agent": user_agent, "Accept": "*/*"})
        response.raise_for_status()
        body = response.content
        if len(body) > MAX_FEED_BYTES:
            logger.warning("ANM nowcasting feed is %d bytes, over the cap", len(body))
            return None
        return body.decode(response.encoding or "utf-8", errors="replace")
    except (httpx.HTTPError, ValueError, UnicodeDecodeError) as exc:
        logger.warning("ANM nowcasting fetch failed: %s", exc)
        return None
