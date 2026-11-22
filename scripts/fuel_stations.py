"""Fuel stations from OpenStreetMap into the small file the app reads.

Input: the GeoJSON sequence `osmium export -f geojsonseq` writes for the
objects tagged amenity=fuel (points, and station areas drawn as polygons).
Output: app/public/data/fuel-stations.json,

    {"version": 1, "updated": "YYYY-MM-DD", "attribution": "...",
     "stations": [[lat, lon, "brand or name"], ...]}

sorted by latitude, coordinates to 5 decimals (about a metre). When the
stations are the same as in the file already there, it is left untouched.

Left out: stations closed to the public, out of use, and the ones that only
sell gas (LPG, CNG) or only diesel, when the map says so.

Usage: python3 scripts/fuel_stations.py fuel.geojsonseq app/public/data/fuel-stations.json
"""

from __future__ import annotations

import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ATTRIBUTION = "© OpenStreetMap contributors, ODbL"
MAX_LABEL = 32
_GAS_ONLY_NAME = re.compile(r"\b(GPL|LPG|CNG|GNC|autogas)\b", re.IGNORECASE)
_PETROL_TAGS = ("fuel:octane_95", "fuel:octane_98", "fuel:octane_100", "fuel:e10", "fuel:e5")


def _points(geometry: dict[str, Any]) -> list[tuple[float, float]]:
    """Every (lon, lat) of a GeoJSON geometry."""
    kind = geometry.get("type")
    coords = geometry.get("coordinates")
    if kind == "Point":
        return [tuple(coords)]  # type: ignore[list-item]
    if kind in ("LineString", "MultiPoint"):
        return [tuple(c) for c in coords]
    if kind in ("Polygon", "MultiLineString"):
        return [tuple(c) for ring in coords for c in _open(ring)]
    if kind == "MultiPolygon":
        return [tuple(c) for polygon in coords for ring in polygon for c in _open(ring)]
    return []


def _open(ring: list[list[float]]) -> list[list[float]]:
    """A closed ring repeats its first point at the end: count it once."""
    return ring[:-1] if len(ring) > 1 and ring[0] == ring[-1] else ring


def position(geometry: dict[str, Any]) -> tuple[float, float] | None:
    """(lat, lon) of a station: the point itself, or the middle of its area."""
    points = _points(geometry)
    if not points:
        return None
    lon = sum(p[0] for p in points) / len(points)
    lat = sum(p[1] for p in points) / len(points)
    return round(lat, 5), round(lon, 5)


def sells_petrol(tags: dict[str, str]) -> bool:
    """False only when the map says the station has no petrol."""
    if any(tags.get(tag) == "yes" for tag in _PETROL_TAGS):
        return True
    if all(tags.get(tag) == "no" for tag in ("fuel:octane_95", "fuel:octane_98")):
        return False
    if tags.get("fuel:diesel") == "yes" and tags.get("hgv") == "only":
        return False
    name = " ".join(tags.get(k, "") for k in ("name", "brand"))
    return not _GAS_ONLY_NAME.search(name)


def is_open_to_riders(tags: dict[str, str]) -> bool:
    if tags.get("access") in ("private", "no", "customers"):
        return False
    return not any(tags.get(k) == "yes" for k in ("disused", "abandoned"))


def label(tags: dict[str, str]) -> str:
    text = tags.get("brand") or tags.get("name") or tags.get("operator") or ""
    return " ".join(text.split())[:MAX_LABEL]


def convert(lines: list[str]) -> list[list[Any]]:
    seen: set[tuple[float, float]] = set()
    stations: list[list[Any]] = []
    for raw in lines:
        line = raw.strip().lstrip("\x1e")
        if not line:
            continue
        feature = json.loads(line)
        tags = feature.get("properties") or {}
        if tags.get("amenity") != "fuel" or not is_open_to_riders(tags) or not sells_petrol(tags):
            continue
        where = position(feature.get("geometry") or {})
        if where is None or where in seen:
            continue
        seen.add(where)
        stations.append([where[0], where[1], label(tags)])
    stations.sort(key=lambda s: (s[0], s[1]))
    return stations


def main(argv: list[str]) -> int:
    if len(argv) != 3:
        print(__doc__.strip().splitlines()[-1], file=sys.stderr)
        return 2
    source, target = Path(argv[1]), Path(argv[2])
    try:
        stations = convert(source.read_text(encoding="utf-8").splitlines())
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Could not read {source}: {exc}", file=sys.stderr)
        return 1
    # A broken download would empty the file: refuse a result that small.
    if len(stations) < 1000:
        print(f"Only {len(stations)} stations, something went wrong: not written.", file=sys.stderr)
        return 1
    # Same stations as last time: leave the file, and its date, alone.
    try:
        if json.loads(target.read_text(encoding="utf-8")).get("stations") == stations:
            print(f"{len(stations)} stations, no change")
            return 0
    except (OSError, json.JSONDecodeError):
        pass  # no file yet, or an unreadable one: write it fresh
    body = {
        "version": 1,
        "updated": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "attribution": ATTRIBUTION,
        "stations": stations,
    }
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(body, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
    print(f"{len(stations)} stations written to {target}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
