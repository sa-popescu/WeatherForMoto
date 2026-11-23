"""Urban routes with live traffic, from the TomTom Routing API.

The key comes from the TOMTOM_API_KEY environment variable (on Cloud Run, the
``mm-tomtom-key`` secret). It is only ever sent to TomTom and never logged:
httpx request logging is turned down in main.py and nothing here prints a URL.

Every request goes to TomTom on its own. TomTom's terms do not allow keeping
a response to serve it to other users, so nothing is cached here; the app
keeps what it got for its own ride and recomputes a saved route when it is
opened again.

The free plan allows 2,500 requests a day. A per-client window and a daily
budget per instance keep the app well under that; past either, the caller
gets a clear "busy" answer and the app falls back to its route without
traffic (OSRM).

A departure later than now (``depart_at``) gets TomTom's predicted traffic
for that moment, which is how the app compares departure times.

The rider's route options (``RouteOptions``) go to TomTom as they are: the
kind of route (fastest, balanced, shortest, economical or one with curves),
what to avoid and the motorcycle's top speed. Without options the request is
the one the app always made: the fastest route, avoiding unpaved roads.
"""

from __future__ import annotations

import logging
import os
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import httpx

logger = logging.getLogger("weatherformoto.urban")

TOMTOM_API_KEY: str = os.getenv("TOMTOM_API_KEY", "").strip()
TOMTOM_ROUTE_URL = "https://api.tomtom.com/routing/1/calculateRoute/{locations}/json"
TOMTOM_TIMEOUT_S = 12.0

# Start, destination and up to eight stops on the way: the planner's ten
# stops (MAX_STOPS in app/src/features/route/stops.ts).
MAX_POINTS = 10
MAX_ALTERNATIVES = 2

# What the rider can ask of the route (TomTom's own names, so they pass as they are).
ROUTE_TYPES = ("fastest", "short", "shortest", "eco", "thrilling")
AVOIDABLE = ("motorways", "tollRoads", "ferries", "unpavedRoads", "tunnels", "borderCrossings")
LEVELS = ("low", "normal", "high")
# What the app always avoided before it had options, and still does by default.
DEFAULT_AVOID: tuple[str, ...] = ("unpavedRoads",)
# The value of ``avoid`` that avoids nothing (an empty query value is dropped on the way).
AVOID_NOTHING = "none"
# A motorcycle's top speed, for scooters and small bikes; TomTom accepts up to 250.
MIN_TOP_SPEED_KMH = 25
MAX_TOP_SPEED_KMH = 250

# Per client (IP): a destination costs up to six requests (the routes, then
# five departure times to compare), so this is about ten destinations, or a
# few reroutes on a ride, in ten minutes.
CLIENT_MAX_REQUESTS = 60
CLIENT_WINDOW_S = 10 * 60
# A departure this close to now is now; further out than this TomTom has no prediction worth asking.
DEPART_NOW_SLACK = timedelta(minutes=2)
DEPART_MAX_AHEAD = timedelta(days=7)
# Per instance and UTC day, under the free plan's 2,500 with room for retries.
DAILY_MAX_REQUESTS = 2_000

ATTRIBUTION = "© TomTom"


class UrbanRouteUnavailable(Exception):
    """Why no traffic route could be given: no-key, busy, quota, no-route or upstream."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


# ---------------------------------------------------------------------------
# Request budget
# ---------------------------------------------------------------------------


class RequestBudget:
    """Sliding window per client plus a daily total, kept in memory per instance."""

    def __init__(self, per_client: int, window_s: float, daily: int) -> None:
        self.per_client = per_client
        self.window_s = window_s
        self.daily = daily
        self._hits: dict[str, deque[float]] = {}
        self._day = ""
        self._day_count = 0

    def allow(self, client: str, now: float) -> bool:
        day = datetime.fromtimestamp(now, tz=timezone.utc).strftime("%Y-%m-%d")
        if day != self._day:
            self._day, self._day_count = day, 0
            # A new day is also a good moment to forget clients long gone.
            self._hits = {k: v for k, v in self._hits.items() if v and now - v[-1] < self.window_s}
        if self._day_count >= self.daily:
            return False
        hits = self._hits.setdefault(client, deque())
        while hits and now - hits[0] >= self.window_s:
            hits.popleft()
        if len(hits) >= self.per_client:
            return False
        hits.append(now)
        self._day_count += 1
        return True


budget = RequestBudget(CLIENT_MAX_REQUESTS, CLIENT_WINDOW_S, DAILY_MAX_REQUESTS)


# ---------------------------------------------------------------------------
# Request and answer
# ---------------------------------------------------------------------------


def parse_points(text: str) -> list[tuple[float, float]]:
    """``lat,lon;lat,lon[;...]`` into coordinates; ValueError with a reason otherwise."""
    points: list[tuple[float, float]] = []
    for part in (p.strip() for p in text.split(";")):
        if not part:
            continue
        try:
            lat_text, lon_text = part.split(",")
            lat, lon = float(lat_text), float(lon_text)
        except ValueError as exc:
            raise ValueError(f"Not a 'lat,lon' pair: {part[:40]}") from exc
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            raise ValueError(f"Coordinates out of range: {part[:40]}")
        points.append((lat, lon))
    if len(points) < 2:
        raise ValueError("At least a start and a destination are needed")
    if len(points) > MAX_POINTS:
        raise ValueError(f"At most {MAX_POINTS} points")
    return points


def parse_depart_at(text: str | None, now: datetime) -> datetime | None:
    """
    A departure as an ISO time with its offset ("2026-11-22T15:30:00Z"), or
    None for now. A time in the past or within two minutes counts as now;
    one without an offset, or more than a week away, is a ValueError.
    """
    if text is None or not text.strip() or text.strip() == "now":
        return None
    try:
        when = datetime.fromisoformat(text.strip())
    except ValueError as exc:
        raise ValueError("depart_at is not an ISO time") from exc
    if when.tzinfo is None:
        raise ValueError("depart_at needs a time zone offset")
    if when - now > DEPART_MAX_AHEAD:
        raise ValueError("depart_at is more than a week away")
    return None if when - now <= DEPART_NOW_SLACK else when.astimezone(timezone.utc)


@dataclass(frozen=True)
class RouteOptions:
    """What the rider asked of the route; the defaults are the app's route before it had options."""

    route_type: str = "fastest"
    avoid: tuple[str, ...] = DEFAULT_AVOID
    # How winding and how hilly a route with curves ("thrilling") should be.
    windingness: str = "normal"
    hilliness: str = "normal"
    top_speed_kmh: int | None = None
    # False for a plan made ahead: the usual speeds of the roads, not today's jams.
    traffic: bool = True


def parse_options(
    route_type: str | None = None,
    avoid: str | None = None,
    windingness: str | None = None,
    hilliness: str | None = None,
    top_speed: int | None = None,
    traffic: bool = True,
) -> RouteOptions:
    """
    The query's options, checked. ``avoid`` is a comma list of AVOIDABLE
    names: None keeps the default (unpaved roads), "none" (or an empty text)
    avoids nothing. ValueError with a reason for anything else.
    """
    kind = (route_type or "fastest").strip()
    if kind not in ROUTE_TYPES:
        raise ValueError(f"route_type must be one of {', '.join(ROUTE_TYPES)}")
    if avoid is None:
        avoided = DEFAULT_AVOID
    else:
        names = {a.strip() for a in avoid.split(",") if a.strip()} - {AVOID_NOTHING}
        unknown = names - set(AVOIDABLE)
        if unknown:
            raise ValueError(f"Cannot avoid: {', '.join(sorted(unknown))[:80]}")
        avoided = tuple(a for a in AVOIDABLE if a in names)
    levels = []
    for name, value in (("windingness", windingness), ("hilliness", hilliness)):
        level = (value or "normal").strip()
        if level not in LEVELS:
            raise ValueError(f"{name} must be one of {', '.join(LEVELS)}")
        levels.append(level)
    if top_speed is not None and not MIN_TOP_SPEED_KMH <= top_speed <= MAX_TOP_SPEED_KMH:
        raise ValueError(f"top_speed must be between {MIN_TOP_SPEED_KMH} and {MAX_TOP_SPEED_KMH} km/h")
    return RouteOptions(kind, avoided, levels[0], levels[1], top_speed, traffic)


def tomtom_request(
    points: list[tuple[float, float]],
    alternatives: int,
    depart_at: datetime | None = None,
    options: RouteOptions | None = None,
) -> tuple[str, dict[str, Any]]:
    """URL and query for a motorcycle route with live (or, later, predicted) traffic and coded turn instructions."""
    opts = options or RouteOptions()
    locations = ":".join(f"{lat:.6f},{lon:.6f}" for lat, lon in points)
    params: dict[str, Any] = {
        "key": TOMTOM_API_KEY,
        "travelMode": "motorcycle",
        "routeType": opts.route_type,
        "traffic": "true" if opts.traffic else "false",
        "departAt": depart_at.strftime("%Y-%m-%dT%H:%M:%SZ") if depart_at else "now",
        # Alternatives are only offered between two points.
        "maxAlternatives": max(0, min(MAX_ALTERNATIVES, alternatives)) if len(points) == 2 else 0,
        "computeTravelTimeFor": "all",
        "instructionsType": "coded",
        "sectionType": "traffic",
    }
    avoid = list(opts.avoid)
    if opts.route_type == "thrilling":
        # A ride for the fun of it should not come back along the road it went out on.
        avoid.append("alreadyUsedRoads")
        params["windingness"] = opts.windingness
        params["hilliness"] = opts.hilliness
    if avoid:
        # httpx repeats the key for each value, as TomTom wants it.
        params["avoid"] = avoid
    if opts.top_speed_kmh is not None:
        params["vehicleMaxSpeed"] = opts.top_speed_kmh
    return TOMTOM_ROUTE_URL.format(locations=locations), params


def _int(value: Any) -> int | None:
    return int(value) if isinstance(value, (int, float)) else None


def _route(raw: dict[str, Any]) -> dict[str, Any]:
    summary = raw.get("summary") or {}
    coords: list[list[float]] = []
    for leg in raw.get("legs") or []:
        for i, p in enumerate(leg.get("points") or []):
            point = [round(float(p["latitude"]), 6), round(float(p["longitude"]), 6)]
            # Legs share their joining point; keep it once.
            if i == 0 and coords and coords[-1] == point:
                continue
            coords.append(point)
    traffic = [
        {
            "from": _int(s.get("startPointIndex")),
            "to": _int(s.get("endPointIndex")),
            "category": s.get("simpleCategory") or "OTHER",
            "magnitude": _int(s.get("magnitudeOfDelay")) or 0,
            "delay_s": _int(s.get("delayInSeconds")),
            "speed_kmh": s.get("effectiveSpeedInKmh"),
        }
        for s in raw.get("sections") or []
        if s.get("sectionType") == "TRAFFIC"
    ]
    instructions = [
        {
            "offset_m": _int(ins.get("routeOffsetInMeters")) or 0,
            "index": _int(ins.get("pointIndex")),
            "time_s": _int(ins.get("travelTimeInSeconds")),
            "maneuver": ins.get("maneuver") or "",
            "street": ins.get("street") or None,
            "road": ", ".join(ins.get("roadNumbers") or []) or None,
            "exit": ins.get("exitNumber") or None,
            "roundabout_exit": _int(ins.get("roundaboutExitNumber")),
            "signpost": ins.get("signpostText") or None,
        }
        for ins in ((raw.get("guidance") or {}).get("instructions") or [])
    ]
    return {
        "distance_m": _int(summary.get("lengthInMeters")) or 0,
        "duration_s": _int(summary.get("travelTimeInSeconds")) or 0,
        "no_traffic_s": _int(summary.get("noTrafficTravelTimeInSeconds")),
        "delay_s": _int(summary.get("trafficDelayInSeconds")) or 0,
        # One length per leg, so the planner knows where each stop falls on the route.
        "legs_m": [_int((leg.get("summary") or {}).get("lengthInMeters")) or 0 for leg in raw.get("legs") or []],
        "coords": coords,
        "traffic": traffic,
        "instructions": instructions,
    }


def parse_tomtom(body: Any) -> list[dict[str, Any]]:
    """The routes of a Calculate Route answer, in the shape the app reads."""
    routes = body.get("routes") if isinstance(body, dict) else None
    if not routes:
        raise UrbanRouteUnavailable("no-route")
    parsed = [_route(r) for r in routes]
    usable = [r for r in parsed if len(r["coords"]) >= 2]
    if not usable:
        raise UrbanRouteUnavailable("no-route")
    return usable


async def fetch_urban_routes(
    client: httpx.AsyncClient,
    points: list[tuple[float, float]],
    alternatives: int,
    depart_at: datetime | None = None,
    options: RouteOptions | None = None,
) -> dict[str, Any]:
    """Asks TomTom once; raises UrbanRouteUnavailable with a reason the app can act on."""
    if not TOMTOM_API_KEY:
        raise UrbanRouteUnavailable("no-key")
    url, params = tomtom_request(points, alternatives, depart_at, options)
    started = time.monotonic()
    try:
        response = await client.get(url, params=params, timeout=TOMTOM_TIMEOUT_S)
    except httpx.HTTPError as exc:
        # The exception text can carry the request URL, and with it the key.
        logger.warning("TomTom routing unreachable: %s", type(exc).__name__)
        raise UrbanRouteUnavailable("upstream") from None
    if response.status_code in (401, 403):
        logger.error("TomTom refused the key (HTTP %s)", response.status_code)
        raise UrbanRouteUnavailable("upstream")
    if response.status_code == 429:
        logger.warning("TomTom quota or rate limit reached")
        raise UrbanRouteUnavailable("quota")
    if response.status_code == 400:
        raise UrbanRouteUnavailable("no-route")
    if response.status_code != 200:
        logger.warning("TomTom routing answered HTTP %s", response.status_code)
        raise UrbanRouteUnavailable("upstream")
    try:
        body = response.json()
    except ValueError:
        raise UrbanRouteUnavailable("upstream") from None
    routes = parse_tomtom(body)
    logger.info("TomTom routes: %d in %.0f ms", len(routes), (time.monotonic() - started) * 1000)
    return {
        "source": "tomtom",
        "attribution": ATTRIBUTION,
        "depart_at": depart_at.strftime("%Y-%m-%dT%H:%M:%SZ") if depart_at else None,
        "routes": routes,
    }
