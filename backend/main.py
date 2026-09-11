"""
WeatherForMoto – FastAPI Backend
=================================
Endpoints
---------
GET /health
    Simple health check.

GET /weather?city=Bucharest
    Returns aggregated weather (current + 7-day daily + hourly) for a city name.

GET /weather?lat=44.43&lon=26.10
    Same but by coordinates.

GET /geocode?city=Cluj-Napoca
    Returns geocoding results (lat/lon/name) for a city query.

GET /meta/scoring
    The moto score model (label thresholds, rain intensity bands, impact
    matrix, caps) exactly as the backend applies it.
"""

import logging
import os
import asyncio
from datetime import datetime as _dt
import pathlib
from contextlib import asynccontextmanager
from collections.abc import AsyncIterator, Awaitable, Callable
from typing import Annotated, Any

from dotenv import load_dotenv

# Load .env BEFORE importing modules that read environment variables at import
# time (auth_alerts, weather_service); otherwise local .env values are ignored.
load_dotenv()

import httpx  # noqa: E402
from fastapi import FastAPI, HTTPException, Query, Request, Response  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.middleware.gzip import GZipMiddleware  # noqa: E402
from fastapi.responses import FileResponse  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402

from weather_service import (  # noqa: E402
    DEFAULT_MET_USER_AGENT,
    geocode_city,
    get_multi_route_weather,
    get_route_weather,
    get_weather,
    http_client_scope,
    scoring_metadata,
    set_http_client,
)
from auth_alerts import router as auth_alerts_router, init_db  # noqa: E402

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("weatherformoto")
# httpx logs every request URL at INFO, and some provider keys travel in the URL
# (OWM query string, Pirate Weather path). Keep those URLs out of Cloud Run logs.
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
# API key must be supplied via the OPENWEATHERMAP_API_KEY environment variable.
OWM_API_KEY: str = os.getenv("OPENWEATHERMAP_API_KEY", "")
DEFAULT_CITY: str = os.getenv("DEFAULT_CITY", "Bucharest")
PIRATE_WEATHER_API_KEY: str = os.getenv("PIRATE_WEATHER_API_KEY", "")
WEATHERXM_API_KEY: str = os.getenv("WEATHERXM_API_KEY", "")
NETATMO_CLIENT_ID: str = os.getenv("NETATMO_CLIENT_ID", "")
NETATMO_CLIENT_SECRET: str = os.getenv("NETATMO_CLIENT_SECRET", "")
NETATMO_REFRESH_TOKEN: str = os.getenv("NETATMO_REFRESH_TOKEN", "")
# MET Norway's terms require an identifying User-Agent (app name + contact URL).
MET_NORWAY_USER_AGENT: str = os.getenv("MET_NORWAY_USER_AGENT", "") or DEFAULT_MET_USER_AGENT

# Shared outbound HTTP client settings: pooled keep-alive connections instead
# of a new TLS handshake per provider per request. Per-call timeouts in
# weather_service still apply on top of these defaults.
HTTP_CLIENT_LIMITS = httpx.Limits(
    max_connections=100, max_keepalive_connections=20, keepalive_expiry=30.0
)
HTTP_CLIENT_TIMEOUT = httpx.Timeout(10.0, connect=5.0)

# Path to the frontend index.html (one level above the backend/ directory)
_REPO_ROOT = pathlib.Path(__file__).parent.parent
INDEX_HTML = _REPO_ROOT / "index.html"
SW_JS = _REPO_ROOT / "sw.js"
MANIFEST_JSON = _REPO_ROOT / "manifest.json"
ICONS_DIR = _REPO_ROOT / "icons"
APPLE_TOUCH_ICON = ICONS_DIR / "motometeo-touch-180.png"

# ---------------------------------------------------------------------------
# Lifespan (startup / shutdown)
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(fastapi_app: FastAPI) -> AsyncIterator[None]:
    # Schema migration opens a remote Turso connection and issues ~35 serial
    # round trips; running it on every cold start is the main cause of the slow
    # first load. Run it only when RUN_DB_MIGRATIONS=true (one-off migration
    # deploy / Cloud Run Job), and off the event loop so it never blocks readiness.
    if os.getenv("RUN_DB_MIGRATIONS", "false").lower() == "true":
        logger.info("RUN_DB_MIGRATIONS=true — running schema migration at startup")
        await asyncio.to_thread(init_db)
    logger.info("WeatherForMoto backend starting up")
    logger.info("INDEX_HTML path: %s (exists=%s)", INDEX_HTML, INDEX_HTML.is_file())
    logger.info("DEFAULT_CITY: %s", DEFAULT_CITY)
    logger.info(
        "OWM API key configured: %s",
        "yes" if OWM_API_KEY else "no (set OPENWEATHERMAP_API_KEY for better data quality)",
    )
    logger.info("Expected port: %d", int(os.getenv("PORT", 8000)))

    http_client = httpx.AsyncClient(limits=HTTP_CLIENT_LIMITS, timeout=HTTP_CLIENT_TIMEOUT)
    set_http_client(http_client)
    try:
        yield
    finally:
        set_http_client(None)
        await http_client.aclose()
        logger.info("Shared HTTP client closed")


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------
app = FastAPI(
    title="WeatherForMoto API",
    description=(
        "Aggregated weather statistics from multiple sources "
        "(OpenWeatherMap + Open-Meteo) optimised for motorcyclists."
    ),
    version="1.0.0",
    lifespan=lifespan,
)

# CORS: restrict to known frontend origins instead of a wildcard. Set
# ALLOWED_ORIGINS (comma-separated) to override; the regex also permits the
# Cloudflare Pages domains used for the static frontend deployment.
ALLOWED_ORIGINS = [
    o.strip()
    for o in os.getenv(
        "ALLOWED_ORIGINS", "https://weatherformoto.bluemouse.cc"
    ).split(",")
    if o.strip()
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_origin_regex=r"https://([a-z0-9-]+\.)*weatherformoto\.pages\.dev",
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# Compress JSON and the frontend HTML (a 14-day /weather payload is ~150 KB
# uncompressed and shrinks roughly 10x). Small bodies are left alone.
app.add_middleware(GZipMiddleware, minimum_size=1000)

# Baseline security headers on every response, including the copy of the
# frontend served from "/". Mirrors _headers on Cloudflare. A strict script-src
# CSP is not possible yet (inline handlers); it comes with the new frontend.
_SECURITY_HEADERS: dict[str, str] = {
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "strict-origin-when-cross-origin",
    "Permissions-Policy": "geolocation=(self), camera=(), microphone=(), payment=()",
    "Content-Security-Policy": "object-src 'none'; base-uri 'self'; frame-ancestors 'none'",
}


@app.middleware("http")
async def add_security_headers(
    request: Request, call_next: Callable[[Request], Awaitable[Response]]
) -> Response:
    response = await call_next(request)
    for name, value in _SECURITY_HEADERS.items():
        response.headers.setdefault(name, value)
    return response


app.include_router(auth_alerts_router)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _validated_departure(departure: str | None) -> str | None:
    """Return the trimmed departure, or None when absent.

    An unparseable value is rejected with 422 instead of silently falling
    back to "now", so the user knows the route was not computed for the time
    they asked for.
    """
    if departure is None or not departure.strip():
        return None
    value = departure.strip()
    try:
        _dt.fromisoformat(value)
    except ValueError as exc:
        raise HTTPException(
            status_code=422,
            detail="Invalid departure: use ISO format, e.g. 2024-06-15T08:00",
        ) from exc
    return value


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["meta"])
async def health() -> dict:
    """Returns API status."""
    return {"status": "ok", "version": "1.0.0"}


@app.get("/meta/scoring", tags=["meta"])
async def meta_scoring(response: Response) -> dict[str, Any]:
    """Score model used by the backend: labels, rain bands, impact matrix, caps."""
    response.headers["Cache-Control"] = "public, max-age=3600"
    return scoring_metadata()


@app.get("/", include_in_schema=False)
async def serve_frontend():
    """Serve the frontend single-page application."""
    if not INDEX_HTML.is_file():
        logger.error("Frontend index.html not found at: %s", INDEX_HTML)
        raise HTTPException(status_code=404, detail=f"Frontend not found at {INDEX_HTML}.")
    return FileResponse(
        INDEX_HTML,
        media_type="text/html",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/sw.js", include_in_schema=False)
async def serve_sw():
    """Serve the PWA service worker."""
    if not SW_JS.is_file():
        raise HTTPException(status_code=404, detail="sw.js not found.")
    return FileResponse(
        SW_JS,
        media_type="application/javascript",
        headers={"Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.get("/manifest.json", include_in_schema=False)
async def serve_manifest():
    """Serve the PWA web manifest."""
    if not MANIFEST_JSON.is_file():
        raise HTTPException(status_code=404, detail="manifest.json not found.")
    return FileResponse(MANIFEST_JSON, media_type="application/manifest+json")


@app.get("/apple-touch-icon.png", include_in_schema=False)
@app.get("/apple-touch-icon-precomposed.png", include_in_schema=False)
async def serve_apple_touch_icon():
    """Serve iOS homescreen icon from a stable default path."""
    if not APPLE_TOUCH_ICON.is_file():
        raise HTTPException(status_code=404, detail="apple-touch-icon not found.")
    return FileResponse(APPLE_TOUCH_ICON, media_type="image/png")


@app.get("/privacy-policy", include_in_schema=False)
@app.get("/privacy-policy.html", include_in_schema=False)
async def serve_privacy_policy():
    """Serve the privacy policy page."""
    privacy = _REPO_ROOT / "privacy-policy.html"
    if not privacy.is_file():
        raise HTTPException(status_code=404, detail="Privacy policy not found.")
    return FileResponse(privacy, media_type="text/html")


@app.get("/geocode", tags=["location"])
async def geocode(
    city: Annotated[str, Query(description="City name to look up")] = DEFAULT_CITY,
):
    """Resolve a city name to coordinates."""
    logger.info("Geocode request received")
    async with http_client_scope() as client:
        try:
            result = await geocode_city(city, client)
        except ValueError as exc:
            logger.warning("Geocoding failed: %s", exc)
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Geocoding error: %s", exc)
            raise HTTPException(
                status_code=502, detail="Geocoding service temporarily unavailable"
            ) from exc
    return result


@app.get("/weather", tags=["weather"])
async def weather(
    city: Annotated[
        str | None,
        Query(description="City name (alternative to lat/lon)"),
    ] = None,
    lat: Annotated[
        float | None,
        Query(ge=-90, le=90, description="Latitude"),
    ] = None,
    lon: Annotated[
        float | None,
        Query(ge=-180, le=180, description="Longitude"),
    ] = None,
    days: Annotated[
        int,
        Query(ge=1, le=16, description="Forecast days (7 free, up to 16 premium)"),
    ] = 7,
):
    """
    Return aggregated weather data.

    Provide either `city` **or** `lat` + `lon`.
    When neither is given the default city (Bucharest) is used.
    """
    # Resolve location
    if (lat is None) != (lon is None):
        raise HTTPException(
            status_code=422, detail="Provide both lat and lon, or neither."
        )
    if lat is not None and lon is not None:
        resolved_city = city or f"{lat:.4f}, {lon:.4f}"
        logger.info("Weather request by coordinates (lat/lon provided)")
    else:
        city_query = city or DEFAULT_CITY
        logger.info("Weather request by city name lookup")
        async with http_client_scope() as client:
            try:
                geo = await geocode_city(city_query, client)
            except ValueError as exc:
                logger.warning("Geocoding failed: %s", exc)
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except Exception as exc:
                logger.exception("Geocoding error: %s", exc)
                raise HTTPException(
                    status_code=502, detail="Geocoding service temporarily unavailable"
                ) from exc
        lat = geo["lat"]
        lon = geo["lon"]
        resolved_city = geo["name"] + (f", {geo['country']}" if geo.get("country") else "")

    try:
        data = await get_weather(lat, lon, resolved_city, OWM_API_KEY, forecast_days=days,
                                 pirate_weather_key=PIRATE_WEATHER_API_KEY,
                                 met_user_agent=MET_NORWAY_USER_AGENT,
                                 weatherxm_api_key=WEATHERXM_API_KEY,
                                 netatmo_client_id=NETATMO_CLIENT_ID,
                                 netatmo_client_secret=NETATMO_CLIENT_SECRET,
                                 netatmo_refresh_token=NETATMO_REFRESH_TOKEN)
    except Exception as exc:
        logger.exception("Weather fetch error: %s", exc)
        raise HTTPException(
            status_code=502, detail="Weather data temporarily unavailable"
        ) from exc

    logger.info("Weather data returned successfully")
    return data


@app.get("/route", tags=["route"])
async def route_weather(
    origin: Annotated[
        str, Query(description="City of origin")
    ] = DEFAULT_CITY,
    destination: Annotated[
        str, Query(description="City of destination")
    ] = "Cluj-Napoca",
    departure: Annotated[
        str | None,
        Query(description="Departure datetime in ISO format, e.g. 2024-06-15T08:00 "
                          "(local time of the origin; default: now at the origin)"),
    ] = None,
    avg_speed: Annotated[
        float,
        Query(gt=0, le=180, description="Average riding speed in km/h"),
    ] = 80.0,
):
    """
    Return weather snapshots along a motorcycle route.

    Provides `origin` → `destination` weather waypoints at estimated arrival
    times based on the departure time and average speed.  Waypoints are
    spaced by linear (great-circle) interpolation; actual road distance may
    differ.
    """
    departure_iso = _validated_departure(departure)

    # Geocode both cities (Nominatim calls are serialised inside geocode_city)
    async with http_client_scope() as client:
        try:
            origin_geo, dest_geo = await asyncio.gather(
                geocode_city(origin, client),
                geocode_city(destination, client),
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Route geocoding error: %s", exc)
            raise HTTPException(
                status_code=502, detail="Geocoding service temporarily unavailable"
            ) from exc

    origin_name = origin_geo["name"] + (
        f", {origin_geo['country']}" if origin_geo.get("country") else ""
    )
    dest_name = dest_geo["name"] + (
        f", {dest_geo['country']}" if dest_geo.get("country") else ""
    )

    try:
        data = await get_route_weather(
            origin_geo["lat"], origin_geo["lon"], origin_name,
            dest_geo["lat"], dest_geo["lon"], dest_name,
            departure_iso, avg_speed, OWM_API_KEY,
        )
    except Exception as exc:
        logger.exception("Route weather error: %s", exc)
        raise HTTPException(
            status_code=502, detail="Route weather temporarily unavailable"
        ) from exc

    logger.info("Route weather data returned successfully")
    return data


@app.get("/route/multi", tags=["route"])
async def route_multi(
    stops: Annotated[str, Query(description="Semicolon-separated city names, e.g. 'Cluj-Napoca;Sibiu;Brașov'")],
    departure: str | None = None,
    avg_speed: float = Query(default=80.0, gt=0, le=180),
):
    """
    Compute weather along a multi-stop motorcycle route (premium feature).
    ``stops`` is a semicolon-separated list of city names (2–5 stops).
    Returns per-segment route weather the same way as /route.
    """
    stop_names = [s.strip() for s in stops.split(";") if s.strip()]
    if len(stop_names) < 2:
        raise HTTPException(status_code=422, detail="At least 2 stops required")
    if len(stop_names) > 5:
        raise HTTPException(status_code=422, detail="Maximum 5 stops allowed")

    departure_iso = _validated_departure(departure)

    # geocode_city caches results and spaces Nominatim calls to respect its
    # 1 request/second policy, so gathering here does not burst the service.
    async with http_client_scope() as client:
        try:
            geo_results = await asyncio.gather(
                *[geocode_city(name, client) for name in stop_names]
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Multi-route geocoding error: %s", exc)
            raise HTTPException(
                status_code=502, detail="Geocoding service temporarily unavailable"
            ) from exc

    geocoded: list[dict] = []
    for name, result in zip(stop_names, geo_results):
        geocoded.append({
            "name": result["name"],
            "lat": result["lat"],
            "lon": result["lon"],
        })

    try:
        data = await get_multi_route_weather(geocoded, departure_iso, avg_speed, OWM_API_KEY)
    except Exception as exc:
        logger.exception("Multi-route weather error: %s", exc)
        raise HTTPException(
            status_code=502, detail="Route weather temporarily unavailable"
        ) from exc

    return data


# ---------------------------------------------------------------------------
# Static icons directory (must be mounted AFTER explicit routes)
# ---------------------------------------------------------------------------
if ICONS_DIR.is_dir():
    app.mount("/icons", StaticFiles(directory=str(ICONS_DIR)), name="icons")


# ---------------------------------------------------------------------------
# Dev entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
