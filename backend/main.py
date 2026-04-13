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
"""

import logging
import os
import asyncio
from datetime import datetime as _dt
import pathlib
from contextlib import asynccontextmanager
from typing import Annotated

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from weather_service import geocode_city, get_weather, get_route_weather, get_multi_route_weather
from auth_alerts import router as auth_alerts_router, init_db
import httpx

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("weatherformoto")

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

# API key must be supplied via the OPENWEATHERMAP_API_KEY environment variable.
# A demo key is provided as a last-resort fallback; replace it with your own key for production.
_DEMO_OWM_KEY = "3e17019022d624b5b3d26b54f7c6b8a5"
OWM_API_KEY: str = os.getenv("OPENWEATHERMAP_API_KEY", _DEMO_OWM_KEY)
DEFAULT_CITY: str = os.getenv("DEFAULT_CITY", "Bucharest")

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
async def lifespan(fastapi_app: FastAPI):
    init_db()
    logger.info("WeatherForMoto backend starting up")
    logger.info("INDEX_HTML path: %s (exists=%s)", INDEX_HTML, INDEX_HTML.is_file())
    logger.info("DEFAULT_CITY: %s", DEFAULT_CITY)
    logger.info(
        "OWM API key configured: %s",
        "yes (custom)" if OWM_API_KEY != _DEMO_OWM_KEY else "no (using demo key)",
    )
    logger.info("Expected port: %d", int(os.getenv("PORT", 8000)))
    yield


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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(auth_alerts_router)


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["meta"])
async def health() -> dict:
    """Returns API status."""
    return {"status": "ok", "version": "1.0.0"}


@app.get("/", include_in_schema=False)
async def serve_frontend():
    """Serve the frontend single-page application."""
    if not INDEX_HTML.is_file():
        logger.error("Frontend index.html not found at: %s", INDEX_HTML)
        raise HTTPException(status_code=404, detail=f"Frontend not found at {INDEX_HTML}.")
    return FileResponse(INDEX_HTML, media_type="text/html")


@app.get("/sw.js", include_in_schema=False)
async def serve_sw():
    """Serve the PWA service worker."""
    if not SW_JS.is_file():
        raise HTTPException(status_code=404, detail="sw.js not found.")
    return FileResponse(SW_JS, media_type="application/javascript")


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
    async with httpx.AsyncClient() as client:
        try:
            result = await geocode_city(city, client)
        except ValueError as exc:
            logger.warning("Geocoding failed: %s", exc)
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Geocoding error: %s", exc)
            raise HTTPException(status_code=502, detail=f"Geocoding error: {exc}") from exc
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
    if lat is not None and lon is not None:
        resolved_city = city or f"{lat:.4f}, {lon:.4f}"
        logger.info("Weather request by coordinates (lat/lon provided)")
    else:
        city_query = city or DEFAULT_CITY
        logger.info("Weather request by city name lookup")
        async with httpx.AsyncClient() as client:
            try:
                geo = await geocode_city(city_query, client)
            except ValueError as exc:
                logger.warning("Geocoding failed: %s", exc)
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except Exception as exc:
                logger.exception("Geocoding error: %s", exc)
                raise HTTPException(
                    status_code=502, detail=f"Geocoding error: {exc}"
                ) from exc
        lat = geo["lat"]
        lon = geo["lon"]
        resolved_city = geo["name"] + (f", {geo['country']}" if geo.get("country") else "")

    try:
        data = await get_weather(lat, lon, resolved_city, OWM_API_KEY, forecast_days=days)
    except Exception as exc:
        logger.exception("Weather fetch error: %s", exc)
        raise HTTPException(
            status_code=502, detail=f"Weather data error: {exc}"
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
        Query(description="Departure datetime in ISO format, e.g. 2024-06-15T08:00"),
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
    if departure is None:
        departure = _dt.now().isoformat()[:16]

    # Geocode both cities concurrently
    async with httpx.AsyncClient() as client:
        try:
            origin_geo, dest_geo = await asyncio.gather(
                geocode_city(origin, client),
                geocode_city(destination, client),
            )
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
            logger.exception("Route geocoding error: %s", exc)
            raise HTTPException(status_code=502, detail=f"Geocoding error: {exc}") from exc

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
            departure, avg_speed, OWM_API_KEY,
        )
    except Exception as exc:
        logger.exception("Route weather error: %s", exc)
        raise HTTPException(
            status_code=502, detail=f"Route weather error: {exc}"
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

    departure_str = departure or _dt.now().isoformat()[:16]

    async with httpx.AsyncClient() as _client:
        geo_tasks = [geocode_city(name, OWM_API_KEY, _client) for name in stop_names]
        geo_results = await asyncio.gather(*geo_tasks)

    geocoded: list[dict] = []
    for i, (name, results) in enumerate(zip(stop_names, geo_results)):
        if not results:
            raise HTTPException(
                status_code=404, detail=f"City not found: {name!r}"
            )
        geocoded.append({
            "name": results[0]["name"],
            "lat": results[0]["lat"],
            "lon": results[0]["lon"],
        })

    try:
        data = await get_multi_route_weather(geocoded, departure_str, avg_speed, OWM_API_KEY)
    except Exception as exc:
        logger.exception("Multi-route weather error: %s", exc)
        raise HTTPException(status_code=502, detail=f"Route weather error: {exc}") from exc

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
