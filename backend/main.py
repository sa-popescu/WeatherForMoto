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
import pathlib
from typing import Annotated

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse

from weather_service import geocode_city, get_weather
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
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Startup event
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def on_startup() -> None:
    logger.info("WeatherForMoto backend starting up")
    logger.info("INDEX_HTML path: %s (exists=%s)", INDEX_HTML, INDEX_HTML.is_file())
    logger.info("DEFAULT_CITY: %s", DEFAULT_CITY)
    logger.info(
        "OWM API key configured: %s",
        "yes (custom)" if OWM_API_KEY != _DEMO_OWM_KEY else "no (using demo key)",
    )
    port = int(os.getenv("PORT", 8000))
    logger.info("Expected port: %d", port)


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
        data = await get_weather(lat, lon, resolved_city, OWM_API_KEY)
    except Exception as exc:
        logger.exception("Weather fetch error: %s", exc)
        raise HTTPException(
            status_code=502, detail=f"Weather data error: {exc}"
        ) from exc

    sources = data.get("current", {}).get("sources", [])
    logger.info("Weather data returned successfully (sources: %s)", sources)
    return data


# ---------------------------------------------------------------------------
# Dev entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
