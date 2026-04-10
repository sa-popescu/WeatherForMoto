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

import os
from typing import Annotated

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from weather_service import geocode_city, get_weather
import httpx

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
load_dotenv()

# API key must be supplied via the OPENWEATHERMAP_API_KEY environment variable.
# A demo key is provided as a last-resort fallback; replace it with your own key for production.
_DEMO_OWM_KEY = "3e17019022d624b5b3d26b54f7c6b8a5"
OWM_API_KEY: str = os.getenv("OPENWEATHERMAP_API_KEY", _DEMO_OWM_KEY)
DEFAULT_CITY: str = os.getenv("DEFAULT_CITY", "Bucharest")

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
# Routes
# ---------------------------------------------------------------------------

@app.get("/health", tags=["meta"])
async def health() -> dict:
    """Returns API status."""
    return {"status": "ok", "version": "1.0.0"}


@app.get("/geocode", tags=["location"])
async def geocode(
    city: Annotated[str, Query(description="City name to look up")] = DEFAULT_CITY,
):
    """Resolve a city name to coordinates."""
    async with httpx.AsyncClient() as client:
        try:
            result = await geocode_city(city, client)
        except ValueError as exc:
            raise HTTPException(status_code=404, detail=str(exc)) from exc
        except Exception as exc:
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
    else:
        city_query = city or DEFAULT_CITY
        async with httpx.AsyncClient() as client:
            try:
                geo = await geocode_city(city_query, client)
            except ValueError as exc:
                raise HTTPException(status_code=404, detail=str(exc)) from exc
            except Exception as exc:
                raise HTTPException(
                    status_code=502, detail=f"Geocoding error: {exc}"
                ) from exc
        lat = geo["lat"]
        lon = geo["lon"]
        resolved_city = geo["name"] + (f", {geo['country']}" if geo.get("country") else "")

    try:
        data = await get_weather(lat, lon, resolved_city, OWM_API_KEY)
    except Exception as exc:
        raise HTTPException(
            status_code=502, detail=f"Weather data error: {exc}"
        ) from exc

    return data


# ---------------------------------------------------------------------------
# Dev entry-point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=True)
