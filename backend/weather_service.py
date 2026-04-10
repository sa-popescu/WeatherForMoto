"""
Weather data aggregation service.

Fetches data from:
  1. OpenWeatherMap (paid key, detailed forecast & air quality)
  2. Open-Meteo (free, no key, high-resolution European model)

Both sources are normalised to the same schema and then merged so that
each numeric field is the weighted average of the available values,
giving more accurate results than any single source alone.
"""

import asyncio
import math
from typing import Any

import httpx

# ---------------------------------------------------------------------------
# Source URLs
# ---------------------------------------------------------------------------
OWM_BASE = "https://api.openweathermap.org"
OPENMETEO_BASE = "https://api.open-meteo.com/v1/forecast"
GEOCODING_OPENMETEO = "https://geocoding-api.open-meteo.com/v1/search"
OWM_GEO_URL = f"{OWM_BASE}/geo/1.0/direct"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _weighted_avg(values: list[float | None], weights: list[float]) -> float | None:
    """Return a weighted average, ignoring None values."""
    total_w = 0.0
    total_v = 0.0
    for v, w in zip(values, weights):
        if v is not None:
            total_v += v * w
            total_w += w
    return round(total_v / total_w, 2) if total_w else None


def _wind_direction_label(degrees: float | None) -> str:
    # Romanian cardinal directions: N=Nord, NE=Nord-Est, E=Est, SE=Sud-Est,
    # S=Sud, SV=Sud-Vest, V=Vest, NV=Nord-Vest
    if degrees is None:
        return "—"
    dirs = ["N", "NE", "E", "SE", "S", "SV", "V", "NV"]
    return dirs[round(degrees / 45) % 8]


def _beaufort(speed_kmh: float | None) -> str:
    if speed_kmh is None:
        return "—"
    mps = speed_kmh / 3.6
    scale = [0.3, 1.5, 3.4, 5.4, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6]
    for i, threshold in enumerate(scale):
        if mps < threshold:
            return str(i)
    return "12"


def _moto_score(
    feels_like: float | None,
    wind_gusts_kmh: float | None,
    precipitation_mm: float | None,
    weather_code: int | None,
) -> int:
    """
    Compute a 0-100 'moto suitability' score.
    Higher = better riding conditions.
    """
    score = 100
    p = precipitation_mm or 0
    g = wind_gusts_kmh or 0
    f = feels_like if feels_like is not None else 20
    code = weather_code or 0

    if p > 5:
        score -= 50
    elif p > 1:
        score -= 30
    elif p > 0.2:
        score -= 15

    if code in (95, 96, 99):  # thunderstorm
        score -= 40

    if g > 70:
        score -= 30
    elif g > 50:
        score -= 15
    elif g > 35:
        score -= 8

    if f < 5:
        score -= 25
    elif f < 10:
        score -= 12
    elif f > 36:
        score -= 10

    if code in (45, 48):  # fog
        score -= 20

    return max(0, min(100, round(score)))


def _moto_label(score: int) -> str:
    if score >= 80:
        return "IDEAL"
    if score >= 60:
        return "OK"
    if score >= 40:
        return "ACCEPTABIL"
    if score >= 20:
        return "RISCANT"
    return "EVITĂ"


# ---------------------------------------------------------------------------
# Open-Meteo WMO weather-code → (description, emoji)
# ---------------------------------------------------------------------------
_WMO_MAP: dict[int, tuple[str, str]] = {
    0: ("Cer senin", "☀️"),
    1: ("Predominant senin", "🌤️"),
    2: ("Parțial înnorat", "⛅"),
    3: ("Înnorat", "☁️"),
    45: ("Ceață", "🌫️"),
    48: ("Ceață cu chiciură", "🌫️"),
    51: ("Burniță ușoară", "🌦️"),
    53: ("Burniță moderată", "🌦️"),
    55: ("Burniță densă", "🌧️"),
    61: ("Ploaie ușoară", "🌧️"),
    63: ("Ploaie moderată", "🌧️"),
    65: ("Ploaie torențială", "🌧️"),
    71: ("Ninsoare ușoară", "🌨️"),
    73: ("Ninsoare moderată", "❄️"),
    75: ("Ninsoare puternică", "❄️"),
    80: ("Averse ușoare", "🌦️"),
    81: ("Averse moderate", "🌧️"),
    82: ("Averse violente", "⛈️"),
    95: ("Furtună", "⛈️"),
    96: ("Furtună cu grindină", "⛈️"),
    99: ("Furtună puternică cu grindină", "⛈️"),
}


def _wmo_desc(code: int | None) -> str:
    if code is None:
        return "—"
    return _WMO_MAP.get(code, ("—", "🌡️"))[0]


def _wmo_icon(code: int | None) -> str:
    if code is None:
        return "🌡️"
    return _WMO_MAP.get(code, ("—", "🌡️"))[1]


# ---------------------------------------------------------------------------
# OWM weather-condition id → WMO-like code (approximate mapping)
# ---------------------------------------------------------------------------
def _owm_id_to_wmo(owm_id: int) -> int:
    """Best-effort mapping of OWM condition IDs to WMO codes."""
    if 200 <= owm_id < 300:
        return 95
    if 300 <= owm_id < 400:
        return 53
    if owm_id in (500,):
        return 61
    if owm_id in (501,):
        return 63
    if 500 <= owm_id < 510:
        return 65
    if 510 <= owm_id < 600:
        return 80
    if owm_id in (600,):
        return 71
    if owm_id in (601,):
        return 73
    if 600 <= owm_id < 700:
        return 75
    if owm_id in (741,):
        return 45
    if owm_id in (701, 711, 721, 731, 751, 761, 762, 771, 781):
        return 45
    if owm_id == 800:
        return 0
    if owm_id == 801:
        return 1
    if owm_id == 802:
        return 2
    if owm_id in (803, 804):
        return 3
    return 0


# ---------------------------------------------------------------------------
# Geocoding
# ---------------------------------------------------------------------------

async def geocode_city(city: str, client: httpx.AsyncClient) -> dict[str, Any]:
    """Return {lat, lon, name, country} for a city name using Open-Meteo geocoding."""
    resp = await client.get(
        GEOCODING_OPENMETEO,
        params={"name": city, "count": 1, "language": "ro"},
        timeout=10,
    )
    resp.raise_for_status()
    data = resp.json()
    if not data.get("results"):
        raise ValueError(f"Orașul '{city}' nu a fost găsit.")
    r = data["results"][0]
    return {
        "lat": r["latitude"],
        "lon": r["longitude"],
        "name": r.get("name", city),
        "country": r.get("country_code", ""),
        "timezone": r.get("timezone", "auto"),
    }


# ---------------------------------------------------------------------------
# Fetch from Open-Meteo
# ---------------------------------------------------------------------------

async def _fetch_openmeteo(lat: float, lon: float, client: httpx.AsyncClient) -> dict[str, Any]:
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": (
            "temperature_2m,apparent_temperature,relative_humidity_2m,"
            "wind_speed_10m,wind_gusts_10m,wind_direction_10m,"
            "precipitation,weather_code,surface_pressure,visibility"
        ),
        "hourly": (
            "temperature_2m,apparent_temperature,precipitation_probability,"
            "precipitation,weather_code,wind_speed_10m,wind_gusts_10m"
        ),
        "daily": (
            "weather_code,temperature_2m_max,temperature_2m_min,"
            "apparent_temperature_max,apparent_temperature_min,"
            "precipitation_sum,wind_speed_10m_max,wind_gusts_10m_max,"
            "precipitation_probability_max"
        ),
        "timezone": "auto",
        "forecast_days": 7,
        "wind_speed_unit": "kmh",
    }
    resp = await client.get(OPENMETEO_BASE, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


# ---------------------------------------------------------------------------
# Fetch from OpenWeatherMap
# ---------------------------------------------------------------------------

async def _fetch_owm_current(
    lat: float, lon: float, api_key: str, client: httpx.AsyncClient
) -> dict[str, Any] | None:
    try:
        resp = await client.get(
            f"{OWM_BASE}/data/2.5/weather",
            params={"lat": lat, "lon": lon, "appid": api_key, "units": "metric", "lang": "ro"},
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


async def _fetch_owm_forecast(
    lat: float, lon: float, api_key: str, client: httpx.AsyncClient
) -> dict[str, Any] | None:
    try:
        resp = await client.get(
            f"{OWM_BASE}/data/2.5/forecast",
            params={
                "lat": lat,
                "lon": lon,
                "appid": api_key,
                "units": "metric",
                "lang": "ro",
                "cnt": 40,
            },
            timeout=15,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


async def _fetch_owm_air(
    lat: float, lon: float, api_key: str, client: httpx.AsyncClient
) -> dict[str, Any] | None:
    try:
        resp = await client.get(
            f"{OWM_BASE}/data/2.5/air_pollution",
            params={"lat": lat, "lon": lon, "appid": api_key},
            timeout=10,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Normalise & merge
# ---------------------------------------------------------------------------

def _merge_current(om_data: dict, owm_current: dict | None, owm_air: dict | None) -> dict:
    c = om_data.get("current", {})

    # --- temperature & feels-like
    om_temp = c.get("temperature_2m")
    om_feel = c.get("apparent_temperature")
    owm_temp = owm_current["main"]["temp"] if owm_current else None
    owm_feel = owm_current["main"]["feels_like"] if owm_current else None

    temp = _weighted_avg([om_temp, owm_temp], [1.0, 1.0])
    feels = _weighted_avg([om_feel, owm_feel], [1.0, 1.0])

    # --- humidity
    om_hum = c.get("relative_humidity_2m")
    owm_hum = owm_current["main"]["humidity"] if owm_current else None
    humidity = _weighted_avg([om_hum, owm_hum], [1.0, 1.0])

    # --- wind speed (km/h) and gusts
    om_wind = c.get("wind_speed_10m")
    owm_wind = (owm_current["wind"]["speed"] * 3.6) if owm_current else None
    wind_speed = _weighted_avg([om_wind, owm_wind], [1.0, 1.0])

    om_gusts = c.get("wind_gusts_10m")
    owm_gusts = (
        owm_current["wind"].get("gust", owm_current["wind"]["speed"]) * 3.6
        if owm_current
        else None
    )
    wind_gusts = _weighted_avg([om_gusts, owm_gusts], [1.2, 0.8])

    wind_dir = c.get("wind_direction_10m")

    # --- precipitation
    om_prec = c.get("precipitation", 0.0)
    owm_prec = (
        owm_current.get("rain", {}).get("1h", 0.0)
        if owm_current
        else None
    )
    precipitation = _weighted_avg([om_prec, owm_prec or 0.0], [1.0, 1.0])

    # --- weather code / description
    om_code = c.get("weather_code")
    owm_code_raw = owm_current["weather"][0]["id"] if owm_current else None
    owm_code = _owm_id_to_wmo(owm_code_raw) if owm_code_raw is not None else None

    # prefer OWM description when available (already in Romanian via lang=ro)
    if owm_current:
        description = owm_current["weather"][0].get("description", _wmo_desc(om_code)).capitalize()
        icon_emoji = _wmo_icon(owm_code or om_code)
    else:
        description = _wmo_desc(om_code)
        icon_emoji = _wmo_icon(om_code)

    # --- pressure, visibility
    pressure = (
        owm_current["main"].get("pressure") if owm_current else c.get("surface_pressure")
    )
    visibility_m = owm_current.get("visibility") if owm_current else c.get("visibility")
    visibility_km = round(visibility_m / 1000, 1) if visibility_m is not None else None

    # --- air quality
    aqi = None
    if owm_air and owm_air.get("list"):
        aqi = owm_air["list"][0]["main"]["aqi"]

    # --- moto score
    score = _moto_score(feels, wind_gusts, precipitation, om_code)

    return {
        "temperature": temp,
        "feels_like": feels,
        "humidity": humidity,
        "wind_speed_kmh": wind_speed,
        "wind_gusts_kmh": wind_gusts,
        "wind_direction_deg": wind_dir,
        "wind_direction": _wind_direction_label(wind_dir),
        "beaufort": _beaufort(wind_speed),
        "precipitation_mm": precipitation,
        "weather_code": om_code,
        "description": description,
        "icon": icon_emoji,
        "pressure_hpa": pressure,
        "visibility_km": visibility_km,
        "aqi": aqi,
        "moto_score": score,
        "moto_label": _moto_label(score),
        "sources": ["open-meteo"] + (["openweathermap"] if owm_current else []),
    }


def _merge_daily(om_data: dict, owm_forecast: dict | None) -> list[dict]:
    daily = om_data.get("daily", {})
    dates = daily.get("time", [])
    result = []

    # Build a per-date dict from OWM 3-hour forecast
    owm_by_date: dict[str, list[dict]] = {}
    if owm_forecast:
        for item in owm_forecast.get("list", []):
            date_str = item["dt_txt"][:10]
            owm_by_date.setdefault(date_str, []).append(item)

    for i, date in enumerate(dates):
        om_code = _safe(daily.get("weather_code"), i)
        owm_items = owm_by_date.get(date, [])

        # temperatures
        t_max_om = _safe(daily.get("temperature_2m_max"), i)
        t_min_om = _safe(daily.get("temperature_2m_min"), i)
        fa_max_om = _safe(daily.get("apparent_temperature_max"), i)
        fa_min_om = _safe(daily.get("apparent_temperature_min"), i)

        if owm_items:
            owm_temps = [x["main"]["temp"] for x in owm_items]
            owm_feels = [x["main"]["feels_like"] for x in owm_items]
            t_max_owm = max(owm_temps)
            t_min_owm = min(owm_temps)
            fa_max_owm = max(owm_feels)
            fa_min_owm = min(owm_feels)
        else:
            t_max_owm = t_min_owm = fa_max_owm = fa_min_owm = None

        t_max = _weighted_avg([t_max_om, t_max_owm], [1.0, 1.0])
        t_min = _weighted_avg([t_min_om, t_min_owm], [1.0, 1.0])
        fa_max = _weighted_avg([fa_max_om, fa_max_owm], [1.0, 1.0])
        fa_min = _weighted_avg([fa_min_om, fa_min_owm], [1.0, 1.0])

        # precipitation
        prec_om = _safe(daily.get("precipitation_sum"), i) or 0.0
        if owm_items:
            prec_owm = sum(
                x.get("rain", {}).get("3h", 0.0) + x.get("snow", {}).get("3h", 0.0)
                for x in owm_items
            )
        else:
            prec_owm = None
        precipitation = _weighted_avg([prec_om, prec_owm], [1.0, 1.0]) if prec_owm is not None else prec_om

        # wind gusts
        gusts_om = _safe(daily.get("wind_gusts_10m_max"), i)
        if owm_items:
            gusts_owm = max(
                (x["wind"].get("gust", x["wind"]["speed"]) * 3.6 for x in owm_items),
                default=None,
            )
        else:
            gusts_owm = None
        wind_gusts = _weighted_avg([gusts_om, gusts_owm], [1.2, 0.8])

        wind_max = _safe(daily.get("wind_speed_10m_max"), i)

        prec_prob = _safe(daily.get("precipitation_probability_max"), i) or 0

        # weather code: prefer majority OWM code mapped to WMO
        if owm_items:
            owm_codes = [_owm_id_to_wmo(x["weather"][0]["id"]) for x in owm_items]
            owm_day_code = max(set(owm_codes), key=owm_codes.count)
        else:
            owm_day_code = None

        final_code = owm_day_code if owm_day_code is not None else om_code

        score = _moto_score(fa_max, wind_gusts, precipitation, final_code)

        result.append({
            "date": date,
            "weather_code": final_code,
            "icon": _wmo_icon(final_code),
            "description": _wmo_desc(final_code),
            "temp_max": t_max,
            "temp_min": t_min,
            "feels_max": fa_max,
            "feels_min": fa_min,
            "precipitation_mm": round(precipitation, 1) if precipitation is not None else 0.0,
            "precipitation_probability": prec_prob,
            "wind_max_kmh": wind_max,
            "wind_gusts_kmh": wind_gusts,
            "moto_score": score,
            "moto_label": _moto_label(score),
        })

    return result


def _build_hourly(om_data: dict) -> list[dict]:
    hourly = om_data.get("hourly", {})
    times = hourly.get("time", [])
    result = []
    for i, t in enumerate(times):
        code = _safe(hourly.get("weather_code"), i)
        result.append({
            "time": t,
            "temperature": _safe(hourly.get("temperature_2m"), i),
            "feels_like": _safe(hourly.get("apparent_temperature"), i),
            "precipitation_mm": _safe(hourly.get("precipitation"), i),
            "precipitation_probability": _safe(hourly.get("precipitation_probability"), i),
            "wind_speed_kmh": _safe(hourly.get("wind_speed_10m"), i),
            "wind_gusts_kmh": _safe(hourly.get("wind_gusts_10m"), i),
            "weather_code": code,
            "icon": _wmo_icon(code),
            "description": _wmo_desc(code),
        })
    return result


def _safe(lst: list | None, i: int) -> Any:
    if lst is None or i >= len(lst):
        return None
    return lst[i]


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def get_weather(
    lat: float,
    lon: float,
    city_name: str,
    owm_api_key: str,
) -> dict[str, Any]:
    """
    Fetch and aggregate weather data from Open-Meteo and OpenWeatherMap.
    Returns a unified JSON-serialisable dict.
    """
    async with httpx.AsyncClient() as client:
        # Launch all requests concurrently
        om_task = _fetch_openmeteo(lat, lon, client)
        owm_cur_task = _fetch_owm_current(lat, lon, owm_api_key, client)
        owm_fc_task = _fetch_owm_forecast(lat, lon, owm_api_key, client)
        owm_air_task = _fetch_owm_air(lat, lon, owm_api_key, client)

        om_data, owm_current, owm_forecast, owm_air = await asyncio.gather(
            om_task, owm_cur_task, owm_fc_task, owm_air_task
        )

    current = _merge_current(om_data, owm_current, owm_air)
    daily = _merge_daily(om_data, owm_forecast)
    hourly = _build_hourly(om_data)

    return {
        "city": city_name,
        "latitude": lat,
        "longitude": lon,
        "timezone": om_data.get("timezone", "UTC"),
        "current": current,
        "daily": daily,
        "hourly": hourly,
    }
