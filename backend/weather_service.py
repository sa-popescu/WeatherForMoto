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
from datetime import datetime, timedelta
from typing import Any

import httpx

# ---------------------------------------------------------------------------
# Source URLs
# ---------------------------------------------------------------------------
OWM_BASE = "https://api.openweathermap.org"
OPENMETEO_BASE = "https://api.open-meteo.com/v1/forecast"
GEOCODING_OPENMETEO = "https://geocoding-api.open-meteo.com/v1/search"
OPENMETEO_AIR_BASE = "https://air-quality-api.open-meteo.com/v1/air-quality"
OWM_GEO_URL = f"{OWM_BASE}/geo/1.0/direct"
MET_NO_BASE = "https://api.met.no/weatherapi/locationforecast/2.0/complete"
PIRATE_WEATHER_BASE = "https://api.pirateweather.net/forecast"


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


_LIGHT_RAIN_CODES = {51, 53, 55, 61, 80}
_HEAVY_RAIN_CODES = {63, 65, 81, 82}
_STORM_CODES = {95, 96, 99}


def _precip_probability_penalty(probability: int | None, *, daily: bool) -> int:
    prob = probability or 0
    if daily:
        if prob >= 80:
            return 55
        if prob >= 60:
            return 40
        if prob >= 40:
            return 28
        if prob >= 20:
            return 18
        if prob > 0:
            return 8
        return 0

    if prob >= 80:
        return 60
    if prob >= 60:
        return 45
    if prob >= 40:
        return 35
    if prob >= 20:
        return 25
    if prob > 0:
        return 12
    return 0


def _precip_amount_penalty(amount_mm: float | None, *, daily: bool) -> int:
    amount = amount_mm or 0
    if daily:
        if amount > 20:
            return 55
        if amount > 10:
            return 35
        if amount > 5:
            return 20
        if amount > 1:
            return 10
        if amount > 0:
            return 6
        return 0

    if amount > 5:
        return 60
    if amount > 1:
        return 38
    if amount > 0.2:
        return 22
    if amount > 0:
        return 10
    return 0


def _weather_code_penalty(code: int | None) -> int:
    weather_code = code or 0
    if weather_code in _STORM_CODES:
        return 40
    if weather_code in _HEAVY_RAIN_CODES:
        return 20
    if weather_code in _LIGHT_RAIN_CODES:
        return 10
    if weather_code in (45, 48):
        return 18
    return 0


def _wind_penalty(wind_gusts_kmh: float | None) -> int:
    gusts = wind_gusts_kmh or 0
    if gusts > 70:
        return 30
    if gusts > 50:
        return 15
    if gusts > 35:
        return 8
    return 0


def _hourly_temperature_penalty(feels_like: float | None) -> int:
    feels = feels_like if feels_like is not None else 20
    if feels < 5:
        return 25
    if feels < 10:
        return 12
    if feels > 36:
        return 10
    return 0


def _daily_temperature_penalty(feels_min: float | None, feels_max: float | None) -> int:
    penalty = 0
    min_feels = feels_min if feels_min is not None else 12
    max_feels = feels_max if feels_max is not None else 24
    if min_feels < 5:
        penalty += 20
    elif min_feels < 10:
        penalty += 10
    if max_feels > 36:
        penalty += 10
    return penalty


def _precipitation_cap(
    precipitation_mm: float | None,
    precipitation_probability: int | None,
    weather_code: int | None,
    *,
    daily: bool,
) -> int:
    amount = precipitation_mm or 0
    prob = precipitation_probability or 0
    code = weather_code or 0
    cap = 100

    has_rain_risk = prob > 0 or amount > 0 or code in (_LIGHT_RAIN_CODES | _HEAVY_RAIN_CODES | _STORM_CODES)
    if has_rain_risk:
        cap = 79

    if daily:
        if prob >= 40 or amount > 5 or code in _HEAVY_RAIN_CODES:
            cap = min(cap, 69)
        if prob >= 60 or amount > 10:
            cap = min(cap, 59)
        if prob >= 80 or amount > 20 or code in _STORM_CODES:
            cap = min(cap, 39)
    else:
        if prob >= 40 or amount > 0.2 or code in _HEAVY_RAIN_CODES:
            cap = min(cap, 69)
        if prob >= 60 or amount > 1:
            cap = min(cap, 59)
        if prob >= 80 or amount > 5 or code in _STORM_CODES:
            cap = min(cap, 39)

    return cap


def _moto_score(
    feels_like: float | None,
    wind_gusts_kmh: float | None,
    precipitation_mm: float | None,
    weather_code: int | None,
    precipitation_probability: int | None = 0,
) -> int:
    """
    Compute a 0-100 'moto suitability' score.
    Higher = better riding conditions.
    """
    score = 100
    score -= max(
        _precip_amount_penalty(precipitation_mm, daily=False),
        _precip_probability_penalty(precipitation_probability, daily=False),
        _weather_code_penalty(weather_code),
    )
    score -= _wind_penalty(wind_gusts_kmh)
    score -= _hourly_temperature_penalty(feels_like)
    score = min(score, _precipitation_cap(
        precipitation_mm,
        precipitation_probability,
        weather_code,
        daily=False,
    ))
    return max(0, min(100, round(score)))


def _moto_score_daily(
    feels_min: float | None,
    feels_max: float | None,
    wind_gusts_kmh: float | None,
    precipitation_mm_day: float | None,
    weather_code: int | None,
    precipitation_probability: int | None = 0,
) -> int:
    """
    Daily 0-100 moto score.

    Uses daily precipitation totals with dedicated thresholds (mm/day),
    avoiding over-penalization from hourly thresholds.
    """
    score = 100
    score -= max(
        _precip_amount_penalty(precipitation_mm_day, daily=True),
        _precip_probability_penalty(precipitation_probability, daily=True),
        _weather_code_penalty(weather_code),
    )
    score -= _wind_penalty(wind_gusts_kmh)
    score -= _daily_temperature_penalty(feels_min, feels_max)
    score = min(score, _precipitation_cap(
        precipitation_mm_day,
        precipitation_probability,
        weather_code,
        daily=True,
    ))
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


def _gear_recommendation(
    feels_like: float | None,
    wind_gusts_kmh: float | None,
    precipitation_mm: float | None,
    weather_code: int | None,
) -> list[dict]:
    """
    Return gear recommendations based on weather conditions.
    Each item: {category, item, reason, urgency: 'info'|'warn'|'required', icon}
    """
    f = feels_like if feels_like is not None else 20
    g = wind_gusts_kmh or 0
    p = precipitation_mm or 0
    code = weather_code or 0
    recs: list[dict] = []

    # ── Rain / waterproofing ──────────────────────────────────────────────
    raining = p > 0.2 or code in (51, 53, 55, 61, 63, 65, 80, 81, 82, 95, 96, 99)
    if raining:
        urgency = "required" if p > 1 or code in (63, 65, 81, 82, 95, 96, 99) else "warn"
        recs.append({
            "category": "ploaie",
            "item": "Costum impermeabil / oversuit",
            "reason": f"Precipitații active ({p:.1f} mm/h) — rămâi uscat și cald",
            "urgency": urgency,
            "icon": "🌧️",
        })
        recs.append({
            "category": "mănuși_ploaie",
            "item": "Mănuși impermeabile",
            "reason": "Mâinile ude reduc controlul și răspunsul la frână",
            "urgency": urgency,
            "icon": "🧤",
        })

    # ── Jacket ───────────────────────────────────────────────────────────
    if f < 0:
        recs.append({
            "category": "geacă",
            "item": "Geacă de iarnă cu protecții CE + liner termic",
            "reason": f"Temperatură resimțită {f:.0f}°C — condiții extreme de frig",
            "urgency": "required",
            "icon": "🧥",
        })
        recs.append({
            "category": "strat_baza",
            "item": "Strat termic de bază (top + pantaloni)",
            "reason": "Protecție împotriva hipotermiei sub 0°C",
            "urgency": "required",
            "icon": "🎿",
        })
    elif f < 10:
        recs.append({
            "category": "geacă",
            "item": "Geacă de moto 3 sezoane cu liner termic activ",
            "reason": f"Temperatură resimțită {f:.0f}°C — vreme rece",
            "urgency": "required",
            "icon": "🧥",
        })
        recs.append({
            "category": "strat_baza",
            "item": "Strat termic de bază",
            "reason": "Confort termic la temperaturi scăzute",
            "urgency": "warn",
            "icon": "🎿",
        })
    elif f < 18:
        recs.append({
            "category": "geacă",
            "item": "Geacă de moto 3 sezoane (fără liner sau cu liner subțire)",
            "reason": f"Temperatură resimțită {f:.0f}°C — vreme răcoroasă",
            "urgency": "warn",
            "icon": "🧥",
        })
    elif f < 28:
        recs.append({
            "category": "geacă",
            "item": "Geacă de moto din textil / piele cu protecții",
            "reason": f"Temperatură resimțită {f:.0f}°C — condiții ideale",
            "urgency": "info",
            "icon": "🧥",
        })
    else:
        recs.append({
            "category": "geacă",
            "item": "Geacă mesh cu ventilație maximă + protecții",
            "reason": f"Temperatură resimțită {f:.0f}°C — căldură puternică",
            "urgency": "warn",
            "icon": "🧥",
        })
        recs.append({
            "category": "hidratare",
            "item": "Hidratare frecventă (min 500 ml/h)",
            "reason": "Risc de deshidratare și colaps termic",
            "urgency": "warn",
            "icon": "💧",
        })

    # ── Gloves ───────────────────────────────────────────────────────────
    if f < 5:
        recs.append({
            "category": "mănuși",
            "item": "Mănuși de iarnă / cu încălzire electrică",
            "reason": f"Sub {f:.0f}°C degetele amorțesc și pierzi controlul frenei",
            "urgency": "required",
            "icon": "🧤",
        })
    elif f < 12:
        recs.append({
            "category": "mănuși",
            "item": "Mănuși de moto cu dublură termică",
            "reason": f"Temperatura mâinilor scade rapid la {f:.0f}°C în mers",
            "urgency": "warn",
            "icon": "🧤",
        })
    elif not raining:
        recs.append({
            "category": "mănuși",
            "item": "Mănuși de moto standard cu protecții",
            "reason": "Protecție esențială la orice temperatură",
            "urgency": "info",
            "icon": "🧤",
        })

    # ── Pants ────────────────────────────────────────────────────────────
    if f < 5:
        recs.append({
            "category": "pantaloni",
            "item": "Pantaloni de moto cu liner termic + protecții CE",
            "reason": "Protecție termică și impact la temperaturi sub 5°C",
            "urgency": "required",
            "icon": "👖",
        })
    elif f < 15:
        recs.append({
            "category": "pantaloni",
            "item": "Pantaloni de moto cu protecții (opțional liner)",
            "reason": "Vreme răcoroasă — protejează genunchii și coapsele",
            "urgency": "warn",
            "icon": "👖",
        })
    else:
        recs.append({
            "category": "pantaloni",
            "item": "Pantaloni de moto textil / piele cu protecții",
            "reason": "Protecție la impact — obligatorie",
            "urgency": "info",
            "icon": "👖",
        })

    # ── Wind / visor ─────────────────────────────────────────────────────
    if g > 50:
        recs.append({
            "category": "vizor",
            "item": "Vizor complet închis + colier gât aerodinamic",
            "reason": f"Rafale de {g:.0f} km/h — turbulențe puternice, oboseală musculară",
            "urgency": "required",
            "icon": "⛑️",
        })
    elif g > 35:
        recs.append({
            "category": "vizor",
            "item": "Vizor intermediar sau complet",
            "reason": f"Rafale de {g:.0f} km/h — confort redus la viteze mari",
            "urgency": "warn",
            "icon": "⛑️",
        })

    # ── Fog / visibility ─────────────────────────────────────────────────
    if code in (45, 48):
        recs.append({
            "category": "vizibilitate",
            "item": "Vestă reflectorizantă fluorescent-galbenă",
            "reason": "Ceață densă — fii văzut de ceilalți participanți la trafic",
            "urgency": "required",
            "icon": "🦺",
        })

    # ── Ice / frost risk ─────────────────────────────────────────────────
    if f < 3:
        recs.append({
            "category": "anvelope",
            "item": "Verifică aderența anvelopelor + presiunea",
            "reason": f"Risc de gheață / brumă la {f:.0f}°C — aderență redusă drastic",
            "urgency": "required",
            "icon": "⚠️",
        })

    return recs


def _road_surface_temp(
    air_temp: float | None,
    humidity: float | None,
    weather_code: int | None,
    precipitation_mm: float | None,
) -> float | None:
    """
    Estimate road surface temperature.
    Dark asphalt absorbs solar radiation — on sunny days it can be
    significantly warmer than the air. Wet roads approach air temp.
    """
    if air_temp is None:
        return None

    t = air_temp
    h = humidity or 60
    code = weather_code or 0
    p = precipitation_mm or 0

    if p > 0.5 or code in (51, 53, 55, 61, 63, 65, 80, 81, 82, 95, 96, 99):
        # Wet road — evaporative cooling, close to air temp
        road_temp = t - 1.0
    elif code in (0, 1):
        # Clear sky — dark asphalt strongly absorbs solar radiation
        road_temp = t + 9.0 if t > 15 else t + 4.0
    elif code == 2:
        # Partly cloudy
        road_temp = t + 4.0
    elif code == 3:
        # Overcast
        road_temp = t + 1.0
    elif code in (45, 48):
        # Fog — high humidity, reduced solar
        road_temp = t - 0.5
    elif code in (71, 73, 75):
        # Snow — insulating layer, road near air temp
        road_temp = t
    else:
        road_temp = t + 1.0

    # High humidity reduces the solar heating effect
    if h > 85 and p < 0.2:
        road_temp -= 2.0

    return round(road_temp, 1)


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Compute great-circle distance in km between two points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return R * 2 * math.asin(math.sqrt(a))


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


def _met_symbol_to_wmo(symbol: str) -> int:
    """Map MET Norway symbol_code strings to WMO weather codes."""
    if not symbol:
        return 0
    s = symbol
    for suffix in ("_day", "_night", "_polartwilight"):
        if symbol.endswith(suffix):
            s = symbol[: -len(suffix)]
            break
    table = {
        "clearsky": 0, "fair": 1, "partlycloudy": 2, "cloudy": 3,
        "fog": 45,
        "lightrain": 61, "rain": 63, "heavyrain": 65,
        "lightrainshowers": 80, "rainshowers": 81, "heavyrainshowers": 82,
        "lightsleet": 68, "sleet": 68, "heavysleet": 67,
        "lightsleetshowers": 68, "sleetshowers": 68, "heavysleetshowers": 67,
        "lightsnow": 71, "snow": 73, "heavysnow": 75,
        "lightsnowshowers": 85, "snowshowers": 85, "heavysnowshowers": 86,
        "lightrainandthunder": 95, "rainandthunder": 95, "heavyrainandthunder": 99,
        "lightsleetandthunder": 95, "sleetandthunder": 95, "heavysleetandthunder": 99,
        "lightsnowandthunder": 95, "snowandthunder": 95, "heavysnowandthunder": 99,
    }
    return table.get(s, 0)


def _pw_icon_to_wmo(icon: str) -> int:
    """Map Pirate Weather (Dark Sky) icon strings to WMO weather codes."""
    table = {
        "clear-day": 0, "clear-night": 0,
        "partly-cloudy-day": 2, "partly-cloudy-night": 2,
        "cloudy": 3, "wind": 3, "fog": 45,
        "rain": 63, "sleet": 68, "snow": 73,
    }
    return table.get(icon or "", 0)


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

async def _fetch_openmeteo(
    lat: float, lon: float, client: httpx.AsyncClient, forecast_days: int = 7
) -> dict[str, Any]:
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
            "precipitation,weather_code,wind_speed_10m,wind_gusts_10m,uv_index"
        ),
        "daily": (
            "weather_code,temperature_2m_max,temperature_2m_min,"
            "apparent_temperature_max,apparent_temperature_min,"
            "precipitation_sum,wind_speed_10m_max,wind_gusts_10m_max,"
            "precipitation_probability_max,sunrise,sunset"
        ),
        "timezone": "auto",
        "forecast_days": min(max(int(forecast_days), 1), 16),
        "wind_speed_unit": "kmh",
    }
    resp = await client.get(OPENMETEO_BASE, params=params, timeout=15)
    if not resp.is_success:
        reason: str = resp.text[:200]
        try:
            reason = resp.json().get("reason", reason)
        except Exception:
            pass
        raise ValueError(f"Open-Meteo error {resp.status_code}: {reason}")
    data = resp.json()
    if data.get("error"):
        raise ValueError(f"Open-Meteo error: {data.get('reason', 'unknown error')}")
    return data


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

async def _fetch_openmeteo_air_quality(
    lat: float, lon: float, client: httpx.AsyncClient
) -> dict[str, Any] | None:
    try:
        resp = await client.get(
            OPENMETEO_AIR_BASE,
            params={
                "latitude": lat,
                "longitude": lon,
                "hourly": (
                    "pm10,pm2_5,ozone,european_aqi,us_aqi,"
                    "alder_pollen,birch_pollen,grass_pollen,mugwort_pollen,ragweed_pollen"
                ),
                "timezone": "auto",
                "forecast_days": 1,
            },
            timeout=10,
        )
        if not resp.is_success:
            return None
        return resp.json()
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Normalise & merge
# ---------------------------------------------------------------------------

def _merge_current(
    om_data: dict,
    owm_current: dict | None,
    owm_air: dict | None,
    om_air: dict | None = None,
) -> dict:
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
    precipitation = _weighted_avg([om_prec, owm_prec], [1.0, 1.0])

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

    pm10 = None
    pm2_5 = None
    ozone = None
    eu_aqi = None
    us_aqi = None
    pollen_index = None
    if om_air and om_air.get("hourly"):
        h = om_air["hourly"]

        def _first(name: str):
            vals = h.get(name) or []
            return vals[0] if vals else None

        pm10 = _first("pm10")
        pm2_5 = _first("pm2_5")
        ozone = _first("ozone")
        eu_aqi = _first("european_aqi")
        us_aqi = _first("us_aqi")

        pollen_vals = [
            _first("alder_pollen"),
            _first("birch_pollen"),
            _first("grass_pollen"),
            _first("mugwort_pollen"),
            _first("ragweed_pollen"),
        ]
        pollen_vals = [v for v in pollen_vals if v is not None]
        if pollen_vals:
            pollen_index = max(float(v) for v in pollen_vals)

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
        "pm10": pm10,
        "pm2_5": pm2_5,
        "ozone": ozone,
        "eu_aqi": eu_aqi,
        "us_aqi": us_aqi,
        "pollen_index": round(float(pollen_index), 1) if pollen_index is not None else None,
        "moto_score": score,
        "moto_label": _moto_label(score),
        "gear_recommendation": _gear_recommendation(feels, wind_gusts, precipitation, om_code),
        "road_surface_temp": _road_surface_temp(temp, humidity, om_code, precipitation),
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

        score = _moto_score_daily(fa_min, fa_max, wind_gusts, precipitation, final_code, prec_prob)

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
            "sunrise": _safe(daily.get("sunrise"), i),
            "sunset": _safe(daily.get("sunset"), i),
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
            "uv_index": _safe(hourly.get("uv_index"), i),
        })
    return result


def _safe(lst: list | None, i: int) -> Any:
    if lst is None or i >= len(lst):
        return None
    return lst[i]


# ---------------------------------------------------------------------------
# Route planner helpers
# ---------------------------------------------------------------------------

async def _fetch_waypoint_weather(
    lat: float, lon: float, eta_iso: str, client: httpx.AsyncClient
) -> dict[str, Any]:
    """
    Fetch an hourly weather snapshot for a waypoint at a given ETA.
    Matches the closest forecast hour to the ETA timestamp.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": (
            "temperature_2m,apparent_temperature,precipitation_probability,"
            "precipitation,weather_code,wind_speed_10m,wind_gusts_10m"
        ),
        "timezone": "auto",
        "forecast_days": 7,
        "wind_speed_unit": "kmh",
    }
    try:
        resp = await client.get(OPENMETEO_BASE, params=params, timeout=15)
        if not resp.is_success:
            return {}
        data = resp.json()
        if data.get("error"):
            return {}

        hourly = data.get("hourly", {})
        times = hourly.get("time", [])

        # Find the first hour >= ETA (truncated to the hour)
        eta_prefix = eta_iso[:13]  # "YYYY-MM-DDTHH"
        best_idx = 0
        for j, t in enumerate(times):
            if t[:13] >= eta_prefix:
                best_idx = j
                break

        code = _safe(hourly.get("weather_code"), best_idx)
        feels = _safe(hourly.get("apparent_temperature"), best_idx)
        gusts = _safe(hourly.get("wind_gusts_10m"), best_idx)
        prec = _safe(hourly.get("precipitation"), best_idx)
        prec_prob = _safe(hourly.get("precipitation_probability"), best_idx)
        score = _moto_score(feels, gusts, prec, code, prec_prob)

        return {
            "time": times[best_idx] if best_idx < len(times) else eta_iso,
            "temperature": _safe(hourly.get("temperature_2m"), best_idx),
            "feels_like": feels,
            "precipitation_mm": prec,
            "precipitation_probability": _safe(hourly.get("precipitation_probability"), best_idx),
            "wind_speed_kmh": _safe(hourly.get("wind_speed_10m"), best_idx),
            "wind_gusts_kmh": gusts,
            "weather_code": code,
            "icon": _wmo_icon(code),
            "description": _wmo_desc(code),
            "moto_score": score,
            "moto_label": _moto_label(score),
        }
    except Exception:
        return {}


async def get_route_weather(
    origin_lat: float,
    origin_lon: float,
    origin_name: str,
    dest_lat: float,
    dest_lon: float,
    dest_name: str,
    departure_iso: str,
    avg_speed_kmh: float,
    owm_api_key: str,
    num_segments: int = 4,
) -> dict[str, Any]:
    """
    Compute weather along a motorcycle route from origin to destination.

    Uses linear (great-circle) interpolation for intermediate waypoints.
    Returns num_segments+1 waypoints with weather snapshots at estimated ETAs.
    """
    try:
        dep_dt = datetime.fromisoformat(departure_iso)
    except ValueError:
        dep_dt = datetime.now()

    total_dist_km = _haversine_km(origin_lat, origin_lon, dest_lat, dest_lon)
    total_hours = total_dist_km / avg_speed_kmh if avg_speed_kmh > 0 else 0

    # Build waypoint metadata (start, N-1 intermediate, destination)
    waypoints_meta: list[dict[str, Any]] = []
    for i in range(num_segments + 1):
        frac = i / num_segments
        wp_lat = origin_lat + frac * (dest_lat - origin_lat)
        wp_lon = origin_lon + frac * (dest_lon - origin_lon)
        hours_offset = frac * total_hours
        wp_eta = dep_dt + timedelta(hours=hours_offset)

        if i == 0:
            wp_name = origin_name
        elif i == num_segments:
            wp_name = dest_name
        else:
            wp_name = f"~{round(frac * total_dist_km)} km"

        waypoints_meta.append({
            "index": i,
            "name": wp_name,
            "lat": round(wp_lat, 4),
            "lon": round(wp_lon, 4),
            "distance_from_origin_km": round(frac * total_dist_km, 1),
            "eta_iso": wp_eta.isoformat()[:16],
        })

    # Fetch weather for all waypoints concurrently
    async with httpx.AsyncClient() as client:
        tasks = [
            _fetch_waypoint_weather(wp["lat"], wp["lon"], wp["eta_iso"], client)
            for wp in waypoints_meta
        ]
        weather_results = await asyncio.gather(*tasks)

    result_waypoints = [
        {**wp, "weather": weather}
        for wp, weather in zip(waypoints_meta, weather_results)
    ]

    return {
        "origin": {"name": origin_name, "lat": origin_lat, "lon": origin_lon},
        "destination": {"name": dest_name, "lat": dest_lat, "lon": dest_lon},
        "departure": departure_iso,
        "avg_speed_kmh": avg_speed_kmh,
        "total_distance_km": round(total_dist_km, 1),
        "estimated_duration_h": round(total_hours, 2),
        "waypoints": result_waypoints,
    }


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

async def get_weather(
    lat: float,
    lon: float,
    city_name: str,
    owm_api_key: str,
    forecast_days: int = 7,
) -> dict[str, Any]:
    """
    Fetch and aggregate weather data from Open-Meteo and OpenWeatherMap.
    Returns a unified JSON-serialisable dict.
    forecast_days: 7 (free) or 14 (premium — Open-Meteo supports up to 16).
    """
    async with httpx.AsyncClient() as client:
        # Launch all requests concurrently
        om_task = _fetch_openmeteo(lat, lon, client, forecast_days=forecast_days)
        owm_cur_task = _fetch_owm_current(lat, lon, owm_api_key, client)
        owm_fc_task = _fetch_owm_forecast(lat, lon, owm_api_key, client)
        owm_air_task = _fetch_owm_air(lat, lon, owm_api_key, client)
        om_air_task = _fetch_openmeteo_air_quality(lat, lon, client)

        om_data, owm_current, owm_forecast, owm_air, om_air = await asyncio.gather(
            om_task,
            owm_cur_task,
            owm_fc_task,
            owm_air_task,
            om_air_task,
        )

    current = _merge_current(om_data, owm_current, owm_air, om_air)
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


# ---------------------------------------------------------------------------
# Multi-stop route weather (premium)
# ---------------------------------------------------------------------------

async def get_multi_route_weather(
    stops: list[dict],
    departure_iso: str,
    avg_speed_kmh: float,
    owm_api_key: str,
) -> dict[str, Any]:
    """
    Compute weather along a multi-stop motorcycle route.

    Parameters
    ----------
    stops: list of dicts with keys ``name``, ``lat``, ``lon`` (2–5 items).
    departure_iso: ISO-8601 departure time string.
    avg_speed_kmh: average speed in km/h.
    owm_api_key: OpenWeatherMap API key.

    Returns a dict with ``segments`` (one entry per consecutive stop pair),
    ``total_distance_km``, ``estimated_duration_h``, and ``stops`` metadata.
    """
    try:
        dep_dt = datetime.fromisoformat(departure_iso)
    except ValueError:
        dep_dt = datetime.now()

    segments: list[dict[str, Any]] = []
    elapsed_hours = 0.0
    total_dist_km = 0.0

    all_waypoints: list[dict[str, Any]] = []

    for seg_idx in range(len(stops) - 1):
        origin = stops[seg_idx]
        dest = stops[seg_idx + 1]

        seg_dist_km = _haversine_km(origin["lat"], origin["lon"], dest["lat"], dest["lon"])
        seg_hours = seg_dist_km / avg_speed_kmh if avg_speed_kmh > 0 else 0

        num_seg = 3  # intermediate points per segment
        seg_waypoints: list[dict[str, Any]] = []
        for i in range(num_seg + 1):
            frac = i / num_seg
            wp_lat = origin["lat"] + frac * (dest["lat"] - origin["lat"])
            wp_lon = origin["lon"] + frac * (dest["lon"] - origin["lon"])
            wp_eta = dep_dt + timedelta(hours=elapsed_hours + frac * seg_hours)

            if i == 0:
                wp_name = origin["name"]
            elif i == num_seg:
                wp_name = dest["name"]
            else:
                wp_name = f"~{round(frac * seg_dist_km + total_dist_km)} km"

            seg_waypoints.append({
                "index": len(all_waypoints) + i,
                "name": wp_name,
                "lat": round(wp_lat, 4),
                "lon": round(wp_lon, 4),
                "distance_from_start_km": round(total_dist_km + frac * seg_dist_km, 1),
                "eta_iso": wp_eta.isoformat()[:16],
            })

        all_waypoints.extend(seg_waypoints[:-1] if seg_idx < len(stops) - 2 else seg_waypoints)
        total_dist_km += seg_dist_km
        elapsed_hours += seg_hours

        segments.append({
            "from": origin["name"],
            "to": dest["name"],
            "distance_km": round(seg_dist_km, 1),
            "duration_h": round(seg_hours, 2),
            "waypoints": seg_waypoints,
        })

    # Fetch weather for unique waypoints concurrently
    async with httpx.AsyncClient() as client:
        tasks = [
            _fetch_waypoint_weather(wp["lat"], wp["lon"], wp["eta_iso"], client)
            for wp in all_waypoints
        ]
        weather_results = await asyncio.gather(*tasks)

    # Attach weather to waypoints
    for wp, weather in zip(all_waypoints, weather_results):
        wp["weather"] = weather

    # Map weather back to segment waypoints by index
    weather_by_index = {wp["index"]: wp["weather"] for wp in all_waypoints}
    for seg in segments:
        for wp in seg["waypoints"]:
            wp["weather"] = weather_by_index.get(wp["index"], {})

    return {
        "stops": [{"name": s["name"], "lat": s["lat"], "lon": s["lon"]} for s in stops],
        "departure": departure_iso,
        "avg_speed_kmh": avg_speed_kmh,
        "total_distance_km": round(total_dist_km, 1),
        "estimated_duration_h": round(elapsed_hours, 2),
        "segments": segments,
    }
