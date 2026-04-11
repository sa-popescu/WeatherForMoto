"""Unit tests for weather_service aggregation logic (no network required)."""

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))

from weather_service import (
    _weighted_avg,
    _wind_direction_label,
    _beaufort,
    _moto_score,
    _moto_label,
    _wmo_desc,
    _wmo_icon,
    _owm_id_to_wmo,
    _merge_current,
    _merge_daily,
    _build_hourly,
    _gear_recommendation,
    _road_surface_temp,
    _haversine_km,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def test_weighted_avg_basic():
    assert _weighted_avg([10.0, 20.0], [1.0, 1.0]) == 15.0


def test_weighted_avg_none_ignored():
    assert _weighted_avg([10.0, None], [1.0, 1.0]) == 10.0


def test_weighted_avg_all_none():
    assert _weighted_avg([None, None], [1.0, 1.0]) is None


def test_wind_direction_label():
    assert _wind_direction_label(0) == "N"
    assert _wind_direction_label(90) == "E"
    assert _wind_direction_label(180) == "S"
    assert _wind_direction_label(270) == "V"
    assert _wind_direction_label(None) == "—"


def test_beaufort():
    assert _beaufort(0) == "0"
    assert _beaufort(None) == "—"
    # ~20 km/h ≈ 5.5 m/s → Beaufort 4
    assert _beaufort(20) == "4"


def test_moto_score_ideal():
    score = _moto_score(feels_like=20, wind_gusts_kmh=10, precipitation_mm=0, weather_code=0)
    assert score == 100


def test_moto_score_rain():
    score = _moto_score(feels_like=20, wind_gusts_kmh=10, precipitation_mm=6, weather_code=63)
    assert score < 60


def test_moto_score_thunderstorm():
    score = _moto_score(feels_like=20, wind_gusts_kmh=10, precipitation_mm=2, weather_code=95)
    assert score <= 30


def test_moto_label():
    assert _moto_label(100) == "IDEAL"
    assert _moto_label(75) == "OK"
    assert _moto_label(50) == "ACCEPTABIL"
    assert _moto_label(30) == "RISCANT"
    assert _moto_label(10) == "EVITĂ"


def test_wmo_desc():
    assert _wmo_desc(0) == "Cer senin"
    assert _wmo_desc(None) == "—"
    assert _wmo_desc(9999) == "—"


def test_wmo_icon():
    assert _wmo_icon(0) == "☀️"
    assert _wmo_icon(None) == "🌡️"


def test_owm_id_to_wmo():
    assert _owm_id_to_wmo(800) == 0   # clear sky
    assert _owm_id_to_wmo(500) == 61  # light rain
    assert _owm_id_to_wmo(200) == 95  # thunderstorm


# ---------------------------------------------------------------------------
# Merge current
# ---------------------------------------------------------------------------

def _make_om_data():
    return {
        "current": {
            "temperature_2m": 18.0,
            "apparent_temperature": 16.0,
            "relative_humidity_2m": 60,
            "wind_speed_10m": 20.0,
            "wind_gusts_10m": 30.0,
            "wind_direction_10m": 90.0,
            "precipitation": 0.0,
            "weather_code": 1,
            "surface_pressure": 1013,
            "visibility": 10000,
        },
        "hourly": {
            "time": ["2024-06-01T00:00", "2024-06-01T01:00"],
            "temperature_2m": [18.0, 17.5],
            "apparent_temperature": [16.0, 15.5],
            "precipitation": [0.0, 0.0],
            "precipitation_probability": [5, 5],
            "wind_speed_10m": [20.0, 18.0],
            "wind_gusts_10m": [30.0, 28.0],
            "weather_code": [1, 1],
        },
        "daily": {
            "time": ["2024-06-01", "2024-06-02"],
            "weather_code": [1, 2],
            "temperature_2m_max": [22.0, 20.0],
            "temperature_2m_min": [14.0, 13.0],
            "apparent_temperature_max": [21.0, 19.0],
            "apparent_temperature_min": [13.0, 12.0],
            "precipitation_sum": [0.0, 1.0],
            "wind_speed_10m_max": [25.0, 30.0],
            "wind_gusts_10m_max": [35.0, 40.0],
            "precipitation_probability_max": [10, 30],
        },
        "timezone": "Europe/Bucharest",
    }


def test_merge_current_no_owm():
    om_data = _make_om_data()
    result = _merge_current(om_data, None, None)
    assert result["temperature"] == 18.0
    assert result["wind_direction"] == "E"
    assert result["sources"] == ["open-meteo"]
    assert result["moto_score"] >= 0
    assert result["moto_score"] <= 100


def test_merge_current_with_owm():
    om_data = _make_om_data()
    owm_current = {
        "main": {
            "temp": 20.0,
            "feels_like": 18.0,
            "humidity": 55,
            "pressure": 1014,
        },
        "wind": {"speed": 5.0, "gust": 7.0},
        "weather": [{"id": 801, "description": "puțin înnorat"}],
        "visibility": 9000,
    }
    result = _merge_current(om_data, owm_current, None)
    # Average of 18 and 20
    assert result["temperature"] == 19.0
    assert "openweathermap" in result["sources"]


def test_merge_current_with_owm_and_aqi():
    om_data = _make_om_data()
    owm_current = {
        "main": {"temp": 20.0, "feels_like": 18.0, "humidity": 55, "pressure": 1014},
        "wind": {"speed": 5.0},
        "weather": [{"id": 800, "description": "cer senin"}],
        "visibility": 10000,
    }
    owm_air = {"list": [{"main": {"aqi": 1}}]}
    result = _merge_current(om_data, owm_current, owm_air)
    assert result["aqi"] == 1


def test_merge_daily():
    om_data = _make_om_data()
    daily = _merge_daily(om_data, None)
    assert len(daily) == 2
    assert daily[0]["date"] == "2024-06-01"
    assert daily[0]["temp_max"] == 22.0
    assert 0 <= daily[0]["moto_score"] <= 100


def test_build_hourly():
    om_data = _make_om_data()
    hourly = _build_hourly(om_data)
    assert len(hourly) == 2
    assert hourly[0]["time"] == "2024-06-01T00:00"
    assert hourly[0]["temperature"] == 18.0
    assert hourly[0]["icon"] == "🌤️"  # code 1


# ---------------------------------------------------------------------------
# Gear recommendation
# ---------------------------------------------------------------------------

def test_gear_recommendation_rain():
    recs = _gear_recommendation(feels_like=15, wind_gusts_kmh=20, precipitation_mm=2.0, weather_code=63)
    categories = [r["category"] for r in recs]
    assert "ploaie" in categories
    assert "mănuși_ploaie" in categories
    # Rain should be required urgency (>1mm)
    rain_rec = next(r for r in recs if r["category"] == "ploaie")
    assert rain_rec["urgency"] == "required"


def test_gear_recommendation_cold():
    recs = _gear_recommendation(feels_like=-5, wind_gusts_kmh=10, precipitation_mm=0, weather_code=0)
    categories = [r["category"] for r in recs]
    assert "geacă" in categories
    assert "strat_baza" in categories
    assert "mănuși" in categories
    assert "anvelope" in categories
    jacket = next(r for r in recs if r["category"] == "geacă")
    assert jacket["urgency"] == "required"


def test_gear_recommendation_ideal():
    recs = _gear_recommendation(feels_like=22, wind_gusts_kmh=15, precipitation_mm=0, weather_code=1)
    # Should still recommend jacket and gloves at info level
    categories = [r["category"] for r in recs]
    assert "geacă" in categories
    jacket = next(r for r in recs if r["category"] == "geacă")
    assert jacket["urgency"] == "info"


def test_gear_recommendation_fog():
    recs = _gear_recommendation(feels_like=15, wind_gusts_kmh=10, precipitation_mm=0, weather_code=45)
    categories = [r["category"] for r in recs]
    assert "vizibilitate" in categories


def test_gear_recommendation_strong_wind():
    recs = _gear_recommendation(feels_like=20, wind_gusts_kmh=60, precipitation_mm=0, weather_code=0)
    categories = [r["category"] for r in recs]
    assert "vizor" in categories
    visor = next(r for r in recs if r["category"] == "vizor")
    assert visor["urgency"] == "required"


# ---------------------------------------------------------------------------
# Road surface temperature
# ---------------------------------------------------------------------------

def test_road_surface_temp_sunny():
    t = _road_surface_temp(air_temp=20, humidity=50, weather_code=0, precipitation_mm=0)
    assert t is not None
    assert t > 20  # sunny day, road warmer than air


def test_road_surface_temp_rain():
    t = _road_surface_temp(air_temp=15, humidity=90, weather_code=63, precipitation_mm=3.0)
    assert t is not None
    assert t < 15  # wet road, slightly cooler


def test_road_surface_temp_none():
    t = _road_surface_temp(air_temp=None, humidity=60, weather_code=0, precipitation_mm=0)
    assert t is None


# ---------------------------------------------------------------------------
# Haversine distance
# ---------------------------------------------------------------------------

def test_haversine_km_same_point():
    d = _haversine_km(44.43, 26.10, 44.43, 26.10)
    assert d == 0.0


def test_haversine_km_bucharest_cluj():
    # Bucharest to Cluj-Napoca ~326 km straight line
    d = _haversine_km(44.43, 26.10, 46.77, 23.60)
    assert 300 < d < 360


# ---------------------------------------------------------------------------
# Merge current includes new fields
# ---------------------------------------------------------------------------

def test_merge_current_includes_gear_and_road_temp():
    om_data = _make_om_data()
    result = _merge_current(om_data, None, None)
    assert "gear_recommendation" in result
    assert isinstance(result["gear_recommendation"], list)
    assert "road_surface_temp" in result


# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    tests = [v for k, v in list(globals().items()) if k.startswith("test_")]
    passed = failed = 0
    for fn in tests:
        try:
            fn()
            print(f"  ✓  {fn.__name__}")
            passed += 1
        except Exception as e:
            print(f"  ✗  {fn.__name__}: {e}")
            failed += 1
    print(f"\n{passed} passed, {failed} failed")
    if failed:
        raise SystemExit(1)
