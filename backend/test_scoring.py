"""Tests for scoring model v2, source merging, caching and the time budget.

Run from backend/:  python -m unittest test_scoring -v   (or: python test_scoring.py)
No network: every HTTP call goes through httpx.MockTransport.
"""

import asyncio
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from unittest.mock import patch

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parent))

import weather_service as ws  # noqa: E402
from weather_service import (  # noqa: E402
    _MET_SYMBOL_TO_WMO,
    _WMO_MAP,
    _aggregate_met_daily,
    _build_hourly,
    _frost_risk,
    _merge_current,
    _merge_daily,
    _met_symbol_to_wmo,
    _moto_label,
    _moto_score,
    _normalize_met_current,
    _normalize_wxm_current,
    _owm_items_by_local_date,
    _pw_icon_to_wmo,
    _rain_impact,
    _rain_intensity,
    _resolve_departure,
    _road_surface_temp,
    _score_with_breakdown,
    _wmo_desc,
)

OFFSET_S = 3 * 3600  # Europe/Bucharest in summer
FIXED_NOW = datetime(2024, 6, 1, 10, 15)  # local wall clock used by the merge tests
CURRENT_HOUR_INDEX = 11  # first hourly slot at or after 10:15 is 11:00


def make_om_payload(now_local: datetime = FIXED_NOW, hours: int = 48, **hourly_overrides: Any) -> dict:
    """Open-Meteo-shaped payload: dry, mild, breezy; hourly from 00:00 of now_local's day."""
    start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
    times = [(start + timedelta(hours=i)).strftime("%Y-%m-%dT%H:%M") for i in range(hours)]
    n = len(times)
    hourly: dict[str, list] = {
        "time": times,
        "temperature_2m": [18.0] * n,
        "apparent_temperature": [18.0] * n,
        "precipitation_probability": [0] * n,
        "precipitation": [0.0] * n,
        "weather_code": [1] * n,
        "wind_speed_10m": [10.0] * n,
        "wind_gusts_10m": [20.0] * n,
        "wind_direction_10m": [90.0] * n,
        "uv_index": [3.0] * n,
        "relative_humidity_2m": [60] * n,
        "surface_pressure": [1000.0] * n,
        "pressure_msl": [1013.0] * n,
        "dew_point_2m": [10.0] * n,
        "cloud_cover": [20] * n,
        "visibility": [20000.0] * n,
        "is_day": [1 if 7 <= int(t[11:13]) < 20 else 0 for t in times],
    }
    for key, value in hourly_overrides.items():
        hourly[key] = value
    dates = sorted({t[:10] for t in times})
    d = len(dates)
    daily = {
        "time": dates,
        "weather_code": [1] * d,
        "temperature_2m_max": [22.0] * d,
        "temperature_2m_min": [14.0] * d,
        "apparent_temperature_max": [21.0] * d,
        "apparent_temperature_min": [13.0] * d,
        "precipitation_sum": [0.0] * d,
        "wind_speed_10m_max": [20.0] * d,
        "wind_gusts_10m_max": [30.0] * d,
        "precipitation_probability_max": [5] * d,
        "sunrise": [f"{x}T06:30" for x in dates],
        "sunset": [f"{x}T20:00" for x in dates],
    }
    current = {
        "time": now_local.strftime("%Y-%m-%dT%H:%M"),
        "temperature_2m": 18.0,
        "apparent_temperature": 18.0,
        "relative_humidity_2m": 60,
        "wind_speed_10m": 10.0,
        "wind_gusts_10m": 20.0,
        "wind_direction_10m": 90.0,
        "precipitation": 0.0,
        "weather_code": 1,
        "surface_pressure": 1000.0,
        "pressure_msl": 1013.0,
        "visibility": 20000.0,
        "is_day": 1,
    }
    return {
        "latitude": 44.43,
        "longitude": 26.10,
        "timezone": "Europe/Bucharest",
        "utc_offset_seconds": OFFSET_S,
        "current": current,
        "hourly": hourly,
        "daily": daily,
    }


def with_value_at(values: list, index: int, value: Any) -> list:
    """Copy of a list with one element replaced."""
    copy = list(values)
    copy[index] = value
    return copy


def utc_ts(year: int, month: int, day: int, hour: int) -> int:
    return int(datetime(year, month, day, hour, tzinfo=timezone.utc).timestamp())


# ---------------------------------------------------------------------------
# Rain = probability x intensity
# ---------------------------------------------------------------------------

class RainMatrixTests(unittest.TestCase):
    def test_70_percent_with_traces_is_not_real_rain(self) -> None:
        self.assertGreaterEqual(_moto_score(20, 10, 0.1, 61, 70), 80)
        self.assertGreaterEqual(_moto_score(20, 10, 0.1, 0, 70), 80)

    def test_80_percent_moderate_rain_is_at_most_59(self) -> None:
        self.assertLessEqual(_moto_score(20, 10, 3.2, 63, 80), 59)

    def test_90_percent_heavy_rain_is_at_most_39(self) -> None:
        self.assertLessEqual(_moto_score(20, 10, 12.0, 65, 90), 39)

    def test_high_probability_without_amount_counts_as_traces(self) -> None:
        score = _moto_score(20, 10, 0.0, 0, 70)
        self.assertLessEqual(score, 84)
        self.assertGreaterEqual(score, 80)

    def test_intensity_band_boundaries(self) -> None:
        cases = [(None, "none"), (0.0, "none"), (0.04, "none"), (0.05, "urme"), (0.49, "urme"),
                 (0.5, "slaba"), (2.49, "slaba"), (2.5, "moderata"), (7.49, "moderata"),
                 (7.5, "puternica"), (30.0, "puternica")]
        for amount, expected in cases:
            with self.subTest(amount=amount):
                self.assertEqual(_rain_intensity(amount), expected)

    def test_impact_matrix_cells(self) -> None:
        cases = [
            (20, 0.3, "none"), (20, 1.0, "none"), (20, 3.0, "low"), (20, 8.0, "medium"),
            (30, 1.0, "low"), (45, 0.3, "none"), (45, 3.0, "medium"), (60, 8.0, "high"),
            (61, 0.3, "low"), (70, 1.0, "medium"), (70, 3.0, "high"), (95, 8.0, "high"),
        ]
        for probability, amount, expected in cases:
            with self.subTest(probability=probability, amount=amount):
                self.assertEqual(_rain_impact(probability, _rain_intensity(amount)), expected)

    def test_score_is_explained_by_breakdown(self) -> None:
        score, breakdown = _score_with_breakdown(2, 55, 3.2, 63, 80, visibility_m=400)
        self.assertTrue(breakdown)
        for item in breakdown:
            self.assertEqual(set(item), {"factor", "penalty", "cap", "detail"})
            self.assertIsInstance(item["penalty"], int)
            self.assertTrue(item["cap"] is None or isinstance(item["cap"], int))
            self.assertIsInstance(item["detail"], str)
        expected = 100 - sum(item["penalty"] for item in breakdown)
        caps = [item["cap"] for item in breakdown if item["cap"] is not None]
        self.assertEqual(score, max(0, min([expected] + caps)))

    def test_no_input_is_not_scored(self) -> None:
        self.assertIsNone(_moto_score(None, None, None, None, None))
        self.assertEqual(_score_with_breakdown(None, None, None, None, None), (None, []))
        self.assertIsNone(_moto_label(None))


class LabelTests(unittest.TestCase):
    def test_label_boundaries(self) -> None:
        cases = {39: "EVITĂ", 40: "ATENȚIE", 59: "ATENȚIE", 60: "OK", 84: "OK", 85: "IDEAL"}
        for score, label in cases.items():
            with self.subTest(score=score):
                self.assertEqual(_moto_label(score), label)


# ---------------------------------------------------------------------------
# Cold, snow / ice, fog
# ---------------------------------------------------------------------------

class HazardTests(unittest.TestCase):
    def test_sub_zero_dry_day_is_never_ideal_or_ok(self) -> None:
        for feels in (-10, -6, -3, -0.5):
            with self.subTest(feels=feels):
                self.assertLessEqual(_moto_score(feels, 10, 0, 0, 0), 59)
        self.assertEqual(_moto_label(_moto_score(-10, 10, 0, 0, 0)), "EVITĂ")

    def test_cold_tiers_are_graded(self) -> None:
        scores = [_moto_score(feels, 10, 0, 0, 0) for feels in (-6, -1, 3, 8, 12, 18)]
        self.assertEqual(scores, sorted(scores))
        self.assertEqual(scores[-1], 100)

    def test_snow_and_ice_caps(self) -> None:
        self.assertLessEqual(_moto_score(1, 10, 1.0, 73, 80), 30)
        self.assertLessEqual(_moto_score(20, 10, None, 71, None), 30)
        self.assertLessEqual(_moto_score(1, 10, 0.5, 66, 80), 25)
        self.assertLessEqual(_moto_score(1, 10, 0.2, 56, 50), 25)

    def test_new_wmo_descriptions(self) -> None:
        for code in (56, 57, 66, 67, 77, 85, 86):
            with self.subTest(code=code):
                self.assertNotEqual(_wmo_desc(code), "—")

    def test_provider_codes_are_valid(self) -> None:
        for symbol, code in _MET_SYMBOL_TO_WMO.items():
            with self.subTest(symbol=symbol):
                self.assertIn(code, _WMO_MAP)
        self.assertIn(_met_symbol_to_wmo("sleet"), ws.SNOW_CODES)
        self.assertEqual(_met_symbol_to_wmo("lightrainshowersandthunder_day"), 95)
        self.assertIsNone(_met_symbol_to_wmo("not-a-symbol"))
        self.assertIsNone(_met_symbol_to_wmo(None))
        self.assertIn(_pw_icon_to_wmo("sleet"), ws.ICE_CODES)
        self.assertIsNone(_pw_icon_to_wmo("unknown-icon"))

    def test_fog_is_never_ignored_when_dry(self) -> None:
        self.assertLessEqual(_moto_score(15, 10, 0, 45, 0), 84)
        self.assertLessEqual(_moto_score(15, 10, 0, 45, 0, visibility_m=20000), 84)
        self.assertLessEqual(_moto_score(15, 10, 0, 45, 0, visibility_m=800), 74)
        self.assertLessEqual(_moto_score(15, 10, 0, 45, 0, visibility_m=400), 59)
        self.assertLessEqual(_moto_score(15, 10, 0, 45, 0, visibility_m=150), 39)

    def test_stale_storm_code_is_deweighted_only_when_probability_is_known_low(self) -> None:
        self.assertGreaterEqual(_moto_score(20, 10, 0, 95, 10), 85)
        self.assertLessEqual(_moto_score(20, 10, 0, 95, 40), 39)
        self.assertLessEqual(_moto_score(20, 10, 0, 95, None), 39)


class FrostAndRoadTests(unittest.TestCase):
    def test_clear_night_road_is_colder_than_air(self) -> None:
        self.assertLess(_road_surface_temp(2, 60, 0, 0, is_day=False), 2)
        self.assertGreater(_road_surface_temp(2, 60, 0, 0, is_day=True), 2)

    def test_frost_risk_without_precipitation(self) -> None:
        road = _road_surface_temp(2, 90, 0, 0, is_day=False)
        self.assertTrue(_frost_risk(2, road, 0.5, 0.0, 0))
        # Sunny afternoon at 2 °C: the asphalt is warmed well above freezing.
        road_day = _road_surface_temp(2, 60, 0, 0, is_day=True)
        self.assertFalse(_frost_risk(2, road_day, -5.0, 0.0, 0))
        # Dry air far below the surface temperature: no deposition.
        self.assertFalse(_frost_risk(0.5, 0.5, -12.0, 0.0, 0))

    def test_hourly_frost_caps_the_score(self) -> None:
        n = 48
        night_idx = 3  # 03:00, is_day = 0
        om = make_om_payload(
            temperature_2m=with_value_at([18.0] * n, night_idx, 2.0),
            apparent_temperature=with_value_at([18.0] * n, night_idx, 1.0),
            relative_humidity_2m=with_value_at([60] * n, night_idx, 92),
            dew_point_2m=with_value_at([10.0] * n, night_idx, 0.8),
            weather_code=with_value_at([1] * n, night_idx, 0),
        )
        hour = _build_hourly(om)[night_idx]
        self.assertFalse(hour["is_day"])
        self.assertTrue(hour["frost_risk"])
        self.assertLessEqual(hour["moto_score"], 59)


# ---------------------------------------------------------------------------
# Current block
# ---------------------------------------------------------------------------

class CurrentBlockTests(unittest.TestCase):
    def test_storm_code_uses_current_hour_probability(self) -> None:
        om = make_om_payload(precipitation_probability=with_value_at([0] * 48, CURRENT_HOUR_INDEX, 40))
        om["current"]["weather_code"] = 95
        current = _merge_current(om, None, None)
        self.assertEqual(current["precipitation_probability"], 40)
        self.assertLessEqual(current["moto_score"], 39)
        self.assertEqual(current["weather_code"], 95)

    def test_stale_storm_code_is_downgraded(self) -> None:
        om = make_om_payload(precipitation_probability=with_value_at([0] * 48, CURRENT_HOUR_INDEX, 10))
        om["current"]["weather_code"] = 95
        current = _merge_current(om, None, None)
        self.assertEqual(current["weather_code"], 3)
        self.assertGreaterEqual(current["moto_score"], 85)

    def test_current_contract_fields(self) -> None:
        current = _merge_current(make_om_payload(), None, None)
        for key in ("moto_score", "moto_label", "precipitation_probability", "rain_intensity",
                    "frost_risk", "score_breakdown", "is_day", "road_surface_temp"):
            self.assertIn(key, current)
        self.assertIsInstance(current["score_breakdown"], list)
        self.assertEqual(current["pressure_hpa"], 1013.0)  # pressure_msl, not surface

    def test_owm_clear_sky_code_zero_is_not_missing(self) -> None:
        om = make_om_payload()
        om["current"]["weather_code"] = 3
        owm_current = {
            "main": {"temp": 18.0, "feels_like": 18.0, "humidity": 60, "pressure": 1013},
            "wind": {"speed": 3.0},
            "weather": [{"id": 800, "description": "cer senin"}],
            "visibility": 10000,
        }
        current = _merge_current(om, owm_current, None)
        self.assertEqual(current["weather_code"], 0)
        # OWM without a gust value must not turn sustained wind into a gust.
        self.assertEqual(current["wind_gusts_kmh"], 20.0)


class WeatherXMTests(unittest.TestCase):
    @staticmethod
    def _stormy_om() -> dict:
        om = make_om_payload(precipitation_probability=with_value_at([0] * 48, CURRENT_HOUR_INDEX, 80))
        om["current"].update({"wind_gusts_10m": 70.0, "precipitation": 2.0, "weather_code": 63})
        return om

    def test_missing_sensors_fall_back_to_models(self) -> None:
        now = datetime.now(timezone.utc)
        wxm = _normalize_wxm_current({"observation": {
            "timestamp": (now - timedelta(minutes=5)).isoformat(),
            "temperature": 18.0, "feels_like": 18.0, "humidity": 60,
        }})
        self.assertIsNotNone(wxm)
        self.assertIsNone(wxm["wind_gusts_kmh"])
        self.assertIsNone(wxm["precipitation"])
        self.assertIsNone(wxm["wmo_code"])  # no icon → no code, not "clear sky"

        om = self._stormy_om()
        with_station = _merge_current(om, None, None, wxm_norm=wxm)
        without_station = _merge_current(om, None, None)
        self.assertEqual(with_station["wind_gusts_kmh"], 70.0)
        self.assertEqual(with_station["precipitation_mm"], 2.0)
        self.assertEqual(with_station["moto_score"], without_station["moto_score"])
        self.assertLessEqual(with_station["moto_score"], 59)

    def test_stale_observation_is_rejected(self) -> None:
        now = datetime.now(timezone.utc)
        stale = {"observation": {"timestamp": (now - timedelta(minutes=45)).isoformat(),
                                 "temperature": 30.0}}
        self.assertIsNone(_normalize_wxm_current(stale))
        self.assertIsNone(_normalize_wxm_current({"observation": {"temperature": 30.0}}))


# ---------------------------------------------------------------------------
# Daily merge: UTC vs local, coverage, severity, daily score
# ---------------------------------------------------------------------------

def owm_item(ts: int, owm_id: int = 800, temp: float = 20.0, gust: float | None = 5.0) -> dict:
    wind: dict[str, float] = {"speed": 3.0}
    if gust is not None:
        wind["gust"] = gust
    return {
        "dt": ts,
        "dt_txt": datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
        "main": {"temp": temp, "feels_like": temp},
        "wind": wind,
        "weather": [{"id": owm_id}],
    }


class DailyMergeTests(unittest.TestCase):
    def test_owm_utc_items_are_grouped_by_local_date(self) -> None:
        # 21:00 UTC on June 1 is 00:00 local (UTC+3) on June 2.
        items = [owm_item(utc_ts(2024, 6, 1, 21) + 3 * 3600 * k) for k in range(8)]
        grouped = _owm_items_by_local_date({"list": items}, OFFSET_S)
        self.assertEqual(list(grouped), ["2024-06-02"])
        self.assertEqual(len(grouped["2024-06-02"]), 8)

    def test_partial_days_are_not_merged(self) -> None:
        items = [owm_item(utc_ts(2024, 6, 1, 21) + 3 * 3600 * k) for k in range(4)]
        self.assertEqual(_owm_items_by_local_date({"list": items}, OFFSET_S), {})

    def test_daily_code_is_the_most_severe_daylight_code(self) -> None:
        n = 48
        codes = [1] * n
        codes[10] = 3        # June 1, 10:00 overcast
        codes[14] = 95       # June 1, 14:00 thunderstorm (daylight)
        codes[24 + 2] = 95   # June 2, 02:00 thunderstorm (night only)
        probability = with_value_at(with_value_at([0] * n, 14, 40), 24 + 2, 60)
        om = make_om_payload(weather_code=codes, precipitation_probability=probability)
        by_date = {d["date"]: d for d in _merge_daily(om, None, None, now_local="2000-01-01T00:00")}
        self.assertEqual(by_date["2024-06-01"]["weather_code"], 95)
        # A night storm does not put a storm icon on a dry riding day.
        self.assertEqual(by_date["2024-06-02"]["weather_code"], 1)

    def test_stale_daylight_code_does_not_drive_the_icon(self) -> None:
        n = 48
        om = make_om_payload(
            weather_code=with_value_at([1] * n, 12, 63),
            precipitation_probability=with_value_at([0] * n, 12, 5),
        )
        day = _merge_daily(om, None, None, now_local="2000-01-01T00:00")[0]
        self.assertEqual(day["weather_code"], 3)

    def test_whole_day_code_is_used_without_daylight_hours(self) -> None:
        om = make_om_payload(hours=24)  # hourly data only for June 1
        for values in om["daily"].values():
            values.append(values[0])
        om["daily"]["time"][1] = "2024-06-02"
        om["daily"]["sunrise"][1] = "2024-06-02T06:30"
        om["daily"]["sunset"][1] = "2024-06-02T20:00"
        items = [owm_item(utc_ts(2024, 6, 1, 21) + 3 * 3600 * k) for k in range(7)]
        items.append(owm_item(utc_ts(2024, 6, 2, 18), owm_id=211))  # one thunderstorm slot
        daily = _merge_daily(om, {"list": items}, None, now_local="2000-01-01T00:00")
        by_date = {d["date"]: d for d in daily}
        self.assertEqual(by_date["2024-06-02"]["weather_code"], 95)
        self.assertEqual(by_date["2024-06-01"]["weather_code"], 1)

    def test_met_daily_uses_local_dates_and_real_gusts(self) -> None:
        start = datetime(2024, 6, 1, 21, tzinfo=timezone.utc)
        timeseries = []
        for k in range(24):
            details: dict[str, float] = {"air_temperature": 15.0 + k % 5, "wind_speed": 5.0}
            if k == 10:
                details["wind_speed_of_gust"] = 12.0
            timeseries.append({
                "time": (start + timedelta(hours=k)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "data": {"instant": {"details": details},
                         "next_1_hours": {"summary": {"symbol_code": "cloudy"},
                                          "details": {"precipitation_amount": 0.0}}},
            })
        met = {"properties": {"timeseries": timeseries}}
        days = _aggregate_met_daily(met, OFFSET_S)
        self.assertEqual(list(days), ["2024-06-02"])
        self.assertAlmostEqual(days["2024-06-02"]["gust_max_kmh"], 43.2)
        self.assertAlmostEqual(days["2024-06-02"]["wind_max_kmh"], 18.0)

    def test_met_current_without_gust_keeps_none(self) -> None:
        met = {"properties": {"timeseries": [{
            "time": "2024-06-01T07:00:00Z",
            "data": {"instant": {"details": {"air_temperature": 15.0, "wind_speed": 8.0}},
                     "next_1_hours": {"summary": {"symbol_code": "rain"},
                                      "details": {"precipitation_amount": 1.2}}},
        }]}}
        norm = _normalize_met_current(met)
        self.assertIsNone(norm["wind_gusts_kmh"])
        self.assertAlmostEqual(norm["wind_speed_kmh"], 28.8)

    def test_evening_rain_leaves_the_day_ok(self) -> None:
        # Rain from 16:00 to 21:00, sunset ~19:40: only 4 of 13 daylight hours wet.
        n = 48
        rain = {16: (55, 0.6, 61), 17: (80, 3.2, 63), 18: (75, 2.4, 61), 19: (60, 1.1, 61),
                20: (60, 1.0, 61), 21: (50, 0.5, 61)}
        precipitation, probability, codes = [0.0] * n, [0] * n, [1] * n
        for hour, (prob, amount, code) in rain.items():
            probability[hour], precipitation[hour], codes[hour] = prob, amount, code
        om = make_om_payload(precipitation=precipitation, precipitation_probability=probability,
                             weather_code=codes)
        om["daily"]["sunset"] = [f"{x}T19:40" for x in om["daily"]["time"]]
        day = _merge_daily(om, None, None, now_local="2000-01-01T00:00")[0]
        self.assertEqual(day["moto_label"], "OK")
        self.assertEqual(day["rain_intensity_max"], "moderata")
        self.assertEqual(day["precipitation_max_mm_h"], 3.2)
        self.assertEqual(day["weather_code"], 63)

    def test_rain_most_of_the_day_is_still_avoid(self) -> None:
        wet = range(8, 19)  # 11 of the 13 daylight hours
        om = make_om_payload(
            precipitation=[3.2 if h in wet else 0.0 for h in range(48)],
            precipitation_probability=[80 if h in wet else 0 for h in range(48)],
            weather_code=[63 if h in wet else 1 for h in range(48)],
        )
        day = _merge_daily(om, None, None, now_local="2000-01-01T00:00")[0]
        self.assertEqual(day["moto_label"], "EVITĂ")

    def test_storm_most_of_the_afternoon_is_at_most_caution(self) -> None:
        stormy = range(11, 20)  # 9 of the 13 daylight hours
        om = make_om_payload(
            precipitation=[1.0 if h in stormy else 0.0 for h in range(48)],
            precipitation_probability=[50 if h in stormy else 0 for h in range(48)],
            weather_code=[95 if h in stormy else 1 for h in range(48)],
        )
        day = _merge_daily(om, None, None, now_local="2000-01-01T00:00")[0]
        self.assertLessEqual(day["moto_score"], 59)
        self.assertEqual(day["weather_code"], 95)

    def test_sustained_cap_uses_a_quarter_of_the_hours(self) -> None:
        def hour(cap_prob: int, amount: float) -> dict:
            return {"moto_score": 50, "is_day": True, "precipitation_mm": amount,
                    "precipitation_probability": cap_prob, "weather_code": 63}
        dry = {"moto_score": 100, "is_day": True, "precipitation_mm": 0.0,
               "precipitation_probability": 0, "weather_code": 1}
        # 1-4 scored hours: the worst hour's cap applies.
        self.assertEqual(ws._sustained_hazard_cap([hour(80, 3.2), dry, dry, dry]), 39)
        # 8 hours: index ceil(2) - 1 = 1, so two hours must share the cap.
        self.assertIsNone(ws._sustained_hazard_cap([hour(80, 3.2)] + [dry] * 7))
        self.assertEqual(ws._sustained_hazard_cap([hour(80, 3.2), hour(80, 3.2)] + [dry] * 6), 39)

    def test_night_rain_does_not_spoil_the_riding_day(self) -> None:
        n = 48
        om = make_om_payload(
            precipitation=with_value_at([0.0] * n, 2, 12.0),
            precipitation_probability=with_value_at([0] * n, 2, 90),
            weather_code=with_value_at([1] * n, 2, 65),
        )
        day = _merge_daily(om, None, None, now_local="2000-01-01T00:00")[0]
        self.assertGreaterEqual(day["moto_score"], 85)
        self.assertEqual(day["rain_intensity_max"], "none")
        self.assertEqual(day["weather_code"], 1)

    def test_today_only_counts_the_hours_ahead(self) -> None:
        wet = range(7, 16)  # heavy rain all morning and early afternoon
        om = make_om_payload(
            precipitation=[12.0 if h in wet else 0.0 for h in range(48)],
            precipitation_probability=[90 if h in wet else 0 for h in range(48)],
        )
        later_today = _merge_daily(om, None, None, now_local="2024-06-01T16:00")[0]
        this_morning = _merge_daily(om, None, None, now_local="2024-06-01T06:00")[0]
        self.assertGreaterEqual(later_today["moto_score"], 85)
        self.assertLessEqual(this_morning["moto_score"], 39)

    def test_daily_contract_fields(self) -> None:
        for day in _merge_daily(make_om_payload(), None, None):
            for key in ("moto_score", "moto_label", "precipitation_max_mm_h", "rain_intensity_max"):
                self.assertIn(key, day)


# ---------------------------------------------------------------------------
# Route departure
# ---------------------------------------------------------------------------

class RouteDepartureTests(unittest.TestCase):
    def test_default_departure_is_local_time_at_origin(self) -> None:
        local, utc = _resolve_departure(None, OFFSET_S)
        self.assertEqual(round((local - utc).total_seconds()), OFFSET_S)
        self.assertLess(abs((utc - datetime.now(timezone.utc).replace(tzinfo=None)).total_seconds()), 5)

    def test_naive_and_aware_departures(self) -> None:
        local, utc = _resolve_departure("2024-06-15T08:00", OFFSET_S)
        self.assertEqual((local.hour, utc.hour), (8, 5))
        local, utc = _resolve_departure("2024-06-15T08:00+00:00", OFFSET_S)
        self.assertEqual((local.hour, utc.hour), (11, 8))

    def test_invalid_departure_raises(self) -> None:
        with self.assertRaises(ValueError):
            _resolve_departure("tomorrow morning", OFFSET_S)


# ---------------------------------------------------------------------------
# Network behaviour: time budget, retries, cache (httpx.MockTransport)
# ---------------------------------------------------------------------------

def now_local_for_offset() -> datetime:
    return datetime.now(timezone.utc).replace(tzinfo=None) + timedelta(seconds=OFFSET_S)


class NetworkTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        ws.clear_caches()
        ws._wxm_cache.clear()
        self.calls: dict[str, int] = {}

    def tearDown(self) -> None:
        ws.set_http_client(None)
        ws.clear_caches()
        ws._wxm_cache.clear()

    async def test_weatherxm_stale_cached_observation_is_refetched(self) -> None:
        now = datetime.now(timezone.utc)
        fresh = {"observation": {"timestamp": now.isoformat(), "temperature": 21.0}}
        recent = {"observation": {"timestamp": (now - timedelta(minutes=5)).isoformat(),
                                  "temperature": 20.0}}
        stale = {"observation": {"timestamp": (now - timedelta(minutes=45)).isoformat(),
                                 "temperature": 30.0}}
        paths: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            paths.append(request.url.path)
            if request.url.path.endswith("/stations/near"):
                return httpx.Response(200, json={"stations": [{"id": "s1", "lastDayQod": 0.9}]})
            return httpx.Response(200, json=fresh)

        key = (round(44.43, 2), round(26.10, 2))
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch.object(ws, "_wxm_backoff_loaded", True), patch.object(ws, "_wxm_backoff_until", 0.0):
                # Stale, but fetched 2 minutes ago: no refetch yet (quota guard).
                ws._wxm_cache[key] = (time.monotonic() - 120, stale)
                self.assertIs(await ws._fetch_weatherxm(44.43, 26.10, "key", client), stale)
                # Still-fresh observation cached 15 minutes ago: served from cache.
                ws._wxm_cache[key] = (time.monotonic() - 900, recent)
                self.assertIs(await ws._fetch_weatherxm(44.43, 26.10, "key", client), recent)
                self.assertEqual(paths, [])
                # Stale observation cached 15 minutes ago: refetched.
                ws._wxm_cache[key] = (time.monotonic() - 900, stale)
                result = await ws._fetch_weatherxm(44.43, 26.10, "key", client)
        self.assertEqual(result, fresh)
        self.assertEqual(len(paths), 2)
        self.assertIsNotNone(_normalize_wxm_current(result))

    def count(self, name: str) -> None:
        self.calls[name] = self.calls.get(name, 0) + 1

    def make_client(self, handler: Any) -> httpx.AsyncClient:
        client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        ws.set_http_client(client)
        return client

    async def test_slow_optional_provider_does_not_block(self) -> None:
        payload = make_om_payload(now_local_for_offset())

        async def handler(request: httpx.Request) -> httpx.Response:
            host = request.url.host
            if host == "api.open-meteo.com":
                return httpx.Response(200, json=payload)
            if host == "api.met.no":
                self.count("met")
                await asyncio.sleep(2.0)
                return httpx.Response(200, json={"properties": {"timeseries": []}})
            return httpx.Response(404, json={})

        async with self.make_client(handler):
            with patch.object(ws, "OPTIONAL_PROVIDER_BUDGET_S", 0.3):
                started = time.perf_counter()
                data = await ws.get_weather(44.43, 26.10, "Test", "", forecast_days=2)
                elapsed = time.perf_counter() - started
        self.assertLess(elapsed, 1.5)
        self.assertEqual(self.calls.get("met"), 1)
        self.assertNotIn("met-norway", data["current"]["sources"])
        self.assertEqual(data["timezone"], "Europe/Bucharest")
        self.assertEqual(data["utc_offset_seconds"], OFFSET_S)

    async def test_whole_request_is_bounded(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            await asyncio.sleep(3.0)
            return httpx.Response(200, json={})

        async with self.make_client(handler):
            with patch.object(ws, "WEATHER_TOTAL_BUDGET_S", 0.4):
                started = time.perf_counter()
                with self.assertRaises(RuntimeError):
                    await ws.get_weather(44.43, 26.10, "Test", "", forecast_days=2)
                self.assertLess(time.perf_counter() - started, 1.5)

    async def test_open_meteo_is_retried_once(self) -> None:
        payload = make_om_payload(now_local_for_offset())

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "api.open-meteo.com":
                self.count("om")
                if self.calls["om"] == 1:
                    return httpx.Response(503, text="busy")
                return httpx.Response(200, json=payload)
            return httpx.Response(404, json={})

        async with self.make_client(handler):
            with patch.object(ws, "OPENMETEO_RETRY_DELAY_S", 0):
                data = await ws.get_weather(44.43, 26.10, "Test", "", forecast_days=2)
        self.assertEqual(self.calls["om"], 2)
        self.assertIn("current", data)

    async def test_concurrent_requests_share_one_upstream_call(self) -> None:
        payload = make_om_payload(now_local_for_offset())

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "api.open-meteo.com":
                self.count("om")
                await asyncio.sleep(0.1)
                return httpx.Response(200, json=payload)
            return httpx.Response(404, json={})

        async with self.make_client(handler):
            # Both points fall in the same ~5 km cache cell.
            await asyncio.gather(
                ws.get_weather(44.431, 26.101, "A", "", forecast_days=2),
                ws.get_weather(44.439, 26.109, "B", "", forecast_days=2),
            )
            await ws.get_weather(44.43, 26.10, "C", "", forecast_days=2)
        self.assertEqual(self.calls["om"], 1)

    async def test_cache_single_flight_ttl_and_bound(self) -> None:
        cache = ws._TTLCache("test", max_entries=2)

        async def fetch() -> tuple[str, float]:
            self.count("fetch")
            await asyncio.sleep(0.05)
            return "value", 60.0

        results = await asyncio.gather(*[cache.get_or_fetch("k", fetch) for _ in range(5)])
        self.assertEqual(results, ["value"] * 5)
        self.assertEqual(self.calls["fetch"], 1)
        self.assertEqual(await cache.get_or_fetch("k", fetch), "value")
        self.assertEqual(self.calls["fetch"], 1)

        cache.put("short", "x", 0.01)
        await asyncio.sleep(0.03)
        self.assertIsNone(cache.get("short"))

        cache.put("a", 1, 60)
        cache.put("b", 2, 60)
        self.assertEqual(len(cache), 2)
        self.assertIsNone(cache.get("k"))  # least recently used entry evicted

    async def test_hourly_items_carry_contract_fields(self) -> None:
        payload = make_om_payload(now_local_for_offset())

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "api.open-meteo.com":
                return httpx.Response(200, json=payload)
            return httpx.Response(404, json={})

        async with self.make_client(handler):
            data = await ws.get_weather(44.43, 26.10, "Test", "", forecast_days=2)
        self.assertIsInstance(data["utc_offset_seconds"], int)
        labels = {"IDEAL", "OK", "ATENȚIE", "EVITĂ", None}
        intensities = {"none", "urme", "slaba", "moderata", "puternica"}
        for hour in data["hourly"]:
            self.assertRegex(hour["time"], r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}$")
            self.assertTrue(hour["moto_score"] is None or 0 <= hour["moto_score"] <= 100)
            self.assertIn(hour["moto_label"], labels)
            self.assertIn(hour["rain_intensity"], intensities)
            self.assertIsInstance(hour["frost_risk"], bool)
            self.assertIsInstance(hour["is_day"], bool)
            for key in ("precipitation_mm", "precipitation_probability", "temperature",
                        "feels_like", "wind_gusts_kmh", "weather_code"):
                self.assertIn(key, hour)
        for key in ("precipitation_probability", "rain_intensity", "frost_risk", "score_breakdown"):
            self.assertIn(key, data["current"])
        for day in data["daily"]:
            for key in ("moto_score", "moto_label", "precipitation_max_mm_h", "rain_intensity_max"):
                self.assertIn(key, day)

    async def test_met_user_agent_comes_from_env(self) -> None:
        seen: list[str] = []
        payload = make_om_payload(now_local_for_offset())

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "api.met.no":
                seen.append(request.headers.get("User-Agent", ""))
                return httpx.Response(200, json={"properties": {"timeseries": []}})
            if request.url.host == "api.open-meteo.com":
                return httpx.Response(200, json=payload)
            return httpx.Response(404, json={})

        async with self.make_client(handler):
            with patch.dict("os.environ", {"MET_NORWAY_USER_AGENT": "TestAgent/2.0 (+https://example.test)"}):
                await ws.get_weather(44.43, 26.10, "Test", "", forecast_days=2)
        self.assertEqual(seen, ["TestAgent/2.0 (+https://example.test)"])
        with patch.dict("os.environ", {"MET_NORWAY_USER_AGENT": "X github.com/user/WeatherForMoto"}):
            self.assertEqual(ws._resolve_met_user_agent(None), ws.DEFAULT_MET_USER_AGENT)


class GeocodingTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        ws.clear_caches()

    def tearDown(self) -> None:
        ws.clear_caches()

    async def test_wikidata_outage_is_not_reported_as_not_found(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "nominatim.openstreetmap.org":
                return httpx.Response(200, json=[])
            return httpx.Response(503, text="maintenance")

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch.object(ws, "NOMINATIM_MIN_INTERVAL_S", 0):
                with self.assertRaises(RuntimeError):
                    await ws.geocode_city("Razmiresti, Teleorman", client)

    async def test_nothing_found_is_value_error_and_hits_are_cached(self) -> None:
        calls: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.host)
            if request.url.host == "geocoding-api.open-meteo.com":
                if "Sibiu" in str(request.url):
                    return httpx.Response(200, json={"results": [
                        {"latitude": 45.8, "longitude": 24.15, "name": "Sibiu", "country_code": "RO"}]})
                return httpx.Response(200, json={})
            if request.url.host == "nominatim.openstreetmap.org":
                return httpx.Response(200, json=[])
            return httpx.Response(404, json={})

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch.object(ws, "NOMINATIM_MIN_INTERVAL_S", 0):
                with self.assertRaises(ValueError):
                    await ws.geocode_city("Nowhereville", client)
                first = await ws.geocode_city("Sibiu", client)
                calls_after_first = len(calls)
                second = await ws.geocode_city("  sibiu ", client)
        self.assertEqual(first, second)
        self.assertEqual(len(calls), calls_after_first)

    async def test_nominatim_calls_are_spaced(self) -> None:
        stamps: list[float] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            stamps.append(time.monotonic())
            return httpx.Response(200, json=[])

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with patch.object(ws, "NOMINATIM_MIN_INTERVAL_S", 0.2):
                await asyncio.gather(
                    ws._nominatim_search(client, {"q": "a"}),
                    ws._nominatim_search(client, {"q": "b"}),
                    ws._nominatim_search(client, {"q": "c"}),
                )
        gaps = [b - a for a, b in zip(stamps, stamps[1:])]
        self.assertTrue(all(gap >= 0.18 for gap in gaps), gaps)


# ---------------------------------------------------------------------------
# API endpoints (no network needed)
# ---------------------------------------------------------------------------

class ApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        from fastapi.testclient import TestClient
        import main
        cls.client = TestClient(main.app)

    def test_meta_scoring_shape(self) -> None:
        resp = self.client.get("/meta/scoring", headers={"Accept-Encoding": "gzip"})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.headers.get("content-encoding"), "gzip")
        body = resp.json()
        self.assertEqual(
            [(x["label"], x["min_score"]) for x in body["labels"]],
            [("IDEAL", 85), ("OK", 60), ("ATENȚIE", 40), ("EVITĂ", 0)],
        )
        matrix = body["rain"]["impact_matrix"]
        self.assertEqual(matrix["columns"], ["urme", "slaba", "moderata", "puternica"])
        self.assertEqual(matrix["rows"], [
            ["none", "none", "low", "medium"],
            ["none", "low", "medium", "high"],
            ["low", "medium", "high", "high"],
        ])
        self.assertEqual(body["rain"]["impact_caps"], {"none": None, "low": 84, "medium": 59, "high": 39})
        bands = {b["name"]: (b["min_mm_h"], b["max_mm_h"]) for b in body["rain"]["intensity_bands"]}
        self.assertEqual(bands["slaba"], (0.5, 2.5))
        self.assertEqual(bands["puternica"], (7.5, None))
        self.assertEqual(body["hazards"]["snow"]["cap"], 30)
        self.assertEqual(body["hazards"]["ice"]["cap"], 25)

    def test_invalid_departure_is_422(self) -> None:
        resp = self.client.get("/route", params={"departure": "tomorrow 8am"})
        self.assertEqual(resp.status_code, 422)
        resp = self.client.get("/route/multi", params={"stops": "A;B", "departure": "31/12 08:00"})
        self.assertEqual(resp.status_code, 422)


if __name__ == "__main__":
    unittest.main(verbosity=2)
