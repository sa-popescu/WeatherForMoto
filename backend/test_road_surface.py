"""The asphalt's temperature from its heat balance (road_surface.py) and how the answer uses it.

The physics cases are the ones every rider knows: a sunny afternoon heats the
asphalt well above the air, a clear night takes it below the air, clouds keep
it warm, rain and snow pin it to the air and to 0 °C. The service cases check
that an airport's observed sky beats the model's cloud, and that the value,
its range and its reasons reach the answer.
"""

from __future__ import annotations

import math
import unittest
from datetime import datetime, timezone

import metar
import road_surface as rs
import weather_service as ws
from test_scoring import make_om_payload

LAT, LON = 44.43, 26.10


def days(cloud: float, sun_peak: float, *, month: int = 11, tmin: float = 0.0, tmax: float = 8.0,
         n_days: int = 5, wind: float = 8.0, rain=None, snowing=False) -> list[rs.Hour]:
    """Hourly marks in Bucharest (UTC+2), air peaking at 15:00, clear-sky sun scaled by the cloud."""
    start = datetime(2026, month, 20, tzinfo=timezone.utc).timestamp() - 2 * 3600
    hours = []
    for h in range(n_days * 24):
        ts = start + h * 3600
        local = h % 24
        air = (tmin + tmax) / 2 + (tmax - tmin) / 2 * math.cos((local - 15) / 24 * 2 * math.pi)
        sun = sun_peak * max(0.0, rs.cos_zenith(LAT, LON, ts - 1800)) * (1 - 0.75 * cloud)
        hours.append(rs.Hour(ts=ts, temp=air, dew=air - 4, wind_kmh=wind, cloud=cloud, sun=sun,
                             precip_mm=rain(h) if rain else 0.0, snowing=snowing, soil_deep=(tmin + tmax) / 2 + 2))
    return hours


def simulate(hours: list[rs.Hour]) -> rs.Result:
    return rs.run(hours, LAT, LON, rs.initial_profile(hours[0].temp, [], hours[0].soil_deep))


def at(hours: list[rs.Hour], result: rs.Result, local_hour: int) -> tuple[float, float]:
    """(air, road) on the last day at that local hour."""
    i = len(hours) - 24 + local_hour
    return hours[i].temp, result.temps[i]


class PhysicsTests(unittest.TestCase):
    def test_sunny_november_afternoon_heats_the_asphalt_above_the_air(self):
        hours = days(cloud=0.0, sun_peak=900)
        air, road = at(hours, simulate(hours), 13)
        self.assertGreater(road, air + 4)
        self.assertLess(road, air + 15)

    def test_clear_night_takes_the_asphalt_below_the_air(self):
        hours = days(cloud=0.0, sun_peak=900)
        air, road = at(hours, simulate(hours), 5)
        self.assertLess(road, air - 0.5)

    def test_clouds_keep_the_night_road_warmer_than_a_clear_sky(self):
        clear = days(cloud=0.0, sun_peak=900)
        cloudy = days(cloud=1.0, sun_peak=900)
        self.assertGreater(at(cloudy, simulate(cloudy), 5)[1], at(clear, simulate(clear), 5)[1] + 1.5)

    def test_summer_afternoon_reaches_the_asphalt_temperatures_measured_on_roads(self):
        hours = days(cloud=0.0, sun_peak=1000, month=7, tmin=20, tmax=34)
        air, road = at(hours, simulate(hours), 15)
        self.assertGreater(road, 48)
        self.assertLess(road, 68)

    def test_a_sunny_day_keeps_the_evening_road_warmer_than_a_dull_one(self):
        sunny = days(cloud=0.0, sun_peak=900)
        dull = days(cloud=0.0, sun_peak=0)
        # 19:00, after sunset, under the same clear sky: the afternoon's heat is still in the road.
        self.assertGreater(at(sunny, simulate(sunny), 19)[1], at(dull, simulate(dull), 19)[1] + 1.0)

    def test_rain_keeps_the_road_close_to_the_air(self):
        hours = days(cloud=1.0, sun_peak=900, rain=lambda h: 2.0)
        air, road = at(hours, simulate(hours), 13)
        self.assertLess(abs(road - air), 2.0)
        self.assertGreater(simulate(hours).wet[-1], 0.9)

    def test_falling_snow_holds_the_surface_near_zero(self):
        hours = days(cloud=1.0, sun_peak=900, tmin=-1, tmax=1, snowing=True, rain=lambda h: 1.0)
        result = simulate(hours)
        self.assertLessEqual(max(result.temps[-24:]), rs.SNOW_SURFACE_MAX_C + 1e-9)

    def test_the_range_brackets_the_central_value_and_widens_with_doubt(self):
        hours = days(cloud=0.5, sun_peak=900)
        result = simulate(hours)
        for low, mid, high in zip(result.lows, result.temps, result.highs):
            self.assertLessEqual(low, mid + 1e-9)
            self.assertGreaterEqual(high, mid - 1e-9)
        for h in hours:
            h.cloud_spread = 0.4
        wide = simulate(hours)
        self.assertGreater(wide.highs[-20] - wide.lows[-20], result.highs[-20] - result.lows[-20])

    def test_a_measured_sun_replaces_the_models_for_its_hour(self):
        hours = days(cloud=1.0, sun_peak=900)
        start = rs.initial_profile(hours[0].temp, [], hours[0].soil_deep)
        noon = len(hours) - 24 + 13
        dull = rs.run(hours, LAT, LON, start)
        bright = rs.run(hours, LAT, LON, start, sun_override={noon: 450.0})
        self.assertGreater(bright.temps[noon], dull.temps[noon] + 2)

    def test_layered_cloud_weighs_low_cloud_most(self):
        self.assertAlmostEqual(rs.cloud_effective(None, 100, 0, 0), 1.0)
        self.assertAlmostEqual(rs.cloud_effective(None, 0, 0, 100), rs.CLOUD_WEIGHT_HIGH)
        self.assertEqual(rs.cloud_effective(50), 0.5)
        self.assertIsNone(rs.cloud_effective(None))

    def test_observed_cloud_counts_in_full_now_and_fades_after(self):
        self.assertEqual(rs.blend_observed_cloud(1.0, 0.0, 0), (0.0, 1.0))
        later, weight = rs.blend_observed_cloud(1.0, 0.0, 2)
        self.assertGreater(later, 0.0)
        self.assertLess(weight, 1.0)
        self.assertEqual(rs.blend_observed_cloud(1.0, 0.0, 8), (1.0, 0.0))

    def test_asphalt_far_from_every_models_ground_lowers_confidence(self):
        self.assertTrue(rs.skin_is_odd(-6.0, [1.0, 2.0], False))
        self.assertFalse(rs.skin_is_odd(3.0, [1.0, 2.0], False))
        self.assertEqual(rs.confidence(3.0, 4.0, False), "high")
        self.assertEqual(rs.confidence(3.0, 4.0, True), "medium")
        self.assertEqual(rs.confidence(0.0, 6.0, False), "low")


class AirportSkyTests(unittest.TestCase):
    def test_cavok_is_a_clear_low_sky_that_does_not_see_cirrus(self):
        sky = metar.sky_cover({"cover": "CAVOK", "clouds": []})
        self.assertEqual(sky, {"cloud": 0.0, "sky": "CAVOK", "sees_high": False})

    def test_layers_weigh_by_height(self):
        low = metar.sky_cover({"cover": "OVC", "clouds": [{"cover": "OVC", "base": 1200}]})
        mid = metar.sky_cover({"cover": "OVC", "clouds": [{"cover": "OVC", "base": 6700}]})
        self.assertAlmostEqual(low["cloud"], 1.0)
        self.assertAlmostEqual(mid["cloud"], 0.85)
        self.assertTrue(mid["sees_high"])

    def test_no_sky_information_is_none(self):
        self.assertIsNone(metar.sky_cover({}))


def november_evening(cloud_pct: float = 95.0) -> dict:
    """Bucharest, 25 Nov 17:40: a sunny day, then the models' overcast after sunset."""
    now = datetime(2026, 11, 25, 17, 40)
    om = make_om_payload(now_local=now, hours=48)
    om["utc_offset_seconds"] = 2 * 3600
    times = om["hourly"]["time"]
    n = len(times)
    temps, sun, cloud = [], [], []
    for t in times:
        local = int(t[11:13])
        temps.append(2.0 + 3.0 * math.cos((local - 15) / 24 * 2 * math.pi))
        ts = rs.local_to_ts(t, 2 * 3600)
        sun.append(400 * max(0.0, rs.cos_zenith(LAT, LON, ts - 1800)) / 0.4)
        cloud.append(0.0 if local < 15 or t[:10] > "2026-11-25" else cloud_pct)
    om["hourly"].update({
        "temperature_2m": temps, "apparent_temperature": [t - 3 for t in temps],
        "relative_humidity_2m": [60] * n, "dew_point_2m": [-3.0] * n, "wind_speed_10m": [6.0] * n,
        "cloud_cover": cloud, "cloud_cover_low": cloud, "cloud_cover_mid": [0] * n, "cloud_cover_high": [0] * n,
        "shortwave_radiation": sun, "soil_temperature_0cm": [t - 1 for t in temps],
        "soil_temperature_54cm": [6.0] * n, "is_day": [1 if 8 <= int(t[11:13]) < 17 else 0 for t in times],
    })
    om["current"].update({"temperature_2m": temps[17], "apparent_temperature": temps[17] - 3,
                          "is_day": 0, "weather_code": 3, "relative_humidity_2m": 60})
    return om


def answer(om: dict, metar_sky: dict | None = None) -> tuple[list[dict], dict]:
    hourly = ws._build_hourly(om)
    current = ws._merge_current(om, None, None, hourly=hourly)
    ws._apply_road_surface(om, hourly, current, None, lat=LAT, lon=LON, metar_sky=metar_sky)
    return hourly, current


CAVOK_BANEASA = {"cloud": 0.0, "sky": "CAVOK", "sees_high": False, "station": "LRBS",
                 "name": "Bucharest/Băneasa Intl", "distance_km": 8.0}


class ServiceTests(unittest.TestCase):
    def test_the_answer_carries_value_range_confidence_and_why(self):
        hourly, current = answer(november_evening())
        road = current["road_surface"]
        self.assertEqual(current["road_surface_temp"], road["temp"])
        self.assertLessEqual(road["low"], road["temp"])
        self.assertGreaterEqual(road["high"], road["temp"])
        self.assertIn(road["confidence"], ("high", "medium", "low"))
        self.assertTrue(road["explanation"])
        self.assertEqual(road["method"], "heat-balance")
        self.assertIsNotNone(hourly[17]["road_surface_range"])
        self.assertIn("_road_sim", hourly[17])

    def test_a_clear_airport_cools_the_road_the_model_kept_under_cloud(self):
        _, modelled = answer(november_evening())
        _, observed = answer(november_evening(), CAVOK_BANEASA)
        self.assertLess(observed["road_surface_temp"], modelled["road_surface_temp"])
        self.assertIn("Băneasa", observed["road_surface"]["explanation"])
        self.assertEqual(observed["road_surface"]["sky"]["station"], "LRBS")

    def test_the_hours_ahead_follow_the_clear_night(self):
        hourly, current = answer(november_evening(cloud_pct=0.0))
        self.assertLess(hourly[23]["road_surface_temp"], hourly[17]["road_surface_temp"])

    def test_a_road_freezing_under_a_clear_sky_turns_on_the_frost_risk_and_caps_the_score(self):
        om = november_evening(cloud_pct=0.0)
        hourly = om["hourly"]
        hourly["temperature_2m"] = [t - 2.5 for t in hourly["temperature_2m"]]
        hourly["dew_point_2m"] = [-1.0] * len(hourly["time"])
        hourly["relative_humidity_2m"] = [85] * len(hourly["time"])
        om["current"].update({"temperature_2m": hourly["temperature_2m"][17], "relative_humidity_2m": 85})
        hours, current = answer(om, CAVOK_BANEASA)
        frozen = [h for h in hours if h["road_surface_temp"] is not None and h["road_surface_temp"] <= 0]
        self.assertTrue(frozen)
        self.assertTrue(all(h["frost_risk"] for h in frozen if h.get("_road_sim") is not None))
        if current["road_surface_temp"] <= ws.FROST_SURFACE_MAX_C:
            self.assertTrue(current["frost_risk"])
            self.assertLessEqual(current["moto_score"], ws.FROST_RISK_CAP)

    def test_without_the_inputs_the_quick_estimate_stays(self):
        om = make_om_payload()
        om["hourly"]["temperature_2m"] = [None] * len(om["hourly"]["time"])
        hourly = ws._build_hourly(om)
        current = ws._merge_current(om, None, None, hourly=hourly)
        before = current["road_surface_temp"]
        ws._apply_road_surface(om, hourly, current, None, lat=LAT, lon=LON)
        self.assertEqual(current["road_surface_temp"], before)
        self.assertIsNone(current["road_surface"])

    def test_past_days_start_the_column(self):
        om = november_evening()
        past_times = [f"2026-11-{d:02d}T{h:02d}:00" for d in (22, 23, 24) for h in range(24)]
        n = len(past_times)
        past = {"hourly": {
            "time": past_times, "precipitation": [0.0] * n, "temperature_2m": [3.0] * n,
            "dew_point_2m": [-3.0] * n, "wind_speed_10m": [6.0] * n, "weather_code": [0] * n,
            "shortwave_radiation": [0.0] * n, "cloud_cover": [100] * n, "cloud_cover_low": [100] * n,
            "cloud_cover_mid": [0] * n, "cloud_cover_high": [0] * n,
            "soil_temperature_0cm": [3.0] * n, "soil_temperature_6cm": [4.0] * n,
            "soil_temperature_18cm": [5.0] * n, "soil_temperature_54cm": [6.0] * n,
        }}
        hourly = ws._build_hourly(om)
        current = ws._merge_current(om, None, None, hourly=hourly)
        ws._apply_road_surface(om, hourly, current, past, lat=LAT, lon=LON)
        self.assertIsNotNone(current["road_surface"])


if __name__ == "__main__":
    unittest.main()
