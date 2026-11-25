"""Temperature, real feel, humidity, wind and gusts: every witness weighed, not averaged.

PR 165 made rain, fog and storms a matter of evidence. These tests hold the
numbers to the same rule: the real feel follows the temperature that was
decided, a day agrees with its own hours, one station alone cannot overrule
everyone, and stations that agree with each other beat the models.
"""

from __future__ import annotations

import sqlite3
import time
import unittest

import calibration
import value_arbiter
import verification
import weather_service as ws
from test_scoring import CURRENT_HOUR_INDEX, make_om_payload


OFFICIAL_COLD = {"station": "Bucuresti-Baneasa", "distance_km": 4.0, "temp": 14.0, "humidity": 80.0}


class RealFeelTests(unittest.TestCase):
    def test_steadman_matches_the_published_formula(self) -> None:
        # 20 °C, 50 %, calm: e = 11.69 hPa -> 20 + 3.86 - 4 = 19.86.
        self.assertAlmostEqual(ws._apparent_temperature(20.0, 50.0, 0.0), 19.86, places=1)
        # Wind takes 0.7 °C per m/s.
        calm = ws._apparent_temperature(5.0, 70.0, 0.0)
        windy = ws._apparent_temperature(5.0, 70.0, 36.0)
        self.assertAlmostEqual(calm - windy, 7.0, places=1)
        self.assertIsNone(ws._apparent_temperature(None, 50.0, 10.0))

    def test_the_real_feel_follows_a_station_that_moves_the_temperature(self) -> None:
        om = make_om_payload()
        without = ws._merge_current(om, None, None)
        with_station = ws._merge_current(om, None, None, official_norm=OFFICIAL_COLD)
        drop_temp = without["temperature"] - with_station["temperature"]
        drop_feel = without["feels_like"] - with_station["feels_like"]
        self.assertGreater(drop_temp, 1.0)
        # The real feel falls with the air, not stuck on the models' 18 °C.
        self.assertGreater(drop_feel, 0.6 * drop_temp)

    def test_the_real_feel_is_computed_from_the_final_values(self) -> None:
        current = ws._merge_current(make_om_payload(), None, None)
        expected = ws._apparent_temperature(current["temperature"], current["humidity"],
                                            current["wind_speed_kmh"])
        self.assertEqual(current["feels_like"], expected)


class DailyFromHoursTests(unittest.TestCase):
    def test_a_full_day_takes_its_extremes_from_its_hours(self) -> None:
        temps = [10.0 + (i % 24) * 0.5 for i in range(48)]  # 10.0 .. 21.5 each day
        gusts = [20.0] * 48
        gusts[15] = 55.0
        om = make_om_payload(temperature_2m=temps, apparent_temperature=temps, wind_gusts_10m=gusts)
        hourly = ws._build_hourly(om)
        daily = ws._merge_daily(om, None, hourly=hourly, now_local="2024-06-01T00:00")
        day = daily[0]
        hour_temps = [h["temperature"] for h in hourly[:24]]
        self.assertEqual(day["temp_max"], round(max(hour_temps), 1))
        self.assertEqual(day["temp_min"], round(min(hour_temps), 1))
        self.assertEqual(day["wind_gusts_kmh"], 55.0)

    def test_a_day_the_hours_only_partly_cover_keeps_the_providers(self) -> None:
        self.assertIsNone(ws._day_extremes_from_hours([{"temperature": 5.0}] * 6))


class ValueArbiterTests(unittest.TestCase):
    MODELS = ([18.0, 18.4, 17.8, 18.2], [1.2, 1.0, 1.1, 0.8])

    def test_without_stations_the_models_speak_as_before(self) -> None:
        verdict = value_arbiter.judge_value("temperature", models=self.MODELS, stations={})
        expected = ws._weighted_avg(*self.MODELS)
        self.assertAlmostEqual(verdict["value"], expected, places=2)
        self.assertFalse(verdict["notable"])
        self.assertIn("modelelor", verdict["explanation"])

    def test_a_near_official_station_leads(self) -> None:
        verdict = value_arbiter.judge_value("temperature", models=self.MODELS, stations={
            "official": {"value": 15.0, "distance_km": 3.0, "station": "Băneasa"}})
        self.assertLess(verdict["value"], 15.6)
        self.assertTrue(verdict["notable"])
        self.assertIn("Băneasa", verdict["explanation"])
        self.assertIn("modelele dădeau", verdict["explanation"])

    def test_a_far_station_counts_for_less(self) -> None:
        near = value_arbiter.judge_value("temperature", models=self.MODELS, stations={
            "official": {"value": 15.0, "distance_km": 3.0}})
        far = value_arbiter.judge_value("temperature", models=self.MODELS, stations={
            "official": {"value": 15.0, "distance_km": 35.0}})
        self.assertGreater(far["value"], near["value"] + 0.5)

    def test_one_station_alone_against_everyone_is_set_aside(self) -> None:
        # A WeatherXM station in the sun reads 27° while models and ANM say 18°.
        verdict = value_arbiter.judge_value("temperature", models=self.MODELS, stations={
            "official": {"value": 17.6, "distance_km": 5.0},
            "weatherxm": {"value": 27.0, "distance_km": 1.0}})
        self.assertLess(verdict["value"], 18.5)
        wxm = next(w for w in verdict["witnesses"] if w["id"] == "weatherxm")
        self.assertTrue(wxm.get("set_aside"))
        self.assertEqual(wxm["share"], 0.0)
        self.assertIn("WeatherXM", verdict["explanation"])
        self.assertIn("n-a fost luată în calcul", verdict["explanation"])

    def test_stations_that_agree_beat_the_models(self) -> None:
        # A valley inversion: every station reads 11-12° under models at 18°.
        verdict = value_arbiter.judge_value("temperature", models=self.MODELS, stations={
            "official": {"value": 11.5, "distance_km": 6.0},
            "netatmo": {"value": 12.0, "distance_km": 1.0},
            "sensor-community": {"value": 11.8}})
        self.assertLess(verdict["value"], 12.5)
        self.assertFalse(any(w.get("set_aside") for w in verdict["witnesses"]))

    def test_learning_tightens_and_debiases(self) -> None:
        learned = {"sigma": {"temperature": {"netatmo": 0.9}}, "bias": {"temperature": {"netatmo": 1.5}}}
        plain = value_arbiter.judge_value("temperature", models=self.MODELS, stations={
            "netatmo": {"value": 21.0, "distance_km": 1.0}})
        taught = value_arbiter.judge_value("temperature", models=self.MODELS, stations={
            "netatmo": {"value": 21.0, "distance_km": 1.0}}, learned=learned)
        netatmo = next(w for w in taught["witnesses"] if w["id"] == "netatmo")
        self.assertAlmostEqual(netatmo["value"], 19.5)
        self.assertLess(netatmo["sigma"], next(w for w in plain["witnesses"] if w["id"] == "netatmo")["sigma"])

    def test_humidity_stays_within_bounds(self) -> None:
        verdict = value_arbiter.judge_value("humidity", models=([99.0, 100.0], [1.0, 1.0]), stations={
            "official": {"value": 100.0, "distance_km": 2.0}})
        self.assertLessEqual(verdict["value"], 100.0)


class MergeCurrentValuesTests(unittest.TestCase):
    def test_a_lone_weatherxm_no_longer_overrules_everyone(self) -> None:
        om = make_om_payload()
        wxm = {"temp": 29.0, "feels_like": 30.0, "humidity": 60, "wind_speed_kmh": 10.0,
               "wind_gusts_kmh": 20.0, "precipitation": 0.0, "pressure": 1013.0, "wmo_code": None,
               "distance_km": 0.8}
        current = ws._merge_current(om, None, None, wxm_norm=wxm, official_norm=OFFICIAL_COLD)
        self.assertLess(current["temperature"], 20.0)
        values = current["verdict"]["values"]
        self.assertTrue(values["temperature"]["notable"])
        self.assertTrue(any(w.get("set_aside") for w in values["temperature"]["witnesses"]))

    def test_the_verdict_carries_every_number(self) -> None:
        current = ws._merge_current(make_om_payload(), None, None)
        self.assertEqual(set(current["verdict"]["values"]), {"temperature", "humidity", "wind", "gusts"})
        self.assertEqual(current["verdict"]["values"]["temperature"]["value"], current["temperature"])


class HourlyProvidersTests(unittest.TestCase):
    def test_met_and_pirate_join_the_hours(self) -> None:
        om = make_om_payload()
        hourly = ws._build_hourly(om)
        slot = hourly[CURRENT_HOUR_INDEX]["time"]
        # Local time slot back to UTC with the payload's offset.
        from datetime import datetime, timedelta, timezone
        local = datetime.fromisoformat(slot)
        utc = (local - timedelta(seconds=om["utc_offset_seconds"])).replace(tzinfo=timezone.utc)
        met = {"properties": {"timeseries": [{
            "time": utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "data": {"instant": {"details": {"air_temperature": 24.0, "relative_humidity": 60,
                                             "wind_speed": 2.0, "wind_speed_of_gust": 8.0}},
                     "next_1_hours": {"details": {"precipitation_amount": 0.0}}}}]}}
        pirate = {"hourly": {"data": [{"time": int(utc.timestamp()), "temperature": 24.0,
                                       "humidity": 0.6, "windSpeed": 2.0, "windGust": 8.0}]}}
        before = hourly[CURRENT_HOUR_INDEX]["temperature"]
        before_feel = hourly[CURRENT_HOUR_INDEX]["feels_like"]
        ws._blend_provider_hours(hourly, met, pirate, om["utc_offset_seconds"])
        hour = hourly[CURRENT_HOUR_INDEX]
        expected = ws._weighted_avg([before, 24.0, 24.0], [3.0, 1.1, 0.8])
        self.assertEqual(hour["temperature"], expected)
        self.assertAlmostEqual(hour["feels_like"] - before_feel, expected - before, places=1)
        # Hours the providers do not cover stay as they were.
        self.assertEqual(hourly[CURRENT_HOUR_INDEX + 1]["temperature"], before)


class LearningTests(unittest.TestCase):
    @staticmethod
    def _observations(hours: int, netatmo_offset: float) -> list[dict]:
        rows = []
        for i in range(hours):
            hour = f"2026-11-{1 + i // 24:02d}T{i % 24:02d}"
            truth = 5.0 + (i % 7)
            rows.append({"cell": "44.45,26.10", "hour": hour, "source": "official",
                         "distance_km": 4.0, "temp": truth, "gusts": 30.0})
            rows.append({"cell": "44.45,26.10", "hour": hour, "source": "netatmo",
                         "distance_km": 1.0, "temp": truth + netatmo_offset + (0.5 if i % 2 else -0.5),
                         "gusts": 20.0})
        return rows

    def test_a_network_that_reads_warm_is_learned(self) -> None:
        values = calibration.learn_values(self._observations(100, 2.0), [])
        # Halfway (LEARNED_SHARE) from no bias to the measured +2 °C.
        self.assertAlmostEqual(values["bias"]["temperature"]["netatmo"], 1.0, places=2)
        self.assertLess(values["sigma"]["temperature"]["netatmo"], value_arbiter.SIGMA["temperature"]["netatmo"])
        self.assertAlmostEqual(values["bias"]["gusts"]["netatmo"], -5.0, places=2)

    def test_too_few_pairs_teach_nothing(self) -> None:
        values = calibration.learn_values(self._observations(20, 2.0), [])
        self.assertNotIn("temperature", values["bias"])
        self.assertEqual(values["pairs"]["temperature:netatmo"], 20)

    def test_models_that_run_cold_in_a_cell_are_corrected_there(self) -> None:
        observations = self._observations(100, 0.0)
        forecasts = [{"cell": row["cell"], "valid_hour": row["hour"], "lead_h": 1,
                      "om_temp": row["temp"] - 2.0, "gusts": None}
                     for row in observations if row["source"] == "official"]
        values = calibration.learn_values(observations, forecasts)
        self.assertAlmostEqual(calibration.cell_temperature_bias(values, "44.45,26.10"), -1.0, places=2)
        self.assertIsNone(calibration.cell_temperature_bias(values, "45.00,25.00"))
        cell = calibration.values_for_cell(values, "44.45,26.10")
        self.assertAlmostEqual(cell["bias"]["temperature"]["models"], -1.0, places=2)

        om = make_om_payload()
        hourly = ws._build_hourly(om)
        before = hourly[5]["temperature"]
        ws._apply_cell_temperature_bias(hourly, -1.0)
        self.assertEqual(hourly[5]["temperature"], round(before + 1.0, 2))

    def test_the_learned_numbers_survive_storage(self) -> None:
        values = calibration.learn_values(self._observations(100, 2.0), [])
        snapshot = calibration.Snapshot(weights={"ensemble": 3.0}, pairs=1, computed_at=time.time(), values=values)
        text = calibration.to_stored(snapshot, {"ensemble": 3.0})
        back = calibration.from_stored(text, {"ensemble": 3.0}, time.time())
        self.assertEqual(back.values["bias"], values["bias"])

    def test_the_window_is_read_in_two_scans(self) -> None:
        conn = sqlite3.connect(":memory:")
        conn.row_factory = sqlite3.Row
        for table in verification.TABLES:
            conn.execute(table)
        conn.execute("INSERT INTO observation_log (cell, hour, source, distance_km, temp, gusts, raining) "
                     "VALUES ('c', '2026-11-20T10', 'official', 3, 7.5, 30, 0)")
        conn.execute("INSERT INTO observation_log (cell, hour, source, distance_km, temp, gusts, raining) "
                     "VALUES ('c', '2026-11-20T10', 'netatmo', 1, 9.0, NULL, NULL)")
        conn.execute("INSERT INTO forecast_log (cell, issued_hour, valid_hour, lead_h, temp, om_temp, gusts, "
                     "precip_prob, prob_ensemble) VALUES ('c', '2026-11-20T09', '2026-11-20T10', 1, 7, 6.5, 25, 10, 0.1)")
        window = verification.read_window(conn, "2026-11-01T00", ("precip_prob", "prob_ensemble"))
        self.assertEqual(len(window["rain"]), 1)
        self.assertEqual(window["rain"][0]["raining"], 0)
        self.assertEqual(len(window["observations"]), 2)
        self.assertEqual(window["forecasts"][0]["om_temp"], 6.5)


class SkyTests(unittest.TestCase):
    def test_an_overcast_airport_beats_a_clear_openweathermap(self) -> None:
        om = make_om_payload()
        om["current"]["weather_code"] = 2
        owm = {"main": {"temp": 18.0, "feels_like": 18.0, "humidity": 60, "pressure": 1013},
               "wind": {"speed": 3.0}, "weather": [{"id": 800, "description": "cer senin"}],
               "clouds": {"all": 0}, "visibility": 10000}
        airport = {"station": "LROP", "distance_km": 6.0, "wmo_code": None,
                   "sky": {"cloud": 1.0, "sky": "OVC", "sees_high": True}}
        current = ws._merge_current(om, owm, None, metar_obs=airport)
        self.assertEqual(current["weather_code"], 3)
        self.assertNotEqual(current["description"], "Cer senin")
        self.assertGreater(current["cloud_cover"], 70)

    def test_a_phenomenon_keeps_its_code(self) -> None:
        sky = ws._judge_sky(metar_sky=None, model_cover=10.0, owm_cover=None, met_cover=None, pirate_cover=None)
        self.assertEqual(sky["code"], 0)
        self.assertIsNone(ws._judge_sky(metar_sky=None, model_cover=None, owm_cover=None,
                                        met_cover=None, pirate_cover=None))


class RainAmountTests(unittest.TestCase):
    def test_the_amount_is_weighed_like_the_chance(self) -> None:
        import rain_fusion
        fused = rain_fusion.fuse_hour(
            open_meteo_probability=80, wet_models=5, rain_models=6, model_amount=0.0,
            votes={"ensemble": {"probability": 0.8, "amount": 1.6, "range": (0.4, 3.0), "members": 100},
                   "pirate-weather": {"probability": 0.7, "amount": 1.2}})
        self.assertGreater(fused["amount_mm"], 0.5)
        self.assertLess(fused["amount_mm"], 1.6)

    def test_a_day_sums_its_hours(self) -> None:
        rain = [0.0] * 48
        rain[14], rain[15] = 1.5, 2.5
        om = make_om_payload(precipitation=rain, precipitation_probability=[80] * 48)
        hourly = ws._build_hourly(om)
        daily = ws._merge_daily(om, None, hourly=hourly, now_local="2024-06-01T00:00")
        self.assertEqual(daily[0]["precipitation_mm"], round(sum(h["precipitation_mm"] for h in hourly[:24]), 1))


class VisibilityTests(unittest.TestCase):
    def test_a_near_airport_measures_it(self) -> None:
        self.assertEqual(ws._judge_visibility(airport_m=800.0, airport_km=4.0, providers=[10000.0, 24000.0]), 800.0)

    def test_one_provider_alone_cannot_call_fog(self) -> None:
        self.assertEqual(ws._judge_visibility(airport_m=None, airport_km=None, providers=[10000.0, 600.0, 24000.0]),
                         10000.0)

    def test_a_far_airport_still_votes(self) -> None:
        self.assertLess(ws._judge_visibility(airport_m=2000.0, airport_km=18.0, providers=[10000.0, 10000.0]), 10000.0)


if __name__ == "__main__":
    unittest.main()
