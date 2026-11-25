"""The arbiter: one verdict on now, from every witness, with its reasons.

Each case is a moment the app used to get wrong because one source spoke
alone, or a moment it must keep getting right because the witnesses agree.
"""

from __future__ import annotations

import unittest

import arbiter
import weather_service as ws
from test_false_precip import DRY_AIRPORT, owm
from test_scoring import CURRENT_HOUR_INDEX, make_om_payload, with_value_at

BANEASA = {**DRY_AIRPORT, "name": "Bucharest/Băneasa Intl", "visibility_m": 10000.0}
DRY_MET = {"precipitation": 0.0, "wmo_code": 3}
DRY_PW = {"precipitation": 0.0, "wmo_code": 3}


def dry_now(probability: int = 0) -> dict:
    om = make_om_payload(
        precipitation_probability=with_value_at([0] * 48, CURRENT_HOUR_INDEX, probability))
    om["current"].update({"weather_code": 3, "precipitation": 0.0})
    return om


class BucharestTests(unittest.TestCase):
    """25 Nov 17:00: OWM alone said 1 mm of rain under a CAVOK airport."""

    def setUp(self) -> None:
        self.current = ws._merge_current(
            dry_now(), owm(501, "ploaie moderată", 1.0), None,
            met_norm=DRY_MET, pw_norm=DRY_PW, metar_obs=BANEASA)
        self.verdict = self.current["verdict"]["precipitation"]

    def test_it_does_not_rain(self) -> None:
        self.assertEqual(self.verdict["state"], "dry")
        self.assertGreaterEqual(self.verdict["confidence"], 90)
        self.assertEqual(self.current["precipitation_mm"], 0.0)
        self.assertEqual(self.current["weather_code"], 3)

    def test_and_says_why(self) -> None:
        text = self.verdict["explanation"]
        self.assertTrue(text.startswith("Nu plouă: aeroportul Băneasa și 3 din 4 surse spun uscat."), text)
        self.assertIn("OpenWeatherMap dă ploaie, dar e singura sursă", text)
        self.assertEqual(self.current["verdict"]["summary"], text)
        self.assertTrue(self.current["verdict"]["summary_en"].startswith("No rain: Băneasa airport"))

    def test_every_witness_is_listed(self) -> None:
        ids = [w["id"] for w in self.verdict["witnesses"]]
        self.assertEqual(ids, ["airport", "open-meteo", "openweathermap", "met-norway", "pirate-weather"])


class ObservedRainTests(unittest.TestCase):
    def test_an_airport_seeing_rain_is_rain_even_at_a_low_chance(self) -> None:
        # Before: the code was called stale at 10% and shown as a grey sky.
        current = ws._merge_current(dry_now(10), owm(800, "cer senin"), None,
                                    metar_obs={**BANEASA, "wmo_code": 61})
        self.assertEqual(current["verdict"]["precipitation"]["state"], "wet")
        self.assertEqual(current["weather_code"], 61)
        self.assertGreater(current["precipitation_mm"], 0)
        self.assertTrue(current["verdict"]["summary"].startswith("Plouă: aeroportul Băneasa"))

    def test_a_weatherxm_gauge_is_ground_truth(self) -> None:
        current = ws._merge_current(dry_now(), None, None, metar_obs=BANEASA,
                                    wxm_norm={"temp": 8.0, "precipitation": 1.2})
        verdict = current["verdict"]["precipitation"]
        self.assertEqual(verdict["state"], "wet")
        self.assertIn("stația WeatherXM (1,2 mm)", verdict["explanation"])

    def test_snow_is_named(self) -> None:
        current = ws._merge_current(dry_now(80), None, None,
                                    metar_obs={**BANEASA, "wmo_code": 73})
        self.assertEqual(current["verdict"]["precipitation"]["kind"], "snow")
        self.assertTrue(current["verdict"]["summary"].startswith("Ninge"))


class RadarTests(unittest.TestCase):
    def test_radar_rain_over_dry_air_is_set_aside_and_said(self) -> None:
        witnesses = arbiter.precipitation_witnesses(
            providers={"open-meteo": (0.0, 3)}, radar_mm=2.2, humidity=52.0)
        verdict = arbiter.judge_precipitation(witnesses, hour_probability=5)
        self.assertEqual(verdict["state"], "dry")
        self.assertIn("Radarul vede ploaie, dar aer prea uscat (52%)", verdict["explanation"])

    def test_radar_rain_in_moist_air_counts(self) -> None:
        witnesses = arbiter.precipitation_witnesses(
            providers={"open-meteo": (0.3, 61)}, radar_mm=2.2, humidity=90.0)
        verdict = arbiter.judge_precipitation(witnesses, hour_probability=60)
        self.assertEqual(verdict["state"], "wet")
        self.assertTrue(verdict["observed"])

    def test_the_current_hours_radar_reaches_the_merge(self) -> None:
        om = dry_now(5)
        hour_time = ws._build_hourly(om)[CURRENT_HOUR_INDEX]["time"]
        current = ws._merge_current(om, None, None, radar_hours={hour_time: 0.0})
        ids = [w["id"] for w in current["verdict"]["precipitation"]["witnesses"]]
        self.assertIn("radar", ids)


class DisagreementTests(unittest.TestCase):
    def test_real_disagreement_is_said_and_left_to_the_forecast(self) -> None:
        # Two providers give 3 mm, the airport next door is dry: not a verdict either way.
        om = dry_now(40)
        om["current"].update({"weather_code": 63, "precipitation": 3.0})
        current = ws._merge_current(om, owm(501, "ploaie moderată", 3.0), None, metar_obs=BANEASA)
        verdict = current["verdict"]["precipitation"]
        self.assertEqual(verdict["state"], "uncertain")
        self.assertTrue(verdict["explanation"].startswith("Semnale amestecate"))
        self.assertGreater(current["precipitation_mm"], 0)

    def test_the_national_models_count_one_by_one(self) -> None:
        witnesses = arbiter.precipitation_witnesses(providers={"open-meteo": (0.0, 3)}, models=(1, 10))
        verdict = arbiter.judge_precipitation(witnesses, hour_probability=5)
        self.assertIn("10 din 11 surse spun uscat", verdict["explanation"])

    def test_learned_trust_moves_a_source(self) -> None:
        def weight(learned: dict[str, float] | None) -> float:
            ws_ = arbiter.precipitation_witnesses(
                providers={"openweathermap": (1.0, 61)}, learned=learned, defaults={"openweathermap": 1.0})
            return ws_[0].llr
        self.assertLess(weight({"openweathermap": 0.6}), weight(None))
        self.assertEqual(weight({"openweathermap": 9.0}), weight(None) * arbiter.TRUST_MAX)


class FogTests(unittest.TestCase):
    def test_a_clear_airport_overrules_a_models_fog(self) -> None:
        om = make_om_payload()
        om["current"]["weather_code"] = 45
        current = ws._merge_current(om, owm(800, "cer senin"), None, metar_obs=BANEASA)
        self.assertEqual(current["weather_code"], 0)
        fog = current["verdict"]["fog"]
        self.assertEqual(fog["state"], "clear")
        self.assertEqual(fog["explanation"], "Fără ceață: aeroportul Băneasa vede 10 km, deși 1 din 2 surse dau ceață.")

    def test_fog_the_airport_sees_is_fog(self) -> None:
        current = ws._merge_current(make_om_payload(), owm(800, "cer senin"), None,
                                    metar_obs={**BANEASA, "visibility_m": 300.0, "wmo_code": 45})
        self.assertEqual(current["weather_code"], 45)
        self.assertEqual(current["verdict"]["fog"]["state"], "fog")

    def test_no_word_about_fog_when_no_one_mentions_it(self) -> None:
        current = ws._merge_current(make_om_payload(), owm(800, "cer senin"), None, metar_obs=BANEASA)
        self.assertIsNone(current["verdict"]["fog"])


class StormTests(unittest.TestCase):
    one = {"count": 1, "near_count": 1, "nearest_km": 6.0, "bearing_deg": 90, "last_minutes_ago": 4, "radius_km": 30}

    def test_a_stray_flash_is_explained(self) -> None:
        current = {"precipitation_probability": 0, "weather_code": 3, "verdict": {"storm": None}}
        ws._corroborated_lightning(self.one, current)
        storm = current["verdict"]["storm"]
        self.assertEqual(storm["state"], "none")
        self.assertIn("un singur fulger la 6 km", storm["explanation"])

    def test_many_flashes_are_a_storm(self) -> None:
        current = {"precipitation_probability": 0, "weather_code": 3, "verdict": {"storm": None}}
        ws._corroborated_lightning({**self.one, "count": 5}, current)
        self.assertEqual(current["verdict"]["storm"]["state"], "storm")
        self.assertIn("5 fulgere", current["verdict"]["storm"]["explanation"])

    def test_quiet_sky_no_storm_verdict(self) -> None:
        current = {"precipitation_probability": 0, "weather_code": 3, "verdict": {"storm": None}}
        ws._corroborated_lightning(None, current)
        self.assertIsNone(current["verdict"]["storm"])


class NamesTests(unittest.TestCase):
    def test_airport_names(self) -> None:
        self.assertEqual(arbiter.airport_name({"name": "Bucharest/Băneasa Intl"}), "Băneasa")
        self.assertEqual(arbiter.airport_name({"name": "Cluj-Napoca Intl"}), "Cluj-Napoca")
        self.assertEqual(arbiter.airport_name({"station": "LRIA"}), "LRIA")
        self.assertIsNone(arbiter.airport_name(None))


class CivicCentreTests(unittest.TestCase):
    """25 Nov 18:42, clear: the radar summed 0.1-0.8 mm an hour from 15:00 under
    a mid-level overcast at 47-66% humidity; every gauge and the airport dry."""

    def test_a_light_echo_in_air_under_75_percent_is_set_aside(self) -> None:
        self.assertFalse(arbiter.radar_rain_plausible(0.24, 65.8))
        self.assertTrue(arbiter.radar_rain_plausible(0.24, 80.0))
        self.assertTrue(arbiter.radar_rain_plausible(2.0, 66.0))
        self.assertFalse(arbiter.radar_rain_plausible(2.0, 52.0))
        witnesses = arbiter.precipitation_witnesses(providers={"open-meteo": (0.0, 0)}, radar_mm=0.24, humidity=65.8)
        verdict = arbiter.judge_precipitation(witnesses, hour_probability=0)
        self.assertIn("Radarul vede ploaie, dar aer prea uscat (66%)", verdict["explanation"])

    def test_the_road_ignores_it_too(self) -> None:
        hour = {"precipitation_mm": 0.0, "relative_humidity": 65.8}
        self.assertFalse(ws._radar_rain_plausible(hour, 0.24))

    def test_gauges_that_measured_the_hour_dry_veto_the_radar(self) -> None:
        self.assertTrue(ws._gauges_saw_dry_hour({"precipitation": 0.0, "distance_km": 8}, {"precipitation": 0}))
        self.assertFalse(ws._gauges_saw_dry_hour({"precipitation": 0.4, "distance_km": 8}, {"precipitation": 0}))
        self.assertFalse(ws._gauges_saw_dry_hour({"precipitation": 0.0, "distance_km": 60}))
        self.assertFalse(ws._gauges_saw_dry_hour(None, {}))


class ShortLineTests(unittest.TestCase):
    def test_the_verdict_fits_one_line(self) -> None:
        current = ws._merge_current(
            dry_now(), owm(501, "ploaie moderată", 1.0), None,
            met_norm=DRY_MET, pw_norm=DRY_PW, metar_obs=BANEASA)
        verdict = current["verdict"]["precipitation"]
        self.assertEqual(verdict["short"], "Nu plouă · 4 din 5 surse")
        self.assertEqual(verdict["short_en"], "No rain · 4 of 5 sources")

    def test_mixed_signals_say_so(self) -> None:
        om = dry_now(40)
        om["current"].update({"weather_code": 63, "precipitation": 3.0})
        current = ws._merge_current(om, owm(501, "ploaie moderată", 3.0), None, metar_obs=BANEASA)
        self.assertEqual(current["verdict"]["precipitation"]["short"], "Semnale amestecate · 2 din 3 surse dau ploaie")


if __name__ == "__main__":
    unittest.main()
