"""Rain, snow, fog and lightning that only one source reports, and nothing around backs.

Each case is a way the app used to show weather that was not there: a model
trace at 0%, a "possible shower" kept after the fusion said 5%, a leftover
snow code raising the ice warning, OpenWeatherMap's haze shown as fog, one
yard gauge or one satellite flash taken at face value.
"""

from __future__ import annotations

import unittest

import weather_service as ws
from test_scoring import CURRENT_HOUR_INDEX, make_om_payload, with_value_at

DRY_AIRPORT = {"station": "LRBS", "distance_km": 4.1, "wmo_code": None, "observed_at": "2024-06-01T07:00:00Z"}
WET_AIRPORT = {**DRY_AIRPORT, "wmo_code": 61}
FAR_DRY_AIRPORT = {**DRY_AIRPORT, "distance_km": 18.0}


def owm(owm_id: int, description: str, rain_mm: float | None = None) -> dict:
    current = {
        "main": {"temp": 18.0, "feels_like": 18.0, "humidity": 60, "pressure": 1013},
        "wind": {"speed": 3.0},
        "weather": [{"id": owm_id, "description": description}],
        "visibility": 10000,
    }
    if rain_mm is not None:
        current["rain"] = {"1h": rain_mm}
    return current


class HourlyTraceTests(unittest.TestCase):
    def test_a_model_trace_the_sources_give_no_chance_reads_dry(self) -> None:
        om = make_om_payload(
            precipitation=with_value_at([0.0] * 48, 14, 0.2),
            weather_code=with_value_at([1] * 48, 14, 61),
            precipitation_probability=with_value_at([0] * 48, 14, 5),
        )
        hourly = ws._build_hourly(om)
        ws._settle_hours(hourly)
        hour = hourly[14]
        self.assertEqual(hour["precipitation_mm"], 0.0)
        self.assertEqual(hour["rain_intensity"], "none")
        self.assertEqual(hour["weather_code"], 3)
        self.assertEqual(hour["description"], "Înnorat")

    def test_a_trace_with_a_real_chance_stays(self) -> None:
        om = make_om_payload(
            precipitation=with_value_at([0.0] * 48, 14, 0.2),
            weather_code=with_value_at([1] * 48, 14, 61),
            precipitation_probability=with_value_at([0] * 48, 14, 45),
        )
        hourly = ws._build_hourly(om)
        ws._settle_hours(hourly)
        self.assertEqual(hourly[14]["precipitation_mm"], 0.2)
        self.assertEqual(hourly[14]["rain_intensity"], "urme")

    def test_the_fused_probability_decides_the_words(self) -> None:
        # Open-Meteo alone said 30%: "possible showers". The fusion says 5%.
        om = make_om_payload(
            weather_code=with_value_at([1] * 48, 14, 61),
            precipitation_probability=with_value_at([0] * 48, 14, 30),
        )
        hourly = ws._build_hourly(om)
        self.assertEqual(hourly[14]["weather_code"], ws.RAIN_CHANCE_CODE)
        hourly[14]["precipitation_probability"] = 5
        ws._settle_hours(hourly)
        self.assertEqual(hourly[14]["weather_code"], 3)
        self.assertEqual(hourly[14]["description"], "Înnorat")

    def test_and_can_bring_the_rain_back_when_the_sources_agree(self) -> None:
        om = make_om_payload(
            weather_code=with_value_at([1] * 48, 14, 61),
            precipitation_probability=with_value_at([0] * 48, 14, 10),
        )
        hourly = ws._build_hourly(om)
        self.assertEqual(hourly[14]["weather_code"], 3)
        hourly[14]["precipitation_probability"] = 80
        ws._settle_hours(hourly)
        self.assertEqual(hourly[14]["weather_code"], 61)

    def test_working_fields_never_leave(self) -> None:
        hourly = ws._build_hourly(make_om_payload())
        self.assertIn("_code", hourly[0])
        ws._strip_private(hourly)
        self.assertFalse(any(k.startswith("_") for h in hourly for k in h))


class GroundTests(unittest.TestCase):
    def test_a_leftover_snow_code_does_not_raise_the_ice_warning(self) -> None:
        # 0.5 °C, dry air (dew point far below), a snow code at 0% and 0 mm.
        om = make_om_payload(
            temperature_2m=[0.5] * 48, apparent_temperature=[-2.0] * 48,
            dew_point_2m=[-12.0] * 48, relative_humidity_2m=[40] * 48,
            weather_code=with_value_at([3] * 48, 2, 71),
        )
        hourly = ws._build_hourly(om)
        self.assertFalse(hourly[2]["frost_risk"])
        self.assertEqual(ws._ground_code(71, 0.0, 0), 3)

    def test_a_snow_code_with_a_real_chance_still_does(self) -> None:
        self.assertEqual(ws._ground_code(71, 0.0, 70), 71)

    def test_possible_showers_do_not_wet_the_road(self) -> None:
        self.assertEqual(ws._ground_code(61, 0.0, 40), 3)
        self.assertEqual(ws._ground_code(61, 1.0, 40), 61)


class AirportWitnessTests(unittest.TestCase):
    def test_a_dry_airport_next_door_turns_a_trace_into_a_chance(self) -> None:
        om = make_om_payload(precipitation_probability=with_value_at([0] * 48, CURRENT_HOUR_INDEX, 40))
        om["current"].update({"weather_code": 3, "precipitation": 0.0})
        current = ws._merge_current(om, owm(500, "ploaie ușoară", 0.5), None, metar_obs=DRY_AIRPORT)
        self.assertEqual(current["precipitation_mm"], 0.0)
        self.assertEqual(current["weather_code"], ws.RAIN_CHANCE_CODE)

    def test_a_far_airport_is_no_witness(self) -> None:
        om = make_om_payload(precipitation_probability=with_value_at([0] * 48, CURRENT_HOUR_INDEX, 40))
        om["current"].update({"weather_code": 3, "precipitation": 0.0})
        current = ws._merge_current(om, owm(500, "ploaie ușoară", 0.5), None, metar_obs=FAR_DRY_AIRPORT)
        self.assertGreater(current["precipitation_mm"], 0)

    def test_a_wet_airport_confirms_it(self) -> None:
        om = make_om_payload(precipitation_probability=with_value_at([0] * 48, CURRENT_HOUR_INDEX, 40))
        om["current"].update({"weather_code": 3, "precipitation": 0.0})
        current = ws._merge_current(om, owm(500, "ploaie ușoară", 0.5), None, metar_obs=WET_AIRPORT)
        self.assertGreater(current["precipitation_mm"], 0)
        self.assertEqual(current["weather_code"], 61)

    def test_a_real_amount_stands_against_a_dry_airport(self) -> None:
        om = make_om_payload(precipitation_probability=with_value_at([0] * 48, CURRENT_HOUR_INDEX, 40))
        om["current"].update({"weather_code": 63, "precipitation": 3.0})
        current = ws._merge_current(om, owm(501, "ploaie moderată", 3.0), None, metar_obs=DRY_AIRPORT)
        self.assertGreaterEqual(current["precipitation_mm"], ws.UNBACKED_TRACE_MAX_MM_H)


class GaugeTests(unittest.TestCase):
    @staticmethod
    def netatmo(rain: float) -> dict:
        return {"temp": 18.0, "humidity": 60.0, "precipitation": rain}

    def test_a_dry_gauge_here_overrules_a_providers_trace(self) -> None:
        om = make_om_payload(precipitation_probability=with_value_at([0] * 48, CURRENT_HOUR_INDEX, 40))
        current = ws._merge_current(om, owm(500, "ploaie ușoară", 0.6), None, netatmo_norm=self.netatmo(0.0))
        self.assertEqual(current["precipitation_mm"], 0.0)

    def test_one_yard_gauge_against_a_dry_sky_is_not_taken(self) -> None:
        om = make_om_payload()
        current = ws._merge_current(om, None, None, netatmo_norm=self.netatmo(0.4), metar_obs=DRY_AIRPORT)
        self.assertEqual(current["precipitation_mm"], 0.0)
        self.assertEqual(current["rain_intensity"], "none")

    def test_a_yard_gauge_is_taken_when_the_sky_could_rain(self) -> None:
        om = make_om_payload(precipitation_probability=with_value_at([0] * 48, CURRENT_HOUR_INDEX, 40))
        current = ws._merge_current(om, None, None, netatmo_norm=self.netatmo(0.4), metar_obs=DRY_AIRPORT)
        self.assertGreater(current["precipitation_mm"], 0)


class HazeTests(unittest.TestCase):
    def test_owm_haze_and_mist_are_not_fog(self) -> None:
        for owm_id in (701, 711, 721, 731, 751, 761, 762, 771, 781):
            with self.subTest(owm_id=owm_id):
                self.assertIsNone(ws._owm_id_to_wmo(owm_id))
        self.assertEqual(ws._owm_id_to_wmo(741), 45)

    def test_a_hazy_afternoon_reads_as_the_models_sky(self) -> None:
        om = make_om_payload()
        om["current"]["weather_code"] = 2
        current = ws._merge_current(om, owm(721, "ceață ușoară"), None)
        self.assertEqual(current["weather_code"], 2)
        self.assertNotIn("ceață", current["description"].lower())


class LightningTests(unittest.TestCase):
    one = {"count": 1, "near_count": 1, "nearest_km": 6.0, "bearing_deg": 90, "last_minutes_ago": 4, "radius_km": 30}

    def test_one_flash_over_a_dry_quiet_sky_is_set_aside(self) -> None:
        out = ws._corroborated_lightning(self.one, {"precipitation_probability": 0, "weather_code": 3})
        self.assertEqual(out["count"], 0)
        self.assertEqual(out["near_count"], 0)
        self.assertEqual(out["unconfirmed"], 1)

    def test_one_flash_in_convective_air_is_shown(self) -> None:
        current = {"precipitation_probability": 10, "weather_code": 3, "rain_character": "convective"}
        self.assertEqual(ws._corroborated_lightning(self.one, current)["count"], 1)

    def test_one_flash_with_a_real_chance_is_shown(self) -> None:
        self.assertEqual(ws._corroborated_lightning(self.one, {"precipitation_probability": 40})["count"], 1)

    def test_several_flashes_are_always_shown(self) -> None:
        many = {**self.one, "count": 3}
        self.assertEqual(ws._corroborated_lightning(many, {"precipitation_probability": 0})["count"], 3)


if __name__ == "__main__":
    unittest.main()
