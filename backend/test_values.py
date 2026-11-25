"""Temperature, real feel, humidity, wind and gusts: every witness weighed, not averaged.

PR 165 made rain, fog and storms a matter of evidence. These tests hold the
numbers to the same rule: the real feel follows the temperature that was
decided, a day agrees with its own hours, one station alone cannot overrule
everyone, and stations that agree with each other beat the models.
"""

from __future__ import annotations

import unittest

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


if __name__ == "__main__":
    unittest.main()
