"""The state of the road: still wet after rain, or the first rain after dry days (wet_road.py)."""

import unittest

import weather_service as ws
import wet_road


def hour(mm: float = 0.0, *, temp: float = 15.0, humidity: float = 60.0, wind: float = 10.0,
         cloud: float = 50.0, day: bool = True, time: str = "2026-11-24T10:00") -> dict:
    return {
        "time": time, "precipitation_mm": mm, "temperature": temp, "relative_humidity": humidity,
        "wind_speed_kmh": wind, "cloud_cover": cloud, "is_day": day,
    }


class DryingTests(unittest.TestCase):
    def test_mild_weather_takes_the_base_time(self) -> None:
        self.assertEqual(wet_road.drying_hours(15, 60, 10, 50, True), wet_road.BASE_DRYING_HOURS)

    def test_cold_damp_dark_and_still_take_longer_sun_and_wind_shorter(self) -> None:
        slow = wet_road.drying_hours(2, 95, 5, 100, False)
        fast = wet_road.drying_hours(22, 40, 30, 10, True)
        self.assertEqual(slow, wet_road.MAX_DRYING_HOURS)
        self.assertLess(fast, wet_road.BASE_DRYING_HOURS)
        self.assertGreaterEqual(fast, wet_road.MIN_DRYING_HOURS)

    def test_unknown_values_change_nothing(self) -> None:
        self.assertEqual(wet_road.drying_hours(None, None, None, None, None), wet_road.BASE_DRYING_HOURS)


class StateTests(unittest.TestCase):
    def test_road_stays_wet_for_a_while_after_rain(self) -> None:
        hours = [hour(2.0), hour(), hour(), hour(), hour()]
        states = wet_road.road_states(hours, past_rain=[0.0] * 3)
        # During the rain it is the rain that counts; then two mild hours of drying.
        self.assertEqual(states, [None, "wet", None, None, None])

    def test_a_cold_night_keeps_it_wet_longer(self) -> None:
        night = dict(temp=3.0, humidity=92.0, wind=5.0, cloud=100.0, day=False)
        hours = [hour(1.0), *[hour(**night) for _ in range(8)]]
        states = wet_road.road_states(hours, past_rain=[])
        self.assertEqual(states.count("wet"), 6)

    def test_last_nights_rain_still_counts_in_the_morning(self) -> None:
        states = wet_road.road_states([hour(), hour()], past_rain=[0.0, 3.0])
        self.assertEqual(states[0], "wet")

    def test_first_rain_after_dry_days(self) -> None:
        dry_days = [0.0] * wet_road.FIRST_RAIN_DRY_HOURS
        states = wet_road.road_states([hour(), hour(0.5), hour(1.0), hour(1.0), hour()], past_rain=dry_days)
        self.assertEqual(states[1:4], ["first_rain", "first_rain", None])
        # Rain yesterday: not a first rain.
        wet_week = [0.0] * 100 + [1.0] + [0.0] * 30
        self.assertNotIn("first_rain", wet_road.road_states([hour(1.0)], past_rain=wet_week))

    def test_without_the_past_a_first_rain_is_never_guessed(self) -> None:
        self.assertEqual(wet_road.road_states([hour(1.0), hour()], past_rain=None), [None, "wet"])

    def test_the_measured_rain_decides_now(self) -> None:
        self.assertIsNone(wet_road.current_state("wet", 1.2))
        self.assertEqual(wet_road.current_state("wet", 0.0), "wet")
        self.assertIsNone(wet_road.current_state("first_rain", 0.0))
        self.assertEqual(wet_road.current_state("first_rain", None), "first_rain")


class ScoreTests(unittest.TestCase):
    def test_a_wet_road_costs_points_and_says_why(self) -> None:
        dry, _ = ws._score_with_breakdown(18, 10, 0.0, 1, 5)
        wet, factors = ws._score_with_breakdown(18, 10, 0.0, 1, 5, wet_road_state="wet")
        self.assertEqual(dry - wet, wet_road.WET_ROAD_PENALTY)
        self.assertEqual(factors[-1]["factor"], "wet_road")
        self.assertIn("estimare", factors[-1]["detail"])

    def test_first_rain_adds_to_the_rain(self) -> None:
        rain, _ = ws._score_with_breakdown(18, 10, 1.0, 61, 80)
        first, factors = ws._score_with_breakdown(18, 10, 1.0, 61, 80, wet_road_state="first_rain")
        self.assertLessEqual(first, rain)
        self.assertIn("first_rain", [f["factor"] for f in factors])

    def test_hours_are_marked_and_rescored_in_place(self) -> None:
        hours = [ws._build_hourly({"hourly": {
            "time": ["2026-11-24T08:00", "2026-11-24T09:00", "2026-11-24T10:00"],
            "temperature_2m": [15, 15, 15], "apparent_temperature": [15, 15, 15],
            "precipitation": [2.0, 0.0, 0.0], "precipitation_probability": [90, 5, 5],
            "weather_code": [61, 1, 1], "wind_gusts_10m": [10, 10, 10], "wind_speed_10m": [8, 8, 8],
            "relative_humidity_2m": [70, 70, 70], "cloud_cover": [50, 50, 50], "is_day": [1, 1, 1],
        }})]
        hourly = hours[0]
        before = hourly[1]["moto_score"]
        past = {"hourly": {"time": ["2026-11-24T06:00", "2026-11-24T07:00", "2026-11-24T08:00"], "precipitation": [0.0, 0.0, 2.0]}}
        ws._apply_wet_road(hourly, past)
        self.assertEqual([h["wet_road"] for h in hourly], [None, "wet", None])
        self.assertEqual(before - hourly[1]["moto_score"], wet_road.WET_ROAD_PENALTY)

    def test_published_rules(self) -> None:
        meta = ws.scoring_metadata()["wet_road"]
        self.assertEqual(meta["wet_penalty"], wet_road.WET_ROAD_PENALTY)
        self.assertEqual(meta["first_rain"]["dry_hours"], wet_road.FIRST_RAIN_DRY_HOURS)


if __name__ == "__main__":
    unittest.main()
