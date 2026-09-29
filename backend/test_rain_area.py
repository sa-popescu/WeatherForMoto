"""Tests for the spatial coverage of rain (rain_area.py). No network."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import rain_area  # noqa: E402


def payload(times: list[str], amounts: list[float | None]) -> dict:
    return {"hourly": {"time": times, "precipitation": amounts}}


class RingTests(unittest.TestCase):
    def test_the_ring_is_the_centre_plus_its_circle(self) -> None:
        points = rain_area.ring_points(44.43, 26.10)
        self.assertEqual(len(points), rain_area.RING_POINTS + 1)
        self.assertEqual(points[0], (44.43, 26.10))

    def test_every_point_lands_about_the_asked_distance_away(self) -> None:
        centre, *ring = rain_area.ring_points(44.43, 26.10, radius_km=15)
        for lat, lon in ring:
            north_km = (lat - centre[0]) * 111
            east_km = (lon - centre[1]) * 111 * 0.712  # cos(44.43°)
            self.assertAlmostEqual((north_km**2 + east_km**2) ** 0.5, 15, delta=0.5)


class CoverageTests(unittest.TestCase):
    TIMES = ["2026-09-29T12:00", "2026-09-29T13:00"]

    def test_counts_the_share_of_points_that_get_wet(self) -> None:
        raw = [
            payload(self.TIMES, [0.5, 0.0]),
            payload(self.TIMES, [0.3, 0.0]),
            payload(self.TIMES, [0.0, 0.0]),
            payload(self.TIMES, [0.0, 0.0]),
        ]
        self.assertEqual(rain_area.coverage_by_time(raw), {self.TIMES[0]: 0.5, self.TIMES[1]: 0.0})

    def test_a_point_below_the_bar_is_dry(self) -> None:
        raw = [payload(self.TIMES, [0.05, 0.1]), payload(self.TIMES, [0.0, 0.0])]
        self.assertEqual(rain_area.coverage_by_time(raw)[self.TIMES[0]], 0.0)
        self.assertEqual(rain_area.coverage_by_time(raw)[self.TIMES[1]], 0.5)

    def test_a_point_that_came_back_short_is_not_counted_as_dry(self) -> None:
        raw = [payload(self.TIMES, [0.5, 0.5]), payload(self.TIMES, [None, None])]
        # One usable point, and it is wet: coverage is 1, not 0.5.
        self.assertEqual(rain_area.coverage_by_time(raw)[self.TIMES[0]], 1.0)

    def test_one_coordinate_answers_as_a_bare_object(self) -> None:
        self.assertEqual(rain_area.coverage_by_time(payload(self.TIMES, [1.0, 0.0])),
                         {self.TIMES[0]: 1.0, self.TIMES[1]: 0.0})

    def test_nothing_usable_is_no_coverage_rather_than_zero(self) -> None:
        self.assertEqual(rain_area.coverage_by_time(None), {})
        self.assertEqual(rain_area.coverage_by_time([{"hourly": {}}]), {})


class ExtentTests(unittest.TestCase):
    def test_names_the_three_bands(self) -> None:
        self.assertEqual(rain_area.extent(0.1), rain_area.EXTENT_ISOLATED)
        self.assertEqual(rain_area.extent(0.4), rain_area.EXTENT_SCATTERED)
        self.assertEqual(rain_area.extent(0.9), rain_area.EXTENT_WIDESPREAD)
        self.assertIsNone(rain_area.extent(None))


if __name__ == "__main__":
    unittest.main(verbosity=2)
