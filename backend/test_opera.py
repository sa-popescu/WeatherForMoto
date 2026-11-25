"""Rain measured by the European radar composite (opera_radar.py): the grid, the files and the road after rain."""

import asyncio
import io
import unittest
from unittest import mock

import h5py
import httpx
import numpy as np

import opera_radar as op
import weather_service as ws

NODATA, UNDETECT = -9999000.0, -8888000.0
BUCHAREST = (44.43, 26.10)


def acrr_file(rain_at: dict[tuple[float, float], float], *, poor_at: tuple[float, float] | None = None) -> bytes:
    """An ODIM hourly sum like OPERA's (2200 x 1900 cells of 2 km): no rain seen anywhere, but the given points."""
    data = np.full((2200, 1900), UNDETECT)
    data[:100, :] = NODATA  # the far north: no radar
    quality = np.ones((2200, 1900))
    for (lat, lon), mm in rain_at.items():
        row, col = op.grid_cell(lat, lon, 2000.0)
        data[row - 1:row + 2, col - 1:col + 2] = mm
    if poor_at is not None:
        row, col = op.grid_cell(*poor_at, 2000.0)
        quality[row - 1:row + 2, col - 1:col + 2] = 0.05
    buffer = io.BytesIO()
    with h5py.File(buffer, "w") as f:
        f.create_group("where").attrs.update({"xscale": 2000.0, "yscale": 2000.0, "xsize": 1900, "ysize": 2200})
        f.create_dataset("dataset1/data1/data", data=data, compression="gzip", chunks=(69, 119))
        f["dataset1/data1"].create_group("what").attrs.update(
            {"gain": 1.0, "offset": 0.0, "nodata": NODATA, "undetect": UNDETECT, "quantity": "ACRR"})
        f.create_dataset("dataset1/data1/quality1/data", data=quality, compression="gzip", chunks=(69, 119))
        f["dataset1/data1/quality1"].create_group("what").attrs.update({"gain": 1.0, "offset": 0.0})
    return buffer.getvalue()


class GridTests(unittest.TestCase):
    def test_the_projection_lands_on_the_corners_of_the_grid(self) -> None:
        # The composite's own corner coordinates are the outer edges of its corner cells.
        self.assertEqual(op.grid_cell(67.0228327624372, -39.5357864125034, 2000.0), (0, -1))
        self.assertEqual(op.grid_cell(31.987650276733, 29.421038635578, 2000.0), (2200, 1900))

    def test_romania_is_inside_the_region_kept(self) -> None:
        r0, r1, c0, c1 = op.region(2000.0, 2200, 1900)
        for lat, lon in (BUCHAREST, (47.65, 26.25), (45.75, 21.23), (44.17, 28.63)):
            row, col = op.grid_cell(lat, lon, 2000.0)
            self.assertTrue(r0 < row < r1 and c0 < col < c1, (lat, lon))

    def test_reading_a_file_and_a_point(self) -> None:
        grid = op.read_hourly_sum(acrr_file({BUCHAREST: 3.5, (44.17, 28.63): 40.0}, poor_at=(44.17, 28.63)))
        self.assertEqual(op.value_at(grid, *BUCHAREST), 3.5)
        # Seen by a radar, nothing fell.
        self.assertEqual(op.value_at(grid, 46.77, 23.60), 0.0)
        # Echoes the composite rates poor (sea clutter): unknown, not rain.
        self.assertIsNone(op.value_at(grid, 44.17, 28.63))
        # Outside the region kept.
        self.assertIsNone(op.value_at(grid, 52.52, 13.40))

    def test_local_hours(self) -> None:
        self.assertEqual(op.local_hours({"2026-11-23T19:00": 5.9}, 7200), {"2026-11-23T21:00": 5.9})
        self.assertEqual(op.local_hours(None, 7200), {})


class RefreshTests(unittest.IsolatedAsyncioTestCase):
    async def test_the_last_full_hours_are_fetched_once_and_shared(self) -> None:
        stamps = [f"20261123T{h:02d}{m:02d}" for h in range(14, 22) for m in (0, 15, 30, 45)]
        listing = {"links": [
            {"rel": "items", "href": f"https://files.example/OPERA@{s}@0@ACRR.h5"} for s in stamps
        ]}
        body = acrr_file({BUCHAREST: 2.0})
        fetched: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            if request.url.host == "api.meteogate.eu":
                return httpx.Response(200, json=listing)
            fetched.append(request.url.path)
            return httpx.Response(200, content=body)

        hourly = op.HourlyRain()
        with mock.patch.object(op, "_hourly", hourly):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                first = await op.measured_rain(*BUCHAREST, client)
                again = await op.measured_rain(*BUCHAREST, client)
        # The six sums that end on the hour, newest last: 16:00 to 21:00.
        self.assertEqual(list(first), [f"2026-11-23T{h}:00" for h in range(16, 22)])
        self.assertEqual(set(first.values()), {2.0})
        self.assertEqual(again, first)
        self.assertEqual(len(fetched), 6)

    async def test_a_radar_that_does_not_answer_gives_nothing(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503)

        with mock.patch.object(op, "_hourly", op.HourlyRain()):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                self.assertIsNone(await op.measured_rain(*BUCHAREST, client))


class RoadTests(unittest.TestCase):
    def hours(self, rain: list[float]) -> list[dict]:
        times = [f"2026-11-23T{h:02d}:00" for h in range(20, 20 + len(rain))]
        return ws._build_hourly({"hourly": {
            "time": times, "temperature_2m": [12] * len(rain), "apparent_temperature": [12] * len(rain),
            "precipitation": rain, "precipitation_probability": [10] * len(rain), "weather_code": [3] * len(rain),
            "wind_gusts_10m": [10] * len(rain), "wind_speed_10m": [8] * len(rain),
            "relative_humidity_2m": [70] * len(rain), "cloud_cover": [50] * len(rain), "is_day": [0] * len(rain),
        }})

    def test_rain_the_radar_saw_wets_the_road_the_model_called_dry(self) -> None:
        hourly = self.hours([0.0, 0.0, 0.0])
        ws._apply_wet_road(hourly, None, {"2026-11-23T20:00": 4.0})
        self.assertEqual([h["wet_road"] for h in hourly], [None, "wet", "wet"])
        # The forecast shown keeps its own numbers.
        self.assertEqual(hourly[0]["precipitation_mm"], 0.0)

    def test_an_echo_over_dry_air_does_not_wet_the_road(self) -> None:
        # Bucharest, 25 Nov 2026: 2.2 mm "measured" at 52% humidity, airport CAVOK.
        hourly = self.hours([0.0, 0.0, 0.0])
        for hour in hourly:
            hour["relative_humidity"] = 52
        ws._apply_wet_road(hourly, None, {"2026-11-23T20:00": 2.2})
        self.assertEqual([h["wet_road"] for h in hourly], [None, None, None])

    def test_rain_the_model_forecast_but_the_radar_did_not_see_leaves_it_dry(self) -> None:
        hourly = self.hours([3.0, 0.0])
        ws._apply_wet_road(hourly, None, {"2026-11-23T20:00": 0.0})
        self.assertEqual([h["wet_road"] for h in hourly], [None, None])


if __name__ == "__main__":
    unittest.main()
