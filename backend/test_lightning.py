"""Lightning seen by Meteosat Third Generation (mtg_lightning.py): the files, a place's summary and the refresh."""

import io
import time
import unittest
import zipfile
from datetime import datetime, timedelta, timezone
from unittest import mock

import h5py
import httpx
import numpy as np

import mtg_lightning as ml
import weather_service as ws

BUCHAREST = (44.43, 26.10)
SCALE = 0.0027
FILL = -32767


def flash_file(flashes: list[tuple[float, float, datetime]], *, fill_one: bool = False) -> bytes:
    """A LI-2-LFL body like EUMETSAT's: packed int16 latitude / longitude, seconds since 2000 for the time."""
    lat = np.array([round(f[0] / SCALE) for f in flashes], dtype=np.int16)
    lon = np.array([round(f[1] / SCALE) for f in flashes], dtype=np.int16)
    t = np.array([(f[2] - ml.EPOCH).total_seconds() for f in flashes], dtype=np.float64)
    if fill_one and flashes:
        lat[0] = FILL
    buffer = io.BytesIO()
    with h5py.File(buffer, "w") as f:
        for name, values in (("latitude", lat), ("longitude", lon)):
            ds = f.create_dataset(name, data=values)
            ds.attrs.update({"scale_factor": np.float32(SCALE), "add_offset": np.float32(0.0),
                             "_FillValue": np.int16(FILL)})
        f.create_dataset("flash_time", data=t).attrs["units"] = "seconds since 2000-01-01 00:00:00.0"
    return buffer.getvalue()


def product_zip(body: bytes, stamp: str = "20261124211008_20261124212008") -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        z.writestr(f"W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+LI-2-LFL--FD--CHK-BODY--ARC-NC4E_C_EUMT_1_L2PF_OPE_{stamp}_N__O_0128_0001.nc", body)
        z.writestr("manifest.xml", "<manifest/>")
    return buffer.getvalue()


class FileTests(unittest.TestCase):
    def test_flashes_outside_the_region_and_fill_values_are_dropped(self) -> None:
        now = datetime(2026, 11, 24, 21, 15, tzinfo=timezone.utc)
        data = flash_file([(44.5, 26.1, now), (44.0, 26.0, now), (10.0, 5.0, now), (48.0, 2.0, now)], fill_one=True)
        flashes = ml.read_flashes(ml.body_from_zip(product_zip(data)))
        self.assertEqual(len(flashes), 1)
        self.assertAlmostEqual(float(flashes.lat[0]), 44.0, places=2)
        self.assertAlmostEqual(float(flashes.t[0]), now.timestamp(), places=3)

    def test_a_file_without_flash_variables_is_refused(self) -> None:
        buffer = io.BytesIO()
        with h5py.File(buffer, "w") as f:
            f.create_dataset("something", data=[1, 2])
        with self.assertRaises(ValueError):
            ml.read_flashes(buffer.getvalue())

    def test_product_name_gives_the_end_of_its_period(self) -> None:
        pid = "W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+LI-2-LFL--FD--x-x--ARC-x_C_EUMT_20261124212021_L2PF_OPE_20261124211008_20261124212008_N__O_0128_0000"
        self.assertEqual(ml._end_of(pid), datetime(2026, 11, 24, 21, 20, 8, tzinfo=timezone.utc))


class SummaryTests(unittest.TestCase):
    def test_count_nearest_direction_and_age(self) -> None:
        now = datetime(2026, 11, 24, 21, 30, tzinfo=timezone.utc)
        flashes = ml.read_flashes(flash_file([
            (44.43, 26.20, now - timedelta(minutes=5)),    # about 8 km east
            (44.60, 26.10, now - timedelta(minutes=12)),   # about 19 km north
            (45.40, 26.10, now - timedelta(minutes=2)),    # about 108 km: too far
            (44.44, 26.11, now - timedelta(minutes=50)),   # close, but too old
        ]))
        out = ml.summary([flashes], *BUCHAREST, now.timestamp())
        self.assertEqual(out["count"], 2)
        self.assertEqual(out["near_count"], 1)
        self.assertAlmostEqual(out["nearest_km"], 8.0, delta=0.5)
        self.assertTrue(80 <= out["bearing_deg"] <= 100, out["bearing_deg"])
        self.assertEqual(out["last_minutes_ago"], 5)
        self.assertEqual(out["attribution"], "© EUMETSAT")

    def test_no_flashes_is_an_answer_not_a_failure(self) -> None:
        out = ml.summary([ml.read_flashes(flash_file([]))], *BUCHAREST, time.time())
        self.assertEqual((out["count"], out["nearest_km"], out["last_minutes_ago"]), (0, None, None))


class RefreshTests(unittest.IsolatedAsyncioTestCase):
    async def test_files_are_fetched_once_with_a_token_and_shared(self) -> None:
        now = datetime.now(timezone.utc)
        end = now - timedelta(minutes=8)
        stamp = f"{(end - timedelta(minutes=10)):%Y%m%d%H%M%S}_{end:%Y%m%d%H%M%S}"
        pid = f"W_XX-EUMETSAT-Darmstadt,IMG+SAT,MTI1+LI-2-LFL--FD--x-x--ARC-x_C_EUMT_1_L2PF_OPE_{stamp}_N__O_0128_0000"
        old_end = now - timedelta(hours=3)
        old_pid = pid.replace(stamp, f"{(old_end - timedelta(minutes=10)):%Y%m%d%H%M%S}_{old_end:%Y%m%d%H%M%S}")
        body = product_zip(flash_file([(44.45, 26.12, end - timedelta(minutes=3))]), stamp)
        calls: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            calls.append(request.url.path)
            if request.url.path == "/token":
                self.assertTrue(request.headers["authorization"].startswith("Basic "))
                return httpx.Response(200, json={"access_token": "t0k", "expires_in": 3600})
            if request.url.path.endswith("/os"):
                return httpx.Response(200, json={"features": [
                    {"id": i, "properties": {"links": {"data": [{"href": f"https://api.eumetsat.int/data/download/{n}"}]}}}
                    for n, i in enumerate((pid, old_pid))]})
            self.assertEqual(request.headers["authorization"], "Bearer t0k")
            self.assertEqual(request.url.path, "/data/download/0")
            return httpx.Response(200, content=body)

        with mock.patch.object(ml, "_lightning", ml.Lightning()), \
                mock.patch.object(ml, "CONSUMER_KEY", "k"), mock.patch.object(ml, "CONSUMER_SECRET", "s"):
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                first = await ml.near(*BUCHAREST, client)
                again = await ml.near(*BUCHAREST, client)
        self.assertEqual(first["count"], 1)
        self.assertLess(first["nearest_km"], 5)
        self.assertEqual(again["count"], 1)
        self.assertEqual(calls.count("/data/download/0"), 1)
        self.assertEqual(calls.count("/token"), 1)

    async def test_off_without_keys_and_quiet_when_unreachable(self) -> None:
        async def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            with mock.patch.object(ml, "CONSUMER_KEY", ""):
                self.assertIsNone(await ml.near(*BUCHAREST, client))
            with mock.patch.object(ml, "_lightning", ml.Lightning()), \
                    mock.patch.object(ml, "CONSUMER_KEY", "k"), mock.patch.object(ml, "CONSUMER_SECRET", "s"):
                self.assertIsNone(await ml.near(*BUCHAREST, client))


class SourceStatusTests(unittest.TestCase):
    def row(self, lightning: tuple) -> dict:
        rows = ws._source_status(current={}, keys={}, used={s: False for s in ("openweathermap", "met-norway", "pirate-weather")},
                                 stations=(None, None), airports=(None, None), weatherxm=(None, None, None),
                                 warnings={}, lightning=lightning)
        return next(r for r in rows if r["id"] == "mtg-lightning")

    def test_states(self) -> None:
        self.assertEqual(self.row((False, None))["status"], "off")
        self.assertEqual(self.row((True, None))["status"], "no-data")
        used = self.row((True, {"count": 3, "nearest_km": 12.5, "observed_until": "2026-11-24T21:20:08+00:00"}))
        self.assertEqual((used["status"], used["count"], used["distance_km"]), ("used", 3, 12.5))


if __name__ == "__main__":
    unittest.main()
