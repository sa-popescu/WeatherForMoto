"""The station networks asked one area at a time: Sensor.Community, Netatmo, and the forecast-only mode."""

import asyncio
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import httpx

import sensor_community
import weather_service as ws

NOW = datetime(2026, 11, 23, 21, 30, tzinfo=timezone.utc)


def reading(sensor_id: int, temp: float | None, *, humidity: float | None = 80.0, pressure_pa: float | None = 101_800.0,
            lat: float = 44.43, lon: float = 26.10, indoor: int = 0, age_min: int = 2) -> dict:
    values = []
    if temp is not None:
        values.append({"value_type": "temperature", "value": str(temp)})
    if humidity is not None:
        values.append({"value_type": "humidity", "value": str(humidity)})
    if pressure_pa is not None:
        values.append({"value_type": "pressure_at_sealevel", "value": str(pressure_pa)})
    return {
        "sensor": {"id": sensor_id, "sensor_type": {"name": "BME280"}},
        "location": {"latitude": str(lat), "longitude": str(lon), "indoor": indoor},
        "timestamp": (NOW - timedelta(minutes=age_min)).strftime("%Y-%m-%d %H:%M:%S"),
        "sensordatavalues": values,
    }


class SensorCommunityTests(unittest.TestCase):
    def test_the_median_of_the_sensors_that_agree(self) -> None:
        records = [
            reading(1, 3.6, lat=44.431), reading(2, 3.9), reading(3, 4.1, lat=44.45),
            reading(4, 20.4),            # a sensor by a warm wall
            reading(5, 21.0, indoor=1),  # indoors: never counted
        ]
        norm = sensor_community.normalize(records, 44.43, 26.10, now=NOW)
        self.assertEqual(norm["temp"], 3.9)
        self.assertEqual(norm["sensors"], 3)
        self.assertEqual(norm["humidity"], 80.0)
        self.assertEqual(norm["pressure"], 1018.0)
        self.assertLess(norm["distance_km"], 0.5)

    def test_too_few_sensors_or_old_readings_give_nothing(self) -> None:
        self.assertIsNone(sensor_community.normalize([reading(1, 3.6), reading(2, 3.9)], 44.43, 26.10, now=NOW))
        old = [reading(i, 4.0, age_min=40) for i in range(5)]
        self.assertIsNone(sensor_community.normalize(old, 44.43, 26.10, now=NOW))
        self.assertIsNone(sensor_community.normalize(None, 44.43, 26.10, now=NOW))

    def test_each_sensor_counts_once_with_its_newest_reading(self) -> None:
        records = [reading(1, 9.0, age_min=4), reading(1, 4.0, age_min=1), reading(2, 4.2), reading(3, 3.8)]
        norm = sensor_community.normalize(records, 44.43, 26.10, now=NOW)
        self.assertEqual((norm["temp"], norm["sensors"]), (4.0, 3))

    def test_the_blend_takes_them_below_the_stations(self) -> None:
        om = {"current": {"temperature_2m": 6.0, "apparent_temperature": 4.0, "relative_humidity_2m": 70}, "hourly": {"time": []}}
        alone = ws._merge_current(om, None, None)
        with_sensors = ws._merge_current(om, None, None, community_norm={"temp": 3.0, "humidity": 90.0})
        self.assertLess(with_sensors["temperature"], alone["temperature"])
        self.assertIn("sensor-community", with_sensors["sources"])


def netatmo_body(temp: float, age_s: int = 300) -> dict:
    ts = int(time.time()) - age_s
    return {"body": [{
        "place": {"location": [26.101, 44.431], "street": "Strada Test"},
        "measures": {"m1": {"res": {str(ts): [temp, 75]}, "type": ["temperature", "humidity"]}},
    }]}


class NetatmoTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        ws._netatmo_cache.clear()
        ws._netatmo_note.clear()
        self.patches = [
            patch.object(ws, "_netatmo_backoff_loaded", True),
            patch.object(ws, "_netatmo_backoff_until", 0.0),
            patch.object(ws, "_netatmo_token", "token"),
            patch.object(ws, "_netatmo_token_expiry", time.time() + 3600),
            patch.object(ws, "_backoff_read_str", lambda key: None),
            patch.object(ws, "_backoff_write", lambda *a: None),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self) -> None:
        for p in self.patches:
            p.stop()
        ws._netatmo_cache.clear()
        ws._netatmo_note.clear()

    async def fetch(self, client: httpx.AsyncClient) -> dict | None:
        return await ws._fetch_netatmo(44.43, 26.10, "id", "secret", "refresh", client)

    async def test_a_failure_keeps_the_last_list_and_is_tried_again_soon(self) -> None:
        answers = [httpx.Response(200, json=netatmo_body(4.5)), httpx.Response(503, json={"error": "busy"})]

        async def handler(request: httpx.Request) -> httpx.Response:
            return answers.pop(0)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            first = await self.fetch(client)
            self.assertEqual(len(first["stations"]), 1)
            cell = ws._coord_key(44.43, 26.10)
            # The list is old: the last one is served at once while a new one is asked for.
            ws._netatmo_cache[cell]["at"] -= ws._NETATMO_REFRESH_S + 1
            served = await self.fetch(client)
            self.assertEqual(served["stations"], first["stations"])
            await ws._netatmo_cache[cell]["task"]
        entry = ws._netatmo_cache[cell]
        # Netatmo failed: the old list stays, and the next try is minutes away, not an hour.
        self.assertEqual(entry["stations"], first["stations"])
        self.assertLessEqual(entry["retry_at"] - time.monotonic(), ws._NETATMO_RETRY_S)
        self.assertEqual(ws._netatmo_note[cell], {"reason": "unavailable", "detail": "HTTP 503"})
        norm = ws._normalize_netatmo_current(served)
        self.assertEqual(norm["temp"], 4.5)
        row = ws._netatmo_row(configured=True, raw=served, norm=norm, note=ws._netatmo_note[cell])
        self.assertEqual((row["status"], row["station"]), ("used", "Strada Test"))

    async def test_a_refused_token_is_renewed_once(self) -> None:
        seen: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.path)
            if request.url.path.endswith("/token"):
                return httpx.Response(200, json={"access_token": "new", "expires_in": 10800})
            if request.headers.get("Authorization") == "Bearer token":
                return httpx.Response(403, json={"error": {"code": 3, "message": "Access token expired"}})
            return httpx.Response(200, json=netatmo_body(5.0))

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            result = await self.fetch(client)
        self.assertEqual(len(result["stations"]), 1)
        self.assertEqual([p.rsplit("/", 1)[-1] for p in seen], ["getpublicdata", "token", "getpublicdata"])

    def test_the_row_says_why(self) -> None:
        self.assertEqual(ws._netatmo_row(configured=False, raw=None, norm=None, note=None)["status"], "off")
        quiet = ws._netatmo_row(configured=True, raw=None, norm=None, note=None)
        self.assertEqual((quiet["status"], quiet["reason"]), ("no-data", "slow"))
        failed = ws._netatmo_row(configured=True, raw=None, norm=None, note={"reason": "unavailable", "detail": "HTTP 503"})
        self.assertEqual((failed["reason"], failed["detail"]), ("unavailable", "HTTP 503"))
        old = ws._netatmo_row(configured=True, raw={"stations": []}, norm=None, note=None)
        self.assertEqual(old["status"], "none-nearby")


class ForecastOnlyTests(unittest.IsolatedAsyncioTestCase):
    async def test_a_forecast_for_later_asks_no_station_network(self) -> None:
        asked: list[str] = []

        async def handler(request: httpx.Request) -> httpx.Response:
            asked.append(request.url.host)
            return httpx.Response(404, json={})

        async def ask(stations: bool) -> set[str]:
            asked.clear()
            async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
                ws.set_http_client(client)
                try:
                    with patch.object(ws, "OPENMETEO_RETRY_DELAY_S", 0), patch.object(ws, "_wxm_backoff_loaded", True), \
                         patch.object(ws, "_wxm_backoff_until", 0.0), patch.object(ws, "_wxm_spent_loaded", True), \
                         patch.object(ws, "_netatmo_backoff_loaded", True), patch.object(ws, "_netatmo_backoff_until", 0.0), \
                         patch.object(ws, "_backoff_read_str", lambda key: None), patch.object(ws, "_backoff_write_str", lambda *a: None):
                        with self.assertRaises(Exception):
                            await ws.get_weather(44.43, 26.10, "Test", "", forecast_days=2, weatherxm_api_key="key",
                                                 netatmo_client_id="id", netatmo_client_secret="s",
                                                 netatmo_refresh_token="r", stations=stations)
                        await asyncio.sleep(0.05)
                finally:
                    ws.set_http_client(None)
                    ws.clear_caches()
                    ws._wxm_near_cache.clear()
                    ws._wxm_spent.clear()
                    ws._netatmo_cache.clear()
            return set(asked)

        networks = {"pro.weatherxm.com", "api.netatmo.com", "data.sensor.community"}
        # The place on screen asks them all; a forecast for later asks none.
        self.assertEqual(await ask(True) & networks, networks)
        self.assertEqual(await ask(False) & networks, set())


if __name__ == "__main__":
    unittest.main()
