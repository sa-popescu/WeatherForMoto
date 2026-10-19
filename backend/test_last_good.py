"""The store behind the last good answer: packing, throttling, and staying quiet."""

import asyncio
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).parent))

import last_good  # noqa: E402


class PackTests(unittest.TestCase):
    def test_a_forecast_survives_the_round_trip(self) -> None:
        payload = {
            "current": {"temperature": 12.5, "code": 61},
            "hourly": [{"time": f"2026-10-19T{h:02d}:00", "precipitation": 0.2} for h in range(24)],
            "stale": None,
        }
        self.assertEqual(last_good._unpack(last_good._pack(payload)), payload)

    def test_packing_is_worth_the_trouble(self) -> None:
        # A forecast is mostly repeated keys; if gzip did not pay off here, the
        # store would be writing hundreds of kilobytes per cell per hour.
        payload = {"hourly": [{"time": f"2026-10-19T{h:02d}:00", "temperature": 11.0 + h % 5} for h in range(168)]}
        raw = len(json.dumps(payload))
        self.assertLess(len(last_good._pack(payload)), raw / 3)


class WriteGuardTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        last_good.reset_for_tests()

    async def test_without_a_database_nothing_is_attempted(self) -> None:
        # Local runs and most deployments of this code have no Turso; the store
        # must cost nothing there rather than leaving a failing thread behind
        # every single request.
        with patch.dict("os.environ", {"TURSO_DATABASE_URL": "", "TURSO_AUTH_TOKEN": ""}):
            self.assertFalse(last_good._configured())
            last_good.remember("44.4300,26.1000", 7, 1000.0, {"a": 1})
            self.assertEqual(last_good._written_at, {})
            self.assertIsNone(await last_good.load("44.4300,26.1000", 7))

    async def test_a_cell_is_not_rewritten_on_every_request(self) -> None:
        written: list[tuple[str, int, float]] = []

        def fake_write(cell: str, days: int, stored_at: float, payload: dict) -> None:
            written.append((cell, days, stored_at))

        with patch.object(last_good, "_configured", lambda: True), \
             patch.object(last_good, "_write", fake_write):
            start = 1000.0
            last_good.remember("44.43,26.10", 7, start, {"a": 1})
            last_good.remember("44.43,26.10", 7, start + 60, {"a": 2})
            last_good.remember("44.43,26.10", 7, start + last_good.MIN_WRITE_INTERVAL_S + 1, {"a": 3})
            last_good.remember("45.00,25.00", 7, start + 60, {"a": 4})
            # The queued writes run in threads; let them finish.
            await asyncio.sleep(0.1)

        self.assertEqual([w[2] for w in written if w[0] == "44.43,26.10"],
                         [start, start + last_good.MIN_WRITE_INTERVAL_S + 1])
        self.assertEqual([w[0] for w in written if w[0] == "45.00,25.00"], ["45.00,25.00"])


if __name__ == "__main__":
    unittest.main()
