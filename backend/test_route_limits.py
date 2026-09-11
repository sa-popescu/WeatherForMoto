"""The multi-stop route endpoint accepts as many stops as the app's editor."""

import unittest

from fastapi.testclient import TestClient

import main


class RouteStopLimitTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(main.app)

    def _stops(self, count: int) -> str:
        return ";".join(f"City{i}" for i in range(count))

    def test_editor_limit_and_api_limit_match(self) -> None:
        self.assertEqual(main.MAX_ROUTE_STOPS, 10)

    def test_one_stop_is_rejected(self) -> None:
        response = self.client.get("/route/multi", params={"stops": self._stops(1)})
        self.assertEqual(response.status_code, 422)
        self.assertIn("At least 2 stops", response.json()["detail"])

    def test_more_than_the_limit_is_rejected(self) -> None:
        count = main.MAX_ROUTE_STOPS + 1
        response = self.client.get("/route/multi", params={"stops": self._stops(count)})
        self.assertEqual(response.status_code, 422)
        self.assertIn(f"Maximum {main.MAX_ROUTE_STOPS} stops", response.json()["detail"])


if __name__ == "__main__":
    unittest.main()
