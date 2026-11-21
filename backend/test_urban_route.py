"""Urban routes with live traffic (TomTom): parsing, the request budget and the endpoint."""

import asyncio
import unittest
from unittest import mock

import httpx
from fastapi.testclient import TestClient

import main
import urban_route

TOMTOM_ANSWER = {
    "formatVersion": "0.0.12",
    "routes": [
        {
            "summary": {
                "lengthInMeters": 2839,
                "travelTimeInSeconds": 540,
                "trafficDelayInSeconds": 120,
                "noTrafficTravelTimeInSeconds": 420,
            },
            "legs": [
                {"points": [{"latitude": 44.4268, "longitude": 26.1025}, {"latitude": 44.43, "longitude": 26.104}]},
                {"points": [{"latitude": 44.43, "longitude": 26.104}, {"latitude": 44.444, "longitude": 26.1105}]},
            ],
            "sections": [
                {"startPointIndex": 0, "endPointIndex": 1, "sectionType": "TRAFFIC", "simpleCategory": "JAM", "magnitudeOfDelay": 2, "delayInSeconds": 120, "effectiveSpeedInKmh": 12},
                {"startPointIndex": 0, "endPointIndex": 2, "sectionType": "TRAVEL_MODE", "travelMode": "motorcycle"},
            ],
            "guidance": {
                "instructions": [
                    {"routeOffsetInMeters": 0, "pointIndex": 0, "travelTimeInSeconds": 0, "maneuver": "DEPART", "street": "Piața Unirii"},
                    {"routeOffsetInMeters": 640, "pointIndex": 1, "travelTimeInSeconds": 150, "maneuver": "ROUNDABOUT_RIGHT", "roundaboutExitNumber": 1, "street": "Bulevardul Nicolae Bălcescu", "roadNumbers": ["DN1", "DN7"]},
                    {"routeOffsetInMeters": 2839, "pointIndex": 2, "travelTimeInSeconds": 540, "maneuver": "ARRIVE"},
                ]
            },
        }
    ],
}


class ParseTest(unittest.TestCase):
    def test_points(self) -> None:
        self.assertEqual(urban_route.parse_points("44.4,26.1; 44.5,26.2"), [(44.4, 26.1), (44.5, 26.2)])
        for bad in ("44.4,26.1", "44.4;26.1", "95,26;44,26", ";".join(["44,26"] * 6)):
            with self.assertRaises(ValueError):
                urban_route.parse_points(bad)

    def test_request_asks_for_a_motorcycle_with_traffic(self) -> None:
        url, params = urban_route.tomtom_request([(44.4, 26.1), (44.5, 26.2)], 2)
        self.assertIn("44.400000,26.100000:44.500000,26.200000", url)
        self.assertEqual(params["travelMode"], "motorcycle")
        self.assertEqual(params["traffic"], "true")
        self.assertEqual(params["maxAlternatives"], 2)
        self.assertEqual(params["instructionsType"], "coded")
        # Alternatives only between two points.
        _, via = urban_route.tomtom_request([(44.4, 26.1), (44.45, 26.15), (44.5, 26.2)], 2)
        self.assertEqual(via["maxAlternatives"], 0)

    def test_answer(self) -> None:
        [route] = urban_route.parse_tomtom(TOMTOM_ANSWER)
        self.assertEqual(route["distance_m"], 2839)
        self.assertEqual(route["duration_s"], 540)
        self.assertEqual(route["delay_s"], 120)
        self.assertEqual(route["no_traffic_s"], 420)
        # The joining point of the two legs is kept once.
        self.assertEqual(len(route["coords"]), 3)
        self.assertEqual(route["traffic"], [{"from": 0, "to": 1, "category": "JAM", "magnitude": 2, "delay_s": 120, "speed_kmh": 12}])
        self.assertEqual(route["instructions"][1]["maneuver"], "ROUNDABOUT_RIGHT")
        self.assertEqual(route["instructions"][1]["road"], "DN1, DN7")
        self.assertEqual(route["instructions"][1]["roundabout_exit"], 1)

    def test_no_route(self) -> None:
        with self.assertRaises(urban_route.UrbanRouteUnavailable):
            urban_route.parse_tomtom({"routes": []})


class BudgetTest(unittest.TestCase):
    def test_per_client_window_and_daily_total(self) -> None:
        budget = urban_route.RequestBudget(per_client=2, window_s=60, daily=3)
        now = 1_800_000_000.0
        self.assertTrue(budget.allow("a", now))
        self.assertTrue(budget.allow("a", now + 1))
        self.assertFalse(budget.allow("a", now + 2))
        # The window slides.
        self.assertTrue(budget.allow("a", now + 61))
        # The day is used up for everyone.
        self.assertFalse(budget.allow("b", now + 62))


def _client(handler) -> httpx.AsyncClient:
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


class FetchTest(unittest.TestCase):
    def run_fetch(self, handler):
        async def go():
            async with _client(handler) as client:
                return await urban_route.fetch_urban_routes(client, [(44.4, 26.1), (44.5, 26.2)], 2)

        return asyncio.run(go())

    def test_routes_and_attribution(self) -> None:
        with mock.patch.object(urban_route, "TOMTOM_API_KEY", "test-key"):
            result = self.run_fetch(lambda request: httpx.Response(200, json=TOMTOM_ANSWER))
        self.assertEqual(result["source"], "tomtom")
        self.assertEqual(result["attribution"], "© TomTom")
        self.assertEqual(len(result["routes"]), 1)

    def test_reasons(self) -> None:
        cases = {429: "quota", 403: "upstream", 400: "no-route", 500: "upstream"}
        for status, reason in cases.items():
            with self.subTest(status=status), mock.patch.object(urban_route, "TOMTOM_API_KEY", "test-key"):
                with self.assertRaises(urban_route.UrbanRouteUnavailable) as ctx:
                    self.run_fetch(lambda request, s=status: httpx.Response(s))
                self.assertEqual(ctx.exception.reason, reason)

    def test_no_key(self) -> None:
        with mock.patch.object(urban_route, "TOMTOM_API_KEY", ""):
            with self.assertRaises(urban_route.UrbanRouteUnavailable) as ctx:
                self.run_fetch(lambda request: httpx.Response(200, json=TOMTOM_ANSWER))
        self.assertEqual(ctx.exception.reason, "no-key")

    def test_network_error_does_not_carry_the_key(self) -> None:
        def fail(request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError(f"cannot reach {request.url}")

        with mock.patch.object(urban_route, "TOMTOM_API_KEY", "secret-key-123"):
            with self.assertRaises(urban_route.UrbanRouteUnavailable) as ctx:
                self.run_fetch(fail)
        self.assertNotIn("secret-key-123", repr(ctx.exception))
        self.assertIsNone(ctx.exception.__cause__)


class EndpointTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(main.app)
        urban_route.budget = urban_route.RequestBudget(per_client=30, window_s=600, daily=2000)

    def test_bad_points(self) -> None:
        response = self.client.get("/route/urban", params={"points": "44.4,26.1"})
        self.assertEqual(response.status_code, 422)

    def test_without_a_key(self) -> None:
        with mock.patch.object(urban_route, "TOMTOM_API_KEY", ""):
            response = self.client.get("/route/urban", params={"points": "44.4,26.1;44.5,26.2"})
        self.assertEqual(response.status_code, 503)
        self.assertEqual(response.json()["detail"], "no-key")

    def test_routes(self) -> None:
        answer = {"source": "tomtom", "attribution": "© TomTom", "routes": urban_route.parse_tomtom(TOMTOM_ANSWER)}
        with mock.patch.object(urban_route, "TOMTOM_API_KEY", "test-key"), mock.patch.object(
            urban_route, "fetch_urban_routes", mock.AsyncMock(return_value=answer)
        ):
            response = self.client.get("/route/urban", params={"points": "44.4,26.1;44.5,26.2"})
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["routes"][0]["distance_m"], 2839)

    def test_busy(self) -> None:
        urban_route.budget = urban_route.RequestBudget(per_client=1, window_s=600, daily=2000)
        with mock.patch.object(urban_route, "TOMTOM_API_KEY", "test-key"), mock.patch.object(
            urban_route, "fetch_urban_routes", mock.AsyncMock(side_effect=urban_route.UrbanRouteUnavailable("no-route"))
        ):
            first = self.client.get("/route/urban", params={"points": "44.4,26.1;44.5,26.2"})
            second = self.client.get("/route/urban", params={"points": "44.4,26.1;44.5,26.2"})
        self.assertEqual(first.status_code, 404)
        self.assertEqual(second.status_code, 429)
        self.assertEqual(second.json()["detail"], "busy")


if __name__ == "__main__":
    unittest.main()
