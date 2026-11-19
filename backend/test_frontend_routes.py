"""The API origin (run.app) no longer serves the app: "/" redirects to the
static host and /sw.js retires the old service worker."""

import os
import unittest
from unittest import mock

os.environ.setdefault("APP_BASE_URL", "https://weatherformoto.bluemouse.cc")

from fastapi.testclient import TestClient  # noqa: E402

import main  # noqa: E402


class FrontendRoutesTest(unittest.TestCase):
    def setUp(self) -> None:
        self.client = TestClient(main.app, follow_redirects=False)

    def test_root_redirects_to_the_static_app(self) -> None:
        response = self.client.get("/")
        self.assertEqual(response.status_code, 302)
        self.assertEqual(response.headers["location"], f"{main.APP_BASE_URL}/")

    def test_root_never_redirects_to_itself(self) -> None:
        own_host = main.APP_BASE_URL.split("://", 1)[-1]
        response = self.client.get("/", headers={"host": own_host})
        self.assertNotEqual(response.status_code, 302)

    def test_service_worker_retires_itself(self) -> None:
        response = self.client.get("/sw.js")
        self.assertEqual(response.status_code, 200)
        self.assertIn("javascript", response.headers["content-type"])
        self.assertIn("unregister()", response.text)
        self.assertIn(f'"{main.APP_BASE_URL}/"', response.text)
        self.assertIn("no-store", response.headers["cache-control"])


class HealthTest(unittest.TestCase):
    """/health stays 200 whatever the database does; /health/db is the strict one."""

    CONFIGURED = {"TURSO_DATABASE_URL": "libsql://tests.invalid", "TURSO_AUTH_TOKEN": "test-token"}

    def setUp(self) -> None:
        self.client = TestClient(main.app)
        main._db_check = None
        self.addCleanup(setattr, main, "_db_check", None)

    def test_without_a_database_nothing_is_checked(self) -> None:
        with mock.patch.dict(os.environ, {"TURSO_DATABASE_URL": "", "TURSO_AUTH_TOKEN": ""}), \
             mock.patch.object(main, "check_database") as check:
            self.assertEqual(self.client.get("/health").json()["database"], "off")
            self.assertEqual(self.client.get("/health/db").status_code, 200)
        check.assert_not_called()

    def test_a_database_that_answers(self) -> None:
        with mock.patch.dict(os.environ, self.CONFIGURED), mock.patch.object(main, "check_database") as check:
            first = self.client.get("/health")
            self.assertEqual(self.client.get("/health/db").json(), {"database": "ok"})
        self.assertEqual((first.status_code, first.json()["status"], first.json()["database"]), (200, "ok", "ok"))
        # Two pings inside a minute share one look at the database.
        self.assertEqual(check.call_count, 1)

    def test_a_dead_database_does_not_make_the_api_look_dead(self) -> None:
        with mock.patch.dict(os.environ, self.CONFIGURED), \
             mock.patch.object(main, "check_database", side_effect=ValueError("Hrana: blocked")):
            health = self.client.get("/health")
            strict = self.client.get("/health/db")
        self.assertEqual((health.status_code, health.json()["database"]), (200, "unavailable"))
        self.assertEqual(strict.status_code, 503)


if __name__ == "__main__":
    unittest.main()
