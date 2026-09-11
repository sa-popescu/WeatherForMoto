"""The API origin (run.app) no longer serves the app: "/" redirects to the
static host and /sw.js retires the old service worker."""

import os
import unittest

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


if __name__ == "__main__":
    unittest.main()
