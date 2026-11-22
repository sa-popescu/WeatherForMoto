"""Error reports from the app: what is kept, what is mailed and how often."""

import json
import os
import unittest
from unittest import mock

from fastapi.testclient import TestClient

import client_errors
import main

DAY = 1_795_000_000.0  # some moment in late November 2026, UTC


def _body(**overrides: object) -> bytes:
    data: dict[str, object] = {
        "kind": "error",
        "message": "Cannot read properties of undefined (reading 'lat')",
        "stack": "TypeError: x\n at f (https://motonor.bluemouse.cc/assets/index-1a2b.js?v=3#x:1:200)\n at g (index.js:9:9)",
        "version": "2.0.0+abc1234",
        "tab": "drum",
        "browser": "Mozilla/5.0 (iPhone)",
    }
    data.update(overrides)
    return json.dumps(data).encode("utf-8")


class FakeState:
    def __init__(self, broken: bool = False) -> None:
        self.values: dict[str, str] = {}
        self.broken = broken
        self.reads = 0
        self.writes = 0

    def get(self, key: str) -> str | None:
        self.reads += 1
        if self.broken:
            raise RuntimeError("BLOCKED")
        return self.values.get(key)

    def set(self, key: str, value: str) -> None:
        self.writes += 1
        if self.broken:
            raise RuntimeError("BLOCKED")
        self.values[key] = value


class ParseTest(unittest.TestCase):
    def test_a_report_is_cut_to_size_and_loses_query_strings(self) -> None:
        report = client_errors.parse(_body(message="x" * 1000))
        self.assertEqual(len(report.message), client_errors.MAX_MESSAGE)
        self.assertNotIn("?v=3", report.stack)
        self.assertNotIn("#x", report.stack)
        self.assertIn("https://motonor.bluemouse.cc/assets/index-1a2b.js", report.stack)

    def test_odd_tab_and_version_are_dropped(self) -> None:
        report = client_errors.parse(_body(tab="drum?lat=44.4", version="<script>"))
        self.assertEqual(report.tab, "")
        self.assertEqual(report.version, "")

    def test_what_is_not_a_report_is_refused(self) -> None:
        for body in (b"nope", b"[]", _body(kind="other"), _body(message="  ")):
            with self.subTest(body=body[:30]):
                with self.assertRaises(ValueError):
                    client_errors.parse(body)
        with self.assertRaises(ValueError):
            client_errors.parse(b"{" + b" " * client_errors.MAX_BODY_BYTES + b"}")

    def test_numbers_do_not_make_a_new_error(self) -> None:
        a = client_errors.parse(_body(message="Tile 12/2300/1400 failed"))
        b = client_errors.parse(_body(message="Tile 13/4601/2800 failed"))
        self.assertEqual(a.fingerprint, b.fingerprint)


class HandleTest(unittest.TestCase):
    def setUp(self) -> None:
        client_errors._reset_memory_for_tests()
        self.sent: list[tuple[str, str, str]] = []
        env = mock.patch.dict(os.environ, {"ERROR_ALERT_EMAIL": "owner@example.com"})
        env.start()
        self.addCleanup(env.stop)

    def _send(self, to: str, subject: str, text: str) -> None:
        self.sent.append((to, subject, text))

    def _handle(self, state: FakeState, body: bytes | None = None, client: str = "1.2.3.4", now: float = DAY) -> str:
        report = client_errors.parse(body or _body())
        return client_errors.handle(report, client, state.get, state.set, self._send, now=now)

    def test_a_new_error_is_mailed_once_a_day(self) -> None:
        state = FakeState()
        self.assertEqual(self._handle(state), "mailed")
        self.assertEqual(self._handle(state), "logged")
        self.assertEqual(len(self.sent), 1)
        self.assertEqual(self.sent[0][0], "owner@example.com")
        self.assertIn("Cannot read properties", self.sent[0][2])

    def test_another_instance_does_not_mail_it_again(self) -> None:
        state = FakeState()
        self._handle(state)
        client_errors._reset_memory_for_tests()  # a second instance
        self.assertEqual(self._handle(state), "logged (seen today)")
        self.assertEqual(len(self.sent), 1)

    def test_the_next_day_it_is_mailed_again(self) -> None:
        state = FakeState()
        self._handle(state)
        self.assertEqual(self._handle(state, now=DAY + 86_400), "mailed")
        self.assertEqual(len(self.sent), 2)

    def test_daily_email_cap(self) -> None:
        state = FakeState()
        results = [self._handle(state, _body(message=f"error {chr(65 + i)}"), client=f"10.0.0.{i}") for i in range(15)]
        self.assertEqual(results.count("mailed"), client_errors.DAILY_EMAILS)
        self.assertEqual(len(self.sent), client_errors.DAILY_EMAILS)

    def test_without_the_database_memory_decides_with_a_smaller_cap(self) -> None:
        state = FakeState(broken=True)
        results = [self._handle(state, _body(message=f"error {chr(65 + i)}"), client=f"10.0.0.{i}") for i in range(6)]
        self.assertEqual(results.count("mailed"), client_errors.FALLBACK_DAILY_EMAILS)

    def test_one_client_cannot_flood_the_log(self) -> None:
        state = FakeState()
        results = [self._handle(state) for _ in range(client_errors.CLIENT_MAX + 5)]
        self.assertEqual(results.count("limited"), 5)

    def test_the_database_is_read_only_for_errors_new_to_the_instance(self) -> None:
        state = FakeState()
        for _ in range(5):
            self._handle(state)
        self.assertEqual(state.reads, 2)  # the error's key and the day's count, once
        self.assertEqual(state.writes, 2)

    def test_without_a_recipient_nothing_touches_the_database(self) -> None:
        state = FakeState()
        with mock.patch.dict(os.environ, {"ERROR_ALERT_EMAIL": ""}):
            self.assertEqual(self._handle(state), "logged")
        self.assertEqual((state.reads, state.writes, len(self.sent)), (0, 0, 0))

    def test_a_failed_email_is_not_raised(self) -> None:
        def fail(to: str, subject: str, text: str) -> None:
            raise RuntimeError("Brevo down")

        report = client_errors.parse(_body())
        state = FakeState()
        result = client_errors.handle(report, "1.2.3.4", state.get, state.set, fail, now=DAY)
        self.assertEqual(result, "logged (mail failed)")


class EndpointTest(unittest.TestCase):
    def setUp(self) -> None:
        client_errors._reset_memory_for_tests()
        self.client = TestClient(main.app)

    def test_a_report_is_taken(self) -> None:
        with mock.patch.dict(os.environ, {"ERROR_ALERT_EMAIL": ""}):
            response = self.client.post("/client-errors", content=_body(), headers={"Content-Type": "text/plain"})
        self.assertEqual(response.status_code, 204)

    def test_a_bad_report_is_refused(self) -> None:
        response = self.client.post("/client-errors", content=b"not json", headers={"Content-Type": "text/plain"})
        self.assertEqual(response.status_code, 400)

    def test_a_huge_report_is_refused(self) -> None:
        body = b"x" * (client_errors.MAX_BODY_BYTES + 1)
        response = self.client.post("/client-errors", content=body, headers={"Content-Type": "text/plain"})
        self.assertEqual(response.status_code, 413)


if __name__ == "__main__":
    unittest.main()
