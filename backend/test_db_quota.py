"""The daily Turso quota look: when it asks, what it adds up, when it mails."""

import os
import unittest
from datetime import datetime, timezone
from unittest import mock

import httpx

import db_quota

NOV_21 = datetime(2026, 11, 21, 12, tzinfo=timezone.utc).timestamp()
CONFIGURED = {"TURSO_PLATFORM_TOKEN": "platform-token", "QUOTA_ALERT_EMAIL": "ops@example.test"}


def turso_api(rows_read: int, rows_written: int, orgs: list | None = None) -> httpx.MockTransport:
    """A fake Turso Platform API with two databases sharing the usage."""
    orgs = orgs if orgs is not None else [
        {"slug": "team-x", "type": "team"},
        {"slug": "rider", "type": "personal"},
    ]

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.headers["authorization"] == "Bearer platform-token"
        if request.url.path == "/v1/organizations":
            return httpx.Response(200, json=orgs)
        if request.url.path == "/v1/organizations/rider/usage":
            return httpx.Response(200, json={
                "organization": {"uuid": "o", "usage": {"rows_read": 500_000_000}},
                "databases": [
                    {"uuid": "a", "total": {"rows_read": rows_read - 1000, "rows_written": rows_written}},
                    {"uuid": "b", "total": {"rows_read": 1000, "rows_written": 0}},
                ],
            })
        return httpx.Response(404, text="not found")

    return httpx.MockTransport(handler)


class QuotaCheckTest(unittest.TestCase):
    def setUp(self) -> None:
        self.state: dict[str, str] = {}
        self.mails: list[tuple[str, str, str]] = []
        env = mock.patch.dict(os.environ, CONFIGURED)
        env.start()
        self.addCleanup(env.stop)
        for name in ("TURSO_ORG", "TURSO_ROWS_READ_LIMIT", "TURSO_ROWS_WRITTEN_LIMIT"):
            os.environ.pop(name, None)

    def run_check(self, transport: httpx.MockTransport, now: float = NOV_21, mail_fails: bool = False) -> dict:
        real_client = httpx.Client

        def client(**kwargs):
            return real_client(transport=transport, **kwargs)

        def send(to: str, subject: str, text: str) -> None:
            if mail_fails:
                raise RuntimeError("smtp down")
            self.mails.append((to, subject, text))

        with mock.patch.object(db_quota.httpx, "Client", side_effect=client):
            return db_quota.check(self.state.get, self.state.__setitem__, send, now=now)

    def test_without_token_or_address_nothing_is_asked(self) -> None:
        with mock.patch.dict(os.environ, {"TURSO_PLATFORM_TOKEN": ""}):
            self.assertEqual(self.run_check(turso_api(0, 0))["reason"], "not configured")
        with mock.patch.dict(os.environ, {"QUOTA_ALERT_EMAIL": ""}):
            self.assertEqual(self.run_check(turso_api(0, 0))["reason"], "not configured")
        self.assertEqual(self.state, {})

    def test_low_usage_is_recorded_without_mail(self) -> None:
        result = self.run_check(turso_api(5_000_000, 100_000))
        self.assertEqual((result["checked"], result["rows_read_pct"], result["warned"]), (True, 1.0, None))
        self.assertEqual(self.mails, [])
        self.assertIn(db_quota.CHECKED_KEY, self.state)

    def test_one_look_a_day(self) -> None:
        self.run_check(turso_api(5_000_000, 0))
        again = self.run_check(turso_api(5_000_000, 0), now=NOV_21 + 3600)
        self.assertEqual(again["reason"], "checked recently")
        later = self.run_check(turso_api(5_000_000, 0), now=NOV_21 + 21 * 3600)
        self.assertTrue(later["checked"])

    def test_reads_past_80_percent_mail_once_a_month(self) -> None:
        first = self.run_check(turso_api(410_000_000, 0))
        self.assertEqual(first["warned"], 80)
        to, subject, text = self.mails[0]
        self.assertEqual(to, "ops@example.test")
        self.assertIn("80%", subject)
        self.assertIn("410.000.000 din 500.000.000 (82%)", text)
        self.assertEqual(self.state[db_quota.WARNED_KEY], "2026-11:80")

        self.run_check(turso_api(420_000_000, 0), now=NOV_21 + 86400)
        self.assertEqual(len(self.mails), 1)

    def test_95_percent_mails_again_and_a_new_month_starts_over(self) -> None:
        self.run_check(turso_api(410_000_000, 0))
        self.run_check(turso_api(480_000_000, 0), now=NOV_21 + 86400)
        self.assertEqual(["80%" in self.mails[0][1], "95%" in self.mails[1][1]], [True, True])

        december = datetime(2026, 12, 20, tzinfo=timezone.utc).timestamp()
        self.run_check(turso_api(410_000_000, 0), now=december)
        self.assertEqual(len(self.mails), 3)
        self.assertEqual(self.state[db_quota.WARNED_KEY], "2026-12:80")

    def test_writes_count_too(self) -> None:
        result = self.run_check(turso_api(1_000_000, 9_600_000))
        self.assertEqual(result["warned"], 95)

    def test_configured_org_and_limits(self) -> None:
        with mock.patch.dict(os.environ, {"TURSO_ORG": "rider", "TURSO_ROWS_READ_LIMIT": "2500000000"}):
            result = self.run_check(turso_api(410_000_000, 0, orgs=[]))
        self.assertEqual((result["rows_read_pct"], result["warned"]), (16.4, None))

    def test_api_trouble_is_reported_not_raised(self) -> None:
        broken = httpx.MockTransport(lambda request: httpx.Response(401, text="bad token"))
        result = self.run_check(broken)
        self.assertEqual(result, {"checked": False, "reason": "turso api error"})
        self.assertNotIn(db_quota.CHECKED_KEY, self.state)

    def test_a_failed_mail_is_tried_again_next_time(self) -> None:
        result = self.run_check(turso_api(410_000_000, 0), mail_fails=True)
        self.assertIsNone(result["warned"])
        self.assertNotIn(db_quota.WARNED_KEY, self.state)


class AlreadyWarnedTest(unittest.TestCase):
    def test_levels_and_months(self) -> None:
        self.assertFalse(db_quota.already_warned(None, "2026-11", 80))
        self.assertTrue(db_quota.already_warned("2026-11:80", "2026-11", 80))
        self.assertFalse(db_quota.already_warned("2026-11:80", "2026-11", 95))
        self.assertTrue(db_quota.already_warned("2026-11:95", "2026-11", 80))
        self.assertFalse(db_quota.already_warned("2026-10:95", "2026-11", 80))
        self.assertFalse(db_quota.already_warned("garbage", "2026-11", 80))


if __name__ == "__main__":
    unittest.main()
