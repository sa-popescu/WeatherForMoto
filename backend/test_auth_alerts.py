"""
Tests for auth_alerts: authentication, security hardening and alert dispatch.

No network and no Turso: a stub `libsql_experimental` module hands out sqlite3
connections to a temporary file, so the real SQL runs on SQLite. Email, web
push and get_weather are replaced by in-memory fakes.

Run from backend/:
    python -m unittest test_auth_alerts -v
"""

import html
import os
import re
import shutil
import sqlite3
import sys
import tempfile
import threading
import types
import unittest
from datetime import datetime, timedelta, timezone
from typing import Any
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Configuration must be in place before auth_alerts is imported.
os.environ["TURSO_DATABASE_URL"] = "libsql://tests.invalid"
os.environ["TURSO_AUTH_TOKEN"] = "test-token"
os.environ["PBKDF2_ITERATIONS"] = "1000"
os.environ["APP_ENV"] = "test"
os.environ["ALLOW_INSECURE_AUTH_CODE"] = "false"
os.environ["BREVO_API_KEY"] = ""
os.environ["SMTP_HOST"] = ""
os.environ["AUTH_CODE_PEPPER"] = ""

_DB: dict[str, str] = {"path": ""}


def _stub_connect(url: str, auth_token: str | None = None) -> sqlite3.Connection:
    return sqlite3.connect(_DB["path"], timeout=10, check_same_thread=False)


_libsql_stub = types.ModuleType("libsql_experimental")
_libsql_stub.connect = _stub_connect  # type: ignore[attr-defined]
sys.modules["libsql_experimental"] = _libsql_stub

import auth_alerts as aa  # noqa: E402
from fastapi import FastAPI, HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402
from starlette.requests import Request  # noqa: E402

PASSWORD = "correct-horse-1"
PUSH_SUB = {
    "endpoint": "https://fcm.googleapis.com/fcm/send/device-1",
    "keys": {"p256dh": "B" * 87, "auth": "A" * 22},
}


def _ip(address: str) -> dict[str, str]:
    """Headers making the request come from ``address`` (right-most XFF entry)."""
    return {"X-Forwarded-For": f"203.0.113.250, {address}"}


def _auth(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


def _make_request(headers: list[tuple[str, str]], client: tuple[str, int] = ("10.0.0.9", 5555)) -> Request:
    """Starlette request for calling handlers directly (repeated headers allowed)."""
    raw = [(k.lower().encode(), v.encode()) for k, v in headers]
    return Request({"type": "http", "headers": raw, "client": client})


def _benign_hour(ts: datetime) -> dict[str, Any]:
    return {
        "time": ts.strftime("%Y-%m-%dT%H:%M"),
        "temperature": 20.0,
        "feels_like": 20.0,
        "precipitation_mm": 0.0,
        "precipitation_probability": 0,
        "rain_intensity": "none",
        "wind_gusts_kmh": 10.0,
        "weather_code": 0,
        "moto_score": 90,
        "moto_label": "IDEAL",
        "frost_risk": False,
        "is_day": True,
    }


def _hourly_from(start: datetime, count: int = 48) -> list[dict[str, Any]]:
    return [_benign_hour(start + timedelta(hours=i)) for i in range(count)]


class _Base(unittest.TestCase):
    """Fresh database, fakes for email/push/weather, TestClient per test."""

    def setUp(self) -> None:
        self.tmp = tempfile.mkdtemp()
        _DB["path"] = os.path.join(self.tmp, "test.db")
        aa._SCHEMA_CACHE.clear()
        aa.init_db()

        self.emails: list[dict[str, Any]] = []
        self.pushes: list[dict[str, Any]] = []
        self.email_error: Exception | None = None
        self.push_error: Exception | None = None
        self.weather: dict[str, Any] = self.make_weather({})

        for patcher in (
            mock.patch.object(aa, "_send_email", side_effect=self._fake_send_email),
            mock.patch.object(aa, "_send_push", side_effect=self._fake_send_push),
            mock.patch.object(aa, "get_weather", new=self._fake_get_weather),
        ):
            patcher.start()
            self.addCleanup(patcher.stop)

        app = FastAPI()
        app.include_router(aa.router)
        self.client = TestClient(app)

    def tearDown(self) -> None:
        shutil.rmtree(self.tmp, ignore_errors=True)

    # -- fakes ---------------------------------------------------------------

    def _fake_send_email(self, email: str, subject: str, text: str, links: Any = (),
                         unsubscribe_url: str | None = None) -> None:
        if self.email_error:
            raise self.email_error
        self.emails.append({"to": email, "subject": subject, "text": text,
                            "links": list(links), "unsubscribe_url": unsubscribe_url})

    def _fake_send_push(self, sub: Any, title: str, body: str, data: dict[str, Any]) -> None:
        if self.push_error:
            raise self.push_error
        self.pushes.append({"endpoint": sub["endpoint"], "title": title, "body": body})

    async def _fake_get_weather(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        return self.weather

    # -- helpers -------------------------------------------------------------

    def db(self) -> sqlite3.Connection:
        conn = sqlite3.connect(_DB["path"], timeout=10)
        conn.row_factory = sqlite3.Row
        self.addCleanup(conn.close)
        return conn

    def make_weather(self, overrides: dict[int, dict[str, Any]], offset_seconds: int = 0) -> dict[str, Any]:
        """48 hourly items from local midnight; ``overrides`` is keyed by hours
        relative to the current local hour."""
        local_now = (datetime.now(timezone.utc) + timedelta(seconds=offset_seconds)).replace(tzinfo=None)
        midnight = local_now.replace(hour=0, minute=0, second=0, microsecond=0)
        hourly = _hourly_from(midnight)
        for rel, values in overrides.items():
            hourly[local_now.hour + rel].update(values)
        return {"timezone": "Europe/Bucharest", "utc_offset_seconds": offset_seconds, "hourly": hourly}

    def signup(self, email: str, password: str = PASSWORD, ip: str = "198.51.100.1") -> dict[str, Any]:
        resp = self.client.post("/auth/signup", json={"email": email, "password": password}, headers=_ip(ip))
        self.assertEqual(resp.status_code, 200, resp.text)
        return resp.json()

    def login(self, email: str, password: str = PASSWORD, ip: str = "198.51.100.1") -> Any:
        return self.client.post("/auth/login", json={"email": email, "password": password}, headers=_ip(ip))

    def user_id(self, email: str) -> int:
        return int(self.db().execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()["id"])

    def verification_link(self, email: str) -> str:
        mails = [m for m in self.emails if m["to"] == email and "confirmă" in m["subject"]]
        self.assertTrue(mails, f"no verification email for {email}")
        return mails[-1]["links"][0]

    def verify(self, email: str, password: str = PASSWORD) -> str:
        """Confirm via the link's button (POST) and log in again, because
        confirming signs out every existing session. Returns a fresh token."""
        resp = self.client.post(self.verification_link(email).replace(aa.API_BASE_URL, ""))
        self.assertEqual(resp.status_code, 200, resp.text)
        login = self.login(email, password)
        self.assertEqual(login.status_code, 200, login.text)
        return login.json()["token"]

    def put_prefs(self, token: str, **overrides: Any) -> Any:
        payload: dict[str, Any] = {"enabled": True, "email_alerts_enabled": True, "home_lat": 44.43,
                                   "home_lon": 26.10, "city": "Bucuresti", "severity": "medium"}
        payload.update(overrides)
        return self.client.put("/me/prefs", json=payload, headers=_auth(token))

    def alert_ready_user(self, email: str = "rider@example.com", verified: bool = True) -> tuple[str, int]:
        token = self.signup(email)["token"]
        if verified:
            token = self.verify(email)
        self.assertEqual(self.put_prefs(token).status_code, 200)
        resp = self.client.post("/me/push-subscriptions", json=PUSH_SUB, headers=_auth(token))
        self.assertEqual(resp.status_code, 200, resp.text)
        return token, self.user_id(email)

    def alert_emails(self) -> list[dict[str, Any]]:
        return [m for m in self.emails if m["subject"].startswith("WeatherForMoto alertă")]


# ---------------------------------------------------------------------------
# 1. Client IP
# ---------------------------------------------------------------------------

class ClientIpTests(unittest.TestCase):
    def _request(self, headers: dict[str, str]) -> Request:
        raw = [(k.lower().encode(), v.encode()) for k, v in headers.items()]
        return Request({"type": "http", "headers": raw, "client": ("10.0.0.9", 5555)})

    def test_rightmost_forwarded_entry_by_default(self) -> None:
        req = self._request({"X-Forwarded-For": "6.6.6.6, 9.9.9.9"})
        self.assertEqual(aa._client_ip(req), "9.9.9.9")

    def test_trusted_hops_select_entry_from_the_right(self) -> None:
        req = self._request({"X-Forwarded-For": "1.1.1.1, 6.6.6.6, 9.9.9.9"})
        with mock.patch.object(aa, "TRUSTED_PROXY_HOPS", 2):
            self.assertEqual(aa._client_ip(req), "6.6.6.6")
        with mock.patch.object(aa, "TRUSTED_PROXY_HOPS", 5):
            self.assertEqual(aa._client_ip(req), "1.1.1.1")

    def test_cf_connecting_ip_only_on_opt_in(self) -> None:
        req = self._request({"CF-Connecting-IP": "4.4.4.4", "X-Forwarded-For": "9.9.9.9"})
        self.assertEqual(aa._client_ip(req), "9.9.9.9")
        with mock.patch.object(aa, "TRUST_CF_CONNECTING_IP", True):
            self.assertEqual(aa._client_ip(req), "4.4.4.4")

    def test_falls_back_to_peer_address(self) -> None:
        self.assertEqual(aa._client_ip(self._request({})), "10.0.0.9")

    def test_forwarded_for_split_over_several_header_lines(self) -> None:
        # A client-supplied line first, the proxy's appended line last.
        req = _make_request([("X-Forwarded-For", "6.6.6.6"), ("X-Forwarded-For", "1.2.3.4, 9.9.9.9")])
        self.assertEqual(aa._client_ip(req), "9.9.9.9")
        with mock.patch.object(aa, "TRUSTED_PROXY_HOPS", 3):
            self.assertEqual(aa._client_ip(req), "6.6.6.6")

    def test_ipv6_is_bucketed_by_64_and_ipv4_is_unchanged(self) -> None:
        a = aa._ip_bucket_key("2001:db8:1:2:aaaa::1")
        self.assertEqual(a, aa._ip_bucket_key("2001:db8:1:2:bbbb:cccc:dddd:2"))
        self.assertEqual(a, "2001:db8:1:2::/64")
        self.assertNotEqual(a, aa._ip_bucket_key("2001:db8:1:3::1"))
        self.assertEqual(aa._ip_bucket_key("203.0.113.7"), "203.0.113.7")
        self.assertEqual(aa._ip_bucket_key("::ffff:203.0.113.7"), "203.0.113.7")
        self.assertEqual(aa._mask_ip("203.0.113.7"), "203.0.113.x")
        self.assertEqual(aa._mask_ip("2001:db8:1:2:aaaa::1"), "2001:db8:1:2::/64")


# ---------------------------------------------------------------------------
# 2. Email ownership
# ---------------------------------------------------------------------------

class EmailVerificationTests(_Base):
    def test_signup_returns_session_and_starts_unverified(self) -> None:
        data = self.signup("new@example.com")
        self.assertTrue(data["token"])
        self.assertFalse(data["user"]["email_verified"])
        me = self.client.get("/me", headers=_auth(data["token"])).json()
        self.assertFalse(me["email_verified"])
        link = self.verification_link("new@example.com")
        self.assertTrue(link.startswith(f"{aa.API_BASE_URL}/auth/verify-email?token="))

    def test_unverified_address_gets_push_but_no_alert_email(self) -> None:
        token, _ = self.alert_ready_user("unverified@example.com", verified=False)
        self.weather = self.make_weather({1: {"wind_gusts_kmh": 80}})
        result = self.client.post("/alerts/check-now", headers=_auth(token)).json()
        self.assertEqual(result["sent"], 1)
        self.assertEqual(result["email_sent"], 0)
        self.assertEqual(self.alert_emails(), [])
        self.assertEqual(len(self.pushes), 1)

    def test_get_only_confirms_post_verifies_and_signs_everyone_out(self) -> None:
        token = self.signup("owner@example.com")["token"]
        self.client.post("/me/push-subscriptions", json=PUSH_SUB, headers=_auth(token))
        path = self.verification_link("owner@example.com").replace(aa.API_BASE_URL, "")

        page = self.client.get(path)   # what a mail security scanner does
        self.assertEqual(page.status_code, 200)
        self.assertIn(f'<form method="post" action="{html.escape(path, quote=True)}">', page.text)
        self.assertIn("form-action 'self'", page.headers["content-security-policy"])
        self.assertEqual(page.headers.get("referrer-policy"), "no-referrer")
        self.assertFalse(self.client.get("/me", headers=_auth(token)).json()["email_verified"])

        done = self.client.post(path)
        self.assertEqual(done.status_code, 200)
        self.assertIn("Adresă confirmată", done.text)
        self.assertIn(aa.APP_BASE_URL, done.text)
        # A password existed before verification: all sessions and push devices are revoked.
        self.assertEqual(self.client.get("/me", headers=_auth(token)).status_code, 401)
        self.assertEqual(self.client.post(path).status_code, 400)   # single use
        fresh = self.login("owner@example.com").json()["token"]
        me = self.client.get("/me", headers=_auth(fresh)).json()
        self.assertTrue(me["email_verified"])
        self.assertEqual(me["pushSubscriptions"], 0)

    def test_verified_address_receives_alert_email_with_unsubscribe(self) -> None:
        token, _ = self.alert_ready_user("mail@example.com")
        self.weather = self.make_weather({1: {"wind_gusts_kmh": 80}})
        result = self.client.post("/alerts/check-now", headers=_auth(token)).json()
        self.assertEqual(result["email_sent"], 1)
        mail = self.alert_emails()[0]
        self.assertTrue(mail["unsubscribe_url"].startswith(f"{aa.API_BASE_URL}/alerts/unsubscribe?token="))
        self.assertIn(mail["unsubscribe_url"], mail["text"])

    def test_expired_link_is_rejected(self) -> None:
        self.signup("late@example.com")
        self.db().execute("UPDATE email_verification_tokens SET expires_at = '2000-01-01T00:00:00+00:00'").connection.commit()
        resp = self.client.post(self.verification_link("late@example.com").replace(aa.API_BASE_URL, ""))
        self.assertEqual(resp.status_code, 400)
        self.assertIn("expirat", resp.text)

    def test_link_for_the_old_address_is_useless_after_email_change(self) -> None:
        token = self.signup("first@example.com")["token"]
        old_path = self.verification_link("first@example.com").replace(aa.API_BASE_URL, "")
        resp = self.client.put("/me/email", json={"new_email": "second@example.com", "password": PASSWORD},
                               headers=_auth(token))
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertEqual(self.client.post(old_path).status_code, 400)
        # Even a token row that survived, pinned to the old address, is refused.
        raw = "old-address-token-" + "a" * 20
        conn = self.db()
        conn.execute(
            "INSERT INTO email_verification_tokens(user_id, email, token_hash, expires_at, created_at) "
            "VALUES (?, 'first@example.com', ?, '2999-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')",
            (self.user_id("second@example.com"), aa._hash_token(raw)),
        )
        conn.commit()
        self.assertEqual(self.client.post(f"/auth/verify-email?token={raw}").status_code, 400)
        self.assertFalse(self.client.get("/me", headers=_auth(token)).json()["email_verified"])

    def test_email_change_marks_unverified_and_revokes_other_sessions(self) -> None:
        self.signup("before@example.com")
        token_a = self.verify("before@example.com")
        token_b = self.login("before@example.com").json()["token"]
        resp = self.client.put("/me/email", json={"new_email": "after@example.com", "password": PASSWORD},
                               headers=_auth(token_a))
        self.assertEqual(resp.status_code, 200, resp.text)
        self.assertFalse(resp.json()["email_verified"])
        self.assertFalse(self.client.get("/me", headers=_auth(token_a)).json()["email_verified"])
        self.assertEqual(self.client.get("/me", headers=_auth(token_b)).status_code, 401)
        self.verification_link("after@example.com")  # sent to the NEW address

    def test_email_change_wrong_password_is_rate_limited(self) -> None:
        token = self.signup("pw@example.com")["token"]
        body = {"new_email": "x@example.com", "password": "wrong-password"}
        codes = [self.client.put("/me/email", json=body, headers=_auth(token)).status_code
                 for _ in range(aa.PASSWORD_CHECK_RATE_MAX + 1)]
        self.assertEqual(codes[:-1], [400] * aa.PASSWORD_CHECK_RATE_MAX)
        self.assertEqual(codes[-1], 429)

    def _code_for(self, email: str) -> str:
        mail = [m for m in self.emails if m["to"] == email and "cod autentificare" in m["subject"]][-1]
        return re.search(r"\b(\d{6})\b", mail["text"]).group(1)

    def test_code_login_evicts_squatter_of_unverified_account(self) -> None:
        squatter_token = self.signup("victim@example.com", password="squatter-pass")["token"]
        self.client.post("/me/push-subscriptions", json=PUSH_SUB, headers=_auth(squatter_token))

        self.client.post("/auth/request-code", json={"email": "victim@example.com"}, headers=_ip("7.7.7.7"))
        resp = self.client.post("/auth/verify-code", json={"email": "victim@example.com",
                                                           "code": self._code_for("victim@example.com")},
                                headers=_ip("7.7.7.7"))
        self.assertEqual(resp.status_code, 200, resp.text)
        owner_token = resp.json()["token"]

        self.assertEqual(self.client.get("/me", headers=_auth(squatter_token)).status_code, 401)
        self.assertEqual(self.login("victim@example.com", "squatter-pass").status_code, 401)
        me = self.client.get("/me", headers=_auth(owner_token)).json()
        self.assertTrue(me["email_verified"])
        self.assertEqual(me["pushSubscriptions"], 0)
        row = self.db().execute("SELECT password_hash FROM users WHERE email = 'victim@example.com'").fetchone()
        self.assertIsNone(row["password_hash"])

    def test_code_login_keeps_password_of_verified_account(self) -> None:
        self.signup("kept@example.com")
        self.verify("kept@example.com")
        self.client.post("/auth/request-code", json={"email": "kept@example.com"})
        resp = self.client.post("/auth/verify-code", json={"email": "kept@example.com",
                                                           "code": self._code_for("kept@example.com")})
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(self.login("kept@example.com").status_code, 200)

    def test_password_reset_proves_ownership(self) -> None:
        self.signup("reset@example.com")
        self.client.post("/auth/request-reset", json={"email": "reset@example.com"})
        mail = [m for m in self.emails if "resetare" in m["subject"]][-1]
        raw_token = mail["links"][0].split("reset_token=", 1)[1]
        resp = self.client.post("/auth/reset-password", json={"token": raw_token, "new_password": "brand-new-pass"})
        self.assertEqual(resp.status_code, 200)
        token = self.login("reset@example.com", "brand-new-pass").json()["token"]
        self.assertTrue(self.client.get("/me", headers=_auth(token)).json()["email_verified"])


# ---------------------------------------------------------------------------
# 6. Rate limiter
# ---------------------------------------------------------------------------

class RateLimitTests(_Base):
    def test_successful_logins_never_lock_out(self) -> None:
        self.signup("busy@example.com")
        for _ in range(aa.LOGIN_RATE_MAX + 4):
            self.assertEqual(self.login("busy@example.com").status_code, 200)

    def test_attacker_cannot_lock_out_the_owner(self) -> None:
        self.signup("target@example.com")
        codes = [self.login("target@example.com", "wrong-password", ip="6.6.6.6").status_code
                 for _ in range(aa.LOGIN_RATE_MAX + 1)]
        self.assertEqual(codes[:-1], [401] * aa.LOGIN_RATE_MAX)
        self.assertEqual(codes[-1], 429)
        self.assertEqual(self.login("target@example.com", ip="7.7.7.7").status_code, 200)

    def test_increments_are_atomic_under_concurrency(self) -> None:
        results: list[bool] = []
        lock = threading.Lock()

        def worker() -> None:
            conn = aa._connect()
            try:
                for _ in range(5):
                    over = aa._rate_hit(conn, "test:atomic", 20, 3600)
                    with lock:
                        results.append(over)
            finally:
                conn.close()

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        count = self.db().execute("SELECT count FROM rate_limits WHERE bucket = 'test:atomic'").fetchone()["count"]
        self.assertEqual(count, 40)
        self.assertEqual(results.count(False), 20)

    def test_expired_window_restarts_the_count(self) -> None:
        conn = aa._connect()
        self.addCleanup(conn.close)
        self.assertFalse(aa._rate_hit(conn, "test:window", 1, 60))
        self.assertTrue(aa._rate_hit(conn, "test:window", 1, 60))
        old = (datetime.now(timezone.utc) - timedelta(minutes=5)).isoformat()
        conn.execute("UPDATE rate_limits SET window_start = ? WHERE bucket = 'test:window'", (old,))
        conn.commit()
        self.assertFalse(aa._rate_hit(conn, "test:window", 1, 60))

    def test_storage_error_fails_closed(self) -> None:
        class Broken:
            def execute(self, *args: Any) -> Any:
                raise RuntimeError("database unavailable")

            def commit(self) -> None:
                raise RuntimeError("database unavailable")

        with mock.patch.object(aa, "_RATE_TABLE_READY", True):
            with self.assertRaises(HTTPException) as ctx:
                aa._rate_hit(Broken(), "test:broken", 5, 60)  # type: ignore[arg-type]
        self.assertEqual(ctx.exception.status_code, 503)

    def _request_code(self, email: str) -> str:
        self.client.post("/auth/request-code", json={"email": email})
        mail = [m for m in self.emails if m["to"] == email][-1]
        return re.search(r"\b(\d{6})\b", mail["text"]).group(1)

    def _wrong(self, code: str) -> str:
        return "000000" if code != "000000" else "111111"

    def test_wrong_guesses_do_not_kill_the_owners_code(self) -> None:
        code = self._request_code("coder@example.com")
        body = {"email": "coder@example.com", "code": self._wrong(code)}
        codes = [self.client.post("/auth/verify-code", json=body, headers=_ip("6.6.6.6")).status_code
                 for _ in range(aa.CODE_MAX_ATTEMPTS + 1)]
        self.assertEqual(codes[:-1], [400] * aa.CODE_MAX_ATTEMPTS)
        self.assertEqual(codes[-1], 429)
        ok = self.client.post("/auth/verify-code", json={"email": "coder@example.com", "code": code},
                              headers=_ip("7.7.7.7"))
        self.assertEqual(ok.status_code, 200, ok.text)

    def test_code_invalidated_after_total_failures(self) -> None:
        code = self._request_code("brute@example.com")
        body = {"email": "brute@example.com", "code": self._wrong(code)}
        statuses = []
        for i in range(aa.CODE_MAX_FAILURES):
            ip = f"6.6.{i // aa.CODE_MAX_ATTEMPTS}.1"
            statuses.append(self.client.post("/auth/verify-code", json=body, headers=_ip(ip)).status_code)
        self.assertEqual(statuses[-1], 429)
        late = self.client.post("/auth/verify-code", json={"email": "brute@example.com", "code": code},
                                headers=_ip("7.7.7.7"))
        self.assertEqual(late.status_code, 400)

    def _verify_direct(self, email: str, code: str, ip: str) -> dict[str, Any]:
        """Call the handler directly so tests can interleave requests."""
        request = _make_request(list(_ip(ip).items()))
        return aa.auth_verify_code(aa.VerifyCodePayload(email=email, code=code), request)

    def test_code_is_consumed_once_when_requests_interleave(self) -> None:
        email = "race@example.com"
        code = self._request_code(email)
        real_consume = aa._consume_code
        state: dict[str, Any] = {"nested": False}

        def interleaved(conn: Any, code_id: int) -> bool:
            if not state["nested"]:
                state["nested"] = True
                # A second request with the same correct code runs to completion
                # while the first sits between "code matched" and "code consumed".
                state["inner"] = self._verify_direct(email, code, "7.7.7.8")
            return real_consume(conn, code_id)

        with mock.patch.object(aa, "_consume_code", side_effect=interleaved):
            with self.assertRaises(HTTPException) as ctx:
                self._verify_direct(email, code, "7.7.7.7")
        self.assertEqual(ctx.exception.status_code, 400)
        self.assertTrue(state["inner"]["token"])
        sessions = self.db().execute("SELECT COUNT(*) AS c FROM sessions WHERE user_id = ?",
                                     (self.user_id(email),)).fetchone()["c"]
        self.assertEqual(sessions, 1)

    def test_interleaved_wrong_guesses_cannot_exceed_the_code_budget(self) -> None:
        email = "budget@example.com"
        wrong = self._wrong(self._request_code(email))
        real_matches = aa._code_matches
        compared: list[str] = []

        def interleaved(stored: str, supplied: str) -> bool:
            compared.append(supplied)
            if len(compared) == 1:
                # Many more guesses arrive while the first one is being compared.
                for i in range(aa.CODE_MAX_FAILURES + 5):
                    try:
                        self._verify_direct(email, wrong, f"6.6.{i}.1")
                    except HTTPException:
                        pass
            return real_matches(stored, supplied)

        with mock.patch.object(aa, "_code_matches", side_effect=interleaved):
            with self.assertRaises(HTTPException):
                self._verify_direct(email, wrong, "6.6.99.1")
        self.assertLessEqual(len(compared), aa.CODE_MAX_FAILURES)

    def test_daily_code_cap_blocks_code_login_but_not_password_login(self) -> None:
        self.signup("capped@example.com")
        code = self._request_code("capped@example.com")
        with mock.patch.object(aa, "CODE_FAILURES_PER_EMAIL_DAY", 3):
            for i in range(3):
                resp = self.client.post("/auth/verify-code", headers=_ip(f"6.6.{i}.1"),
                                        json={"email": "capped@example.com", "code": self._wrong(code)})
                self.assertEqual(resp.status_code, 400)
            blocked = self.client.post("/auth/verify-code", headers=_ip("7.7.7.7"),
                                       json={"email": "capped@example.com", "code": code})
            self.assertEqual(blocked.status_code, 429)
        self.assertEqual(self.login("capped@example.com", ip="7.7.7.7").status_code, 200)

    def test_ipv6_rotation_within_one_64_shares_a_bucket(self) -> None:
        self.signup("v6@example.com")
        codes = [self.login("v6@example.com", "wrong-password", ip=f"2001:db8:1:2::{i + 1:x}").status_code
                 for i in range(aa.LOGIN_RATE_MAX + 1)]
        self.assertEqual(codes[-1], 429)
        self.assertEqual(self.login("v6@example.com", ip="2001:db8:9:9::1").status_code, 200)

    def test_429_logs_the_masked_client_ip(self) -> None:
        self.signup("logged@example.com")
        with self.assertLogs("weatherformoto.auth_alerts", level="WARNING") as logs:
            for _ in range(aa.LOGIN_RATE_MAX + 1):
                self.login("logged@example.com", "wrong-password", ip="6.6.6.6")
        joined = "\n".join(logs.output)
        self.assertIn("client=6.6.6.x xff_entries=2", joined)
        self.assertNotIn("6.6.6.6", joined)

    def test_password_login_in_flight_gets_no_session_after_eviction(self) -> None:
        self.signup("inflight@example.com")
        uid = self.user_id("inflight@example.com")
        real_prefs = aa._upsert_default_prefs

        def evict_meanwhile(conn: Any, user_id: int) -> None:
            real_prefs(conn, user_id)
            # A code login by the address owner lands between this login's
            # password check and its session insert.
            other = aa._connect()
            try:
                aa._evict_other_holders(other, user_id)
            finally:
                other.close()

        with mock.patch.object(aa, "_upsert_default_prefs", side_effect=evict_meanwhile):
            self.assertEqual(self.login("inflight@example.com").status_code, 401)
        count = self.db().execute("SELECT COUNT(*) AS c FROM sessions WHERE user_id = ?", (uid,)).fetchone()["c"]
        self.assertEqual(count, 0)


# ---------------------------------------------------------------------------
# 5. Hazards
# ---------------------------------------------------------------------------

class HazardTests(_Base):
    def _hazard(self, **overrides: Any) -> dict[str, Any]:
        body = {"lat": 44.43, "lon": 26.10, "hazard_type": "gravel", "severity": 3,
                "description": "Pietris pe carosabil", "ttl_hours": 6}
        body.update(overrides)
        return body

    def test_bounding_box_prefilter_keeps_nearby_reports(self) -> None:
        conn = self.db()
        now = datetime.now(timezone.utc)
        rows = [(44.43, 26.10, "ice", "aproape")]
        rows += [(46.77, 23.60, "gravel", f"departe {i}") for i in range(320)]
        conn.executemany(
            "INSERT INTO hazard_reports(user_id, lat, lon, hazard_type, severity, description, expires_at, created_at) "
            "VALUES (NULL, ?, ?, ?, 3, ?, ?, ?)",
            [(lat, lon, kind, desc, (now + timedelta(hours=5)).isoformat(), now.isoformat())
             for lat, lon, kind, desc in rows],
        )
        conn.commit()
        hazards = self.client.get("/hazards?lat=44.40&lon=26.05&radius_km=50").json()["hazards"]
        self.assertEqual([h["description"] for h in hazards], ["aproape"])

    def test_bounding_box_handles_antimeridian(self) -> None:
        sql, params = aa._bounding_box_filter(0.0, 179.9, 50)
        self.assertIn("OR", sql)
        self.assertLess(params[3], -179)

    def test_per_user_rate_limit(self) -> None:
        token = self.signup("reporter@example.com")["token"]
        statuses = [self.client.post("/hazards", json=self._hazard(), headers=_auth(token)).status_code
                    for _ in range(aa.HAZARD_RATE_MAX + 1)]
        self.assertEqual(statuses[:-1], [200] * aa.HAZARD_RATE_MAX)
        self.assertEqual(statuses[-1], 429)

    def test_type_allowlist_and_description_length(self) -> None:
        token = self.signup("types@example.com")["token"]
        for raw, expected in (("Pietriș", "gravel"), ("POLEI", "ice"), ("lucrări", "roadworks"), ("ufo", "other")):
            self.client.post("/hazards", json=self._hazard(hazard_type=raw), headers=_auth(token))
            stored = self.db().execute("SELECT hazard_type FROM hazard_reports ORDER BY id DESC LIMIT 1").fetchone()
            self.assertEqual(stored["hazard_type"], expected)
        too_long = self.client.post("/hazards", json=self._hazard(description="x" * 221), headers=_auth(token))
        self.assertEqual(too_long.status_code, 422)


# ---------------------------------------------------------------------------
# 7. Account deletion, 8. unsubscribe, 9. passwords
# ---------------------------------------------------------------------------

class AccountLifecycleTests(_Base):
    def test_delete_account_removes_everything(self) -> None:
        email = "gone@example.com"
        token = self.signup(email)["token"]
        uid = self.user_id(email)
        self.put_prefs(token)
        self.client.post("/me/push-subscriptions", json=PUSH_SUB, headers=_auth(token))
        self.client.post("/hazards", json={"lat": 44.4, "lon": 26.1, "hazard_type": "ice", "severity": 2,
                                           "description": "polei"}, headers=_auth(token))
        self.client.post("/me/routes", json={"name": "Tura", "stops": ["A", "B"]}, headers=_auth(token))
        self.client.post("/auth/request-reset", json={"email": email})
        self.client.post("/auth/request-code", json={"email": email})
        self.login(email, "wrong-password")

        self.assertEqual(self.client.delete("/me", headers=_auth(token)).status_code, 200)

        conn = self.db()
        for table in ("sessions", "push_subscriptions", "alert_events", "saved_routes", "ride_logs",
                      "hazard_reports", "alert_prefs", "password_reset_tokens", "email_verification_tokens"):
            count = conn.execute(f"SELECT COUNT(*) AS c FROM {table} WHERE user_id = ?", (uid,)).fetchone()["c"]
            self.assertEqual(count, 0, table)
        self.assertEqual(conn.execute("SELECT COUNT(*) AS c FROM users WHERE id = ?", (uid,)).fetchone()["c"], 0)
        self.assertEqual(conn.execute("SELECT COUNT(*) AS c FROM auth_codes WHERE email = ?", (email,)).fetchone()["c"], 0)
        key = aa._email_key(email)
        leftover = conn.execute("SELECT bucket FROM rate_limits WHERE bucket LIKE ? OR bucket LIKE ?",
                                (f"%{key}%", f"%:user:{uid}")).fetchall()
        self.assertEqual([r["bucket"] for r in leftover], [])

    def _email_alerts_enabled(self, uid: int) -> int:
        return self.db().execute("SELECT email_alerts_enabled FROM alert_prefs WHERE user_id = ?",
                                 (uid,)).fetchone()["email_alerts_enabled"]

    def _unsubscribe_path(self, uid: int) -> str:
        conn = aa._connect()
        self.addCleanup(conn.close)
        return aa._get_unsubscribe_url(conn, uid).replace(aa.API_BASE_URL, "")

    def test_unsubscribe_get_only_confirms_and_post_unsubscribes(self) -> None:
        token, uid = self.alert_ready_user("unsub@example.com")
        path = self._unsubscribe_path(uid)

        page = self.client.get(path)
        self.assertEqual(page.status_code, 200)
        self.assertIn(f'<form method="post" action="{html.escape(path, quote=True)}">', page.text)
        self.assertIn("form-action 'self'", page.headers["content-security-policy"])
        self.assertEqual(self._email_alerts_enabled(uid), 1)   # a link scanner's GET changes nothing

        confirmed = self.client.post(path, headers={"Content-Type": "application/x-www-form-urlencoded"})
        self.assertEqual(confirmed.status_code, 200)
        self.assertIn(aa._UNSUBSCRIBE_DONE_TITLE, confirmed.text)
        self.assertEqual(self._email_alerts_enabled(uid), 0)

        self.put_prefs(token, email_alerts_enabled=True)
        self.assertEqual(self._email_alerts_enabled(uid), 1)
        one_click = self.client.post(path, content="List-Unsubscribe=One-Click",
                                     headers={"Content-Type": "application/x-www-form-urlencoded"})
        self.assertEqual(one_click.status_code, 200)
        self.assertEqual(self._email_alerts_enabled(uid), 0)

    def test_unknown_unsubscribe_token_is_indistinguishable(self) -> None:
        _, uid = self.alert_ready_user("probe@example.com")
        real_path = self._unsubscribe_path(uid)
        real_token = real_path.split("token=", 1)[1]
        fake_token = "x" * len(real_token)
        fake_path = f"/alerts/unsubscribe?token={fake_token}"

        real_get, fake_get = self.client.get(real_path), self.client.get(fake_path)
        self.assertEqual(real_get.status_code, fake_get.status_code)
        self.assertEqual(real_get.text.replace(real_token, "T"), fake_get.text.replace(fake_token, "T"))

        real_post, fake_post = self.client.post(real_path), self.client.post(fake_path)
        self.assertEqual((real_post.status_code, real_post.text), (fake_post.status_code, fake_post.text))

        malformed = self.client.get("/alerts/unsubscribe?token=bad")
        self.assertEqual(malformed.status_code, 400)
        self.assertNotIn("<form", malformed.text)
        bad_post = self.client.post("/alerts/unsubscribe?token=bad")
        self.assertEqual((bad_post.status_code, bad_post.text), (real_post.status_code, real_post.text))

    def test_alert_email_keeps_unsubscribe_headers(self) -> None:
        headers = aa._unsubscribe_headers("https://api.example/alerts/unsubscribe?token=abc")
        self.assertEqual(headers["List-Unsubscribe"], "<https://api.example/alerts/unsubscribe?token=abc>")
        self.assertEqual(headers["List-Unsubscribe-Post"], "List-Unsubscribe=One-Click")

    def test_pbkdf2_rehash_on_login(self) -> None:
        self.signup("legacy@example.com")
        conn = self.db()
        conn.execute("UPDATE users SET password_hash = ? WHERE email = 'legacy@example.com'",
                     (aa._hash_password(PASSWORD, iterations=500),))
        conn.commit()
        self.assertEqual(self.login("legacy@example.com").status_code, 200)
        stored = self.db().execute("SELECT password_hash FROM users WHERE email = 'legacy@example.com'").fetchone()
        self.assertEqual(aa._parse_password_hash(stored["password_hash"])[0], aa.PBKDF2_ITERATIONS)
        self.assertEqual(self.login("legacy@example.com").status_code, 200)

    def test_auth_code_pepper_changes_the_hash(self) -> None:
        plain = aa._hash_code("123456")
        with mock.patch.object(aa, "AUTH_CODE_PEPPER", "pepper"):
            self.assertNotEqual(aa._hash_code("123456"), plain)
        self.assertEqual(plain, aa._hash_token("123456"))


# ---------------------------------------------------------------------------
# 11. Risk engine (pure functions)
# ---------------------------------------------------------------------------

class RiskEngineTests(unittest.TestCase):
    prefs: dict[str, Any] = {"max_wind_gust": 50, "min_score": 45, "min_temp": None, "max_temp": None,
                             "frost_risk_enabled": 1, "severity": "medium"}

    def _day(self) -> list[dict[str, Any]]:
        return _hourly_from(datetime(2026, 9, 11, 0, 0))

    def _at(self, hourly: list[dict[str, Any]], stamp: str) -> dict[str, Any]:
        return next(h for h in hourly if h["time"] == stamp)

    def test_window_starts_at_current_hour_and_crosses_midnight(self) -> None:
        hourly = self._day()
        self._at(hourly, "2026-09-11T10:00")["wind_gusts_kmh"] = 80   # already past
        self._at(hourly, "2026-09-12T01:00")["wind_gusts_kmh"] = 80   # tomorrow, inside window
        self._at(hourly, "2026-09-12T23:00")["wind_gusts_kmh"] = 80   # beyond 24 h
        now = datetime(2026, 9, 11, 22, 30)
        events = aa._build_risk_events(hourly, self.prefs, now)
        wind = [e for e in events if e["type"] == "wind"]
        self.assertEqual(wind[0]["when"], "2026-09-12T01:00")
        self.assertIn("(mâine)", wind[0]["body"])

        self._at(hourly, "2026-09-12T01:00")["wind_gusts_kmh"] = 10
        self.assertEqual([e for e in aa._build_risk_events(hourly, self.prefs, now) if e["type"] == "wind"], [])

    def test_local_now_uses_utc_offset(self) -> None:
        utc = datetime(2026, 9, 11, 21, 30, tzinfo=timezone.utc)
        self.assertEqual(aa._local_now({"utc_offset_seconds": 10800}, utc), datetime(2026, 9, 12, 0, 30))

    def test_missing_moto_score_is_skipped(self) -> None:
        hourly = self._day()
        for h in hourly:
            h["moto_score"] = None
            h["moto_label"] = None
        now = datetime(2026, 9, 11, 8, 0)
        self.assertEqual([e for e in aa._build_risk_events(hourly, self.prefs, now) if e["type"] == "score"], [])
        self._at(hourly, "2026-09-11T12:00").update(moto_score=30, moto_label="EVITĂ")
        score = [e for e in aa._build_risk_events(hourly, self.prefs, now) if e["type"] == "score"][0]
        self.assertEqual((score["when"], score["level"]), ("2026-09-11T12:00", "EVITĂ"))

    def test_frost_does_not_require_precipitation(self) -> None:
        hourly = self._day()
        self._at(hourly, "2026-09-11T05:00").update(frost_risk=True, temperature=1.0, precipitation_mm=0.0)
        frost = [e for e in aa._build_risk_events(hourly, self.prefs, datetime(2026, 9, 11, 1, 0))
                 if e["type"] == "frost"]
        self.assertEqual(frost[0]["level"], "EVITĂ")

    def test_rain_matrix(self) -> None:
        cases = {
            (70, "urme"): "low", (80, "moderata"): "high", (20, "puternica"): "medium",
            (45, "slaba"): "low", (10, "moderata"): "low", (30, "urme"): "none",
            (60, "moderata"): "medium", (61, "slaba"): "medium", (5, "none"): "none",
        }
        for (prob, intensity), expected in cases.items():
            self.assertEqual(aa._rain_impact(prob, intensity), expected, (prob, intensity))

    def test_seventy_percent_trace_rain_is_not_an_alert(self) -> None:
        hourly = self._day()
        self._at(hourly, "2026-09-11T17:00").update(precipitation_probability=70, precipitation_mm=0.1,
                                                    rain_intensity="urme")
        events = aa._build_risk_events(hourly, self.prefs, datetime(2026, 9, 11, 12, 0))
        self.assertEqual([e for e in events if e["type"] == "rain"], [])

    def test_eighty_percent_moderate_rain_alert_states_both_numbers(self) -> None:
        hourly = self._day()
        self._at(hourly, "2026-09-11T17:00").update(precipitation_probability=80, precipitation_mm=3.2,
                                                    rain_intensity="moderata")
        rain = [e for e in aa._build_risk_events(hourly, self.prefs, datetime(2026, 9, 11, 12, 0))
                if e["type"] == "rain"][0]
        self.assertEqual(rain["body"], "80% șanse, până la 3,2 mm/h (moderată) de la 17:00")
        self.assertEqual(rain["level"], "EVITĂ")

    def test_severity_mapping(self) -> None:
        events = [{"type": "a", "level": "INFO"}, {"type": "b", "level": "ATENȚIE"}, {"type": "c", "level": "EVITĂ"}]
        self.assertEqual([e["type"] for e in aa._filter_by_severity(events, "low")], ["c"])
        self.assertEqual([e["type"] for e in aa._filter_by_severity(events, "medium")], ["b", "c"])
        self.assertEqual([e["type"] for e in aa._filter_by_severity(events, "high")], ["a", "b", "c"])

    def test_quiet_hours(self) -> None:
        night = {"quiet_hours_enabled": 1, "quiet_start_hour": 22, "quiet_end_hour": 7}
        self.assertTrue(aa._in_quiet_hours(23, night))
        self.assertTrue(aa._in_quiet_hours(3, night))
        self.assertFalse(aa._in_quiet_hours(12, night))
        same = {"quiet_hours_enabled": 1, "quiet_start_hour": 5, "quiet_end_hour": 5}
        self.assertFalse(any(aa._in_quiet_hours(h, same) for h in range(24)))
        self.assertFalse(aa._in_quiet_hours(23, {**night, "quiet_hours_enabled": 0}))


# ---------------------------------------------------------------------------
# 11/12. Dispatch through the API
# ---------------------------------------------------------------------------

class DispatchTests(_Base):
    def _check_now(self, token: str) -> dict[str, Any]:
        resp = self.client.post("/alerts/check-now", headers=_auth(token))
        self.assertEqual(resp.status_code, 200, resp.text)
        return resp.json()

    def _events(self, uid: int) -> list[sqlite3.Row]:
        return self.db().execute("SELECT * FROM alert_events WHERE user_id = ?", (uid,)).fetchall()

    def test_quiet_hours_gate_delivery_at_local_time(self) -> None:
        token, uid = self.alert_ready_user()
        offset = ((23 - datetime.now(timezone.utc).hour) % 24) * 3600   # local time is 23:xx
        self.weather = self.make_weather({0: {"wind_gusts_kmh": 80}}, offset_seconds=offset)
        self.put_prefs(token, quiet_hours_enabled=True, quiet_start_hour=22, quiet_end_hour=7)
        result = self._check_now(token)
        self.assertEqual(result["reason"], "quiet_hours")
        self.assertEqual((self.pushes, self._events(uid)), ([], []))

        self.put_prefs(token, quiet_hours_enabled=True, quiet_start_hour=22, quiet_end_hour=22)
        self.assertEqual(self._check_now(token)["delivered"], 1)

    def test_dispatch_is_idempotent(self) -> None:
        token, uid = self.alert_ready_user()
        self.weather = self.make_weather({1: {"wind_gusts_kmh": 80}})
        self.assertEqual(self._check_now(token)["delivered"], 1)
        self.assertEqual(self._check_now(token)["delivered"], 0)
        self.assertEqual(len(self.pushes), 1)
        self.assertIsNotNone(self._events(uid)[0]["delivered_at"])

    def test_not_marked_sent_when_every_channel_fails(self) -> None:
        token, uid = self.alert_ready_user()
        self.weather = self.make_weather({1: {"wind_gusts_kmh": 80}})
        self.push_error = RuntimeError("push service down")
        self.email_error = RuntimeError("smtp down")
        self.assertEqual(self._check_now(token)["delivered"], 0)
        self.assertEqual(self._events(uid), [])
        self.push_error = self.email_error = None
        self.assertEqual(self._check_now(token)["delivered"], 1)

    def test_cooldown_stops_hourly_repeats_of_an_ongoing_condition(self) -> None:
        token, _ = self.alert_ready_user()
        self.weather = self.make_weather({1: {"wind_gusts_kmh": 80}})
        self.assertEqual(self._check_now(token)["delivered"], 1)
        self.weather = self.make_weather({2: {"wind_gusts_kmh": 80}})
        self.assertEqual(self._check_now(token)["delivered"], 0)

    def test_severity_low_only_delivers_evita_events(self) -> None:
        token, _ = self.alert_ready_user()
        self.put_prefs(token, severity="low")
        self.weather = self.make_weather({1: {"wind_gusts_kmh": 55}})
        self.assertEqual(self._check_now(token)["delivered"], 0)
        self.weather = self.make_weather({1: {"wind_gusts_kmh": 75}})
        self.assertEqual(self._check_now(token)["delivered"], 1)

    def test_check_now_is_rate_limited(self) -> None:
        token, _ = self.alert_ready_user()
        statuses = [self.client.post("/alerts/check-now", headers=_auth(token)).status_code
                    for _ in range(aa.CHECK_NOW_RATE_MAX + 1)]
        self.assertEqual(statuses[-1], 429)

    def test_dispatch_all_secret_handling_and_purge(self) -> None:
        self.alert_ready_user()
        conn = self.db()
        conn.execute("INSERT INTO sessions(user_id, token_hash, expires_at, created_at) "
                     "VALUES (999, 'stale', '2000-01-01T00:00:00+00:00', '2000-01-01T00:00:00+00:00')")
        conn.execute("INSERT INTO rate_limits(bucket, count, window_start) "
                     "VALUES ('old:bucket', 3, '2000-01-01T00:00:00+00:00')")
        conn.commit()
        self.weather = self.make_weather({1: {"wind_gusts_kmh": 80}})
        with mock.patch.dict(os.environ, {"ALERT_DISPATCH_SECRET": "s3cret-value"}):
            non_ascii = self.client.post("/alerts/dispatch-all", headers={"X-Dispatch-Secret": "sécret".encode()})
            self.assertEqual(non_ascii.status_code, 401)
            query = self.client.post("/alerts/dispatch-all?secret=s3cret-value")
            self.assertEqual(query.status_code, 401)
            ok = self.client.post("/alerts/dispatch-all", headers={"X-Dispatch-Secret": "s3cret-value"})
        self.assertEqual(ok.status_code, 200, ok.text)
        self.assertEqual((ok.json()["users"], ok.json()["events"]), (1, 1))
        conn = self.db()
        self.assertIsNone(conn.execute("SELECT 1 FROM sessions WHERE token_hash = 'stale'").fetchone())
        self.assertIsNone(conn.execute("SELECT 1 FROM rate_limits WHERE bucket = 'old:bucket'").fetchone())

    def test_claim_uses_returning_and_folds_in_the_cooldown(self) -> None:
        _, uid = self.alert_ready_user()

        class NoRowcountCursor:
            """Driver cursor that reports no rowcount, like libsql may."""
            rowcount = -1

            def __init__(self, cur: sqlite3.Cursor) -> None:
                self._cur = cur

            def __getattr__(self, name: str) -> Any:
                return getattr(self._cur, name)

        class NoRowcountConnection:
            def __init__(self, conn: sqlite3.Connection) -> None:
                self._conn = conn

            def execute(self, sql: str, params: Any = ()) -> NoRowcountCursor:
                return NoRowcountCursor(self._conn.execute(sql, params))

            def __getattr__(self, name: str) -> Any:
                return getattr(self._conn, name)

        conn = aa._TursoConn(NoRowcountConnection(_stub_connect("libsql://tests.invalid")))
        self.addCleanup(conn.close)
        self.assertTrue(aa._claim_event(conn, uid, "temp_low", "temp_low:2026-09-11T10"))
        self.assertFalse(aa._claim_event(conn, uid, "temp_low", "temp_low:2026-09-11T10"))   # same key
        self.assertFalse(aa._claim_event(conn, uid, "temp_low", "temp_low:2026-09-11T11"))   # cooldown
        self.assertTrue(aa._claim_event(conn, uid, "wind", "wind:2026-09-11T10"))           # other type


# ---------------------------------------------------------------------------
# Degraded mode: new code on a database that was not migrated yet
# ---------------------------------------------------------------------------

class UnmigratedSchemaTests(_Base):
    def setUp(self) -> None:
        super().setUp()
        conn = self.db()
        conn.executescript(
            "DROP INDEX idx_users_unsubscribe_token;"
            "ALTER TABLE users DROP COLUMN email_verified;"
            "ALTER TABLE users DROP COLUMN unsubscribe_token;"
            "DROP TABLE email_verification_tokens;"
            "ALTER TABLE alert_events DROP COLUMN delivered_at;"
        )
        conn.commit()
        aa._SCHEMA_CACHE.clear()

    def test_everything_keeps_working_with_legacy_behaviour(self) -> None:
        data = self.signup("old@example.com")
        self.assertTrue(data["user"]["email_verified"])
        self.assertEqual(self.emails, [])
        token = data["token"]
        self.assertEqual(self.login("old@example.com").status_code, 200)
        self.assertEqual(self.put_prefs(token).status_code, 200)
        self.client.post("/me/push-subscriptions", json=PUSH_SUB, headers=_auth(token))
        self.weather = self.make_weather({1: {"wind_gusts_kmh": 80}})
        first = self.client.post("/alerts/check-now", headers=_auth(token)).json()
        self.assertEqual((first["sent"], first["email_sent"]), (1, 1))
        self.assertIsNone(self.alert_emails()[0]["unsubscribe_url"])
        self.assertEqual(self.client.post("/alerts/check-now", headers=_auth(token)).json()["delivered"], 0)
        self.assertEqual(self.client.post("/alerts/unsubscribe?token=" + "w" * 32).status_code, 200)
        self.assertEqual(self.client.post("/auth/verify-email?token=whatever-token-123").status_code, 400)
        changed = self.client.put("/me/email", json={"new_email": "new@example.com", "password": PASSWORD},
                                  headers=_auth(token))
        self.assertTrue(changed.json()["email_verified"])
        self.assertEqual(self.client.delete("/me", headers=_auth(token)).status_code, 200)


# ---------------------------------------------------------------------------
# 2/4/10. Input limits, email rendering, small fixes
# ---------------------------------------------------------------------------

class MiscTests(_Base):
    def test_request_reset_swallows_email_failure(self) -> None:
        self.signup("reset-fail@example.com")
        self.email_error = RuntimeError("provider down")
        known = self.client.post("/auth/request-reset", json={"email": "reset-fail@example.com"})
        unknown = self.client.post("/auth/request-reset", json={"email": "nobody@example.com"})
        self.assertEqual((known.status_code, known.json()), (200, {"ok": True}))
        self.assertEqual((unknown.status_code, unknown.json()), (200, {"ok": True}))
        # The token row is written by the background task, only for the known address.
        self.assertEqual(self.db().execute("SELECT COUNT(*) AS c FROM password_reset_tokens").fetchone()["c"], 1)

    def test_execute_atomically_rolls_back_every_statement(self) -> None:
        conn = aa._connect()
        self.addCleanup(conn.close)
        insert = ("INSERT INTO app_state(key, value, updated_at) VALUES ('k', 'v', 'now')", ())
        with self.assertRaises(sqlite3.OperationalError):
            aa._execute_atomically(conn, [insert, ("INSERT INTO missing_table VALUES (1)", ())])
        self.assertIsNone(aa.get_app_state("k"))
        aa._execute_atomically(conn, [insert])
        self.assertEqual(aa.get_app_state("k"), "v")

    def test_email_html_escapes_text_and_links_only_trusted_urls(self) -> None:
        trusted = f"{aa.API_BASE_URL}/auth/verify-email?token=abc"
        rendered = aa._text_to_html(
            f"Locație: <b>Oraș</b> https://evil.example/login\nLink: {trusted}",
            trusted_links=[trusted, "https://evil.example/login"],
        )
        self.assertIn("&lt;b&gt;Oraș&lt;/b&gt;", rendered)
        self.assertNotIn('href="https://evil.example', rendered)
        self.assertIn(f'href="{trusted}"', rendered)

    def test_city_rejects_links(self) -> None:
        token = self.signup("city@example.com")["token"]
        for bad in ("Click https://evil.com/x", "evil.com", "www.evil.net", "<b>Cluj</b>", "login.evil.de",
                    "Brasov pay.dev", "1.2.3.4/login"):
            self.assertEqual(self.put_prefs(token, city=bad).status_code, 422, bad)
        self.assertEqual(self.put_prefs(token, city="Sfântu Gheorghe, RO").status_code, 200)

    def test_input_bounds(self) -> None:
        token = self.signup("bounds@example.com")["token"]
        long_keys = {"endpoint": PUSH_SUB["endpoint"], "keys": {"p256dh": "B" * 300, "auth": "A" * 22}}
        self.assertEqual(self.client.post("/me/push-subscriptions", json=long_keys, headers=_auth(token)).status_code, 422)
        many_stops = {"name": "Lunga", "stops": [f"Oras {i}" for i in range(21)]}
        self.assertEqual(self.client.post("/me/routes", json=many_stops, headers=_auth(token)).status_code, 422)
        self.assertEqual(self.put_prefs(token, alert_states="[1, 2]").status_code, 422)
        huge = self.client.post("/auth/login", json={"email": "bounds@example.com", "password": "x" * 257})
        self.assertEqual(huge.status_code, 422)

    def test_mask_email(self) -> None:
        self.assertEqual(aa._mask_email("andrei@example.com"), "a***@example.com")
        self.assertEqual(aa._mask_email(None), "***")


if __name__ == "__main__":
    unittest.main()
