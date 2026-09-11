"""
Accounts, authentication, alerts and hazards for WeatherForMoto.

Storage is Turso (libSQL). Schema changes go through init_db(), which only
runs when RUN_DB_MIGRATIONS=true at deploy time. Every code path that relies
on a column or table added later checks for it first (see _has_column) and
degrades safely until the migration has run.

Blocking work (sync libSQL calls, SMTP, web push over `requests`, PBKDF2) never
runs on the event loop: handlers that only do blocking work are plain `def`
(FastAPI runs them in its threadpool) and the async alert handlers push their
blocking parts through asyncio.to_thread.
"""

import asyncio
import base64
import hashlib
import hmac
import html
import ipaddress
import json
import logging
import math
import os
import re
import secrets
import smtplib
import time
import unicodedata
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any
from urllib.parse import urlparse

import httpx
from fastapi import APIRouter, BackgroundTasks, Depends, Header, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, EmailStr, Field, field_validator

from weather_service import get_weather, _haversine_km

logger = logging.getLogger("weatherformoto.auth_alerts")

DB_PATH = os.getenv("APP_DB_PATH", os.path.join(os.path.dirname(__file__), "app.db"))
TURSO_URL = os.getenv("TURSO_DATABASE_URL", "")
TURSO_TOKEN = os.getenv("TURSO_AUTH_TOKEN", "")
AUTH_CODE_TTL_MIN = int(os.getenv("AUTH_CODE_TTL_MIN", "10"))
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "30"))
ALLOW_INSECURE_AUTH_CODE = os.getenv("ALLOW_INSECURE_AUTH_CODE", "false").lower() == "true"

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER or "weatherformoto@bluemouse.cc")
BREVO_API_KEY = os.getenv("BREVO_API_KEY", "")
OWM_API_KEY = os.getenv("OPENWEATHERMAP_API_KEY", "")
# OWASP 2023 guidance for PBKDF2-HMAC-SHA256. Older hashes keep their own
# iteration count (stored in the hash) and are upgraded on the next login.
PBKDF2_ITERATIONS = int(os.getenv("PBKDF2_ITERATIONS", "600000"))
# Optional server-side secret mixed into login-code hashes (HMAC-SHA256), so a
# database leak alone does not allow offline brute force of the 6-digit codes.
AUTH_CODE_PEPPER = os.getenv("AUTH_CODE_PEPPER", "")
# Public URL of the static frontend (Cloudflare), used for links back to the app.
APP_BASE_URL = os.getenv("APP_BASE_URL", "https://weatherformoto.bluemouse.cc").rstrip("/")
# Public URL of THIS backend. Links that must hit the API (email verification)
# are built from it, because APP_BASE_URL is static hosting.
API_BASE_URL = os.getenv(
    "API_BASE_URL", "https://weatherformoto-1056457771445.europe-west1.run.app"
).rstrip("/")

# Environment marker. ALLOW_INSECURE_AUTH_CODE (dev fallback that returns the
# login code in the HTTP response) is honoured ONLY when this is not "production".
APP_ENV = os.getenv("APP_ENV", "production").lower()

# Client IP detection. Cloud Run's Google front end APPENDS the peer address to
# X-Forwarded-For, so the right-most entry is the only one a client cannot forge.
# Raise TRUSTED_PROXY_HOPS only when another trusted proxy sits in front of Google
# and also appends its peer. CF-Connecting-IP is client-controlled when the
# run.app URL is called directly, so it is honoured only on explicit opt-in.
TRUSTED_PROXY_HOPS = max(0, int(os.getenv("TRUSTED_PROXY_HOPS", "1")))
TRUST_CF_CONNECTING_IP = os.getenv("TRUST_CF_CONNECTING_IP", "false").lower() == "true"

# Brute-force protection thresholds (fixed-window counters persisted in Turso).
# Login and code verification count FAILED attempts only, per (email, IP) pair,
# so an attacker elsewhere cannot lock the real owner out.
LOGIN_RATE_MAX = int(os.getenv("LOGIN_RATE_MAX", "8"))             # failed logins per (email, IP) / window
LOGIN_RATE_MAX_IP = int(os.getenv("LOGIN_RATE_MAX_IP", "40"))     # failed logins per IP / window
LOGIN_RATE_WINDOW_SEC = int(os.getenv("LOGIN_RATE_WINDOW_SEC", "900"))  # 15 min
CODE_MAX_ATTEMPTS = int(os.getenv("CODE_MAX_ATTEMPTS", "5"))       # failed code guesses per (email, IP)
CODE_MAX_FAILURES = int(os.getenv("CODE_MAX_FAILURES", "10"))      # failed guesses per code, all IPs; then invalidated
VERIFY_RATE_MAX_IP = int(os.getenv("VERIFY_RATE_MAX_IP", "30"))    # failed verify-code per IP / TTL window
REQCODE_RATE_MAX = int(os.getenv("REQCODE_RATE_MAX", "5"))         # request-code per email / window
REQCODE_RATE_MAX_IP = int(os.getenv("REQCODE_RATE_MAX_IP", "15"))  # request-code per IP / window
REQCODE_RATE_WINDOW_SEC = int(os.getenv("REQCODE_RATE_WINDOW_SEC", "900"))  # 15 min
SIGNUP_RATE_MAX_IP = 10            # signups per IP / hour
PASSWORD_CHECK_RATE_MAX = 8        # wrong current-password attempts per user / window
RESEND_VERIFY_RATE_MAX = 3         # verification re-sends per user / hour
DISPATCH_RATE_MAX_IP = 10          # failed dispatch-all secrets per IP / hour
HAZARD_RATE_MAX = int(os.getenv("HAZARD_RATE_MAX", "10"))          # hazard reports per user / hour
CHECK_NOW_RATE_MAX = int(os.getenv("CHECK_NOW_RATE_MAX", "10"))    # manual alert checks per user / hour
# Failed code verifications per email per day, all IPs together. Blocks CODE
# login only (password login and reset keep working), so it cannot lock the
# owner out, while a botnet cannot grind codes forever either.
CODE_FAILURES_PER_EMAIL_DAY = int(os.getenv("CODE_FAILURES_PER_EMAIL_DAY", "30"))
# Failed password logins per email per day (all IPs) at which a warning is
# logged. Detection only, deliberately no lockout: see _note_password_failure.
PASSWORD_FAILURE_ALERT_DAY = max(1, int(os.getenv("PASSWORD_FAILURE_ALERT_DAY", "100")))

EMAIL_VERIFY_TTL_HOURS = 48
RESET_TOKEN_TTL_MINUTES = 30
PASSWORD_MAX_LENGTH = 256
MAX_PUSH_SUBSCRIPTIONS_PER_USER = 20

# Alert dispatch tuning.
DISPATCH_CONCURRENCY = max(1, int(os.getenv("DISPATCH_CONCURRENCY", "5")))
# The risk window starts at the current hour, so an ongoing condition would
# otherwise produce a new event key every hour. One alert per type per cooldown.
ALERT_COOLDOWN_HOURS = int(os.getenv("ALERT_COOLDOWN_HOURS", "6"))
ALERT_WINDOW_HOURS = 24
ALERT_EVENT_RETENTION_DAYS = 3
STALE_CLAIM_MINUTES = 15

VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "")
VAPID_SUBJECT = os.getenv("VAPID_SUBJECT", "mailto:weatherformoto@bluemouse.cc")

_TOO_MANY_ATTEMPTS = "Prea multe încercări. Încearcă din nou mai târziu."
_TOO_MANY_REQUESTS = "Prea multe cereri. Încearcă din nou mai târziu."


@dataclass
class SessionUser:
    user_id: int
    email: str


# ---------------------------------------------------------------------------
# Input validation helpers
# ---------------------------------------------------------------------------

# Anything that looks like a link. City names end up in push notifications, so
# a URL there would be a phishing vector ("click here to keep your alerts").
# Rejects schemes, "www.", anything shaped like a domain ("word.tld" with any
# 2+ letter TLD) and dotted IPv4 addresses. Trade-off: a name typed without a
# space after an abbreviation ("Sf.Gheorghe") is rejected too; "Sf. Gheorghe"
# and geocoded names like "Sfântu Gheorghe, RO" are fine.
_URL_LIKE_RE = re.compile(
    r"(://|www\.|\b[\w-]+\.[a-z]{2,}\b|\b\d{1,3}(?:\.\d{1,3}){3}\b)",
    re.IGNORECASE,
)


def _validate_city(value: str | None) -> str | None:
    if value is None:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if any(unicodedata.category(ch) == "Cc" for ch in cleaned) or "<" in cleaned or ">" in cleaned:
        raise ValueError("Numele orașului conține caractere nepermise.")
    if _URL_LIKE_RE.search(cleaned):
        raise ValueError("Numele orașului nu poate conține linkuri.")
    return cleaned


def _validate_alert_states(value: str | None) -> str | None:
    """alert_states is a JSON object of per-alert toggles produced by the frontend."""
    if value is None:
        return None
    try:
        parsed = json.loads(value)
    except ValueError as exc:
        raise ValueError("alert_states trebuie să fie JSON valid.") from exc
    if not isinstance(parsed, dict) or len(parsed) > 64:
        raise ValueError("alert_states trebuie să fie un obiect cu cel mult 64 de chei.")
    for key, item in parsed.items():
        if len(key) > 64:
            raise ValueError("Cheie alert_states prea lungă.")
        if isinstance(item, str) and len(item) > 64:
            raise ValueError("Valoare alert_states prea lungă.")
        if isinstance(item, (dict, list)):
            raise ValueError("alert_states acceptă doar valori simple.")
    return value


class RequestCodePayload(BaseModel):
    email: EmailStr


class VerifyCodePayload(BaseModel):
    email: EmailStr
    code: str = Field(min_length=4, max_length=12)


class SignupPayload(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=PASSWORD_MAX_LENGTH)
    display_name: str | None = Field(default=None, max_length=80)


class LoginPayload(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=PASSWORD_MAX_LENGTH)


class ProfilePayload(BaseModel):
    display_name: str = Field(min_length=1, max_length=80)


class ChangeEmailPayload(BaseModel):
    new_email: EmailStr
    password: str = Field(min_length=1, max_length=PASSWORD_MAX_LENGTH)


class AlertPrefsPayload(BaseModel):
    # Alerts go out by web push only. Fields that older clients still send
    # (the retired email_alert_* toggles) are ignored by pydantic.
    enabled: bool = True
    min_score: int = Field(default=45, ge=0, le=100)
    max_wind_gust: float = Field(default=50, ge=10, le=200)
    max_precip: float = Field(default=2, ge=0, le=50)
    max_rain_probability: int = Field(default=70, ge=0, le=100)
    min_temp: float | None = Field(default=None, ge=-60, le=45)
    max_temp: float | None = Field(default=None, ge=-20, le=70)
    frost_risk_enabled: bool = True
    quiet_hours_enabled: bool = False
    quiet_start_hour: int = Field(default=22, ge=0, le=23)
    quiet_end_hour: int = Field(default=7, ge=0, le=23)
    severity: str = Field(default="medium", pattern="^(low|medium|high)$")
    home_lat: float | None = Field(default=None, ge=-90, le=90)
    home_lon: float | None = Field(default=None, ge=-180, le=180)
    city: str | None = Field(default=None, max_length=120)
    alert_states: str | None = Field(default=None, max_length=4000)  # JSON string of per-alert-type enabled states
    moto_type: str = Field(default="naked", pattern="^(naked|touring|enduro|sport|scooter)$")
    comfort_temp: int = Field(default=20, ge=-20, le=50)
    wind_tolerance: str = Field(default="medium", pattern="^(low|medium|high)$")
    rain_tolerance: str = Field(default="medium", pattern="^(low|medium|high)$")

    @field_validator("city")
    @classmethod
    def _check_city(cls, value: str | None) -> str | None:
        return _validate_city(value)

    @field_validator("alert_states")
    @classmethod
    def _check_alert_states(cls, value: str | None) -> str | None:
        return _validate_alert_states(value)


class PushKeysPayload(BaseModel):
    # Browser subscriptions carry base64url keys: p256dh is 87 chars, auth 22.
    p256dh: str = Field(min_length=1, max_length=256)
    auth: str = Field(min_length=1, max_length=64)


class PushSubscriptionPayload(BaseModel):
    endpoint: str = Field(min_length=1, max_length=2048)
    keys: PushKeysPayload


class SavedRoutePayload(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    stops: list[str] = Field(max_length=20)
    total_distance_km: float | None = Field(default=None, ge=0, le=5000)

    @field_validator("stops")
    @classmethod
    def _check_stops(cls, value: list[str]) -> list[str]:
        if any(len(stop) > 160 for stop in value):
            raise ValueError("Numele unei opriri poate avea cel mult 160 de caractere.")
        return value


class RideLogPayload(BaseModel):
    route_name: str | None = Field(default=None, max_length=80)
    start_city: str = Field(min_length=1, max_length=120)
    end_city: str = Field(min_length=1, max_length=120)
    distance_km: float = Field(gt=0, le=5000)
    duration_min: int = Field(gt=0, le=24 * 60)
    avg_moto_score: int | None = Field(default=None, ge=0, le=100)
    max_wind_gust: float | None = Field(default=None, ge=0, le=250)
    max_precip: float | None = Field(default=None, ge=0, le=200)


class HazardPayload(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    hazard_type: str = Field(min_length=2, max_length=40)
    severity: int = Field(ge=1, le=5)
    description: str = Field(min_length=3, max_length=220)
    ttl_hours: int = Field(default=6, ge=1, le=72)


class RequestResetPayload(BaseModel):
    email: EmailStr


class ChangePasswordPayload(BaseModel):
    current_password: str = Field(min_length=1, max_length=PASSWORD_MAX_LENGTH)
    new_password: str = Field(min_length=8, max_length=PASSWORD_MAX_LENGTH)


class ResetPasswordPayload(BaseModel):
    token: str = Field(min_length=1, max_length=128)
    new_password: str = Field(min_length=8, max_length=PASSWORD_MAX_LENGTH)


router = APIRouter(tags=["account", "alerts"])


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ---------------------------------------------------------------------------
# Database access
# ---------------------------------------------------------------------------

class _TursoCursor:
    """Wraps a libsql cursor so fetchone/fetchall return dicts (like sqlite3.Row)."""

    def __init__(self, cur: Any) -> None:
        self._cur = cur

    @property
    def description(self) -> Any:
        return self._cur.description

    @property
    def lastrowid(self) -> int | None:
        return getattr(self._cur, "lastrowid", None)

    def _row(self, row: Any) -> Any:
        if row is None:
            return None
        desc = self._cur.description
        if desc:
            return {desc[i][0]: row[i] for i in range(len(desc))}
        try:
            return dict(row)
        except (TypeError, ValueError):
            return row

    def fetchone(self) -> Any:
        return self._row(self._cur.fetchone())

    def fetchall(self) -> list[Any]:
        desc = self._cur.description
        rows = self._cur.fetchall()
        if not rows:
            return []
        if desc:
            cols = [d[0] for d in desc]
            return [{cols[i]: row[i] for i in range(len(cols))} for row in rows]
        result = []
        for row in rows:
            try:
                result.append(dict(row))
            except (TypeError, ValueError):
                result.append(row)
        return result


class _TursoConn:
    """Wraps a libsql connection to return dict rows from every cursor."""

    def __init__(self, conn: Any) -> None:
        self._conn = conn

    def execute(self, sql: str, params: Sequence[Any] = ()) -> _TursoCursor:
        return _TursoCursor(self._conn.execute(sql, params))

    def executemany(self, sql: str, params_list: Sequence[Sequence[Any]]) -> Any:
        return self._conn.executemany(sql, params_list)

    def commit(self) -> None:
        self._conn.commit()

    def close(self) -> None:
        self._conn.close()


def _connect() -> _TursoConn:
    if not TURSO_URL or not TURSO_TOKEN:
        raise RuntimeError("TURSO_DATABASE_URL and TURSO_AUTH_TOKEN must be set")
    import libsql_experimental as libsql  # type: ignore
    return _TursoConn(libsql.connect(TURSO_URL, auth_token=TURSO_TOKEN))


def _row(row: Any) -> Any:
    if row is None:
        return None
    if isinstance(row, dict):
        return row
    try:
        return dict(row)
    except (TypeError, ValueError):
        return row


def _execute_atomically(conn: _TursoConn, statements: Sequence[tuple[str, Sequence[Any]]]) -> None:
    """Run several write statements in ONE explicit transaction; roll back
    everything on any failure.

    sqlite3 and libsql differ in how they open implicit transactions, so this
    does not rely on them: it closes anything left open, then sends literal
    BEGIN / COMMIT / ROLLBACK statements, which both drivers pass through.
    Code that needs a row count uses RETURNING, never cursor.rowcount.
    """
    conn.commit()  # close an implicit transaction left open by earlier statements
    conn.execute("BEGIN")
    try:
        for sql, params in statements:
            conn.execute(sql, params)
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception as rollback_exc:
            logger.error("rollback failed after transaction error: %s", rollback_exc)
        raise


# Schema probe cache: (table, column) -> (present, checked_at_monotonic).
# Positive answers never expire; negative ones are re-checked every few
# minutes so an instance notices a migration that ran after it started.
_SCHEMA_CACHE: dict[tuple[str, str], tuple[bool, float]] = {}
_NEGATIVE_SCHEMA_TTL_SEC = 300.0


def _has_column(conn: _TursoConn, table: str, column: str) -> bool:
    """True when ``table.column`` exists. ``table`` must be an internal constant."""
    key = (table, column)
    now = time.monotonic()
    cached = _SCHEMA_CACHE.get(key)
    if cached and (cached[0] or now - cached[1] < _NEGATIVE_SCHEMA_TTL_SEC):
        return cached[0]
    try:
        cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
        present = column in {c["name"] for c in cols}
    except Exception as exc:
        logger.warning("schema probe failed for %s.%s: %s", table, column, exc)
        return False
    _SCHEMA_CACHE[key] = (present, now)
    return present


def _ensure_column(conn: _TursoConn, table: str, column: str, ddl: str) -> bool:
    """Add a column when missing. Returns True when it was just added."""
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names = {c["name"] for c in cols}
    if column not in names:
        conn.execute(ddl)
        return True
    return False


_INIT_TABLES = [
    """CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT UNIQUE NOT NULL,
        created_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS auth_codes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        email TEXT NOT NULL,
        code TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        created_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token_hash TEXT UNIQUE NOT NULL,
        expires_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""",
    """CREATE TABLE IF NOT EXISTS alert_prefs (
        user_id INTEGER PRIMARY KEY,
        enabled INTEGER NOT NULL DEFAULT 1,
        min_score INTEGER NOT NULL DEFAULT 45,
        max_wind_gust REAL NOT NULL DEFAULT 50,
        max_precip REAL NOT NULL DEFAULT 2,
        frost_risk_enabled INTEGER NOT NULL DEFAULT 1,
        home_lat REAL,
        home_lon REAL,
        city TEXT,
        updated_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""",
    """CREATE TABLE IF NOT EXISTS push_subscriptions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        endpoint TEXT UNIQUE NOT NULL,
        p256dh TEXT NOT NULL,
        auth TEXT NOT NULL,
        last_seen TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""",
    """CREATE TABLE IF NOT EXISTS alert_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        event_key TEXT NOT NULL,
        created_at TEXT NOT NULL,
        UNIQUE(user_id, event_key)
    )""",
    """CREATE TABLE IF NOT EXISTS saved_routes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        name TEXT NOT NULL,
        stops_json TEXT NOT NULL,
        total_distance_km REAL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""",
    """CREATE TABLE IF NOT EXISTS ride_logs (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        route_name TEXT,
        start_city TEXT NOT NULL,
        end_city TEXT NOT NULL,
        distance_km REAL NOT NULL,
        duration_min INTEGER NOT NULL,
        avg_moto_score INTEGER,
        max_wind_gust REAL,
        max_precip REAL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""",
    """CREATE TABLE IF NOT EXISTS hazard_reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        lat REAL NOT NULL,
        lon REAL NOT NULL,
        hazard_type TEXT NOT NULL,
        severity INTEGER NOT NULL,
        description TEXT NOT NULL,
        expires_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""",
    """CREATE TABLE IF NOT EXISTS password_reset_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        token_hash TEXT UNIQUE NOT NULL,
        expires_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""",
    """CREATE TABLE IF NOT EXISTS app_state (
        key TEXT PRIMARY KEY,
        value TEXT NOT NULL,
        updated_at TEXT NOT NULL
    )""",
    """CREATE TABLE IF NOT EXISTS rate_limits (
        bucket TEXT PRIMARY KEY,
        count INTEGER NOT NULL,
        window_start TEXT NOT NULL
    )""",
    # Single-use email verification links. `email` pins the token to the address
    # it was sent to, so a later email change cannot be verified with an old link.
    """CREATE TABLE IF NOT EXISTS email_verification_tokens (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        email TEXT NOT NULL,
        token_hash TEXT UNIQUE NOT NULL,
        expires_at TEXT NOT NULL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(user_id) REFERENCES users(id)
    )""",
]

_INIT_INDEXES = [
    # Lets GET /hazards prefilter by bounding box before LIMIT.
    "CREATE INDEX IF NOT EXISTS idx_hazard_reports_lat_lon ON hazard_reports(lat, lon)",
]


def init_db() -> None:
    conn = _connect()
    try:
        for stmt in _INIT_TABLES:
            conn.execute(stmt)
        for stmt in _INIT_INDEXES:
            conn.execute(stmt)
        conn.commit()
        _ensure_column(conn, "users", "display_name", "ALTER TABLE users ADD COLUMN display_name TEXT")
        _ensure_column(conn, "users", "password_hash", "ALTER TABLE users ADD COLUMN password_hash TEXT")
        # DEFAULT 1 grandfathers every existing account as verified; new signups
        # and email changes write 0 explicitly.
        _ensure_column(
            conn,
            "users",
            "email_verified",
            "ALTER TABLE users ADD COLUMN email_verified INTEGER NOT NULL DEFAULT 1",
        )
        # Email alerts were retired: older databases still carry
        # users.unsubscribe_token and the alert_prefs.email_alert* columns. They
        # have defaults, nothing reads or writes them, and they are left in place.
        if _ensure_column(conn, "alert_events", "delivered_at", "ALTER TABLE alert_events ADD COLUMN delivered_at TEXT"):
            # Rows written before this column existed were recorded after a send
            # attempt, so treat them as delivered (keeps their dedupe effect).
            conn.execute("UPDATE alert_events SET delivered_at = created_at WHERE delivered_at IS NULL")
        _ensure_column(
            conn,
            "alert_prefs",
            "max_rain_probability",
            "ALTER TABLE alert_prefs ADD COLUMN max_rain_probability INTEGER NOT NULL DEFAULT 70",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "min_temp",
            "ALTER TABLE alert_prefs ADD COLUMN min_temp REAL",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "max_temp",
            "ALTER TABLE alert_prefs ADD COLUMN max_temp REAL",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "quiet_hours_enabled",
            "ALTER TABLE alert_prefs ADD COLUMN quiet_hours_enabled INTEGER NOT NULL DEFAULT 0",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "quiet_start_hour",
            "ALTER TABLE alert_prefs ADD COLUMN quiet_start_hour INTEGER NOT NULL DEFAULT 22",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "quiet_end_hour",
            "ALTER TABLE alert_prefs ADD COLUMN quiet_end_hour INTEGER NOT NULL DEFAULT 7",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "severity",
            "ALTER TABLE alert_prefs ADD COLUMN severity TEXT NOT NULL DEFAULT 'medium'",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "alert_states",
            "ALTER TABLE alert_prefs ADD COLUMN alert_states TEXT",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "moto_type",
            "ALTER TABLE alert_prefs ADD COLUMN moto_type TEXT NOT NULL DEFAULT 'naked'",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "comfort_temp",
            "ALTER TABLE alert_prefs ADD COLUMN comfort_temp INTEGER NOT NULL DEFAULT 20",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "wind_tolerance",
            "ALTER TABLE alert_prefs ADD COLUMN wind_tolerance TEXT NOT NULL DEFAULT 'medium'",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "rain_tolerance",
            "ALTER TABLE alert_prefs ADD COLUMN rain_tolerance TEXT NOT NULL DEFAULT 'medium'",
        )
        conn.commit()
    finally:
        conn.close()
    # This process may have cached "missing" answers before the migration.
    _SCHEMA_CACHE.clear()


# ---------------------------------------------------------------------------
# Generic key/value app state (persists across Cloud Run cold starts & instances)
# ---------------------------------------------------------------------------

def get_app_state(key: str) -> str | None:
    """Return the stored value for ``key``, or None if absent."""
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT value FROM app_state WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None
    finally:
        conn.close()


def set_app_state(key: str, value: str) -> None:
    """Insert or update ``key`` with ``value``."""
    conn = _connect()
    try:
        conn.execute(
            "INSERT INTO app_state (key, value, updated_at) VALUES (?, ?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, "
            "updated_at = excluded.updated_at",
            (key, value, _utc_now().isoformat()),
        )
        conn.commit()
    finally:
        conn.close()


_distance_km = _haversine_km

# ---------------------------------------------------------------------------
# Hashing: tokens, login codes, passwords
# ---------------------------------------------------------------------------

def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _hash_code(code: str) -> str:
    """Hash a login code for storage. With AUTH_CODE_PEPPER set this is
    HMAC-SHA256 keyed by the pepper; without it, plain SHA-256 (legacy)."""
    if AUTH_CODE_PEPPER:
        return hmac.new(AUTH_CODE_PEPPER.encode("utf-8"), code.encode("utf-8"), hashlib.sha256).hexdigest()
    return _hash_token(code)


def _email_key(email: str) -> str:
    """Stable pseudonymous key for an email, used inside rate-limit bucket names
    so the table holds no raw addresses and a user's buckets can be deleted."""
    return hashlib.sha256(email.strip().lower().encode("utf-8")).hexdigest()[:32]


def _mask_email(email: str | None) -> str:
    """Log-safe form of an address: first letter of the local part + domain."""
    if not email or "@" not in email:
        return "***"
    local, _, domain = email.partition("@")
    return f"{local[:1]}***@{domain}"


def _hash_password(password: str, iterations: int | None = None) -> str:
    rounds = iterations or PBKDF2_ITERATIONS
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, rounds)
    return f"pbkdf2_sha256${rounds}${base64.b64encode(salt).decode()}${base64.b64encode(dk).decode()}"


def _parse_password_hash(stored: str | None) -> tuple[int, bytes, bytes] | None:
    """Split 'pbkdf2_sha256$iterations$salt$hash' into its parts, or None."""
    if not stored:
        return None
    try:
        algo, iters, salt_b64, hash_b64 = stored.split("$", 3)
        if algo != "pbkdf2_sha256":
            return None
        return int(iters), base64.b64decode(salt_b64.encode()), base64.b64decode(hash_b64.encode())
    except (ValueError, TypeError) as exc:
        logger.warning("malformed password hash in storage: %s", type(exc).__name__)
        return None


_DUMMY_PASSWORD_HASH: str | None = None


def _dummy_password_hash() -> str:
    """A throwaway hash used to spend the same CPU time when there is no real
    hash to check, so response timing does not reveal whether an account exists."""
    global _DUMMY_PASSWORD_HASH
    if _DUMMY_PASSWORD_HASH is None:
        _DUMMY_PASSWORD_HASH = _hash_password(secrets.token_urlsafe(16))
    return _DUMMY_PASSWORD_HASH


def _verify_password(password: str, stored: str | None) -> bool:
    parsed = _parse_password_hash(stored)
    if parsed is None:
        dummy = _parse_password_hash(_dummy_password_hash())
        if dummy:
            hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), dummy[1], dummy[0])
        return False
    iterations, salt, expected = parsed
    derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(derived, expected)


def _password_needs_rehash(stored: str | None) -> bool:
    parsed = _parse_password_hash(stored)
    return parsed is not None and parsed[0] < PBKDF2_ITERATIONS


def _issue_session(conn: _TursoConn, user_id: int) -> str:
    token = secrets.token_urlsafe(32)
    token_hash = _hash_token(token)
    now = _utc_now()
    expires = now + timedelta(days=SESSION_TTL_DAYS)
    conn.execute(
        "INSERT INTO sessions(user_id, token_hash, expires_at, created_at) VALUES (?, ?, ?, ?)",
        (user_id, token_hash, expires.isoformat(), now.isoformat()),
    )
    conn.commit()
    return token


def _issue_session_if_password(conn: _TursoConn, user_id: int, password_hash: str) -> str | None:
    """Issue a session only if the account still has the password hash that was
    just verified. One conditional INSERT closes the race where a password login
    in flight lands right after the account was evicted (password cleared) or
    the password was reset/changed. Returns None when the hash moved on."""
    token = secrets.token_urlsafe(32)
    now = _utc_now()
    expires = now + timedelta(days=SESSION_TTL_DAYS)
    row = conn.execute(
        "INSERT INTO sessions(user_id, token_hash, expires_at, created_at) "
        "SELECT ?, ?, ?, ? WHERE EXISTS (SELECT 1 FROM users WHERE id = ? AND password_hash = ?) "
        "RETURNING id",
        (user_id, _hash_token(token), expires.isoformat(), now.isoformat(), user_id, password_hash),
    ).fetchone()
    conn.commit()
    return token if row else None


def _bearer_token(authorization: str | None) -> str | None:
    if not authorization or not authorization.lower().startswith("bearer "):
        return None
    token = authorization.split(" ", 1)[1].strip()
    if not token or len(token) > 256:
        return None
    return token


# Link tokens we issue are token_urlsafe output; anything else is malformed.
# The check also makes a token safe to echo into a confirmation form's URL.
_URLSAFE_TOKEN_RE = re.compile(r"^[A-Za-z0-9_-]{16,128}$")


def _is_well_formed_token(token: str) -> bool:
    return bool(_URLSAFE_TOKEN_RE.match(token or ""))


# ---------------------------------------------------------------------------
# Brute-force rate limiting. Fixed-window counters persisted in Turso so they
# survive Cloud Run cold starts and span multiple instances.
#
# Failure policy: the limiter FAILS CLOSED (HTTP 503) when its storage errors.
# It lives in the same database every guarded endpoint needs anyway, so a
# storage error almost always means the request would have failed regardless,
# while failing open would silently switch brute-force protection off exactly
# when the database is misbehaving.
# ---------------------------------------------------------------------------

_RATE_TABLE_READY = False
_RATE_UNAVAILABLE = "Serviciu temporar indisponibil. Încearcă din nou în câteva momente."


def _ensure_rate_table(conn: _TursoConn) -> None:
    """Create the rate_limits table on first use (no migration dependency)."""
    global _RATE_TABLE_READY
    if _RATE_TABLE_READY:
        return
    conn.execute(
        "CREATE TABLE IF NOT EXISTS rate_limits ("
        "bucket TEXT PRIMARY KEY, count INTEGER NOT NULL, window_start TEXT NOT NULL)"
    )
    conn.commit()
    _RATE_TABLE_READY = True


def _bucket_kind(bucket: str) -> str:
    """Bucket names carry email keys and IPs; logs only get the prefix."""
    return bucket.split(":", 1)[0]


def _rate_hit_count(conn: _TursoConn, bucket: str, window_sec: int) -> int:
    """Atomically register one hit on ``bucket`` and return the new count in
    the current window.

    A single UPSERT ... RETURNING both resets an expired window and increments,
    so concurrent requests cannot read the same count and all slip through.
    Raises HTTP 503 on storage errors (fail closed, see the section comment).
    """
    now = _utc_now()
    cutoff = (now - timedelta(seconds=window_sec)).isoformat()
    try:
        _ensure_rate_table(conn)
        row = conn.execute(
            "INSERT INTO rate_limits(bucket, count, window_start) VALUES (?, 1, ?) "
            "ON CONFLICT(bucket) DO UPDATE SET "
            "count = CASE WHEN rate_limits.window_start <= ? THEN 1 ELSE rate_limits.count + 1 END, "
            "window_start = CASE WHEN rate_limits.window_start <= ? "
            "THEN excluded.window_start ELSE rate_limits.window_start END "
            "RETURNING count",
            (bucket, now.isoformat(), cutoff, cutoff),
        ).fetchone()
        conn.commit()
    except Exception as exc:
        logger.error("rate-limit storage error on %s bucket (failing closed): %s", _bucket_kind(bucket), exc)
        raise HTTPException(status_code=503, detail=_RATE_UNAVAILABLE) from exc
    if not row:
        logger.error("rate-limit upsert returned no row for %s bucket (failing closed)", _bucket_kind(bucket))
        raise HTTPException(status_code=503, detail=_RATE_UNAVAILABLE)
    return int(row["count"])


def _rate_hit(conn: _TursoConn, bucket: str, max_count: int, window_sec: int) -> bool:
    """Register one hit; True when the caller is now OVER the limit and must be rejected."""
    return _rate_hit_count(conn, bucket, window_sec) > max_count


def _rate_undo(conn: _TursoConn, bucket: str) -> None:
    """Give back one hit. Used to make "reserve first, refund on success"
    counters that effectively count failures only, without a race window."""
    try:
        conn.execute("UPDATE rate_limits SET count = MAX(count - 1, 0) WHERE bucket = ?", (bucket,))
        conn.commit()
    except Exception as exc:
        logger.warning("rate-limit refund error on %s bucket: %s", _bucket_kind(bucket), exc)


def _rate_reset(conn: _TursoConn, bucket: str) -> None:
    try:
        conn.execute("DELETE FROM rate_limits WHERE bucket = ?", (bucket,))
        conn.commit()
    except Exception as exc:
        logger.warning("rate-limit reset error on %s bucket: %s", _bucket_kind(bucket), exc)


def _enforce_limit(
    conn: _TursoConn,
    bucket: str,
    max_count: int,
    window_sec: int,
    detail: str,
    request: Request | None = None,
) -> None:
    if _rate_hit(conn, bucket, max_count, window_sec):
        if request is not None:
            _log_rate_limited(bucket, request)
        raise HTTPException(status_code=429, detail=detail)


def _enforce_limit_standalone(bucket: str, max_count: int, window_sec: int, detail: str) -> None:
    """Same as _enforce_limit with its own connection (for asyncio.to_thread)."""
    conn = _connect()
    try:
        _enforce_limit(conn, bucket, max_count, window_sec, detail)
    finally:
        conn.close()


def _forwarded_for_entries(request: Request) -> list[str]:
    """All X-Forwarded-For entries in order, even when the list is split over
    several header lines (a client can send its own line before the proxy's)."""
    joined = ",".join(request.headers.getlist("x-forwarded-for"))
    return [p.strip() for p in joined.split(",") if p.strip()]


def _client_ip(request: Request) -> str:
    """Client IP as seen by the trusted proxy chain (see TRUSTED_PROXY_HOPS)."""
    if TRUST_CF_CONNECTING_IP:
        cf = (request.headers.get("cf-connecting-ip") or "").strip()
        if cf:
            return cf
    parts = _forwarded_for_entries(request)
    if parts and TRUSTED_PROXY_HOPS > 0:
        # Each trusted hop appends one entry; the entry our nearest trusted
        # proxy saw as its peer is TRUSTED_PROXY_HOPS positions from the end.
        return parts[max(len(parts) - TRUSTED_PROXY_HOPS, 0)]
    return request.client.host if request.client else "unknown"


def _ip_bucket_key(ip: str) -> str:
    """Rate-limit identity of an address. IPv6 is grouped by /64, because one
    subscriber usually gets a whole /64 and could rotate through it; IPv4 (and
    IPv4-mapped IPv6) stays the single address."""
    try:
        addr = ipaddress.ip_address(ip.strip())
    except ValueError:
        return ip.strip()[:64] or "unknown"
    if isinstance(addr, ipaddress.IPv6Address):
        if addr.ipv4_mapped:
            return str(addr.ipv4_mapped)
        return str(ipaddress.IPv6Network((addr, 64), strict=False))
    return str(addr)


def _mask_ip(ip: str) -> str:
    """Log-safe address: last IPv4 octet or last 64 IPv6 bits hidden."""
    try:
        addr = ipaddress.ip_address(ip.strip())
    except ValueError:
        return "invalid"
    if isinstance(addr, ipaddress.IPv6Address) and not addr.ipv4_mapped:
        return str(ipaddress.IPv6Network((addr, 64), strict=False))
    v4 = addr.ipv4_mapped if isinstance(addr, ipaddress.IPv6Address) else addr
    return ".".join(str(v4).split(".")[:3] + ["x"])


def _rate_ip(request: Request) -> str:
    """Client identity used inside per-IP rate-limit bucket names."""
    return _ip_bucket_key(_client_ip(request))


def _log_rate_limited(bucket: str, request: Request) -> None:
    """Logs the chosen (masked) client IP and the X-Forwarded-For length on a
    429, so production logs show whether the right-most hop really is the
    client and not a value the client injected."""
    logger.warning(
        "rate limit hit on %s bucket: client=%s xff_entries=%d",
        _bucket_kind(bucket),
        _mask_ip(_client_ip(request)),
        len(_forwarded_for_entries(request)),
    )


# Known Web Push service hosts. A subscription endpoint must be HTTPS on one of
# these, to avoid storing/sending to an attacker-controlled SSRF target.
_PUSH_ENDPOINT_HOSTS = (
    "fcm.googleapis.com",
    "android.googleapis.com",
    ".push.services.mozilla.com",
    ".notify.windows.com",
    ".push.apple.com",
    ".push.microsoft.com",
    ".push.cn.miui.com",
)


def _is_valid_push_endpoint(endpoint: str) -> bool:
    try:
        u = urlparse(endpoint or "")
        if u.scheme != "https" or not u.hostname:
            return False
        host = u.hostname.lower()
        return any(host == h or host.endswith(h) for h in _PUSH_ENDPOINT_HOSTS)
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Email rendering and delivery
# ---------------------------------------------------------------------------

def _is_trusted_link(url: str) -> bool:
    """Only links to our own frontend or API may become clickable in emails."""
    for base in (APP_BASE_URL, API_BASE_URL):
        if base and (url == base or url.startswith(base + "/") or url.startswith(base + "?")):
            return True
    return False


def _html_link(url: str, label: str | None = None) -> str:
    safe_url = html.escape(url, quote=True)
    safe_label = html.escape(label, quote=True) if label else safe_url
    return f'<a href="{safe_url}" style="color:#f97316;text-decoration:underline;">{safe_label}</a>'


def _text_to_html(text: str, trusted_links: Sequence[str] = ()) -> str:
    """Convert a plain-text email body to a simple branded HTML email.

    Every character of ``text`` is HTML-escaped and nothing in it is turned into
    a link, except exact occurrences of ``trusted_links`` (URLs we generated and
    that point at our own domains). User-controlled values embedded in the text,
    such as the city name, therefore can never become clickable.
    """
    anchors = {html.escape(u, quote=True): _html_link(u) for u in trusted_links if _is_trusted_link(u)}
    lines_html = []
    for line in text.splitlines():
        escaped = html.escape(line, quote=True)
        for escaped_url, anchor in anchors.items():
            escaped = escaped.replace(escaped_url, anchor)
        if escaped.strip():
            lines_html.append(f"<p style='margin:0 0 8px 0;'>{escaped}</p>")
        else:
            lines_html.append("<br>")

    body_inner = "\n".join(lines_html)
    app_link = _html_link(APP_BASE_URL)
    return f"""<!DOCTYPE html>
<html><head><meta charset="utf-8"></head>
<body style="margin:0;padding:0;background:#111827;font-family:Arial,sans-serif;color:#e5e7eb;">
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#111827;padding:32px 0;">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="background:#1f2937;border-radius:12px;overflow:hidden;max-width:560px;width:100%;">
        <tr><td style="background:#111827;padding:20px 32px;border-bottom:2px solid #f97316;">
          <span style="font-size:20px;font-weight:bold;letter-spacing:2px;color:#f97316;">MOTO /// METEO</span>
          <span style="font-size:11px;color:#9ca3af;margin-left:12px;">WeatherForMoto</span>
        </td></tr>
        <tr><td style="padding:28px 32px;font-size:14px;line-height:1.7;color:#d1d5db;">
          {body_inner}
        </td></tr>
        <tr><td style="padding:16px 32px;background:#111827;font-size:11px;color:#6b7280;border-top:1px solid #374151;">
          Echipa WeatherForMoto &nbsp;·&nbsp; {app_link}
        </td></tr>
      </table>
    </td></tr>
  </table>
</body></html>"""


def _send_email(email: str, subject: str, text: str, links: Sequence[str] = ()) -> None:
    """Send one account email (codes, verification, password reset), blocking.
    Raises on delivery errors; callers decide whether that is fatal. Run it
    from a threadpool, never on the event loop."""
    html_body = _text_to_html(text, links)

    # Prefer Brevo Email API when configured (more reliable in cloud runtimes).
    if BREVO_API_KEY:
        logger.info("_send_email: using Brevo API, to=%s", _mask_email(email))
        payload: dict[str, Any] = {
            "sender": {"email": SMTP_FROM},
            "to": [{"email": email}],
            "subject": subject,
            "textContent": text,
            "htmlContent": html_body,
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "api-key": BREVO_API_KEY,
        }
        with httpx.Client(timeout=8) as client:
            resp = client.post("https://api.brevo.com/v3/smtp/email", json=payload, headers=headers)
        if resp.status_code >= 400:
            raise RuntimeError(f"Brevo API error: HTTP {resp.status_code} - {resp.text[:180]}")
        logger.info("_send_email: Brevo OK (HTTP %s)", resp.status_code)
        return

    if not SMTP_HOST:
        logger.warning("_send_email: no BREVO_API_KEY and no SMTP_HOST, email to %s not sent", _mask_email(email))
        return

    logger.info("_send_email: using SMTP host=%s to=%s", SMTP_HOST, _mask_email(email))
    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = email
    msg.set_content(text)
    msg.add_alternative(html_body, subtype="html")

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=8) as server:
        server.starttls()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


def _send_email_safely(email: str, subject: str, text: str, purpose: str, links: Sequence[str] = ()) -> bool:
    """_send_email for non-critical mail (background tasks, confirmations):
    failures are logged with context and reported as False, never raised."""
    try:
        _send_email(email, subject, text, links=links)
        return True
    except Exception as exc:
        logger.warning("Could not send %s email to %s: %s", purpose, _mask_email(email), exc)
        return False


def _send_auth_email(email: str, code: str) -> None:
    text = (
        "Codul tău de autentificare WeatherForMoto este: "
        f"{code}\n\nValabil {AUTH_CODE_TTL_MIN} minute."
    )
    _send_email(email, "WeatherForMoto — cod autentificare", text)


def _send_verification_email(email: str, raw_token: str) -> None:
    link = f"{API_BASE_URL}/auth/verify-email?token={raw_token}"
    text = (
        "Salut,\n\n"
        "Confirmă adresa de email a contului tău WeatherForMoto accesând linkul de mai jos "
        f"(valabil {EMAIL_VERIFY_TTL_HOURS} de ore):\n{link}\n\n"
        "Dacă nu tu ai creat contul, ignoră acest mesaj.\n\n"
        "Echipa WeatherForMoto"
    )
    _send_email_safely(email, "WeatherForMoto — confirmă adresa de email", text, "verification", links=[link])


def _html_page(
    title: str,
    message: str,
    status_code: int = 200,
    form_action: str | None = None,
    button_label: str = "",
) -> HTMLResponse:
    """Small self-contained Romanian page for links opened from emails.
    Everything is escaped; the only link goes back to the app, and the optional
    form posts back to this backend."""
    form = ""
    if form_action:
        form = (
            f'<form method="post" action="{html.escape(form_action, quote=True)}">'
            f'<button type="submit">{html.escape(button_label)}</button></form>'
        )
    page = f"""<!DOCTYPE html>
<html lang="ro"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{html.escape(title)} · WeatherForMoto</title>
<style>
  body{{margin:0;padding:32px 16px;background:#111827;color:#e5e7eb;font-family:Arial,sans-serif;}}
  main{{max-width:480px;margin:0 auto;background:#1f2937;border-radius:12px;padding:28px;border-top:3px solid #f97316;}}
  h1{{font-size:20px;margin:0 0 12px;color:#f97316;}}
  p{{line-height:1.6;margin:0 0 20px;}}
  a{{color:#f97316;}}
  button{{background:#f97316;color:#111827;border:0;border-radius:8px;padding:10px 18px;
    font-size:15px;font-weight:bold;cursor:pointer;margin:0 0 20px;}}
</style></head>
<body><main>
  <h1>{html.escape(title)}</h1>
  <p>{html.escape(message)}</p>
  {form}
  <p><a href="{html.escape(APP_BASE_URL, quote=True)}">Deschide WeatherForMoto</a></p>
</main></body></html>"""
    return HTMLResponse(
        content=page,
        status_code=status_code,
        # The request URL carries a secret token: never cache it, never leak it
        # through the Referer header when the user follows the link to the app.
        # The page needs no scripts: lock it down to inline styles and a
        # same-origin form (this overrides the app-wide baseline CSP).
        headers={
            "Cache-Control": "no-store",
            "Referrer-Policy": "no-referrer",
            "Content-Security-Policy": (
                "default-src 'none'; style-src 'unsafe-inline'; form-action 'self'; "
                "base-uri 'none'; frame-ancestors 'none'"
            ),
        },
    )


# ---------------------------------------------------------------------------
# User helpers: verification state, tokens
# ---------------------------------------------------------------------------

def _email_verification_supported(conn: _TursoConn) -> bool:
    """False until the migration added users.email_verified and the token table."""
    return _has_column(conn, "users", "email_verified") and _has_column(
        conn, "email_verification_tokens", "token_hash"
    )


def _is_email_verified(conn: _TursoConn, user_id: int) -> bool:
    """Before the migration every account counts as verified (legacy behaviour)."""
    if not _has_column(conn, "users", "email_verified"):
        return True
    row = conn.execute("SELECT email_verified FROM users WHERE id = ?", (user_id,)).fetchone()
    return bool(row and row["email_verified"])


def _create_verification_token(conn: _TursoConn, user_id: int, email: str) -> str:
    """Replace any pending token for the user with a new one; returns the raw token."""
    raw = secrets.token_urlsafe(32)
    now = _utc_now()
    expires = now + timedelta(hours=EMAIL_VERIFY_TTL_HOURS)
    conn.execute("DELETE FROM email_verification_tokens WHERE user_id = ?", (user_id,))
    conn.execute(
        "INSERT INTO email_verification_tokens(user_id, email, token_hash, expires_at, created_at) "
        "VALUES (?, ?, ?, ?, ?)",
        (user_id, email, _hash_token(raw), expires.isoformat(), now.isoformat()),
    )
    conn.commit()
    return raw


def _evict_other_holders(conn: _TursoConn, user_id: int) -> None:
    """The address owner just proved control of an account that was still
    unverified. Whoever registered it first (possibly a squatter) loses access:
    password cleared, all sessions and push devices removed, pending tokens
    dropped. The caller issues the owner's new session afterwards."""
    statements: list[tuple[str, Sequence[Any]]] = [
        ("UPDATE users SET password_hash = NULL, email_verified = 1 WHERE id = ?", (user_id,)),
        ("DELETE FROM sessions WHERE user_id = ?", (user_id,)),
        ("DELETE FROM push_subscriptions WHERE user_id = ?", (user_id,)),
        ("DELETE FROM password_reset_tokens WHERE user_id = ?", (user_id,)),
        ("DELETE FROM email_verification_tokens WHERE user_id = ?", (user_id,)),
    ]
    _execute_atomically(conn, statements)


def _is_unique_violation(exc: Exception) -> bool:
    return "unique" in str(exc).lower()


def _get_or_create_user(conn: _TursoConn, email: str) -> int:
    row = conn.execute("SELECT id FROM users WHERE email = ?", (email.lower(),)).fetchone()
    if row:
        return int(row["id"])
    now = _utc_now().isoformat()
    cur = conn.execute(
        "INSERT INTO users(email, created_at, display_name) VALUES (?, ?, ?)",
        (email.lower(), now, None),
    )
    conn.commit()
    return int(cur.lastrowid)


def _upsert_default_prefs(conn: _TursoConn, user_id: int) -> None:
    now = _utc_now().isoformat()
    conn.execute(
        """
        INSERT INTO alert_prefs(
            user_id, enabled,
            min_score, max_wind_gust, max_precip, max_rain_probability, min_temp, max_temp,
            frost_risk_enabled, quiet_hours_enabled, quiet_start_hour, quiet_end_hour, severity, updated_at
        )
        VALUES (?, 1, 45, 50, 2, 70, NULL, NULL, 1, 0, 22, 7, 'medium', ?)
        ON CONFLICT(user_id) DO NOTHING
        """,
        (user_id, now),
    )
    conn.commit()


def get_current_user(authorization: str | None = Header(default=None)) -> SessionUser:
    # Plain def: FastAPI runs sync dependencies in its threadpool.
    token = _bearer_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token_hash = _hash_token(token)
    conn = _connect()
    try:
        row = conn.execute(
            """
            SELECT s.user_id, s.expires_at, u.email
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token_hash = ?
            """,
            (token_hash,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Invalid token")
        if datetime.fromisoformat(row["expires_at"]) < _utc_now():
            raise HTTPException(status_code=401, detail="Token expired")
        return SessionUser(user_id=row["user_id"], email=row["email"])
    finally:
        conn.close()


def _revoke_other_sessions(conn: _TursoConn, user_id: int, authorization: str | None) -> None:
    """Delete every session of the user except the one making this request."""
    token = _bearer_token(authorization)
    if token:
        conn.execute(
            "DELETE FROM sessions WHERE user_id = ? AND token_hash != ?",
            (user_id, _hash_token(token)),
        )
    else:
        conn.execute("DELETE FROM sessions WHERE user_id = ?", (user_id,))


def _check_current_password(conn: _TursoConn, user_id: int, password: str, detail: str, status_code: int) -> None:
    """Verify the user's current password with a per-user failure counter, so a
    stolen session cannot be used to brute-force the password."""
    bucket = f"pwcheck:user:{user_id}"
    _enforce_limit(conn, bucket, PASSWORD_CHECK_RATE_MAX, LOGIN_RATE_WINDOW_SEC, _TOO_MANY_ATTEMPTS)
    row = conn.execute("SELECT password_hash FROM users WHERE id = ?", (user_id,)).fetchone()
    if not _verify_password(password, row["password_hash"] if row else None):
        raise HTTPException(status_code=status_code, detail=detail)
    _rate_reset(conn, bucket)

# ---------------------------------------------------------------------------
# Authentication endpoints
# ---------------------------------------------------------------------------

@router.get("/push/public-key")
def push_public_key() -> dict[str, str]:
    if not VAPID_PUBLIC_KEY:
        raise HTTPException(status_code=503, detail="VAPID key not configured")
    return {"publicKey": VAPID_PUBLIC_KEY}


@router.post("/auth/request-code")
def auth_request_code(payload: RequestCodePayload, request: Request) -> dict[str, Any]:
    email = payload.email.lower()
    code = f"{secrets.randbelow(1000000):06d}"
    now = _utc_now()
    expires = now + timedelta(minutes=AUTH_CODE_TTL_MIN)

    conn = _connect()
    try:
        # Throttle code requests to curb email bombing and account enumeration.
        _enforce_limit(
            conn, f"reqcode:ip:{_rate_ip(request)}", REQCODE_RATE_MAX_IP, REQCODE_RATE_WINDOW_SEC,
            _TOO_MANY_REQUESTS, request,
        )
        _enforce_limit(
            conn, f"reqcode:e:{_email_key(email)}", REQCODE_RATE_MAX, REQCODE_RATE_WINDOW_SEC,
            _TOO_MANY_REQUESTS, request,
        )

        conn.execute("DELETE FROM auth_codes WHERE email = ?", (email,))
        # Store only a hash of the code at rest — a DB leak must not yield live codes.
        conn.execute(
            "INSERT INTO auth_codes(email, code, expires_at, created_at) VALUES (?, ?, ?, ?)",
            (email, _hash_code(code), expires.isoformat(), now.isoformat()),
        )
        conn.commit()
    finally:
        conn.close()

    email_sent = False
    try:
        _send_auth_email(email, code)
        email_sent = True
    except Exception as exc:
        logger.warning("Could not send auth email to %s: %s", _mask_email(email), exc)

    response: dict[str, Any] = {"ok": True, "message": "Cod trimis. Verifică emailul."}
    # Dev-only fallback: NEVER expose the code in production, regardless of email state.
    if ALLOW_INSECURE_AUTH_CODE and APP_ENV != "production" and (not SMTP_HOST or not email_sent):
        response["dev_code"] = code
        response["message"] = "Cod generat (fallback development)."
    return response


def _claim_account_by_code(conn: _TursoConn, email: str) -> int:
    """Resolve the account for a successful code login. The code proves control
    of the address, so an unverified account with that address is taken over
    by the owner and anyone else holding it is evicted."""
    row = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if not row:
        # New accounts get email_verified from the column default (1).
        return _get_or_create_user(conn, email)
    user_id = int(row["id"])
    if _email_verification_supported(conn) and not _is_email_verified(conn, user_id):
        _evict_other_holders(conn, user_id)
        logger.warning(
            "verify-code: unverified account for %s claimed by the address owner; "
            "password cleared, sessions and push devices revoked",
            _mask_email(email),
        )
    return user_id


def _code_matches(stored_hash: str, supplied_code: str) -> bool:
    # Codes are stored hashed: compare hash to hash, timing-safe.
    return hmac.compare_digest(str(stored_hash), _hash_code(supplied_code.strip()))


def _consume_code(conn: _TursoConn, code_id: int) -> bool:
    """Delete the code and report whether THIS call removed it. Only the
    request that gets the row back may log in, so two concurrent requests with
    the same correct code can never both obtain a session."""
    row = conn.execute("DELETE FROM auth_codes WHERE id = ? RETURNING id", (code_id,)).fetchone()
    conn.commit()
    return row is not None


_CODE_EXHAUSTED = "Prea multe încercări. Solicită un cod nou."


@router.post("/auth/verify-code")
def auth_verify_code(payload: VerifyCodePayload, request: Request) -> dict[str, Any]:
    email = payload.email.lower()
    ip_key = _rate_ip(request)
    window = AUTH_CODE_TTL_MIN * 60
    ip_bucket = f"verify:ip:{ip_key}"
    pair_bucket = f"verify:e:{_email_key(email)}:ip:{ip_key}"
    day_bucket = f"verify:day:e:{_email_key(email)}"
    conn = _connect()
    try:
        # Attempts are reserved up front (atomic) and refunded on success, so the
        # counters effectively hold failures only. The per-(email, IP) bucket means
        # an attacker's guesses never lock the owner out from their own network.
        _enforce_limit(conn, ip_bucket, VERIFY_RATE_MAX_IP, window, _TOO_MANY_ATTEMPTS, request)
        _enforce_limit(conn, pair_bucket, CODE_MAX_ATTEMPTS, window, _TOO_MANY_ATTEMPTS, request)
        # Per-address daily cap across all IPs. It only blocks CODE login;
        # password login and password reset stay available to the owner.
        _enforce_limit(
            conn, day_bucket, CODE_FAILURES_PER_EMAIL_DAY, 86400,
            "Prea multe coduri greșite azi pentru această adresă. Autentifică-te cu parola sau încearcă mâine.",
            request,
        )

        row = conn.execute(
            "SELECT id, code, expires_at FROM auth_codes WHERE email = ? ORDER BY id DESC LIMIT 1",
            (email,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=400, detail="No code requested")

        expires_at = datetime.fromisoformat(row["expires_at"])
        if expires_at < _utc_now():
            raise HTTPException(status_code=400, detail="Code expired")

        # Reserve this guess against the code's total budget BEFORE comparing, so
        # concurrent requests can never compare more than CODE_MAX_FAILURES times.
        # A wrong guess leaves the code alive until the budget is used up.
        code_bucket = f"verify:code:{row['id']}"
        attempt = _rate_hit_count(conn, code_bucket, window)
        if attempt > CODE_MAX_FAILURES:
            _consume_code(conn, row["id"])
            raise HTTPException(status_code=429, detail=_CODE_EXHAUSTED)
        if not _code_matches(row["code"], payload.code):
            if attempt >= CODE_MAX_FAILURES:
                _consume_code(conn, row["id"])
                raise HTTPException(status_code=429, detail=_CODE_EXHAUSTED)
            raise HTTPException(status_code=400, detail="Invalid code")
        if not _consume_code(conn, row["id"]):
            raise HTTPException(status_code=400, detail="Code already used")

        _rate_undo(conn, ip_bucket)
        _rate_reset(conn, pair_bucket)
        _rate_undo(conn, day_bucket)
        _rate_reset(conn, code_bucket)

        user_id = _claim_account_by_code(conn, email)
        _upsert_default_prefs(conn, user_id)
        token = _issue_session(conn, user_id)

        conn.execute("DELETE FROM auth_codes WHERE email = ?", (email,))
        conn.commit()

        return {
            "token": token,
            "user": {"email": email, "email_verified": True},
            "expiresInDays": SESSION_TTL_DAYS,
        }
    finally:
        conn.close()


@router.post("/auth/signup")
def auth_signup(payload: SignupPayload, request: Request, background_tasks: BackgroundTasks) -> dict[str, Any]:
    conn = _connect()
    try:
        email = payload.email.lower()
        # Throttle to curb signup spam and account enumeration via signup.
        _enforce_limit(conn, f"signup:ip:{_rate_ip(request)}", SIGNUP_RATE_MAX_IP, 3600, _TOO_MANY_ATTEMPTS, request)
        exists = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        if exists:
            raise HTTPException(status_code=409, detail="Account already exists")

        verification = _email_verification_supported(conn)
        now = _utc_now().isoformat()
        pwd_hash = _hash_password(payload.password)
        try:
            if verification:
                cur = conn.execute(
                    "INSERT INTO users(email, created_at, display_name, password_hash, email_verified) "
                    "VALUES (?, ?, ?, ?, 0)",
                    (email, now, payload.display_name or None, pwd_hash),
                )
            else:
                cur = conn.execute(
                    "INSERT INTO users(email, created_at, display_name, password_hash) VALUES (?, ?, ?, ?)",
                    (email, now, payload.display_name or None, pwd_hash),
                )
            conn.commit()
        except Exception as exc:
            if _is_unique_violation(exc):
                raise HTTPException(status_code=409, detail="Account already exists") from exc
            raise
        user_id = int(cur.lastrowid)
        _upsert_default_prefs(conn, user_id)
        # The app expects a session right away; the address is confirmed later
        # through the emailed link.
        token = _issue_session(conn, user_id)
        if verification:
            raw_token = _create_verification_token(conn, user_id, email)
            background_tasks.add_task(_send_verification_email, email, raw_token)
        return {
            "token": token,
            "user": {
                "email": email,
                "display_name": payload.display_name or "",
                "email_verified": not verification,
            },
            "expiresInDays": SESSION_TTL_DAYS,
            "verification_email_sent": verification,
        }
    finally:
        conn.close()


def _note_password_failure(conn: _TursoConn, email: str) -> None:
    """Count failed password logins per address per day, across all IPs.

    Detection only, deliberately not a lockout: a hard cap would let anyone
    lock a victim out of the legacy frontend (which has no code login to fall
    back on), and an artificial delay would park threadpool workers and so
    amplify a flood. The per-(email, IP) and per-IP limits (IPv6 per /64) plus
    the PBKDF2 cost bound the guessing rate; this log line makes a distributed
    attempt visible so it can be acted on."""
    count = _rate_hit_count(conn, f"loginfail:day:e:{_email_key(email)}", 86400)
    if count % PASSWORD_FAILURE_ALERT_DAY == 0:
        logger.warning(
            "password login: %d failed attempts in 24 h for %s (possible distributed guessing)",
            count, _mask_email(email),
        )


def _upgrade_password_hash(conn: _TursoConn, user_id: int, password: str, stored_hash: str) -> str | None:
    """Transparently re-hash with the current iteration count. Compare-and-set,
    so a concurrent password change is never overwritten. Returns the hash now
    stored, or None when the password changed underneath this login."""
    if not _password_needs_rehash(stored_hash):
        return stored_hash
    new_hash = _hash_password(password)
    row = conn.execute(
        "UPDATE users SET password_hash = ? WHERE id = ? AND password_hash = ? RETURNING id",
        (new_hash, user_id, stored_hash),
    ).fetchone()
    conn.commit()
    return new_hash if row else None


@router.post("/auth/login")
def auth_login(payload: LoginPayload, request: Request) -> dict[str, Any]:
    email = payload.email.lower()
    ip_key = _rate_ip(request)
    ip_bucket = f"login:ip:{ip_key}"
    pair_bucket = f"login:e:{_email_key(email)}:ip:{ip_key}"
    invalid = HTTPException(status_code=401, detail="Email sau parolă invalidă")
    conn = _connect()
    try:
        # Reserve-then-refund: failures stay counted, successes are given back.
        _enforce_limit(conn, ip_bucket, LOGIN_RATE_MAX_IP, LOGIN_RATE_WINDOW_SEC, _TOO_MANY_ATTEMPTS, request)
        _enforce_limit(conn, pair_bucket, LOGIN_RATE_MAX, LOGIN_RATE_WINDOW_SEC, _TOO_MANY_ATTEMPTS, request)

        row = conn.execute(
            "SELECT id, email, display_name, password_hash FROM users WHERE email = ?",
            (email,),
        ).fetchone()
        stored_hash = row["password_hash"] if row else None
        # _verify_password burns the same PBKDF2 time for unknown accounts.
        if not _verify_password(payload.password, stored_hash) or not row:
            _note_password_failure(conn, email)
            raise invalid

        _rate_undo(conn, ip_bucket)
        _rate_reset(conn, pair_bucket)

        user_id = int(row["id"])
        current_hash = _upgrade_password_hash(conn, user_id, payload.password, stored_hash)
        if current_hash is None:
            raise invalid
        _upsert_default_prefs(conn, user_id)
        token = _issue_session_if_password(conn, user_id, current_hash)
        if token is None:
            # The password was cleared or replaced while this login was in flight.
            raise invalid
        return {
            "token": token,
            "user": {
                "email": row["email"],
                "display_name": row["display_name"] or "",
                "email_verified": _is_email_verified(conn, user_id),
            },
            "expiresInDays": SESSION_TTL_DAYS,
        }
    finally:
        conn.close()


def _verify_link_invalid_page() -> HTMLResponse:
    return _html_page(
        "Link invalid",
        "Linkul de confirmare nu este valid sau a fost deja folosit. "
        "Autentifică-te în aplicație pentru a primi un link nou.",
        400,
    )


@router.get("/auth/verify-email", include_in_schema=False)
def auth_verify_email_page(token: str = Query(default="", max_length=256)) -> HTMLResponse:
    """Confirmation page only, no state change: mail security scanners open
    every link, and a GET that verified would let a scanner confirm the address
    for whoever registered it. The button POSTs back to this same URL."""
    if not _is_well_formed_token(token):
        return _verify_link_invalid_page()
    return _html_page(
        "Confirmă adresa de email",
        "Apasă butonul doar dacă tu ai creat contul WeatherForMoto; altfel închide pagina și ignoră emailul. "
        "Din motive de securitate, după confirmare te vei autentifica din nou în aplicație.",
        form_action=f"/auth/verify-email?token={token}",
        button_label="Confirm adresa",
    )


@router.post("/auth/verify-email", include_in_schema=False)
def auth_verify_email(token: str = Query(default="", max_length=256)) -> HTMLResponse:
    if not _is_well_formed_token(token):
        return _verify_link_invalid_page()
    conn = _connect()
    try:
        if not _email_verification_supported(conn):
            return _verify_link_invalid_page()
        # Consume first: DELETE ... RETURNING hands the token to exactly one request.
        row = conn.execute(
            "DELETE FROM email_verification_tokens WHERE token_hash = ? RETURNING user_id, email, expires_at",
            (_hash_token(token),),
        ).fetchone()
        conn.commit()
        if not row:
            return _verify_link_invalid_page()
        if row["expires_at"] < _utc_now().isoformat():
            return _html_page(
                "Link expirat",
                "Linkul de confirmare a expirat. Autentifică-te în aplicație pentru a primi unul nou.",
                400,
            )
        user = conn.execute("SELECT email, password_hash FROM users WHERE id = ?", (row["user_id"],)).fetchone()
        if not user or user["email"] != row["email"]:
            # The account moved to another address after this link was sent.
            return _verify_link_invalid_page()

        statements: list[tuple[str, Sequence[Any]]] = [
            ("UPDATE users SET email_verified = 1 WHERE id = ? AND email = ?", (row["user_id"], row["email"])),
            ("DELETE FROM email_verification_tokens WHERE user_id = ?", (row["user_id"],)),
        ]
        # Clicking the link proves control of the mailbox, not that the clicker
        # chose the password: someone else may have registered this address. If a
        # password existed before verification, every session and push device is
        # revoked, so nobody stays signed in on the strength of that password
        # alone. The real owner just logs in again.
        signed_out = bool(user["password_hash"])
        if signed_out:
            statements += [
                ("DELETE FROM sessions WHERE user_id = ?", (row["user_id"],)),
                ("DELETE FROM push_subscriptions WHERE user_id = ?", (row["user_id"],)),
            ]
        _execute_atomically(conn, statements)
        message = f"Adresa {row['email']} a fost confirmată."
        if signed_out:
            message += " Din motive de securitate te-am deconectat de pe toate dispozitivele: autentifică-te din nou."
        return _html_page("Adresă confirmată", message)
    finally:
        conn.close()


@router.post("/me/resend-verification")
def resend_verification(
    background_tasks: BackgroundTasks, user: SessionUser = Depends(get_current_user)
) -> dict[str, Any]:
    conn = _connect()
    try:
        if not _email_verification_supported(conn) or _is_email_verified(conn, user.user_id):
            return {"ok": True, "email_verified": True}
        _enforce_limit(conn, f"resendverify:user:{user.user_id}", RESEND_VERIFY_RATE_MAX, 3600, _TOO_MANY_REQUESTS)
        raw_token = _create_verification_token(conn, user.user_id, user.email)
        background_tasks.add_task(_send_verification_email, user.email, raw_token)
        return {"ok": True, "email_verified": False}
    finally:
        conn.close()


@router.post("/auth/logout")
def auth_logout(authorization: str | None = Header(default=None), user: SessionUser = Depends(get_current_user)) -> dict[str, bool]:
    token = _bearer_token(authorization)
    if not token:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    conn = _connect()
    try:
        conn.execute("DELETE FROM sessions WHERE token_hash = ? AND user_id = ?", (_hash_token(token), user.user_id))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


def _create_reset_token(email: str) -> str | None:
    """Blocking: store a fresh reset token for the account, or None when the
    address has no account or storage failed (logged)."""
    conn = _connect()
    try:
        row = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        if not row:
            return None
        raw_token = secrets.token_urlsafe(32)
        now = _utc_now()
        expires = (now + timedelta(minutes=RESET_TOKEN_TTL_MINUTES)).isoformat()
        _execute_atomically(
            conn,
            [
                ("DELETE FROM password_reset_tokens WHERE user_id = ?", (int(row["id"]),)),
                (
                    "INSERT INTO password_reset_tokens(user_id, token_hash, expires_at, created_at) VALUES (?, ?, ?, ?)",
                    (int(row["id"]), _hash_token(raw_token), expires, now.isoformat()),
                ),
            ],
        )
        return raw_token
    except Exception as exc:
        logger.error("could not create password reset token for %s: %s", _mask_email(email), exc)
        return None
    finally:
        conn.close()


def _issue_password_reset(email: str) -> None:
    """Background task: account lookup, token write and email all happen after
    the response, so neither the reply nor its timing depends on whether the
    address has an account. Failures are logged, never raised."""
    raw_token = _create_reset_token(email)
    if not raw_token:
        return
    reset_link = f"{APP_BASE_URL}/?reset_token={raw_token}"
    text = (
        "Salut,\n\nAi solicitat resetarea parolei.\n\n"
        f"Accesează linkul de mai jos (valabil {RESET_TOKEN_TTL_MINUTES} de minute):\n{reset_link}\n\n"
        "Dacă nu ai solicitat resetarea, ignoră acest email.\n\nEchipa WeatherForMoto"
    )
    _send_email_safely(email, "WeatherForMoto — resetare parolă", text, "password reset", [reset_link])


@router.post("/auth/request-reset")
def auth_request_reset(
    payload: RequestResetPayload, request: Request, background_tasks: BackgroundTasks
) -> dict[str, bool]:
    email = payload.email.lower()
    conn = _connect()
    try:
        # Throttle to prevent reset-email bombing.
        _enforce_limit(
            conn, f"reqreset:ip:{_rate_ip(request)}", REQCODE_RATE_MAX_IP, REQCODE_RATE_WINDOW_SEC,
            _TOO_MANY_REQUESTS, request,
        )
        _enforce_limit(
            conn, f"reqreset:e:{_email_key(email)}", REQCODE_RATE_MAX, REQCODE_RATE_WINDOW_SEC,
            _TOO_MANY_REQUESTS, request,
        )
    finally:
        conn.close()
    background_tasks.add_task(_issue_password_reset, email)
    return {"ok": True}


@router.post("/auth/reset-password")
def auth_reset_password(payload: ResetPasswordPayload) -> dict[str, bool]:
    conn = _connect()
    try:
        token_hash = _hash_token(payload.token)
        now = _utc_now().isoformat()
        row = conn.execute(
            "SELECT user_id, expires_at FROM password_reset_tokens WHERE token_hash = ?",
            (token_hash,),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=400, detail="Link de resetare invalid sau expirat.")
        if row["expires_at"] < now:
            conn.execute("DELETE FROM password_reset_tokens WHERE token_hash = ?", (token_hash,))
            conn.commit()
            raise HTTPException(status_code=400, detail="Linkul de resetare a expirat. Solicită unul nou.")
        user_id = row["user_id"]
        # The link went to the account's current address (email changes drop
        # pending reset tokens), so using it proves ownership of that address.
        was_unverified = _email_verification_supported(conn) and not _is_email_verified(conn, user_id)
        statements: list[tuple[str, Sequence[Any]]] = [
            ("UPDATE users SET password_hash = ? WHERE id = ?", (_hash_password(payload.new_password), user_id)),
            ("DELETE FROM password_reset_tokens WHERE user_id = ?", (user_id,)),
            # Invalidate every existing session so a stolen token dies on reset.
            ("DELETE FROM sessions WHERE user_id = ?", (user_id,)),
        ]
        if was_unverified:
            statements += [
                ("UPDATE users SET email_verified = 1 WHERE id = ?", (user_id,)),
                ("DELETE FROM push_subscriptions WHERE user_id = ?", (user_id,)),
                ("DELETE FROM email_verification_tokens WHERE user_id = ?", (user_id,)),
            ]
        _execute_atomically(conn, statements)
        return {"ok": True}
    finally:
        conn.close()


@router.post("/auth/change-password")
def auth_change_password(payload: ChangePasswordPayload, authorization: str | None = Header(default=None), user: SessionUser = Depends(get_current_user)) -> dict[str, bool]:
    conn = _connect()
    try:
        _check_current_password(conn, user.user_id, payload.current_password, "Parola curentă este incorectă", 401)
        conn.execute(
            "UPDATE users SET password_hash = ? WHERE id = ?",
            (_hash_password(payload.new_password), user.user_id),
        )
        # Invalidate all OTHER sessions (keep the current one) so a stolen token
        # dies when the user changes their password.
        _revoke_other_sessions(conn, user.user_id, authorization)
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Account endpoints
# ---------------------------------------------------------------------------

@router.get("/me")
def me(user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
        profile = conn.execute(
            "SELECT email, created_at, display_name FROM users WHERE id = ?",
            (user.user_id,),
        ).fetchone()
        prefs = conn.execute(
            "SELECT enabled, min_score, max_wind_gust, max_precip, max_rain_probability, min_temp, max_temp, frost_risk_enabled, quiet_hours_enabled, quiet_start_hour, quiet_end_hour, severity, home_lat, home_lon, city, alert_states, moto_type, comfort_temp, wind_tolerance, rain_tolerance FROM alert_prefs WHERE user_id = ?",
            (user.user_id,),
        ).fetchone()
        sub_count = conn.execute(
            "SELECT COUNT(*) AS c FROM push_subscriptions WHERE user_id = ?",
            (user.user_id,),
        ).fetchone()["c"]
        return {
            "email": profile["email"] if profile else user.email,
            "created_at": profile["created_at"] if profile else None,
            "display_name": (profile["display_name"] if profile else None) or "",
            "email_verified": _is_email_verified(conn, user.user_id),
            "prefs": dict(prefs) if prefs else None,
            "pushSubscriptions": int(sub_count),
        }
    finally:
        conn.close()


@router.put("/me/prefs")
def update_prefs(payload: AlertPrefsPayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
        now = _utc_now().isoformat()
        conn.execute(
            """
            INSERT INTO alert_prefs(
                user_id, enabled,
                min_score, max_wind_gust, max_precip, max_rain_probability, min_temp, max_temp,
                frost_risk_enabled, quiet_hours_enabled, quiet_start_hour, quiet_end_hour,
                severity, home_lat, home_lon, city, alert_states,
                moto_type, comfort_temp, wind_tolerance, rain_tolerance, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                enabled = excluded.enabled,
                min_score = excluded.min_score,
                max_wind_gust = excluded.max_wind_gust,
                max_precip = excluded.max_precip,
                max_rain_probability = excluded.max_rain_probability,
                min_temp = excluded.min_temp,
                max_temp = excluded.max_temp,
                frost_risk_enabled = excluded.frost_risk_enabled,
                quiet_hours_enabled = excluded.quiet_hours_enabled,
                quiet_start_hour = excluded.quiet_start_hour,
                quiet_end_hour = excluded.quiet_end_hour,
                severity = excluded.severity,
                home_lat = excluded.home_lat,
                home_lon = excluded.home_lon,
                city = excluded.city,
                alert_states = excluded.alert_states,
                moto_type = excluded.moto_type,
                comfort_temp = excluded.comfort_temp,
                wind_tolerance = excluded.wind_tolerance,
                rain_tolerance = excluded.rain_tolerance,
                updated_at = excluded.updated_at
            """,
            (
                user.user_id,
                int(payload.enabled),
                payload.min_score,
                payload.max_wind_gust,
                payload.max_precip,
                payload.max_rain_probability,
                payload.min_temp,
                payload.max_temp,
                int(payload.frost_risk_enabled),
                int(payload.quiet_hours_enabled),
                payload.quiet_start_hour,
                payload.quiet_end_hour,
                payload.severity,
                payload.home_lat,
                payload.home_lon,
                payload.city,
                payload.alert_states,
                payload.moto_type,
                payload.comfort_temp,
                payload.wind_tolerance,
                payload.rain_tolerance,
                now,
            ),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.post("/me/push-subscriptions")
def upsert_subscription(payload: PushSubscriptionPayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    if not _is_valid_push_endpoint(payload.endpoint):
        raise HTTPException(status_code=400, detail="Invalid push endpoint")

    conn = _connect()
    try:
        now = _utc_now().isoformat()
        conn.execute(
            """
            INSERT INTO push_subscriptions(user_id, endpoint, p256dh, auth, last_seen, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(endpoint) DO UPDATE SET
                user_id = excluded.user_id,
                p256dh = excluded.p256dh,
                auth = excluded.auth,
                last_seen = excluded.last_seen
            """,
            (user.user_id, payload.endpoint, payload.keys.p256dh, payload.keys.auth, now, now),
        )
        # Keep only the most recently seen devices per user.
        conn.execute(
            "DELETE FROM push_subscriptions WHERE user_id = ? AND id NOT IN ("
            "SELECT id FROM push_subscriptions WHERE user_id = ? ORDER BY last_seen DESC, id DESC LIMIT ?)",
            (user.user_id, user.user_id, MAX_PUSH_SUBSCRIPTIONS_PER_USER),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.delete("/me/push-subscriptions")
def delete_subscription(
    endpoint: str = Query(max_length=2048), user: SessionUser = Depends(get_current_user)
) -> dict[str, Any]:
    conn = _connect()
    try:
        conn.execute(
            "DELETE FROM push_subscriptions WHERE user_id = ? AND endpoint = ?",
            (user.user_id, endpoint),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.put("/me/profile")
def update_profile(payload: ProfilePayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
        conn.execute("UPDATE users SET display_name = ? WHERE id = ?", (payload.display_name.strip(), user.user_id))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.put("/me/email")
def change_email(
    payload: ChangeEmailPayload,
    background_tasks: BackgroundTasks,
    authorization: str | None = Header(default=None),
    user: SessionUser = Depends(get_current_user),
) -> dict[str, Any]:
    conn = _connect()
    try:
        _check_current_password(conn, user.user_id, payload.password, "Parolă incorectă.", 400)
        new_email = str(payload.new_email).lower()
        current = conn.execute("SELECT email FROM users WHERE id = ?", (user.user_id,)).fetchone()
        old_email = current["email"] if current else user.email
        if new_email == old_email:
            return {"ok": True, "email": new_email, "email_verified": _is_email_verified(conn, user.user_id)}
        existing = conn.execute("SELECT id FROM users WHERE email = ? AND id != ?", (new_email, user.user_id)).fetchone()
        if existing:
            raise HTTPException(status_code=409, detail="Adresa de email este deja folosită.")

        verification = _email_verification_supported(conn)
        if verification:
            update: tuple[str, Sequence[Any]] = (
                "UPDATE users SET email = ?, email_verified = 0 WHERE id = ?", (new_email, user.user_id)
            )
        else:
            update = ("UPDATE users SET email = ? WHERE id = ?", (new_email, user.user_id))
        statements: list[tuple[str, Sequence[Any]]] = [
            update,
            # A reset link or login code sent to the OLD address must not keep
            # working for the account now bound to the new one.
            ("DELETE FROM password_reset_tokens WHERE user_id = ?", (user.user_id,)),
            ("DELETE FROM auth_codes WHERE email = ?", (old_email,)),
        ]
        try:
            _execute_atomically(conn, statements)
        except Exception as exc:
            if _is_unique_violation(exc):
                raise HTTPException(status_code=409, detail="Adresa de email este deja folosită.") from exc
            raise
        # Identity changed: drop all other sessions, keep the current one.
        _revoke_other_sessions(conn, user.user_id, authorization)
        conn.commit()
        if verification:
            raw_token = _create_verification_token(conn, user.user_id, new_email)
            background_tasks.add_task(_send_verification_email, new_email, raw_token)
        return {"ok": True, "email": new_email, "email_verified": not verification}
    finally:
        conn.close()


@router.delete("/me")
def delete_account(user: SessionUser = Depends(get_current_user)) -> dict[str, bool]:
    conn = _connect()
    try:
        uid = user.user_id
        row = conn.execute("SELECT email FROM users WHERE id = ?", (uid,)).fetchone()
        email = row["email"] if row else user.email
        _ensure_rate_table(conn)
        statements: list[tuple[str, Sequence[Any]]] = [
            ("DELETE FROM sessions WHERE user_id = ?", (uid,)),
            ("DELETE FROM push_subscriptions WHERE user_id = ?", (uid,)),
            ("DELETE FROM alert_events WHERE user_id = ?", (uid,)),
            ("DELETE FROM saved_routes WHERE user_id = ?", (uid,)),
            ("DELETE FROM ride_logs WHERE user_id = ?", (uid,)),
            ("DELETE FROM hazard_reports WHERE user_id = ?", (uid,)),
            ("DELETE FROM alert_prefs WHERE user_id = ?", (uid,)),
            ("DELETE FROM password_reset_tokens WHERE user_id = ?", (uid,)),
            ("DELETE FROM auth_codes WHERE email = ?", (email,)),
            # Buckets keyed by the email (hashed key is hex, so LIKE is safe).
            ("DELETE FROM rate_limits WHERE bucket LIKE ?", (f"%:e:{_email_key(email)}%",)),
            # Buckets keyed by user id, plus raw-email buckets from older releases.
            (
                "DELETE FROM rate_limits WHERE bucket IN (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    f"pwcheck:user:{uid}",
                    f"resendverify:user:{uid}",
                    f"hazard:user:{uid}",
                    f"checknow:user:{uid}",
                    f"login:email:{email}",
                    f"verify:email:{email}",
                    f"reqcode:email:{email}",
                    f"reqreset:email:{email}",
                ),
            ),
        ]
        if _has_column(conn, "email_verification_tokens", "token_hash"):
            statements.append(("DELETE FROM email_verification_tokens WHERE user_id = ?", (uid,)))
        statements.append(("DELETE FROM users WHERE id = ?", (uid,)))
        _execute_atomically(conn, statements)
        return {"ok": True}
    finally:
        conn.close()


@router.get("/me/routes")
def list_saved_routes(user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
        rows = conn.execute(
            "SELECT id, name, stops_json, total_distance_km, created_at FROM saved_routes WHERE user_id = ? ORDER BY id DESC",
            (user.user_id,),
        ).fetchall()
        routes = []
        for r in rows:
            try:
                stops = json.loads(r["stops_json"])
            except ValueError:
                logger.warning("saved route %s has malformed stops_json", r["id"])
                stops = []
            routes.append({
                "id": int(r["id"]),
                "name": r["name"],
                "stops": stops,
                "total_distance_km": r["total_distance_km"],
                "created_at": r["created_at"],
            })
        return {"routes": routes}
    finally:
        conn.close()


@router.post("/me/routes")
def create_saved_route(payload: SavedRoutePayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    cleaned_stops = [s.strip() for s in payload.stops if s and s.strip()]
    if len(cleaned_stops) < 2:
        raise HTTPException(status_code=422, detail="Ruta trebuie să conțină minim 2 opriri")

    conn = _connect()
    try:
        now = _utc_now().isoformat()
        cur = conn.execute(
            "INSERT INTO saved_routes(user_id, name, stops_json, total_distance_km, created_at) VALUES (?, ?, ?, ?, ?)",
            (user.user_id, payload.name.strip(), json.dumps(cleaned_stops, ensure_ascii=False), payload.total_distance_km, now),
        )
        conn.commit()
        return {"ok": True, "route_id": int(cur.lastrowid)}
    finally:
        conn.close()


@router.delete("/me/routes/{route_id}")
def delete_saved_route(route_id: int, user: SessionUser = Depends(get_current_user)) -> dict[str, bool]:
    conn = _connect()
    try:
        conn.execute("DELETE FROM saved_routes WHERE id = ? AND user_id = ?", (route_id, user.user_id))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.post("/me/rides/log")
def log_ride(payload: RideLogPayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
        now = _utc_now().isoformat()
        cur = conn.execute(
            """
            INSERT INTO ride_logs(user_id, route_name, start_city, end_city, distance_km, duration_min, avg_moto_score, max_wind_gust, max_precip, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user.user_id,
                payload.route_name,
                payload.start_city.strip(),
                payload.end_city.strip(),
                payload.distance_km,
                payload.duration_min,
                payload.avg_moto_score,
                payload.max_wind_gust,
                payload.max_precip,
                now,
            ),
        )
        conn.commit()
        return {"ok": True, "ride_id": int(cur.lastrowid)}
    finally:
        conn.close()


@router.get("/me/rides/stats")
def ride_stats(user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
        agg = conn.execute(
            """
            SELECT
              COUNT(*) AS rides,
              COALESCE(SUM(distance_km), 0) AS total_distance_km,
              COALESCE(SUM(duration_min), 0) AS total_duration_min,
              AVG(avg_moto_score) AS avg_score,
              MAX(max_wind_gust) AS peak_wind,
              MAX(max_precip) AS peak_precip
            FROM ride_logs
            WHERE user_id = ?
            """,
            (user.user_id,),
        ).fetchone()
        recent = conn.execute(
            """
            SELECT id, route_name, start_city, end_city, distance_km, duration_min, avg_moto_score, created_at
            FROM ride_logs
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT 12
            """,
            (user.user_id,),
        ).fetchall()
        return {
            "rides": int(agg["rides"] or 0),
            "total_distance_km": round(float(agg["total_distance_km"] or 0), 1),
            "total_duration_min": int(agg["total_duration_min"] or 0),
            "avg_score": round(float(agg["avg_score"]), 1) if agg["avg_score"] is not None else None,
            "peak_wind": round(float(agg["peak_wind"]), 1) if agg["peak_wind"] is not None else None,
            "peak_precip": round(float(agg["peak_precip"]), 1) if agg["peak_precip"] is not None else None,
            "recent": [dict(r) for r in recent],
        }
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Hazards
# ---------------------------------------------------------------------------

# Canonical hazard types (stored value) and accepted spellings (EN + RO,
# diacritics folded). Anything else is stored as "other".
_HAZARD_TYPE_ALIASES = {
    "gravel": "gravel", "pietris": "gravel",
    "ice": "ice", "polei": "ice",
    "flood": "flood", "inundatie": "flood",
    "accident": "accident",
    "animals": "animals", "animale": "animals",
    "roadworks": "roadworks", "lucrari": "roadworks",
    "other": "other", "altul": "other",
}
_KM_PER_DEGREE_LAT = 111.32


def _fold_ascii(value: str) -> str:
    """Lowercase and strip diacritics: 'Pietriș' -> 'pietris'."""
    decomposed = unicodedata.normalize("NFKD", value.strip().lower())
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def _normalize_hazard_type(raw: str) -> str:
    return _HAZARD_TYPE_ALIASES.get(_fold_ascii(raw), "other")


def _bounding_box_filter(lat: float, lon: float, radius_km: float) -> tuple[str, list[float]]:
    """SQL fragment + params selecting rows inside a lat/lon box that contains
    the search circle. Handles the poles and the antimeridian."""
    lat_delta = radius_km / _KM_PER_DEGREE_LAT
    min_lat, max_lat = max(-90.0, lat - lat_delta), min(90.0, lat + lat_delta)
    lat_sql = "lat BETWEEN ? AND ?"
    # The circle is widest in longitude at the box edge nearest a pole.
    cos_edge = math.cos(math.radians(max(abs(min_lat), abs(max_lat))))
    if cos_edge < 0.01:
        return lat_sql, [min_lat, max_lat]
    lon_delta = radius_km / (_KM_PER_DEGREE_LAT * cos_edge)
    if lon_delta >= 180:
        return lat_sql, [min_lat, max_lat]
    min_lon, max_lon = lon - lon_delta, lon + lon_delta
    if min_lon < -180:
        return f"{lat_sql} AND (lon >= ? OR lon <= ?)", [min_lat, max_lat, min_lon + 360, max_lon]
    if max_lon > 180:
        return f"{lat_sql} AND (lon >= ? OR lon <= ?)", [min_lat, max_lat, min_lon, max_lon - 360]
    return f"{lat_sql} AND lon BETWEEN ? AND ?", [min_lat, max_lat, min_lon, max_lon]


@router.post("/hazards")
def report_hazard(payload: HazardPayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
        _enforce_limit(
            conn, f"hazard:user:{user.user_id}", HAZARD_RATE_MAX, 3600,
            "Ai raportat prea multe hazarduri. Încearcă din nou mai târziu.",
        )
        now = _utc_now()
        expires = now + timedelta(hours=payload.ttl_hours)
        cur = conn.execute(
            """
            INSERT INTO hazard_reports(user_id, lat, lon, hazard_type, severity, description, expires_at, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user.user_id,
                payload.lat,
                payload.lon,
                _normalize_hazard_type(payload.hazard_type),
                payload.severity,
                payload.description.strip(),
                expires.isoformat(),
                now.isoformat(),
            ),
        )
        conn.commit()
        return {"ok": True, "hazard_id": int(cur.lastrowid)}
    finally:
        conn.close()


@router.get("/hazards")
def list_hazards(
    lat: float = Query(ge=-90, le=90),
    lon: float = Query(ge=-180, le=180),
    radius_km: float = 80.0,
) -> dict[str, Any]:
    radius_km = max(1.0, min(radius_km, 400.0))
    box_sql, box_params = _bounding_box_filter(lat, lon, radius_km)
    conn = _connect()
    try:
        now = _utc_now().isoformat()
        # Prefilter by bounding box BEFORE the LIMIT, so busy areas elsewhere
        # cannot push nearby reports out of the result.
        rows = conn.execute(
            f"""
            SELECT id, lat, lon, hazard_type, severity, description, created_at, expires_at
            FROM hazard_reports
            WHERE expires_at > ? AND {box_sql}
            ORDER BY id DESC
            LIMIT 300
            """,
            (now, *box_params),
        ).fetchall()

        hazards = []
        for r in rows:
            d = _distance_km(lat, lon, float(r["lat"]), float(r["lon"]))
            if d <= radius_km:
                hazards.append({
                    "id": int(r["id"]),
                    "lat": float(r["lat"]),
                    "lon": float(r["lon"]),
                    "hazard_type": r["hazard_type"],
                    "severity": int(r["severity"]),
                    "description": r["description"],
                    "distance_km": round(d, 1),
                    "created_at": r["created_at"],
                    "expires_at": r["expires_at"],
                })
        return {"hazards": hazards}
    finally:
        conn.close()

# ---------------------------------------------------------------------------
# Alert engine
#
# Every event carries a level taken from the moto-score bands of the weather
# contract: EVITĂ (score < 40), ATENȚIE (40-59), INFO (anything milder).
#   score      level of the hour's moto_label (or its score band)
#   wind       crossing the user's gust threshold = ATENȚIE, >= 70 km/h = EVITĂ
#   rain       rain impact matrix: medium = ATENȚIE, high = EVITĂ
#   temp_low   crossing the user's threshold = ATENȚIE, <= 0 °C = EVITĂ
#   temp_high  crossing the user's threshold = ATENȚIE, >= 38 °C = EVITĂ
#   frost      always EVITĂ (ice on two wheels)
# The stored `severity` preference picks the minimum level delivered:
#   low    -> only EVITĂ-level events
#   medium -> ATENȚIE and EVITĂ (default; matches what users got before)
#   high   -> everything, including INFO-level score alerts
# ---------------------------------------------------------------------------

_LEVEL_RANK = {"INFO": 1, "ATENȚIE": 2, "EVITĂ": 3}
_SEVERITY_MIN_RANK = {"low": 3, "medium": 2, "high": 1}
MOTO_SCORE_EVITA_BELOW = 40
MOTO_SCORE_ATENTIE_BELOW = 60
WIND_GUST_EVITA_KMH = 70.0
TEMP_LOW_EVITA_C = 0.0
TEMP_HIGH_EVITA_C = 38.0

# Rain impact matrix (owner requirement). Rows are probability bands
# (< 30 %, 30-60 %, > 60 %), columns the contract's rain_intensity values.
# Only "medium" and "high" impact produce an alert.
_RAIN_INTENSITIES = ("urme", "slaba", "moderata", "puternica")
_RAIN_IMPACT_MATRIX = (
    ("none", "none", "low", "medium"),
    ("none", "low", "medium", "high"),
    ("low", "medium", "high", "high"),
)
_RAIN_ALERT_IMPACTS = {"medium": "ATENȚIE", "high": "EVITĂ"}
_RAIN_INTENSITY_LABELS = {
    "urme": "urme",
    "slaba": "slabă",
    "moderata": "moderată",
    "puternica": "puternică",
}


def _pref(prefs: Mapping[str, Any], key: str, default: Any) -> Any:
    """Read a preference, tolerating columns that are missing or NULL."""
    value = prefs.get(key) if hasattr(prefs, "get") else None
    return default if value is None else value


def _parse_local_time(value: Any) -> datetime | None:
    """Hourly `time` is local wall-clock time ("YYYY-MM-DDTHH:MM")."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed.replace(tzinfo=None)


def _local_now(weather: Mapping[str, Any], utc_now: datetime | None = None) -> datetime:
    """Current wall-clock time at the forecast location (naive), from the
    contract's utc_offset_seconds, else the IANA timezone, else UTC."""
    now = utc_now or _utc_now()
    offset = weather.get("utc_offset_seconds")
    if isinstance(offset, (int, float)) and not isinstance(offset, bool):
        return (now + timedelta(seconds=offset)).replace(tzinfo=None)
    tz_name = weather.get("timezone")
    if tz_name and tz_name != "auto":
        try:
            from zoneinfo import ZoneInfo
            return now.astimezone(ZoneInfo(str(tz_name))).replace(tzinfo=None)
        except Exception as exc:
            logger.warning("unknown forecast timezone %r, using UTC: %s", tz_name, exc)
    return now.replace(tzinfo=None)


def _hours_in_window(
    hourly: Sequence[Mapping[str, Any]], now_local: datetime, hours: int = ALERT_WINDOW_HOURS
) -> list[Mapping[str, Any]]:
    """Forecast hours from the current local hour up to ``hours`` ahead."""
    start = now_local.replace(minute=0, second=0, microsecond=0)
    end = start + timedelta(hours=hours)
    window = []
    for item in hourly:
        ts = _parse_local_time(item.get("time"))
        if ts is not None and start <= ts < end:
            window.append(item)
    return window


def _when_label(when: str, now_local: datetime) -> str:
    """'17:00' for today, '03:00 (mâine)' for tomorrow."""
    ts = _parse_local_time(when)
    if ts is None:
        return str(when)
    label = ts.strftime("%H:%M")
    return label if ts.date() == now_local.date() else f"{label} (mâine)"


def _format_ro(value: float, decimals: int = 1) -> str:
    """Romanian decimal comma: 3.2 -> '3,2'."""
    return f"{value:.{decimals}f}".replace(".", ",")


def _score_level(item: Mapping[str, Any]) -> str:
    label = item.get("moto_label")
    if label in _LEVEL_RANK and label != "INFO":
        return str(label)
    score = item.get("moto_score")
    if score is None:
        return "INFO"
    if score < MOTO_SCORE_EVITA_BELOW:
        return "EVITĂ"
    if score < MOTO_SCORE_ATENTIE_BELOW:
        return "ATENȚIE"
    return "INFO"


def _rain_intensity(item: Mapping[str, Any]) -> str:
    """Contract field; derived from mm/h only if the forecast lacks it."""
    if "rain_intensity" in item:
        return str(item.get("rain_intensity") or "none")
    mm = float(item.get("precipitation_mm") or 0)
    if mm <= 0:
        return "none"
    if mm < 0.5:
        return "urme"
    if mm < 2.5:
        return "slaba"
    if mm < 7.6:
        return "moderata"
    return "puternica"


def _rain_impact(probability: float | None, intensity: str) -> str:
    """Impact from the matrix: 'none' | 'low' | 'medium' | 'high'.
    A missing probability counts as the lowest band."""
    if intensity not in _RAIN_INTENSITIES:
        return "none"
    prob = float(probability or 0)
    band = 0 if prob < 30 else (1 if prob <= 60 else 2)
    return _RAIN_IMPACT_MATRIX[band][_RAIN_INTENSITIES.index(intensity)]


def _event(kind: str, item: Mapping[str, Any], value: Any, title: str, body: str, level: str) -> dict[str, Any]:
    return {
        "type": kind,
        "when": str(item.get("time", "")),
        "value": value,
        "title": title,
        "body": body,
        "level": level,
    }


def _wind_event(hours: Sequence[Mapping[str, Any]], prefs: Mapping[str, Any], now_local: datetime) -> dict[str, Any] | None:
    max_wind = float(_pref(prefs, "max_wind_gust", 50))
    for h in hours:
        gust = h.get("wind_gusts_kmh")
        if gust is not None and gust >= max_wind:
            level = "EVITĂ" if gust >= WIND_GUST_EVITA_KMH else "ATENȚIE"
            body = f"Rafale estimate {round(gust)} km/h la {_when_label(str(h.get('time')), now_local)}"
            return _event("wind", h, gust, "Rafale puternice", body, level)
    return None


def _rain_event(hours: Sequence[Mapping[str, Any]], now_local: datetime) -> dict[str, Any] | None:
    """First run of consecutive hours whose rain impact is medium or high."""
    run: list[tuple[Mapping[str, Any], str]] = []
    for h in hours:
        impact = _rain_impact(h.get("precipitation_probability"), _rain_intensity(h))
        if impact in _RAIN_ALERT_IMPACTS:
            run.append((h, impact))
        elif run:
            break
    if not run:
        return None
    first = run[0][0]
    peak = max(run, key=lambda pair: float(pair[0].get("precipitation_mm") or 0))[0]
    peak_mm = float(peak.get("precipitation_mm") or 0)
    max_prob = max(float(h.get("precipitation_probability") or 0) for h, _ in run)
    level = "EVITĂ" if any(impact == "high" for _, impact in run) else "ATENȚIE"
    intensity_label = _RAIN_INTENSITY_LABELS.get(_rain_intensity(peak), _rain_intensity(peak))
    body = (
        f"{round(max_prob)}% șanse, până la {_format_ro(peak_mm)} mm/h ({intensity_label}) "
        f"de la {_when_label(str(first.get('time')), now_local)}"
    )
    title = "Risc ridicat de ploaie" if level == "EVITĂ" else "Risc de ploaie"
    return _event("rain", first, round(peak_mm, 1), title, body, level)


def _score_event(hours: Sequence[Mapping[str, Any]], prefs: Mapping[str, Any], now_local: datetime) -> dict[str, Any] | None:
    min_score = int(_pref(prefs, "min_score", 45))
    for h in hours:
        score = h.get("moto_score")
        if score is None:
            continue  # no score for this hour: never assume it is fine or bad
        if score <= min_score:
            body = f"Scor estimat {score}/100 la {_when_label(str(h.get('time')), now_local)}"
            return _event("score", h, score, "Scor moto scăzut", body, _score_level(h))
    return None


def _temperature_event(
    hours: Sequence[Mapping[str, Any]], prefs: Mapping[str, Any], now_local: datetime, low: bool
) -> dict[str, Any] | None:
    threshold = _pref(prefs, "min_temp" if low else "max_temp", None)
    if threshold is None:
        return None
    limit = float(threshold)
    for h in hours:
        temp = h.get("temperature")
        if temp is None or (temp > limit if low else temp < limit):
            continue
        when = _when_label(str(h.get("time")), now_local)
        if low:
            level = "EVITĂ" if temp <= TEMP_LOW_EVITA_C else "ATENȚIE"
            return _event("temp_low", h, temp, "Temperatură foarte scăzută",
                          f"Temperatură estimată {_format_ro(temp)}°C la {when}", level)
        level = "EVITĂ" if temp >= TEMP_HIGH_EVITA_C else "ATENȚIE"
        return _event("temp_high", h, temp, "Temperatură foarte ridicată",
                      f"Temperatură estimată {_format_ro(temp)}°C la {when}", level)
    return None


def _hour_has_frost_risk(item: Mapping[str, Any]) -> bool:
    """Contract field frost_risk; without it, fall back to sub-zero air."""
    if "frost_risk" in item:
        return bool(item.get("frost_risk"))
    temp = item.get("temperature")
    return temp is not None and temp <= 0


def _frost_event(hours: Sequence[Mapping[str, Any]], now_local: datetime) -> dict[str, Any] | None:
    for h in hours:
        if _hour_has_frost_risk(h):
            body = f"Risc de polei / carosabil înghețat de la {_when_label(str(h.get('time')), now_local)}"
            return _event("frost", h, h.get("temperature"), "Risc carosabil alunecos", body, "EVITĂ")
    return None


def _build_risk_events(
    hourly: Sequence[Mapping[str, Any]], prefs: Mapping[str, Any], now_local: datetime
) -> list[dict[str, Any]]:
    """At most one event per type: the first matching hour in the next 24 h
    starting at the current local hour. Quiet hours and severity are applied
    at delivery time, not here."""
    hours = _hours_in_window(hourly, now_local)
    candidates = [
        _wind_event(hours, prefs, now_local),
        _rain_event(hours, now_local),
        _score_event(hours, prefs, now_local),
        _temperature_event(hours, prefs, now_local, low=True),
        _temperature_event(hours, prefs, now_local, low=False),
        _frost_event(hours, now_local) if bool(_pref(prefs, "frost_risk_enabled", True)) else None,
    ]
    return [ev for ev in candidates if ev]


def _filter_by_severity(events: Sequence[dict[str, Any]], severity: str | None) -> list[dict[str, Any]]:
    min_rank = _SEVERITY_MIN_RANK.get(str(severity or "medium"), _SEVERITY_MIN_RANK["medium"])
    return [ev for ev in events if _LEVEL_RANK.get(ev.get("level", "INFO"), 1) >= min_rank]


def _in_quiet_hours(local_hour: int, prefs: Mapping[str, Any]) -> bool:
    """Quiet hours gate DELIVERY at the location's local time. start == end
    is treated as "disabled" (an empty interval), never as "always quiet"."""
    if not bool(_pref(prefs, "quiet_hours_enabled", False)):
        return False
    start = int(_pref(prefs, "quiet_start_hour", 22))
    end = int(_pref(prefs, "quiet_end_hour", 7))
    if start == end:
        return False
    if start < end:
        return start <= local_hour < end
    return local_hour >= start or local_hour < end


# How long a push service may hold an alert for an offline device. Alerts stay
# relevant for a few hours, and pywebpush's default TTL of 0 is rejected by
# Windows push (WNS: "Ttl value conflicts with X-WNS-Cache-Policy", HTTP 400).
PUSH_TTL_SECONDS = int(os.getenv("PUSH_TTL_SECONDS", str(6 * 3600)))


def _push_error_detail(exc: Exception) -> str:
    """HTTP status plus the push service's own reason (WNS and APNs send it in headers)."""
    response = getattr(exc, "response", None)
    if response is None:
        return str(exc)
    headers = getattr(response, "headers", None) or {}
    reason = headers.get("X-WNS-ERROR-DESCRIPTION") or headers.get("apns-reason") or (getattr(response, "text", "") or "")[:200]
    return f"HTTP {getattr(response, 'status_code', '?')}: {reason}".strip()


def _send_push(subscription: Mapping[str, Any], title: str, body: str, data: dict[str, Any]) -> None:
    if not (VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY):
        raise RuntimeError("VAPID keys not configured")
    # Imported lazily: pywebpush pulls in requests + cryptography, which only
    # alert dispatch needs, so cold starts of every other endpoint skip it.
    from pywebpush import webpush

    payload = {
        "title": title,
        "body": body,
        "data": data,
        "icon": "/icon-192.png",
        "badge": "/icon-192.png",
        "url": "/",
    }
    sub = {
        "endpoint": subscription["endpoint"],
        "keys": {
            "p256dh": subscription["p256dh"],
            "auth": subscription["auth"],
        },
    }

    webpush(
        subscription_info=sub,
        data=json.dumps(payload),
        vapid_private_key=VAPID_PRIVATE_KEY,
        vapid_claims={"sub": VAPID_SUBJECT},
        ttl=PUSH_TTL_SECONDS,
        timeout=10,
    )


# ---------------------------------------------------------------------------
# Alert dispatch (web push only; email alerts were retired)
# ---------------------------------------------------------------------------

_DISPATCH_PREF_COLUMNS = (
    "enabled, min_score, max_wind_gust, max_precip, max_rain_probability, min_temp, max_temp, "
    "frost_risk_enabled, quiet_hours_enabled, quiet_start_hour, quiet_end_hour, severity, "
    "home_lat, home_lon, city"
)


@dataclass
class _DispatchContext:
    user_id: int
    email: str                   # only for (masked) log lines
    prefs: dict[str, Any]
    lat: float
    lon: float
    city: str
    subscriptions: list[dict[str, Any]]


def _load_dispatch_context(user_id: int, email: str) -> tuple[_DispatchContext | None, str | None]:
    """Blocking: everything dispatch needs from the database, or a reason to skip."""
    conn = _connect()
    try:
        prefs = conn.execute(
            f"SELECT {_DISPATCH_PREF_COLUMNS} FROM alert_prefs WHERE user_id = ?", (user_id,)
        ).fetchone()
        if not prefs or not prefs["enabled"]:
            return None, "alerts_disabled"
        if prefs["home_lat"] is None or prefs["home_lon"] is None:
            return None, "missing_home_location"
        subscriptions = conn.execute(
            "SELECT endpoint, p256dh, auth FROM push_subscriptions WHERE user_id = ?",
            (user_id,),
        ).fetchall()
        return _DispatchContext(
            user_id=user_id,
            email=email,
            prefs=dict(prefs),
            lat=float(prefs["home_lat"]),
            lon=float(prefs["home_lon"]),
            city=prefs["city"] or "Locația mea",
            subscriptions=list(subscriptions),
        ), None
    finally:
        conn.close()


def _claim_event(conn: _TursoConn, user_id: int, event_type: str, event_key: str) -> bool:
    """Insert-first idempotency with the cooldown folded in: ONE conditional
    INSERT creates the row only if this key does not exist yet and no event of
    the same type was created within ALERT_COOLDOWN_HOURS. A single statement is
    atomic, so check-now and dispatch-all (or two instances) cannot both win.
    RETURNING, not rowcount (which libsql may not report like sqlite3), tells
    whether this caller created the row and may deliver."""
    now = _utc_now()
    since = (now - timedelta(hours=ALERT_COOLDOWN_HOURS)).isoformat()
    type_prefix = event_type.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + ":%"
    row = conn.execute(
        "INSERT INTO alert_events(user_id, event_key, created_at) "
        "SELECT ?, ?, ? WHERE NOT EXISTS ("
        "SELECT 1 FROM alert_events WHERE user_id = ? AND event_key LIKE ? ESCAPE '\\' AND created_at >= ?"
        ") ON CONFLICT(user_id, event_key) DO NOTHING RETURNING id",
        (user_id, event_key, now.isoformat(), user_id, type_prefix, since),
    ).fetchone()
    conn.commit()
    return row is not None


def _finish_event_claim(conn: _TursoConn, user_id: int, event_key: str, delivered: bool) -> None:
    """Mark a claimed event delivered, or release the claim so a later run
    retries it when every device failed."""
    try:
        if delivered:
            if _has_column(conn, "alert_events", "delivered_at"):
                conn.execute(
                    "UPDATE alert_events SET delivered_at = ? WHERE user_id = ? AND event_key = ?",
                    (_utc_now().isoformat(), user_id, event_key),
                )
        else:
            conn.execute("DELETE FROM alert_events WHERE user_id = ? AND event_key = ?", (user_id, event_key))
        conn.commit()
    except Exception as exc:
        logger.error("could not finalise alert event %s for user %s: %s", event_key, user_id, exc)


def _push_to_all(conn: _TursoConn, ctx: _DispatchContext, ev: dict[str, Any]) -> int:
    """Send the event to every device; drop subscriptions the push service
    reports as gone. Returns the number of successful sends."""
    sent = 0
    for sub in ctx.subscriptions:
        try:
            _send_push(sub, ev["title"], ev["body"], {"event": ev, "city": ctx.city})
            sent += 1
        except Exception as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status in (404, 410):
                try:
                    conn.execute("DELETE FROM push_subscriptions WHERE endpoint = ?", (sub["endpoint"],))
                    conn.commit()
                    logger.info("removed expired push subscription for user %s", ctx.user_id)
                except Exception as db_exc:
                    logger.warning("could not remove expired push subscription: %s", db_exc)
            else:
                logger.warning("Web push failed for %s: %s", _mask_email(ctx.email), _push_error_detail(exc))
    return sent


def _deliver_event(ctx: _DispatchContext, ev: dict[str, Any]) -> dict[str, Any]:
    """Blocking: claim, push to every device, then mark delivered or release.
    Without a device nothing is claimed, so the event can still go out once
    the user turns push on."""
    if not ctx.subscriptions:
        return {"status": "no_channel", "push": 0}
    event_key = f"{ev['type']}:{ev['when'][:13]}"
    conn = _connect()
    try:
        if not _claim_event(conn, ctx.user_id, ev["type"], event_key):
            return {"status": "duplicate", "push": 0}

        push_sent = _push_to_all(conn, ctx, ev)
        delivered = push_sent > 0
        _finish_event_claim(conn, ctx.user_id, event_key, delivered)
        return {"status": "delivered" if delivered else "failed", "push": push_sent}
    finally:
        conn.close()


async def _dispatch_for_user(user_id: int, email: str, owm_api_key: str) -> dict[str, Any]:
    ctx, reason = await asyncio.to_thread(_load_dispatch_context, user_id, email)
    if ctx is None:
        return {"sent": 0, "delivered": 0, "events": [], "reason": reason}

    weather = await get_weather(ctx.lat, ctx.lon, ctx.city, owm_api_key, forecast_days=2)
    now_local = _local_now(weather)
    events = _filter_by_severity(
        _build_risk_events(weather.get("hourly", []), ctx.prefs, now_local),
        ctx.prefs.get("severity"),
    )
    result: dict[str, Any] = {"sent": 0, "delivered": 0, "events": events}
    if _in_quiet_hours(now_local.hour, ctx.prefs):
        # Nothing is recorded, so still-relevant events go out after quiet hours.
        result["reason"] = "quiet_hours"
        return result

    for ev in events:
        outcome = await asyncio.to_thread(_deliver_event, ctx, ev)
        result["sent"] += outcome["push"]
        result["delivered"] += int(outcome["status"] == "delivered")
    return result


@router.post("/alerts/check-now")
async def alerts_check_now(user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    await asyncio.to_thread(
        _enforce_limit_standalone,
        f"checknow:user:{user.user_id}",
        CHECK_NOW_RATE_MAX,
        3600,
        "Prea multe verificări. Încearcă din nou mai târziu.",
    )
    try:
        result = await _dispatch_for_user(user.user_id, user.email, OWM_API_KEY)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("check-now failed for user %s: %s", user.user_id, exc)
        raise HTTPException(status_code=502, detail="Verificarea alertelor a eșuat temporar.") from exc
    return {"ok": True, **result}


def _secret_matches(provided: str | None, expected: str) -> bool:
    """Constant-time comparison on bytes: compare_digest raises TypeError for
    non-ASCII str, and header values may contain any latin-1 character."""
    if not expected or not provided:
        return False
    return hmac.compare_digest(provided.encode("utf-8"), expected.encode("utf-8"))


def _verify_dispatch_secret(request: Request, provided: str | None) -> None:
    """Blocking: per-IP limit on FAILED secrets, so the scheduler's own calls
    never count but brute force is capped."""
    bucket = f"dispatch:ip:{_rate_ip(request)}"
    conn = _connect()
    try:
        _enforce_limit(conn, bucket, DISPATCH_RATE_MAX_IP, 3600, "Too many requests", request)
        if not _secret_matches(provided, os.getenv("ALERT_DISPATCH_SECRET", "")):
            raise HTTPException(status_code=401, detail="Invalid secret")
        _rate_undo(conn, bucket)
    finally:
        conn.close()


def _purge_stale_rows() -> dict[str, int]:
    """Blocking housekeeping run by dispatch-all. Each statement is independent:
    one failing is logged and does not stop the others or the dispatch."""
    now = _utc_now()
    rate_window = max(86400, LOGIN_RATE_WINDOW_SEC, REQCODE_RATE_WINDOW_SEC, AUTH_CODE_TTL_MIN * 60)
    jobs: list[tuple[str, str, Sequence[Any]]] = [
        ("rate_limits", "DELETE FROM rate_limits WHERE window_start < ?",
         ((now - timedelta(seconds=rate_window)).isoformat(),)),
        ("sessions", "DELETE FROM sessions WHERE expires_at < ?", (now.isoformat(),)),
        ("auth_codes", "DELETE FROM auth_codes WHERE expires_at < ?", (now.isoformat(),)),
        ("password_reset_tokens", "DELETE FROM password_reset_tokens WHERE expires_at < ?", (now.isoformat(),)),
        ("alert_events", "DELETE FROM alert_events WHERE created_at < ?",
         ((now - timedelta(days=ALERT_EVENT_RETENTION_DAYS)).isoformat(),)),
    ]
    purged: dict[str, int] = {}
    conn = _connect()
    try:
        if _has_column(conn, "email_verification_tokens", "token_hash"):
            jobs.append(("email_verification_tokens",
                         "DELETE FROM email_verification_tokens WHERE expires_at < ?", (now.isoformat(),)))
        if _has_column(conn, "alert_events", "delivered_at"):
            # Claims left behind by an instance that died mid-delivery.
            jobs.append(("alert_event_claims",
                         "DELETE FROM alert_events WHERE delivered_at IS NULL AND created_at < ?",
                         ((now - timedelta(minutes=STALE_CLAIM_MINUTES)).isoformat(),)))
        for name, sql, params in jobs:
            try:
                # RETURNING instead of rowcount, which libsql may not report.
                purged[name] = len(conn.execute(f"{sql} RETURNING 1 AS gone", params).fetchall())
                conn.commit()
            except Exception as exc:
                logger.warning("purge of %s failed: %s", name, exc)
    finally:
        conn.close()
    return purged


def _list_alert_users() -> list[dict[str, Any]]:
    """Blocking: only users who can actually receive something, meaning alerts
    on, a home location and at least one push device."""
    conn = _connect()
    try:
        return conn.execute(
            "SELECT u.id, u.email FROM users u JOIN alert_prefs p ON p.user_id = u.id "
            "WHERE p.enabled = 1 AND p.home_lat IS NOT NULL AND p.home_lon IS NOT NULL "
            "AND EXISTS (SELECT 1 FROM push_subscriptions s WHERE s.user_id = u.id)"
        ).fetchall()
    finally:
        conn.close()


@router.post("/alerts/dispatch-all")
async def alerts_dispatch_all(
    request: Request,
    x_dispatch_secret: str | None = Header(default=None, alias="X-Dispatch-Secret"),
) -> dict[str, Any]:
    # The secret is accepted ONLY from the X-Dispatch-Secret header; the old
    # ?secret= query parameter leaked into access logs and is no longer read.
    await asyncio.to_thread(_verify_dispatch_secret, request, x_dispatch_secret)

    purged = await asyncio.to_thread(_purge_stale_rows)
    users = await asyncio.to_thread(_list_alert_users)
    semaphore = asyncio.Semaphore(DISPATCH_CONCURRENCY)

    async def _run(u: Mapping[str, Any]) -> dict[str, Any]:
        async with semaphore:
            return await _dispatch_for_user(int(u["id"]), u["email"], OWM_API_KEY)

    results = await asyncio.gather(*[_run(u) for u in users], return_exceptions=True)
    total_sent = total_events = failed = 0
    for u, r in zip(users, results):
        if isinstance(r, BaseException):
            failed += 1
            logger.warning("dispatch_for_user error for user %s: %s", u["id"], r)
            continue
        total_sent += int(r.get("sent", 0))
        total_events += int(r.get("delivered", 0))
    return {
        "ok": True,
        "users": len(users),
        "sent": total_sent,
        "events": total_events,
        "failed_users": failed,
        "purged": purged,
    }
