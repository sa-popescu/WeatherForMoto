import hashlib
import hmac
import json
import logging
import os
import secrets
import smtplib
import sqlite3
import base64
import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
import httpx
from pydantic import BaseModel, EmailStr, Field
from pywebpush import WebPushException, webpush

from weather_service import get_weather, _haversine_km

logger = logging.getLogger("weatherformoto.auth_alerts")

DB_PATH = os.getenv("APP_DB_PATH", os.path.join(os.path.dirname(__file__), "app.db"))
AUTH_CODE_TTL_MIN = int(os.getenv("AUTH_CODE_TTL_MIN", "10"))
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "30"))
ALLOW_INSECURE_AUTH_CODE = os.getenv("ALLOW_INSECURE_AUTH_CODE", "false").lower() == "true"

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER or "no-reply@motometeo.local")
BREVO_API_KEY = os.getenv("BREVO_API_KEY", "")
OWM_API_KEY = os.getenv("OPENWEATHERMAP_API_KEY", "")
PBKDF2_ITERATIONS = int(os.getenv("PBKDF2_ITERATIONS", "210000"))

VAPID_PUBLIC_KEY = os.getenv("VAPID_PUBLIC_KEY", "")
VAPID_PRIVATE_KEY = os.getenv("VAPID_PRIVATE_KEY", "")
VAPID_SUBJECT = os.getenv("VAPID_SUBJECT", "mailto:admin@motometeo.local")


@dataclass
class SessionUser:
    user_id: int
    email: str


class RequestCodePayload(BaseModel):
    email: EmailStr


class VerifyCodePayload(BaseModel):
    email: EmailStr
    code: str = Field(min_length=4, max_length=12)


class SignupPayload(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)
    display_name: str | None = Field(default=None, max_length=80)


class LoginPayload(BaseModel):
    email: EmailStr
    password: str = Field(min_length=8, max_length=128)


class ProfilePayload(BaseModel):
    display_name: str = Field(min_length=1, max_length=80)


class AlertPrefsPayload(BaseModel):
    enabled: bool = True
    email_alerts_enabled: bool = True
    email_alert_wind: bool = True
    email_alert_rain: bool = True
    email_alert_rain_probability: bool = True
    email_alert_score: bool = True
    email_alert_temp_low: bool = True
    email_alert_temp_high: bool = True
    email_alert_frost: bool = True
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
    city: str | None = None


class PushSubscriptionPayload(BaseModel):
    endpoint: str
    keys: dict[str, str]


class SavedRoutePayload(BaseModel):
    name: str = Field(min_length=2, max_length=80)
    stops: list[str]
    total_distance_km: float | None = Field(default=None, ge=0, le=5000)


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


router = APIRouter(tags=["account", "alerts"])


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    cols = conn.execute(f"PRAGMA table_info({table})").fetchall()
    names = {c[1] for c in cols}
    if column not in names:
        conn.execute(ddl)


def init_db() -> None:
    conn = _connect()
    try:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS auth_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT NOT NULL,
                code TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                token_hash TEXT UNIQUE NOT NULL,
                expires_at TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS alert_prefs (
                user_id INTEGER PRIMARY KEY,
                enabled INTEGER NOT NULL DEFAULT 1,
                email_alerts_enabled INTEGER NOT NULL DEFAULT 1,
                email_alert_wind INTEGER NOT NULL DEFAULT 1,
                email_alert_rain INTEGER NOT NULL DEFAULT 1,
                email_alert_rain_probability INTEGER NOT NULL DEFAULT 1,
                email_alert_score INTEGER NOT NULL DEFAULT 1,
                email_alert_temp_low INTEGER NOT NULL DEFAULT 1,
                email_alert_temp_high INTEGER NOT NULL DEFAULT 1,
                email_alert_frost INTEGER NOT NULL DEFAULT 1,
                min_score INTEGER NOT NULL DEFAULT 45,
                max_wind_gust REAL NOT NULL DEFAULT 50,
                max_precip REAL NOT NULL DEFAULT 2,
                frost_risk_enabled INTEGER NOT NULL DEFAULT 1,
                home_lat REAL,
                home_lon REAL,
                city TEXT,
                updated_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS push_subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                endpoint TEXT UNIQUE NOT NULL,
                p256dh TEXT NOT NULL,
                auth TEXT NOT NULL,
                last_seen TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS alert_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                event_key TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(user_id, event_key)
            );

            CREATE TABLE IF NOT EXISTS saved_routes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                stops_json TEXT NOT NULL,
                total_distance_km REAL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(user_id) REFERENCES users(id)
            );

            CREATE TABLE IF NOT EXISTS ride_logs (
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
            );

            CREATE TABLE IF NOT EXISTS hazard_reports (
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
            );
            """
        )
        _ensure_column(conn, "users", "display_name", "ALTER TABLE users ADD COLUMN display_name TEXT")
        _ensure_column(conn, "users", "password_hash", "ALTER TABLE users ADD COLUMN password_hash TEXT")
        _ensure_column(
            conn,
            "alert_prefs",
            "email_alerts_enabled",
            "ALTER TABLE alert_prefs ADD COLUMN email_alerts_enabled INTEGER NOT NULL DEFAULT 1",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "email_alert_wind",
            "ALTER TABLE alert_prefs ADD COLUMN email_alert_wind INTEGER NOT NULL DEFAULT 1",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "email_alert_rain",
            "ALTER TABLE alert_prefs ADD COLUMN email_alert_rain INTEGER NOT NULL DEFAULT 1",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "email_alert_rain_probability",
            "ALTER TABLE alert_prefs ADD COLUMN email_alert_rain_probability INTEGER NOT NULL DEFAULT 1",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "email_alert_score",
            "ALTER TABLE alert_prefs ADD COLUMN email_alert_score INTEGER NOT NULL DEFAULT 1",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "email_alert_temp_low",
            "ALTER TABLE alert_prefs ADD COLUMN email_alert_temp_low INTEGER NOT NULL DEFAULT 1",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "email_alert_temp_high",
            "ALTER TABLE alert_prefs ADD COLUMN email_alert_temp_high INTEGER NOT NULL DEFAULT 1",
        )
        _ensure_column(
            conn,
            "alert_prefs",
            "email_alert_frost",
            "ALTER TABLE alert_prefs ADD COLUMN email_alert_frost INTEGER NOT NULL DEFAULT 1",
        )
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
        conn.commit()
    finally:
        conn.close()


_distance_km = _haversine_km


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _hash_password(password: str) -> str:
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, PBKDF2_ITERATIONS)
    return f"pbkdf2_sha256${PBKDF2_ITERATIONS}${base64.b64encode(salt).decode()}${base64.b64encode(dk).decode()}"


def _verify_password(password: str, stored: str | None) -> bool:
    if not stored:
        return False
    try:
        algo, iters, salt_b64, hash_b64 = stored.split("$", 3)
        if algo != "pbkdf2_sha256":
            return False
        salt = base64.b64decode(salt_b64.encode())
        expected = base64.b64decode(hash_b64.encode())
        derived = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, int(iters))
        return hmac.compare_digest(derived, expected)
    except Exception:
        return False


def _issue_session(conn: sqlite3.Connection, user_id: int) -> str:
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


async def _send_email(email: str, subject: str, text: str) -> None:

    # Prefer Brevo Email API when configured (more reliable in cloud runtimes).
    if BREVO_API_KEY:
        payload = {
            "sender": {"email": SMTP_FROM},
            "to": [{"email": email}],
            "subject": subject,
            "textContent": text,
        }
        headers = {
            "accept": "application/json",
            "content-type": "application/json",
            "api-key": BREVO_API_KEY,
        }
        async with httpx.AsyncClient(timeout=8) as client:
            resp = await client.post("https://api.brevo.com/v3/smtp/email", json=payload, headers=headers)
            if resp.status_code >= 400:
                raise RuntimeError(f"Brevo API error: HTTP {resp.status_code} - {resp.text[:180]}")
        return

    if not SMTP_HOST:
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = email
    msg.set_content(text)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=8) as server:
        server.starttls()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


async def _send_auth_email(email: str, code: str) -> None:
    text = (
        "Codul tău de autentificare MotoMeteo este: "
        f"{code}\n\nValabil {AUTH_CODE_TTL_MIN} minute."
    )
    await _send_email(email, "MotoMeteo login code", text)


async def get_current_user(authorization: str | None = Header(default=None)) -> SessionUser:
    if not authorization or not authorization.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")

    token = authorization.split(" ", 1)[1].strip()
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


def _get_or_create_user(conn: sqlite3.Connection, email: str) -> int:
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


def _upsert_default_prefs(conn: sqlite3.Connection, user_id: int) -> None:
    now = _utc_now().isoformat()
    conn.execute(
        """
        INSERT INTO alert_prefs(
            user_id, enabled, email_alerts_enabled,
            email_alert_wind, email_alert_rain, email_alert_rain_probability,
            email_alert_score, email_alert_temp_low, email_alert_temp_high, email_alert_frost,
            min_score, max_wind_gust, max_precip, max_rain_probability, min_temp, max_temp,
            frost_risk_enabled, quiet_hours_enabled, quiet_start_hour, quiet_end_hour, severity, updated_at
        )
        VALUES (?, 1, 1, 1, 1, 1, 1, 1, 1, 1, 45, 50, 2, 70, NULL, NULL, 1, 0, 22, 7, 'medium', ?)
        ON CONFLICT(user_id) DO NOTHING
        """,
        (user_id, now),
    )
    conn.commit()


@router.get("/push/public-key")
async def push_public_key() -> dict[str, str]:
    if not VAPID_PUBLIC_KEY:
        raise HTTPException(status_code=503, detail="VAPID key not configured")
    return {"publicKey": VAPID_PUBLIC_KEY}


@router.post("/auth/request-code")
async def auth_request_code(payload: RequestCodePayload) -> dict[str, Any]:
    code = f"{secrets.randbelow(1000000):06d}"
    now = _utc_now()
    expires = now + timedelta(minutes=AUTH_CODE_TTL_MIN)

    conn = _connect()
    try:
        conn.execute("DELETE FROM auth_codes WHERE email = ?", (payload.email.lower(),))
        conn.execute(
            "INSERT INTO auth_codes(email, code, expires_at, created_at) VALUES (?, ?, ?, ?)",
            (payload.email.lower(), code, expires.isoformat(), now.isoformat()),
        )
        conn.commit()
    finally:
        conn.close()

    email_sent = False
    try:
        await _send_auth_email(payload.email.lower(), code)
        email_sent = True
    except Exception as exc:
        logger.warning("Could not send auth email: %s", exc)

    response: dict[str, Any] = {"ok": True, "message": "Cod trimis. Verifică emailul."}
    if (not SMTP_HOST or not email_sent) and ALLOW_INSECURE_AUTH_CODE:
        response["dev_code"] = code
        response["message"] = "Cod generat (fallback development)."
    return response


@router.post("/auth/verify-code")
async def auth_verify_code(payload: VerifyCodePayload) -> dict[str, Any]:
    conn = _connect()
    try:
        row = conn.execute(
            "SELECT code, expires_at FROM auth_codes WHERE email = ? ORDER BY id DESC LIMIT 1",
            (payload.email.lower(),),
        ).fetchone()
        if not row:
            raise HTTPException(status_code=400, detail="No code requested")

        expires_at = datetime.fromisoformat(row["expires_at"])
        if expires_at < _utc_now():
            raise HTTPException(status_code=400, detail="Code expired")

        if not hmac.compare_digest(str(row["code"]), payload.code.strip()):
            raise HTTPException(status_code=400, detail="Invalid code")

        user_id = _get_or_create_user(conn, payload.email.lower())
        _upsert_default_prefs(conn, user_id)
        token = _issue_session(conn, user_id)

        conn.execute("DELETE FROM auth_codes WHERE email = ?", (payload.email.lower(),))
        conn.commit()

        return {
            "token": token,
            "user": {"email": payload.email.lower()},
            "expiresInDays": SESSION_TTL_DAYS,
        }
    finally:
        conn.close()


@router.post("/auth/signup")
async def auth_signup(payload: SignupPayload) -> dict[str, Any]:
    conn = _connect()
    try:
        email = payload.email.lower()
        exists = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
        if exists:
            raise HTTPException(status_code=409, detail="Account already exists")

        now = _utc_now().isoformat()
        pwd_hash = _hash_password(payload.password)
        cur = conn.execute(
            "INSERT INTO users(email, created_at, display_name, password_hash) VALUES (?, ?, ?, ?)",
            (email, now, payload.display_name or None, pwd_hash),
        )
        user_id = int(cur.lastrowid)
        _upsert_default_prefs(conn, user_id)
        token = _issue_session(conn, user_id)
        return {
            "token": token,
            "user": {"email": email, "display_name": payload.display_name or ""},
            "expiresInDays": SESSION_TTL_DAYS,
        }
    finally:
        conn.close()


@router.post("/auth/login")
async def auth_login(payload: LoginPayload) -> dict[str, Any]:
    conn = _connect()
    try:
        email = payload.email.lower()
        row = conn.execute(
            "SELECT id, email, display_name, password_hash FROM users WHERE email = ?",
            (email,),
        ).fetchone()
        if not row or not _verify_password(payload.password, row["password_hash"]):
            raise HTTPException(status_code=401, detail="Email sau parolă invalidă")

        user_id = int(row["id"])
        _upsert_default_prefs(conn, user_id)
        token = _issue_session(conn, user_id)
        return {
            "token": token,
            "user": {"email": row["email"], "display_name": row["display_name"] or ""},
            "expiresInDays": SESSION_TTL_DAYS,
        }
    finally:
        conn.close()


@router.post("/auth/logout")
async def auth_logout(authorization: str | None = Header(default=None), user: SessionUser = Depends(get_current_user)) -> dict[str, bool]:
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization.split(" ", 1)[1].strip()
    token_hash = _hash_token(token)
    conn = _connect()
    try:
        conn.execute("DELETE FROM sessions WHERE token_hash = ? AND user_id = ?", (token_hash, user.user_id))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.get("/me")
async def me(user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
        profile = conn.execute(
            "SELECT email, created_at, display_name FROM users WHERE id = ?",
            (user.user_id,),
        ).fetchone()
        prefs = conn.execute(
            "SELECT enabled, email_alerts_enabled, email_alert_wind, email_alert_rain, email_alert_rain_probability, email_alert_score, email_alert_temp_low, email_alert_temp_high, email_alert_frost, min_score, max_wind_gust, max_precip, max_rain_probability, min_temp, max_temp, frost_risk_enabled, quiet_hours_enabled, quiet_start_hour, quiet_end_hour, severity, home_lat, home_lon, city FROM alert_prefs WHERE user_id = ?",
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
            "prefs": dict(prefs) if prefs else None,
            "pushSubscriptions": int(sub_count),
        }
    finally:
        conn.close()


@router.put("/me/prefs")
async def update_prefs(payload: AlertPrefsPayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
        now = _utc_now().isoformat()
        conn.execute(
            """
            INSERT INTO alert_prefs(
                user_id, enabled, email_alerts_enabled,
                email_alert_wind, email_alert_rain, email_alert_rain_probability,
                email_alert_score, email_alert_temp_low, email_alert_temp_high, email_alert_frost,
                min_score, max_wind_gust, max_precip, max_rain_probability, min_temp, max_temp,
                frost_risk_enabled, quiet_hours_enabled, quiet_start_hour, quiet_end_hour,
                severity, home_lat, home_lon, city, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                enabled = excluded.enabled,
                email_alerts_enabled = excluded.email_alerts_enabled,
                email_alert_wind = excluded.email_alert_wind,
                email_alert_rain = excluded.email_alert_rain,
                email_alert_rain_probability = excluded.email_alert_rain_probability,
                email_alert_score = excluded.email_alert_score,
                email_alert_temp_low = excluded.email_alert_temp_low,
                email_alert_temp_high = excluded.email_alert_temp_high,
                email_alert_frost = excluded.email_alert_frost,
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
                updated_at = excluded.updated_at
            """,
            (
                user.user_id,
                int(payload.enabled),
                int(payload.email_alerts_enabled),
                int(payload.email_alert_wind),
                int(payload.email_alert_rain),
                int(payload.email_alert_rain_probability),
                int(payload.email_alert_score),
                int(payload.email_alert_temp_low),
                int(payload.email_alert_temp_high),
                int(payload.email_alert_frost),
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
                now,
            ),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.post("/me/push-subscriptions")
async def upsert_subscription(payload: PushSubscriptionPayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    p256dh = payload.keys.get("p256dh")
    auth = payload.keys.get("auth")
    if not p256dh or not auth:
        raise HTTPException(status_code=400, detail="Invalid push keys")

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
            (user.user_id, payload.endpoint, p256dh, auth, now, now),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.delete("/me/push-subscriptions")
async def delete_subscription(endpoint: str, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
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
async def update_profile(payload: ProfilePayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
        conn.execute("UPDATE users SET display_name = ? WHERE id = ?", (payload.display_name.strip(), user.user_id))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.post("/me/unsubscribe-email-alerts")
async def unsubscribe_email_alerts(user: SessionUser = Depends(get_current_user)) -> dict[str, bool]:
    conn = _connect()
    try:
        now = _utc_now().isoformat()
        conn.execute(
            """
            INSERT INTO alert_prefs(
                user_id, enabled, email_alerts_enabled,
                email_alert_wind, email_alert_rain, email_alert_rain_probability,
                email_alert_score, email_alert_temp_low, email_alert_temp_high, email_alert_frost,
                min_score, max_wind_gust, max_precip, max_rain_probability, min_temp, max_temp,
                frost_risk_enabled, quiet_hours_enabled, quiet_start_hour, quiet_end_hour, severity, updated_at
            )
            VALUES (?, 1, 0, 0, 0, 0, 0, 0, 0, 0, 45, 50, 2, 70, NULL, NULL, 1, 0, 22, 7, 'medium', ?)
            ON CONFLICT(user_id) DO UPDATE SET email_alerts_enabled = 0, updated_at = excluded.updated_at
            """,
            (user.user_id, now),
        )
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.delete("/me")
async def delete_account(user: SessionUser = Depends(get_current_user)) -> dict[str, bool]:
    conn = _connect()
    try:
        conn.execute("DELETE FROM sessions WHERE user_id = ?", (user.user_id,))
        conn.execute("DELETE FROM push_subscriptions WHERE user_id = ?", (user.user_id,))
        conn.execute("DELETE FROM alert_events WHERE user_id = ?", (user.user_id,))
        conn.execute("DELETE FROM saved_routes WHERE user_id = ?", (user.user_id,))
        conn.execute("DELETE FROM ride_logs WHERE user_id = ?", (user.user_id,))
        conn.execute("DELETE FROM hazard_reports WHERE user_id = ?", (user.user_id,))
        conn.execute("DELETE FROM alert_prefs WHERE user_id = ?", (user.user_id,))
        conn.execute("DELETE FROM users WHERE id = ?", (user.user_id,))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.get("/me/routes")
async def list_saved_routes(user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
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
            except Exception:
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
async def create_saved_route(payload: SavedRoutePayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
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
async def delete_saved_route(route_id: int, user: SessionUser = Depends(get_current_user)) -> dict[str, bool]:
    conn = _connect()
    try:
        conn.execute("DELETE FROM saved_routes WHERE id = ? AND user_id = ?", (route_id, user.user_id))
        conn.commit()
        return {"ok": True}
    finally:
        conn.close()


@router.post("/me/rides/log")
async def log_ride(payload: RideLogPayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
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
async def ride_stats(user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
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


@router.post("/hazards")
async def report_hazard(payload: HazardPayload, user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
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
                payload.hazard_type.strip().lower(),
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
async def list_hazards(lat: float, lon: float, radius_km: float = 80.0) -> dict[str, Any]:
    radius_km = max(1.0, min(radius_km, 400.0))
    conn = _connect()
    try:
        now = _utc_now().isoformat()
        rows = conn.execute(
            """
            SELECT id, lat, lon, hazard_type, severity, description, created_at, expires_at
            FROM hazard_reports
            WHERE expires_at > ?
            ORDER BY id DESC
            LIMIT 300
            """,
            (now,),
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


def _build_risk_events(hourly: list[dict[str, Any]], prefs: sqlite3.Row) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    max_wind = float(prefs["max_wind_gust"])
    max_precip = float(prefs["max_precip"])
    max_rain_prob = int(prefs["max_rain_probability"]) if prefs["max_rain_probability"] is not None else 70
    min_score = int(prefs["min_score"])
    min_temp = prefs["min_temp"]
    max_temp = prefs["max_temp"]
    frost_enabled = bool(prefs["frost_risk_enabled"])
    quiet_enabled = bool(prefs["quiet_hours_enabled"]) if "quiet_hours_enabled" in prefs.keys() else False
    quiet_start = int(prefs["quiet_start_hour"]) if "quiet_start_hour" in prefs.keys() else 22
    quiet_end = int(prefs["quiet_end_hour"]) if "quiet_end_hour" in prefs.keys() else 7

    def _in_quiet_hours(ts: str) -> bool:
        if not quiet_enabled:
            return False
        try:
            hour = datetime.fromisoformat(ts).hour
        except Exception:
            return False
        if quiet_start == quiet_end:
            return True
        if quiet_start < quiet_end:
            return quiet_start <= hour < quiet_end
        return hour >= quiet_start or hour < quiet_end

    def _iter_active_hours() -> list[dict[str, Any]]:
        return [h for h in hourly[:24] if not _in_quiet_hours(str(h.get("time", "")))]

    active_hours = _iter_active_hours()
    if not active_hours:
        active_hours = hourly[:24]

    for h in active_hours:
        time = h.get("time", "")
        gust = h.get("wind_gusts_kmh") or 0

        if gust >= max_wind:
            events.append({"type": "wind", "when": time, "value": gust, "title": "Rafale puternice", "body": f"Rafale estimate {round(gust)} km/h"})
            break

    for h in active_hours:
        time = h.get("time", "")
        precip = h.get("precipitation_mm") or 0
        if precip >= max_precip:
            events.append({"type": "rain", "when": time, "value": precip, "title": "Ploaie puternică", "body": f"Precipitații estimate {round(precip, 1)} mm/h"})
            break

    for h in active_hours:
        time = h.get("time", "")
        rain_prob = h.get("precipitation_probability") or 0
        if rain_prob >= max_rain_prob:
            events.append({"type": "rain_prob", "when": time, "value": rain_prob, "title": "Probabilitate ploaie ridicată", "body": f"Probabilitate estimată {round(rain_prob)}%"})
            break

    for h in active_hours:
        time = h.get("time", "")
        score = h.get("moto_score") if h.get("moto_score") is not None else 100
        if score <= min_score:
            events.append({"type": "score", "when": time, "value": score, "title": "Scor moto scăzut", "body": f"Scor estimat {score}/100"})
            break

    if min_temp is not None:
        for h in active_hours:
            time = h.get("time", "")
            temp = h.get("temperature")
            if temp is not None and temp <= float(min_temp):
                events.append({"type": "temp_low", "when": time, "value": temp, "title": "Temperatură foarte scăzută", "body": f"Temperatură estimată {round(temp, 1)}°C"})
                break

    if max_temp is not None:
        for h in active_hours:
            time = h.get("time", "")
            temp = h.get("temperature")
            if temp is not None and temp >= float(max_temp):
                events.append({"type": "temp_high", "when": time, "value": temp, "title": "Temperatură foarte ridicată", "body": f"Temperatură estimată {round(temp, 1)}°C"})
                break

    if frost_enabled:
        for h in active_hours:
            time = h.get("time", "")
            temp = h.get("temperature")
            precip = h.get("precipitation_mm") or 0
            if temp is not None and temp <= 3 and precip > 0:
                events.append({"type": "frost", "when": time, "value": temp, "title": "Risc carosabil alunecos", "body": "Temperaturi joase cu precipitații în următoarele 24h"})
                break

    return events


def _send_push(subscription: sqlite3.Row, title: str, body: str, data: dict[str, Any]) -> None:
    if not (VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY):
        raise RuntimeError("VAPID keys not configured")

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
    )


def _is_email_event_enabled(prefs: sqlite3.Row, event_type: str) -> bool:
    column_by_event = {
        "wind": "email_alert_wind",
        "rain": "email_alert_rain",
        "rain_prob": "email_alert_rain_probability",
        "score": "email_alert_score",
        "temp_low": "email_alert_temp_low",
        "temp_high": "email_alert_temp_high",
        "frost": "email_alert_frost",
    }
    col = column_by_event.get(event_type)
    if not col:
        return True
    if col not in prefs.keys():
        return True
    return bool(prefs[col])


async def _dispatch_for_user(user_id: int, email: str, owm_api_key: str) -> dict[str, Any]:
    conn = _connect()
    try:
        prefs = conn.execute(
            "SELECT enabled, email_alerts_enabled, email_alert_wind, email_alert_rain, email_alert_rain_probability, email_alert_score, email_alert_temp_low, email_alert_temp_high, email_alert_frost, min_score, max_wind_gust, max_precip, max_rain_probability, min_temp, max_temp, frost_risk_enabled, quiet_hours_enabled, quiet_start_hour, quiet_end_hour, severity, home_lat, home_lon, city FROM alert_prefs WHERE user_id = ?",
            (user_id,),
        ).fetchone()
        if not prefs or not prefs["enabled"]:
            return {"sent": 0, "events": []}

        lat = prefs["home_lat"]
        lon = prefs["home_lon"]
        city = prefs["city"] or "Locația mea"
        if lat is None or lon is None:
            return {"sent": 0, "events": [], "reason": "missing_home_location"}

        subscriptions = conn.execute(
            "SELECT endpoint, p256dh, auth FROM push_subscriptions WHERE user_id = ?",
            (user_id,),
        ).fetchall()
    finally:
        conn.close()

    weather = await get_weather(float(lat), float(lon), city, owm_api_key, forecast_days=2)
    hourly = weather.get("hourly", [])
    events = _build_risk_events(hourly, prefs)

    sent = 0
    email_sent = 0
    email_enabled = bool(prefs["email_alerts_enabled"]) if "email_alerts_enabled" in prefs.keys() else True
    for ev in events:
        event_key = f"{ev['type']}:{ev['when'][:13]}"

        conn = _connect()
        try:
            exists = conn.execute(
                "SELECT 1 FROM alert_events WHERE user_id = ? AND event_key = ?",
                (user_id, event_key),
            ).fetchone()
            if exists:
                conn.close()
                continue

            for sub in subscriptions:
                try:
                    _send_push(sub, ev["title"], ev["body"], {"event": ev, "city": city})
                    sent += 1
                except WebPushException as exc:
                    logger.warning("Web push failed for %s: %s", email, exc)
                except Exception as exc:
                    logger.warning("Push dispatch error for %s: %s", email, exc)

            if email_enabled and _is_email_event_enabled(prefs, ev.get("type", "")):
                try:
                    subject = f"MotoMeteo alertă: {ev['title']}"
                    body = (
                        f"{ev['body']}\n"
                        f"Locație: {city}\n"
                        f"Interval: {ev['when']}\n\n"
                        "Poți dezactiva alertele email din contul tău MotoMeteo."
                    )
                    await _send_email(email, subject, body)
                    email_sent += 1
                except Exception as exc:
                    logger.warning("Email alert dispatch error for %s: %s", email, exc)

            conn.execute(
                "INSERT OR IGNORE INTO alert_events(user_id, event_key, created_at) VALUES (?, ?, ?)",
                (user_id, event_key, _utc_now().isoformat()),
            )
            conn.commit()
        finally:
            conn.close()

    return {"sent": sent, "email_sent": email_sent, "events": events}


@router.post("/alerts/check-now")
async def alerts_check_now(user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    result = await _dispatch_for_user(user.user_id, user.email, OWM_API_KEY)
    return {"ok": True, **result}


@router.post("/alerts/dispatch-all")
async def alerts_dispatch_all(secret: str) -> dict[str, Any]:
    expected = os.getenv("ALERT_DISPATCH_SECRET", "")
    if not expected or not hmac.compare_digest(secret, expected):
        raise HTTPException(status_code=401, detail="Invalid secret")

    conn = _connect()
    try:
        users = conn.execute("SELECT id, email FROM users").fetchall()
    finally:
        conn.close()

    results = await asyncio.gather(
        *[_dispatch_for_user(u["id"], u["email"], OWM_API_KEY) for u in users],
        return_exceptions=True,
    )
    total_sent = 0
    total_events = 0
    for r in results:
        if isinstance(r, Exception):
            logger.warning("dispatch_for_user error: %s", r)
            continue
        total_sent += int(r.get("sent", 0))
        total_events += len(r.get("events", []))
    return {"ok": True, "users": len(users), "sent": total_sent, "events": total_events}
