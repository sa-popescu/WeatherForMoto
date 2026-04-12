import hashlib
import hmac
import logging
import os
import secrets
import smtplib
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from email.message import EmailMessage
from typing import Any

from fastapi import APIRouter, Depends, Header, HTTPException
import httpx
from pydantic import BaseModel, EmailStr, Field
from pywebpush import WebPushException, webpush

from weather_service import get_weather

logger = logging.getLogger("weatherformoto.auth_alerts")

DB_PATH = os.getenv("APP_DB_PATH", os.path.join(os.path.dirname(__file__), "app.db"))
AUTH_CODE_TTL_MIN = int(os.getenv("AUTH_CODE_TTL_MIN", "10"))
SESSION_TTL_DAYS = int(os.getenv("SESSION_TTL_DAYS", "30"))
ALLOW_INSECURE_AUTH_CODE = os.getenv("ALLOW_INSECURE_AUTH_CODE", "true").lower() == "true"

SMTP_HOST = os.getenv("SMTP_HOST")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")
SMTP_FROM = os.getenv("SMTP_FROM", SMTP_USER or "no-reply@motometeo.local")
BREVO_API_KEY = os.getenv("BREVO_API_KEY", "")

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


class AlertPrefsPayload(BaseModel):
    enabled: bool = True
    min_score: int = Field(default=45, ge=0, le=100)
    max_wind_gust: float = Field(default=50, ge=10, le=200)
    max_precip: float = Field(default=2, ge=0, le=50)
    frost_risk_enabled: bool = True
    home_lat: float | None = Field(default=None, ge=-90, le=90)
    home_lon: float | None = Field(default=None, ge=-180, le=180)
    city: str | None = None


class PushSubscriptionPayload(BaseModel):
    endpoint: str
    keys: dict[str, str]


router = APIRouter(tags=["account", "alerts"])


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


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
            """
        )
        conn.commit()
    finally:
        conn.close()


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


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


async def _send_auth_email(email: str, code: str) -> None:
    text = (
        "Codul tău de autentificare MotoMeteo este: "
        f"{code}\n\nValabil {AUTH_CODE_TTL_MIN} minute."
    )

    # Prefer Brevo Email API when configured (more reliable in cloud runtimes).
    if BREVO_API_KEY:
        payload = {
            "sender": {"email": SMTP_FROM},
            "to": [{"email": email}],
            "subject": "MotoMeteo login code",
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
    msg["Subject"] = "MotoMeteo login code"
    msg["From"] = SMTP_FROM
    msg["To"] = email
    msg.set_content(text)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=8) as server:
        server.starttls()
        if SMTP_USER and SMTP_PASS:
            server.login(SMTP_USER, SMTP_PASS)
        server.send_message(msg)


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
        "INSERT INTO users(email, created_at) VALUES (?, ?)",
        (email.lower(), now),
    )
    conn.commit()
    return int(cur.lastrowid)


def _upsert_default_prefs(conn: sqlite3.Connection, user_id: int) -> None:
    now = _utc_now().isoformat()
    conn.execute(
        """
        INSERT INTO alert_prefs(user_id, enabled, min_score, max_wind_gust, max_precip, frost_risk_enabled, updated_at)
        VALUES (?, 1, 45, 50, 2, 1, ?)
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


@router.get("/me")
async def me(user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    conn = _connect()
    try:
        prefs = conn.execute(
            "SELECT enabled, min_score, max_wind_gust, max_precip, frost_risk_enabled, home_lat, home_lon, city FROM alert_prefs WHERE user_id = ?",
            (user.user_id,),
        ).fetchone()
        sub_count = conn.execute(
            "SELECT COUNT(*) AS c FROM push_subscriptions WHERE user_id = ?",
            (user.user_id,),
        ).fetchone()["c"]
        return {
            "email": user.email,
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
            INSERT INTO alert_prefs(user_id, enabled, min_score, max_wind_gust, max_precip, frost_risk_enabled, home_lat, home_lon, city, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                enabled = excluded.enabled,
                min_score = excluded.min_score,
                max_wind_gust = excluded.max_wind_gust,
                max_precip = excluded.max_precip,
                frost_risk_enabled = excluded.frost_risk_enabled,
                home_lat = excluded.home_lat,
                home_lon = excluded.home_lon,
                city = excluded.city,
                updated_at = excluded.updated_at
            """,
            (
                user.user_id,
                int(payload.enabled),
                payload.min_score,
                payload.max_wind_gust,
                payload.max_precip,
                int(payload.frost_risk_enabled),
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


def _build_risk_events(hourly: list[dict[str, Any]], prefs: sqlite3.Row) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    max_wind = float(prefs["max_wind_gust"])
    max_precip = float(prefs["max_precip"])
    min_score = int(prefs["min_score"])
    frost_enabled = bool(prefs["frost_risk_enabled"])

    for h in hourly[:24]:
        time = h.get("time", "")
        temp = h.get("temperature")
        gust = h.get("wind_gusts_kmh") or 0
        precip = h.get("precipitation_mm") or 0
        score = h.get("moto_score") if h.get("moto_score") is not None else 100

        if gust >= max_wind:
            events.append({"type": "wind", "when": time, "value": gust, "title": "Rafale puternice", "body": f"Rafale estimate {round(gust)} km/h"})
            break

    for h in hourly[:24]:
        time = h.get("time", "")
        precip = h.get("precipitation_mm") or 0
        if precip >= max_precip:
            events.append({"type": "rain", "when": time, "value": precip, "title": "Ploaie puternică", "body": f"Precipitații estimate {round(precip, 1)} mm/h"})
            break

    for h in hourly[:24]:
        time = h.get("time", "")
        score = h.get("moto_score") if h.get("moto_score") is not None else 100
        if score <= min_score:
            events.append({"type": "score", "when": time, "value": score, "title": "Scor moto scăzut", "body": f"Scor estimat {score}/100"})
            break

    if frost_enabled:
        for h in hourly[:24]:
            time = h.get("time", "")
            temp = h.get("temperature")
            precip = h.get("precipitation_mm") or 0
            if temp is not None and temp <= 3 and precip > 0:
                events.append({"type": "frost", "when": time, "value": temp, "title": "Risc carosabil alunecos", "body": "Temperaturi joase cu precipitații în următoarele 24h"})
                break

    return events


def _send_push(subscription: sqlite3.Row, title: str, body: str, data: dict[str, Any]) -> None:
    if not (VAPID_PUBLIC_KEY and VAPID_PRIVATE_KEY):
        raise HTTPException(status_code=503, detail="VAPID keys not configured")

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
        data=str(payload).replace("'", '"'),
        vapid_private_key=VAPID_PRIVATE_KEY,
        vapid_claims={"sub": VAPID_SUBJECT},
    )


async def _dispatch_for_user(conn: sqlite3.Connection, user_id: int, email: str, owm_api_key: str) -> dict[str, Any]:
    prefs = conn.execute(
        "SELECT enabled, min_score, max_wind_gust, max_precip, frost_risk_enabled, home_lat, home_lon, city FROM alert_prefs WHERE user_id = ?",
        (user_id,),
    ).fetchone()
    if not prefs or not prefs["enabled"]:
        return {"sent": 0, "events": []}

    lat = prefs["home_lat"]
    lon = prefs["home_lon"]
    city = prefs["city"] or "Locația mea"
    if lat is None or lon is None:
        return {"sent": 0, "events": [], "reason": "missing_home_location"}

    weather = await get_weather(float(lat), float(lon), city, owm_api_key, forecast_days=2)
    hourly = weather.get("hourly", [])
    events = _build_risk_events(hourly, prefs)

    subscriptions = conn.execute(
        "SELECT endpoint, p256dh, auth FROM push_subscriptions WHERE user_id = ?",
        (user_id,),
    ).fetchall()

    sent = 0
    for ev in events:
        event_key = f"{ev['type']}:{ev['when'][:13]}"
        exists = conn.execute(
            "SELECT 1 FROM alert_events WHERE user_id = ? AND event_key = ?",
            (user_id, event_key),
        ).fetchone()
        if exists:
            continue

        for sub in subscriptions:
            try:
                _send_push(sub, ev["title"], ev["body"], {"event": ev, "city": city})
                sent += 1
            except WebPushException as exc:
                logger.warning("Web push failed for %s: %s", email, exc)
            except Exception as exc:
                logger.warning("Push dispatch error for %s: %s", email, exc)

        conn.execute(
            "INSERT OR IGNORE INTO alert_events(user_id, event_key, created_at) VALUES (?, ?, ?)",
            (user_id, event_key, _utc_now().isoformat()),
        )

    conn.commit()
    return {"sent": sent, "events": events}


@router.post("/alerts/check-now")
async def alerts_check_now(user: SessionUser = Depends(get_current_user)) -> dict[str, Any]:
    owm_api_key = os.getenv("OPENWEATHERMAP_API_KEY", "3e17019022d624b5b3d26b54f7c6b8a5")
    conn = _connect()
    try:
        result = await _dispatch_for_user(conn, user.user_id, user.email, owm_api_key)
        return {"ok": True, **result}
    finally:
        conn.close()


@router.post("/alerts/dispatch-all")
async def alerts_dispatch_all(secret: str) -> dict[str, Any]:
    expected = os.getenv("ALERT_DISPATCH_SECRET", "")
    if not expected or not hmac.compare_digest(secret, expected):
        raise HTTPException(status_code=401, detail="Invalid secret")

    owm_api_key = os.getenv("OPENWEATHERMAP_API_KEY", "3e17019022d624b5b3d26b54f7c6b8a5")
    conn = _connect()
    try:
        users = conn.execute("SELECT id, email FROM users").fetchall()
        total_sent = 0
        total_events = 0
        for u in users:
            result = await _dispatch_for_user(conn, u["id"], u["email"], owm_api_key)
            total_sent += int(result.get("sent", 0))
            total_events += len(result.get("events", []))
        return {"ok": True, "users": len(users), "sent": total_sent, "events": total_events}
    finally:
        conn.close()
