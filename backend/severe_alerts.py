"""
Severe weather alerts pushed to the rider: storms, squalls (vijelie), hail,
downpours, strong wind, snow and glaze.

Nothing here invents a warning from one number. An alert goes out only when
a witness that already weighs several sources says so, or two witnesses agree:

- an ANM nowcasting warning drawn over the point: a forecaster's call made
  from radar, satellite and stations, valid for the next hour or two;
- a Meteoalarm (ANM county) warning in orange or red;
- a yellow county warning only when our own forecast for its hours agrees
  (a storm code, strong gusts, real rain, frost), because yellow covers whole
  counties for half a day and most of it stays dry;
- lightning the satellite sees close by, only as a cluster (several flashes
  within 10 km in the last half hour), never one stray flash.

Each warning is sent once (its key carries the warning's id) and the same kind
of warning at the same colour waits out a cooldown, so a storm that ANM
re-issues every half hour does not ring the phone every half hour.
"""

from __future__ import annotations

import hashlib
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from typing import Any

import anm_nowcast
import meteoalarm

# The warnings worth a push for a rider. Heat, fog, fire and the coast are
# shown in the app but only pushed at orange or red.
RIDE_HAZARDS = frozenset({"thunderstorm", "wind", "rain", "snow-ice", "rain-flood", "flood"})
OTHER_PUSHED_AT = frozenset({"orange", "red"})
PUSH_LEVELS_COUNTY = frozenset({"orange", "red"})

LEVEL_WORD = {"yellow": "galben", "orange": "portocaliu", "red": "roșu"}
LEVEL_IMPACT = {"yellow": "ATENȚIE", "orange": "EVITĂ", "red": "EVITĂ"}

# Hours a same-kind, same-colour warning waits after one went out.
NOWCAST_COOLDOWN_H = 2
COUNTY_COOLDOWN_H = 6
LIGHTNING_COOLDOWN_H = 2

# A lightning cluster: this many flashes within mtg_lightning.NEAR_KM, or a
# larger storm within LIGHTNING_WIDE_KM.
LIGHTNING_NEAR_MIN = 3
LIGHTNING_WIDE_MIN = 8
LIGHTNING_WIDE_KM = 20.0

# What our forecast must show in a yellow warning's hours to back it.
BACKING_GUST_KMH = 55.0
BACKING_RAIN_MM = 2.5
BACKING_RAIN_PROB = 60.0
BACKING_STORM_PROB = 40.0
STORM_CODES = frozenset({95, 96, 99})
HAIL_CODES = frozenset({96, 99})

BODY_MAX = 170

# Phenomena named in the warning text, in the order they are read out.
_PHENOMENA: tuple[tuple[str, str], ...] = (
    ("vijelie", "vijelie"),
    ("grindina", "grindină"),
    ("descarcari electrice", "descărcări electrice"),
    ("oraj", "furtună"),
    ("furtuna", "furtună"),
    ("torential", "ploi torențiale"),
    ("averse", "averse"),
    ("cantitati de apa", "cantități mari de apă"),
    ("intensificari", "vânt puternic"),
    ("vant", "vânt puternic"),
    ("viscol", "viscol"),
    ("ninsoare", "ninsoare"),
    ("polei", "polei"),
    ("ceata", "ceață"),
    ("canicula", "caniculă"),
)
_TYPE_WORD = {
    "thunderstorm": "furtună",
    "wind": "vânt puternic",
    "rain": "ploi abundente",
    "rain-flood": "ploi și inundații",
    "flood": "inundații",
    "snow-ice": "ninsoare și polei",
    "fog": "ceață",
    "high-temperature": "caniculă",
    "low-temperature": "ger",
    "forest-fire": "risc de incendiu",
    "wildfire": "risc de incendiu",
    "coastal-event": "fenomene pe litoral",
    "avalanche": "avalanșe",
}


def _parse(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def romania_clock(moment: datetime) -> datetime:
    """Wall-clock time in Romania (the app's warnings are Romanian)."""
    utc = moment.astimezone(timezone.utc)
    return (utc + anm_nowcast._romania_offset(utc)).replace(tzinfo=None)


def _clock_label(moment: datetime, now: datetime) -> str:
    local, today = romania_clock(moment), romania_clock(now)
    label = local.strftime("%H:%M")
    if local.date() == today.date():
        return label
    if local.date() == today.date() + timedelta(days=1):
        return f"mâine {label}"
    return f"{local.day:02d}.{local.month:02d} {label}"


def phenomena(warning: Mapping[str, Any]) -> list[str]:
    """The phenomena the warning names (vijelie, grindină...), at most three."""
    text = meteoalarm.fold(" ".join(str(warning.get(k) or "") for k in ("headline", "event", "description")))
    found: list[str] = []
    for word, label in _PHENOMENA:
        if word in text and label not in found:
            found.append(label)
    if "ploi torențiale" in found and "averse" in found:
        found.remove("averse")
    if not found:
        found.append(_TYPE_WORD.get(str(warning.get("awareness_type") or ""), "fenomene periculoase"))
    return found[:3]


def _join_ro(words: Sequence[str]) -> str:
    return words[0] if len(words) == 1 else ", ".join(words[:-1]) + " și " + words[-1]


def _short(text: str, limit: int = BODY_MAX) -> str:
    text = " ".join(text.split())
    return text if len(text) <= limit else text[: limit - 1].rsplit(" ", 1)[0] + "…"


def _is_nowcast(warning: Mapping[str, Any]) -> bool:
    return str(warning.get("id") or "").startswith("anm-nowcast:")


def _window_hours(
    hourly: Sequence[Mapping[str, Any]], onset: datetime | None, expires: datetime | None,
    utc_offset_s: int, now: datetime,
) -> list[Mapping[str, Any]]:
    """Forecast hours inside the warning (local times read with the forecast's offset)."""
    start = max(onset or now, now) - timedelta(hours=1)
    end = expires or (now + timedelta(hours=12))
    tz = timezone(timedelta(seconds=utc_offset_s))
    picked = []
    for hour in hourly:
        try:
            ts = datetime.fromisoformat(str(hour.get("time"))).replace(tzinfo=tz)
        except ValueError:
            continue
        if start <= ts < end:
            picked.append(hour)
    return picked


def forecast_backs(kind: str | None, hours: Sequence[Mapping[str, Any]]) -> bool:
    """Does our own forecast agree with a yellow warning of this kind?"""
    def num(hour: Mapping[str, Any], key: str) -> float:
        value = hour.get(key)
        return float(value) if isinstance(value, (int, float)) and not isinstance(value, bool) else 0.0

    for h in hours:
        code = h.get("weather_code")
        prob = num(h, "precipitation_probability")
        if kind == "thunderstorm":
            if code in STORM_CODES and (prob >= BACKING_STORM_PROB or h.get("rain_character") == "convective"):
                return True
        elif kind == "wind":
            if num(h, "wind_gusts_kmh") >= BACKING_GUST_KMH:
                return True
        elif kind in ("rain", "rain-flood", "flood"):
            if num(h, "precipitation_mm") >= BACKING_RAIN_MM and prob >= BACKING_RAIN_PROB:
                return True
        elif kind == "snow-ice":
            if h.get("frost_risk") or (h.get("temperature") is not None and num(h, "temperature") <= 1 and prob >= 50):
                return True
    return False


def warning_event(
    warning: Mapping[str, Any], now: datetime, *,
    hourly: Sequence[Mapping[str, Any]] | None = None, utc_offset_s: int = 0,
) -> dict[str, Any] | None:
    """One push for one official warning, or None when it is not worth one."""
    level = str(warning.get("level") or "")
    if level not in LEVEL_WORD:
        return None
    kind = warning.get("awareness_type")
    nowcast = _is_nowcast(warning)
    if kind not in RIDE_HAZARDS and level not in OTHER_PUSHED_AT:
        return None
    expires = _parse(warning.get("expires"))
    onset = _parse(warning.get("onset"))
    if expires is not None and expires <= now:
        return None
    if not nowcast and level not in PUSH_LEVELS_COUNTY:
        # Yellow for the county: only with our forecast behind it.
        if hourly is None or not forecast_backs(kind, _window_hours(hourly, onset, expires, utc_offset_s, now)):
            return None
        # Too far ahead to matter yet; the next run sends it.
        if onset is not None and onset - now > timedelta(hours=12):
            return None

    words = phenomena(warning)
    source = "ANM nowcasting" if nowcast else "ANM"
    title = f"Cod {LEVEL_WORD[level]}: {_join_ro(words)}"
    when = []
    if onset is not None and onset > now + timedelta(minutes=10):
        when.append(f"de la {_clock_label(onset, now)}")
    if expires is not None:
        when.append(f"până la {_clock_label(expires, now)}")
    lead = f"{source}, {' '.join(when)}." if when else f"{source}."
    detail = str(warning.get("headline") or warning.get("description") or "").strip()
    if detail and meteoalarm.fold(detail) not in meteoalarm.fold(title):
        lead = f"{lead} {detail}"
    family = "anm" if nowcast else "county"
    kind_key = str(kind or "other")
    event_type = f"severe-{family}-{kind_key}-{level}"
    digest = hashlib.sha1(str(warning.get("id") or title).encode("utf-8")).hexdigest()[:12]
    return {
        "type": event_type,
        "key": f"{event_type}:{digest}",
        "when": (onset or now).isoformat(),
        "value": level,
        "title": title,
        "body": _short(lead),
        "level": LEVEL_IMPACT[level],
        "severe": True,
        "cooldown_hours": NOWCAST_COOLDOWN_H if nowcast else COUNTY_COOLDOWN_H,
        # A red warning reaches the rider even in quiet hours.
        "bypass_quiet": level == "red",
        "expires": expires.isoformat() if expires else None,
        "url": "/#/acum",
    }


def lightning_event(lightning: Mapping[str, Any] | None, now: datetime,
                    storm: Mapping[str, Any] | None = None) -> dict[str, Any] | None:
    """A storm the satellite sees around the point, as a cluster of flashes.

    The arbiter's storm verdict, when there is one, must agree; without it (the
    quick check between forecasts) the cluster alone has to be dense.
    """
    if not lightning:
        return None
    near = int(lightning.get("near_count") or 0)
    count = int(lightning.get("count") or 0)
    nearest = lightning.get("nearest_km")
    wide = count >= LIGHTNING_WIDE_MIN and nearest is not None and float(nearest) <= LIGHTNING_WIDE_KM
    if near < LIGHTNING_NEAR_MIN and not wide:
        return None
    if storm is not None and storm.get("state") != "storm":
        return None
    where = f"la {round(float(nearest))} km" if nearest is not None else "în apropiere"
    bearing = lightning.get("bearing_deg")
    side = f" spre {_compass_ro(int(bearing))}" if isinstance(bearing, (int, float)) else ""
    minutes = lightning.get("last_minutes_ago")
    ago = f", ultimul acum {int(minutes)} min" if isinstance(minutes, (int, float)) else ""
    body = (f"Satelitul vede {count} fulgere în ultima jumătate de oră, cel mai apropiat {where}{side}{ago}. "
            "Caută un adăpost și evită drumul deschis.")
    # One key per cooldown window, so the same storm rings once.
    slot = now.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    slot = slot.replace(hour=slot.hour - slot.hour % LIGHTNING_COOLDOWN_H)
    return {
        "type": "severe-lightning",
        "key": f"severe-lightning:{slot.isoformat()[:13]}",
        "when": now.isoformat(),
        "value": count,
        "title": "Furtună în apropiere",
        "body": _short(body),
        "level": "EVITĂ",
        "severe": True,
        "cooldown_hours": LIGHTNING_COOLDOWN_H,
        "bypass_quiet": False,
        "expires": (now + timedelta(hours=1)).isoformat(),
        "url": "/#/acum",
    }


_COMPASS_RO = ("nord", "nord-est", "est", "sud-est", "sud", "sud-vest", "vest", "nord-vest")


def _compass_ro(bearing: int) -> str:
    return _COMPASS_RO[int(((bearing % 360) + 22.5) // 45) % 8]


def enabled(prefs: Mapping[str, Any]) -> bool:
    """On unless the rider turned it off: alert_states {"severe": false}."""
    import json

    raw = prefs.get("alert_states") if hasattr(prefs, "get") else None
    if not raw:
        return True
    try:
        states = json.loads(raw)
    except (TypeError, ValueError):
        return True
    return not (isinstance(states, dict) and states.get("severe") is False)


def severe_events(
    alerts: Sequence[Mapping[str, Any]] | None,
    lightning: Mapping[str, Any] | None,
    now: datetime | None = None, *,
    hourly: Sequence[Mapping[str, Any]] | None = None,
    utc_offset_s: int = 0,
    storm: Mapping[str, Any] | None = None,
) -> list[dict[str, Any]]:
    """Every severe push due for one place, worst first, one per warning."""
    moment = now or datetime.now(timezone.utc)
    events: list[dict[str, Any]] = []
    seen: set[str] = set()
    for warning in alerts or []:
        ev = warning_event(warning, moment, hourly=hourly, utc_offset_s=utc_offset_s)
        if ev and ev["key"] not in seen:
            seen.add(ev["key"])
            events.append(ev)
    # A storm already under an ANM thunderstorm nowcast needs no second ring.
    if not any(ev["type"].startswith("severe-anm-thunderstorm") for ev in events):
        ev = lightning_event(lightning, moment, storm)
        if ev:
            events.append(ev)
    rank = {"red": 0, "orange": 1, "yellow": 2}
    events.sort(key=lambda e: (rank.get(str(e["value"]), 1), e["type"]))
    return events
