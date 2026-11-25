"""
The daily forecast push: the next 24 hours at the alert location, told in a
few plain Romanian sentences, sent at the hour the rider picked.

Everything here is pure: it reads the hourly series that get_weather already
fused (rain probability from every witness, gusts and temperature weighed,
asphalt state, moto score) and only puts it into words. Nothing is
re-forecast, so the text never disagrees with what the app shows.

``build_digest`` returns the notification (title, body) and the facts it was
written from. The facts are compact and self-contained, so a language model
can later rewrite them into freer prose without seeing anything else.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import Any

from weather_service import _rain_impact, _rain_intensity

WINDOW_HOURS = 24

# A gust worth a sentence, and one worth a warning (km/h).
GUST_NOTE_KMH = 40.0
GUST_STRONG_KMH = 60.0
# Below this visibility (m) the hour counts as fog.
FOG_VISIBILITY_M = 1000.0
# Feels-like this much below the air temperature is worth saying (°C).
FEELS_GAP_C = 3.0
# Wind from which the cold it adds is named as the cause (km/h).
WINDY_KMH = 10.0
# A rain chance below this, with no rainy hour, is "no rain".
DRY_CHANCE_PCT = 25.0
# Riding hours: the windows for a ride are looked for between these local hours.
RIDE_FIRST_HOUR = 6
RIDE_LAST_HOUR = 21
RIDE_OK_SCORE = 60
RIDE_IDEAL_SCORE = 85
# From this local hour on, the 24 hours ahead are mostly tomorrow.
EVENING_HOUR = 18

_THUNDER_CODES = frozenset({95, 96, 99})
_SNOW_CODES = frozenset({71, 73, 75, 77, 85, 86})
_FREEZING_CODES = frozenset({56, 57, 66, 67})

_INTENSITY_WORDS = {
    "urme": "slabă",
    "slaba": "slabă",
    "moderata": "moderată",
    "puternica": "puternică",
}


@dataclass
class _Hour:
    at: datetime
    raw: Mapping[str, Any]

    def num(self, key: str) -> float | None:
        value = self.raw.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return None
        return float(value)

    @property
    def code(self) -> int | None:
        value = self.num("weather_code")
        return int(value) if value is not None else None


@dataclass
class Digest:
    title: str
    body: str
    facts: dict[str, Any] = field(default_factory=dict)


def _parse(value: Any) -> datetime | None:
    try:
        return datetime.fromisoformat(str(value)[:16])
    except (TypeError, ValueError):
        return None


def _window(hourly: Sequence[Mapping[str, Any]], now_local: datetime) -> list[_Hour]:
    start = now_local.replace(minute=0, second=0, microsecond=0)
    end = start + timedelta(hours=WINDOW_HOURS)
    hours = []
    for item in hourly:
        at = _parse(item.get("time"))
        if at is not None and start <= at < end:
            hours.append(_Hour(at, item))
    hours.sort(key=lambda h: h.at)
    return hours


def _num(value: float) -> str:
    rounded = round(value)
    return f"{'-' if rounded < 0 else ''}{abs(rounded)}"


def _deg(value: float) -> str:
    return f"{_num(value)}°C"


def _clock(at: datetime) -> str:
    return at.strftime("%H:%M")


def _day_word(at: datetime, today: datetime) -> str:
    """The day in front of a time. In the evening the forecast is about
    tomorrow, so tomorrow's hours go bare and tonight's say so."""
    evening = today.hour >= EVENING_HOUR
    if at.date() > today.date():
        return "" if evening else "mâine "
    return "diseară " if evening else ""


def _span(first: datetime, last: datetime, today: datetime) -> str:
    """Words for the hours first..last (inclusive): the end is one hour after the last."""
    end = last + timedelta(hours=1)
    prefix = _day_word(first, today)
    if first == last:
        return f"{prefix}în jurul orei {_clock(first)}"
    return f"{prefix}între {_clock(first)} și {_clock(end)}"


def _at(at: datetime, today: datetime) -> str:
    return f"{_day_word(at, today)}pe la {_clock(at)}"


def _runs(hours: Sequence[_Hour], match, max_gap: int = 1) -> list[list[_Hour]]:
    """Consecutive matching hours, joining runs split by at most ``max_gap`` dry hours."""
    runs: list[list[_Hour]] = []
    for hour in hours:
        if not match(hour):
            continue
        if runs and (hour.at - runs[-1][-1].at) <= timedelta(hours=max_gap + 1):
            runs[-1].append(hour)
        else:
            runs.append([hour])
    return runs


def _wet(hour: _Hour) -> bool:
    mm = hour.num("precipitation_mm")
    prob = hour.num("precipitation_probability") or 0.0
    code = hour.code
    if code in _THUNDER_CODES and prob >= 40:
        return True
    impact = _rain_impact(prob, _rain_intensity(mm))
    return impact in ("medium", "high") or (impact == "low" and prob >= 50)


# ---------------------------------------------------------------------------
# Sentences
# ---------------------------------------------------------------------------

def _temperature(hours: Sequence[_Hour], today: datetime, facts: dict[str, Any]) -> str | None:
    temps = [(h, h.num("temperature")) for h in hours]
    temps = [(h, t) for h, t in temps if t is not None]
    if not temps:
        return None
    low_h, low = min(temps, key=lambda pair: pair[1])
    high_h, high = max(temps, key=lambda pair: pair[1])
    facts["temperature"] = {
        "min": round(low), "min_at": low_h.at.isoformat(timespec="minutes"),
        "max": round(high), "max_at": high_h.at.isoformat(timespec="minutes"),
    }
    if round(low) == round(high):
        sentence = f"Temperatura rămâne în jur de {_deg(high)}."
    else:
        sentence = f"Temperaturi între {_num(low)} și {_deg(high)}, cel mai cald {_at(high_h.at, today)}."

    riding = [(h, t) for h, t in temps if RIDE_FIRST_HOUR <= h.at.hour <= RIDE_LAST_HOUR]
    feels = [(h, h.num("feels_like"), t) for h, t in riding]
    feels = [(h, f, t) for h, f, t in feels if f is not None]
    if feels:
        coldest_h, coldest_feel, air = min(feels, key=lambda triple: triple[1])
        if air - coldest_feel >= FEELS_GAP_C:
            facts["temperature"]["feels_min"] = round(coldest_feel)
            windy = (coldest_h.num("wind_speed_kmh") or 0.0) >= WINDY_KMH
            cause = "Din cauza vântului, " if windy else ""
            when = _at(coldest_h.at, today)
            lead = f"{cause}{when}" if windy else when[0].upper() + when[1:]
            sentence += f" {lead} se simte ca {_deg(coldest_feel)}."
    return sentence


def _precipitation(hours: Sequence[_Hour], today: datetime, facts: dict[str, Any]) -> str:
    runs = _runs(hours, _wet)
    if not runs:
        chances = [(h, h.num("precipitation_probability") or 0.0) for h in hours]
        top_h, top = max(chances, key=lambda pair: pair[1]) if chances else (None, 0.0)
        facts["precipitation"] = {"runs": [], "max_probability": round(top)}
        if top < DRY_CHANCE_PCT or top_h is None:
            return "Nu se anunță precipitații."
        return f"Șanse mici de ploaie, de cel mult {round(top)}%, {_at(top_h.at, today)}."

    described = []
    for run in runs[:2]:
        codes = {h.code for h in run}
        peak = max(run, key=lambda h: h.num("precipitation_mm") or 0.0)
        peak_mm = peak.num("precipitation_mm") or 0.0
        prob = max(h.num("precipitation_probability") or 0.0 for h in run)
        intensity = _INTENSITY_WORDS.get(_rain_intensity(peak_mm), "slabă")
        if codes & _THUNDER_CODES:
            kind = "Averse cu descărcări electrice"
        elif codes & _SNOW_CODES:
            kind = f"Ninsoare {intensity}"
        elif codes & _FREEZING_CODES:
            kind = "Ploaie care îngheață pe carosabil"
        else:
            kind = f"Ploaie {intensity}"
        described.append({
            "kind": kind,
            "from": run[0].at.isoformat(timespec="minutes"),
            "to": (run[-1].at + timedelta(hours=1)).isoformat(timespec="minutes"),
            "probability": round(prob),
            "peak_mm_h": round(peak_mm, 1),
        })
    facts["precipitation"] = {"runs": described, "more_runs": max(0, len(runs) - 2)}

    parts = []
    for run, info in zip(runs[:2], described):
        parts.append(f"{info['kind']} {_span(run[0].at, run[-1].at, today)}, cu {info['probability']}% șanse")
    if len(parts) == 1:
        return parts[0] + "."
    second = parts[1][0].lower() + parts[1][1:]
    return f"{parts[0]}; apoi {second}."


def _wind(hours: Sequence[_Hour], today: datetime, facts: dict[str, Any]) -> str | None:
    gusts = [(h, h.num("wind_gusts_kmh")) for h in hours]
    gusts = [(h, g) for h, g in gusts if g is not None]
    if not gusts:
        return None
    peak_h, peak = max(gusts, key=lambda pair: pair[1])
    facts["wind"] = {"max_gust_kmh": round(peak), "at": peak_h.at.isoformat(timespec="minutes")}
    if peak >= GUST_STRONG_KMH:
        return f"Rafale puternice, de până la {round(peak)} km/h, {_at(peak_h.at, today)}."
    if peak >= GUST_NOTE_KMH:
        return f"Vânt în rafale de până la {round(peak)} km/h {_at(peak_h.at, today)}."
    return None


def _road(hours: Sequence[_Hour], today: datetime, facts: dict[str, Any]) -> list[str]:
    sentences = []
    road: dict[str, Any] = {}
    frost = _runs(hours, lambda h: bool(h.raw.get("frost_risk")), max_gap=0)
    if frost:
        road["frost"] = [frost[0][0].at.isoformat(timespec="minutes"), frost[0][-1].at.isoformat(timespec="minutes")]
        sentences.append(f"Risc de polei {_span(frost[0][0].at, frost[0][-1].at, today)}.")
    fog = _runs(hours, lambda h: (h.num("visibility") or FOG_VISIBILITY_M) < FOG_VISIBILITY_M, max_gap=0)
    if fog:
        road["fog"] = [fog[0][0].at.isoformat(timespec="minutes"), fog[0][-1].at.isoformat(timespec="minutes")]
        sentences.append(f"Ceață {_span(fog[0][0].at, fog[0][-1].at, today)}.")
    # Wet asphalt after the rain has stopped, in riding hours only.
    damp = _runs(
        hours,
        lambda h: h.raw.get("wet_road") == "wet" and not _wet(h) and RIDE_FIRST_HOUR <= h.at.hour <= RIDE_LAST_HOUR,
        max_gap=0,
    )
    if damp and not frost:
        road["wet"] = [damp[0][0].at.isoformat(timespec="minutes"), damp[0][-1].at.isoformat(timespec="minutes")]
        sentences.append(f"Asfaltul rămâne ud {_span(damp[0][0].at, damp[0][-1].at, today)}.")
    if road:
        facts["road"] = road
    return sentences


def _ride(hours: Sequence[_Hour], today: datetime, facts: dict[str, Any]) -> str | None:
    # The day the rider plans: today until the evening, then tomorrow.
    day = (today + timedelta(days=1)).date() if today.hour >= EVENING_HOUR else today.date()
    riding = [
        h for h in hours
        if h.at.date() == day and RIDE_FIRST_HOUR <= h.at.hour <= RIDE_LAST_HOUR and h.num("moto_score") is not None
    ]
    if not riding:
        return None
    best_hour = max(riding, key=lambda h: h.num("moto_score") or 0.0)
    best_score = round(best_hour.num("moto_score") or 0.0)
    good = _runs(riding, lambda h: (h.num("moto_score") or 0.0) >= RIDE_OK_SCORE, max_gap=0)
    if not good:
        facts["ride"] = {"verdict": "poor", "best_score": best_score, "best_at": best_hour.at.isoformat(timespec="minutes")}
        return f"Nu e o zi bună de motor: scorul nu trece de {best_score}/100."

    def quality(run: list[_Hour]) -> tuple[float, int]:
        return (sum(h.num("moto_score") or 0.0 for h in run) / len(run), len(run))

    longest = max(good, key=lambda run: (len(run), quality(run)[0]))
    average = round(quality(longest)[0])
    facts["ride"] = {
        "verdict": "ideal" if average >= RIDE_IDEAL_SCORE else "ok",
        "from": longest[0].at.isoformat(timespec="minutes"),
        "to": (longest[-1].at + timedelta(hours=1)).isoformat(timespec="minutes"),
        "score": average,
    }
    if len(longest) == len(riding) and len(riding) >= 8:
        word = "ideală" if average >= RIDE_IDEAL_SCORE else "bună"
        return f"Zi {word} de motor, toată ziua (scor {average}/100)."
    when = _span(longest[0].at, longest[-1].at, today)
    if len(longest) == 1:
        return f"De mers cu motorul e bine doar {when} (scor {average}/100)."
    word = "Cel mai bun interval" if average >= RIDE_IDEAL_SCORE else "Intervalul bun"
    return f"{word} pentru motor: {when} (scor {average}/100)."


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def _title(city: str, now_local: datetime) -> str:
    if now_local.hour < 12:
        when = "Prognoza zilei"
    elif now_local.hour < EVENING_HOUR:
        when = "Prognoza până mâine"
    else:
        when = "Prognoza pentru mâine"
    return f"{when} · {city}" if city else when


def build_digest(weather: Mapping[str, Any], now_local: datetime, city: str) -> Digest | None:
    """The next 24 hours in a few sentences, or None when there are no hours to tell."""
    hours = _window(weather.get("hourly") or [], now_local)
    if len(hours) < 6:
        return None
    facts: dict[str, Any] = {
        "city": city,
        "from": hours[0].at.isoformat(timespec="minutes"),
        "to": (hours[-1].at + timedelta(hours=1)).isoformat(timespec="minutes"),
    }
    sentences = [
        _temperature(hours, now_local, facts),
        _precipitation(hours, now_local, facts),
        _wind(hours, now_local, facts),
        *_road(hours, now_local, facts),
        _ride(hours, now_local, facts),
    ]
    body = " ".join(s for s in sentences if s)
    return Digest(title=_title(city, now_local), body=body, facts=facts)
