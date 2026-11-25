"""
One verdict on the weather right now, from every witness at once.

The app hears a dozen voices about the present moment: an airport observer, a
WeatherXM or Netatmo gauge, an official station, the radar, the satellite's
lightning, four providers' "current" blocks, the national models and the
hour's fused chance of rain. They disagree all the time, and most false alarms
came from one of them speaking alone: OpenWeatherMap's lone 1 mm under a
CAVOK airport, a radar echo over 52% humidity, a sprinkler in a Netatmo
bucket, one satellite flash on a calm evening.

Instead of a separate rule for each of those, every witness here adds evidence
for or against, on one scale (log-odds, the way a forecaster weighs reports):

- what is observed beats what is modelled: a gauge on the spot or an airport
  observer next door counts for several models;
- evidence fades with distance, and an airport beyond AIRPORT_DRY_KM only
  counts when it sees rain (showers are local, a dry sky 18 km away is not);
- a model counts for more the more it gives (a trace is a hint, 3 mm is a
  claim), and for as much as the verification log has learned to trust it here
  (calibration.py);
- evidence that cannot be physically right is set aside and said so: radar
  rain falling into air too dry to reach the ground.

The sum, started from the hour's fused chance of rain, is a probability. Above
WET_AT the verdict is that it rains (snows, drizzles...), below DRY_AT that it
does not, and in between the sources really disagree, which is said too, and
the forecast is left to speak. Each verdict carries its confidence and one
sentence a rider can read: "Nu plouă: aeroportul Băneasa și 10 din 11 surse
spun uscat."

The functions are pure. weather_service gathers the witnesses, asks for the
verdict and applies it.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Iterable

# The verdict's thresholds, as probabilities that it is falling.
WET_AT = 0.7
DRY_AT = 0.3
# The hour's fused chance is the prior, damped: the models in it also speak below.
PRIOR_SHARE = 0.6
PRIOR_WITHOUT_FORECAST = 0.15
# A measured or modelled rate counts as rain from here (rain_fusion.WET_MM).
WET_MM = 0.1
GAUGE_WET_MM = 0.05
# Airports: full weight up to AIRPORT_DRY_KM; a dry one beyond it says nothing,
# a wet one fades to AIRPORT_FAR_WEIGHT at metar.MAX_DISTANCE_KM.
AIRPORT_DRY_KM = 10.0
AIRPORT_MAX_KM = 20.0
AIRPORT_FAR_WEIGHT = 0.4
# Official stations report the last full hour, often from farther away.
OFFICIAL_MAX_KM = 40.0
# Radar rain over air drier than this rarely lands (virga, false echoes).
RADAR_MIN_HUMIDITY = 65.0

# Evidence, in log-odds, for (wet) and against (dry).
LLR = {
    "weatherxm": (8.0, -3.0),     # a vetted gauge on the spot
    "netatmo": (2.5, -1.5),       # one bucket in someone's yard
    "official": (1.5, -1.0),      # an official gauge, last hour, some km away
    "airport": (3.5, -2.5),       # a trained observer (METAR)
    "radar": (2.0, -1.2),         # the last full hour, seen from above
    "radar_dry_air": (0.3, 0.0),  # radar rain into air too dry for it
    "models": (1.0, -1.0),        # at full agreement of the national models
}
# A provider's current block: a trace is a hint, an amount a claim.
FORECAST_WET_LLR = ((2.0, 1.5), (0.5, 1.0), (0.0, 0.4))  # (from mm, llr)
FORECAST_DRY_LLR = -0.35
# Trust multipliers from the learned weights are kept in this band.
TRUST_MIN, TRUST_MAX = 0.5, 1.5

FOG_CODES = frozenset({45, 48})
STORM_CODES = frozenset({95, 96, 99})
SNOW_CODES = frozenset({71, 73, 75, 77, 85, 86})
ICE_CODES = frozenset({56, 57, 66, 67})
DRIZZLE_CODES = frozenset({51, 53, 55})
PRECIP_MIN_CODE = 51

# Fog: an observed visibility under this is fog, over CLEAR_VIS_M it is not.
FOG_VIS_M = 1000.0
CLEAR_VIS_M = 3000.0
FOG_PRIOR = -1.5
FOG_AT = 0.6

# Lightning (the satellite's flashes) and storms.
STORM_PRIOR = -2.0
STORM_MANY_FLASHES = 2
STORM_AT = 0.5


@dataclass
class Witness:
    """One voice about now: who, what it says, and how much that counts."""

    id: str
    kind: str  # observation | gauge | radar | forecast | models
    name_ro: str
    name_en: str
    wet: bool
    llr: float
    amount_mm: float | None = None
    code: int | None = None
    # A group of sources (the national models) counts as many in the tally.
    count: int = 1
    wet_count: int = 0
    set_aside: str | None = None  # why it was not believed, in Romanian
    set_aside_en: str | None = None

    def as_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "id": self.id,
            "kind": self.kind,
            "says": "wet" if self.wet else "dry",
            "weight": round(self.llr, 2),
        }
        if self.amount_mm is not None:
            out["amount_mm"] = round(self.amount_mm, 2)
        if self.count > 1:
            out["count"] = self.count
            out["wet_count"] = self.wet_count
        if self.set_aside:
            out["set_aside"] = self.set_aside
        return out


@dataclass
class Evidence:
    witnesses: list[Witness] = field(default_factory=list)

    def add(self, w: Witness | None) -> None:
        if w is not None:
            self.witnesses.append(w)


def _sigmoid(x: float) -> float:
    return 1 / (1 + math.exp(-max(-30.0, min(30.0, x))))


def _logit(p: float) -> float:
    p = min(0.98, max(0.02, p))
    return math.log(p / (1 - p))


def _num(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def _trust(name: str, learned: dict[str, float] | None, defaults: dict[str, float] | None) -> float:
    """How much more (or less) the log has learned to trust a source here than by hand."""
    if not learned or not defaults or name not in learned or not defaults.get(name):
        return 1.0
    return max(TRUST_MIN, min(TRUST_MAX, float(learned[name]) / float(defaults[name])))


def airport_name(obs: dict[str, Any] | None) -> str | None:
    """ "Bucharest/Băneasa Intl" -> "Băneasa"; the ICAO code when there is no name."""
    if not obs:
        return None
    name = obs.get("name")
    if isinstance(name, str) and name.strip():
        part = name.split("/")[-1].strip()
        words = [w for w in part.split() if w.lower().strip(".") not in {"intl", "international", "airport", "arpt", "aeroport"}]
        if words:
            return " ".join(words)
    station = obs.get("station")
    return str(station) if station else None


def _fade(distance_km: float | None, full_km: float, max_km: float, floor: float) -> float:
    if distance_km is None or distance_km <= full_km:
        return 1.0
    if distance_km >= max_km:
        return floor
    return 1.0 - (1.0 - floor) * (distance_km - full_km) / (max_km - full_km)


def _forecast_wet_llr(amount: float) -> float:
    for from_mm, llr in FORECAST_WET_LLR:
        if amount >= from_mm:
            return llr
    return FORECAST_WET_LLR[-1][1]


# ---------------------------------------------------------------------------
# Rain, snow, drizzle: is anything falling right now?
# ---------------------------------------------------------------------------

PROVIDER_NAMES = {
    "open-meteo": ("Open-Meteo", "Open-Meteo"),
    "openweathermap": ("OpenWeatherMap", "OpenWeatherMap"),
    "met-norway": ("MET Norway", "MET Norway"),
    "pirate-weather": ("Pirate Weather", "Pirate Weather"),
}


def precipitation_witnesses(
    *,
    providers: dict[str, tuple[float | None, int | None]],
    models: tuple[int, int] | None = None,
    weatherxm_mm: float | None = None,
    netatmo_mm: float | None = None,
    official: dict[str, Any] | None = None,
    airport: dict[str, Any] | None = None,
    radar_mm: float | None = None,
    humidity: float | None = None,
    learned: dict[str, float] | None = None,
    defaults: dict[str, float] | None = None,
) -> list[Witness]:
    """Everyone who says something about rain falling now.

    ``providers`` maps a provider id to (its current rate in mm/h, its WMO
    code); ``models`` is (wet, answered) for the national models this hour.
    """
    ev = Evidence()

    if weatherxm_mm is not None:
        wet = weatherxm_mm >= GAUGE_WET_MM
        ev.add(Witness("weatherxm", "gauge", "stația WeatherXM", "the WeatherXM station", wet,
                       LLR["weatherxm"][0 if wet else 1], amount_mm=weatherxm_mm))
    if netatmo_mm is not None:
        wet = netatmo_mm >= GAUGE_WET_MM
        ev.add(Witness("netatmo", "gauge", "pluviometrul Netatmo", "the Netatmo gauge", wet,
                       LLR["netatmo"][0 if wet else 1], amount_mm=netatmo_mm))

    off_mm = _num((official or {}).get("precipitation"))
    if off_mm is not None:
        wet = off_mm >= WET_MM
        fade = _fade(_num(official.get("distance_km")), 10.0, OFFICIAL_MAX_KM, 0.3)
        ev.add(Witness("official", "gauge", "stația ANM", "the official station", wet,
                       LLR["official"][0 if wet else 1] * fade, amount_mm=off_mm))

    if airport:
        code = airport.get("wmo_code")
        distance = _num(airport.get("distance_km"))
        wet = code is not None and code >= PRECIP_MIN_CODE
        name = airport_name(airport)
        name_ro = f"aeroportul {name}" if name else "aeroportul din apropiere"
        name_en = f"{name} airport" if name else "the nearby airport"
        if wet:
            fade = _fade(distance, AIRPORT_DRY_KM, AIRPORT_MAX_KM, AIRPORT_FAR_WEIGHT)
            ev.add(Witness("airport", "observation", name_ro, name_en, True, LLR["airport"][0] * fade, code=code))
        elif distance is None or distance <= AIRPORT_DRY_KM:
            ev.add(Witness("airport", "observation", name_ro, name_en, False, LLR["airport"][1], code=code))

    if radar_mm is not None:
        wet = radar_mm >= WET_MM
        if wet and humidity is not None and humidity < RADAR_MIN_HUMIDITY:
            ev.add(Witness("radar", "radar", "radarul", "the radar", True, LLR["radar_dry_air"][0], amount_mm=radar_mm,
                           set_aside=f"aer prea uscat ({round(humidity)}%) ca ploaia să ajungă jos",
                           set_aside_en=f"air too dry ({round(humidity)}%) for the rain to reach the ground"))
        else:
            ev.add(Witness("radar", "radar", "radarul", "the radar", wet, LLR["radar"][0 if wet else 1], amount_mm=radar_mm))

    for pid, (amount, code) in providers.items():
        amount = _num(amount)
        if amount is None:
            if code is None:
                continue
            # A code without an amount: a falling code is a weak claim, anything else dry.
            wet = code >= PRECIP_MIN_CODE and code not in FOG_CODES
            llr = FORECAST_WET_LLR[-1][1] if wet else FORECAST_DRY_LLR
        else:
            wet = amount >= WET_MM
            llr = _forecast_wet_llr(amount) if wet else FORECAST_DRY_LLR
        ro, en = PROVIDER_NAMES.get(pid, (pid, pid))
        ev.add(Witness(pid, "forecast", ro, en, wet, llr * _trust(pid, learned, defaults), amount_mm=amount, code=code))

    if models and models[1] > 0:
        wet_n, total = models
        share = wet_n / total
        wet_llr, dry_llr = LLR["models"]
        llr = (wet_llr if share >= 0.5 else -dry_llr) * (2 * share - 1)
        ev.add(Witness("models", "models", "modelele naționale", "the national models", share >= 0.5,
                       llr * _trust("models", learned, defaults), count=total, wet_count=wet_n))
    return ev.witnesses


def _kind_of(witnesses: Iterable[Witness], temperature: float | None) -> str:
    """What is falling: the observed codes first, then the models', then the air."""
    ranked = sorted(
        (w for w in witnesses if w.wet and w.code is not None),
        key=lambda w: {"observation": 0, "gauge": 1, "radar": 2, "forecast": 3, "models": 4}.get(w.kind, 5),
    )
    for w in ranked:
        if w.code in ICE_CODES:
            return "ice"
        if w.code in SNOW_CODES:
            return "snow"
        if w.code in DRIZZLE_CODES:
            return "drizzle"
        if w.code >= PRECIP_MIN_CODE:
            return "rain"
    if temperature is not None and temperature <= 0.5:
        return "snow"
    return "rain"


WET_WORD = {
    "rain": ("Plouă", "It is raining"),
    "drizzle": ("Burniță", "Drizzle"),
    "snow": ("Ninge", "It is snowing"),
    "ice": ("Ploaie care îngheață", "Freezing rain"),
}
DRY_WORD = {
    "rain": ("Nu plouă", "No rain"),
    "drizzle": ("Nu plouă", "No rain"),
    "snow": ("Nu ninge", "No snow"),
    "ice": ("Nu plouă", "No rain"),
}


def _join(items: list[str], last_ro: str) -> str:
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    return ", ".join(items[:-1]) + f" {last_ro} " + items[-1]


def _mm(value: float, lang: str) -> str:
    text = f"{value:.1f}"
    return text.replace(".", ",") if lang == "ro" else text


def _sources_ro(n: int) -> str:
    return "1 sursă" if n == 1 else f"{n} surse"


def _sources_en(n: int) -> str:
    return "1 source" if n == 1 else f"{n} sources"


def _cap(text: str) -> str:
    return text[:1].upper() + text[1:]


def _explain(
    state: str, kind: str, witnesses: list[Witness], hour_probability: float | None = None
) -> tuple[str, str]:
    """One sentence per language: the verdict, who backs it, and who was not believed."""
    observed = [w for w in witnesses if w.kind in ("observation", "gauge", "radar") and not w.set_aside]
    counted = [w for w in witnesses if w.kind in ("forecast", "models")]
    total = sum(w.count for w in counted)
    wet_total = sum((w.wet_count if w.count > 1 else int(w.wet)) for w in counted)
    dry_total = total - wet_total
    set_aside = [w for w in witnesses if w.set_aside]

    def named(ws: list[Witness], lang: str) -> list[str]:
        out = []
        for w in ws:
            name = w.name_ro if lang == "ro" else w.name_en
            if w.wet and w.amount_mm is not None and w.kind == "gauge":
                name += f" ({_mm(w.amount_mm, lang)} mm)"
            out.append(name)
        return out

    def clause(items_ro: list[str], items_en: list[str], plural_ro: str, single_ro: str, verb_en: str) -> tuple[str, str]:
        many = len(items_ro) > 1 or bool(items_ro and "surse" in items_ro[-1])
        verb_ro = plural_ro if many else single_ro
        if not many:
            verb_en = {"see it": "sees it", "say dry": "says dry", "see rain": "sees rain"}.get(verb_en, verb_en)
        return _join(items_ro, "și") + " " + verb_ro, _join(items_en, "and") + " " + verb_en

    if state == "uncertain":
        wet_obs = [w for w in observed if w.wet]
        dry_obs = [w for w in observed if not w.wet]
        parts_ro, parts_en = [], []
        if total:
            parts_ro.append(f"{wet_total} din {_sources_ro(total)} dau ploaie")
            parts_en.append(f"{wet_total} of {_sources_en(total)} give rain")
        if wet_obs:
            r, e = clause(named(wet_obs, "ro"), named(wet_obs, "en"), "văd ploaie", "vede ploaie", "see rain")
            parts_ro.append(r)
            parts_en.append(e)
        if dry_obs:
            r, e = clause(named(dry_obs, "ro"), named(dry_obs, "en"), "spun uscat", "spune uscat", "say dry")
            parts_ro.append(r)
            parts_en.append(e)
        ro, en = "Semnale amestecate", "Mixed signals"
        if parts_ro:
            ro += ": " + ", ".join(parts_ro)
            en += ": " + ", ".join(parts_en)
        return ro + ".", en + "."

    wet = state == "wet"
    backers = [w for w in observed if w.wet == wet]
    lead_ro, lead_en = (WET_WORD if wet else DRY_WORD)[kind]
    who_ro, who_en = named(backers, "ro"), named(backers, "en")
    agree = wet_total if wet else dry_total
    # The tally is named only when most of the sources agree with the verdict.
    if total and agree * 2 >= total:
        if agree == total and total == 2:
            who_ro.append("ambele surse")
            who_en.append("both sources")
        elif agree == total and total > 2:
            who_ro.append(f"toate cele {total} surse")
            who_en.append(f"all {total} sources")
        else:
            who_ro.append(f"{agree} din {_sources_ro(total)}")
            who_en.append(f"{agree} of {_sources_en(total)}")
    if wet:
        rain_ro = "ninsoare" if kind == "snow" else "ploaie"
        r, e = clause(who_ro, who_en, f"văd {rain_ro}", f"vede {rain_ro}", "see it")
    else:
        r, e = clause(who_ro, who_en, "spun uscat", "spune uscat", "say dry")
    if who_ro:
        ro, en = f"{lead_ro}: {r}.", f"{lead_en}: {e}."
    elif hour_probability is not None:
        ro = f"{lead_ro}: prognoza orei dă {round(hour_probability)}% șanse de ploaie."
        en = f"{lead_en}: the hour's forecast gives a {round(hour_probability)}% chance of rain."
    else:
        ro, en = f"{lead_ro}.", f"{lead_en}."

    # Who said otherwise and was outweighed: worth one clause when it is one voice.
    dissent = [w for w in witnesses if w.wet != wet and not w.set_aside and w.count == 1]
    if not wet and len(dissent) == 1:
        d = dissent[0]
        verb = "arată" if d.kind == "gauge" else "vede" if d.kind in ("observation", "radar") else "dă"
        ro += f" {_cap(d.name_ro)} {verb} ploaie, dar e singura sursă care spune asta."
        en += f" {_cap(d.name_en)} gives rain, but it is the only one."
    for w in set_aside:
        ro += f" {_cap(w.name_ro)} vede ploaie, dar {w.set_aside}."
        en += f" {_cap(w.name_en)} sees rain, but the {w.set_aside_en}."
    return ro, en


def judge_precipitation(
    witnesses: list[Witness],
    *,
    hour_probability: float | None,
    temperature: float | None = None,
) -> dict[str, Any]:
    """The verdict on rain (snow, drizzle) falling now.

    ``state`` is "wet", "dry" or "uncertain"; ``probability`` is the chance it
    is falling (0-100) and ``confidence`` how sure the verdict is (50-100).
    """
    prior = (
        _logit(float(hour_probability) / 100) if hour_probability is not None else _logit(PRIOR_WITHOUT_FORECAST)
    ) * PRIOR_SHARE
    total = prior + sum(w.llr for w in witnesses)
    p = _sigmoid(total)
    state = "wet" if p >= WET_AT else "dry" if p <= DRY_AT else "uncertain"
    kind = _kind_of(witnesses, temperature)
    ro, en = _explain(state, kind, witnesses, hour_probability)
    observed_wet = any(w.wet and not w.set_aside and w.kind in ("observation", "gauge", "radar") for w in witnesses)
    return {
        "state": state,
        "kind": kind if state != "dry" else None,
        "probability": round(p * 100),
        "confidence": round(max(p, 1 - p) * 100),
        "observed": observed_wet,
        "explanation": ro,
        "explanation_en": en,
        "witnesses": [w.as_dict() for w in witnesses],
    }


# ---------------------------------------------------------------------------
# Fog
# ---------------------------------------------------------------------------

def judge_fog(
    *,
    codes: dict[str, int | None],
    airport: dict[str, Any] | None = None,
    model_visibility_m: float | None = None,
) -> dict[str, Any] | None:
    """Is there fog here now? None when no one mentions fog or a low visibility.

    ``codes`` maps a provider to its current WMO code. An airport within
    AIRPORT_MAX_KM that measures its visibility outweighs every model.
    """
    fog_said = [pid for pid, code in codes.items() if code in FOG_CODES]
    obs_vis = _num((airport or {}).get("visibility_m"))
    obs_code = (airport or {}).get("wmo_code")
    low_vis = (obs_vis is not None and obs_vis < FOG_VIS_M) or (
        model_visibility_m is not None and model_visibility_m < FOG_VIS_M)
    if not fog_said and obs_code not in FOG_CODES and not low_vis:
        return None

    total = FOG_PRIOR
    reasons_ro: list[str] = []
    reasons_en: list[str] = []
    for pid, code in codes.items():
        if code is None:
            continue
        total += 0.8 if code in FOG_CODES else -0.4
    if model_visibility_m is not None:
        total += 1.0 if model_visibility_m < FOG_VIS_M else -0.6 if model_visibility_m > 5000 else 0.0
    if airport:
        fade = _fade(_num(airport.get("distance_km")), AIRPORT_DRY_KM, AIRPORT_MAX_KM, 0.5)
        name = airport_name(airport)
        name_ro = f"aeroportul {name}" if name else "aeroportul"
        name_en = f"{name} airport" if name else "the airport"
        if obs_code in FOG_CODES or (obs_vis is not None and obs_vis < FOG_VIS_M):
            total += 5.0 * fade
            if obs_vis is not None:
                reasons_ro.append(f"{name_ro} vede {round(obs_vis)} m")
                reasons_en.append(f"{name_en} sees {round(obs_vis)} m")
            else:
                reasons_ro.append(f"{name_ro} raportează ceață")
                reasons_en.append(f"{name_en} reports fog")
        elif obs_vis is not None and obs_vis >= CLEAR_VIS_M:
            total -= 3.0 * fade
            km = obs_vis / 1000
            reasons_ro.append(f"{name_ro} vede {_mm(km, 'ro').replace(',0', '')} km")
            reasons_en.append(f"{name_en} sees {_mm(km, 'en').replace('.0', '')} km")
    p = _sigmoid(total)
    fog = p >= FOG_AT
    n = len([c for c in codes.values() if c is not None])
    tally_ro = f"{len(fog_said)} din {_sources_ro(n)} dau ceață" if fog_said and n else ""
    tally_en = f"{len(fog_said)} of {_sources_en(n)} give fog" if fog_said and n else ""
    if fog:
        if tally_ro:
            reasons_ro.append(tally_ro)
            reasons_en.append(tally_en)
        ro = "Ceață" + (": " + _join(reasons_ro, "și") if reasons_ro else "") + "."
        en = "Fog" + (": " + _join(reasons_en, "and") if reasons_en else "") + "."
    else:
        ro = "Fără ceață" + (": " + _join(reasons_ro, "și") if reasons_ro else "")
        en = "No fog" + (": " + _join(reasons_en, "and") if reasons_en else "")
        if tally_ro:
            ro += (", deși " if reasons_ro else ": doar ") + tally_ro
            en += (", although " if reasons_en else ": only ") + tally_en
        ro += "."
        en += "."
    return {
        "state": "fog" if fog else "clear",
        "probability": round(p * 100),
        "confidence": round(max(p, 1 - p) * 100),
        "explanation": ro,
        "explanation_en": en,
    }


# ---------------------------------------------------------------------------
# Storms and the satellite's lightning
# ---------------------------------------------------------------------------

def judge_storm(
    *,
    flashes: int,
    convective: bool,
    storm_code: bool,
    probability: float | None,
    airport_storm: bool = False,
    nearest_km: float | None = None,
) -> dict[str, Any] | None:
    """Is a storm here? None when nothing mentions one.

    Several flashes are a storm; one flash needs the air to back it (convective
    air, a storm code, a real chance of rain), or it is set aside as a stray.
    """
    if flashes <= 0 and not storm_code and not airport_storm:
        return None
    total = STORM_PRIOR
    if flashes >= STORM_MANY_FLASHES:
        total += 4.0
    elif flashes == 1:
        total += 1.5
    if convective:
        total += 1.0
    if storm_code:
        total += 1.5
    if airport_storm:
        total += 4.0
    if probability is not None and probability >= 20:
        total += 1.0
    p = _sigmoid(total)
    storm = p >= STORM_AT
    where_ro = f" la {round(nearest_km)} km" if nearest_km is not None else ""
    where_en = f" {round(nearest_km)} km away" if nearest_km is not None else ""
    if storm:
        if flashes >= STORM_MANY_FLASHES:
            ro, en = f"Furtună: satelitul vede {flashes} fulgere{where_ro}.", f"Storm: the satellite sees {flashes} flashes{where_en}."
        elif airport_storm:
            ro, en = "Furtună: aeroportul raportează oraj.", "Storm: the airport reports a thunderstorm."
        elif flashes == 1:
            ro, en = f"Posibil oraj: un fulger{where_ro}, în aer instabil.", f"Possible storm: one flash{where_en}, in unstable air."
        elif convective:
            ro, en = "Posibil oraj: aerul e instabil.", "Possible storm: unstable air."
        else:
            ro, en = "Posibil oraj: prognoza dă furtună.", "Possible storm: the forecast gives a storm."
    elif flashes == 1:
        ro = f"Fără furtună: un singur fulger{where_ro}, iar aerul e calm și uscat."
        en = f"No storm: a single flash{where_en}, and the air is calm and dry."
    else:
        ro, en = "Fără furtună: nimic nu confirmă orajul.", "No storm: nothing backs the storm."
    return {
        "state": "storm" if storm else "none",
        "probability": round(p * 100),
        "confidence": round(max(p, 1 - p) * 100),
        "explanation": ro,
        "explanation_en": en,
    }
