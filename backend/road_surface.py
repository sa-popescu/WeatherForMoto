"""
The temperature of the asphalt, from the heat it gains and loses, hour by hour.

The road is not the air. By day dark asphalt soaks up the sun and runs well
above the air; after sunset it hands that heat back slowly from the layers
underneath; under a clear sky it radiates to space and ends up below the air,
which is when black ice forms; wet, it is cooled by evaporation; under falling
snow it is held near 0 °C. No free service measures road surfaces in Romania
(CNAIR's road weather stations have no public API), so the value is computed,
the way road weather services (METRo, Environment Canada; the Nordic road
models) do it: a column of asphalt, base and soil whose surface balances

    sun absorbed + sky's infrared  =  infrared emitted + heat to the air
                                      + evaporation + heat into the road

and whose layers conduct heat up and down. Running that column over the past
days and the forecast hours gives each hour a surface temperature with the
right memory: a sunny afternoon keeps the road warm into the evening, a grey
week keeps it cold.

What each term needs and where it comes from:

- sun: the models' shortwave radiation (best match pulled toward the national
  models' mean), spread inside each hour along the sun's height;
- sky infrared: from the air temperature and its moisture (Brutsaert's clear
  sky) and the cloud, low cloud weighing most and cirrus little; a nearby
  airport's observed cloud replaces the model's around now, because a forecast
  of "overcast" over a CAVOK airport is the cloud being wrong, not the sky;
- air: the hour's temperature, humidity and wind, already corrected by the
  stations measuring them now (weather_service);
- below: the models' soil temperatures start the column and hold its bottom.

The uncertainty is not a fixed margin either: the column runs again with the
cloud and the sun as far apart as the national models put them (a clearer and
a cloudier sky), and the spread of those runs is the range shown. A model's
own surface ("skin") temperature is a witness: grass and soil are not asphalt,
but an asphalt far outside what every model gives for the ground is a reason
to say so and to trust the value less.

Pure functions; weather_service gathers the inputs and applies the result.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Sequence

SIGMA = 5.670374e-8
KELVIN = 273.15

# The surface. Aged asphalt reflects about a tenth of the sun.
ALBEDO = 0.10
EMISSIVITY = 0.95
# A road sees most of the sky; trees, slopes and buildings hide some of it and
# send back infrared at about the air's temperature.
SKY_VIEW = 0.9

# The column: (bottom depth in m, conductivity W/m K, volumetric heat capacity J/m3 K).
LAYERS: tuple[tuple[float, float, float], ...] = (
    (0.10, 1.2, 2.1e6),   # asphalt
    (0.40, 1.4, 1.9e6),   # crushed stone base
    (0.54, 1.2, 2.3e6),   # soil; the bottom follows the models' 54 cm soil
)
NODES: tuple[float, ...] = (0.0, 0.01, 0.025, 0.045, 0.07, 0.10, 0.14, 0.19, 0.25, 0.32, 0.40, 0.47, 0.54)
STEPS_PER_HOUR = 6

# Heat to the air: natural convection plus forced (Jurges / McAdams), W/m2 K,
# with the wind brought from 10 m to about 2 m.
CONVECTION_STILL = 5.6
CONVECTION_PER_MS = 4.0
WIND_2M_SHARE = 0.72

# Water on the road: rain fills a film, runoff caps it, evaporation empties it.
FILM_MAX_MM = 1.0
FILM_FULL_MM = 0.2
LATENT_HEAT = 2.5e6
AIR_CP = 1005.0
WATER_VOLUMETRIC_HEAT = 4.186e6
# Falling snow that melts holds the surface about here.
SNOW_SURFACE_MAX_C = 0.5
SNOW_AIR_MAX_C = 1.0

# Cloud weights for the sky's infrared: low cloud is warm and thick, cirrus cold and thin.
CLOUD_WEIGHT_LOW = 1.0
CLOUD_WEIGHT_MID = 0.85
CLOUD_WEIGHT_HIGH = 0.35

# The smallest spread used for the clearer / cloudier runs when the models agree
# or are not there (the past hours): a tenth of cloud, a seventh of the sun.
MIN_CLOUD_SPREAD = 0.12
MIN_SUN_SPREAD_SHARE = 0.15

# An observed sky counts in full for the hour being lived and fades around it (hours from now).
OBSERVED_CLOUD_WEIGHT: dict[int, float] = {-2: 0.5, -1: 0.8, 0: 1.0, 1: 0.7, 2: 0.4, 3: 0.15}

# What no input tells: shade, the road's age and colour, traffic, the exact sky
# view. Added to every range: a degree, and a share of how far the sun or the
# night sky has taken the road from the air (that gap is the part that varies).
STRUCTURAL_C = 1.0
STRUCTURAL_SHARE = 0.15
# Range (high - low, °C) for each confidence.
HIGH_CONFIDENCE_RANGE = 3.0
MEDIUM_CONFIDENCE_RANGE = 5.0
# How far asphalt may sit from every model's ground surface before it is odd.
SKIN_BELOW_MAX_C = 3.0
SKIN_ABOVE_MAX_C = {True: 18.0, False: 7.0}


@dataclass
class Hour:
    """The weather one hour mark brings to the road. Times are UTC epoch seconds."""
    ts: float
    temp: float
    dew: float | None = None
    wind_kmh: float | None = None
    # Cloud for the sky's infrared, 0..1 (cloud_effective), and how far the models spread it.
    cloud: float | None = None
    cloud_spread: float = 0.0
    # Mean shortwave of the hour before this mark (Open-Meteo convention), W/m2, and its spread.
    sun: float | None = None
    sun_spread: float = 0.0
    precip_mm: float | None = None
    snowing: bool = False
    # The models' soil at 54 cm, the bottom of the column.
    soil_deep: float | None = None


@dataclass
class Result:
    temps: list[float]
    lows: list[float]
    highs: list[float]
    # Wetness of the film at each mark, 0..1, for the explanation.
    wet: list[float] = field(default_factory=list)


def cloud_effective(total: float | None, low: float | None = None, mid: float | None = None,
                    high: float | None = None) -> float | None:
    """The cloud the road "sees" in infrared, 0..1, from percentages per level (or the total)."""
    if low is None and mid is None and high is None:
        return None if total is None else max(0.0, min(1.0, total / 100))
    clear = 1.0
    for value, weight in ((low, CLOUD_WEIGHT_LOW), (mid, CLOUD_WEIGHT_MID), (high, CLOUD_WEIGHT_HIGH)):
        clear *= 1 - weight * max(0.0, min(1.0, (value or 0) / 100))
    return 1 - clear


def cos_zenith(lat: float, lon: float, ts: float) -> float:
    """Cosine of the sun's zenith angle (NOAA's approximation); negative when the sun is down."""
    moment = datetime.fromtimestamp(ts, tz=timezone.utc)
    day = moment.timetuple().tm_yday
    hours = moment.hour + moment.minute / 60 + moment.second / 3600
    gamma = 2 * math.pi / 365 * (day - 1 + (hours - 12) / 24)
    decl = (0.006918 - 0.399912 * math.cos(gamma) + 0.070257 * math.sin(gamma)
            - 0.006758 * math.cos(2 * gamma) + 0.000907 * math.sin(2 * gamma)
            - 0.002697 * math.cos(3 * gamma) + 0.00148 * math.sin(3 * gamma))
    eqtime = 229.18 * (0.000075 + 0.001868 * math.cos(gamma) - 0.032077 * math.sin(gamma)
                       - 0.014615 * math.cos(2 * gamma) - 0.040849 * math.sin(2 * gamma))
    solar_minutes = hours * 60 + eqtime + 4 * lon
    hour_angle = math.radians(solar_minutes / 4 - 180)
    phi = math.radians(lat)
    return math.sin(phi) * math.sin(decl) + math.cos(phi) * math.cos(decl) * math.cos(hour_angle)


def _vapour_hpa(temp_c: float) -> float:
    """Saturation vapour pressure over water (Magnus), hPa."""
    return 6.112 * math.exp(17.62 * temp_c / (243.12 + temp_c))


def _specific_humidity(vapour_hpa: float, pressure_hpa: float = 1013.0) -> float:
    return 0.622 * vapour_hpa / (pressure_hpa - 0.378 * vapour_hpa)


def sky_infrared(temp_c: float, dew_c: float | None, cloud: float | None) -> float:
    """Downwelling infrared at the road, W/m2: Brutsaert's clear sky, cloud filling the gap to a black body."""
    t = temp_c + KELVIN
    vapour = _vapour_hpa(dew_c if dew_c is not None else temp_c - 5.0)
    clear = 1.24 * (vapour / t) ** (1 / 7)
    n = 0.5 if cloud is None else cloud
    emissivity = min(1.0, clear + (1 - clear) * n)
    sky = emissivity * SIGMA * t ** 4
    return SKY_VIEW * sky + (1 - SKY_VIEW) * SIGMA * t ** 4


def convection(wind_kmh: float | None) -> float:
    wind = WIND_2M_SHARE * max(0.0, wind_kmh if wind_kmh is not None else 7.0) / 3.6
    if wind <= 5:
        return CONVECTION_STILL + CONVECTION_PER_MS * wind
    return 7.2 * wind ** 0.78


def _grid() -> tuple[list[float], list[float]]:
    """Per node: heat capacity of its slice (J/m2 K) and conductance to the next node (W/m2 K)."""
    def props(depth: float) -> tuple[float, float]:
        for bottom, k, rc in LAYERS:
            if depth <= bottom + 1e-9:
                return k, rc
        return LAYERS[-1][1], LAYERS[-1][2]

    capacity: list[float] = []
    for i, z in enumerate(NODES):
        top = 0.0 if i == 0 else (NODES[i - 1] + z) / 2
        bottom = z if i == len(NODES) - 1 else (z + NODES[i + 1]) / 2
        capacity.append(props((top + bottom) / 2)[1] * (bottom - top))
    conductance = [props((a + b) / 2)[0] / (b - a) for a, b in zip(NODES, NODES[1:])]
    return capacity, conductance


_CAPACITY, _CONDUCTANCE = _grid()


def _lerp(a: float | None, b: float | None, f: float) -> float | None:
    if a is None:
        return b
    if b is None:
        return a
    return a + (b - a) * f


def initial_profile(surface: float, soil: Sequence[tuple[float, float | None]], deep: float | None) -> list[float]:
    """Temperatures at NODES from the models' soil at some depths (m, °C), else a line to ``deep``."""
    known = sorted((d, t) for d, t in soil if t is not None)
    if not known:
        known = [(0.0, surface), (NODES[-1], surface if deep is None else deep)]
    if known[0][0] > 0:
        known.insert(0, (0.0, surface))
    out = []
    for z in NODES:
        below = next(((d, t) for d, t in known if d >= z), known[-1])
        above = next(((d, t) for d, t in reversed(known) if d <= z), known[0])
        if below[0] == above[0]:
            out.append(below[1])
        else:
            out.append(above[1] + (below[1] - above[1]) * (z - above[0]) / (below[0] - above[0]))
    return out


def simulate(hours: Sequence[Hour], lat: float, lon: float, start: Sequence[float],
             member: int = 0, sun_override: dict[int, float] | None = None,
             keep: int | None = None, start_film: float = 0.0) -> tuple[list[float], list[float], tuple[list[float], float] | None]:
    """Surface temperature and film wetness at every hour mark, and the column at mark ``keep``.

    ``member`` is 0 for the central run, -1 for the clearer sky (less cloud,
    more sun) and +1 for the cloudier one. ``sun_override`` maps the index of
    an hour mark to the sun (W/m2) to use for the hour before it, in place of
    the model's mean (a station measuring it now).
    ``start`` is the column at the first mark; the kept state is (column, film).
    """
    temps = list(start)
    kept = (list(temps), start_film) if keep == 0 else None
    n = len(temps)
    film = start_film
    out_t = [temps[0]]
    out_w = [min(1.0, film / FILM_FULL_MM)]
    dt = 3600.0 / STEPS_PER_HOUR
    for h in range(1, len(hours)):
        prev, cur = hours[h - 1], hours[h]
        span = max(60.0, cur.ts - prev.ts)
        steps = max(1, round(span / dt))
        step = span / steps
        # The hour's sun, spread along the sun's height inside the hour.
        sun_mean = cur.sun if cur.sun is not None else 0.0
        if sun_override and h in sun_override:
            sun_mean = sun_override[h]
        if member:
            sun_mean = max(0.0, sun_mean - member * max(cur.sun_spread, MIN_SUN_SPREAD_SHARE * sun_mean))
        mids = [prev.ts + (s + 0.5) * step for s in range(steps)]
        heights = [max(0.0, cos_zenith(lat, lon, t)) for t in mids]
        mean_height = sum(heights) / steps
        for s in range(steps):
            f = (s + 0.5) / steps
            air = _lerp(prev.temp, cur.temp, f)
            dew = _lerp(prev.dew, cur.dew, f)
            wind = _lerp(prev.wind_kmh, cur.wind_kmh, f)
            cloud = _lerp(prev.cloud, cur.cloud, f)
            if member and cloud is not None:
                spread = max(_lerp(prev.cloud_spread, cur.cloud_spread, f) or 0.0, MIN_CLOUD_SPREAD)
                cloud = max(0.0, min(1.0, cloud + member * spread))
            deep = _lerp(prev.soil_deep, cur.soil_deep, f)
            if mean_height > 0.01:
                sun = sun_mean * heights[s] / mean_height
            else:
                sun = sun_mean
            rain = (cur.precip_mm or 0.0) / 3600.0  # mm/s, the hour's rain is the hour before the mark
            surface = temps[0]
            # Surface energy balance, linearised around the current surface temperature.
            t_k = surface + KELVIN
            emitted = EMISSIVITY * SIGMA * t_k ** 4
            d_emitted = 4 * EMISSIVITY * SIGMA * t_k ** 3
            h_c = convection(wind)
            gain = (1 - ALBEDO) * sun + EMISSIVITY * sky_infrared(air, dew, cloud) - emitted + d_emitted * surface
            loss = d_emitted + h_c
            gain += h_c * air
            # Rain arrives at about the wet bulb and exchanges its heat with the road.
            if rain > 0:
                wet_bulb = air - (air - dew) / 3 if dew is not None else air
                rain_h = WATER_VOLUMETRIC_HEAT * rain / 1000
                gain += rain_h * wet_bulb
                loss += rain_h
            # Evaporation from the film (or dew and frost deposit on any road).
            q_air = _specific_humidity(_vapour_hpa(dew)) if dew is not None else None
            if q_air is not None:
                q_surface = _specific_humidity(_vapour_hpa(surface))
                dq = 0.622 * _vapour_hpa(surface) * 17.62 * 243.12 / (243.12 + surface) ** 2 / 1013.0
                wetness = min(1.0, film / FILM_FULL_MM)
                if q_surface < q_air:
                    wetness = 1.0  # condensation needs no film
                h_e = h_c * LATENT_HEAT / AIR_CP * wetness
                gain -= h_e * (q_surface - q_air - dq * surface)
                loss += h_e * dq
            # Implicit step of the column (Thomas algorithm).
            a = [0.0] * n
            b = [0.0] * n
            c = [0.0] * n
            d = [0.0] * n
            for i in range(n - 1):
                cap = _CAPACITY[i] / step
                up = _CONDUCTANCE[i - 1] if i > 0 else 0.0
                down = _CONDUCTANCE[i]
                a[i] = -up
                c[i] = -down
                b[i] = cap + up + down
                d[i] = cap * temps[i]
            b[0] += loss
            d[0] += gain
            bottom = deep if deep is not None else temps[-1]
            a[n - 1], b[n - 1], c[n - 1], d[n - 1] = 0.0, 1.0, 0.0, bottom
            for i in range(1, n):
                w = a[i] / b[i - 1]
                b[i] -= w * c[i - 1]
                d[i] -= w * d[i - 1]
            temps[n - 1] = d[n - 1] / b[n - 1]
            for i in range(n - 2, -1, -1):
                temps[i] = (d[i] - c[i] * temps[i + 1]) / b[i]
            if (cur.snowing or (rain > 0 and air <= SNOW_AIR_MAX_C)) and temps[0] > SNOW_SURFACE_MAX_C:
                temps[0] = SNOW_SURFACE_MAX_C
            # The film: rain fills it, evaporation (or dew) changes it, runoff caps it.
            if q_air is not None:
                q_surface = _specific_humidity(_vapour_hpa(temps[0]))
                wetness = 1.0 if q_surface < q_air else min(1.0, film / FILM_FULL_MM)
                evaporation = h_c / AIR_CP * (q_surface - q_air) * wetness  # kg/m2 s = mm/s
            else:
                evaporation = 0.0
            film = max(0.0, min(FILM_MAX_MM, film + (rain - evaporation) * step))
        out_t.append(temps[0])
        out_w.append(min(1.0, film / FILM_FULL_MM))
        if keep == h:
            kept = (list(temps), film)
    return out_t, out_w, kept


def run(hours: Sequence[Hour], lat: float, lon: float, start: Sequence[float],
        sun_override: dict[int, float] | None = None,
        window: tuple[int, int] | None = None) -> Result:
    """The central run and the clearer and cloudier ones, with the range of the three.

    The two other skies only run inside ``window`` (first mark, end), started
    from the central column there: what matters is the doubt about the hours
    around now and ahead, and it saves two thirds of the work. Every range then
    widens by what no input tells (STRUCTURAL_*), outside the window too.
    """
    first, end = window if window else (0, len(hours))
    first = max(0, min(first, len(hours) - 1))
    end = max(first + 1, min(end, len(hours)))
    central, wet, kept = simulate(hours, lat, lon, start, 0, sun_override, keep=first)
    lows, highs = list(central), list(central)
    if kept is not None and end - first > 1:
        shifted = {i - first: v for i, v in (sun_override or {}).items() if first <= i < end}
        column, film = kept
        for member in (-1, 1):
            temps, _, _ = simulate(hours[first:end], lat, lon, column, member, shifted, start_film=film)
            for i, value in enumerate(temps):
                lows[first + i] = min(lows[first + i], value)
                highs[first + i] = max(highs[first + i], value)
    for i, hour in enumerate(hours):
        margin = STRUCTURAL_C + STRUCTURAL_SHARE * abs(central[i] - hour.temp)
        lows[i] = min(lows[i], central[i] - margin)
        highs[i] = max(highs[i], central[i] + margin)
    return Result(temps=central, lows=lows, highs=highs, wet=wet)


def blend_observed_cloud(model: float | None, observed: float | None, offset_h: int) -> tuple[float | None, float]:
    """The hour's cloud with an observed sky mixed in by how close the hour is to now; and the observation's weight."""
    weight = OBSERVED_CLOUD_WEIGHT.get(offset_h, 0.0) if observed is not None else 0.0
    if weight == 0 or model is None:
        return (observed if model is None and weight else model), weight
    return model + (observed - model) * weight, weight


def confidence(low: float, high: float, skin_odd: bool) -> str:
    width = high - low
    level = "high" if width <= HIGH_CONFIDENCE_RANGE else "medium" if width <= MEDIUM_CONFIDENCE_RANGE else "low"
    if skin_odd:
        level = {"high": "medium", "medium": "low", "low": "low"}[level]
    return level


def skin_is_odd(road: float, skins: Sequence[float], is_day: bool) -> bool:
    """True when the asphalt sits far outside what every model gives for the ground's surface."""
    if not skins:
        return False
    return road < min(skins) - SKIN_BELOW_MAX_C or road > max(skins) + SKIN_ABOVE_MAX_C[bool(is_day)]


def explain(*, road: float, air: float | None, is_day: bool, sun: float | None, cloud: float | None,
            wet: float, snowing: bool, observed_sky: str | None, low: float, high: float,
            skin_odd: bool) -> tuple[str, str]:
    """One sentence each in Romanian and English: why the asphalt is where it is."""
    diff = None if air is None else road - air
    if snowing:
        ro, en = ("Ninge: zăpada care se topește ține asfaltul în jur de 0°.",
                  "Snowing: melting snow holds the asphalt near 0°.")
    elif wet >= 0.5:
        ro, en = ("Asfalt ud: apa care se evaporă îl ține aproape de aer.",
                  "Wet asphalt: evaporating water keeps it close to the air.")
    elif is_day and (sun or 0) >= 250:
        ro, en = ("Soarele încălzește asfaltul închis la culoare peste aer.",
                  "The sun heats the dark asphalt above the air.")
    elif is_day:
        ro, en = ("Soare puțin: asfaltul rămâne aproape de aer.",
                  "Little sun: the asphalt stays close to the air.")
    elif cloud is not None and cloud < 0.35:
        if diff is not None and diff < -0.5:
            ro, en = ("Cer senin: asfaltul radiază căldura spre cer și e mai rece decât aerul.",
                      "Clear sky: the asphalt radiates its heat away and is colder than the air.")
        else:
            ro, en = ("Cer senin: asfaltul pierde căldura zilei și se răcește spre aer și sub el.",
                      "Clear sky: the asphalt is losing the day's heat and cooling toward and below the air.")
    else:
        ro, en = ("Noros: norii țin căldura, asfaltul se răcește încet.",
                  "Cloudy: the clouds hold the heat in, the asphalt cools slowly.")
    if observed_sky:
        ro += f" Cerul e cel văzut acum de aeroportul {observed_sky}."
        en += f" The sky is the one {observed_sky} airport sees now."
    if round(high) - round(low) >= 1:
        ro += f" Probabil între {round(low)}° și {round(high)}°."
        en += f" Likely between {round(low)}° and {round(high)}°."
    if skin_odd:
        ro += " Modelele dau altceva pentru sol, deci încredere mai mică."
        en += " The models give the ground something else, so lower confidence."
    return ro, en


def local_to_ts(local_iso: str, utc_offset_seconds: int) -> float | None:
    try:
        moment = datetime.fromisoformat(str(local_iso)[:16])
    except ValueError:
        return None
    return (moment - timedelta(seconds=utc_offset_seconds)).replace(tzinfo=timezone.utc).timestamp()


def metadata() -> dict[str, Any]:
    """The model's constants, for GET /meta/scoring."""
    return {
        "albedo": ALBEDO,
        "emissivity": EMISSIVITY,
        "sky_view": SKY_VIEW,
        "layers": [{"bottom_m": b, "conductivity": k, "heat_capacity": rc} for b, k, rc in LAYERS],
        "steps_per_hour": STEPS_PER_HOUR,
        "cloud_weights": {"low": CLOUD_WEIGHT_LOW, "mid": CLOUD_WEIGHT_MID, "high": CLOUD_WEIGHT_HIGH},
        "confidence_range_c": {"high": HIGH_CONFIDENCE_RANGE, "medium": MEDIUM_CONFIDENCE_RANGE},
    }
