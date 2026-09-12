import { currentHourIndex, localNowIso } from './format';
import { getScoringMeta, rainBandOf, rainImpact, type ScoringMeta } from './scoring';
import type { CurrentWeather, DailyWeather, HourlyWeather, MotoLabel, Place, RainBand, ScoreFactor, WeatherResponse } from './types';
import { describeCode } from './weatherCodes';

// Fallback used only when the backend does not answer: Open-Meteo is called
// directly and scores are estimated in the browser with a simplified port of
// the backend rules (backend/weather_service.py), driven by the published
// scoring constants. One source only, so no AQI, pollen or multi-source merge.

const OPEN_METEO_URL = 'https://api.open-meteo.com/v1/forecast';
const DIRECT_TIMEOUT_MS = 10_000;
const DIRECT_FORECAST_DAYS = 7;
const CURRENT_FIELDS =
  'temperature_2m,apparent_temperature,relative_humidity_2m,precipitation,weather_code,wind_speed_10m,wind_gusts_10m,wind_direction_10m,surface_pressure,pressure_msl,visibility,is_day';
const HOURLY_FIELDS =
  'temperature_2m,apparent_temperature,precipitation_probability,precipitation,weather_code,wind_speed_10m,wind_gusts_10m,wind_direction_10m,uv_index,relative_humidity_2m,surface_pressure,pressure_msl,dew_point_2m,cloud_cover,visibility,is_day';
const DAILY_FIELDS =
  'weather_code,temperature_2m_max,temperature_2m_min,apparent_temperature_max,apparent_temperature_min,precipitation_sum,precipitation_probability_max,wind_speed_10m_max,wind_gusts_10m_max,sunrise,sunset';

// WMO code families the published constants do not carry.
const LIGHT_RAIN_CODES = new Set([51, 53, 55, 61, 80]);
const HEAVY_RAIN_CODES = new Set([63, 65, 81, 82]);
const WET_ROAD_CODES = new Set([51, 53, 55, 61, 63, 65, 80, 81, 82, 95, 96, 99]);
const RIME_FOG_CODE = 48;
const OVERCAST_CODE = 3;
const CODE_SEVERITY = [0, 1, 2, 3, 45, 48, 51, 53, 55, 61, 80, 63, 81, 65, 82, 71, 77, 85, 73, 75, 86, 56, 57, 66, 67, 95, 96, 99];
const WIND_LABELS_RO = ['N', 'NE', 'E', 'SE', 'S', 'SV', 'V', 'NV'];
const BEAUFORT_MPS = [0.3, 1.5, 3.4, 5.4, 7.9, 10.7, 13.8, 17.1, 20.7, 24.4, 28.4, 32.6];

/** Fields of GET /meta/scoring that the presentation type in scoring.ts leaves out. */
interface MetaExtras {
  rain: {
    amount_penalty?: { scale: number; max: number };
    probability_penalty_per_pct?: number;
    code_implied_intensity?: { light: RainBand; heavy: RainBand };
  };
  stale_code_rule?: { codes_from: number; min_probability_pct: number; min_amount_mm: number };
  hazards: Record<string, { visibility_tiers?: { below_m: number; penalty: number; cap: number }[] }>;
  daily?: { worst_hours_fraction: number; worst_hours_weight: number; hazard_factors: string[]; sustained_cap_fraction: number };
}
const meta = (): ScoringMeta & MetaExtras => getScoringMeta() as ScoringMeta & MetaExtras;

export interface ScoreInput {
  feelsLike: number | null;
  gustsKmh: number | null;
  precipitationMm: number | null;
  weatherCode: number | null;
  probability: number | null;
  visibilityM?: number | null;
  frostRisk?: boolean;
}

export interface ScoreResult {
  score: number | null;
  factors: ScoreFactor[];
}

/** Python's round(): halves go to the even neighbour (0.5 -> 0, 2.5 -> 2), like the backend. */
function roundHalfEven(value: number): number {
  const rounded = Math.round(value);
  return Math.abs(value % 1) === 0.5 && rounded % 2 !== 0 ? rounded - 1 : rounded;
}

const factor = (name: string, penalty: number, cap: number | null, detail: string): ScoreFactor => ({ factor: name, penalty: roundHalfEven(penalty), cap, detail });

/** A rain / storm code with a known low probability and no amount is a leftover, not weather. */
export function codeIsStale(code: number | null, amount: number | null, probability: number | null): boolean {
  const rule = meta().stale_code_rule ?? { codes_from: 51, min_probability_pct: 20, min_amount_mm: 0.1 };
  if (code == null || code < rule.codes_from || probability == null) return false;
  return probability < rule.min_probability_pct && (amount ?? 0) < rule.min_amount_mm;
}

function rainFactor(amount: number | null, probability: number | null, code: number | null): ScoreFactor | null {
  const m = meta();
  let band = rainBandOf(amount);
  if (amount == null && code != null && !codeIsStale(code, amount, probability)) {
    const implied = m.rain.code_implied_intensity ?? { light: 'slaba', heavy: 'moderata' };
    if (LIGHT_RAIN_CODES.has(code)) band = implied.light;
    else if (HEAVY_RAIN_CODES.has(code)) band = implied.heavy;
  }
  if (probability == null && band === 'none') return null;
  // Unknown probability with rain present means it is raining: scored as a certainty.
  const prob = probability == null ? 100 : Math.max(0, Math.min(100, probability));
  const amountRule = m.rain.amount_penalty ?? { scale: 10, max: 35 };
  const mm = Math.max(0, amount ?? 0);
  const penalty = Math.min(amountRule.max, amountRule.scale * Math.log2(1 + mm)) + prob * (m.rain.probability_penalty_per_pct ?? 0.1);
  const cap = m.rain.impact_caps[rainImpact(prob, band)] ?? null;
  return factor('rain', penalty, cap, `${Math.round(prob)}%, ${amount == null ? '?' : mm.toFixed(1)} mm/h (${band})`);
}

function codeHazardFactor(code: number | null, amount: number | null, probability: number | null): ScoreFactor | null {
  if (code == null || codeIsStale(code, amount, probability)) return null;
  for (const name of ['ice', 'snow', 'storm']) {
    const hazard = meta().hazards[name];
    if (hazard?.codes.includes(code)) return factor(name, hazard.penalty, hazard.cap, `WMO ${code}`);
  }
  return null;
}

function visibilityFactor(code: number | null, visibilityM: number | null): ScoreFactor | null {
  const fog = meta().hazards.fog;
  if (!fog) return null;
  if (visibilityM != null) {
    const tiers = [...(fog.visibility_tiers ?? [])].sort((a, b) => a.below_m - b.below_m);
    const hit = tiers.find((t) => visibilityM < t.below_m);
    if (hit) return factor('fog', hit.penalty, hit.cap, `${Math.round(visibilityM)} m`);
  }
  return code != null && fog.codes.includes(code) ? factor('fog', fog.penalty, fog.cap, `WMO ${code}`) : null;
}

function windFactor(gusts: number | null): ScoreFactor | null {
  if (gusts == null) return null;
  const hit = [...meta().wind_gust_tiers].sort((a, b) => b.above_kmh - a.above_kmh).find((t) => gusts > t.above_kmh);
  return hit ? factor('wind', hit.penalty, null, `${Math.round(gusts)} km/h`) : null;
}

function temperatureFactors(feels: number | null, probability: number | null): ScoreFactor[] {
  if (feels == null) return [];
  const t = meta().temperature;
  const out: ScoreFactor[] = [];
  const cold = [...t.cold_tiers].sort((a, b) => a.feels_below_c - b.feels_below_c).find((c) => feels < c.feels_below_c);
  const heat = [...t.heat_tiers].sort((a, b) => b.feels_above_c - a.feels_above_c).find((h) => feels > h.feels_above_c);
  if (cold) out.push(factor('cold', cold.penalty + (feels < 0 ? t.subzero_extra_penalty : 0), null, `${Math.round(feels)} °C`));
  else if (heat) out.push(factor('heat', heat.penalty, null, `${Math.round(feels)} °C`));
  if (feels < t.cold_wet.feels_below_c && probability != null && probability >= t.cold_wet.min_probability_pct) {
    out.push(factor('cold_wet', t.cold_wet.penalty, null, `${Math.round(feels)} °C, ${Math.round(probability)}%`));
  }
  return out;
}

/** 0-100 score and its factors: 100 minus the penalties, limited by the lowest cap. */
export function scoreBreakdown(input: ScoreInput): ScoreResult {
  const visibility = input.visibilityM ?? null;
  const inputs = [input.feelsLike, input.gustsKmh, input.precipitationMm, input.weatherCode, input.probability, visibility];
  if (inputs.every((v) => v == null)) return { score: null, factors: [] };
  const rain = rainFactor(input.precipitationMm, input.probability, input.weatherCode);
  const hazard = codeHazardFactor(input.weatherCode, input.precipitationMm, input.probability);
  // Storm / snow / ice water is already counted by rain: the hazard adds only the excess.
  if (rain && hazard) hazard.penalty = Math.max(0, hazard.penalty - rain.penalty);
  const candidates = [
    rain,
    hazard,
    visibilityFactor(input.weatherCode, visibility),
    windFactor(input.gustsKmh),
    ...temperatureFactors(input.feelsLike, input.probability),
    input.frostRisk ? factor('frost', 0, meta().frost.cap, 'frost') : null,
  ];
  const factors = candidates.filter((f): f is ScoreFactor => f !== null && (f.penalty > 0 || f.cap !== null));
  let score = 100 - factors.reduce((sum, f) => sum + f.penalty, 0);
  for (const f of factors) if (f.cap !== null) score = Math.min(score, f.cap);
  return { score: Math.max(0, Math.min(100, score)), factors };
}

export function scoreHour(h: HourlyWeather): ScoreResult {
  return scoreBreakdown({
    feelsLike: h.feels_like,
    gustsKmh: h.wind_gusts_kmh,
    precipitationMm: h.precipitation_mm,
    weatherCode: h.weather_code,
    probability: h.precipitation_probability,
    visibilityM: h.visibility,
    frostRisk: h.frost_risk === true,
  });
}

export function labelFor(score: number | null): MotoLabel | null {
  if (score == null) return null;
  const hit = [...meta().labels].sort((a, b) => b.min_score - a.min_score).find((l) => score >= l.min_score);
  return (hit?.label ?? 'EVITĂ') as MotoLabel;
}

/** Asphalt temperature estimate: solar gain by day, radiative cooling on clear nights. */
export function roadSurfaceTemp(air: number | null, humidity: number | null, code: number | null, precip: number | null, isDay: boolean | null): number | null {
  if (air == null) return null;
  const c = code ?? 0;
  const p = precip ?? 0;
  const night = isDay === false;
  let road: number;
  if (p > 0.5 || WET_ROAD_CODES.has(c)) road = air - 1;
  else if (meta().hazards.snow?.codes.includes(c)) road = air;
  else if (meta().hazards.fog?.codes.includes(c)) road = air - 0.5;
  else if (night) road = c <= 1 ? air - 1.5 : c === 2 ? air - 0.5 : air + 0.5;
  else if (c <= 1) road = air > 15 ? air + 9 : air + 4;
  else road = c === 2 ? air + 4 : air + 1;
  if (!night && (humidity ?? 60) > 85 && p < 0.2) road -= 2;
  return Math.round(road * 10) / 10;
}

/** Dew point from temperature and relative humidity (Magnus formula). */
export function dewPoint(temp: number | null, humidity: number | null): number | null {
  if (temp == null || humidity == null || humidity <= 0) return null;
  const gamma = Math.log(Math.min(humidity, 100) / 100) + (17.62 * temp) / (243.12 + temp);
  return Math.round(((243.12 * gamma) / (17.62 - gamma)) * 10) / 10;
}

/** Ice or hoar frost needs a surface near 0 °C plus moisture (precipitation, frozen codes or dew). */
export function frostRisk(air: number | null, road: number | null, dew: number | null, precip: number | null, code: number | null): boolean {
  const rule = meta().frost;
  const surface = road ?? air;
  if (surface == null || surface > rule.surface_max_c) return false;
  const frozen = [...(meta().hazards.ice?.codes ?? []), ...(meta().hazards.snow?.codes ?? []), RIME_FOG_CODE];
  if (code != null && frozen.includes(code)) return true;
  const negligible = meta().rain.intensity_bands.find((b) => b.name === 'urme')?.min_mm_h ?? 0.05;
  if ((precip ?? 0) >= negligible) return true;
  return dew != null ? surface <= dew + rule.dewpoint_margin_c : surface <= 0;
}

function hazardCap(h: HourlyWeather, names: string[]): number {
  const caps = scoreHour(h).factors.filter((f) => names.includes(f.factor) && f.cap !== null).map((f) => f.cap as number);
  return caps.length ? Math.min(...caps) : Infinity;
}

/** Daily score: worst quarter and overall mean mixed, then the cap a sustained quarter of the hours share. */
export function dailyScore(hours: HourlyWeather[]): number | null {
  const rule = meta().daily ?? { worst_hours_fraction: 0.25, worst_hours_weight: 0.5, hazard_factors: ['rain', 'storm', 'snow', 'ice'], sustained_cap_fraction: 0.25 };
  const scored = hours.filter((h) => h.moto_score != null);
  if (!scored.length) return null;
  const scores = scored.map((h) => h.moto_score as number).sort((a, b) => a - b);
  const mean = (list: number[]): number => list.reduce((s, v) => s + v, 0) / list.length;
  const worst = scores.slice(0, Math.max(1, Math.ceil(scores.length * rule.worst_hours_fraction)));
  let score = rule.worst_hours_weight * mean(worst) + (1 - rule.worst_hours_weight) * mean(scores);
  const caps = scored.map((h) => hazardCap(h, rule.hazard_factors)).sort((a, b) => a - b);
  const cap = caps[Math.max(1, Math.ceil(caps.length * rule.sustained_cap_fraction)) - 1];
  if (Number.isFinite(cap)) score = Math.min(score, cap);
  return Math.max(0, Math.min(100, roundHalfEven(score)));
}

/** Daylight hours of a day; for today only those still ahead (falls back to what is left). */
function ridingHours(dayHours: HourlyWeather[], nowLocal: string | null): HourlyWeather[] {
  const ahead = nowLocal ? dayHours.filter((h) => h.time >= nowLocal.slice(0, 13)) : dayHours;
  const candidates = ahead.length ? ahead : dayHours;
  const daylight = candidates.filter((h) => h.is_day);
  return daylight.length ? daylight : candidates;
}

type Series = ReadonlyArray<number | string | null> | undefined;

/** The subset of the Open-Meteo forecast response this module reads. */
export interface OpenMeteoForecast {
  latitude: number;
  longitude: number;
  timezone: string;
  utc_offset_seconds: number;
  current?: Record<string, number | string | null | undefined>;
  hourly?: Record<string, Series> & { time?: string[] };
  daily?: Record<string, Series> & { time?: string[] };
}

const num = (series: Series, i: number): number | null => {
  const v = series?.[i];
  return typeof v === 'number' && Number.isFinite(v) ? v : null;
};
const str = (series: Series, i: number): string | null => {
  const v = series?.[i];
  return typeof v === 'string' ? v : null;
};
const round1 = (v: number): number => Math.round(v * 10) / 10;
const displayCode = (h: HourlyWeather): number | null =>
  h.weather_code != null && codeIsStale(h.weather_code, h.precipitation_mm, h.precipitation_probability) ? OVERCAST_CODE : h.weather_code;

function buildHourly(raw: OpenMeteoForecast): HourlyWeather[] {
  const h = raw.hourly ?? {};
  const d = raw.daily ?? {};
  const sun = new Map((d.time ?? []).map((date, i) => [date, [str(d.sunrise, i), str(d.sunset, i)]]));
  return (h.time ?? []).map((time, i) => {
    const temp = num(h.temperature_2m, i);
    const humidity = num(h.relative_humidity_2m, i);
    const code = num(h.weather_code, i);
    const precip = num(h.precipitation, i);
    const dewRaw = num(h.dew_point_2m, i);
    const flag = num(h.is_day, i);
    const [rise, set] = sun.get(time.slice(0, 10)) ?? [null, null];
    const isDay = flag != null ? flag === 1 : rise && set ? time >= rise && time < set : null;
    const road = roadSurfaceTemp(temp, humidity, code, precip, isDay);
    const hour: HourlyWeather = {
      time,
      temperature: temp,
      feels_like: num(h.apparent_temperature, i),
      precipitation_mm: precip,
      precipitation_probability: num(h.precipitation_probability, i),
      rain_intensity: precip == null ? null : rainBandOf(precip),
      wind_speed_kmh: num(h.wind_speed_10m, i),
      wind_gusts_kmh: num(h.wind_gusts_10m, i),
      weather_code: code,
      icon: null,
      description: describeCode(code, 'ro'),
      uv_index: num(h.uv_index, i),
      relative_humidity: humidity,
      surface_pressure: num(h.surface_pressure, i),
      pressure_msl: num(h.pressure_msl, i),
      dew_point_2m: dewRaw,
      cloud_cover: num(h.cloud_cover, i),
      visibility: num(h.visibility, i),
      wind_direction_10m: num(h.wind_direction_10m, i),
      is_day: isDay,
      road_surface_temp: road,
      frost_risk: frostRisk(temp, road, dewRaw ?? dewPoint(temp, humidity), precip, code),
      moto_score: null,
      moto_label: null,
      // The browser-direct fallback talks to Open-Meteo alone, so there is no ensemble.
      forecast_confidence: null,
      model_count: null,
    };
    hour.moto_score = scoreHour(hour).score;
    hour.moto_label = labelFor(hour.moto_score);
    return hour;
  });
}

function buildCurrent(raw: OpenMeteoForecast, row: HourlyWeather | undefined): CurrentWeather {
  const c = raw.current ?? {};
  const pick = (key: string, fallback: number | null | undefined): number | null => (typeof c[key] === 'number' ? (c[key] as number) : fallback ?? null);
  const temp = pick('temperature_2m', row?.temperature);
  const feels = pick('apparent_temperature', row?.feels_like);
  const humidity = pick('relative_humidity_2m', row?.relative_humidity);
  const precip = pick('precipitation', row?.precipitation_mm);
  const gusts = pick('wind_gusts_10m', row?.wind_gusts_kmh);
  const speed = pick('wind_speed_10m', row?.wind_speed_kmh);
  const dir = pick('wind_direction_10m', row?.wind_direction_10m);
  const visM = pick('visibility', row?.visibility);
  const flag = pick('is_day', null);
  const isDay = flag != null ? flag === 1 : row?.is_day ?? null;
  const probability = row?.precipitation_probability ?? null;
  const rawCode = pick('weather_code', row?.weather_code);
  const code = rawCode != null && codeIsStale(rawCode, precip, probability) ? OVERCAST_CODE : rawCode;
  const dew = row?.dew_point_2m ?? dewPoint(temp, humidity);
  const road = roadSurfaceTemp(temp, humidity, code, precip, isDay);
  const frost = frostRisk(temp, road, dew, precip, code);
  const { score, factors } = scoreBreakdown({ feelsLike: feels, gustsKmh: gusts, precipitationMm: precip, weatherCode: code, probability, visibilityM: visM, frostRisk: frost });
  const beaufort = speed == null ? null : BEAUFORT_MPS.findIndex((limit) => speed / 3.6 < limit);
  return {
    temperature: temp, feels_like: feels, humidity, wind_speed_kmh: speed, wind_gusts_kmh: gusts,
    wind_direction_deg: dir, wind_direction: dir == null ? null : WIND_LABELS_RO[Math.round(dir / 45) % 8],
    beaufort: beaufort === -1 ? 12 : beaufort, precipitation_mm: precip, precipitation_probability: probability,
    rain_intensity: precip == null ? null : rainBandOf(precip), weather_code: code, description: describeCode(code, 'ro'), icon: null,
    pressure_hpa: pick('pressure_msl', pick('surface_pressure', null)), visibility_km: visM == null ? null : round1(visM / 1000),
    aqi: null, pm10: null, pm2_5: null, ozone: null, eu_aqi: null, us_aqi: null, pollen_index: null,
    uv_index: row?.uv_index ?? null, is_day: isDay, dew_point: dew, frost_risk: frost, moto_score: score, moto_label: labelFor(score),
    score_breakdown: factors, gear_recommendation: null, road_surface_temp: road,
    forecast_confidence: null, model_count: null, sources: ['open-meteo'],
  };
}

function buildDaily(raw: OpenMeteoForecast, hourly: HourlyWeather[], nowLocal: string): DailyWeather[] {
  const d = raw.daily ?? {};
  return (d.time ?? []).map((date, i) => {
    const dayHours = hourly.filter((h) => h.time.startsWith(date));
    const riding = ridingHours(dayHours, date === nowLocal.slice(0, 10) ? nowLocal : null);
    const score = dailyScore(riding);
    const amounts = dayHours.map((h) => h.precipitation_mm).filter((v): v is number => v != null);
    const maxMm = amounts.length ? round1(Math.max(...amounts)) : null;
    const codes = riding.filter((h) => h.is_day).map(displayCode).filter((c): c is number => c != null);
    const code = codes.length ? codes.reduce((a, b) => (CODE_SEVERITY.indexOf(b) > CODE_SEVERITY.indexOf(a) ? b : a)) : num(d.weather_code, i);
    return {
      date, weather_code: code, icon: null, description: describeCode(code, 'ro'),
      temp_max: num(d.temperature_2m_max, i), temp_min: num(d.temperature_2m_min, i),
      feels_max: num(d.apparent_temperature_max, i), feels_min: num(d.apparent_temperature_min, i),
      precipitation_mm: num(d.precipitation_sum, i) ?? 0, precipitation_probability: num(d.precipitation_probability_max, i),
      precipitation_max_mm_h: maxMm, rain_intensity_max: maxMm == null ? null : rainBandOf(maxMm),
      wind_max_kmh: num(d.wind_speed_10m_max, i), wind_gusts_kmh: num(d.wind_gusts_10m_max, i),
      moto_score: score, moto_label: labelFor(score), sunrise: str(d.sunrise, i), sunset: str(d.sunset, i),
    };
  });
}

/** Maps an Open-Meteo forecast to the backend's WeatherResponse shape, scores included. */
export function mapOpenMeteo(raw: OpenMeteoForecast, place: Place, nowMs: number = Date.now()): WeatherResponse {
  const offset = raw.utc_offset_seconds ?? 0;
  const hourly = buildHourly(raw);
  const nowLocal = localNowIso(offset, nowMs);
  return {
    city: place.name,
    latitude: raw.latitude,
    longitude: raw.longitude,
    timezone: raw.timezone,
    utc_offset_seconds: offset,
    current: buildCurrent(raw, hourly[currentHourIndex(hourly, offset, nowMs)]),
    hourly,
    daily: buildDaily(raw, hourly, nowLocal),
  };
}

export async function fetchDirectWeather(place: Place, signal: AbortSignal): Promise<WeatherResponse> {
  const params = new URLSearchParams({
    latitude: place.lat.toFixed(4),
    longitude: place.lon.toFixed(4),
    current: CURRENT_FIELDS,
    hourly: HOURLY_FIELDS,
    daily: DAILY_FIELDS,
    timezone: 'auto',
    forecast_days: String(DIRECT_FORECAST_DAYS),
    wind_speed_unit: 'kmh',
  });
  const controller = new AbortController();
  const timer = window.setTimeout(() => controller.abort(), DIRECT_TIMEOUT_MS);
  const forwardAbort = (): void => controller.abort();
  signal.addEventListener('abort', forwardAbort, { once: true });
  try {
    const res = await fetch(`${OPEN_METEO_URL}?${params.toString()}`, { signal: controller.signal });
    if (!res.ok) throw new Error(`Open-Meteo HTTP ${res.status}`);
    const raw = (await res.json()) as OpenMeteoForecast;
    if (!raw.hourly?.time?.length) throw new Error('Open-Meteo returned no hourly data');
    return mapOpenMeteo(raw, place);
  } catch (err) {
    if (signal.aborted) throw new DOMException('Aborted', 'AbortError');
    if (controller.signal.aborted) throw new Error('Open-Meteo timeout');
    throw err instanceof Error ? err : new Error(String(err));
  } finally {
    window.clearTimeout(timer);
    signal.removeEventListener('abort', forwardAbort);
  }
}
