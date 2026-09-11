import { fmtTemp, hourOf, minutesBetween, addMinutesLocal } from '../../../lib/format';
import { fmt, pick, type Lang } from '../../../lib/i18n';
import { getScoringMeta, rainBandOf, rainImpact, type RainImpact } from '../../../lib/scoring';
import type { HourlyWeather } from '../../../lib/types';
import { maxOf, minOf, rainPhrase } from './formatting';
import { GEAR_TEXTS } from './gearStrings';

// Gear checklist, a TypeScript port of the legacy gearRecommendationClient,
// driven by the ride hours instead of a single reading: rain by probability
// x intensity, cold (coldest hour), heat (hottest hour), wind, UV, darkness.

export type GearUrgency = 'required' | 'recommended' | 'advice';

export interface GearRec {
  id: keyof typeof GEAR_TEXTS.ro;
  urgency: GearUrgency;
  item: string;
  reason: string;
}

export interface RideConditions {
  minFeels: number | null;
  maxFeels: number | null;
  maxGust: number | null;
  rainImpact: RainImpact;
  rainProbability: number | null;
  rainMm: number | null;
  /** First hour with a rain impact; null when it rains now. */
  rainFrom: string | null;
  fog: boolean;
  maxUv: number | null;
  uvFrom: string | null;
  uvTo: string | null;
  /** It is dark right now. */
  night: boolean;
  /** Dark now, or sunset within the next two hours. */
  dark: boolean;
  sunset: string | null;
  frost: boolean;
}

const IMPACT_RANK: Record<RainImpact, number> = { none: 0, low: 1, medium: 2, high: 3 };
const HIGH_UV = 6;
const SUNSET_SOON_MIN = 120;
const FOG_VISIBILITY_M = 1000;

export function rideConditions(hours: ReadonlyArray<HourlyWeather>, opts: { night: boolean; sunset: string | null; nowIso: string }): RideConditions {
  const impactOf = (h: HourlyWeather): RainImpact => rainImpact(h.precipitation_probability, rainBandOf(h.precipitation_mm));
  const wet = hours.filter((h) => impactOf(h) !== 'none');
  const worst = wet.reduce<RainImpact>((acc, h) => (IMPACT_RANK[impactOf(h)] > IMPACT_RANK[acc] ? impactOf(h) : acc), 'none');
  const sunny = hours.filter((h) => (h.uv_index ?? 0) >= HIGH_UV);
  const fogCodes = getScoringMeta().hazards.fog?.codes ?? [];
  const toSunset = opts.sunset ? minutesBetween(opts.nowIso, opts.sunset) : null;
  return {
    minFeels: minOf(hours.map((h) => h.feels_like)),
    maxFeels: maxOf(hours.map((h) => h.feels_like)),
    maxGust: maxOf(hours.map((h) => h.wind_gusts_kmh)),
    rainImpact: worst,
    rainProbability: maxOf(wet.map((h) => h.precipitation_probability)),
    rainMm: maxOf(wet.map((h) => h.precipitation_mm)),
    rainFrom: wet.length && wet[0].time.slice(0, 13) > opts.nowIso.slice(0, 13) ? wet[0].time : null,
    fog: hours.some((h) => (h.weather_code != null && fogCodes.includes(h.weather_code)) || (h.visibility ?? Infinity) < FOG_VISIBILITY_M),
    maxUv: maxOf(sunny.map((h) => h.uv_index)),
    uvFrom: sunny[0]?.time ?? null,
    uvTo: sunny.length ? addMinutesLocal(sunny[sunny.length - 1].time, 60) : null,
    night: opts.night,
    dark: opts.night || (toSunset != null && toSunset > 0 && toSunset <= SUNSET_SOON_MIN),
    sunset: opts.sunset,
    frost: hours.some((h) => h.frost_risk === true),
  };
}

const URGENCY_ORDER: Record<GearUrgency, number> = { required: 0, recommended: 1, advice: 2 };

export function gearFor(c: RideConditions, lang: Lang): GearRec[] {
  const t = pick(GEAR_TEXTS, lang);
  const recs: GearRec[] = [];
  const add = (id: GearRec['id'], urgency: GearUrgency, reason: string): void => {
    recs.push({ id, urgency, item: t[id], reason });
  };
  const cold = c.minFeels ?? 20;
  const hot = c.maxFeels ?? cold;
  const f = fmtTemp(cold, lang);

  if (c.rainImpact !== 'none') {
    const rain = rainPhrase({ probability: c.rainProbability, mm: c.rainMm }, lang);
    const why = c.rainFrom ? fmt(t.whyRain, { rain, hour: hourOf(c.rainFrom) }) : fmt(t.whyRainNow, { rain });
    if (c.rainImpact === 'low') add('pinlock', 'advice', fmt(t.whyDrops, { rain }));
    else {
      const urgency: GearUrgency = c.rainImpact === 'high' ? 'required' : 'recommended';
      add('rain_suit', urgency, why);
      add('rain_gloves', urgency, t.whyWetHands);
    }
  }
  const soaked = IMPACT_RANK[c.rainImpact] >= IMPACT_RANK.medium;

  if (cold < 0) {
    add('jacket_winter', 'required', fmt(t.whyExtremeCold, { f }));
    add('base_full', 'required', t.whyHypothermia);
  } else if (cold < 10) {
    add('jacket_liner', 'required', fmt(t.whyCold, { f }));
    add('base', 'recommended', t.whyThermal);
  } else if (cold < 18) add('jacket_3s', 'recommended', fmt(t.whyCool, { f }));
  else if (hot < 28) add('jacket_std', 'advice', fmt(t.whyMild, { f }));
  else add('jacket_mesh', 'recommended', fmt(t.whyHeat, { f: fmtTemp(hot, lang) }));
  if (hot >= 28) add('hydration', 'recommended', t.whyDehydration);

  if (cold < 5) add('gloves_winter', 'required', fmt(t.whyNumb, { f }));
  else if (cold < 12) add('gloves_thermal', 'recommended', fmt(t.whyColdHands, { f }));
  else if (!soaked) add('gloves_std', 'advice', t.whyAlways);

  if (cold < 5) add('pants_winter', 'required', t.whyPantsWinter);
  else if (cold < 15) add('pants_cool', 'recommended', t.whyPantsCool);
  else add('pants_std', 'advice', t.whyImpact);

  const gust = c.maxGust ?? 0;
  if (gust > 50) add('visor_full', 'required', fmt(t.whyGustsStrong, { g: Math.round(gust) }));
  else if (gust > 35) add('visor_half', 'recommended', fmt(t.whyGusts, { g: Math.round(gust) }));
  if (c.fog) add('vest', 'required', t.whyFog);
  if (cold < 3 || c.frost) add('tyres', 'required', t.whyIce);
  if (c.maxUv != null && c.uvFrom && c.uvTo) add('uv_visor', 'advice', fmt(t.whyUv, { uv: Math.round(c.maxUv), from: hourOf(c.uvFrom), to: hourOf(c.uvTo) }));
  if (c.dark && !c.fog) {
    add('dark_visor', 'recommended', !c.night && c.sunset ? fmt(t.whySunset, { sunset: hourOf(c.sunset) }) : t.whyNight);
  }

  return recs.sort((a, b) => URGENCY_ORDER[a.urgency] - URGENCY_ORDER[b.urgency]);
}
