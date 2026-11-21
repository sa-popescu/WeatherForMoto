import { localNowIso } from '../../../lib/format';
import { rainBandOf, rainImpact } from '../../../lib/scoring';
import type { HourlyWeather, RainBand } from '../../../lib/types';
import { pickSlot } from '../../route/timing';
import type { PointWeather, SamplePoint } from '../../route/types';

// The weather ahead on a followed route: at each weather point of the plan
// the rider will reach within the next hour, the forecast hour of the moment
// they get there, and the first thing in it worth a warning. The forecasts
// are the planner's, taken along when the ride starts.

/** What a ride keeps of a forecast hour: only what the warnings read. */
export interface AheadSlot {
  /** Local time of the point, "YYYY-MM-DDTHH:MM". */
  time: string;
  band: RainBand;
  rainProb: number | null;
  gustKmh: number | null;
  frost: boolean;
  score: number | null;
}

export interface AheadPoint {
  km: number;
  name: string;
  utcOffsetSeconds: number;
  slots: AheadSlot[];
}

export type AheadKind = 'frost' | 'rain' | 'gust' | 'score';

export interface AheadWarning {
  kind: AheadKind;
  /** Riding minutes until the point. */
  minutes: number;
  km: number;
  name: string;
  /** The rain's band, for kind rain. */
  band: RainBand | null;
  /** The gusts, for kind gust; the score, for kind score. */
  value: number | null;
  /** Same warning, same place: said once, not every second. */
  key: string;
}

/** How far ahead, in riding time, the warnings look. */
export const AHEAD_HORIZON_MIN = 60;
/** Gusts from here unsettle a motorcycle (the alerts' default threshold too). */
const GUST_WARN_KMH = 50;
/** Under this the hour is EVITĂ. */
const SCORE_WARN = 40;
/** Forecast hours kept per point: a long day's ride. */
const HOURS_KEPT = 24;

export function toAheadSlot(h: HourlyWeather): AheadSlot {
  return {
    time: h.time,
    band: h.rain_intensity ?? rainBandOf(h.precipitation_mm),
    rainProb: h.precipitation_probability,
    gustKmh: h.wind_gusts_kmh,
    frost: h.frost_risk === true,
    score: h.moto_score,
  };
}

/** The plan's points with their next hours of forecast; points with no forecast are left out. */
export function aheadPoints(points: readonly SamplePoint[], weather: Readonly<Record<string, PointWeather | undefined>>, nowMs: number): AheadPoint[] {
  const out: AheadPoint[] = [];
  for (const p of points) {
    const forecast = weather[p.id]?.forecast;
    if (!forecast) continue;
    const from = localNowIso(forecast.utcOffsetSeconds, nowMs - 3_600_000).slice(0, 13);
    const to = localNowIso(forecast.utcOffsetSeconds, nowMs + HOURS_KEPT * 3_600_000).slice(0, 13);
    const slots = forecast.hourly.filter((h) => h.time.slice(0, 13) >= from && h.time.slice(0, 13) <= to).map(toAheadSlot);
    if (slots.length > 0) out.push({ km: p.km, name: p.name, utcOffsetSeconds: forecast.utcOffsetSeconds, slots });
  }
  return out.sort((a, b) => a.km - b.km);
}

const HEAVY: ReadonlySet<RainBand> = new Set(['moderata', 'puternica']);

/** The one thing worth saying about an hour, most serious first. */
function slotWarning(slot: AheadSlot): Pick<AheadWarning, 'kind' | 'band' | 'value'> | null {
  if (slot.frost) return { kind: 'frost', band: null, value: null };
  const impact = rainImpact(slot.rainProb, slot.band);
  const rain = impact === 'medium' || impact === 'high';
  // A high chance with no amount still means rain of some kind.
  const band: RainBand = slot.band === 'none' ? 'urme' : slot.band;
  if (rain && HEAVY.has(band)) return { kind: 'rain', band, value: null };
  if (slot.gustKmh !== null && slot.gustKmh >= GUST_WARN_KMH) return { kind: 'gust', band: null, value: Math.round(slot.gustKmh) };
  if (rain) return { kind: 'rain', band, value: null };
  if (slot.score !== null && slot.score < SCORE_WARN) return { kind: 'score', band: null, value: slot.score };
  return null;
}

/**
 * The first warning at a point ahead within the horizon, or null. `speedKmh`
 * is the speed the rider is expected to keep (see rideSpeedKmh).
 */
export function warningAhead(
  points: readonly AheadPoint[],
  progressKm: number,
  speedKmh: number,
  nowMs: number,
  horizonMin: number = AHEAD_HORIZON_MIN,
): AheadWarning | null {
  if (speedKmh <= 0) return null;
  const reachKm = progressKm + (speedKmh * horizonMin) / 60;
  for (const p of points) {
    if (p.km < progressKm || p.km > reachKm) continue;
    const minutes = ((p.km - progressKm) / speedKmh) * 60;
    const slot = pickSlot(p.slots, localNowIso(p.utcOffsetSeconds, nowMs + minutes * 60_000));
    const found = slot ? slotWarning(slot) : null;
    if (found) return { ...found, minutes: Math.round(minutes), km: p.km, name: p.name, key: `${found.kind}|${p.km.toFixed(1)}` };
  }
  return null;
}
