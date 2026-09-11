import { addMinutesLocal, fmtMm } from '../../../lib/format';
import { fmt, pick, type Lang } from '../../../lib/i18n';
import { rainBandOf } from '../../../lib/scoring';
import type { HourlyWeather, RainBand } from '../../../lib/types';
import { ALERT_TEXTS } from './texts';

// The next rain episode in the coming hours: consecutive hours where rain is
// likely (>= 30 %) or measurable (>= 0.05 mm/h), bridging one dry hour. A
// trace the models give less than a 20 % chance is noise, not an episode.

const WET_PROBABILITY_PCT = 30;
const WET_AMOUNT_MM = 0.05;
/** Same floor as the backend's stale-code rule. */
const TRACE_MIN_PROBABILITY_PCT = 20;
/** From light rain up, an amount always counts, whatever the probability. */
const LIGHT_RAIN_MM = 0.5;
const MAX_DRY_GAP_HOURS = 1;
export const OUTLOOK_HOURS = 24;

export interface RainOutlook {
  kind: 'episode' | 'dry';
  /** Local ISO start / end of the episode (end = one hour after the last wet hour). */
  start: string | null;
  end: string | null;
  startsNow: boolean;
  peakProbability: number;
  peakProbabilityTime: string | null;
  peakMm: number;
  peakMmTime: string | null;
  totalMm: number;
  hours: number;
  band: RainBand;
}

/** The hour's amount, or 0 for a trace with a known chance under 20 %. */
export function countedMm(h: HourlyWeather): number {
  const mm = Math.max(0, h.precipitation_mm ?? 0);
  const p = h.precipitation_probability;
  return mm >= LIGHT_RAIN_MM || p == null || p >= TRACE_MIN_PROBABILITY_PCT ? mm : 0;
}

export function isWetHour(h: HourlyWeather): boolean {
  return (h.precipitation_probability ?? 0) >= WET_PROBABILITY_PCT || countedMm(h) >= WET_AMOUNT_MM;
}

function peak(hours: HourlyWeather[], read: (h: HourlyWeather) => number | null): { value: number; time: string | null } {
  let best = { value: 0, time: null as string | null };
  for (const h of hours) {
    const v = read(h) ?? 0;
    if (v > best.value) best = { value: v, time: h.time };
  }
  return best;
}

function summarize(kind: RainOutlook['kind'], hours: HourlyWeather[], startsNow: boolean): RainOutlook {
  const prob = peak(hours, (h) => h.precipitation_probability);
  const mm = peak(hours, countedMm);
  const total = hours.reduce((sum, h) => sum + countedMm(h), 0);
  const last = hours[hours.length - 1];
  return {
    kind,
    start: kind === 'episode' ? hours[0]?.time ?? null : null,
    end: kind === 'episode' && last ? addMinutesLocal(last.time, 60) : null,
    startsNow,
    peakProbability: Math.round(prob.value),
    peakProbabilityTime: prob.time,
    peakMm: Math.round(mm.value * 10) / 10,
    peakMmTime: mm.time,
    totalMm: Math.round(total * 10) / 10,
    hours: kind === 'episode' ? hours.length : 0,
    band: rainBandOf(mm.value),
  };
}

export function rainOutlook(hourly: ReadonlyArray<HourlyWeather>, startIndex: number, count: number = OUTLOOK_HOURS): RainOutlook {
  const window = hourly.slice(startIndex, startIndex + count);
  const first = window.findIndex(isWetHour);
  if (first < 0) return summarize('dry', window, false);
  let last = first;
  let gap = 0;
  for (let i = first + 1; i < window.length; i += 1) {
    if (isWetHour(window[i])) {
      last = i;
      gap = 0;
    } else {
      gap += 1;
      if (gap > MAX_DRY_GAP_HOURS) break;
    }
  }
  return summarize('episode', window.slice(first, last + 1), first === 0);
}

/** Plain-language consequence for the rider. */
export function rainConsequence(o: RainOutlook, lang: Lang): string {
  const t = pick(ALERT_TEXTS, lang);
  if (o.kind === 'dry' || o.band === 'none') {
    return o.peakProbability > 0 ? fmt(t.consequenceDryChance, { p: o.peakProbability }) : t.consequenceDry;
  }
  const byBand: Record<Exclude<RainBand, 'none'>, string> = {
    urme: t.consequenceUrme,
    slaba: t.consequenceSlaba,
    moderata: t.consequenceModerata,
    puternica: t.consequencePuternica,
  };
  const body = fmt(byBand[o.band], { total: fmtMm(o.totalMm, lang), hours: o.hours });
  if (o.peakProbability >= WET_PROBABILITY_PCT) return body;
  return fmt(t.unlikelyPrefix, { p: o.peakProbability }) + body.charAt(0).toLowerCase() + body.slice(1);
}
