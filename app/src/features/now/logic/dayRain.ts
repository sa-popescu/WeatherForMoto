import { defineStrings, fmt, pick, type Lang } from '../../../lib/i18n';
import { rainBandOf } from '../../../lib/scoring';
import type { DailyWeather } from '../../../lib/types';
import { bandWord } from './formatting';

// One-line rain summary for a day row: "80% · moderată" (the intensity word
// is the amount class; exact mm/h is in the day sheet) or "fără ploaie".

const DAY_RAIN = defineStrings(
  { none: 'fără ploaie', noAmount: '{p}% · sub 0,1 mm/h', withBand: '{p}% · {band}' },
  { none: 'no rain', noAmount: '{p}% · <0.1 mm/h', withBand: '{p}% · {band}' },
);

const SHOW_CHANCE_FROM_PCT = 20;

export function dayRainLine(d: DailyWeather, lang: Lang): { text: string; wet: boolean } {
  const t = pick(DAY_RAIN, lang);
  const band = d.rain_intensity_max ?? rainBandOf(d.precipitation_max_mm_h);
  const p = d.precipitation_probability;
  if (band !== 'none') return { text: p == null ? bandWord(band, lang) : fmt(t.withBand, { p: Math.round(p), band: bandWord(band, lang) }), wet: true };
  if (p != null && p >= SHOW_CHANCE_FROM_PCT) return { text: fmt(t.noAmount, { p: Math.round(p) }), wet: true };
  return { text: t.none, wet: false };
}
