import { fmtMm } from '../../lib/format';
import { fmt, pick, type Lang } from '../../lib/i18n';
import { rainBandOf } from '../../lib/scoring';
import type { HourlyWeather, RainBand } from '../../lib/types';
import { CORE } from '../../i18n/core';
import { RS } from './strings';

// The owner's rule for rain: never a percentage alone. Every line carries the
// chance, the amount and the intensity word, or says plainly there is none.

type RainSlot = Pick<HourlyWeather, 'precipitation_probability' | 'precipitation_mm' | 'rain_intensity'>;

const BAND_KEY: Record<Exclude<RainBand, 'none'>, 'bandUrme' | 'bandSlaba' | 'bandModerata' | 'bandPuternica'> = {
  urme: 'bandUrme',
  slaba: 'bandSlaba',
  moderata: 'bandModerata',
  puternica: 'bandPuternica',
};

export interface RainLine {
  text: string;
  /** Measurable rain is expected. */
  wet: boolean;
}

export function rainLine(slot: RainSlot, lang: Lang): RainLine {
  const s = pick(RS, lang);
  const core = pick(CORE, lang);
  const mm = slot.precipitation_mm ?? 0;
  const prob = slot.precipitation_probability;
  const band: RainBand = slot.rain_intensity && slot.rain_intensity !== 'none' ? slot.rain_intensity : rainBandOf(mm);
  if (band === 'none') {
    if (prob != null && prob > 0) return { text: fmt(s.rainNoneChance, { prob: Math.round(prob) }), wet: false };
    return { text: s.rainNone, wet: false };
  }
  const vars = { mm: fmtMm(mm, lang), band: core[BAND_KEY[band]] };
  if (prob == null) return { text: fmt(s.rainAmount, vars), wet: true };
  return { text: fmt(s.rainChance, { ...vars, prob: Math.round(prob) }), wet: true };
}
