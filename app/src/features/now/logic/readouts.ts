import { scoreBreakdown } from '../../../lib/directWeather';
import { defineStrings, fmt, pick, type Lang } from '../../../lib/i18n';
import { tierOf, type Tier } from '../../../lib/scoring';
import type { CurrentWeather } from '../../../lib/types';

// Words and tiers for the instrument readouts (air, gusts, asphalt, extras).

export const READOUT_TEXTS = defineStrings(
  {
    gustCalm: 'slabe',
    gustModerate: 'moderate',
    gustStrong: 'puternice',
    gustVeryStrong: 'foarte puternice',
    gustDangerous: 'periculoase',
    gustFrom: 'din {dir}, {word}',
    asphaltDry: 'uscat',
    asphaltWet: 'ud, aderență redusă',
    asphaltFrost: 'risc de polei',
    uvLow: 'scăzut',
    uvModerate: 'moderat',
    uvHigh: 'ridicat',
    uvVeryHigh: 'foarte ridicat',
    uvExtreme: 'extrem',
    aqiGood: 'bun',
    aqiFair: 'acceptabil',
    aqiModerate: 'moderat',
    aqiPoor: 'slab',
    aqiVeryPoor: 'foarte slab',
    aqiExtreme: 'extrem de slab',
    pollenLow: 'scăzut',
    pollenModerate: 'moderat',
    pollenHigh: 'ridicat',
    pollenVeryHigh: 'foarte ridicat',
  },
  {
    gustCalm: 'light',
    gustModerate: 'moderate',
    gustStrong: 'strong',
    gustVeryStrong: 'very strong',
    gustDangerous: 'dangerous',
    gustFrom: 'from {dir}, {word}',
    asphaltDry: 'dry',
    asphaltWet: 'wet, less grip',
    asphaltFrost: 'black ice risk',
    uvLow: 'low',
    uvModerate: 'moderate',
    uvHigh: 'high',
    uvVeryHigh: 'very high',
    uvExtreme: 'extreme',
    aqiGood: 'good',
    aqiFair: 'fair',
    aqiModerate: 'moderate',
    aqiPoor: 'poor',
    aqiVeryPoor: 'very poor',
    aqiExtreme: 'extremely poor',
    pollenLow: 'low',
    pollenModerate: 'moderate',
    pollenHigh: 'high',
    pollenVeryHigh: 'very high',
  },
);

const DIRS: Record<Lang, string[]> = {
  ro: ['N', 'NE', 'E', 'SE', 'S', 'SV', 'V', 'NV'],
  en: ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW'],
};

export function windDirLabel(deg: number | null, lang: Lang): string {
  return deg == null ? '–' : DIRS[lang][Math.round(deg / 45) % 8];
}

/** Picks the word of the first band whose upper limit the value stays under. */
function band(value: number, limits: number[], words: string[]): string {
  const i = limits.findIndex((limit) => value < limit);
  return words[i === -1 ? words.length - 1 : i];
}

export function gustText(gust: number | null, deg: number | null, lang: Lang): string {
  const w = pick(READOUT_TEXTS, lang);
  const word = band(gust ?? 0, [20, 35, 50, 70], [w.gustCalm, w.gustModerate, w.gustStrong, w.gustVeryStrong, w.gustDangerous]);
  return deg == null ? word : fmt(w.gustFrom, { dir: windDirLabel(deg, lang), word });
}

export function uvWord(uv: number, lang: Lang): string {
  const w = pick(READOUT_TEXTS, lang);
  return band(uv, [3, 6, 8, 11], [w.uvLow, w.uvModerate, w.uvHigh, w.uvVeryHigh, w.uvExtreme]);
}

/** European AQI bands (0-20 good ... above 100 extremely poor). */
export function aqiWord(aqi: number, lang: Lang): string {
  const w = pick(READOUT_TEXTS, lang);
  return band(aqi, [20, 40, 60, 80, 100], [w.aqiGood, w.aqiFair, w.aqiModerate, w.aqiPoor, w.aqiVeryPoor, w.aqiExtreme]);
}

/** Total pollen grains / m³ as the backend sums them. */
export function pollenWord(grains: number, lang: Lang): string {
  const w = pick(READOUT_TEXTS, lang);
  return band(grains, [20, 100, 500], [w.pollenLow, w.pollenModerate, w.pollenHigh, w.pollenVeryHigh]);
}

export function asphalt(current: CurrentWeather, lang: Lang): { word: string; tier: Tier } {
  const w = pick(READOUT_TEXTS, lang);
  if (current.frost_risk) return { word: w.asphaltFrost, tier: 'evita' };
  const wet = (current.precipitation_mm ?? 0) >= 0.05 || (current.rain_intensity != null && current.rain_intensity !== 'none');
  return wet ? { word: w.asphaltWet, tier: 'atentie' } : { word: w.asphaltDry, tier: 'ideal' };
}

/** Tier of a single factor, scored alone with the published rules. */
export function airTier(feels: number | null): Tier | null {
  return feels == null ? null : tierOf(scoreBreakdown({ feelsLike: feels, gustsKmh: null, precipitationMm: null, weatherCode: null, probability: null }).score);
}

export function gustTier(gust: number | null): Tier | null {
  return gust == null ? null : tierOf(scoreBreakdown({ feelsLike: null, gustsKmh: gust, precipitationMm: null, weatherCode: null, probability: null }).score);
}
