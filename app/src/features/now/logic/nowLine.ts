import type { CurrentWeather } from '../../../lib/types';

// The weather now in a few words, under the temperature: the sky, then what
// matters on a bike if anything does (rain the sources disagree on, fog the
// code does not show, the wind, ice on the road). "Senin, vânt slab din NE".

type Lang = 'ro' | 'en';

/** Gusts from here are named with their speed; below, the steady wind in words. */
export const GUST_NAMED_KMH = 40;
const WIND_MODERATE_KMH = 20;
const WIND_STRONG_KMH = 40;

const WORDS = {
  ro: {
    maybeRain: 'posibil ploaie',
    fog: 'ceață',
    gusts: 'rafale de {g} km/h',
    calm: 'vânt slab',
    moderate: 'vânt moderat',
    strong: 'vânt puternic',
    from: 'din',
    ice: 'risc de polei',
  },
  en: {
    maybeRain: 'rain possible',
    fog: 'fog',
    gusts: 'gusts of {g} km/h',
    calm: 'light wind',
    moderate: 'moderate wind',
    strong: 'strong wind',
    from: 'from the',
    ice: 'black ice risk',
  },
} as const;

type LineInput = Pick<
  CurrentWeather,
  'wind_speed_kmh' | 'wind_gusts_kmh' | 'wind_direction' | 'frost_risk' | 'verdict'
>;

/** One short line: "<sky>, <what else matters>". `sky` is the sky already in words. */
export function nowLine(current: LineInput, sky: string, lang: Lang, skyIsFog = false): string {
  const w = WORDS[lang];
  const parts: string[] = [sky];
  const rain = current.verdict?.precipitation;
  if (rain?.state === 'uncertain') parts.push(w.maybeRain);
  if (!skyIsFog && current.verdict?.fog?.state === 'fog') parts.push(w.fog);

  const gust = current.wind_gusts_kmh;
  const speed = current.wind_speed_kmh;
  if (gust != null && gust >= GUST_NAMED_KMH) {
    parts.push(w.gusts.replace('{g}', String(Math.round(gust))));
  } else if (speed != null) {
    const word = speed >= WIND_STRONG_KMH ? w.strong : speed >= WIND_MODERATE_KMH ? w.moderate : w.calm;
    const dir = current.wind_direction;
    parts.push(dir && speed >= 5 ? `${word} ${w.from} ${dir}` : word);
  }
  if (current.frost_risk) parts.push(w.ice);
  return parts.join(', ');
}
