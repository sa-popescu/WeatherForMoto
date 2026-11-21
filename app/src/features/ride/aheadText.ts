import { fmt, type Lang } from '../../lib/i18n';
import type { RainBand } from '../../lib/types';
import type { AheadWarning } from './logic/ahead';
import { RIDE_S } from './strings';
import { spokenKmh, spokenMinutes, spokenWhere } from './voice';

// A warning ahead in words: a short line for the screen, a sentence for the voice.

/** Two minutes or less away is "right now". */
const NOW_MIN = 2;

function rainWhat(band: RainBand | null, lang: Lang): string {
  const s = RIDE_S[lang];
  if (band === 'puternica') return s.rainPuternica;
  if (band === 'moderata') return s.rainModerata;
  if (band === 'slaba') return s.rainSlaba;
  return s.rainUrme;
}

/** "Ploaie moderată în ~20 min · Pitești" */
export function aheadLine(w: AheadWarning, lang: Lang): string {
  const s = RIDE_S[lang];
  const when = w.minutes <= NOW_MIN ? s.aheadNow : fmt(s.aheadIn, { min: w.minutes });
  const place = w.name;
  if (w.kind === 'frost') return fmt(s.aheadFrost, { when, place });
  if (w.kind === 'gust') return fmt(s.aheadGust, { gust: w.value ?? '', when, place });
  if (w.kind === 'score') return fmt(s.aheadScore, { score: w.value ?? '', when, place });
  return fmt(s.aheadRain, { what: rainWhat(w.band, lang), when, place });
}

/** "Ploaie moderată în aproximativ 20 de minute, pe la Pitești." */
export function aheadSentence(w: AheadWarning, lang: Lang): string {
  const s = RIDE_S[lang];
  const when = w.minutes <= NOW_MIN ? s.aheadNow : fmt(s.sayIn, { dur: spokenMinutes(w.minutes, lang) });
  const where = spokenWhere(w.name, lang);
  if (w.kind === 'frost') return fmt(s.sayAheadFrost, { when, where });
  if (w.kind === 'gust') return fmt(s.sayAheadGust, { gust: spokenKmh(w.value ?? 0, lang), when, where });
  if (w.kind === 'score') return fmt(s.sayAheadScore, { when, where });
  return fmt(s.sayAheadRain, { what: rainWhat(w.band, lang), when, where });
}
