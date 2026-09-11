import { fmt, type Lang } from '../../lib/i18n';
import { relativeSpan } from './hazards';
import { frameClock } from './radar';
import type { MAP_STRINGS } from './strings';

// Small text builders shared by the radar panel and the hazard sheets.

export type MapText = (typeof MAP_STRINGS)['en'];

/** "acum 5 min", "acum 2 h", "acum o zi". */
export function agoText(s: MapText, ms: number): string {
  const { n, unit } = relativeSpan(ms);
  if (unit === 'min') return n < 1 ? s.radarJustNow : fmt(s.agoMin, { n });
  if (unit === 'h') return fmt(s.agoH, { n });
  return n === 1 ? s.agoDay : fmt(s.agoD, { n });
}

/** "în 40 min", "în 3 h", "într-o zi". */
export function inText(s: MapText, ms: number): string {
  const { n, unit } = relativeSpan(ms);
  if (unit === 'min') return fmt(s.inMin, { n: Math.max(1, n) });
  if (unit === 'h') return fmt(s.inH, { n });
  return n === 1 ? s.inDay : fmt(s.inD, { n });
}

/** "acum 20 min", "chiar acum" or "peste 10 min" for a radar frame. */
export function radarOffsetText(s: MapText, minutes: number): string {
  if (minutes > 0) return fmt(s.radarAhead, { min: minutes });
  if (minutes > -3) return s.radarJustNow;
  return fmt(s.radarAgo, { min: -minutes });
}

export function clockText(ms: number, lang: Lang): string {
  return frameClock(ms / 1000, lang);
}
