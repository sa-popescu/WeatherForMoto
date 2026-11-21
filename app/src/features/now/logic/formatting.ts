import { CORE } from '../../../i18n/core';
import { fmtMm, fmtNumber, hourOf, weekdayWord } from '../../../lib/format';
import { fmt, pick, type Lang } from '../../../lib/i18n';
import { bandRange, rainBandOf } from '../../../lib/scoring';
import type { RainBand } from '../../../lib/types';
import { S_NOW } from '../strings';
import { nextDate } from './daylight';
import { TEXTS } from './texts';

/** "300 m" under a kilometre, "1,2 km" / "12 km" above. */
export function fmtVisibility(metres: number, lang: Lang): string {
  if (metres < 1000) return `${Math.round(metres / 10) * 10} m`;
  return `${fmtNumber(metres / 1000, lang, metres < 10_000 ? 1 : 0)} km`;
}

/**
 * "16:00" for the same day as `todayIso`, "mâine 16:00" for the next one and
 * "marți 16:00" further out (the desktop hour table reaches the whole forecast).
 */
export function timeLabel(localIso: string, todayIso: string, lang: Lang): string {
  const hour = hourOf(localIso);
  const date = localIso.slice(0, 10);
  const today = todayIso.slice(0, 10);
  if (date === today) return hour;
  const t = pick(TEXTS, lang);
  if (date === nextDate(today)) return fmt(t.tomorrowAt, { hour });
  return fmt(t.dayAt, { day: weekdayWord(date, lang), hour });
}

export function capitalize(text: string): string {
  return text ? text.charAt(0).toUpperCase() + text.slice(1) : text;
}

/** The intensity as an adjective: for chips and table rows ("80% · moderată"). */
export function bandWord(band: RainBand | null, lang: Lang): string {
  const s = pick(CORE, lang);
  const words: Record<RainBand, string> = { none: s.bandNone, urme: s.bandUrme, slaba: s.bandSlaba, moderata: s.bandModerata, puternica: s.bandPuternica };
  return words[band ?? 'none'];
}

/** A band's range: "sub 0,5", "0,5–2,5", "peste 7,5" (optionally with the unit). */
export function bandRangeLabel(band: Exclude<RainBand, 'none'>, lang: Lang, withUnit = false): string {
  const s = pick(S_NOW, lang);
  const { min, max } = bandRange(band);
  const unit = withUnit ? ' mm/h' : '';
  if (band === 'urme' && max != null) return `${fmt(s.scaleBelow, { v: fmtMm(max, lang) })}${unit}`;
  if (max == null) return `${fmt(s.scaleAbove, { v: fmtMm(min, lang) })}${unit}`;
  return `${fmtMm(min, lang)}–${fmtMm(max, lang)}${unit}`;
}

/**
 * The intensity as a noun phrase: for sentences ("ploaie moderată").
 * The adjective above belongs to a column header, not to a verb — "plouă
 * (puternică)" agrees with nothing, and "plouă (urme)" is not Romanian at all.
 */
export function rainNoun(band: RainBand | null, lang: Lang): string {
  const s = pick(CORE, lang);
  const words: Record<RainBand, string> = {
    none: s.rainNounNone,
    urme: s.rainNounUrme,
    slaba: s.rainNounSlaba,
    moderata: s.rainNounModerata,
    puternica: s.rainNounPuternica,
  };
  return words[band ?? 'none'];
}

/** Opposite of capitalize: a sentence folded into one that already started. */
export function uncapitalize(text: string): string {
  return text ? text.charAt(0).toLowerCase() + text.slice(1) : text;
}

export interface RainPeak {
  probability: number | null;
  mm: number | null;
}

/**
 * Rain as probability AND amount, never one without the other:
 * "80% șanse, până la 3,2 mm/h, ploaie moderată", "70% șanse, dar doar urme (0,2 mm/h)".
 */
export function rainPhrase(peak: RainPeak, lang: Lang): string {
  const t = pick(TEXTS, lang);
  const mm = Math.max(0, peak.mm ?? 0);
  const band = rainBandOf(mm);
  const vars = { p: Math.round(peak.probability ?? 0), mm: fmtMm(mm, lang), band: rainNoun(band, lang) };
  if (peak.probability == null) return fmt(t.rainAmountOnly, vars);
  if (band === 'none') return fmt(t.rainNoAmount, vars);
  if (band === 'urme') return fmt(t.rainTraces, vars);
  return fmt(t.rainUpTo, vars);
}

/** Largest / smallest of a list with gaps. */
export function maxOf(values: ReadonlyArray<number | null | undefined>): number | null {
  const known = values.filter((v): v is number => v != null && Number.isFinite(v));
  return known.length ? Math.max(...known) : null;
}

export function minOf(values: ReadonlyArray<number | null | undefined>): number | null {
  const known = values.filter((v): v is number => v != null && Number.isFinite(v));
  return known.length ? Math.min(...known) : null;
}
