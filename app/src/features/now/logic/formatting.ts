import { CORE } from '../../../i18n/core';
import { fmtMm, fmtNumber, hourOf } from '../../../lib/format';
import { fmt, pick, type Lang } from '../../../lib/i18n';
import { rainBandOf } from '../../../lib/scoring';
import type { RainBand } from '../../../lib/types';
import { TEXTS } from './texts';

export function capitalize(text: string): string {
  return text ? text.charAt(0).toUpperCase() + text.slice(1) : text;
}

/** "300 m" under a kilometre, "1,2 km" / "12 km" above. */
export function fmtVisibility(metres: number, lang: Lang): string {
  if (metres < 1000) return `${Math.round(metres / 10) * 10} m`;
  return `${fmtNumber(metres / 1000, lang, metres < 10_000 ? 1 : 0)} km`;
}

/** "16:00" for the same day as `todayIso`, "mâine 16:00" for the next one. */
export function timeLabel(localIso: string, todayIso: string, lang: Lang): string {
  const hour = hourOf(localIso);
  return localIso.slice(0, 10) === todayIso.slice(0, 10) ? hour : fmt(pick(TEXTS, lang).tomorrowAt, { hour });
}

export function bandWord(band: RainBand | null, lang: Lang): string {
  const s = pick(CORE, lang);
  const words: Record<RainBand, string> = { none: s.bandNone, urme: s.bandUrme, slaba: s.bandSlaba, moderata: s.bandModerata, puternica: s.bandPuternica };
  return words[band ?? 'none'];
}

export interface RainPeak {
  probability: number | null;
  mm: number | null;
}

/**
 * Rain as probability AND amount, never one without the other:
 * "80% șanse, până la 3,2 mm/h (moderată)", "70% șanse, dar doar urme (0,2 mm/h)".
 */
export function rainPhrase(peak: RainPeak, lang: Lang): string {
  const t = pick(TEXTS, lang);
  const mm = Math.max(0, peak.mm ?? 0);
  const band = rainBandOf(mm);
  const vars = { p: Math.round(peak.probability ?? 0), mm: fmtMm(mm, lang), band: bandWord(band, lang) };
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
