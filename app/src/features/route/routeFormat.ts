import { dayMonth, dayName, fmtNumber } from '../../lib/format';
import { fmt, type Lang } from '../../lib/i18n';
import { tierOf, type Tier } from '../../lib/scoring';
import type { MotoLabel } from '../../lib/types';

// Small formatting helpers for this feature (numbers, durations, ages, labels).

export function fmtKm(km: number, lang: Lang): string {
  return fmtNumber(Math.round(km), lang);
}

export function fmtDuration(minutes: number, t: { durHm: string; durM: string }): string {
  const h = Math.floor(minutes / 60);
  const m = Math.round(minutes % 60);
  return h > 0 ? fmt(t.durHm, { h, m: String(m).padStart(2, '0') }) : fmt(t.durM, { m });
}

export function ageText(sinceMs: number, nowMs: number, t: { ageNow: string; ageMin: string; ageHours: string; ageDays: string }): string {
  const min = Math.max(0, Math.round((nowMs - sinceMs) / 60_000));
  if (!Number.isFinite(min) || min < 2) return t.ageNow;
  if (min < 60) return fmt(t.ageMin, { n: min });
  const hours = Math.round(min / 60);
  if (hours < 48) return fmt(t.ageHours, { n: hours });
  return fmt(t.ageDays, { n: Math.round(hours / 24) });
}

/** "Azi", "Mâine", then "Lun 14". */
export function dayChipLabel(date: string, index: number, lang: Lang, t: { today: string; tomorrow: string }): string {
  if (index === 0) return t.today;
  if (index === 1) return t.tomorrow;
  return `${dayName(date, lang)} ${dayMonth(date, lang).split(' ')[0]}`;
}

type TierWords = { tierIdeal: string; tierOk: string; tierAtentie: string; tierEvita: string };

const TIER_KEY: Record<Tier, keyof TierWords> = { ideal: 'tierIdeal', ok: 'tierOk', atentie: 'tierAtentie', evita: 'tierEvita' };
const LABEL_TIER: Record<MotoLabel, Tier> = { IDEAL: 'ideal', OK: 'ok', 'ATENȚIE': 'atentie', 'EVITĂ': 'evita' };

/** The backend's label as a word in the UI language (falls back to the score band). */
export function scoreWord(label: MotoLabel | null | undefined, score: number | null | undefined, words: TierWords): string {
  const tier = label ? LABEL_TIER[label] : tierOf(score);
  return tier ? words[TIER_KEY[tier]] : '–';
}

/** Class suffix for tier-coloured SVG and map strokes. */
export function tierClass(score: number | null | undefined): Tier | 'none' {
  return tierOf(score) ?? 'none';
}
