import defaultMeta from './meta-scoring.default.json';
import type { RainBand } from './types';

// The backend is the single source of truth for scores. This module only
// mirrors its published constants (GET /meta/scoring) for presentation:
// tier colours, rain band words, the rain impact matrix in the explainer.

export type Tier = 'ideal' | 'ok' | 'atentie' | 'evita';
export type RainImpact = 'none' | 'low' | 'medium' | 'high';

export interface ScoringMeta {
  version: number;
  labels: { label: string; min_score: number }[];
  rain: {
    intensity_bands: { name: string; min_mm_h: number; max_mm_h: number | null }[];
    impact_matrix: { columns: string[]; rows: string[][] };
    impact_caps: Record<string, number | null>;
    high_probability_implies_traces: boolean;
  };
  hazards: Record<string, { codes: number[]; penalty: number; cap: number }>;
  wind_gust_tiers: { above_kmh: number; penalty: number }[];
  temperature: {
    cold_tiers: { feels_below_c: number; penalty: number }[];
    subzero_extra_penalty: number;
    heat_tiers: { feels_above_c: number; penalty: number }[];
    cold_wet: { feels_below_c: number; min_probability_pct: number; penalty: number };
  };
  frost: { surface_max_c: number; dewpoint_margin_c: number; cap: number };
}

let meta: ScoringMeta = defaultMeta as unknown as ScoringMeta;

export function getScoringMeta(): ScoringMeta {
  return meta;
}

/** Replaces the bundled constants with the live ones (called once at start-up). */
export function setScoringMeta(next: ScoringMeta): void {
  if (next && Array.isArray(next.labels) && next.rain?.impact_matrix) meta = next;
}

const TIER_BY_LABEL: Record<string, Tier> = { IDEAL: 'ideal', OK: 'ok', 'ATENȚIE': 'atentie', 'EVITĂ': 'evita' };

export function tierOf(score: number | null | undefined): Tier | null {
  if (score == null || !Number.isFinite(score)) return null;
  const sorted = [...meta.labels].sort((a, b) => b.min_score - a.min_score);
  const hit = sorted.find((l) => score >= l.min_score);
  return hit ? TIER_BY_LABEL[hit.label] ?? 'evita' : 'evita';
}

/** CSS custom property carrying each tier's colour (theme-aware). */
export const TIER_COLOR: Record<Tier, string> = {
  ideal: 'var(--t-ideal)',
  ok: 'var(--t-ok)',
  atentie: 'var(--t-atentie)',
  evita: 'var(--t-evita)',
};

export function tierColor(score: number | null | undefined): string {
  const tier = tierOf(score);
  return tier ? TIER_COLOR[tier] : 'var(--dim)';
}

export const RAIN_BANDS: Exclude<RainBand, 'none'>[] = ['urme', 'slaba', 'moderata', 'puternica'];

export const RAIN_BAND_COLOR: Record<Exclude<RainBand, 'none'>, string> = {
  urme: 'var(--rain-1)',
  slaba: 'var(--rain-2)',
  moderata: 'var(--rain-3)',
  puternica: 'var(--rain-4)',
};

export function rainBandOf(mmPerHour: number | null | undefined): RainBand {
  if (mmPerHour == null || !Number.isFinite(mmPerHour)) return 'none';
  let band: RainBand = 'none';
  for (const b of meta.rain.intensity_bands) {
    if (mmPerHour >= b.min_mm_h) band = b.name as RainBand;
  }
  return band;
}

/** Lower bound of each band, for scales and explainers. */
export function bandRange(band: Exclude<RainBand, 'none'>): { min: number; max: number | null } {
  const b = meta.rain.intensity_bands.find((x) => x.name === band);
  return { min: b?.min_mm_h ?? 0, max: b?.max_mm_h ?? null };
}

function probabilityRow(probability: number): number {
  if (probability < 30) return 0;
  if (probability <= 60) return 1;
  return 2;
}

/** Impact of rain from probability x intensity, exactly like the backend matrix. */
export function rainImpact(probability: number | null | undefined, band: RainBand): RainImpact {
  const prob = probability ?? (band === 'none' ? 0 : 100);
  const row = probabilityRow(prob);
  let effective = band;
  if (effective === 'none') {
    if (!(meta.rain.high_probability_implies_traces && row === 2)) return 'none';
    effective = 'urme';
  }
  const col = meta.rain.impact_matrix.columns.indexOf(effective);
  return (meta.rain.impact_matrix.rows[row]?.[col] as RainImpact | undefined) ?? 'none';
}

export const IMPACT_TIER: Record<RainImpact, Tier> = { none: 'ideal', low: 'ok', medium: 'atentie', high: 'evita' };
