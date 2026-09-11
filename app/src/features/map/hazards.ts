import { ApiError } from '../../lib/api';
import { distanceKm } from '../../lib/geo';
import type { Hazard, HazardType } from '../../lib/types';
import type { MAP_STRINGS } from './strings';

// Hazard reports: type metadata, search radius, merging and form rules.
// Pure (no Leaflet, no React) so they can be unit-tested.

type MapKey = keyof typeof MAP_STRINGS.ro;

export interface LatLon {
  lat: number;
  lon: number;
}

export const HAZARD_TYPES: readonly HazardType[] = ['gravel', 'ice', 'flood', 'accident', 'animals', 'roadworks', 'other'];

const TYPE_LABEL: Record<HazardType, MapKey> = {
  gravel: 'typeGravel',
  ice: 'typeIce',
  flood: 'typeFlood',
  accident: 'typeAccident',
  animals: 'typeAnimals',
  roadworks: 'typeRoadworks',
  other: 'typeOther',
};

/** Canonical type of a stored value; anything unknown is shown as "other". */
export function hazardTypeOf(raw: string): HazardType {
  return (HAZARD_TYPES as readonly string[]).includes(raw) ? (raw as HazardType) : 'other';
}

export function hazardLabelKey(raw: string): MapKey {
  return TYPE_LABEL[hazardTypeOf(raw)];
}

export type Severity = 1 | 2 | 3 | 4 | 5;
export const SEVERITIES: readonly Severity[] = [1, 2, 3, 4, 5];
const SEVERITY_LABEL: Record<Severity, MapKey> = { 1: 'sev1', 2: 'sev2', 3: 'sev3', 4: 'sev4', 5: 'sev5' };

export function clampSeverity(value: number): Severity {
  if (!Number.isFinite(value)) return 3;
  return Math.min(5, Math.max(1, Math.round(value))) as Severity;
}

export function severityKey(value: number): MapKey {
  return SEVERITY_LABEL[clampSeverity(value)];
}

export type SeverityTone = 'low' | 'mid' | 'high';

/** Marker colour, reusing the score tier colours: 1-2 yellow, 3 orange, 4-5 red. */
export function severityTone(value: number): SeverityTone {
  const level = clampSeverity(value);
  if (level <= 2) return 'low';
  return level === 3 ? 'mid' : 'high';
}

export const MAX_HAZARD_RADIUS_KM = 150;
export const MIN_HAZARD_RADIUS_KM = 5;

/** Search radius that covers the visible map (centre to corner), capped at 150 km. */
export function radiusFromBounds(center: LatLon, corner: LatLon): number {
  const km = Math.ceil(distanceKm(center, corner));
  return Math.min(MAX_HAZARD_RADIUS_KM, Math.max(MIN_HAZARD_RADIUS_KM, km));
}

/** API timestamps carry an offset; one without it is read as UTC, never device time. NaN when unreadable. */
export function parseApiTime(iso: string): number {
  const trimmed = iso.trim();
  return Date.parse(/(?:Z|[+-]\d{2}:?\d{2})$/i.test(trimmed) ? trimmed : `${trimmed}Z`);
}

/** Adds an API answer to the known hazards: one entry per id, newer copies win, expired reports go. */
export function mergeHazards(known: ReadonlyMap<number, Hazard>, incoming: readonly Hazard[], nowMs: number): Map<number, Hazard> {
  const merged = new Map(known);
  for (const hazard of incoming) merged.set(hazard.id, hazard);
  for (const [id, hazard] of merged) {
    const expires = parseApiTime(hazard.expires_at);
    if (Number.isFinite(expires) && expires <= nowMs) merged.delete(id);
  }
  return merged;
}

export type SpanUnit = 'min' | 'h' | 'd';

/** Rounded size of a time span, in the largest unit that fits. */
export function relativeSpan(ms: number): { n: number; unit: SpanUnit } {
  const minutes = Math.round(Math.abs(ms) / 60_000);
  if (minutes < 60) return { n: minutes, unit: 'min' };
  const hours = Math.round(minutes / 60);
  if (hours < 24) return { n: hours, unit: 'h' };
  return { n: Math.round(hours / 24), unit: 'd' };
}

// Report form rules, matching backend HazardPayload (description 3-220, ttl 1-72 h).
export const DESCRIPTION_MIN = 3;
export const DESCRIPTION_MAX = 220;
export const DURATIONS_H = [2, 6, 12, 24] as const;
export type DurationH = (typeof DURATIONS_H)[number];

export function isDescriptionValid(text: string): boolean {
  const length = text.trim().length;
  return length >= DESCRIPTION_MIN && length <= DESCRIPTION_MAX;
}

export type ReportFailure = 'auth' | 'rate' | 'invalid' | 'network' | 'other';

export function reportFailure(err: unknown): ReportFailure {
  if (!(err instanceof ApiError)) return 'other';
  if (err.isAuth) return 'auth';
  if (err.status === 429) return 'rate';
  if (err.status === 400 || err.status === 422) return 'invalid';
  return err.isNetwork ? 'network' : 'other';
}
