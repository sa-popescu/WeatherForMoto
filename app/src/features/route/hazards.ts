import type { Hazard, HazardType } from '../../lib/types';
import { nearestOnLine } from './geometry';
import type { RouteLine } from './types';

// Hazards reported by riders, placed along the route.

export const HAZARD_TYPES: readonly HazardType[] = ['gravel', 'ice', 'flood', 'accident', 'animals', 'roadworks', 'other'];
export const HAZARD_RADIUS_KM = 25;
export const HAZARD_TTL_HOURS = [2, 6, 12, 24, 72] as const;
export const DESCRIPTION_MIN = 3;
export const DESCRIPTION_MAX = 220;

export interface RouteHazard extends Hazard {
  /** Distance along the route of the closest point, km. */
  routeKm: number;
  /** How far from the route it lies, km. */
  offRouteKm: number;
}

/** Dedupes by id, keeps those near the line and sorts them in riding order. */
export function placeHazards(hazards: readonly Hazard[], line: RouteLine, maxOffKm: number = HAZARD_RADIUS_KM): RouteHazard[] {
  const seen = new Set<number>();
  const out: RouteHazard[] = [];
  for (const h of hazards) {
    if (seen.has(h.id)) continue;
    seen.add(h.id);
    const near = nearestOnLine(line.coords, line.cumKm, h);
    if (near.offKm <= maxOffKm) out.push({ ...h, routeKm: near.km, offRouteKm: near.offKm });
  }
  return out.sort((a, b) => a.routeKm - b.routeKm);
}

/** Server timestamps are UTC; older rows may lack the zone suffix. */
export function serverTimeMs(iso: string): number {
  const hasZone = /(?:Z|[+-]\d{2}:?\d{2})$/.test(iso);
  return Date.parse(hasZone ? iso : `${iso}Z`);
}

export function isKnownHazardType(value: string): value is HazardType {
  return (HAZARD_TYPES as readonly string[]).includes(value);
}

/** Key of the hazard type word in the action strings. */
export function typeKey(type: HazardType): `type_${HazardType}` {
  return `type_${type}`;
}
