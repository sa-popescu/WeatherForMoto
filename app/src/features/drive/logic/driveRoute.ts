import type { UrbanInstruction, UrbanRoute } from '../../../lib/types';
import { cumulativeKm } from '../../route/geometry';
import type { RoadSpan, RouteLine } from '../../route/types';

// The routes "Drum" offers, whichever service drew them: TomTom with live
// traffic (backend /route/urban), or OSRM without traffic when TomTom cannot
// answer. Pure, so it is tested on its own.

export type RouteSource = 'tomtom' | 'osrm';

/** A stretch with traffic, along the route. */
export interface TrafficStretch {
  fromKm: number;
  toKm: number;
  /** JAM, ROAD_WORK, ROAD_CLOSURE and the like. */
  category: string;
  /** 0 unknown, 1 minor, 2 moderate, 3 major, 4 closures. */
  magnitude: number;
  delayS: number | null;
}

export interface DriveRoute {
  id: string;
  source: RouteSource;
  line: RouteLine;
  /** Expected riding time, seconds: with live traffic from TomTom, by car at the limits from OSRM. */
  durationS: number;
  /** Time lost to traffic, seconds (0 without traffic data). */
  delayS: number;
  traffic: TrafficStretch[];
  /** Coded turn instructions (TomTom's codes, OSRM's turned into them), for the spoken directions. */
  instructions: UrbanInstruction[];
}

/** Without a time from OSRM, a town-and-country pace. */
const FALLBACK_KMH = 50;

/** The first road number, else the street, of an instruction. */
function roadOf(ins: UrbanInstruction): string {
  return ins.road?.split(',')[0]?.trim() || ins.street?.trim() || '';
}

/**
 * Named stretches from the turn instructions: after each turn the rider is on
 * the road it names, until the next one. Neighbours with the same name merge.
 */
export function roadSpans(instructions: readonly UrbanInstruction[], distanceKm: number): RoadSpan[] {
  const spans: RoadSpan[] = [];
  instructions.forEach((ins, i) => {
    const label = roadOf(ins);
    const fromKm = ins.offset_m / 1000;
    const toKm = i + 1 < instructions.length ? instructions[i + 1].offset_m / 1000 : distanceKm;
    if (!label || toKm <= fromKm) return;
    const last = spans[spans.length - 1];
    if (last && last.label === label && Math.abs(last.toKm - fromKm) < 0.001) last.toKm = toKm;
    else spans.push({ fromKm, toKm, label });
  });
  return spans;
}

/**
 * Where each stop falls along the route, km: from the length of each leg,
 * scaled to the whole distance; just the two ends without them.
 */
export function stopKmOf(legsM: readonly number[] | undefined, distanceKm: number): number[] {
  const total = (legsM ?? []).reduce((sum, m) => sum + Math.max(0, m), 0);
  if (!legsM || legsM.length === 0 || total <= 0) return [0, distanceKm];
  const stops = [0];
  let along = 0;
  for (const m of legsM) {
    along += Math.max(0, m);
    stops.push((along / total) * distanceKm);
  }
  return stops;
}

/** A TomTom route as "Drum" uses it, or null when it has no usable line. */
export function fromUrban(route: UrbanRoute, index: number): DriveRoute | null {
  const coords = route.coords.filter(([lat, lon]) => Number.isFinite(lat) && Number.isFinite(lon)).map(([lat, lon]) => ({ lat, lon }));
  if (coords.length < 2) return null;
  const geomCum = cumulativeKm(coords);
  const geomTotal = geomCum[geomCum.length - 1];
  const distanceKm = route.distance_m > 0 ? route.distance_m / 1000 : geomTotal;
  const scale = geomTotal > 0 ? distanceKm / geomTotal : 1;
  const cumKm = geomCum.map((k) => k * scale);
  const kmAt = (i: number | null): number | null => (i === null ? null : cumKm[Math.max(0, Math.min(cumKm.length - 1, i))]);
  const traffic: TrafficStretch[] = [];
  for (const t of route.traffic) {
    const fromKm = kmAt(t.from);
    const toKm = kmAt(t.to);
    if (fromKm === null || toKm === null || toKm <= fromKm) continue;
    traffic.push({ fromKm, toKm, category: t.category, magnitude: t.magnitude, delayS: t.delay_s });
  }
  return {
    id: `tomtom-${index}`,
    source: 'tomtom',
    line: { coords, cumKm, distanceKm, stopKm: stopKmOf(route.legs_m, distanceKm), roads: roadSpans(route.instructions, distanceKm) },
    durationS: Math.max(60, route.duration_s),
    delayS: Math.max(0, route.delay_s),
    traffic,
    instructions: route.instructions,
  };
}

/** An OSRM route, with OSRM's own time when it gave one, and its turns. */
export function fromOsrm(line: RouteLine, durationS: number | null): DriveRoute {
  return {
    id: 'osrm',
    source: 'osrm',
    line,
    durationS: Math.max(60, durationS ?? (line.distanceKm / FALLBACK_KMH) * 3600),
    delayS: 0,
    traffic: [],
    instructions: line.instructions ?? [],
  };
}

/** The pace the time implies, km/h: what the weather along the way is timed with. */
export function routeKmh(route: DriveRoute): number {
  return route.line.distanceKm / (route.durationS / 3600);
}

/** The road the route spends the longest on, to tell the alternatives apart ("prin A1"). */
export function mainRoad(route: DriveRoute): string | null {
  const byLabel = new Map<string, number>();
  for (const span of route.line.roads) byLabel.set(span.label, (byLabel.get(span.label) ?? 0) + span.toKm - span.fromKm);
  let best: string | null = null;
  let longest = 0;
  for (const [label, km] of byLabel) {
    if (km > longest) {
      best = label;
      longest = km;
    }
  }
  return best;
}
