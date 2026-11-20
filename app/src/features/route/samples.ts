import type { Place } from '../../lib/types';
import type { Summit } from './elevation';
import { planSamples, pointAtKm } from './geometry';
import { roadAt } from './osrm';
import type { RouteLine, SamplePoint } from './types';

// Turns the sampling plan into named points: stops keep their own name and
// coordinates, points in between are named after the road (from OSRM) or
// the previous stop.

export type NameBetween = (road: string | null, previousStop: string) => string;

/** The name of the extra point at the top of the route, from its height in metres. */
export type NameSummit = (elevationM: number) => string;

export const SUMMIT_POINT_ID = 'summit';

function previousStopIndex(stopKm: readonly number[], km: number): number {
  let index = 0;
  for (let i = 0; i < stopKm.length; i += 1) if (stopKm[i] <= km) index = i;
  return index;
}

export function buildSamplePoints(route: RouteLine, stops: readonly Place[], nameBetween: NameBetween): SamplePoint[] {
  return planSamples(route.stopKm).map((spec, i) => {
    const id = `p${i}`;
    if (spec.stopIndex !== null) {
      const stop = stops[spec.stopIndex];
      return { id, lat: stop.lat, lon: stop.lon, km: spec.km, name: stop.name, stopIndex: spec.stopIndex };
    }
    const pos = pointAtKm(route.coords, route.cumKm, spec.km);
    const previous = stops[previousStopIndex(route.stopKm, spec.km)]?.name ?? '';
    return { id, lat: pos.lat, lon: pos.lon, km: spec.km, name: nameBetween(roadAt(route.roads, spec.km), previous), stopIndex: null };
  });
}

/** The points with the summit (see elevation.ts) slotted in along the way. */
export function withSummit(points: readonly SamplePoint[], summit: Summit | null, nameSummit: NameSummit): SamplePoint[] {
  if (!summit) return [...points];
  const top: SamplePoint = {
    id: SUMMIT_POINT_ID,
    lat: summit.lat,
    lon: summit.lon,
    km: summit.km,
    name: nameSummit(summit.elevationM),
    stopIndex: null,
  };
  // Stable sort: on a tie the existing point (a stop) stays first.
  return [...points, top].sort((a, b) => a.km - b.km);
}
