import { describe, expect, it } from 'vitest';
import { cumulativeKm } from './geometry';
import type { TimelineRow } from './plan';
import { pointKind, routeBounds, routePoints, routeStretches } from './routeMapData';
import type { RouteLine, SamplePoint } from './types';

const coords = [
  { lat: 45, lon: 25 },
  { lat: 45, lon: 25.1 },
  { lat: 45, lon: 25.2 },
  { lat: 45.1, lon: 25.2 },
];
const cumKm = cumulativeKm(coords);
const route: RouteLine = { coords, cumKm, distanceKm: cumKm[3], stopKm: [0, cumKm[3]], roads: [] };

function row(id: string, km: number, stopIndex: number | null, score: number | null): TimelineRow {
  const point: SamplePoint = { id, lat: 45, lon: 25, km, name: id, stopIndex };
  return { point, etaUtcMs: 0, etaLocal: '', status: 'ready', slot: score === null ? null : ({ moto_score: score } as TimelineRow['slot']), dark: null };
}

const rows = [row('a', 0, 0, 90), row('m', cumKm[1] + 1, null, 30), row('b', cumKm[3], 1, 70)];

describe('route map data', () => {
  it('cuts the route at the sample points and colours each stretch by its tier', () => {
    const stretches = routeStretches(route, rows, [90, 30]);
    expect(stretches.features).toHaveLength(2);
    expect(stretches.features.map((f) => f.properties.tier)).toEqual(['ideal', 'evita']);
  });

  it('tells stops from sample points', () => {
    expect(rows.map((r, i) => pointKind(r, i, rows.length))).toEqual(['origin', 'sample', 'destination']);
    expect(routePoints(rows).features[1].properties).toEqual({ kind: 'sample', tier: 'evita' });
  });

  it('frames the whole route', () => {
    expect(routeBounds(route)).toEqual([25, 45, 25.2, 45.1]);
  });
});
