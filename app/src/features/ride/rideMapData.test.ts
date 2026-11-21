import { describe, expect, it } from 'vitest';
import { cumulativeKm } from '../route/geometry';
import type { RideRoute } from './logic/follow';
import type { RidePoint } from './logic/track';
import { bearingDeg, boundsOf, multiLine, routeParts, trackLines } from './rideMapData';

function pt(t: number, lat: number, lon: number, extra: Partial<RidePoint> = {}): RidePoint {
  return { t, lat, lon, acc: 5, speed: 10, alt: null, seg: 0, ...extra };
}

describe('trackLines', () => {
  it('breaks the line at a pause and at a gap, and puts the road across the gap once filled', () => {
    const points = [
      pt(1, 44.0, 26.0),
      pt(2, 44.001, 26.0),
      pt(3, 44.001, 26.0, { still: true }),
      pt(4, 44.01, 26.0, { gap: true }),
      pt(5, 44.011, 26.0),
      pt(6, 44.2, 26.0, { seg: 1 }),
      pt(7, 44.201, 26.0, { seg: 1 }),
    ];
    const straight = trackLines(points, []);
    expect(straight.solid).toHaveLength(3);
    expect(straight.gaps).toEqual([
      [
        [26.0, 44.001],
        [26.0, 44.01],
      ],
    ]);
    const filled = trackLines(points, [{ t: 4, roadM: 1200, coords: [{ lat: 44.004, lon: 26.002 }] }]);
    expect(filled.gaps[0]).toHaveLength(3);
    expect(filled.gaps[0][1]).toEqual([26.002, 44.004]);
  });
});

describe('routeParts', () => {
  const coords = [
    { lat: 45, lon: 25 },
    { lat: 45, lon: 25.01 },
    { lat: 45, lon: 25.02 },
  ];
  const cumKm = cumulativeKm(coords);
  const route: RideRoute = { name: 'X', coords, cumKm, distanceKm: cumKm[2], plannedKmh: 60 };

  it('is all ahead before the start, and splits at the progress later', () => {
    expect(routeParts(route, null).done).toEqual([]);
    expect(routeParts(route, null).ahead).toHaveLength(3);
    const mid = routeParts(route, cumKm[1] + 0.2);
    expect(mid.done[mid.done.length - 1]).toEqual(mid.ahead[0]);
    expect(routeParts(route, cumKm[2] + 1).ahead).toEqual([]);
  });
});

describe('map helpers', () => {
  it('gives the direction of travel', () => {
    expect(bearingDeg({ lat: 45, lon: 25 }, { lat: 45.01, lon: 25 })).toBeCloseTo(0, 3);
    expect(bearingDeg({ lat: 45, lon: 25 }, { lat: 45, lon: 25.01 })).toBeCloseTo(90, 1);
    expect(bearingDeg({ lat: 45, lon: 25 }, { lat: 44.99, lon: 25 })).toBeCloseTo(180, 3);
    expect(bearingDeg({ lat: 45, lon: 25 }, { lat: 45, lon: 24.99 })).toBeCloseTo(270, 1);
  });

  it('frames positions and leaves out lines too short to draw', () => {
    expect(boundsOf([{ lat: 45, lon: 25 }, { lat: 44, lon: 26 }])).toEqual([25, 44, 26, 45]);
    expect(multiLine([[[25, 45]]]).features).toHaveLength(0);
  });
});
