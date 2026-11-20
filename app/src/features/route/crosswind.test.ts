import { describe, expect, it } from 'vitest';
import { bearingDeg, crossWarning, headingAt, roadWinds, windOnRoad } from './crosswind';
import { cumulativeKm } from './geometry';
import type { TimelineRow } from './plan';
import { hour } from './testData';
import type { LatLon, RouteLine } from './types';

function routeOf(coords: LatLon[]): RouteLine {
  const cumKm = cumulativeKm(coords);
  const distanceKm = cumKm[cumKm.length - 1];
  return { coords, cumKm, distanceKm, stopKm: [0, distanceKm], roads: [] };
}

/** About 55 km straight north, a point every ~1 km. */
const NORTH = routeOf(Array.from({ length: 51 }, (_, i) => ({ lat: 45 + i * 0.01, lon: 25 })));

/** Hairpins: east and back west every ~0.8 km while climbing slowly north. */
const HAIRPINS = routeOf(Array.from({ length: 41 }, (_, i) => ({ lat: 45 + i * 0.0005, lon: 25 + (i % 2) * 0.01 })));

function row(km: number, name: string, windFrom: number | null, gust: number | null): TimelineRow {
  return {
    point: { id: name, lat: 45, lon: 25, km, name, stopIndex: null },
    etaUtcMs: 0,
    etaLocal: '2026-11-22T10:40',
    status: 'ready',
    slot: hour('2026-11-22T10:00', 70, { wind_direction_10m: windFrom, wind_gusts_kmh: gust }),
    dark: null,
  };
}

describe('crosswind', () => {
  it('reads compass bearings', () => {
    expect(bearingDeg({ lat: 45, lon: 25 }, { lat: 46, lon: 25 })).toBeCloseTo(0, 0);
    expect(bearingDeg({ lat: 45, lon: 25 }, { lat: 45, lon: 26 })).toBeCloseTo(90, -1);
    expect(bearingDeg({ lat: 46, lon: 25 }, { lat: 45, lon: 25 })).toBeCloseTo(180, 0);
  });

  it('finds the direction of travel, but not on hairpins', () => {
    expect(headingAt(NORTH, 20)).toBeCloseTo(0, 0);
    expect(headingAt(NORTH, 0)).toBeCloseTo(0, 0);
    expect(headingAt(HAIRPINS, 10)).toBeNull();
  });

  it('names the side the gusts come from', () => {
    expect(windOnRoad(0, 0, 50)?.side).toBe('head');
    expect(windOnRoad(0, 180, 50)?.side).toBe('tail');
    expect(windOnRoad(0, 270, 50)).toEqual({ side: 'cross', gustKmh: 50, crossKmh: 50 });
    expect(windOnRoad(350, 40, 50)?.side).toBe('cross');
    expect(windOnRoad(90, 200, 60)?.side).toBe('cross');
    expect(windOnRoad(90, 300, 60)?.side).toBe('tail');
    expect(windOnRoad(0, 30, 60)?.crossKmh).toBeCloseTo(30, 5);
  });

  it('stays quiet for light gusts or missing values', () => {
    expect(windOnRoad(0, 90, 29)).toBeNull();
    expect(windOnRoad(null, 90, 50)).toBeNull();
    expect(windOnRoad(0, null, 50)).toBeNull();
    expect(windOnRoad(0, 90, null)).toBeNull();
  });

  it('warns about the strongest side gust and counts the others', () => {
    const rows = [row(5, 'A', 90, 45), row(20, 'B', 270, 62), row(35, 'C', 0, 80), row(50, 'D', 80, 38)];
    const winds = roadWinds(NORTH, rows);
    expect(winds.map((w) => w?.side ?? null)).toEqual(['cross', 'cross', 'head', 'cross']);
    expect(crossWarning(rows, winds)).toMatchObject({ row: { point: { name: 'B' } }, gustKmh: 62, more: 1 });
    expect(crossWarning(rows.slice(2), winds.slice(2))).toBeNull();
  });
});
