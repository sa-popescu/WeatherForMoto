import { describe, expect, it } from 'vitest';
import { parseFuelFile, planFuel, stationsAlong, type StationOnRoute } from './fuel';
import { cumulativeKm } from './geometry';
import type { LatLon } from './types';

/** A straight road north from 45°N, a vertex every km. */
function road(km: number): { coords: LatLon[]; cumKm: number[] } {
  const coords = Array.from({ length: km + 1 }, (_, i) => ({ lat: 45 + i / 111.2, lon: 25 }));
  return { coords, cumKm: cumulativeKm(coords) };
}

function at(km: number, label = ''): StationOnRoute {
  return { lat: 0, lon: 0, label, km, offKm: 0 };
}

describe('fuel stations file', () => {
  it('reads the stations and skips broken rows', () => {
    expect(parseFuelFile({ stations: [[44.4, 26.1, 'OMV'], [45, 25], 'x', [1, 'a']] })).toEqual([
      { lat: 44.4, lon: 26.1, label: 'OMV' },
      { lat: 45, lon: 25, label: '' },
    ]);
  });

  it('refuses a file without a list', () => {
    expect(() => parseFuelFile({})).toThrow();
    expect(() => parseFuelFile(null)).toThrow();
  });
});

describe('stations along a route', () => {
  it('keeps the stations near the road, with their km, in order', () => {
    const line = road(100);
    const kmPerLonDeg = 111.32 * Math.cos((45.4 * Math.PI) / 180);
    const found = stationsAlong(line, [
      { lat: 45 + 60 / 111.2, lon: 25 + 0.3 / kmPerLonDeg, label: 'B' },
      { lat: 45 + 20 / 111.2, lon: 25, label: 'A' },
      { lat: 45 + 40 / 111.2, lon: 25 + 3 / kmPerLonDeg, label: 'too far' },
      { lat: 44, lon: 25, label: 'south of the start' },
    ]);
    expect(found.map((s) => s.label)).toEqual(['A', 'B']);
    expect(found[0].km).toBeCloseTo(20, 0);
    expect(found[1].offKm).toBeCloseTo(0.3, 1);
  });

  it('measures between vertices too', () => {
    const coords = [
      { lat: 45, lon: 25 },
      { lat: 45 + 50 / 111.2, lon: 25 },
    ];
    const found = stationsAlong({ coords, cumKm: cumulativeKm(coords) }, [{ lat: 45 + 25 / 111.2, lon: 25.001, label: 'mid' }]);
    expect(found).toHaveLength(1);
    expect(found[0].km).toBeCloseTo(25, 0);
  });
});

describe('where to fill up', () => {
  it('needs no stop when one tank covers the route', () => {
    expect(planFuel([at(50)], 150, 200)).toEqual({ stops: [], gaps: [] });
  });

  it('fills up at the last station before the reserve', () => {
    // 200 km range: fill up before 160 km.
    const plan = planFuel([at(60, 'a'), at(140, 'b'), at(170, 'c'), at(290, 'd'), at(380, 'e')], 450, 200);
    expect(plan.stops.map((s) => s.label)).toEqual(['b', 'd']);
    expect(plan.gaps).toEqual([]);
  });

  it('warns about a stretch with no station before the reserve', () => {
    // Nothing between 0 and 160: the first station after it, at 180, is the stop.
    const plan = planFuel([at(180, 'far')], 300, 200);
    expect(plan.stops.map((s) => s.label)).toEqual(['far']);
    expect(plan.gaps).toEqual([{ fromKm: 0, toKm: 180, tooLong: false }]);
  });

  it('says when a stretch is longer than the whole range', () => {
    const plan = planFuel([at(100, 'a'), at(350, 'b')], 400, 200);
    expect(plan.gaps).toEqual([{ fromKm: 100, toKm: 350, tooLong: true }]);
  });

  it('warns up to the destination when no station is left', () => {
    const plan = planFuel([at(100, 'a')], 400, 200);
    expect(plan.stops.map((s) => s.label)).toEqual(['a']);
    expect(plan.gaps).toEqual([{ fromKm: 100, toKm: 400, tooLong: true }]);
  });

  it('does not count a station at the start as a stop', () => {
    const plan = planFuel([at(0.5, 'start'), at(150, 'a')], 250, 200);
    expect(plan.stops.map((s) => s.label)).toEqual(['a']);
  });
});
