import { describe, expect, it } from 'vitest';
import { SEASONAL_ROADS, seasonalRoadsOnRoute, seasonRisk, segmentDistanceKm } from './seasonalClosures';
import type { LatLon } from './types';

const transfagarasan = SEASONAL_ROADS.find((r) => r.id === 'transfagarasan')!;
const transalpina = SEASONAL_ROADS.find((r) => r.id === 'transalpina')!;

/** A line through the points, with extra vertices every few hundred metres like a real route. */
function through(points: readonly LatLon[]): LatLon[] {
  const out: LatLon[] = [];
  for (let i = 0; i + 1 < points.length; i += 1) {
    for (let k = 0; k < 10; k += 1) {
      const t = k / 10;
      out.push({ lat: points[i].lat + (points[i + 1].lat - points[i].lat) * t, lon: points[i].lon + (points[i + 1].lon - points[i].lon) * t });
    }
  }
  out.push(points[points.length - 1]);
  return out;
}

describe('seasonal roads', () => {
  it('knows the season', () => {
    expect(seasonRisk('2026-12-05')).toBe('closed');
    expect(seasonRisk('2027-03-20')).toBe('closed');
    expect(seasonRisk('2027-05-31')).toBe('closed');
    expect(seasonRisk('2027-06-01')).toBe('maybe');
    expect(seasonRisk('2027-07-10')).toBe('maybe');
    expect(seasonRisk('2027-07-11')).toBe('open');
    expect(seasonRisk('2027-09-30')).toBe('open');
    expect(seasonRisk('2027-10-01')).toBe('maybe');
    expect(seasonRisk('2027-11-01')).toBe('closed');
  });

  it('measures the distance to a segment, not only to its ends', () => {
    const segment = [
      { lat: 45, lon: 24 },
      { lat: 45, lon: 24.1 },
    ] as const;
    expect(segmentDistanceKm(segment, { lat: 45.001, lon: 24.05 })).toBeCloseTo(0.111, 2);
  });

  it('finds the Transfăgărășan on a route over the pass, from Curtea de Argeș to Cârțișoara', () => {
    const route = through([{ lat: 45.14, lon: 24.68 }, ...transfagarasan.checkpoints, { lat: 45.7, lon: 24.58 }]);
    expect(seasonalRoadsOnRoute(route).map((r) => r.id)).toEqual(['transfagarasan']);
  });

  it('finds the Transalpina driven only from Rânca to the top', () => {
    const route = through([{ lat: 45.25, lon: 23.69 }, ...transalpina.checkpoints.slice(0, 4)]);
    expect(seasonalRoadsOnRoute(route).map((r) => r.id)).toEqual(['transalpina']);
  });

  it('leaves out a route that only reaches the open end, at Piscu Negru', () => {
    const route = through([{ lat: 45.14, lon: 24.68 }, transfagarasan.checkpoints[0]]);
    expect(seasonalRoadsOnRoute(route)).toEqual([]);
  });

  it('leaves out a route elsewhere, Bucharest to Brașov', () => {
    const route = through([
      { lat: 44.43, lon: 26.1 },
      { lat: 44.94, lon: 26.02 },
      { lat: 45.35, lon: 25.54 },
      { lat: 45.65, lon: 25.6 },
    ]);
    expect(seasonalRoadsOnRoute(route)).toEqual([]);
  });

  it('copes with a route of one point', () => {
    expect(seasonalRoadsOnRoute([{ lat: 45.6, lon: 24.6 }])).toEqual([]);
  });
});
