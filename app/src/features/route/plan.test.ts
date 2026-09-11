import { describe, expect, it } from 'vitest';
import { buildTimeline, originOffsetSec, routeStats, segmentScores } from './plan';
import { dayForecast, hour } from './testData';
import type { SamplePoint } from './types';

const RO = 3 * 3600;
const HU = 2 * 3600;
const DATE = '2026-09-12';

const points: SamplePoint[] = [
  { id: 'a', lat: 44.4, lon: 26.1, km: 0, name: 'București', stopIndex: 0 },
  { id: 'b', lat: 45.1, lon: 24.4, km: 150, name: 'DN7', stopIndex: null },
  { id: 'c', lat: 47.5, lon: 19.0, km: 300, name: 'Budapesta', stopIndex: 1 },
];

const weather = {
  a: { status: 'ready' as const, forecast: dayForecast(DATE, RO, () => 90) },
  b: { status: 'ready' as const, forecast: dayForecast(DATE, RO, (h) => (h === 10 ? 58 : 80)) },
  c: { status: 'ready' as const, forecast: dayForecast(DATE, HU, (h) => (h === 11 ? 64 : 70)) },
};

describe('buildTimeline', () => {
  const rows = buildTimeline(points, weather, { date: DATE, time: '08:00' }, 75, 0);

  it('computes each ETA from the origin clock and reads it on the point clock', () => {
    expect(rows.map((r) => r.etaLocal)).toEqual(['2026-09-12T08:00', '2026-09-12T10:00', '2026-09-12T11:00']);
  });

  it('picks the slot in each point local time', () => {
    expect(rows.map((r) => r.slot?.moto_score)).toEqual([90, 58, 64]);
  });

  it('keeps loading points with the origin clock and no slot', () => {
    const partial = buildTimeline(points, { a: weather.a }, { date: DATE, time: '08:00' }, 75, 0);
    expect(partial[2]).toMatchObject({ status: 'loading', slot: null, etaLocal: '2026-09-12T12:00' });
  });
});

describe('originOffsetSec', () => {
  it('prefers the origin, then any loaded point, then the fallback', () => {
    expect(originOffsetSec(points, weather, 0)).toBe(RO);
    expect(originOffsetSec(points, { c: weather.c }, 0)).toBe(HU);
    expect(originOffsetSec(points, {}, 123)).toBe(123);
  });
});

describe('routeStats and segmentScores', () => {
  const rows = buildTimeline(points, weather, { date: DATE, time: '08:00' }, 75, 0);

  it('summarises the backend scores without recomputing them', () => {
    expect(routeStats(rows)).toMatchObject({ avgScore: 71, worstScore: 58, worstName: 'DN7', maxGustKmh: 20, complete: true });
  });

  it('colours each stretch by its worse end', () => {
    expect(segmentScores(rows)).toEqual([58, 58]);
  });

  it('tolerates missing slots', () => {
    const withGap = rows.map((r, i) => (i === 1 ? { ...r, slot: null } : r));
    expect(segmentScores(withGap)).toEqual([90, 64]);
    expect(routeStats(withGap).complete).toBe(false);
  });

  it('reports the largest rain amount', () => {
    const wet = rows.map((r, i) => (i === 2 ? { ...r, slot: hour(r.etaLocal, 50, { precipitation_mm: 1.2 }) } : r));
    expect(routeStats(wet).maxPrecipMm).toBe(1.2);
  });
});
