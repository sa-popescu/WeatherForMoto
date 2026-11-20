import { beforeEach, describe, expect, it, vi } from 'vitest';
import { SUMMIT_POINT_ID } from './samples';
import { nameTrackPoints, pointsToName } from './trackNames';
import type { SamplePoint } from './types';

const reverse = vi.hoisted(() => vi.fn<(lat: number, lon: number) => Promise<string | null>>());
vi.mock('../../lib/geo', () => ({ NOMINATIM_GAP_MS: 1100, reverseGeocode: reverse }));

const POINTS: SamplePoint[] = [
  { id: 'p0', lat: 44.43, lon: 26.04, km: 0, name: 'Militari', stopIndex: 0 },
  { id: 'p1', lat: 44.6, lon: 25.7, km: 37, name: 'după Militari', stopIndex: null },
  { id: SUMMIT_POINT_ID, lat: 44.7, lon: 25.5, km: 60, name: 'Punctul cel mai înalt · 520 m', stopIndex: null },
  { id: 'p2', lat: 44.8, lon: 25.3, km: 74, name: 'după Militari', stopIndex: null },
  { id: 'p3', lat: 44.53, lon: 26.19, km: 220, name: 'Crețuleasca', stopIndex: 1 },
];

describe('track point names', () => {
  beforeEach(() => reverse.mockReset());

  it('names only the points between stops, never the summit', () => {
    expect(pointsToName(POINTS).map((p) => p.id)).toEqual(['p1', 'p2']);
  });

  it('hands over every locality found, in riding order, and skips the misses', async () => {
    reverse.mockResolvedValueOnce('Găești').mockResolvedValueOnce(null);
    const named: [string, string][] = [];
    await nameTrackPoints(POINTS, 'ro', new AbortController().signal, (id, place) => named.push([id, place]), 0);
    expect(reverse.mock.calls.map(([lat]) => lat)).toEqual([44.6, 44.8]);
    expect(named).toEqual([['p1', 'Găești']]);
  });

  it('stops when a newer calculation takes over', async () => {
    const controller = new AbortController();
    reverse.mockImplementation(async () => {
      controller.abort();
      return 'Găești';
    });
    const named: string[] = [];
    await nameTrackPoints(POINTS, 'ro', controller.signal, (id) => named.push(id), 0);
    expect(reverse).toHaveBeenCalledTimes(1);
    expect(named).toEqual([]);
  });
});
