import { describe, expect, it } from 'vitest';
import { offlineKey } from '../../lib/offlineMapKeys';
import { bandTiles, MAX_TILES, planTiles, routeMapId, tileOf, TOP_ZOOM } from './offlineMap';
import type { LatLon } from './types';

/** A road from (lat, lon) going north for km, a vertex every 2 km. */
function north(lat: number, lon: number, km: number): LatLon[] {
  return Array.from({ length: Math.floor(km / 2) + 1 }, (_, i) => ({ lat: lat + (i * 2) / 111.2, lon }));
}

/** A road going east, a vertex every 2 km. */
function east(lat: number, lon: number, km: number): LatLon[] {
  const perDeg = 111.32 * Math.cos((lat * Math.PI) / 180);
  return Array.from({ length: Math.floor(km / 2) + 1 }, (_, i) => ({ lat, lon: lon + (i * 2) / perDeg }));
}

describe('offline map tiles', () => {
  it('finds the tile of a point like every web map', () => {
    // Bucharest at zoom 14.
    expect(tileOf(44.4268, 26.1025, 14)).toEqual({ x: 9379, y: 5930 });
    expect(tileOf(0, 0, 1)).toEqual({ x: 1, y: 1 });
    expect(tileOf(90, -180, 3)).toEqual({ x: 0, y: 0 });
  });

  it('covers a band either side of the road, each tile once', () => {
    const tiles = bandTiles(north(45, 25, 50), { z: 14, bufferKm: 1 });
    const keys = new Set(tiles.map((t) => `${t.x}/${t.y}`));
    expect(keys.size).toBe(tiles.length);
    // About 1.7 km per tile here: some 30 tiles along, 2 or 3 across.
    expect(tiles.length).toBeGreaterThan(50);
    expect(tiles.length).toBeLessThan(120);
  });

  it('downloads a 300 km route in full, overview first', () => {
    const plan = planTiles(north(44.4, 26.1, 300));
    expect(plan.reduced).toBe(false);
    expect(plan.topZoom).toBe(TOP_ZOOM);
    expect(plan.tiles.length).toBeLessThanOrEqual(MAX_TILES);
    const zooms = plan.tiles.map((t) => t.z);
    expect(zooms[0]).toBe(5);
    expect(zooms[zooms.length - 1]).toBe(TOP_ZOOM);
  });

  it('gives up detail, not the overview, on a very long route', () => {
    const plan = planTiles([...east(45, 20, 1200), ...north(45, 20 + 1200 / (111.32 * Math.cos((45 * Math.PI) / 180)), 600)]);
    expect(plan.reduced).toBe(true);
    expect(plan.tiles.length).toBeLessThanOrEqual(MAX_TILES);
    expect(plan.tiles.some((t) => t.z === 5)).toBe(true);
  });

  it('gives the same route the same id, and another route another one', () => {
    const a = north(45, 25, 100);
    expect(routeMapId(a)).toBe(routeMapId([...a]));
    expect(routeMapId(a)).not.toBe(routeMapId(north(45, 25.5, 100)));
  });

  it('keeps vector tiles under one key whatever the weekly release', () => {
    expect(offlineKey('https://tiles.openfreemap.org/planet/20261115_080022_pt/14/9380/5951.pbf')).toBe(
      'https://tiles.openfreemap.org/planet/offline/14/9380/5951.pbf',
    );
    expect(offlineKey('https://tiles.openfreemap.org/planet/20261122_080022_pt/14/9380/5951.pbf')).toBe(
      'https://tiles.openfreemap.org/planet/offline/14/9380/5951.pbf',
    );
    expect(offlineKey('https://tiles.openfreemap.org/fonts/Noto%20Sans%20Regular/0-255.pbf')).toBe(
      'https://tiles.openfreemap.org/fonts/Noto%20Sans%20Regular/0-255.pbf',
    );
    expect(offlineKey('https://tiles.openfreemap.org/planet')).toBe('https://tiles.openfreemap.org/planet');
  });
});
