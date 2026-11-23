import { describe, expect, it } from 'vitest';
import { hour } from '../now/logic/testFixtures';
import { cumulativeKm } from './geometry';
import type { TimelineRow } from './plan';
import { bearingGap, glareStretches, sunInEyes, sunPosition, timeAlong, timelineGlare } from './sunGlare';
import type { LatLon, RouteLine } from './types';

const BUCHAREST = { lat: 44.43, lon: 26.1 };

/** A straight road from `start` toward `bearing` (degrees), `km` long, a point per km. */
function road(start: LatLon, bearing: number, km: number): RouteLine {
  const coords: LatLon[] = [];
  for (let i = 0; i <= km; i += 1) {
    const dLat = (i * Math.cos((bearing * Math.PI) / 180)) / 111.195;
    const dLon = (i * Math.sin((bearing * Math.PI) / 180)) / (111.195 * Math.cos((start.lat * Math.PI) / 180));
    coords.push({ lat: start.lat + dLat, lon: start.lon + dLon });
  }
  const cumKm = cumulativeKm(coords);
  return { coords, cumKm, distanceKm: cumKm[cumKm.length - 1], stopKm: [0, cumKm[cumKm.length - 1]], roads: [] };
}

describe('the sun', () => {
  it('stands high in the south at midsummer noon in Bucharest', () => {
    const noon = sunPosition(Date.UTC(2026, 5, 21, 10, 15), BUCHAREST.lat, BUCHAREST.lon);
    expect(noon.elevation).toBeGreaterThan(68);
    expect(noon.elevation).toBeLessThan(70);
    expect(noon.azimuth).toBeGreaterThan(175);
    expect(noon.azimuth).toBeLessThan(185);
  });

  it('is low in the south-east on a November morning and low in the south-west before sunset', () => {
    const morning = sunPosition(Date.UTC(2026, 10, 23, 6, 0), BUCHAREST.lat, BUCHAREST.lon);
    expect(morning.elevation).toBeGreaterThan(2);
    expect(morning.elevation).toBeLessThan(10);
    expect(morning.azimuth).toBeGreaterThan(115);
    expect(morning.azimuth).toBeLessThan(135);
    const evening = sunPosition(Date.UTC(2026, 10, 23, 14, 0), BUCHAREST.lat, BUCHAREST.lon);
    expect(evening.elevation).toBeGreaterThan(3);
    expect(evening.elevation).toBeLessThan(12);
    expect(evening.azimuth).toBeGreaterThan(220);
    expect(evening.azimuth).toBeLessThan(240);
    // Night.
    expect(sunPosition(Date.UTC(2026, 10, 23, 22, 0), BUCHAREST.lat, BUCHAREST.lon).elevation).toBeLessThan(0);
  });
});

describe('sun in the eyes', () => {
  const low = { azimuth: 230, elevation: 8 };

  it('needs a low sun ahead and a sky clear enough', () => {
    expect(bearingGap(350, 10)).toBe(20);
    expect(sunInEyes(240, low, 20)).toBe(true);
    expect(sunInEyes(240, low, null)).toBe(true);
    expect(sunInEyes(240, low, 90)).toBe(false);
    expect(sunInEyes(60, low, 20)).toBe(false);
    expect(sunInEyes(null, low, 20)).toBe(false);
    expect(sunInEyes(230, { azimuth: 230, elevation: 35 }, 20)).toBe(false);
  });

  it('finds the stretch ridden into the low sun, with when it starts', () => {
    const depart = Date.UTC(2026, 10, 23, 13, 45);
    const west = road(BUCHAREST, 232, 40);
    const at = (km: number): number => depart + (km / 60) * 3_600_000;
    const [stretch, ...more] = glareStretches(west, at, () => 10);
    expect(more).toHaveLength(0);
    expect(stretch.fromKm).toBe(0);
    expect(stretch.toKm).toBeCloseTo(40, 0);
    expect(stretch.atMs).toBe(depart);
    // Overcast, or riding east: nothing.
    expect(glareStretches(west, at, () => 95)).toEqual([]);
    expect(glareStretches(road(BUCHAREST, 52, 40), at, () => 10)).toEqual([]);
  });

  it('warns in the planner once the weather is known, with the local time and the road', () => {
    const depart = Date.UTC(2026, 10, 23, 13, 45);
    const west = { ...road(BUCHAREST, 232, 40), roads: [{ fromKm: 0, toKm: 40, label: 'DN7' }] };
    const row = (km: number, cloud: number | null, loaded = true): TimelineRow => {
      const etaUtcMs = depart + (km / 60) * 3_600_000;
      const etaLocal = new Date(etaUtcMs + 2 * 3_600_000).toISOString().slice(0, 16);
      const point = { id: `p${km}`, lat: BUCHAREST.lat, lon: BUCHAREST.lon, km, name: `Punct ${km}`, stopIndex: null };
      return { point, etaUtcMs, etaLocal, status: loaded ? 'ready' : 'loading', slot: loaded ? hour(etaLocal, { cloud_cover: cloud }) : null, dark: null };
    };
    const warning = timelineGlare(west, [row(0, 10), row(40, 10)]);
    expect(warning?.place).toBe('DN7');
    expect(warning?.localIso).toBe('2026-11-23T15:45');
    expect(warning?.more).toBe(0);
    // Before the forecast arrives, or under clouds: nothing yet.
    expect(timelineGlare(west, [row(0, null, false), row(40, null, false)])).toBeNull();
    expect(timelineGlare(west, [row(0, 90), row(40, 90)])).toBeNull();
  });

  it('tells the time at any distance from the passing times of a few points', () => {
    const time = timeAlong([
      { km: 0, atMs: 0 },
      { km: 40, atMs: 2_400_000 },
    ]);
    expect(time(20)).toBe(1_200_000);
    expect(time(60)).toBe(2_400_000);
  });
});
