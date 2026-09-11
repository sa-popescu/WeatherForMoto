import { describe, expect, it } from 'vitest';
import { buildGpx, escapeXml, gpxFileName } from './gpx';
import { placeHazards, serverTimeMs } from './hazards';
import { osrmUrl, parseOsrm, roadAt, RouteError } from './osrm';
import { runPool } from './pool';
import { rainLine } from './rainText';
import { addStop, MAX_STOPS, moveItem, newStop, removeStop, roleOf, savedLabel } from './stops';
import type { Hazard } from '../../lib/types';

describe('gpx', () => {
  const gpx = buildGpx({
    name: 'București → Sibiu & <back>',
    line: [
      { lat: 44.4268, lon: 26.1025 },
      { lat: 45.7983, lon: 24.1256 },
    ],
    waypoints: [{ lat: 44.4268, lon: 26.1025, name: 'București' }],
    createdIso: '2026-09-11T10:00:00.000Z',
  });

  it('writes a GPX 1.1 track with waypoints first', () => {
    expect(gpx.startsWith('<?xml version="1.0" encoding="UTF-8"?>')).toBe(true);
    expect(gpx).toContain('xmlns="http://www.topografix.com/GPX/1/1"');
    expect(gpx).toContain('<trkpt lat="45.798300" lon="24.125600"/>');
    expect(gpx.indexOf('<wpt')).toBeLessThan(gpx.indexOf('<trk>'));
  });

  it('escapes names', () => {
    expect(gpx).toContain('București → Sibiu &amp; &lt;back&gt;');
    expect(escapeXml(`"a'`)).toBe('&quot;a&apos;');
  });

  it('builds a plain file name', () => {
    expect(gpxFileName('București → Râmnicu Vâlcea')).toBe('bucuresti-ramnicu-valcea.gpx');
    expect(gpxFileName('→')).toBe('traseu.gpx');
  });
});

describe('rainLine', () => {
  it('always pairs chance, amount and intensity', () => {
    const slot = { precipitation_probability: 45, precipitation_mm: 1.2, rain_intensity: 'slaba' as const };
    expect(rainLine(slot, 'ro')).toEqual({ text: 'ploaie: 45% șanse · 1,2 mm/h, slabă', wet: true });
    expect(rainLine(slot, 'en').text).toBe('rain: 45% chance · 1.2 mm/h, light');
  });

  it('says there is no rain when nothing is measurable', () => {
    expect(rainLine({ precipitation_probability: 0, precipitation_mm: 0, rain_intensity: 'none' }, 'ro').text).toBe('fără ploaie');
    expect(rainLine({ precipitation_probability: 5, precipitation_mm: 0, rain_intensity: 'none' }, 'ro').text).toBe('fără ploaie (5% șanse, 0 mm)');
  });

  it('falls back to the amount when the chance is missing', () => {
    expect(rainLine({ precipitation_probability: null, precipitation_mm: 3.4, rain_intensity: null }, 'ro').text).toBe('ploaie: 3,4 mm/h, moderată');
  });
});

describe('osrm', () => {
  const body = {
    code: 'Ok',
    routes: [
      {
        distance: 222_400,
        geometry: { coordinates: [[26, 44], [27, 44], [28, 44]] },
        legs: [
          { distance: 111_200, steps: [{ distance: 60_000, ref: 'DN7;E81', name: 'Calea' }, { distance: 51_200, ref: 'DN7' }] },
          { distance: 111_200, steps: [{ distance: 111_200, name: 'Transalpina' }] },
        ],
      },
    ],
  };

  it('parses geometry, scaled distances, stops and road names', () => {
    const route = parseOsrm(body);
    expect(route.coords[1]).toEqual({ lat: 44, lon: 27 });
    expect(route.distanceKm).toBeCloseTo(222.4, 6);
    expect(route.cumKm[route.cumKm.length - 1]).toBeCloseTo(222.4, 6);
    expect(route.stopKm).toEqual([0, 111.2, 222.4]);
    expect(route.roads).toEqual([
      { fromKm: 0, toKm: 111.2, label: 'DN7' },
      { fromKm: 111.2, toKm: 222.4, label: 'Transalpina' },
    ]);
    expect(roadAt(route.roads, 150)).toBe('Transalpina');
  });

  it('rejects answers without a route', () => {
    expect(() => parseOsrm({ code: 'NoRoute' })).toThrow(RouteError);
    expect(() => parseOsrm(null)).toThrow(RouteError);
  });

  it('builds the request with lon,lat pairs', () => {
    expect(osrmUrl([{ lat: 44.4268, lon: 26.1025 }, { lat: 45.7983, lon: 24.1256 }])).toBe(
      'https://router.project-osrm.org/route/v1/driving/26.10250,44.42680;24.12560,45.79830?overview=full&geometries=geojson&steps=true',
    );
  });
});

describe('runPool', () => {
  it('never runs more than the limit at once and finishes everything', async () => {
    let running = 0;
    let peak = 0;
    const done: number[] = [];
    await runPool([1, 2, 3, 4, 5, 6, 7], 3, async (n) => {
      running += 1;
      peak = Math.max(peak, running);
      await new Promise((r) => setTimeout(r, 5));
      done.push(n);
      running -= 1;
    });
    expect(peak).toBe(3);
    expect(done.sort()).toEqual([1, 2, 3, 4, 5, 6, 7]);
  });

  it('stops launching work after an abort', async () => {
    const controller = new AbortController();
    const started: number[] = [];
    await runPool([1, 2, 3, 4], 1, async (n) => {
      started.push(n);
      if (n === 2) controller.abort();
    }, controller.signal);
    expect(started).toEqual([1, 2]);
  });
});

describe('hazards', () => {
  const route = parseOsrm({ code: 'Ok', routes: [{ distance: 222_400, geometry: { coordinates: [[26, 44], [27, 44], [28, 44]] }, legs: [] }] });
  const hz = (id: number, lat: number, lon: number): Hazard => ({
    id, lat, lon, hazard_type: 'gravel', severity: 3, description: 'x', distance_km: 0, created_at: '2026-09-11T08:00:00', expires_at: '',
  });

  it('dedupes, drops far ones and sorts along the route', () => {
    const placed = placeHazards([hz(2, 44.05, 28), hz(1, 44, 26.1), hz(2, 44.05, 28), hz(3, 46, 27)], route);
    expect(placed.map((h) => h.id)).toEqual([1, 2]);
    expect(placed[1].routeKm).toBeCloseTo(222.4, 1);
  });

  it('treats timestamps without a zone as UTC', () => {
    expect(serverTimeMs('2026-09-11T08:00:00')).toBe(Date.UTC(2026, 8, 11, 8));
    expect(serverTimeMs('2026-09-11T08:00:00+00:00')).toBe(Date.UTC(2026, 8, 11, 8));
  });
});

describe('stops', () => {
  it('moves, adds before the destination and keeps 2 to 5 rows', () => {
    expect(moveItem(['a', 'b', 'c'], 2, 0)).toEqual(['c', 'a', 'b']);
    expect(moveItem(['a', 'b'], 0, 5)).toEqual(['a', 'b']);
    const two = [newStop({ name: 'A', lat: 1, lon: 1 }), newStop({ name: 'B', lat: 2, lon: 2 })];
    const three = addStop(two);
    expect(three.map((s) => s.text)).toEqual(['A', '', 'B']);
    expect(removeStop(two, two[0].id)).toHaveLength(2);
    let many = two;
    for (let i = 0; i < 10; i += 1) many = addStop(many);
    expect(many).toHaveLength(MAX_STOPS);
    expect(roleOf(0, 3)).toBe('origin');
    expect(roleOf(1, 3)).toBe('via');
    expect(roleOf(2, 3)).toBe('destination');
  });

  it('saves villages with their county', () => {
    expect(savedLabel(newStop({ name: 'Sâmbăta', lat: 45.7, lon: 24.8 }, { region: 'Brașov' }))).toBe('Sâmbăta, Brașov');
    expect(savedLabel(newStop({ name: 'Sibiu', lat: 45.8, lon: 24.1 }))).toBe('Sibiu');
  });
});
