import { describe, expect, it } from 'vitest';
import type { HourlyWeather, UrbanInstruction, UrbanRoute } from '../../../lib/types';
import type { PointWeather } from '../../route/types';
import { fromOsrm, fromUrban, mainRoad, roadSpans, routeKmh } from './driveRoute';
import { planDriveWeather, routeOutlook, weatherForRoute } from './driveWeather';

const OFFSET = 2 * 3600;
/** 10:00 local time (08:00 UTC). */
const NOW = Date.UTC(2026, 10, 22, 8, 0);

function ins(offsetM: number, maneuver: string, street: string | null, road: string | null = null): UrbanInstruction {
  return { offset_m: offsetM, index: null, time_s: null, maneuver, street, road, exit: null, roundabout_exit: null, signpost: null };
}

/** A straight road north from 44.40, `km` long, one point per km. */
function urban(km: number, over: Partial<UrbanRoute> = {}): UrbanRoute {
  const coords = Array.from({ length: km + 1 }, (_, i) => [44.4 + i / 111.195, 26.1] as [number, number]);
  return { distance_m: km * 1000, duration_s: km * 60, no_traffic_s: km * 55, delay_s: 0, coords, traffic: [], instructions: [], ...over };
}

describe('fromUrban', () => {
  it('measures the line, places the traffic along it and names the roads', () => {
    const route = fromUrban(
      urban(10, {
        delay_s: 180,
        traffic: [{ from: 2, to: 4, category: 'JAM', magnitude: 2, delay_s: 180, speed_kmh: 12 }],
        instructions: [ins(0, 'DEPART', 'Bulevardul Unirii'), ins(3000, 'TURN_RIGHT', 'Calea Victoriei', 'DN1, E60'), ins(10_000, 'ARRIVE', null)],
      }),
      0,
    );
    expect(route).not.toBeNull();
    expect(route!.line.distanceKm).toBe(10);
    expect(route!.line.cumKm[route!.line.cumKm.length - 1]).toBeCloseTo(10, 6);
    expect(route!.traffic).toEqual([{ fromKm: expect.closeTo(2, 3), toKm: expect.closeTo(4, 3), category: 'JAM', magnitude: 2, delayS: 180 }]);
    expect(route!.line.roads).toEqual([
      { fromKm: 0, toKm: 3, label: 'Bulevardul Unirii' },
      { fromKm: 3, toKm: 10, label: 'DN1' },
    ]);
    expect(route!.delayS).toBe(180);
    expect(routeKmh(route!)).toBeCloseTo(60, 6);
    expect(mainRoad(route!)).toBe('DN1');
  });

  it('drops a route without a line, and traffic it cannot place', () => {
    expect(fromUrban(urban(0), 0)).toBeNull();
    const route = fromUrban(urban(5, { traffic: [{ from: null, to: 3, category: 'JAM', magnitude: 1, delay_s: 30, speed_kmh: null }] }), 1);
    expect(route?.traffic).toEqual([]);
    expect(route?.id).toBe('tomtom-1');
  });
});

describe('roadSpans', () => {
  it('merges neighbours on the same road and skips turns with no name', () => {
    const spans = roadSpans([ins(0, 'DEPART', 'A'), ins(1000, 'STRAIGHT', 'A'), ins(2000, 'TURN_LEFT', null), ins(2500, 'TURN_RIGHT', 'B')], 4);
    expect(spans).toEqual([
      { fromKm: 0, toKm: 2, label: 'A' },
      { fromKm: 2.5, toKm: 4, label: 'B' },
    ]);
  });
});

describe('fromOsrm', () => {
  it('keeps OSRM time, or makes one up at 50 km/h', () => {
    const line = { coords: [{ lat: 44.4, lon: 26.1 }, { lat: 44.5, lon: 26.1 }], cumKm: [0, 11.1], distanceKm: 11.1, stopKm: [0, 11.1], roads: [] };
    expect(fromOsrm(line, 900).durationS).toBe(900);
    expect(fromOsrm(line, null).durationS).toBeCloseTo((11.1 / 50) * 3600, 6);
    expect(fromOsrm(line, null).traffic).toEqual([]);
  });
});

function forecast(rainFromHour: number | null): PointWeather {
  const hourly = Array.from({ length: 30 }, (_, i) => {
    const hour = i % 24;
    const day = 22 + Math.floor(i / 24);
    const wet = rainFromHour !== null && i >= rainFromHour;
    return {
      time: `2026-11-${day}T${String(hour).padStart(2, '0')}:00`,
      rain_intensity: wet ? 'moderata' : 'none',
      precipitation_mm: wet ? 3 : 0,
      precipitation_probability: wet ? 90 : 0,
      wind_gusts_kmh: 20,
      frost_risk: false,
      moto_score: wet ? 35 : 80,
    } as unknown as HourlyWeather;
  });
  return { status: 'ready', forecast: { hourly, utcOffsetSeconds: OFFSET, sun: [] } };
}

describe('planDriveWeather', () => {
  const origin = { name: 'Start', lat: 44.4, lon: 26.1 };
  const dest = { name: 'End', lat: 44.4 + 100 / 111.195, lon: 26.1 };
  const name = (road: string | null, previous: string) => road ?? `după ${previous}`;

  it('loads close points once for all the routes', () => {
    const a = fromUrban(urban(100), 0)!;
    const b = fromUrban(urban(100), 1)!;
    const plan = planDriveWeather([a, b], origin, dest, name);
    // The two routes are the same road: their points are shared one to one.
    expect(plan.shared).toHaveLength(plan.routes[0].points.length);
    expect(plan.routes[1].points.map((p) => plan.routes[1].shared[p.id])).toEqual(plan.shared.map((p) => p.id));
    expect(new Set(plan.routes.flatMap((r) => r.points.map((p) => p.id))).size).toBe(plan.routes[0].points.length * 2);
  });

  it('times the weather to the moment the rider passes, for each route', () => {
    const route = fromUrban(urban(100, { duration_s: 100 * 60 }), 0)!;
    const plan = planDriveWeather([route], origin, dest, name);
    const end = plan.shared[plan.shared.length - 1].id;
    // Rain from 11:00 at the far end only; the rider gets there at 11:40.
    const shared = Object.fromEntries(plan.shared.map((p) => [p.id, p.id === end ? forecast(11) : forecast(null)]));
    const weather = weatherForRoute(plan.routes[0], shared);
    const outlook = routeOutlook(plan.routes[0].points, weather, routeKmh(route), 100, NOW);
    expect(outlook.settled).toBe(true);
    expect(outlook.warning).toMatchObject({ kind: 'rain', name: 'End', minutes: 100 });
    expect(outlook.worstScore).toBe(35);
    // Leaving three hours earlier, the rain is not there yet.
    const early = routeOutlook(plan.routes[0].points, weather, routeKmh(route), 100, NOW - 3 * 3_600_000);
    expect(early.warning).toBeNull();
    expect(early.worstScore).toBe(80);
  });

  it('knows when the rider gets somewhere after sunset', () => {
    const route = fromUrban(urban(100, { duration_s: 100 * 60 }), 0)!;
    const plan = planDriveWeather([route], origin, dest, name);
    const withSun = (w: PointWeather): PointWeather => ({ ...w, forecast: { ...w.forecast!, sun: [{ date: '2026-11-22', sunrise: '2026-11-22T07:20', sunset: '2026-11-22T16:45' }] } });
    const shared = Object.fromEntries(plan.shared.map((p) => [p.id, withSun(forecast(null))]));
    const weather = weatherForRoute(plan.routes[0], shared);
    expect(routeOutlook(plan.routes[0].points, weather, routeKmh(route), 100, NOW).dark).toBe(false);
    // Leaving at 15:30 local (13:30 UTC) arrives at 17:10, after sunset.
    expect(routeOutlook(plan.routes[0].points, weather, routeKmh(route), 100, Date.UTC(2026, 10, 22, 13, 30)).dark).toBe(true);
  });

  it('is not settled while a point still loads', () => {
    const route = fromUrban(urban(10), 0)!;
    const plan = planDriveWeather([route], origin, { ...dest, lat: 44.49 }, name);
    const outlook = routeOutlook(plan.routes[0].points, weatherForRoute(plan.routes[0], {}), 60, 10, NOW);
    expect(outlook.settled).toBe(false);
    expect(outlook.worstScore).toBeNull();
  });
});
