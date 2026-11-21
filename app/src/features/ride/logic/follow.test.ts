import { describe, expect, it } from 'vitest';
import { cumulativeKm } from '../../route/geometry';
import type { LatLon } from '../../route/types';
import { arrivalAt, fillFromRoute, NEW_FOLLOW, projectOnRoute, routeSlice, stepFollow, type FollowState, type RideRoute } from './follow';
import type { RideFix } from './track';

const M_PER_DEG = 111_195;
const LAT0 = 45;
const LON0 = 25;
const lonPerM = 1 / (M_PER_DEG * Math.cos((LAT0 * Math.PI) / 180));

/** x metres east, y metres north of the origin. */
function at(x: number, y: number): LatLon {
  return { lat: LAT0 + y / M_PER_DEG, lon: LON0 + x * lonPerM };
}

function routeOf(points: LatLon[], name = 'Sibiu'): RideRoute {
  const cumKm = cumulativeKm(points);
  return { name, coords: points, cumKm, distanceKm: cumKm[cumKm.length - 1], plannedKmh: 60 };
}

// 2 km east with only two vertices: a long straight road.
const straight = routeOf([at(0, 0), at(2000, 0)]);
// Out 1 km east and back 1 km on a parallel road 30 m north.
const outAndBack = routeOf([at(0, 0), at(1000, 0), at(1000, 30), at(0, 30)]);

function fix(t: number, p: LatLon, acc = 5): RideFix {
  return { t, lat: p.lat, lon: p.lon, acc, speed: 15, alt: null };
}

describe('projectOnRoute', () => {
  it('measures the distance to the road between two far vertices, not to the vertices', () => {
    const r = projectOnRoute(straight, at(1000, 12), null);
    expect(r.km).toBeCloseTo(1, 2);
    expect(r.offM).toBeCloseTo(12, 0);
  });

  it('keeps to the leg near the last progress on a route that comes back alongside itself', () => {
    // 20 m north sits between the two legs, a bit closer to the way back.
    expect(projectOnRoute(outAndBack, at(300, 20), 0.25).km).toBeCloseTo(0.3, 2);
    expect(projectOnRoute(outAndBack, at(300, 20), null).km).toBeGreaterThan(1.5);
  });
});

describe('stepFollow', () => {
  function run(fixes: RideFix[]): { state: FollowState; events: string[] } {
    let state = NEW_FOLLOW;
    const events: string[] = [];
    for (const f of fixes) {
      const step = stepFollow(state, straight, f);
      state = step.state;
      if (step.event) events.push(step.event);
    }
    return { state, events };
  }

  it('says off the route only after a while, and back once close again', () => {
    const fixes = [
      fix(0, at(100, 0)),
      // A wide junction: 120 m off for 5 s is not worth a word.
      ...[1, 2, 3, 4, 5].map((i) => fix(i * 1000, at(100 + i * 10, 120))),
      fix(6000, at(160, 0)),
      // Really off: 10 s at 200 m.
      ...Array.from({ length: 11 }, (_, i) => fix(7000 + i * 1000, at(200, 200))),
      fix(19_000, at(260, 10)),
    ];
    const { events, state } = run(fixes);
    expect(events).toEqual(['off', 'back']);
    expect(state.km).toBeCloseTo(0.26, 2);
  });

  it('keeps the progress where it was while off the route', () => {
    const { state } = run([fix(0, at(500, 0)), fix(1000, at(1500, 400))]);
    expect(state.km).toBeCloseTo(0.5, 2);
  });

  it('says arrived once, near the end', () => {
    const { events } = run([fix(0, at(1800, 0)), fix(1000, at(1900, 0)), fix(2000, at(1990, 0)), fix(3000, at(2000, 0))]);
    expect(events).toEqual(['arrived']);
  });
});

describe('route fills and arrival', () => {
  it('fills a gap on the route with its own line', () => {
    const bend = routeOf([at(0, 0), at(500, 0), at(500, 500)]);
    const fill = fillFromRoute(bend, at(200, 5), at(500, 300), 0.2);
    expect(fill).not.toBeNull();
    expect(fill!.roadM).toBeCloseTo(600, -1);
    // Through the corner, not across it.
    expect(fill!.coords.some((c) => Math.abs(c.lon - at(500, 0).lon) < 1e-9 && Math.abs(c.lat - at(500, 0).lat) < 1e-9)).toBe(true);
  });

  it('leaves a gap off the route to the road service', () => {
    expect(fillFromRoute(straight, at(200, 0), at(900, 400), 0.2)).toBeNull();
  });

  it('cuts the route between two distances', () => {
    const part = routeSlice(straight, 0.5, 1.5);
    expect(part[0].lon).toBeCloseTo(at(500, 0).lon, 6);
    expect(part[part.length - 1].lon).toBeCloseTo(at(1500, 0).lon, 6);
  });

  it('uses the ride’s own average for the arrival once there is enough of it', () => {
    const now = 1_000_000;
    expect(arrivalAt(30, 45, 60_000, 60, now)).toBe(now + 30 * 60_000);
    expect(arrivalAt(30, 45, 10 * 60_000, 60, now)).toBe(now + 40 * 60_000);
  });
});
