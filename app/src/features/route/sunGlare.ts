import { headingAt } from './crosswind';
import { pointAtKm } from './geometry';
import { roadAt } from './osrm';
import type { TimelineRow } from './plan';
import type { RouteLine } from './types';

// The sun in the rider's eyes. Low in the morning and the evening, a sun
// straight ahead blinds through the visor and hides what is in front, the
// way a car's driver is hidden from a rider coming the other way. Where the
// road runs toward a sun this low, under a sky clear enough for it to shine,
// the stretch is worth a warning, with when and where it starts.
//
// The sun's place comes from the standard low-precision formulas (the
// Astronomical Almanac's, good to about a degree), so no library is needed.
// The direction of travel is the one the crosswind uses (crosswind.ts).

/** A sun lower than this, degrees above the horizon, shines into the visor. */
export const GLARE_MAX_ELEVATION = 20;
/** Below this it is behind hills, trees and haze more often than not. */
export const GLARE_MIN_ELEVATION = 1;
/** Within this many degrees either side of the direction of travel, it is in the eyes. */
export const GLARE_HALF_ANGLE = 25;
/** With more of the sky covered than this, % (Open-Meteo's cloud_cover), the sun does not shine through. */
export const GLARE_MAX_CLOUD = 60;
/** The road is checked this often, km. */
export const GLARE_STEP_KM = 2;
/** A stretch shorter than this is a bend, not a road into the sun, km. */
export const GLARE_MIN_STRETCH_KM = 3;

const RAD = Math.PI / 180;
/** 2000-01-01 12:00 UTC, the epoch of the formulas. */
const J2000_MS = Date.UTC(2000, 0, 1, 12);

export interface SunPosition {
  /** Compass bearing of the sun, degrees: 0 north, 90 east. */
  azimuth: number;
  /** Degrees above the horizon (negative below). */
  elevation: number;
}

const wrap360 = (deg: number): number => ((deg % 360) + 360) % 360;

/** Where the sun is in the sky, seen from `lat`, `lon` at `ms` (UTC). */
export function sunPosition(ms: number, lat: number, lon: number): SunPosition {
  const d = (ms - J2000_MS) / 86_400_000;
  const meanAnomaly = wrap360(357.529 + 0.98560028 * d) * RAD;
  const meanLongitude = wrap360(280.459 + 0.98564736 * d);
  const eclipticLongitude = (meanLongitude + 1.915 * Math.sin(meanAnomaly) + 0.02 * Math.sin(2 * meanAnomaly)) * RAD;
  const obliquity = (23.439 - 0.00000036 * d) * RAD;
  const rightAscension = Math.atan2(Math.cos(obliquity) * Math.sin(eclipticLongitude), Math.cos(eclipticLongitude));
  const declination = Math.asin(Math.sin(obliquity) * Math.sin(eclipticLongitude));
  const siderealDeg = wrap360(280.46061837 + 360.98564736629 * d + lon);
  const hourAngle = siderealDeg * RAD - rightAscension;
  const phi = lat * RAD;
  const elevation = Math.asin(Math.sin(phi) * Math.sin(declination) + Math.cos(phi) * Math.cos(declination) * Math.cos(hourAngle));
  const azimuth = Math.atan2(-Math.sin(hourAngle), Math.cos(phi) * Math.tan(declination) - Math.sin(phi) * Math.cos(hourAngle));
  return { azimuth: wrap360(azimuth / RAD), elevation: elevation / RAD };
}

/** Degrees between two compass bearings, 0 to 180. */
export function bearingGap(a: number, b: number): number {
  return Math.abs((((a - b) % 360) + 540) % 360 - 180);
}

/**
 * Whether a rider heading `heading` (null where the road winds too much to
 * have one) has the sun in the eyes. An unknown cloud cover counts as clear:
 * a warning that may not be needed beats a sun that was not expected.
 */
export function sunInEyes(heading: number | null, sun: SunPosition, cloudCover: number | null): boolean {
  if (heading === null) return false;
  if (sun.elevation < GLARE_MIN_ELEVATION || sun.elevation > GLARE_MAX_ELEVATION) return false;
  if (cloudCover !== null && cloudCover > GLARE_MAX_CLOUD) return false;
  return bearingGap(heading, sun.azimuth) <= GLARE_HALF_ANGLE;
}

export interface GlareStretch {
  fromKm: number;
  toKm: number;
  /** When the rider gets to its start, UTC ms. */
  atMs: number;
}

/**
 * The stretches of `route` ridden into a low sun. `timeAtKm` says when the
 * rider gets to a distance along the route; `cloudAt` the cloud cover there
 * at that moment (null when unknown).
 */
export function glareStretches(
  route: RouteLine,
  timeAtKm: (km: number) => number,
  cloudAt: (km: number, ms: number) => number | null,
): GlareStretch[] {
  const stretches: GlareStretch[] = [];
  if (route.coords.length < 2 || route.distanceKm <= 0) return stretches;
  let open: GlareStretch | null = null;
  for (let km = 0; km <= route.distanceKm + 1e-9; km += GLARE_STEP_KM) {
    const at = timeAtKm(km);
    const where = pointAtKm(route.coords, route.cumKm, km);
    const glare = sunInEyes(headingAt(route, km), sunPosition(at, where.lat, where.lon), cloudAt(km, at));
    if (glare) {
      if (open) open.toKm = Math.min(route.distanceKm, km + GLARE_STEP_KM / 2);
      else open = { fromKm: Math.max(0, km - GLARE_STEP_KM / 2), toKm: Math.min(route.distanceKm, km + GLARE_STEP_KM / 2), atMs: at };
    } else if (open) {
      stretches.push(open);
      open = null;
    }
  }
  if (open) stretches.push(open);
  return stretches.filter((s) => s.toKm - s.fromKm >= GLARE_MIN_STRETCH_KM);
}

/** The time at each distance, from points with known passing times (sorted by km): straight lines between them. */
export function timeAlong(points: ReadonlyArray<{ km: number; atMs: number }>): (km: number) => number {
  return (km) => {
    if (points.length === 0) return Number.NaN;
    if (km <= points[0].km) return points[0].atMs;
    for (let i = 1; i < points.length; i += 1) {
      const a = points[i - 1];
      const b = points[i];
      if (km <= b.km) return b.km === a.km ? a.atMs : a.atMs + ((km - a.km) / (b.km - a.km)) * (b.atMs - a.atMs);
    }
    return points[points.length - 1].atMs;
  };
}

// ---- The planner's timeline -------------------------------------------------

export interface GlareWarning {
  /** The first stretch ridden into a low sun. */
  stretch: GlareStretch;
  /** Other stretches after it. */
  more: number;
  /** Wall clock where it starts, "YYYY-MM-DDTHH:MM". */
  localIso: string;
  /** The road it is on, else the nearest weather point. */
  place: string;
}

function nearestRow<T extends { point: { km: number } }>(rows: readonly T[], km: number): T {
  return rows.reduce((best, row) => (Math.abs(row.point.km - km) < Math.abs(best.point.km - km) ? row : best));
}

/**
 * The planner's warning, from its timeline: when each point is reached, and
 * the forecast there for that hour. A point without its forecast yet counts
 * as overcast, so nothing is said before the weather is known.
 */
export function timelineGlare(route: RouteLine, rows: readonly TimelineRow[]): GlareWarning | null {
  if (rows.length === 0) return null;
  const timeAtKm = timeAlong(rows.map((row) => ({ km: row.point.km, atMs: row.etaUtcMs })));
  const cloudAt = (km: number): number | null => {
    const slot = nearestRow(rows, km).slot;
    return slot ? slot.cloud_cover : 100;
  };
  const [first, ...rest] = glareStretches(route, timeAtKm, cloudAt);
  if (!first) return null;
  const row = nearestRow(rows, first.fromKm);
  const offsetMs = Date.parse(`${row.etaLocal}Z`) - row.etaUtcMs;
  return {
    stretch: first,
    more: rest.length,
    localIso: new Date(first.atMs + (Number.isFinite(offsetMs) ? offsetMs : 0)).toISOString().slice(0, 16),
    place: roadAt(route.roads, first.fromKm) ?? row.point.name,
  };
}
