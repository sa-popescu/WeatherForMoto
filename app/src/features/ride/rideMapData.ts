import type { FeatureCollection, MultiLineString } from 'geojson';
import type { LatLon } from '../route/types';
import type { GapFill } from './gapFill';
import { routeSlice, type RideRoute } from './logic/follow';
import type { RidePoint } from './logic/track';

// What the ride map draws, as plain GeoJSON built from the recorder's data,
// kept apart from MapLibre so it can be tested. Coordinates are [lon, lat].

export type Coord = [number, number];

const toCoord = (p: LatLon): Coord => [p.lon, p.lat];

export interface TrackLines {
  /** The line as the GPS gave it, broken at every pause and every gap. */
  solid: Coord[][];
  /** One piece per gap: along the road once its fill is in, straight until then. */
  gaps: Coord[][];
}

export function trackLines(points: readonly RidePoint[], fills: readonly GapFill[]): TrackLines {
  const fillAt = new Map(fills.map((f) => [f.t, f]));
  const solid: Coord[][] = [];
  const gaps: Coord[][] = [];
  let current: Coord[] | null = null;
  let prev: RidePoint | null = null;
  for (const p of points) {
    // Heartbeats while standing would only scribble GPS jitter.
    if (p.still) continue;
    const here = toCoord(p);
    const sameSeg = prev !== null && prev.seg === p.seg;
    if (!current || !sameSeg || p.gap) {
      if (p.gap && prev && sameSeg) {
        const fill = fillAt.get(p.t);
        gaps.push(fill ? [toCoord(prev), ...fill.coords.map(toCoord), here] : [toCoord(prev), here]);
      }
      current = [];
      solid.push(current);
    }
    current.push(here);
    prev = p;
  }
  return { solid: solid.filter((line) => line.length >= 2), gaps };
}

/** The followed route split at the progress: the part behind fades, the part ahead stands out. */
export function routeParts(route: RideRoute, km: number | null): { done: Coord[]; ahead: Coord[] } {
  if (km === null || km <= 0) return { done: [], ahead: route.coords.map(toCoord) };
  if (km >= route.distanceKm) return { done: route.coords.map(toCoord), ahead: [] };
  return { done: routeSlice(route, 0, km).map(toCoord), ahead: routeSlice(route, km, route.distanceKm).map(toCoord) };
}

export function multiLine(lines: readonly Coord[][]): FeatureCollection<MultiLineString> {
  const coordinates = lines.filter((line) => line.length >= 2);
  return {
    type: 'FeatureCollection',
    features: coordinates.length === 0 ? [] : [{ type: 'Feature', properties: {}, geometry: { type: 'MultiLineString', coordinates } }],
  };
}

/** Direction of travel from a to b, degrees clockwise from north (0 to 360). */
export function bearingDeg(a: LatLon, b: LatLon): number {
  const rad = Math.PI / 180;
  const y = Math.sin((b.lon - a.lon) * rad) * Math.cos(b.lat * rad);
  const x = Math.cos(a.lat * rad) * Math.sin(b.lat * rad) - Math.sin(a.lat * rad) * Math.cos(b.lat * rad) * Math.cos((b.lon - a.lon) * rad);
  return ((Math.atan2(y, x) / rad) % 360 + 360) % 360;
}

/** [west, south, east, north] around some positions. */
export function boundsOf(points: readonly LatLon[]): [number, number, number, number] {
  let west = Infinity;
  let south = Infinity;
  let east = -Infinity;
  let north = -Infinity;
  for (const p of points) {
    west = Math.min(west, p.lon);
    east = Math.max(east, p.lon);
    south = Math.min(south, p.lat);
    north = Math.max(north, p.lat);
  }
  return [west, south, east, north];
}

