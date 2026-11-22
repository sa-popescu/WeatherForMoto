import type { LatLon } from './types';

// The high roads CNAIR closes every winter, and whether a route crosses one.
//
// The closing and opening dates are not fixed: CNAIR decides each year with
// the snow (the 2025-2026 season ran from 20 October to 12 June). So the app
// does not claim a road is closed; it says when it usually is, or may be, and
// points to the live closures.
//
// Each stretch is a few points on the road itself, about 2 km apart, taken
// from OpenStreetMap between the ends of the closed stretch CNAIR announces.
// A route that comes close to two of them drives on the closed stretch.

export interface SeasonalRoad {
  id: 'transfagarasan' | 'transalpina';
  /** The road's name and number, the same in both languages. */
  name: string;
  /** The closed stretch, between its two ends. */
  from: string;
  to: string;
  checkpoints: readonly LatLon[];
}

export const SEASONAL_ROADS: readonly SeasonalRoad[] = [
  {
    // DN7C km 104+000 to 130+800.
    id: 'transfagarasan',
    name: 'Transfăgărășan (DN7C)',
    from: 'Piscu Negru',
    to: 'Bâlea Cascadă',
    checkpoints: [
      { lat: 45.5576, lon: 24.60247 },
      { lat: 45.57462, lon: 24.61234 },
      { lat: 45.58276, lon: 24.6376 },
      { lat: 45.5952, lon: 24.61893 },
      { lat: 45.61279, lon: 24.61321 },
      { lat: 45.6308, lon: 24.61467 },
    ],
  },
  {
    // DN67C km 34+800 to 59+800.
    id: 'transalpina',
    name: 'Transalpina (DN67C)',
    from: 'Rânca',
    to: 'Obârșia Lotrului',
    checkpoints: [
      { lat: 45.29303, lon: 23.68642 },
      { lat: 45.31076, lon: 23.68206 },
      { lat: 45.32868, lon: 23.67848 },
      { lat: 45.34344, lon: 23.66374 },
      { lat: 45.36105, lon: 23.65841 },
      { lat: 45.37765, lon: 23.64762 },
      { lat: 45.39226, lon: 23.63268 },
      { lat: 45.40987, lon: 23.62721 },
    ],
  },
];

/** Where the Romanian Police list the roads closed right now. */
export const LIVE_CLOSURES_URL = 'https://www.politiaromana.ro/ro/info-trafic';

/** How close the route has to pass a checkpoint to be on that road. */
export const ON_ROAD_KM = 0.25;
/** Checkpoints passed before the route counts as driving the closed stretch. */
export const MIN_CHECKPOINTS = 2;

export type SeasonRisk = 'closed' | 'maybe' | 'open';

/**
 * How likely the high roads are closed on a day ("YYYY-MM-DD"):
 * - November to May: usually closed
 * - June to 10 July and October: they open late or close early some years
 * - the rest of the summer: open, short of a storm
 */
export function seasonRisk(date: string): SeasonRisk {
  const [, month, day] = date.split('-').map(Number);
  if (!month || !day) return 'open';
  if (month >= 11 || month <= 5) return 'closed';
  if (month === 6 || (month === 7 && day <= 10) || month === 10) return 'maybe';
  return 'open';
}

/** The phone's local day of a moment, "YYYY-MM-DD", for seasonRisk. */
export function localDay(ms: number): string {
  const d = new Date(ms);
  const pad = (n: number): string => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
}

const KM_PER_DEG_LAT = 111.32;

type Segment = readonly [LatLon, LatLon];

/** Distance from a point to a segment, km (flat projection, fine at this scale). */
export function segmentDistanceKm([a, b]: Segment, p: LatLon): number {
  const kx = KM_PER_DEG_LAT * Math.cos((p.lat * Math.PI) / 180);
  const ax = (a.lon - p.lon) * kx;
  const ay = (a.lat - p.lat) * KM_PER_DEG_LAT;
  const dx = (b.lon - p.lon) * kx - ax;
  const dy = (b.lat - p.lat) * KM_PER_DEG_LAT - ay;
  const len2 = dx * dx + dy * dy;
  const t = len2 === 0 ? 0 : Math.max(0, Math.min(1, -(ax * dx + ay * dy) / len2));
  return Math.hypot(ax + t * dx, ay + t * dy);
}

/** The route's segments that reach into a box around the points: keeps the check cheap on long routes. */
function segmentsNear(line: readonly LatLon[], points: readonly LatLon[], marginDeg: number): Segment[] {
  const lats = points.map((p) => p.lat);
  const lons = points.map((p) => p.lon);
  const south = Math.min(...lats) - marginDeg;
  const north = Math.max(...lats) + marginDeg;
  const west = Math.min(...lons) - marginDeg;
  const east = Math.max(...lons) + marginDeg;
  const out: Segment[] = [];
  for (let i = 0; i + 1 < line.length; i += 1) {
    const a = line[i];
    const b = line[i + 1];
    const misses =
      Math.max(a.lat, b.lat) < south || Math.min(a.lat, b.lat) > north || Math.max(a.lon, b.lon) < west || Math.min(a.lon, b.lon) > east;
    if (!misses) out.push([a, b]);
  }
  return out;
}

/** The seasonal roads whose closed stretch the route drives on. */
export function seasonalRoadsOnRoute(line: readonly LatLon[], roads: readonly SeasonalRoad[] = SEASONAL_ROADS): SeasonalRoad[] {
  return roads.filter((road) => {
    const near = segmentsNear(line, road.checkpoints, 0.01);
    if (near.length === 0) return false;
    const passed = road.checkpoints.filter((c) => near.some((segment) => segmentDistanceKm(segment, c) <= ON_ROAD_KM)).length;
    return passed >= MIN_CHECKPOINTS;
  });
}
