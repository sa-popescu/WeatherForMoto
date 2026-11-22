import type { UrbanInstruction } from '../../lib/types';
import { cumulativeKm } from './geometry';
import type { LatLon, RoadSpan, RouteLine } from './types';

// Driving route from the public OSRM demo server: the geometry, distances,
// road names and the turns, written in TomTom's maneuver codes so the ride's
// turn by turn reads both the same way (ride/logic/turns.ts). OSRM's car
// durations are ignored by the planner, whose ETA comes from the rider's own
// average speed; "Drum" uses them when TomTom cannot answer.

const OSRM_BASE = 'https://router.project-osrm.org/route/v1/driving/';
const OSRM_TIMEOUT_MS = 15_000;

export type RouteErrorKind = 'no-route' | 'network' | 'geocode';

export class RouteError extends Error {
  readonly kind: RouteErrorKind;

  constructor(kind: RouteErrorKind, detail: string = kind) {
    super(detail);
    this.name = 'RouteError';
    this.kind = kind;
  }
}

interface OsrmStep {
  distance: number;
  name?: string;
  ref?: string;
  /** Signposts at a motorway exit: "A1: Pitești". */
  destinations?: string;
  exits?: string;
  maneuver?: { type?: string; modifier?: string; exit?: number };
}

interface OsrmLeg {
  distance: number;
  steps?: OsrmStep[];
}

interface OsrmRoute {
  distance: number;
  /** Seconds by car, at the speed limits: only "Drum" uses it, as a fallback. */
  duration?: number;
  geometry: { coordinates: Array<[number, number]> };
  legs: OsrmLeg[];
}

interface OsrmResponse {
  code?: string;
  routes?: OsrmRoute[];
}

export function osrmUrl(stops: readonly LatLon[]): string {
  const coords = stops.map((s) => `${s.lon.toFixed(5)},${s.lat.toFixed(5)}`).join(';');
  return `${OSRM_BASE}${coords}?overview=full&geometries=geojson&steps=true`;
}

/** "DN7;E81" -> "DN7"; falls back to the street name. */
function roadLabel(step: OsrmStep): string {
  const ref = step.ref?.split(';')[0]?.trim();
  return ref || step.name?.trim() || '';
}

function roadSpans(legs: readonly OsrmLeg[]): RoadSpan[] {
  const spans: RoadSpan[] = [];
  let km = 0;
  for (const leg of legs) {
    for (const step of leg.steps ?? []) {
      const len = (step.distance || 0) / 1000;
      const label = roadLabel(step);
      const last = spans[spans.length - 1];
      if (label && last && last.label === label && Math.abs(last.toKm - km) < 0.001) last.toKm = km + len;
      else if (label && len > 0) spans.push({ fromKm: km, toKm: km + len, label });
      km += len;
    }
  }
  return spans;
}

const BY_MODIFIER: Record<string, string> = {
  uturn: 'MAKE_UTURN',
  'sharp right': 'SHARP_RIGHT',
  right: 'TURN_RIGHT',
  'slight right': 'BEAR_RIGHT',
  straight: 'STRAIGHT',
  'slight left': 'BEAR_LEFT',
  left: 'TURN_LEFT',
  'sharp left': 'SHARP_LEFT',
};

const leftward = (modifier: string): boolean => modifier.includes('left');
const rightward = (modifier: string): boolean => modifier.includes('right');

/** An OSRM maneuver (type and modifier) as the TomTom code that means the same. */
export function maneuverCode(type: string, modifier: string, lastLeg: boolean): string {
  switch (type) {
    case 'depart':
      return 'DEPART';
    case 'arrive':
      if (!lastLeg) return 'WAYPOINT_REACHED';
      return rightward(modifier) ? 'ARRIVE_RIGHT' : leftward(modifier) ? 'ARRIVE_LEFT' : 'ARRIVE';
    case 'new name':
    case 'notification':
    case 'exit roundabout':
    case 'exit rotary':
      return 'STRAIGHT';
    case 'on ramp':
      return 'ENTRANCE_RAMP';
    case 'off ramp':
      return leftward(modifier) ? 'MOTORWAY_EXIT_LEFT' : 'MOTORWAY_EXIT_RIGHT';
    case 'fork':
      return leftward(modifier) ? 'KEEP_LEFT' : rightward(modifier) ? 'KEEP_RIGHT' : 'STRAIGHT';
    case 'merge':
      return leftward(modifier) ? 'BEAR_LEFT' : rightward(modifier) ? 'BEAR_RIGHT' : 'STRAIGHT';
    case 'roundabout':
    case 'rotary':
      if (modifier === 'uturn') return 'ROUNDABOUT_BACK';
      if (modifier === 'straight') return 'ROUNDABOUT_CROSS';
      return leftward(modifier) ? 'ROUNDABOUT_LEFT' : 'ROUNDABOUT_RIGHT';
    default:
      // turn, end of road, continue, roundabout turn: the modifier says it all.
      return BY_MODIFIER[modifier] ?? 'STRAIGHT';
  }
}

const firstRef = (ref: string | undefined): string | null => ref?.split(';')[0]?.trim() || null;

/** The turns of an OSRM route, with their distance along it, as the ride reads them. */
export function osrmInstructions(legs: readonly OsrmLeg[]): UrbanInstruction[] {
  const out: UrbanInstruction[] = [];
  let metres = 0;
  legs.forEach((leg, legIndex) => {
    const steps = leg.steps ?? [];
    steps.forEach((step, i) => {
      const type = step.maneuver?.type ?? '';
      const maneuver = maneuverCode(type, step.maneuver?.modifier ?? 'straight', legIndex === legs.length - 1);
      // Onto a motorway, the road worth naming is the one the ramp leads to.
      const road = type === 'on ramp' ? firstRef(steps[i + 1]?.ref) : firstRef(step.ref);
      out.push({
        offset_m: Math.round(metres),
        index: null,
        time_s: null,
        maneuver,
        street: step.name?.trim() || null,
        road,
        exit: step.exits?.split(';')[0]?.trim() || null,
        roundabout_exit: type === 'roundabout' || type === 'rotary' ? (step.maneuver?.exit ?? null) : null,
        signpost: step.destinations?.trim() || null,
      });
      metres += step.distance || 0;
    });
  });
  return out;
}

/** Validates an OSRM answer and turns it into the screen's RouteLine. */
export function parseOsrm(body: unknown): RouteLine {
  const res = body as OsrmResponse | null;
  const route = res?.code === 'Ok' ? res.routes?.[0] : undefined;
  const raw = route?.geometry?.coordinates;
  if (!route || !Array.isArray(raw) || raw.length < 2) throw new RouteError('no-route', res?.code ?? 'no route');
  const coords = raw.map(([lon, lat]) => ({ lat, lon }));
  const distanceKm = route.distance / 1000;
  const geomCum = cumulativeKm(coords);
  const geomTotal = geomCum[geomCum.length - 1];
  const scale = geomTotal > 0 ? distanceKm / geomTotal : 1;
  const stopKm = [0];
  for (const leg of route.legs ?? []) stopKm.push(stopKm[stopKm.length - 1] + (leg.distance || 0) / 1000);
  return {
    coords,
    cumKm: geomCum.map((k) => k * scale),
    distanceKm,
    stopKm,
    roads: roadSpans(route.legs ?? []),
    instructions: osrmInstructions(route.legs ?? []),
  };
}

export async function fetchRoute(stops: readonly LatLon[], signal: AbortSignal): Promise<RouteLine> {
  return parseOsrm(await fetchOsrmBody(stops, signal));
}

/** The route and OSRM's own driving time, for "Drum" when there is no traffic route. */
export async function fetchRouteWithTime(stops: readonly LatLon[], signal: AbortSignal): Promise<{ line: RouteLine; durationS: number | null }> {
  const body = await fetchOsrmBody(stops, signal);
  const line = parseOsrm(body);
  const duration = (body as OsrmResponse).routes?.[0]?.duration;
  return { line, durationS: typeof duration === 'number' && Number.isFinite(duration) ? duration : null };
}

async function fetchOsrmBody(stops: readonly LatLon[], signal: AbortSignal): Promise<unknown> {
  const controller = new AbortController();
  const forward = (): void => controller.abort();
  signal.addEventListener('abort', forward, { once: true });
  const timer = window.setTimeout(() => controller.abort(), OSRM_TIMEOUT_MS);
  let res: Response;
  try {
    res = await fetch(osrmUrl(stops), { signal: controller.signal });
  } catch (err) {
    if (signal.aborted) throw new DOMException('Aborted', 'AbortError');
    throw new RouteError('network', err instanceof Error ? err.message : 'network');
  } finally {
    window.clearTimeout(timer);
    signal.removeEventListener('abort', forward);
  }
  let body: unknown = null;
  try {
    body = await res.json();
  } catch {
    // An HTML error page from a proxy: only the status matters.
  }
  const code = (body as OsrmResponse | null)?.code;
  if (!res.ok) {
    if (code === 'NoRoute' || code === 'NoSegment') throw new RouteError('no-route', code);
    throw new RouteError('network', `OSRM HTTP ${res.status}`);
  }
  return body;
}

/** Label of the road at a distance along the route, if OSRM named it. */
export function roadAt(roads: readonly RoadSpan[], km: number): string | null {
  return roads.find((r) => km >= r.fromKm && km <= r.toKm)?.label ?? null;
}
