import { cumulativeKm } from './geometry';
import type { LatLon, RoadSpan, RouteLine } from './types';

// Driving route from the public OSRM demo server. Only geometry, distances
// and road names are used; OSRM's car durations are ignored because the ETA
// comes from the rider's own average speed.

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
}

interface OsrmLeg {
  distance: number;
  steps?: OsrmStep[];
}

interface OsrmRoute {
  distance: number;
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
  return { coords, cumKm: geomCum.map((k) => k * scale), distanceKm, stopKm, roads: roadSpans(route.legs ?? []) };
}

export async function fetchRoute(stops: readonly LatLon[], signal: AbortSignal): Promise<RouteLine> {
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
  return parseOsrm(body);
}

/** Label of the road at a distance along the route, if OSRM named it. */
export function roadAt(roads: readonly RoadSpan[], km: number): string | null {
  return roads.find((r) => km >= r.fromKm && km <= r.toKm)?.label ?? null;
}
