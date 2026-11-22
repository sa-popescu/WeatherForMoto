import { SHARE_BASE_URL } from '../../lib/placeLink';
import { cumulativeKm, nearestOnLine } from './geometry';
import { MAX_STOPS, MIN_STOPS } from './stops';
import { SPEEDS, type Speed } from './timing';
import { pickEvenly } from './track';
import type { Departure, LatLon, RouteLine } from './types';

// A ride plan as a link: the stops, the departure and the pace, so the person
// who opens it gets the same ride with the weather of the moment they look.
//
// Everything travels after the "#", which browsers never send to a server, so
// the stops stay out of every access log on the way: the link IS the plan. A
// link too long to paste comfortably is swapped for a short one when shared
// (shortLink.ts), which keeps this same query on Cloudflare for 90 days. A route calculated by the router carries only its stops,
// because routing them again gives the same road. A route imported from GPX
// carries its road too, simplified, because no router would draw it again.
//
//   https://motonor.bluemouse.cc/#/traseu?r=1&t=2026-11-22T08:00&k=75
//     &p=44.4268~26.1025~Bucure%C8%99ti&p=45.7983~24.1256~Sibiu&n=412&l=...

export const ROUTE_TAB_HASH = '#/traseu';
const VERSION = '1';
const MAX_NAME_LENGTH = 80;
/** A stop from a GPS fix is shared to about a kilometre: enough for the weather, not enough to find a front door. */
const GPS_DECIMALS = 2;
const STOP_DECIMALS = 4;
/** The road is kept to within this many metres before it is put in the link. */
const LINE_TOLERANCE_M = 50;
/** Most points a link carries (about 5 KB): a longer road is simplified harder. */
export const MAX_LINK_POINTS = 1000;
/** Points looked at by the simplification; a denser recording is thinned evenly first. */
const MAX_SIMPLIFY_INPUT = 5000;
/** Coordinates in the line are whole multiples of 1e-5 degrees (about a metre). */
const LINE_SCALE = 1e5;
/** URL-safe alphabet for the line: every character survives a link untouched. */
const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
const ALPHABET_INDEX = new Map([...ALPHABET].map((c, i) => [c, i]));
const COORD = /^-?\d{1,3}(?:\.\d{1,7})?$/;
const DEPARTURE = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2})$/;

/** A stop as the link carries it. */
export interface LinkStop {
  name: string;
  lat: number;
  lon: number;
}

/** A stop on its way into a link; `gps` marks one taken from the device position. */
export interface ShareStop extends LinkStop {
  gps: boolean;
}

export interface RouteLink {
  stops: LinkStop[];
  /** null when the link has none, or one this app cannot use. */
  departure: Departure | null;
  /** null when the link has none, or a pace the app does not offer. */
  speed: Speed | null;
  /** The road of an imported GPX, or null for a route the router draws again. */
  line: LatLon[] | null;
}

/** 'gone': a short link (shortLink.ts) that expired or never existed; the Worker sends it here as "r=gone". */
export type ParsedRouteLink = { kind: 'none' } | { kind: 'invalid' } | { kind: 'gone' } | { kind: 'route'; link: RouteLink };

/** The value of "r" in the address a dead short link opens (src/worker/shortLinks.ts). */
const GONE_VERSION = 'gone';

/** encodeURIComponent, plus the few characters it leaves that some chat apps cut a link at. */
function encodePart(text: string): string {
  return encodeURIComponent(text).replace(/[!'()*~]/g, (c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`);
}

const fixed = (value: number, decimals: number): string => String(Number(value.toFixed(decimals)));

// --- the road ------------------------------------------------------------------

/**
 * Douglas-Peucker: drops every point the line can do without and still stay
 * within `toleranceM` metres of where it was. Distances are measured on a flat
 * projection around the middle of the line, which is accurate to a few per
 * cent across a country, plenty for a tolerance.
 */
export function simplifyLine(line: readonly LatLon[], toleranceM: number): LatLon[] {
  const copy = (p: LatLon): LatLon => ({ lat: p.lat, lon: p.lon });
  if (line.length <= 2) return line.map(copy);
  const midLat = ((line[0].lat + line[line.length - 1].lat) / 2) * (Math.PI / 180);
  const kx = 111_320 * Math.cos(midLat);
  const ky = 110_540;
  const xs = line.map((p) => p.lon * kx);
  const ys = line.map((p) => p.lat * ky);
  const keep = new Uint8Array(line.length);
  const last = line.length - 1;
  keep[0] = 1;
  keep[last] = 1;
  const limit = toleranceM * toleranceM;
  // An explicit stack instead of recursion: a long recording would overflow the call stack.
  const stack: [number, number][] = [[0, last]];
  while (stack.length > 0) {
    const [a, b] = stack.pop() as [number, number];
    const dx = xs[b] - xs[a];
    const dy = ys[b] - ys[a];
    const length2 = dx * dx + dy * dy;
    let farthest = -1;
    let farthestD = -1;
    for (let i = a + 1; i < b; i += 1) {
      const t = length2 > 0 ? Math.max(0, Math.min(1, ((xs[i] - xs[a]) * dx + (ys[i] - ys[a]) * dy) / length2)) : 0;
      const ex = xs[a] + t * dx - xs[i];
      const ey = ys[a] + t * dy - ys[i];
      const d = ex * ex + ey * ey;
      if (d > farthestD) {
        farthestD = d;
        farthest = i;
      }
    }
    if (farthest >= 0 && farthestD > limit) {
      keep[farthest] = 1;
      stack.push([a, farthest], [farthest, b]);
    }
  }
  return line.filter((_, i) => keep[i] === 1).map(copy);
}

/** The road as the link will carry it: simplified until it fits the budget. */
export function lineForLink(line: readonly LatLon[]): LatLon[] {
  const input = pickEvenly(line, MAX_SIMPLIFY_INPUT);
  let tolerance = LINE_TOLERANCE_M;
  let out = simplifyLine(input, tolerance);
  while (out.length > MAX_LINK_POINTS) {
    tolerance *= 1.6;
    out = simplifyLine(input, tolerance);
  }
  return out;
}

function encodeValue(value: number, out: string[]): void {
  // Zigzag: small negative and positive steps both become small numbers.
  let rest = value < 0 ? ~(value << 1) : value << 1;
  while (rest >= 0x20) {
    out.push(ALPHABET[(rest & 0x1f) | 0x20]);
    rest >>>= 5;
  }
  out.push(ALPHABET[rest]);
}

/**
 * The line as text: each point is its step from the one before, in 5-bit
 * pieces (the scheme of Google's encoded polylines, with an alphabet that
 * needs no escaping in a URL).
 */
export function encodeLine(line: readonly LatLon[]): string {
  const out: string[] = [];
  let lat = 0;
  let lon = 0;
  for (const p of line) {
    const nextLat = Math.round(p.lat * LINE_SCALE);
    const nextLon = Math.round(p.lon * LINE_SCALE);
    encodeValue(nextLat - lat, out);
    encodeValue(nextLon - lon, out);
    lat = nextLat;
    lon = nextLon;
  }
  return out.join('');
}

/** The line back, or null when the text is damaged or holds anything but `count` real points. */
export function decodeLine(text: string, count: number): LatLon[] | null {
  if (!Number.isInteger(count) || count < 2 || count > MAX_LINK_POINTS) return null;
  let index = 0;
  const next = (): number | null => {
    let result = 0;
    let shift = 0;
    for (;;) {
      // Six pieces (30 bits) hold any step between two points on Earth; a seventh is damage.
      if (index >= text.length || shift > 25) return null;
      const piece = ALPHABET_INDEX.get(text[index]);
      index += 1;
      if (piece === undefined) return null;
      result |= (piece & 0x1f) << shift;
      shift += 5;
      if ((piece & 0x20) === 0) break;
    }
    return result & 1 ? ~(result >>> 1) : result >>> 1;
  };
  const out: LatLon[] = [];
  let lat = 0;
  let lon = 0;
  while (index < text.length) {
    const dLat = next();
    const dLon = next();
    if (dLat === null || dLon === null || out.length >= count) return null;
    lat += dLat;
    lon += dLon;
    const point = { lat: lat / LINE_SCALE, lon: lon / LINE_SCALE };
    if (Math.abs(point.lat) > 90 || Math.abs(point.lon) > 180) return null;
    out.push(point);
  }
  // Fewer points than announced: the end of the link was cut off.
  return out.length === count ? out : null;
}

// --- the link --------------------------------------------------------------------

interface BuildInput {
  stops: readonly ShareStop[];
  departure: Departure;
  speed: number;
  /** The road of an imported GPX, or null. */
  line: readonly LatLon[] | null;
}

export function buildRouteLink({ stops, departure, speed, line }: BuildInput): string {
  const parts = [`r=${VERSION}`, `t=${departure.date}T${departure.time}`, `k=${speed}`];
  for (const stop of stops) {
    const decimals = stop.gps ? GPS_DECIMALS : STOP_DECIMALS;
    const name = stop.name.trim().slice(0, MAX_NAME_LENGTH);
    parts.push(`p=${fixed(stop.lat, decimals)}~${fixed(stop.lon, decimals)}~${encodePart(name)}`);
  }
  if (line && line.length >= 2) {
    const road = lineForLink(line);
    // The count goes first: a link cut short then shows as damaged instead of as a shorter road.
    parts.push(`n=${road.length}`, `l=${encodeLine(road)}`);
  }
  return `${SHARE_BASE_URL}${ROUTE_TAB_HASH}?${parts.join('&')}`;
}

function parseCoord(text: string | undefined, max: number): number | null {
  if (text === undefined || !COORD.test(text)) return null;
  const value = Number(text);
  return Math.abs(value) <= max ? value : null;
}

function parseStop(raw: string): LinkStop | null {
  const [latText, lonText, ...nameParts] = raw.split('~');
  const lat = parseCoord(latText, 90);
  const lon = parseCoord(lonText, 180);
  const name = nameParts.join('~').trim().slice(0, MAX_NAME_LENGTH);
  return lat === null || lon === null || !name ? null : { name, lat, lon };
}

function parseDeparture(raw: string | null): Departure | null {
  const match = raw ? DEPARTURE.exec(raw) : null;
  if (!match) return null;
  const [, date, hh, mm] = match;
  const day = new Date(`${date}T00:00:00Z`);
  // "2026-02-31" parses to March: only a date that comes back as itself is real.
  if (Number.isNaN(day.getTime()) || day.toISOString().slice(0, 10) !== date) return null;
  // The planner offers half hours only; anything else would show an empty time box.
  if (Number(hh) > 23 || (mm !== '00' && mm !== '30')) return null;
  return { date, time: `${hh}:${mm}` };
}

/**
 * What the address holds: no route link at all, a route link that cannot be
 * used (damaged, or from a newer app), or the plan. `hash` is
 * window.location.hash.
 */
export function parseRouteLink(hash: string): ParsedRouteLink {
  const query = hash.startsWith(`${ROUTE_TAB_HASH}?`) ? hash.slice(ROUTE_TAB_HASH.length + 1) : null;
  if (query === null) return { kind: 'none' };
  const params = new URLSearchParams(query);
  if (!params.has('r')) return { kind: 'none' };
  if (params.get('r') === GONE_VERSION) return { kind: 'gone' };
  if (params.get('r') !== VERSION) return { kind: 'invalid' };

  const raw = params.getAll('p');
  if (raw.length < MIN_STOPS || raw.length > MAX_STOPS) return { kind: 'invalid' };
  const stops = raw.map(parseStop);
  if (stops.some((s) => s === null)) return { kind: 'invalid' };

  let line: LatLon[] | null = null;
  if (params.has('l')) {
    line = decodeLine(params.get('l') ?? '', Number(params.get('n')));
    if (line === null) return { kind: 'invalid' };
  }

  const speed = Number(params.get('k'));
  return {
    kind: 'route',
    link: {
      stops: stops as LinkStop[],
      departure: parseDeparture(params.get('t')),
      speed: (SPEEDS as readonly number[]).includes(speed) ? (speed as Speed) : null,
      line,
    },
  };
}

/**
 * The road of a link as the planner uses it, with every stop placed at its
 * nearest point along the way. Null when the line has no length.
 */
export function trackFromLink(line: readonly LatLon[], stops: readonly LinkStop[]): RouteLine | null {
  const coords = line.map((p) => ({ lat: p.lat, lon: p.lon }));
  const cumKm = cumulativeKm(coords);
  const total = cumKm[cumKm.length - 1] ?? 0;
  if (coords.length < 2 || !(total > 0)) return null;
  const stopKm = [0];
  for (const stop of stops.slice(1, -1)) {
    // Never behind the stop before it: the planner reads the stops in order along the line.
    stopKm.push(Math.max(stopKm[stopKm.length - 1], nearestOnLine(coords, cumKm, stop).km));
  }
  stopKm.push(total);
  return { coords, cumKm, distanceKm: total, stopKm, roads: [] };
}

/**
 * Whether a shared departure can still be planned here: one of the days the
 * planner offers, and not already gone. `nowLocalIso` is "YYYY-MM-DDTHH:MM".
 */
export function departureUsable(departure: Departure, dates: readonly string[], nowLocalIso: string): boolean {
  return dates.includes(departure.date) && `${departure.date}T${departure.time}` >= nowLocalIso.slice(0, 16);
}
