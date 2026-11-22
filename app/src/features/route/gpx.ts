import type { LatLon } from './types';

// GPX 1.1 export (the full route geometry as one track, stops as waypoints)
// and GPX import (the points of a file, see parseGpx below).

export function escapeXml(text: string): string {
  return text
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&apos;');
}

export interface GpxInput {
  name: string;
  line: readonly LatLon[];
  waypoints: ReadonlyArray<LatLon & { name: string }>;
  /** ISO timestamp for the metadata, e.g. new Date().toISOString(). */
  createdIso: string;
}

function coord(p: LatLon): string {
  return `lat="${p.lat.toFixed(6)}" lon="${p.lon.toFixed(6)}"`;
}

export function buildGpx({ name, line, waypoints, createdIso }: GpxInput): string {
  const title = escapeXml(name);
  // The schema wants waypoints before the track.
  const wpts = waypoints.map((w) => `  <wpt ${coord(w)}><name>${escapeXml(w.name)}</name></wpt>`);
  const trkpts = line.map((p) => `      <trkpt ${coord(p)}/>`);
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<gpx version="1.1" creator="Motonor" xmlns="http://www.topografix.com/GPX/1/1">',
    `  <metadata><name>${title}</name><time>${escapeXml(createdIso)}</time></metadata>`,
    ...wpts,
    `  <trk><name>${title}</name><trkseg>`,
    ...trkpts,
    '  </trkseg></trk>',
    '</gpx>',
    '',
  ].join('\n');
}

// GPX import. A small scanner instead of DOMParser: it behaves the same in
// the browser and in the tests (node), and nothing from the file is ever
// turned into a document. Only coordinates and point names are read.

export const MAX_GPX_BYTES = 15 * 1024 * 1024;

export type GpxErrorKind = 'empty' | 'too-large';

export class GpxError extends Error {
  readonly kind: GpxErrorKind;

  constructor(kind: GpxErrorKind) {
    super(kind);
    this.name = 'GpxError';
    this.kind = kind;
  }
}

/** A point of the file; `name` is empty when the file gives none. */
export interface GpxPoint extends LatLon {
  name: string;
}

export interface ParsedGpx {
  /** Every <trkpt>, all tracks and segments joined in file order. */
  track: LatLon[];
  /** Every <rtept>: a planned route, dense or just a few via points. */
  routePoints: GpxPoint[];
  /** Every <wpt>: places of interest, usually the stops. */
  waypoints: GpxPoint[];
}

const MAX_NAME_LENGTH = 80;
// The body is bounded so a point that is never closed cannot make the scan crawl.
const POINT_RE = /<(?:\w+:)?(trkpt|rtept|wpt)\b([^>]*?)(?:\/>|>([\s\S]{0,20000}?)<\/(?:\w+:)?\1\s*>)/g;
const NAME_RE = /<(?:\w+:)?name\b[^>]*>([\s\S]*?)<\/(?:\w+:)?name\s*>/;
const CDATA_RE = /^<!\[CDATA\[([\s\S]*?)\]\]>$/;
const LAT_RE = /\blat\s*=\s*["']\s*([-+0-9.eE]+)\s*["']/;
const LON_RE = /\blon\s*=\s*["']\s*([-+0-9.eE]+)\s*["']/;
const ENTITIES: Record<string, string> = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'" };

export function unescapeXml(text: string): string {
  return text.replace(/&(#x[0-9a-f]+|#\d+|[a-z]+);/gi, (whole, body: string) => {
    if (body[0] !== '#') return ENTITIES[body.toLowerCase()] ?? whole;
    const code = body[1].toLowerCase() === 'x' ? Number.parseInt(body.slice(2), 16) : Number.parseInt(body.slice(1), 10);
    return code > 0 && code <= 0x10ffff ? String.fromCodePoint(code) : whole;
  });
}

function pointName(body: string | undefined): string {
  const raw = body ? NAME_RE.exec(body)?.[1].trim() : undefined;
  if (!raw) return '';
  const cdata = CDATA_RE.exec(raw);
  return (cdata ? cdata[1] : unescapeXml(raw)).replace(/\s+/g, ' ').trim().slice(0, MAX_NAME_LENGTH);
}

function coordinate(attrs: string, pattern: RegExp, limit: number): number | null {
  const match = pattern.exec(attrs);
  const value = match ? Number(match[1]) : Number.NaN;
  return Number.isFinite(value) && Math.abs(value) <= limit ? value : null;
}

/** Reads the points of a GPX 1.0 or 1.1 file. Points without valid coordinates are skipped. */
export function parseGpx(xml: string): ParsedGpx {
  const out: ParsedGpx = { track: [], routePoints: [], waypoints: [] };
  const text = xml.replace(/<!--[\s\S]*?-->/g, '');
  for (const match of text.matchAll(POINT_RE)) {
    const [, tag, attrs, body] = match;
    const lat = coordinate(attrs, LAT_RE, 90);
    const lon = coordinate(attrs, LON_RE, 180);
    if (lat === null || lon === null) continue;
    if (tag === 'trkpt') out.track.push({ lat, lon });
    else if (tag === 'rtept') out.routePoints.push({ lat, lon, name: pointName(body) });
    else out.waypoints.push({ lat, lon, name: pointName(body) });
  }
  return out;
}

/** "București → Sibiu" -> "bucuresti-sibiu.gpx" */
export function gpxFileName(name: string): string {
  const slug = name
    .normalize('NFKD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, '-')
    .replace(/^-+|-+$/g, '')
    .slice(0, 60);
  return `${slug || 'traseu'}.gpx`;
}
