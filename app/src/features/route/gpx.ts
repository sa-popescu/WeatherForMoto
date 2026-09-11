import type { LatLon } from './types';

// GPX 1.1 export: the full route geometry as one track, stops as waypoints.

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
    '<gpx version="1.1" creator="MotoMeteo" xmlns="http://www.topografix.com/GPX/1/1">',
    `  <metadata><name>${title}</name><time>${escapeXml(createdIso)}</time></metadata>`,
    ...wpts,
    `  <trk><name>${title}</name><trkseg>`,
    ...trkpts,
    '  </trkseg></trk>',
    '</gpx>',
    '',
  ].join('\n');
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
