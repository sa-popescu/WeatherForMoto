import { escapeXml } from '../../route/gpx';
import type { RidePoint } from './track';

// A recorded ride as GPX 1.1: one track, a new segment after every pause and
// every gap, so a viewer draws no straight line where there was no signal.
// Each point keeps its time, and its GPS altitude when the phone gave one.

function trkpt(p: RidePoint): string {
  const ele = p.alt !== null && Number.isFinite(p.alt) ? `<ele>${p.alt.toFixed(1)}</ele>` : '';
  return `      <trkpt lat="${p.lat.toFixed(6)}" lon="${p.lon.toFixed(6)}">${ele}<time>${new Date(p.t).toISOString()}</time></trkpt>`;
}

/** The ride's points cut into the segments a GPX viewer should draw. */
export function rideSegments(points: readonly RidePoint[]): RidePoint[][] {
  const segments: RidePoint[][] = [];
  let current: RidePoint[] | null = null;
  let prev: RidePoint | null = null;
  for (const p of points) {
    if (!current || !prev || prev.seg !== p.seg || p.gap) {
      current = [];
      segments.push(current);
    }
    current.push(p);
    prev = p;
  }
  return segments;
}

export function buildRideGpx(name: string, points: readonly RidePoint[], createdIso: string): string {
  const title = escapeXml(name);
  const segments = rideSegments(points).map((seg) => ['    <trkseg>', ...seg.map(trkpt), '    </trkseg>'].join('\n'));
  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<gpx version="1.1" creator="Motonor" xmlns="http://www.topografix.com/GPX/1/1">',
    `  <metadata><name>${title}</name><time>${escapeXml(createdIso)}</time></metadata>`,
    `  <trk><name>${title}</name>`,
    ...segments,
    '  </trk>',
    '</gpx>',
    '',
  ].join('\n');
}
