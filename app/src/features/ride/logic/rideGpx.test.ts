import { describe, expect, it } from 'vitest';
import { NEW_CLOCK } from './clock';
import { buildRideGpx, rideSegments } from './rideGpx';
import { summaryOf } from './summary';
import { EMPTY_TOTALS, type RidePoint } from './track';

function pt(t: number, extra: Partial<RidePoint> = {}): RidePoint {
  return { t, lat: 44.4 + t / 1e7, lon: 26.1, acc: 5, speed: 10, alt: 80.25, seg: 0, ...extra };
}

describe('ride GPX', () => {
  const points = [pt(0), pt(1000), pt(40_000, { gap: true }), pt(41_000), pt(600_000, { seg: 1, alt: null })];

  it('starts a new segment after a gap and after a pause', () => {
    expect(rideSegments(points).map((s) => s.length)).toEqual([2, 2, 1]);
  });

  it('writes every point with its time, and its altitude when there is one', () => {
    const gpx = buildRideGpx('Tură <liberă>', points, '2026-11-22T08:00:00.000Z');
    expect(gpx.match(/<trkseg>/g)).toHaveLength(3);
    expect(gpx.match(/<trkpt /g)).toHaveLength(5);
    expect(gpx).toContain('<ele>80.3</ele><time>1970-01-01T00:00:00.000Z</time>');
    expect(gpx).toContain('<name>Tură &lt;liberă&gt;</name>');
    // The last point has no altitude.
    expect(gpx).toMatch(/<trkpt [^>]+><time>1970-01-01T00:10:00.000Z<\/time><\/trkpt>/);
  });
});

describe('ride summary', () => {
  it('keeps the totals and the ride time of a finished clock', () => {
    const clock = { ...NEW_CLOCK, startedAt: 1_000, pausedMs: 60_000, finishedAt: 3_601_000 };
    const summary = summaryOf({ ...EMPTY_TOTALS, distanceM: 50_000, movingMs: 3_000_000 }, clock);
    expect(summary.elapsedMs).toBe(3_540_000);
    expect(summary.distanceM).toBe(50_000);
  });
});
