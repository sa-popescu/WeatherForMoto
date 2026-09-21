import { describe, expect, it } from 'vitest';
import { latToY, lonToX } from '../../map/mercator';
import { NOWCAST_STEP_S, type MotionField } from '../../map/nowcast';
import { buildHeadline } from './headline';
import { dbzToMmH, pointGeometry, rainAlongTrail, summarizeRadar, type RadarRain, type RadarStep } from './radarRain';
import { current, day, hours } from './testFixtures';

const BUCHAREST = { lat: 44.43, lon: 26.1 };
/** RainViewer Universal Blue at 35 dBZ (about 5.6 mm/h). */
const YELLOW = [0xff, 0xee, 0x00, 0xff];

/** A radar picture with one rain column `offsetPx` west of the point (negative: east). */
function pictureWithRainWestOf(offsetPx: number) {
  const geometry = pointGeometry(BUCHAREST.lat, BUCHAREST.lon);
  const rgba = new Uint8ClampedArray(geometry.width * geometry.height * 4);
  const px = Math.round(lonToX(BUCHAREST.lon, geometry.z) - geometry.x0) - offsetPx;
  const py = Math.round(latToY(BUCHAREST.lat, geometry.z) - geometry.y0);
  for (let y = py - 5; y <= py + 5; y += 1) {
    for (let x = px - 3; x <= px + 3; x += 1) rgba.set(YELLOW, (y * geometry.width + x) * 4);
  }
  return { geometry, rgba };
}

/** The same shift everywhere, in canvas pixels per radar interval. */
const eastward = (px: number, size: number): MotionField => ({ cols: 1, rows: 1, blockPx: size, dx: Float32Array.of(px), dy: Float32Array.of(0) });

describe('radar rain', () => {
  it('turns reflectivity into a rain rate', () => {
    expect(dbzToMmH(15)).toBeCloseTo(0.32, 2);
    expect(dbzToMmH(35)).toBeCloseTo(5.6, 1);
  });

  it('centres a square canvas on the point', () => {
    const g = pointGeometry(BUCHAREST.lat, BUCHAREST.lon);
    expect(g.width).toBe(g.height);
    expect(g.bounds.south).toBeLessThan(BUCHAREST.lat);
    expect(g.bounds.north).toBeGreaterThan(BUCHAREST.lat);
    expect(g.width).toBeGreaterThan(200);
  });

  it('brings rain moving east over the point when its trail reaches it', () => {
    const { geometry, rgba } = pictureWithRainWestOf(30);
    const steps = rainAlongTrail(rgba, geometry, eastward(10, geometry.width), BUCHAREST.lat, BUCHAREST.lon, 1_000_000);
    const wet = steps.map((s) => s.mmPerHour > 0);
    // 30 px away at 10 px per interval: overhead after three intervals, then past.
    expect(wet.slice(0, 5)).toEqual([false, false, false, true, false]);
    expect(steps[3].mmPerHour).toBeCloseTo(5.6, 1);
  });

  it('counts arrival and stop from the clock', () => {
    const base = 1_000_000;
    const at = (s: number, mm: number): RadarStep => ({ timeSec: base + s * NOWCAST_STEP_S, mmPerHour: mm });
    const dryThenWet = [at(0, 0), at(1, 0), at(2, 0), at(3, 3), at(4, 3)];
    expect(summarizeRadar(dryThenWet, base + 300)).toMatchObject({ arrivesInMin: 25, stopsInMin: null, peakMmPerHour: 3 });
    const wetThenDry = [at(0, 2), at(1, 2), at(2, 0)];
    expect(summarizeRadar(wetThenDry, base)).toMatchObject({ nowMmPerHour: 2, arrivesInMin: null, stopsInMin: 20 });
    expect(summarizeRadar(dryThenWet, base + 3600)).toBeNull();
  });
});

describe('the radar in the headline', () => {
  const DAILY = [day('2026-09-11'), day('2026-09-12')];
  const hourly = hours('2026-09-11T00:00', 48, () => ({ moto_score: 94 }));
  const radar = (over: Partial<RadarRain>): RadarRain => ({
    nowMmPerHour: 0, arrivesInMin: null, stopsInMin: null, peakMmPerHour: 0, reachMin: 90, baseSec: 0, ...over,
  });
  const headline = (r: RadarRain | null) => buildHeadline({ current: current(), hourly, startIndex: 10, daily: DAILY, lang: 'ro', radar: r });

  it('turns a yes into "yes, but" for a shower within the hour', () => {
    const h = headline(radar({ arrivesInMin: 22, peakMmPerHour: 3 }));
    expect(h.title).toBe('Da, dar în ~20 min vine ploaia.');
    expect(h.sub.startsWith('Radarul: ploaie (moderată) peste ~20 min.')).toBe(true);
  });

  it('only notes drizzle or a shower further off', () => {
    expect(headline(radar({ arrivesInMin: 20, peakMmPerHour: 0.3 })).title).toBe('Da. Drum liber.');
    expect(headline(radar({ arrivesInMin: 80, peakMmPerHour: 3 })).title).toBe('Da. Drum liber.');
  });

  it('says when the rain overhead passes', () => {
    expect(headline(radar({ nowMmPerHour: 1, stopsInMin: 38, peakMmPerHour: 1 })).sub.startsWith('Radarul: ploaia trece în ~40 min.')).toBe(true);
  });

  it('leaves the forecast alone without radar or rain', () => {
    expect(headline(null)).toEqual(headline(radar({})));
  });
});
