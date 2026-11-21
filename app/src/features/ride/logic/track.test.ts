import { describe, expect, it } from 'vitest';
import { EMPTY_TRACK, foldPoints, GAP_MS, HEARTBEAT_MS, listGaps, liveSpeedMs, stepTrack, type RideFix, type RidePoint, type TrackState } from './track';

// One degree of latitude is about 111.2 km, so 0.0001° north is about 11.1 m.
const LAT0 = 44.4;
const LON0 = 26.1;
const M_PER_DEG = 111_195;

function fix(t: number, northM: number, over: Partial<RideFix> = {}): RideFix {
  return { t, lat: LAT0 + northM / M_PER_DEG, lon: LON0, acc: 5, speed: null, alt: null, ...over };
}

/** Feeds fixes in order, as the recorder does, and returns the state plus what was kept. */
function ride(fixes: ReadonlyArray<{ fix: RideFix; seg?: number }>): { state: TrackState; kept: RidePoint[] } {
  let state = EMPTY_TRACK;
  const kept: RidePoint[] = [];
  for (const f of fixes) {
    const step = stepTrack(state, f.fix, f.seg ?? 0);
    state = step.state;
    if (step.kept) kept.push(step.kept);
  }
  return { state, kept };
}

/** A steady ride north at `speed` m/s, one fix a second. */
function steady(seconds: number, speed: number, from = 0, startM = 0): Array<{ fix: RideFix }> {
  return Array.from({ length: seconds + 1 }, (_, i) => ({ fix: fix(from + i * 1000, startM + i * speed, { speed }) }));
}

describe('stepTrack', () => {
  it('adds up a steady ride: distance, moving time and the top speed', () => {
    const { state } = ride(steady(60, 20));
    expect(state.totals.distanceM).toBeCloseTo(1200, -1);
    expect(state.totals.movingMs).toBe(60_000);
    expect(state.totals.topSpeedMs).toBeCloseTo(20, 5);
  });

  it('drops fixes that are too imprecise', () => {
    const { state, kept } = ride([{ fix: fix(0, 0) }, { fix: fix(1000, 50, { acc: 80, speed: 50 }) }, { fix: fix(2000, 40, { speed: 20 }) }]);
    expect(kept).toHaveLength(2);
    expect(state.totals.distanceM).toBeCloseTo(40, 0);
  });

  it('adds nothing for jitter while standing, but keeps a heartbeat now and then', () => {
    const fixes = Array.from({ length: 40 }, (_, i) => ({ fix: fix(i * 1000, i % 2 === 0 ? 0 : 4, { speed: 0.2 }) }));
    const { state, kept } = ride(fixes);
    expect(state.totals.distanceM).toBe(0);
    expect(state.totals.movingMs).toBe(0);
    // The first point, then one every HEARTBEAT_MS.
    expect(kept.filter((p) => p.still)).toHaveLength(Math.floor(39_000 / HEARTBEAT_MS));
  });

  it('does not count a short stop as riding when the ride goes on', () => {
    const go = steady(10, 20);
    // 12 s at a light: speed 0, no heartbeat yet, so nothing is kept.
    const light = Array.from({ length: 12 }, (_, i) => ({ fix: fix(11_000 + i * 1000, 200, { speed: 0 }) }));
    const again = steady(5, 20, 23_000, 200).slice(1);
    const { state } = ride([...go, ...light, ...again]);
    // 10 s before the light, then 5 s after it; the first second after it is the stretch at 20 m/s.
    expect(state.totals.movingMs).toBeCloseTo(15_000, -2);
    expect(state.totals.distanceM).toBeCloseTo(300, -1);
  });

  it('drops a position thrown far aside, and goes on from the good ones', () => {
    const fixes = steady(10, 20).map((f, i) => (i === 5 ? { fix: { ...f.fix, lat: f.fix.lat + 150 / M_PER_DEG } } : f));
    const { state, kept } = ride(fixes);
    expect(kept).toHaveLength(10);
    expect(state.totals.distanceM).toBeCloseTo(200, -1);
  });

  it('treats a move inside the accuracy as jitter when the GPS gives no speed', () => {
    const { state } = ride([{ fix: fix(0, 0, { acc: 15 }) }, { fix: fix(5000, 10, { acc: 15 }) }]);
    expect(state.totals.distanceM).toBe(0);
  });

  it('ignores one speed spike for the top speed', () => {
    const fixes = steady(10, 15).map((f, i) => (i === 5 ? { fix: { ...f.fix, speed: 80 } } : f));
    const { state } = ride(fixes);
    expect(state.totals.topSpeedMs).toBeCloseTo(15, 5);
  });

  it('counts a gap as a straight line, and as riding when the line is fast enough', () => {
    const before = steady(10, 20);
    // 2 minutes without a position, 2.4 km further on: 20 m/s.
    const after = steady(10, 20, 10_000 + 120_000, 200 + 2400);
    const { state } = ride([...before, ...after]);
    expect(state.totals.gapCount).toBe(1);
    expect(state.totals.gapMs).toBe(120_000);
    expect(state.totals.distanceM).toBeCloseTo(200 + 2400 + 200, -1);
    expect(state.totals.movingMs).toBe(10_000 + 120_000 + 10_000);
  });

  it('does not count a gap spent standing as riding', () => {
    const { state } = ride([{ fix: fix(0, 0) }, { fix: fix(GAP_MS + 60_000, 20) }]);
    expect(state.totals.gapCount).toBe(1);
    expect(state.totals.movingMs).toBe(0);
  });

  it('counts nothing between two segments (a pause)', () => {
    const first = steady(10, 20);
    const second = steady(10, 20, 600_000, 5000).map((f) => ({ ...f, seg: 1 }));
    const { state } = ride([...first, ...second]);
    expect(state.totals.distanceM).toBeCloseTo(400, -1);
    expect(state.totals.movingMs).toBe(20_000);
    expect(state.totals.gapCount).toBe(0);
  });
});

describe('gaps along the road', () => {
  const before = steady(10, 20);
  const after = steady(10, 20, 10_000 + 120_000, 200 + 2400);

  it('marks the point that closes a gap, and lists the gap from the last moving point', () => {
    const { kept } = ride([...before, ...after]);
    const closing = kept.filter((p) => p.gap);
    expect(closing).toHaveLength(1);
    const gaps = listGaps(kept);
    expect(gaps).toHaveLength(1);
    expect(gaps[0].t).toBe(closing[0].t);
    expect(gaps[0].straightM).toBeCloseTo(2400, -1);
    expect(gaps[0].from.lat).toBeCloseTo(kept[kept.indexOf(closing[0]) - 1].lat, 9);
  });

  it('counts the road distance across a gap once it is known', () => {
    const { state, kept } = ride([...before, ...after]);
    const gapT = kept.find((p) => p.gap)!.t;
    const withRoad = foldPoints(kept, new Map([[gapT, 3100]]));
    expect(withRoad.totals.distanceM - state.totals.distanceM).toBeCloseTo(3100 - 2400, -1);
    expect(withRoad.totals.movingDistanceM - state.totals.movingDistanceM).toBeCloseTo(3100 - 2400, -1);
  });
});

describe('foldPoints', () => {
  it('rebuilds exactly the same totals from the kept points', () => {
    const fixes = [
      ...steady(30, 18),
      // Standing at a light, with jitter.
      ...Array.from({ length: 40 }, (_, i) => ({ fix: fix(31_000 + i * 1000, 540 + (i % 3), { speed: 0.3 }) })),
      ...steady(30, 22, 80_000, 545),
      ...steady(20, 25, 400_000, 2000).map((f) => ({ ...f, seg: 1 })),
    ];
    const { state, kept } = ride(fixes);
    expect(foldPoints(kept).totals).toEqual(state.totals);
  });
});

describe('liveSpeedMs', () => {
  it('prefers the GPS speed and falls back on the distance between fixes', () => {
    expect(liveSpeedMs(null, fix(0, 0, { speed: 12 }))).toBe(12);
    expect(liveSpeedMs(fix(0, 0), fix(2000, 30))).toBeCloseTo(15, 1);
    expect(liveSpeedMs(null, fix(0, 0))).toBeNull();
    expect(liveSpeedMs(fix(0, 0), fix(1000, 10, { speed: -1 }))).toBeCloseTo(10, 1);
  });
});
