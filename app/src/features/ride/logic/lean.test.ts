import { describe, expect, it } from 'vitest';
import { foldLean, hasLean, leanBetween, NO_LEAN, stepLean, turnDeg } from './lean';
import { EMPTY_TRACK, stepTrack, type RidePoint } from './track';

const G = 9.80665;
const DEG = Math.PI / 180;

function pt(t: number, head: number | null, speed: number | null, over: Partial<RidePoint> = {}): RidePoint {
  return { t, lat: 44.4, lon: 26.1, acc: 5, speed, alt: null, head, seg: 0, ...over };
}

/** One point a second, turning at `rateDegS` (right positive) at a steady speed. */
function turning(seconds: number, speed: number, rateDegS: number, startHead = 0): RidePoint[] {
  return Array.from({ length: seconds + 1 }, (_, i) => pt(i * 1000, (((startHead + i * rateDegS) % 360) + 360) % 360, speed));
}

/** The lean of a steady turn: tan(lean) = v × rate / g. */
function expectedLean(speed: number, rateDegS: number): number {
  return Math.atan((speed * rateDegS * DEG) / G) / DEG;
}

describe('turnDeg', () => {
  it('measures the turn the short way round, right positive', () => {
    expect(turnDeg(10, 30)).toBe(20);
    expect(turnDeg(30, 10)).toBe(-20);
    expect(turnDeg(350, 10)).toBe(20);
    expect(turnDeg(10, 350)).toBe(-20);
  });
});

describe('leanBetween', () => {
  it('gives the lean of a steady turn from the speed and the change of course', () => {
    // 54 km/h round a 50 m radius: 17.2 °/s, about 24.6°.
    const rate = (15 / 50) / DEG;
    const lean = leanBetween(pt(0, 90, 15), pt(1000, 90 + rate, 15));
    expect(lean).toBeCloseTo(Math.atan((15 * 15) / (50 * G)) / DEG, 3);
    expect(lean).toBeCloseTo(24.6, 1);
  });

  it('cannot tell when slow, standing, without a course, imprecise or too far apart', () => {
    expect(leanBetween(pt(0, 90, 3), pt(1000, 100, 3))).toBeNull();
    expect(leanBetween(pt(0, 90, 15, { still: true }), pt(1000, 100, 15))).toBeNull();
    expect(leanBetween(pt(0, null, 15), pt(1000, 100, 15))).toBeNull();
    expect(leanBetween(pt(0, 90, null), pt(1000, 100, null))).toBeNull();
    expect(leanBetween(pt(0, 90, 15, { acc: 40 }), pt(1000, 100, 15))).toBeNull();
    expect(leanBetween(pt(0, 90, 15), pt(5000, 100, 15))).toBeNull();
    expect(leanBetween(pt(0, 90, 15), pt(1000, 100, 15, { seg: 1 }))).toBeNull();
  });

  it('drops a course that jumps faster than a bike turns', () => {
    expect(leanBetween(pt(0, 0, 10), pt(1000, 90, 10))).toBeNull();
  });
});

describe('stepLean and foldLean', () => {
  it('keeps the most on each side', () => {
    const right = turning(10, 15, 15);
    const left = turning(10, 15, -12, right[right.length - 1].head!).map((p) => ({ ...p, t: p.t + 11_000 }));
    const lean = foldLean([...right, ...left]);
    expect(lean.maxRightDeg).toBeCloseTo(expectedLean(15, 15), 3);
    expect(lean.maxLeftDeg).toBeCloseTo(expectedLean(15, 12), 3);
    expect(lean.nowDeg).toBeCloseTo(-expectedLean(15, 12), 3);
  });

  it('works across north', () => {
    const lean = foldLean(turning(6, 20, 10, 340));
    expect(lean.maxRightDeg).toBeCloseTo(expectedLean(20, 10), 3);
    expect(lean.maxLeftDeg).toBe(0);
  });

  it('needs three readings in a row before it shows a lean', () => {
    const points = turning(3, 15, 15);
    let state = NO_LEAN;
    const now: Array<number | null> = [];
    points.forEach((p, i) => {
      state = stepLean(state, i > 0 ? points[i - 1] : null, p);
      now.push(state.nowDeg);
    });
    expect(now.slice(0, 3)).toEqual([null, null, null]);
    expect(now[3]).toBeCloseTo(expectedLean(15, 15), 3);
  });

  it('ignores one bad course on a straight road', () => {
    const points = Array.from({ length: 10 }, (_, i) => pt(i * 1000, i === 5 ? 120 : 90, 20));
    const lean = foldLean(points);
    expect(lean.maxLeftDeg).toBe(0);
    expect(lean.maxRightDeg).toBe(0);
  });

  it('starts over after a stop', () => {
    const before = turning(4, 15, 15);
    const stop = pt(5000, null, 0, { still: true });
    const lean = foldLean([...before, stop]);
    expect(lean.nowDeg).toBeNull();
    expect(lean.recent).toEqual([]);
    expect(lean.maxRightDeg).toBeCloseTo(expectedLean(15, 15), 3);
  });

  it('has nothing to show for a ride recorded without a course', () => {
    const old = turning(10, 15, 15).map(({ head: _head, ...p }) => p as RidePoint);
    expect(hasLean(foldLean(old))).toBe(false);
  });
});

describe('the course on the track', () => {
  it('is kept with each point, and folding the points gives the same lean as riding', () => {
    let track = EMPTY_TRACK;
    let lean = NO_LEAN;
    const kept: RidePoint[] = [];
    // North at 15 m/s, turning right at 15 °/s; the positions only need to move.
    for (let i = 0; i <= 12; i++) {
      const step = stepTrack(track, { t: i * 1000, lat: 44.4 + (i * 15) / 111_195, lon: 26.1, acc: 5, speed: 15, alt: null, head: (i * 15) % 360 }, 0);
      track = step.state;
      if (!step.kept) continue;
      lean = stepLean(lean, kept[kept.length - 1] ?? null, step.kept);
      kept.push(step.kept);
    }
    expect(kept[3].head).toBe(45);
    expect(foldLean(kept)).toEqual(lean);
    expect(lean.maxRightDeg).toBeCloseTo(expectedLean(15, 15), 3);
  });

  it('is null on a point whose fix had none', () => {
    const step = stepTrack(EMPTY_TRACK, { t: 0, lat: 44.4, lon: 26.1, acc: 5, speed: null, alt: null }, 0);
    expect(step.kept?.head).toBeNull();
  });
});
