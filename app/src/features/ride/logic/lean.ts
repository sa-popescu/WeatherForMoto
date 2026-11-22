import { reportedSpeed, type RidePoint } from './track';

// The lean angle in turns, estimated from the GPS alone. In a steady turn the
// bike and rider lean until gravity and the pull of the turn add up along
// them, so tan(lean) = speed × rate of turn / g. The GPS gives both: the speed
// and the course, which it measures from the Doppler shift of the satellites,
// so they are much steadier than anything worked out from the positions.
//
// No motion sensor is used: iPhone asks for that permission again every time
// the app starts, and the gyroscope would also need to know how the phone is
// mounted. The price is one reading a second, so a short peak in a fast
// chicane is smoothed away a little.
//
// What comes out is the lean of bike and rider together. With wide tyres the
// bike itself leans a few degrees more, and hanging off the side, a bit less.
//
// Like the rest of the track, it depends only on the points that were kept,
// so folding them again after a reload gives the same maximum.

/** Standard gravity, m/s². */
const G = 9.80665;
/** Under 15 km/h the course wanders and the lean says little. */
export const MIN_LEAN_SPEED_MS = 15 / 3.6;
/** Two points further apart than this in time are not one reading. */
const MAX_READING_DT_MS = 3_000;
/** Only points at least this precise give a reading. */
const LEAN_ACCURACY_M = 25;
/** Turning faster than this above 15 km/h is the course jumping, not the bike. */
const MAX_TURN_RATE_DEG_S = 60;
/** Steeper than this on the road is a bad reading. */
const MAX_PLAUSIBLE_LEAN_DEG = 60;
/** The lean counts as the middle of three readings in a row, so one bad course does not. */
const LEAN_WINDOW = 3;

export interface LeanState {
  /** The latest readings in a row, in degrees: right turns positive, left negative. */
  recent: readonly number[];
  /** Lean right now (the middle of the last three readings), or null without three in a row. */
  nowDeg: number | null;
  maxLeftDeg: number;
  maxRightDeg: number;
}

export const NO_LEAN: LeanState = { recent: [], nowDeg: null, maxLeftDeg: 0, maxRightDeg: 0 };

/** A usable course over ground, in degrees clockwise from north, or null. */
function courseOf(point: Pick<RidePoint, 'head'>): number | null {
  const { head } = point;
  return head != null && Number.isFinite(head) && head >= 0 && head <= 360 ? head : null;
}

/** The change of course from `from` to `to`, in degrees from -180 to 180: right is positive. */
export function turnDeg(from: number, to: number): number {
  return ((((to - from + 180) % 360) + 360) % 360) - 180;
}

/** The lean between two kept points, in degrees (right positive), or null when they cannot tell. */
export function leanBetween(prev: RidePoint, point: RidePoint): number | null {
  if (prev.still || point.still || prev.seg !== point.seg) return null;
  const dt = point.t - prev.t;
  if (dt <= 0 || dt > MAX_READING_DT_MS) return null;
  if (prev.acc > LEAN_ACCURACY_M || point.acc > LEAN_ACCURACY_M) return null;
  const v1 = reportedSpeed(prev);
  const v2 = reportedSpeed(point);
  if (v1 === null || v2 === null || v1 < MIN_LEAN_SPEED_MS || v2 < MIN_LEAN_SPEED_MS) return null;
  const c1 = courseOf(prev);
  const c2 = courseOf(point);
  if (c1 === null || c2 === null) return null;
  const rateDegS = turnDeg(c1, c2) / (dt / 1000);
  if (Math.abs(rateDegS) > MAX_TURN_RATE_DEG_S) return null;
  const lateral = ((v1 + v2) / 2) * ((rateDegS * Math.PI) / 180);
  const lean = (Math.atan(lateral / G) * 180) / Math.PI;
  return Math.abs(lean) > MAX_PLAUSIBLE_LEAN_DEG ? null : lean;
}

/** Feeds one kept point (and the one kept before it) to the lean. */
export function stepLean(state: LeanState, prev: RidePoint | null, point: RidePoint): LeanState {
  const reading = prev ? leanBetween(prev, point) : null;
  if (reading === null) return { ...state, recent: [], nowDeg: null };
  const recent = [...state.recent, reading].slice(-LEAN_WINDOW);
  if (recent.length < LEAN_WINDOW) return { ...state, recent, nowDeg: null };
  const middle = [...recent].sort((a, b) => a - b)[Math.floor(LEAN_WINDOW / 2)];
  return {
    recent,
    nowDeg: middle,
    maxLeftDeg: Math.max(state.maxLeftDeg, -middle),
    maxRightDeg: Math.max(state.maxRightDeg, middle),
  };
}

/** The lean of a stored ride, from its kept points in time order. */
export function foldLean(points: readonly RidePoint[]): LeanState {
  let state = NO_LEAN;
  let prev: RidePoint | null = null;
  for (const point of points) {
    state = stepLean(state, prev, point);
    prev = point;
  }
  return state;
}

/** Whether the ride has any lean worth showing (the GPS gave a course at speed). */
export function hasLean(state: Pick<LeanState, 'maxLeftDeg' | 'maxRightDeg'>): boolean {
  return state.maxLeftDeg > 0 || state.maxRightDeg > 0;
}
