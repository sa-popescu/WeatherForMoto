import { distanceKm } from '../../../lib/geo';

// Turns the raw GPS fixes of a ride into the points worth keeping and the
// running totals. Every decision depends only on the points already kept, never
// on the fixes that were dropped, so folding the stored points again after a
// reload gives exactly the same totals (see foldPoints).

/** One position as the GPS gave it. */
export interface RideFix {
  /** Milliseconds since the epoch. */
  t: number;
  lat: number;
  lon: number;
  /** Horizontal accuracy radius, in metres. */
  acc: number;
  /** Ground speed in m/s measured by the GPS itself, when the device gives one. */
  speed: number | null;
  /** GPS altitude in metres. Noisy, so it is only carried along for the export. */
  alt: number | null;
  /**
   * Course over ground in degrees clockwise from north, as the GPS measured
   * it; null while standing or when the device gives none. Rides recorded
   * before it was kept have none (see lean.ts).
   */
  head?: number | null;
}

/** A fix that was kept. */
export interface RidePoint extends RideFix {
  /** Grows by one after every pause, so the line and the totals break there. */
  seg: number;
  /** A heartbeat kept while standing: it moves the time on, not the distance. */
  still?: boolean;
  /** The first point after a stretch with no position (see GAP_MS). */
  gap?: boolean;
}

/** Road distance across a gap, by the time of the point that closed it (see gapFill.ts). */
export type GapRoads = ReadonlyMap<number, number>;

export interface RideTotals {
  /** Everything ridden, across GPS gaps too (along the road when known, else in a straight line). */
  distanceM: number;
  /** The part of distanceM covered while actually moving. */
  movingDistanceM: number;
  movingMs: number;
  /** Highest speed, as the median of three readings so one GPS spike does not count. */
  topSpeedMs: number;
  /** Stretches with no position for longer than GAP_MS (signal lost, app in the background). */
  gapCount: number;
  gapMs: number;
}

export interface TrackState {
  totals: RideTotals;
  /** Last kept point: time is measured from here. */
  last: RidePoint | null;
  /** Last point that moved: distance is measured from here, so standing jitter adds nothing. */
  anchor: RidePoint | null;
  /** The latest speed readings that may feed the top speed. */
  recentSpeeds: number[];
}

/** Fixes less precise than this are not kept at all. */
export const MAX_ACCURACY_M = 50;
/** Only fixes at least this precise may set the top speed. */
const TOP_SPEED_ACCURACY_M = 25;
/** A reported speed under this (3.6 km/h) means standing. */
const STILL_SPEED_MS = 1;
/** Slower than this (about 5 km/h) between two points counts as standing, not riding. */
const MOVING_SPEED_MS = 1.5;
/** A moving fix closer than this to the last one waits for the next. */
const MIN_STEP_M = 3;
/** Without a reported speed, a move inside the fixes' own accuracy is treated as jitter. */
const JITTER_MIN_M = 5;
const JITTER_MAX_M = 25;
/** While standing, one point this often keeps the time line going (well under GAP_MS). */
export const HEARTBEAT_MS = 5_000;
/**
 * No point kept for longer than this, while recording, is a gap: the signal
 * was lost or the phone hid the app. At 50 km/h that is about 140 m, enough
 * to cut a corner, so a gap is filled along the road afterwards.
 */
export const GAP_MS = 10_000;
/**
 * An interval whose average speed is at least this share of the speed at its
 * end was ridden all along. Slower, it held a stop (a light, a junction), and
 * only the stretch at the end speed counts as riding.
 */
const RIDDEN_THROUGHOUT_SHARE = 0.5;
/** Anything faster (360 km/h) is a GPS error. */
const MAX_PLAUSIBLE_SPEED_MS = 100;
/** Moving faster than twice the GPS speed plus this (54 km/h) between two fixes is a jump. */
const JUMP_SPEED_FACTOR = 2;
const JUMP_SPEED_SLACK_MS = 15;
const TOP_SPEED_WINDOW = 3;

export const EMPTY_TOTALS: RideTotals = {
  distanceM: 0,
  movingDistanceM: 0,
  movingMs: 0,
  topSpeedMs: 0,
  gapCount: 0,
  gapMs: 0,
};

export const EMPTY_TRACK: TrackState = { totals: EMPTY_TOTALS, last: null, anchor: null, recentSpeeds: [] };

function metresBetween(a: { lat: number; lon: number }, b: { lat: number; lon: number }): number {
  return distanceKm(a, b) * 1000;
}

function median(values: readonly number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor(sorted.length / 2)];
}

/** A usable reported speed, or null (browsers give null, some devices a negative value). */
export function reportedSpeed(fix: Pick<RideFix, 'speed'>): number | null {
  return fix.speed != null && Number.isFinite(fix.speed) && fix.speed >= 0 ? fix.speed : null;
}

/** Speed to show right now: the GPS's own reading, or the distance from the previous fix. */
export function liveSpeedMs(prev: RideFix | null, fix: RideFix): number | null {
  const reported = reportedSpeed(fix);
  if (reported !== null) return reported;
  if (!prev) return null;
  const dt = (fix.t - prev.t) / 1000;
  return dt > 0 ? metresBetween(prev, fix) / dt : null;
}

/**
 * How much of an interval between two kept points was spent riding. Standing
 * fixes are not kept, so the first move after a stop closes an interval that
 * holds the whole stop: the GPS speed at its end tells the two apart.
 */
function ridingMs(d: number, dt: number, reported: number | null): number {
  const intervalSpeed = d / (dt / 1000);
  if (reported === null || reported === 0) return intervalSpeed >= MOVING_SPEED_MS ? dt : 0;
  if (reported < MOVING_SPEED_MS) return 0;
  if (intervalSpeed >= reported * RIDDEN_THROUGHOUT_SHARE) return dt;
  return Math.min(dt, (d / reported) * 1000);
}

/**
 * A position that moved much faster than the GPS's own speed says (a
 * reflection off buildings can throw it 100 m aside) is dropped. If the
 * rider really is there, the next fixes agree, or the gap logic takes over.
 */
function isJump(intervalSpeed: number, reported: number | null): boolean {
  const ceiling = reported === null ? MAX_PLAUSIBLE_SPEED_MS : Math.min(MAX_PLAUSIBLE_SPEED_MS, reported * JUMP_SPEED_FACTOR + JUMP_SPEED_SLACK_MS);
  return intervalSpeed > ceiling;
}

function jitterRadius(a: RideFix, b: RideFix): number {
  return Math.min(JITTER_MAX_M, Math.max(JITTER_MIN_M, (a.acc + b.acc) / 2));
}

function topSpeed(state: TrackState, fix: RideFix, intervalSpeed: number): Pick<TrackState, 'recentSpeeds'> & { top: number } {
  const speed = reportedSpeed(fix) ?? intervalSpeed;
  if (fix.acc > TOP_SPEED_ACCURACY_M || speed > MAX_PLAUSIBLE_SPEED_MS) return { recentSpeeds: state.recentSpeeds, top: state.totals.topSpeedMs };
  const recentSpeeds = [...state.recentSpeeds, speed].slice(-TOP_SPEED_WINDOW);
  const top = recentSpeeds.length === TOP_SPEED_WINDOW ? Math.max(state.totals.topSpeedMs, median(recentSpeeds)) : state.totals.topSpeedMs;
  return { recentSpeeds, top };
}

export interface StepResult {
  state: TrackState;
  /** The point to keep, or null when the fix adds nothing. */
  kept: RidePoint | null;
}

/** First point of the ride or of a segment after a pause: no distance, no time. */
function startSegment(state: TrackState, point: RidePoint): StepResult {
  return { state: { ...state, last: point, anchor: point, recentSpeeds: [] }, kept: point };
}

/**
 * No position for a while: count the road distance across it when known, the
 * straight line until then, and the time as riding if that distance says so.
 */
function crossGap(state: TrackState, point: RidePoint, last: RidePoint, anchor: RidePoint, roadM: number | undefined): StepResult {
  const dt = point.t - last.t;
  const d = roadM ?? metresBetween(anchor, point);
  const moved = d / (dt / 1000) >= MOVING_SPEED_MS;
  const t = state.totals;
  const totals: RideTotals = {
    ...t,
    distanceM: t.distanceM + d,
    movingDistanceM: t.movingDistanceM + (moved ? d : 0),
    movingMs: t.movingMs + (moved ? dt : 0),
    gapCount: t.gapCount + 1,
    gapMs: t.gapMs + dt,
  };
  const kept: RidePoint = { ...point, gap: true };
  return { state: { totals, last: kept, anchor: kept, recentSpeeds: [] }, kept };
}

/**
 * Feeds one fix to the track. `seg` is the recorder's current segment. Returns
 * the new state and the point to keep, if any. `roads` holds the road distance
 * of the gaps already filled.
 */
export function stepTrack(state: TrackState, fix: RideFix, seg: number, roads?: GapRoads): StepResult {
  if (fix.acc > MAX_ACCURACY_M) return { state, kept: null };
  const { last, anchor } = state;
  const point: RidePoint = { t: fix.t, lat: fix.lat, lon: fix.lon, acc: fix.acc, speed: fix.speed, alt: fix.alt, head: fix.head ?? null, seg };
  if (!last || !anchor || last.seg !== seg) return startSegment(state, point);

  const dt = fix.t - last.t;
  if (dt <= 0) return { state, kept: null };
  if (dt > GAP_MS) return crossGap(state, point, last, anchor, roads?.get(fix.t));

  const d = metresBetween(anchor, fix);
  const reported = reportedSpeed(fix);
  const standing = reported !== null ? reported < STILL_SPEED_MS : d < jitterRadius(anchor, fix);
  if (standing) {
    if (dt < HEARTBEAT_MS) return { state, kept: null };
    const still: RidePoint = { ...point, still: true };
    return { state: { ...state, last: still }, kept: still };
  }
  if (d < MIN_STEP_M) return { state, kept: null };
  if (isJump(d / (dt / 1000), reported)) return { state, kept: null };

  const riding = ridingMs(d, dt, reported);
  const { recentSpeeds, top } = topSpeed(state, fix, d / (dt / 1000));
  const t = state.totals;
  const totals: RideTotals = {
    ...t,
    distanceM: t.distanceM + d,
    movingDistanceM: t.movingDistanceM + (riding > 0 ? d : 0),
    movingMs: t.movingMs + riding,
    topSpeedMs: top,
  };
  return { state: { totals, last: point, anchor: point, recentSpeeds }, kept: point };
}

/**
 * Rebuilds the track from the stored points: for a ride picked up after a
 * reload, and whenever a gap gets its road distance.
 */
export function foldPoints(points: readonly RidePoint[], roads?: GapRoads): TrackState {
  let state = EMPTY_TRACK;
  for (const p of points) state = stepTrack(state, p, p.seg, roads).state;
  return state;
}

/** A gap in the track: from the last point that moved before it to the point that closed it. */
export interface TrackGap {
  /** Time of the point that closed the gap (the key of GapRoads). */
  t: number;
  from: { lat: number; lon: number };
  to: { lat: number; lon: number };
  straightM: number;
}

/** The gaps of a stored track, for filling the ones a reload left without a road. */
export function listGaps(points: readonly RidePoint[]): TrackGap[] {
  const gaps: TrackGap[] = [];
  let state = EMPTY_TRACK;
  for (const p of points) {
    const anchor = state.anchor;
    const step = stepTrack(state, p, p.seg);
    state = step.state;
    if (step.kept?.gap && anchor) gaps.push(gapOf(anchor, step.kept));
  }
  return gaps;
}

export function gapOf(anchor: RidePoint, point: RidePoint): TrackGap {
  return {
    t: point.t,
    from: { lat: anchor.lat, lon: anchor.lon },
    to: { lat: point.lat, lon: point.lon },
    straightM: metresBetween(anchor, point),
  };
}
