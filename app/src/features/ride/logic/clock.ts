// The ride's clock: when it started, how long it was paused, when it ended.
// Ride time is wall time from the first GPS point to the end, minus the pauses,
// so a ride that waited for the GPS does not count the wait.

export interface RideClock {
  /** Time of the first kept point; null until the GPS has given one. */
  startedAt: number | null;
  /** Pauses that have ended, in total. */
  pausedMs: number;
  /** Start of the pause in progress, if any. */
  pausedAt: number | null;
  finishedAt: number | null;
}

export const NEW_CLOCK: RideClock = { startedAt: null, pausedMs: 0, pausedAt: null, finishedAt: null };

/** Averages over less riding than this jump around too much to show. */
const MIN_AVERAGE_MS = 30_000;

/** Ride time up to `now` (or the end), without the pauses. */
export function elapsedMs(clock: RideClock, now: number): number {
  if (clock.startedAt === null) return 0;
  const end = clock.finishedAt ?? now;
  const openPause = clock.pausedAt !== null && clock.finishedAt === null ? Math.max(0, end - clock.pausedAt) : 0;
  return Math.max(0, end - clock.startedAt - clock.pausedMs - openPause);
}

export function pauseClock(clock: RideClock, at: number): RideClock {
  return clock.pausedAt !== null ? clock : { ...clock, pausedAt: at };
}

export function resumeClock(clock: RideClock, at: number): RideClock {
  if (clock.pausedAt === null) return clock;
  // A pause before the first point does not count: the ride had not started.
  const pause = clock.startedAt === null ? 0 : Math.max(0, at - Math.max(clock.pausedAt, clock.startedAt));
  return { ...clock, pausedMs: clock.pausedMs + pause, pausedAt: null };
}

export function finishClock(clock: RideClock, at: number): RideClock {
  return { ...resumeClock(clock, at), finishedAt: at };
}

/** Average speed in km/h, or null while there is too little to average. */
export function averageKmh(distanceM: number, ms: number): number | null {
  if (ms < MIN_AVERAGE_MS) return null;
  return distanceM / 1000 / (ms / 3_600_000);
}
