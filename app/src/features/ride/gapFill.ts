import { fetchRoute, RouteError } from '../route/osrm';
import type { LatLon } from '../route/types';
import type { TrackGap } from './logic/track';

// Fills a gap in the track (screen locked, tunnel, signal lost) with the road
// the rider most likely took: the OSRM driving route between the two ends of
// the gap, from the same public server the planner already uses. The fill is
// drawn dotted, since it is a reconstruction, and its road distance replaces
// the straight line in the totals.

export interface GapFill {
  /** Time of the point that closed the gap (TrackGap.t). */
  t: number;
  coords: LatLon[];
  roadM: number;
}

/** Below this, the straight line is the road for all practical purposes. */
const MIN_FILL_M = 40;
/** Above this, something other than riding happened (a ferry, a car train): no guess. */
const MAX_FILL_M = 150_000;
/** A road this many times the straight line (plus the slack) is not the one taken. */
const MAX_DETOUR = 3;
const DETOUR_SLACK_M = 1_000;

export function needsFill(gap: TrackGap): boolean {
  return gap.straightM >= MIN_FILL_M && gap.straightM <= MAX_FILL_M;
}

/** Whether a road of this length can stand for the gap. */
export function plausibleRoad(gap: TrackGap, roadM: number): boolean {
  return roadM >= gap.straightM * 0.9 && roadM <= gap.straightM * MAX_DETOUR + DETOUR_SLACK_M;
}

export type FillOutcome = { kind: 'filled'; fill: GapFill } | { kind: 'skip' } | { kind: 'retry' };

/** A gap that could not be filled (no network) is tried again this often, this many times. */
const RETRY_MS = 30_000;
const MAX_ATTEMPTS = 10;

/**
 * The gaps of one ride waiting for their road, asked one at a time (the
 * public OSRM server wants no bursts). A gap left unfilled stays a straight
 * line; the next start of the app tries it again (recorder.restoreUnfinished).
 */
export class GapFiller {
  private queue: TrackGap[] = [];
  private attempts = new Map<number, number>();
  private readonly controller = new AbortController();
  private busy = false;
  private retryTimer: number | null = null;

  constructor(private readonly onFill: (fill: GapFill) => void) {}

  add(gap: TrackGap): void {
    if (!needsFill(gap) || this.controller.signal.aborted) return;
    this.queue.push(gap);
    void this.drain();
  }

  /** Drops the queue and any request in flight: the ride was closed. */
  stop(): void {
    this.controller.abort();
    this.queue = [];
    if (this.retryTimer !== null) window.clearTimeout(this.retryTimer);
    this.retryTimer = null;
  }

  private async drain(): Promise<void> {
    if (this.busy || this.retryTimer !== null) return;
    this.busy = true;
    try {
      while (this.queue.length > 0) {
        const gap = this.queue[0];
        const outcome = await fillGap(gap, this.controller.signal);
        if (this.controller.signal.aborted) return;
        if (outcome.kind === 'retry') {
          const tries = (this.attempts.get(gap.t) ?? 0) + 1;
          this.attempts.set(gap.t, tries);
          if (tries < MAX_ATTEMPTS) {
            this.retryTimer = window.setTimeout(() => {
              this.retryTimer = null;
              void this.drain();
            }, RETRY_MS);
            return;
          }
        }
        this.queue.shift();
        if (outcome.kind === 'filled') this.onFill(outcome.fill);
      }
    } finally {
      this.busy = false;
    }
  }
}

/** Asks for the road across one gap. Network trouble says retry; no road, or an implausible one, says skip. */
export async function fillGap(gap: TrackGap, signal: AbortSignal): Promise<FillOutcome> {
  try {
    const route = await fetchRoute([gap.from, gap.to], signal);
    const roadM = route.distanceKm * 1000;
    if (!plausibleRoad(gap, roadM)) return { kind: 'skip' };
    return { kind: 'filled', fill: { t: gap.t, coords: route.coords, roadM } };
  } catch (err) {
    if (err instanceof RouteError && err.kind === 'no-route') return { kind: 'skip' };
    if (!signal.aborted) console.warn('[ride] could not fill a gap along the road; will try again', err);
    return { kind: 'retry' };
  }
}
