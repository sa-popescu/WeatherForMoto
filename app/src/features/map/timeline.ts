import { FRAME_HOLD_MS, FRAME_STEP_MS, type RadarFrame } from './radar';
import { FORECAST_HOURS, FORECAST_STEP_MS } from './forecast';
import { HOUR_S, hourEndsFor } from './iconEu';
import { modelWeight, NOWCAST_STEP_S, NOWCAST_STEPS } from './nowcast';

// One band of time for the map: observed radar, then the radar extrapolated
// for up to an hour and a half, then the forecast hours. Pure helpers, so the
// ordering rules are testable without a map.

export type TimelineSource = 'radar' | 'nowcast' | 'forecast';

export interface TimelineEntry {
  /** Unix seconds, UTC. */
  timeSec: number;
  source: TimelineSource;
  /** Index into that source's own list. */
  index: number;
  /** True for anything that has not been observed yet. */
  forecast: boolean;
}

/** Seconds of slack so a moment that coincides with the previous source is not shown twice. */
export const OVERLAP_S = 60;

/**
 * The band in time order. Each source only continues after the one before it
 * ends: radar, then extrapolated frames, then forecast times (all unix seconds).
 */
export function buildTimeline(
  frames: readonly RadarFrame[],
  nowcastTimes: readonly number[],
  forecastTimes: readonly number[],
): TimelineEntry[] {
  const entries: TimelineEntry[] = frames.map((frame, index) => ({
    timeSec: frame.time,
    source: 'radar',
    index,
    forecast: frame.nowcast,
  }));

  let reach = entries.length > 0 ? entries[entries.length - 1].timeSec : null;
  const after = (timeSec: number): boolean => reach === null || timeSec > reach + OVERLAP_S;

  nowcastTimes.forEach((timeSec, index) => {
    if (Number.isFinite(timeSec) && after(timeSec)) entries.push({ timeSec, source: 'nowcast', index, forecast: true });
  });
  reach = entries.length > 0 ? entries[entries.length - 1].timeSec : null;

  forecastTimes.forEach((timeSec, index) => {
    if (Number.isFinite(timeSec) && after(timeSec)) entries.push({ timeSec, source: 'forecast', index, forecast: true });
  });

  return entries.sort((a, b) => a.timeSec - b.timeSec);
}

/** Open-Meteo's "YYYY-MM-DDTHH:MM" (UTC) as unix seconds, NaN when unreadable. */
export function utcTimeSec(time: string): number {
  const ms = Date.parse(`${time}Z`);
  return Number.isNaN(ms) ? Number.NaN : Math.round(ms / 1000);
}

export interface RainPlan {
  /** Extrapolated radar frames, one per radar interval after the base picture. */
  nowcastTimes: number[];
  /** Whole hours painted from the model. */
  modelTimes: number[];
  /** Hourly model totals needed for both (the extrapolated frames fade into the model). */
  hourEnds: number[];
}

export interface RainPlanInput {
  nowSec: number;
  /** Newest observed radar frame, null without radar. */
  lastObservedSec: number | null;
  /** Last radar entry of any kind (RainViewer may add its own forecast frames). */
  lastRadarSec: number | null;
  /** The extrapolation is ready from this picture. */
  nowcastBaseSec: number | null;
  /** An extrapolation is on its way (radar on and nothing failed). */
  nowcastExpected: boolean;
}

/**
 * Which moments the rain part of the band shows. The model hours start after
 * the extrapolation's reach even while it is still being computed, so the band
 * does not shift under the rider's finger when it arrives.
 */
export function planRainBand({ nowSec, lastObservedSec, lastRadarSec, nowcastBaseSec, nowcastExpected }: RainPlanInput): RainPlan {
  const nowcastTimes: number[] = [];
  if (nowcastBaseSec !== null) {
    for (let step = 1; step <= NOWCAST_STEPS; step += 1) {
      const timeSec = nowcastBaseSec + step * NOWCAST_STEP_S;
      if (lastRadarSec === null || timeSec > lastRadarSec + OVERLAP_S) nowcastTimes.push(timeSec);
    }
  }

  let reach: number | null = lastRadarSec;
  if (nowcastTimes.length > 0) reach = nowcastTimes[nowcastTimes.length - 1];
  else if (nowcastExpected && lastObservedSec !== null) reach = Math.max(lastRadarSec ?? 0, lastObservedSec + NOWCAST_STEPS * NOWCAST_STEP_S);

  const currentHour = Math.floor(nowSec / HOUR_S) * HOUR_S;
  const firstHour = reach === null ? currentHour : Math.floor(reach / HOUR_S) * HOUR_S + HOUR_S;
  const lastHour = currentHour + FORECAST_HOURS * HOUR_S;
  const modelTimes: number[] = [];
  for (let hour = firstHour; hour <= lastHour; hour += HOUR_S) modelTimes.push(hour);

  const blended = nowcastBaseSec === null ? [] : nowcastTimes.filter((t) => modelWeight(t - nowcastBaseSec) > 0);
  return { nowcastTimes, modelTimes, hourEnds: hourEndsFor([...blended, ...modelTimes]) };
}

/** Where the band opens: the newest observed moment, else the one closest to now. */
export function startIndex(entries: readonly TimelineEntry[], nowSec: number): number {
  if (entries.length === 0) return -1;
  for (let i = entries.length - 1; i >= 0; i -= 1) if (!entries[i].forecast) return i;
  let best = 0;
  let bestGap = Number.POSITIVE_INFINITY;
  entries.forEach((entry, i) => {
    const gap = Math.abs(entry.timeSec - nowSec);
    if (gap < bestGap) {
      best = i;
      bestGap = gap;
    }
  });
  return best;
}

/**
 * How long an entry stays on screen while playing: radar and extrapolated
 * frames flick past, forecast hours need longer, and the present and the end
 * are held.
 */
export function entryDelayMs(entries: readonly TimelineEntry[], index: number, holdIndex: number): number {
  if (index === holdIndex || index === entries.length - 1) return FRAME_HOLD_MS;
  return entries[index]?.source === 'forecast' ? FORECAST_STEP_MS : FRAME_STEP_MS;
}

/** The next entry, wrapping back to the start. */
export function nextIndex(index: number, count: number): number {
  return count > 0 ? (index + 1) % count : -1;
}
