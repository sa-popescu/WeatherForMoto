import { FRAME_HOLD_MS, FRAME_STEP_MS, type RadarFrame } from './radar';
import { FORECAST_STEP_MS } from './forecast';

// One band of time for the map: observed radar, then RainViewer's half hour of
// nowcast, then the forecast hours. Pure helpers, so the ordering rules are
// testable without a map.

export interface TimelineEntry {
  /** Unix seconds, UTC. */
  timeSec: number;
  source: 'radar' | 'forecast';
  /** Index into that source's own list. */
  index: number;
  /** True for anything that has not been observed yet. */
  forecast: boolean;
}

/** Seconds of slack so an hour that coincides with the last radar frame is not shown twice. */
const OVERLAP_S = 60;

export function buildTimeline(
  frames: readonly RadarFrame[],
  forecastTimes: readonly string[],
): TimelineEntry[] {
  const radar: TimelineEntry[] = frames.map((frame, index) => ({
    timeSec: frame.time,
    source: 'radar',
    index,
    forecast: frame.nowcast,
  }));

  const lastRadar = radar.length > 0 ? radar[radar.length - 1].timeSec : null;
  const forecast: TimelineEntry[] = [];
  forecastTimes.forEach((time, index) => {
    const ms = Date.parse(`${time}Z`);
    if (Number.isNaN(ms)) return;
    const timeSec = Math.round(ms / 1000);
    // The radar already covers its own window; the forecast continues after it.
    if (lastRadar !== null && timeSec <= lastRadar + OVERLAP_S) return;
    forecast.push({ timeSec, source: 'forecast', index, forecast: true });
  });

  return [...radar, ...forecast].sort((a, b) => a.timeSec - b.timeSec);
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
 * How long an entry stays on screen while playing: radar frames flick past,
 * forecast hours need longer, and the present and the end are held.
 */
export function entryDelayMs(entries: readonly TimelineEntry[], index: number, holdIndex: number): number {
  if (index === holdIndex || index === entries.length - 1) return FRAME_HOLD_MS;
  return entries[index]?.source === 'forecast' ? FORECAST_STEP_MS : FRAME_STEP_MS;
}

/** The next entry, wrapping back to the start. */
export function nextIndex(index: number, count: number): number {
  return count > 0 ? (index + 1) % count : -1;
}
