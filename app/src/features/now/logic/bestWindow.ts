import { addMinutesLocal, minutesBetween } from '../../../lib/format';
import { getScoringMeta } from '../../../lib/scoring';
import type { DailyWeather, HourlyWeather } from '../../../lib/types';
import { hoursOfDate, isDaylight, nextDate, sunTimes } from './daylight';

// Best riding window: the longest run of consecutive DAYLIGHT hours at IDEAL
// level (fallback: OK level). Ties go to the higher average, then the earlier.

export interface RideWindow {
  /** Local ISO of the first hour. */
  from: string;
  /** Local ISO of the end: one hour after the last, or sunset when earlier. */
  to: string;
  avg: number;
  hours: number;
  /** true when the run is at IDEAL level. */
  strong: boolean;
}

export function thresholds(): { ideal: number; ok: number } {
  const labels = getScoringMeta().labels;
  return {
    ideal: labels.find((l) => l.label === 'IDEAL')?.min_score ?? 85,
    ok: labels.find((l) => l.label === 'OK')?.min_score ?? 60,
  };
}

function longestRun(hours: HourlyWeather[], min: number): { start: number; len: number; avg: number } | null {
  let best: { start: number; len: number; avg: number } | null = null;
  let i = 0;
  while (i < hours.length) {
    if ((hours[i].moto_score ?? -1) < min) {
      i += 1;
      continue;
    }
    let j = i;
    let sum = 0;
    while (j < hours.length && (hours[j].moto_score ?? -1) >= min && (j === i || minutesBetween(hours[j - 1].time, hours[j].time) === 60)) {
      sum += hours[j].moto_score ?? 0;
      j += 1;
    }
    const len = j - i;
    const avg = sum / len;
    if (!best || len > best.len || (len === best.len && avg > best.avg)) best = { start: i, len, avg };
    i = j;
  }
  return best;
}

/** Shorter windows (one hour clipped by sunset, say) are not worth a ride. */
const MIN_WINDOW_MINUTES = 90;

function toWindow(run: { start: number; len: number; avg: number } | null, daylight: HourlyWeather[], daily: ReadonlyArray<DailyWeather>, strong: boolean): RideWindow | null {
  if (!run) return null;
  const first = daylight[run.start];
  const last = daylight[run.start + run.len - 1];
  let to = addMinutesLocal(last.time, 60);
  const { sunset } = sunTimes(last.time.slice(0, 10), daily);
  if (sunset && sunset > last.time && sunset < to) to = sunset;
  if (minutesBetween(first.time, to) < MIN_WINDOW_MINUTES) return null;
  return { from: first.time, to, avg: Math.round(run.avg), hours: run.len, strong };
}

export function bestWindow(hours: ReadonlyArray<HourlyWeather>, daily: ReadonlyArray<DailyWeather>): RideWindow | null {
  const daylight = hours.filter((h) => h.moto_score != null && isDaylight(h, daily));
  const { ideal, ok } = thresholds();
  return toWindow(longestRun(daylight, ideal), daylight, daily, true) ?? toWindow(longestRun(daylight, ok), daylight, daily, false);
}

/** Today's window from the current hour on, and tomorrow's whole-day window. */
export function todayAndTomorrow(
  hourly: ReadonlyArray<HourlyWeather>,
  startIndex: number,
  daily: ReadonlyArray<DailyWeather>,
): { today: RideWindow | null; tomorrow: RideWindow | null } {
  const now = hourly[startIndex];
  if (!now) return { today: null, tomorrow: null };
  const date = now.time.slice(0, 10);
  const todayHours = hourly.slice(startIndex).filter((h) => h.time.startsWith(date));
  return { today: bestWindow(todayHours, daily), tomorrow: bestWindow(hoursOfDate(hourly, nextDate(date)), daily) };
}
