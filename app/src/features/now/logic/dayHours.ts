import type { DailyWeather } from '../../../lib/types';

// Which day the desktop hour table shows, and which of its hours.
// Dates are the location's local "YYYY-MM-DD", compared as plain strings.

/**
 * The hours of one day still ahead: today starts at the current hour slot
 * (`nowIso`), later days keep all of theirs.
 */
export function hoursOfDay<T extends { time: string }>(hourly: ReadonlyArray<T>, date: string, nowIso: string): T[] {
  return hourly.filter((h) => h.time.startsWith(date) && h.time >= nowIso);
}

/**
 * The day picked on a card, or today until one is picked. A picked day that
 * has left the forecast (the date moved on overnight) falls back to today too.
 */
export function tableDay(daily: ReadonlyArray<DailyWeather>, picked: string | null, today: string): DailyWeather | null {
  return daily.find((d) => d.date === picked) ?? daily.find((d) => d.date === today) ?? daily[0] ?? null;
}
