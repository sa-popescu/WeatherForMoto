import type { DailyWeather } from '../../lib/types';
import type { TimelineRow } from './plan';
import type { SunDay } from './types';

// Riding in the dark. Every weather point loads its forecast with the day's
// sunrise and sunset at that place, so the planner can tell which points you
// reach after sunset or before sunrise, without a request of its own.

/** The sun times a point's forecast carries, one entry per day. */
export function sunDays(daily: readonly DailyWeather[]): SunDay[] {
  return daily.map((d) => ({ date: d.date, sunrise: d.sunrise, sunset: d.sunset }));
}

export interface DarkAt {
  kind: 'before-sunrise' | 'after-sunset';
  /** The sunrise or sunset it falls on the wrong side of, local "YYYY-MM-DDTHH:MM". */
  sun: string;
}

/** Whether `localIso` ("YYYY-MM-DDTHH:MM") is before sunrise or after sunset there; null in daylight or when unknown. */
export function darkAt(localIso: string, sun: readonly SunDay[]): DarkAt | null {
  const day = sun.find((d) => d.date === localIso.slice(0, 10));
  if (!day?.sunrise || !day.sunset) return null;
  if (localIso < day.sunrise) return { kind: 'before-sunrise', sun: day.sunrise };
  return localIso >= day.sunset ? { kind: 'after-sunset', sun: day.sunset } : null;
}

/** The first point in riding order that you reach in the dark, or null when the whole ride is in daylight. */
export function firstDark(rows: readonly TimelineRow[]): (TimelineRow & { dark: DarkAt }) | null {
  for (const row of rows) if (row.dark) return { ...row, dark: row.dark };
  return null;
}
