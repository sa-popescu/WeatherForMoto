import { addMinutesLocal, hourNumber } from '../../../lib/format';
import type { DailyWeather, HourlyWeather } from '../../../lib/types';

// Daylight from the hour's own is_day flag (what the backend scores with),
// falling back to the day's sunrise / sunset, then to 07:00-20:00.

const FALLBACK_DAY_START = 7;
const FALLBACK_DAY_END = 20;

export function sunTimes(date: string, daily: ReadonlyArray<DailyWeather>): { sunrise: string | null; sunset: string | null } {
  const day = daily.find((d) => d.date === date);
  return { sunrise: day?.sunrise ?? null, sunset: day?.sunset ?? null };
}

export function isDaylight(hour: HourlyWeather, daily: ReadonlyArray<DailyWeather>): boolean {
  if (hour.is_day != null) return hour.is_day;
  const { sunrise, sunset } = sunTimes(hour.time.slice(0, 10), daily);
  if (sunrise && sunset) return hour.time >= sunrise && hour.time < sunset;
  const h = hourNumber(hour.time);
  return h >= FALLBACK_DAY_START && h < FALLBACK_DAY_END;
}

/** "2026-09-11" -> "2026-09-12" */
export function nextDate(date: string): string {
  return addMinutesLocal(`${date}T00:00`, 24 * 60).slice(0, 10);
}

export function hoursOfDate(hourly: ReadonlyArray<HourlyWeather>, date: string): HourlyWeather[] {
  return hourly.filter((h) => h.time.startsWith(date));
}

/** Next sunrise at or after the given local time (today's or tomorrow's). */
export function nextSunrise(nowIso: string, daily: ReadonlyArray<DailyWeather>): string | null {
  const today = sunTimes(nowIso.slice(0, 10), daily).sunrise;
  if (today && today > nowIso) return today;
  return sunTimes(nextDate(nowIso.slice(0, 10)), daily).sunrise;
}
