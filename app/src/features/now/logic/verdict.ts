import { tierOf, type Tier } from '../../../lib/scoring';
import type { DailyWeather, HourlyWeather } from '../../../lib/types';
import { bestWindow, thresholds, type RideWindow } from './bestWindow';
import { hoursOfDate, isDaylight, nextDate } from './daylight';

// "Can I ride now?" as a pure decision on the backend's hourly moto_score.
// Rideable means OK or better; only today's daylight hours ahead count for
// "until" / "from" (nobody plans a ride around a 03:00 score).

export type Verdict =
  | { kind: 'go'; tier: Tier; night: boolean }
  | { kind: 'goUntil'; until: string }
  | { kind: 'notNow'; from: string; tomorrow: boolean }
  | { kind: 'notToday'; tomorrow: RideWindow | null }
  | { kind: 'unknown' };

export interface VerdictInput {
  /** Score right now (current.moto_score, else the current hour's). */
  nowScore: number | null;
  hourly: ReadonlyArray<HourlyWeather>;
  /** Index of the current hour in `hourly`. */
  startIndex: number;
  daily: ReadonlyArray<DailyWeather>;
}

const scoreOf = (h: HourlyWeather): number => h.moto_score ?? -1;

/** Today's scored daylight hours after the current one. */
export function daylightAhead(hourly: ReadonlyArray<HourlyWeather>, startIndex: number, daily: ReadonlyArray<DailyWeather>): HourlyWeather[] {
  const now = hourly[startIndex];
  if (!now) return [];
  const today = now.time.slice(0, 10);
  return hourly.slice(startIndex + 1).filter((h) => h.time.startsWith(today) && h.moto_score != null && isDaylight(h, daily));
}

export function computeVerdict({ nowScore, hourly, startIndex, daily }: VerdictInput): Verdict {
  const now = hourly[startIndex];
  if (nowScore == null || !now) return { kind: 'unknown' };
  const { ok } = thresholds();
  const ahead = daylightAhead(hourly, startIndex, daily);

  if (nowScore >= ok) {
    const drop = ahead.find((h) => scoreOf(h) < ok);
    if (drop) return { kind: 'goUntil', until: drop.time };
    return { kind: 'go', tier: tierOf(nowScore) ?? 'ok', night: !isDaylight(now, daily) };
  }

  // Recovery needs two good hours in a row (or the last daylight hour), so a
  // single dry gap inside a rainy afternoon is not sold as "yes".
  const recover = ahead.findIndex((h, i) => scoreOf(h) >= ok && (i === ahead.length - 1 || scoreOf(ahead[i + 1]) >= ok));
  if (recover >= 0) return { kind: 'notNow', from: ahead[recover].time, tomorrow: false };

  const tomorrow = bestWindow(hoursOfDate(hourly, nextDate(now.time.slice(0, 10))), daily);
  if (ahead.length === 0 && tomorrow) return { kind: 'notNow', from: tomorrow.from, tomorrow: true };
  return { kind: 'notToday', tomorrow };
}
