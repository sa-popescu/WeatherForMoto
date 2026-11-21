import type { DailyWeather, Place } from '../../../lib/types';
import { nextDate } from './daylight';

// "Favoritele, pe 7 zile" on a desktop: every favourite on a row, the coming
// days in columns, each cell the day's ride score. Dates are local
// "YYYY-MM-DD" strings, compared as text.

/** Today and the six days after it, which a 7-day forecast covers. */
export const COMPARE_DAYS = 7;

export interface CompareCell {
  date: string;
  score: number | null;
  /** WMO weather code of the day, for the icon. */
  code: number | null;
}

export interface CompareRow {
  place: Place;
  cells: CompareCell[];
  /** The date with the highest score, or null when no day has one. */
  best: string | null;
  /** The forecast could not be loaded: every cell is empty. */
  failed: boolean;
}

/** `count` consecutive dates from `today`. */
export function compareDates(today: string, count: number = COMPARE_DAYS): string[] {
  const dates = [today];
  while (dates.length < count) dates.push(nextDate(dates[dates.length - 1]));
  return dates;
}

/** The highest score wins; the earlier day wins a tie, since it is the surer forecast. */
export function bestDate(cells: ReadonlyArray<CompareCell>): string | null {
  let best: CompareCell | null = null;
  for (const cell of cells) {
    if (cell.score == null) continue;
    if (!best || cell.score > (best.score ?? -1)) best = cell;
  }
  return best?.date ?? null;
}

/** One favourite's row. `daily` is null when its forecast failed to load. */
export function compareRow(place: Place, daily: ReadonlyArray<DailyWeather> | null, dates: ReadonlyArray<string>): CompareRow {
  const cells = dates.map((date) => {
    const day = daily?.find((d) => d.date === date);
    return { date, score: day?.moto_score ?? null, code: day?.weather_code ?? null };
  });
  return { place, cells, best: bestDate(cells), failed: daily === null };
}
