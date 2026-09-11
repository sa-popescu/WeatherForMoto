import type { DailyWeather, Place, RainBand } from '../../../lib/types';

// "Unde merg în weekend": which favourite has the best Saturday or Sunday.
// Dates are local to each location ("YYYY-MM-DD"), handled as plain strings.

export interface WeekendDay {
  date: string;
  score: number | null;
  probability: number | null;
  band: RainBand;
}

export interface WeekendEntry {
  place: Place;
  days: WeekendDay[];
}

export interface RankedPlace extends WeekendEntry {
  best: WeekendDay | null;
}

function parseIsoDate(iso: string): Date {
  const [y, m, d] = iso.slice(0, 10).split('-').map(Number);
  return new Date(Date.UTC(y, m - 1, d));
}

export function addDays(iso: string, days: number): string {
  const date = parseIsoDate(iso);
  date.setUTCDate(date.getUTCDate() + days);
  return date.toISOString().slice(0, 10);
}

/**
 * Weekend days still ahead, from the location's local date.
 * Monday to Friday: the coming Saturday and Sunday. Saturday: today and
 * tomorrow. Sunday: only today (the next Sunday is beyond a 7-day forecast).
 */
export function weekendDates(todayIso: string): string[] {
  const weekday = parseIsoDate(todayIso).getUTCDay(); // 0 = Sunday, 6 = Saturday
  if (weekday === 0) return [todayIso.slice(0, 10)];
  const saturday = addDays(todayIso, 6 - weekday);
  return [saturday, addDays(saturday, 1)];
}

export function pickWeekend(daily: ReadonlyArray<DailyWeather>, todayIso: string): WeekendDay[] {
  return weekendDates(todayIso).map((date) => {
    const day = daily.find((d) => d.date === date);
    return {
      date,
      score: day?.moto_score ?? null,
      probability: day?.precipitation_probability ?? null,
      band: day?.rain_intensity_max ?? 'none',
    };
  });
}

export function bestDay(days: ReadonlyArray<WeekendDay>): WeekendDay | null {
  let best: WeekendDay | null = null;
  for (const day of days) {
    if (day.score == null) continue;
    if (!best || (best.score ?? -1) < day.score) best = day;
  }
  return best;
}

function averageScore(days: ReadonlyArray<WeekendDay>): number {
  const scores = days.map((d) => d.score).filter((s): s is number => s != null);
  return scores.length ? scores.reduce((a, b) => a + b, 0) / scores.length : -1;
}

/** Best weekend day first; ties go to the better average, then the name. Places without data sink. */
export function rankWeekend(entries: ReadonlyArray<WeekendEntry>): RankedPlace[] {
  return entries
    .map((entry) => ({ ...entry, best: bestDay(entry.days) }))
    .sort((a, b) => {
      const scoreDiff = (b.best?.score ?? -1) - (a.best?.score ?? -1);
      if (scoreDiff !== 0) return scoreDiff;
      const avgDiff = averageScore(b.days) - averageScore(a.days);
      if (avgDiff !== 0) return avgDiff;
      return a.place.name.localeCompare(b.place.name);
    });
}

export interface RainWords {
  noRain: string;
  bands: Record<Exclude<RainBand, 'none'>, string>;
}

/**
 * Rain as chance AND amount ("70% · urme"), never a percentage alone. With no
 * measurable amount it reads "no rain", except above 60% where the backend
 * counts a likely drizzle as traces.
 */
export function rainLine(probability: number | null, band: RainBand, words: RainWords): string {
  const prob = probability == null ? null : Math.round(probability);
  let effective = band;
  if (effective === 'none') {
    if (prob == null || prob <= 60) return words.noRain;
    effective = 'urme';
  }
  const word = words.bands[effective];
  return prob == null ? word : `${prob}% · ${word}`;
}
