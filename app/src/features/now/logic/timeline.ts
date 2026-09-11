import { fmtMm, fmtTemp, hourOf } from '../../../lib/format';
import type { Lang } from '../../../lib/i18n';
import { rainBandOf } from '../../../lib/scoring';
import type { DailyWeather, HourlyWeather, RainBand } from '../../../lib/types';
import { isDaylight } from './daylight';
import { timeLabel } from './formatting';

// 24 h timeline model: one bar per hour from the current one.

export const TIMELINE_HOURS = 24;
const LABEL_EVERY_HOURS = 3;

export interface TimelineBar {
  time: string;
  hour: HourlyWeather;
  score: number | null;
  night: boolean;
  /** Hour number shown under every third bar ("16"); the first bar reads "ACUM" in the UI. */
  label: string | null;
  mm: number;
  band: RainBand;
}

export function buildTimeline(hourly: ReadonlyArray<HourlyWeather>, startIndex: number, daily: ReadonlyArray<DailyWeather>, count: number = TIMELINE_HOURS): TimelineBar[] {
  return hourly.slice(startIndex, startIndex + count).map((h, i) => ({
    time: h.time,
    hour: h,
    score: h.moto_score,
    night: !isDaylight(h, daily),
    label: i > 0 && i % LABEL_EVERY_HOURS === 0 ? hourOf(h.time).slice(0, 2) : null,
    mm: Math.max(0, h.precipitation_mm ?? 0),
    band: rainBandOf(h.precipitation_mm),
  }));
}

export interface NightRange {
  start: number;
  /** Exclusive. */
  end: number;
}

export function nightRanges(bars: ReadonlyArray<TimelineBar>): NightRange[] {
  const ranges: NightRange[] = [];
  bars.forEach((bar, i) => {
    if (!bar.night) return;
    const last = ranges[ranges.length - 1];
    if (last && last.end === i) last.end = i + 1;
    else ranges.push({ start: i, end: i + 1 });
  });
  return ranges;
}

/** "17:00 · 34 · 80% · 3,2 mm/h" (rain hours) or "mâine 06:00 · 84 · 18°" (dry hours). */
export function calloutText(bar: TimelineBar, nowIso: string, lang: Lang): string {
  const parts = [timeLabel(bar.time, nowIso, lang), bar.score == null ? '–' : String(bar.score)];
  const probability = bar.hour.precipitation_probability;
  if ((probability ?? 0) > 0 || bar.mm >= 0.05) {
    if (probability != null) parts.push(`${Math.round(probability)}%`);
    parts.push(`${fmtMm(bar.mm, lang)} mm/h`);
  } else {
    parts.push(fmtTemp(bar.hour.temperature, lang));
  }
  return parts.join(' · ');
}

/** Largest hourly amount in the timeline, for the "maxim X mm/h" label. */
export function maxRain(bars: ReadonlyArray<TimelineBar>): number {
  return bars.reduce((max, b) => Math.max(max, b.mm), 0);
}
