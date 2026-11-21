import { addMinutesLocal, localNowIso } from '../../lib/format';
import { defineStrings, fmt, pick, type Lang } from '../../lib/i18n';
import type { HourlyWeather, MotoLabel, WeatherResponse } from '../../lib/types';
import { uncapitalize } from '../now/logic/formatting';
import { rainConsequence, rainOutlook, rainPeakLine, rainPeriod } from '../now/logic/rainOutlook';

// The weather at a point picked on the desktop map, for the moment shown on
// the band of time: the hour that holds that moment, or the current hour for
// a moment already past (the band starts two hours back with the radar).

/** Hours of forecast the rain line looks ahead from the hour shown. */
export const POINT_RAIN_HOURS = 12;
/** Under this chance of rain, a dry outlook is simply called dry. */
const NAMED_CHANCE_PCT = 20;

const POINT_TEXTS = defineStrings(
  {
    dry: 'Fără ploaie în următoarele 12 ore.',
    rainWhen: 'Ploaie {period}.',
  },
  {
    dry: 'No rain in the next 12 hours.',
    rainWhen: 'Rain {period}.',
  },
);

export interface PointSummary {
  /** Local ISO of the hour shown, "YYYY-MM-DDTHH:00". */
  hourIso: string;
  /** Local ISO of now at the point, for "mâine" in time labels. */
  nowIso: string;
  /** The hour shown is the current one. */
  isNow: boolean;
  score: number | null;
  label: MotoLabel | null;
  temperature: number | null;
  gusts: number | null;
  /** The coming rain in one or two sentences. */
  rain: string;
}

/** The viewer's offset from UTC now, in seconds (UTC+2 gives 7200). */
export function viewerOffsetSec(nowMs: number = Date.now()): number {
  return -new Date(nowMs).getTimezoneOffset() * 60;
}

/**
 * The same forecast with its hours on the viewer's clock. The band of time
 * shows the viewer's clock, the forecast comes on the place's: in Romania
 * they are one and the same, but a computer set to another zone would read
 * "11:00" on the band and "12:00" on the card.
 */
export function inViewerClock(data: WeatherResponse, offsetSec: number = viewerOffsetSec()): WeatherResponse {
  const shiftMin = (offsetSec - data.utc_offset_seconds) / 60;
  if (shiftMin === 0) return data;
  return {
    ...data,
    utc_offset_seconds: offsetSec,
    hourly: data.hourly.map((h) => ({ ...h, time: addMinutesLocal(h.time, shiftMin) })),
  };
}

/** Index of the hourly entry for `momentSec` (UTC seconds), or -1 past the forecast. */
export function hourIndexAt(data: WeatherResponse, momentSec: number, nowMs: number = Date.now()): number {
  const ms = Math.max(momentSec * 1000, nowMs);
  const wantedHour = localNowIso(data.utc_offset_seconds, ms).slice(0, 13);
  return data.hourly.findIndex((h) => h.time.slice(0, 13) === wantedHour);
}

/** "Ploaie 13:00 – 15:00. Până la 80% șanse, maxim 1,2 mm/h (slabă) la 14:00." or the dry line. */
export function rainSentence(hourly: ReadonlyArray<HourlyWeather>, index: number, nowIso: string, lang: Lang): string {
  const t = pick(POINT_TEXTS, lang);
  const outlook = rainOutlook(hourly, index, POINT_RAIN_HOURS);
  const period = rainPeriod(outlook, nowIso, lang);
  if (!period) {
    // A chance worth naming gets the shared line ("cele 25% înseamnă un
    // scenariu izolat…"); a few percent is just dry. The shared dry line
    // speaks of 24 hours, this one looks 12 ahead.
    return outlook.peakProbability >= NAMED_CHANCE_PCT ? rainConsequence(outlook, lang) : t.dry;
  }
  const detail = rainPeakLine(outlook, nowIso, lang) ?? rainConsequence(outlook, lang);
  return `${fmt(t.rainWhen, { period: uncapitalize(period) })} ${detail}`;
}

/** What the card shows, or null when the forecast does not reach the moment. */
export function summarizePoint(data: WeatherResponse, momentSec: number, lang: Lang, nowMs: number = Date.now()): PointSummary | null {
  const index = hourIndexAt(data, momentSec, nowMs);
  if (index < 0) return null;
  const hour = data.hourly[index];
  const nowIso = localNowIso(data.utc_offset_seconds, nowMs);
  return {
    hourIso: hour.time,
    nowIso,
    isNow: hour.time.slice(0, 13) === nowIso.slice(0, 13),
    score: hour.moto_score,
    label: hour.moto_label,
    temperature: hour.temperature,
    gusts: hour.wind_gusts_kmh,
    rain: rainSentence(data.hourly, index, nowIso, lang),
  };
}
