import { useMemo } from 'react';
import { localNowIso } from '../../lib/format';
import type { Lang } from '../../lib/i18n';
import type { HourlyWeather, WeatherResponse } from '../../lib/types';
import { todayAndTomorrow, type RideWindow } from './logic/bestWindow';
import { buildHeadline, type Headline } from './logic/headline';
import { rainOutlook, type RainOutlook } from './logic/rainOutlook';
import { buildTimeline, type TimelineBar } from './logic/timeline';
import { nowWarnings, type NowWarning } from './logic/warnings';

// Everything the screen derives from one weather response, recomputed only
// when the data, the location's current hour or the language change.

export interface NowModel {
  /** Local ISO of the current hour slot at the location. */
  nowIso: string;
  startIndex: number;
  headline: Headline;
  warnings: NowWarning[];
  windows: { today: RideWindow | null; tomorrow: RideWindow | null };
  bars: TimelineBar[];
  rain: RainOutlook;
}

function indexForHour(hourly: ReadonlyArray<HourlyWeather>, hourKey: string): number {
  const exact = hourly.findIndex((h) => h.time.slice(0, 13) === hourKey);
  if (exact >= 0) return exact;
  const next = hourly.findIndex((h) => h.time.slice(0, 13) > hourKey);
  return next >= 0 ? next : hourly.length - 1;
}

export function buildNowModel(data: WeatherResponse, hourKey: string, lang: Lang): NowModel {
  const { hourly, daily, current } = data;
  const startIndex = indexForHour(hourly, hourKey);
  const nowIso = hourly[startIndex].time;

  return {
    nowIso,
    startIndex,
    headline: buildHeadline({ current, hourly, startIndex, daily, lang }),
    warnings: nowWarnings(current, hourly, startIndex),
    windows: todayAndTomorrow(hourly, startIndex, daily),
    bars: buildTimeline(hourly, startIndex, daily),
    rain: rainOutlook(hourly, startIndex),
  };
}

export function useNowModel(data: WeatherResponse | null, nowMs: number, lang: Lang): NowModel | null {
  const hourKey = data ? localNowIso(data.utc_offset_seconds, nowMs).slice(0, 13) : '';
  return useMemo(() => (data && data.hourly.length ? buildNowModel(data, hourKey, lang) : null), [data, hourKey, lang]);
}
