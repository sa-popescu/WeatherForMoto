import { useMemo } from 'react';
import { localNowIso } from '../../lib/format';
import type { Lang } from '../../lib/i18n';
import type { HourlyWeather, WeatherResponse } from '../../lib/types';
import { todayAndTomorrow, type RideWindow } from './logic/bestWindow';
import { buildHeadline, type Headline } from './logic/headline';
import type { RadarRain } from './logic/radarRain';
import { rainOutlook, type RainOutlook } from './logic/rainOutlook';
import { buildTimeline, type TimelineBar } from './logic/timeline';
import { nowWarnings, type NowWarning } from './logic/warnings';

// Everything the screen derives from one weather response, recomputed only
// when the data, the location's current minute, the radar or the language change.

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

export function buildNowModel(data: WeatherResponse, nowLocal: string, lang: Lang, radar: RadarRain | null = null): NowModel {
  const { hourly, daily, current } = data;
  const startIndex = indexForHour(hourly, nowLocal.slice(0, 13));
  const nowIso = hourly[startIndex].time;

  return {
    nowIso,
    startIndex,
    headline: buildHeadline({ current, hourly, startIndex, daily, lang, nowLocal, radar }),
    warnings: nowWarnings(current, hourly, startIndex),
    windows: todayAndTomorrow(hourly, startIndex, daily),
    bars: buildTimeline(hourly, startIndex, daily),
    rain: rainOutlook(hourly, startIndex),
  };
}

export function useNowModel(data: WeatherResponse | null, nowMs: number, lang: Lang, radar: RadarRain | null = null): NowModel | null {
  // Keyed to the minute so the night flag flips at sunrise / sunset, not at the next full hour.
  const nowLocal = data ? localNowIso(data.utc_offset_seconds, nowMs) : '';
  return useMemo(() => (data && data.hourly.length ? buildNowModel(data, nowLocal, lang, radar) : null), [data, nowLocal, lang, radar]);
}
