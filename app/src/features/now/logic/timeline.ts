import { CORE } from '../../../i18n/core';
import { fmtMm, fmtNumber, fmtTemp, hourOf } from '../../../lib/format';
import { fmt, pick, type Lang } from '../../../lib/i18n';
import { rainBandOf, tierOf } from '../../../lib/scoring';
import type { DailyWeather, HourlyWeather, RainBand } from '../../../lib/types';
import { S_NOW } from '../strings';
import { isDaylight } from './daylight';
import { bandWord, rainPhrase, timeLabel } from './formatting';
import { gustText } from './readouts';

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

/** Below this an hour counts as dry: the same floor the rain lane draws from. */
export const MEASURABLE_MM = 0.05;
/** A dry hour with at least this chance of rain reads "possible", not "dry". */
const POSSIBLE_RAIN_PCT = 30;

/**
 * The picked hour, spelled out for the panel above the bars. The bars only
 * carry the score, so this is where the hour's temperature, rain and gusts are
 * read without opening the details.
 */
export interface HourReadout {
  /** "Acum · 16:00", "21:00" or "mâine 06:00". */
  when: string;
  score: number | null;
  /** "11°" */
  temp: string;
  /** "resimțit 9°" */
  feels: string;
  rain: {
    /** wet: it rains that hour; possible: dry, but with a real chance; dry: neither. */
    state: 'dry' | 'possible' | 'wet';
    /** "80%", "Posibil" or "Uscat". */
    value: string;
    /** "1,2 mm/h · slabă" or "șanse 45%". */
    sub: string;
  };
  /** Whole km/h, "22". */
  gust: string;
  /** "moderate" */
  gustWord: string;
  /** All of it in one sentence, for a screen reader moving the slider. */
  sentence: string;
}

export function hourReadout(bar: TimelineBar, isNow: boolean, nowIso: string, lang: Lang): HourReadout {
  const s = pick(S_NOW, lang);
  const core = pick(CORE, lang);
  const hour = timeLabel(bar.time, nowIso, lang);
  const when = isNow ? fmt(s.readoutNow, { hour }) : hour;
  const probability = bar.hour.precipitation_probability;
  const pct = probability == null ? null : Math.round(probability);
  const chance = pct == null ? '' : fmt(s.rainChance, { p: pct });

  let rain: HourReadout['rain'];
  if (bar.mm >= MEASURABLE_MM) {
    const amount = `${fmtMm(bar.mm, lang)} mm/h`;
    rain = pct == null
      ? { state: 'wet', value: amount, sub: bandWord(bar.band, lang) }
      : { state: 'wet', value: `${pct}%`, sub: `${amount} · ${bandWord(bar.band, lang)}` };
  } else if ((pct ?? 0) >= POSSIBLE_RAIN_PCT) {
    rain = { state: 'possible', value: s.rainPossible, sub: chance };
  } else {
    rain = { state: 'dry', value: s.rainDry, sub: chance };
  }

  const temp = fmtTemp(bar.hour.temperature, lang);
  const gust = fmtNumber(bar.hour.wind_gusts_kmh == null ? null : Math.round(bar.hour.wind_gusts_kmh), lang);
  const tier = tierOf(bar.score);
  const tierName = tier ? { ideal: core.tierIdeal, ok: core.tierOk, atentie: core.tierAtentie, evita: core.tierEvita }[tier] : '';
  const rainWords = rain.state === 'dry' ? core.rainNounNone : rainPhrase({ probability, mm: bar.mm }, lang);
  const sentence = fmt(s.readoutAria, {
    when,
    score: bar.score == null ? '–' : String(bar.score),
    tier: tierName,
    temp,
    rain: rainWords,
    gust,
  });

  return {
    when,
    score: bar.score,
    temp,
    feels: fmt(s.feels, { t: fmtTemp(bar.hour.feels_like, lang) }),
    rain,
    gust,
    gustWord: gustText(bar.hour.wind_gusts_kmh, null, lang),
    sentence,
  };
}

/** The wettest hour of the timeline, or null when no hour has measurable rain. */
export function peakRain(bars: ReadonlyArray<TimelineBar>): TimelineBar | null {
  let peak: TimelineBar | null = null;
  for (const bar of bars) {
    if (bar.mm >= MEASURABLE_MM && (peak === null || bar.mm > peak.mm)) peak = bar;
  }
  return peak;
}
