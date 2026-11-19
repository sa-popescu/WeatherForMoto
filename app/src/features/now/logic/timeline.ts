import { CORE } from '../../../i18n/core';
import { fmtMm, fmtNumber, fmtTemp, hourOf } from '../../../lib/format';
import { fmt, pick, type Lang } from '../../../lib/i18n';
import { rainBandOf, tierOf } from '../../../lib/scoring';
import type { DailyWeather, HourlyWeather, RainBand } from '../../../lib/types';
import { S_NOW } from '../strings';
import { isDaylight } from './daylight';
import { bandWord, capitalize, rainPhrase, timeLabel } from './formatting';
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
/** A dry hour with at least this chance of rain reads "possible". */
const POSSIBLE_RAIN_PCT = 30;
/** Under this chance, and with no amount, an hour simply reads "no rain": a 5% is noise. */
const UNLIKELY_RAIN_PCT = 10;

/** none: no rain to speak of; unlikely / possible: a chance but no amount; wet: a measurable amount. */
export type RainState = 'none' | 'unlikely' | 'possible' | 'wet';
export type RoadState = 'dry' | 'wet' | 'frost';

/**
 * The picked hour, spelled out for the panel above the bars. The bars only
 * carry the score, so this is where the hour's temperature, gusts, asphalt
 * and rain are read without opening the details.
 */
export interface HourReadout {
  /** "Acum · 16:00", "21:00" or "mâine 06:00". */
  when: string;
  score: number | null;
  /** "11°" */
  temp: string;
  /** "resimțit 9°" */
  feels: string;
  /** Whole km/h, "22". */
  gust: string;
  /** "din E, moderate" */
  gustWord: string;
  road: {
    state: RoadState;
    /** "13°" */
    temp: string;
    /** "ud", "uscat" or "risc de polei". */
    word: string;
  };
  rain: {
    state: RainState;
    /** One word first: "Slabă", "Posibil", "Puțin probabil" or "Nu plouă". */
    value: string;
    /** The numbers after it: "1,2 mm/h · 80% șanse", "40% șanse, cantitate neglijabilă"; empty for no rain. */
    sub: string;
  };
  /** All of it in one sentence, for a screen reader moving the slider. */
  sentence: string;
}

/** The road at that hour: frost first, then rain falling on it. */
export function roadState(hour: HourlyWeather): RoadState {
  if (hour.frost_risk) return 'frost';
  const raining = (hour.precipitation_mm ?? 0) >= MEASURABLE_MM || (hour.rain_intensity != null && hour.rain_intensity !== 'none');
  return raining ? 'wet' : 'dry';
}

function rainReadout(bar: TimelineBar, lang: Lang): HourReadout['rain'] {
  const s = pick(S_NOW, lang);
  const probability = bar.hour.precipitation_probability;
  const pct = probability == null ? null : Math.round(probability);
  if (bar.mm >= MEASURABLE_MM) {
    const amount = fmtMm(bar.mm, lang);
    // The band of the amount as shown: 0.46 mm/h reads "0,5", and "Urme · 0,5"
    // would contradict the scale, where traces stop below 0,5.
    const band = rainBandOf(Math.round(bar.mm * 10) / 10);
    return {
      state: 'wet',
      value: capitalize(bandWord(band, lang)),
      sub: pct == null ? `${amount} mm/h` : fmt(s.rainWetSub, { mm: amount, p: pct }),
    };
  }
  if (pct != null && pct >= POSSIBLE_RAIN_PCT) return { state: 'possible', value: s.rainPossible, sub: fmt(s.rainNegligible, { p: pct }) };
  if (pct != null && pct >= UNLIKELY_RAIN_PCT) return { state: 'unlikely', value: s.rainUnlikely, sub: fmt(s.rainChance, { p: pct }) };
  return { state: 'none', value: s.rainNone, sub: '' };
}

export function hourReadout(bar: TimelineBar, isNow: boolean, nowIso: string, lang: Lang): HourReadout {
  const s = pick(S_NOW, lang);
  const core = pick(CORE, lang);
  const hour = timeLabel(bar.time, nowIso, lang);
  const when = isNow ? fmt(s.readoutNow, { hour }) : hour;
  const rain = rainReadout(bar, lang);
  const road = roadState(bar.hour);
  const roadWord = { dry: s.roadDry, wet: s.roadWet, frost: s.roadFrost }[road];

  const temp = fmtTemp(bar.hour.temperature, lang);
  const gust = fmtNumber(bar.hour.wind_gusts_kmh == null ? null : Math.round(bar.hour.wind_gusts_kmh), lang);
  const roadTemp = fmtTemp(bar.hour.road_surface_temp, lang);
  const tier = tierOf(bar.score);
  const tierName = tier ? { ideal: core.tierIdeal, ok: core.tierOk, atentie: core.tierAtentie, evita: core.tierEvita }[tier] : '';
  const rainWords = rain.state === 'none' ? core.rainNounNone : rainPhrase({ probability: bar.hour.precipitation_probability, mm: bar.mm }, lang);
  const sentence = fmt(s.readoutAria, {
    when,
    score: bar.score == null ? '–' : String(bar.score),
    tier: tierName,
    temp,
    rain: rainWords,
    gust,
    road: `${roadTemp}, ${roadWord}`,
  });

  return {
    when,
    score: bar.score,
    temp,
    feels: fmt(s.feels, { t: fmtTemp(bar.hour.feels_like, lang) }),
    gust,
    gustWord: gustText(bar.hour.wind_gusts_kmh, bar.hour.wind_direction_10m, lang),
    road: { state: road, temp: roadTemp, word: roadWord },
    rain,
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
