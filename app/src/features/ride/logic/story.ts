import { rainBandOf } from '../../../lib/scoring';
import type { RainBand } from '../../../lib/types';
import type { RidePoint } from './track';

// "Povestea turei": the weather the rider actually rode through, not the one
// forecast before leaving. The ride is sampled every few minutes; each
// sample gets the weather of its hour at its place, from the models' data
// after the fact (storyWeather.ts); this file turns those into the story.
// Pure, so it is tested on its own.

/** One place and moment of the ride. */
export interface StorySample {
  t: number;
  lat: number;
  lon: number;
  speedKmh: number | null;
  /** The riding minutes this sample stands for (to the next one). */
  minutes: number;
}

/** The weather at a sample's hour and place. */
export interface SampleWeather {
  tempC: number | null;
  precipMm: number | null;
  gustKmh: number | null;
  isDay: boolean | null;
}

export interface RideStory {
  /** Minutes ridden in rain, from when to when, and how hard at most. */
  rainMin: number;
  rainFrom: number | null;
  rainTo: number | null;
  rainBand: RainBand;
  /** The coldest moment, and how cold it felt at the speed of the moment. */
  coldest: { tempC: number; feelsC: number; at: number; speedKmh: number | null } | null;
  minTempC: number | null;
  warmestC: number | null;
  gust: { kmh: number; at: number } | null;
  /** Minutes ridden before sunrise or after sunset. */
  darkMin: number;
  /** The highest point, metres, from the GPS (absent when the phone gave no altitude). */
  highestM: number | null;
  /** Samples that had their weather: none means no story. */
  samples: number;
}

/** At most this many samples, never closer than this in time. */
const MAX_SAMPLES = 48;
const MIN_STEP_MS = 5 * 60_000;
/** An hour with this much is rain, for a rider. */
const RAIN_MM = 0.2;

export function storySamples(points: readonly RidePoint[]): StorySample[] {
  if (points.length === 0) return [];
  const first = points[0].t;
  const last = points[points.length - 1].t;
  const step = Math.max(MIN_STEP_MS, (last - first) / MAX_SAMPLES);
  const picked: RidePoint[] = [points[0]];
  for (const p of points) if (p.t - picked[picked.length - 1].t >= step) picked.push(p);
  if (picked[picked.length - 1] !== points[points.length - 1]) picked.push(points[points.length - 1]);
  return picked.map((p, i) => {
    const next = picked[i + 1];
    // A pause in between is not riding: no sample stands for more than two steps.
    const minutes = next ? Math.min(next.t - p.t, 2 * step) / 60_000 : 0;
    return { t: p.t, lat: p.lat, lon: p.lon, speedKmh: p.speed != null && p.speed >= 0 ? p.speed * 3.6 : null, minutes };
  });
}

/**
 * How cold it feels at riding speed: the wind chill formula (Environment
 * Canada) with the bike's speed as the wind. It is meant for 10 °C and below;
 * above that, the air temperature is given back.
 */
export function windChill(tempC: number, speedKmh: number | null): number {
  if (speedKmh === null || tempC > 10 || speedKmh < 5) return tempC;
  const v = speedKmh ** 0.16;
  return 13.12 + 0.6215 * tempC - 11.37 * v + 0.3965 * tempC * v;
}

const BAND_ORDER: readonly RainBand[] = ['none', 'urme', 'slaba', 'moderata', 'puternica'];

/** The highest point, from the middle of every three altitudes, so one GPS spike does not count. */
export function highestAltitude(points: readonly RidePoint[]): number | null {
  const alts = points.map((p) => p.alt).filter((a): a is number => a !== null && Number.isFinite(a));
  if (alts.length < 3) return null;
  let top = Number.NEGATIVE_INFINITY;
  for (let i = 1; i < alts.length - 1; i += 1) top = Math.max(top, [alts[i - 1], alts[i], alts[i + 1]].sort((a, b) => a - b)[1]);
  return Math.round(top);
}

export function buildStory(samples: readonly StorySample[], weather: ReadonlyArray<SampleWeather | null>, highestM: number | null): RideStory {
  const story: RideStory = {
    rainMin: 0,
    rainFrom: null,
    rainTo: null,
    rainBand: 'none',
    coldest: null,
    minTempC: null,
    warmestC: null,
    gust: null,
    darkMin: 0,
    highestM,
    samples: 0,
  };
  samples.forEach((sample, i) => {
    const w = weather[i];
    if (!w) return;
    story.samples += 1;
    if (w.precipMm !== null && w.precipMm >= RAIN_MM) {
      story.rainMin += sample.minutes;
      story.rainFrom ??= sample.t;
      story.rainTo = sample.t + sample.minutes * 60_000;
      const band = rainBandOf(w.precipMm);
      if (BAND_ORDER.indexOf(band) > BAND_ORDER.indexOf(story.rainBand)) story.rainBand = band;
    }
    if (w.tempC !== null) {
      const feels = windChill(w.tempC, sample.speedKmh);
      if (!story.coldest || feels < story.coldest.feelsC) story.coldest = { tempC: w.tempC, feelsC: feels, at: sample.t, speedKmh: sample.speedKmh };
      story.minTempC = story.minTempC === null ? w.tempC : Math.min(story.minTempC, w.tempC);
      story.warmestC = story.warmestC === null ? w.tempC : Math.max(story.warmestC, w.tempC);
    }
    if (w.gustKmh !== null && (!story.gust || w.gustKmh > story.gust.kmh)) story.gust = { kmh: w.gustKmh, at: sample.t };
    if (w.isDay === false) story.darkMin += sample.minutes;
  });
  story.rainMin = Math.round(story.rainMin);
  story.darkMin = Math.round(story.darkMin);
  return story;
}
