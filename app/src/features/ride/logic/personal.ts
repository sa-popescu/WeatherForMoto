import type { CurrentWeather } from '../../../lib/types';
import type { RideStory } from './story';

// "Scorul tău": the moto score, learning from the rider's own rides. Every
// finished ride keeps the weather it met (its story); after a few rides the
// app knows how cold, how windy and how hot it has been when this rider still
// rode. Where the weather now is within that, the score takes off less for
// cold, wind and heat. Rain, storms, ice and fog keep their full weight: having
// ridden through them once is no reason to take them lightly. Pure, so it is
// tested on its own; it runs on this phone only.

/** Rides with a story needed before the score is personal. */
export const MIN_RIDES = 5;
/** The share of the cold, wind or heat penalty given back within what the rider has ridden in. */
export const FORGIVE = 0.6;

export interface Tolerance {
  /** The coldest apparent temperature ridden in, °C (the second lowest, so one odd ride does not count). */
  coldFeelsC: number | null;
  /** The strongest gusts ridden in, km/h (the second highest). */
  gustKmh: number | null;
  /** The hottest apparent temperature ridden in, °C (the second highest). */
  heatFeelsC: number | null;
}

export interface Learned {
  /** Rides with a story so far. */
  rides: number;
  /** Null until MIN_RIDES. */
  tolerance: Tolerance | null;
}

function secondLowest(values: readonly number[]): number | null {
  const sorted = [...values].sort((a, b) => a - b);
  return sorted.length >= 2 ? sorted[1] : null;
}

function secondHighest(values: readonly number[]): number | null {
  const sorted = [...values].sort((a, b) => b - a);
  return sorted.length >= 2 ? sorted[1] : null;
}

const numbers = (values: ReadonlyArray<number | null | undefined>): number[] => values.filter((v): v is number => typeof v === 'number' && Number.isFinite(v));

export function learnTolerance(stories: ReadonlyArray<RideStory | null>): Learned {
  const known = stories.filter((s): s is RideStory => s !== null && s.samples > 0);
  if (known.length < MIN_RIDES) return { rides: known.length, tolerance: null };
  return {
    rides: known.length,
    tolerance: {
      coldFeelsC: secondLowest(numbers(known.map((s) => s.minFeelsC))),
      gustKmh: secondHighest(numbers(known.map((s) => s.gust?.kmh))),
      heatFeelsC: secondHighest(numbers(known.map((s) => s.maxFeelsC))),
    },
  };
}

/**
 * The score now for this rider: the general score, with part of the cold,
 * wind and heat penalties given back where the weather is within what they
 * have ridden in, and never above a cap that something else set (rain, a
 * storm, frost). Null without the score's breakdown.
 */
export function personalScore(current: Pick<CurrentWeather, 'moto_score' | 'feels_like' | 'wind_gusts_kmh' | 'score_breakdown'>, tolerance: Tolerance): number | null {
  const { moto_score: score, feels_like: feels, wind_gusts_kmh: gust, score_breakdown: factors } = current;
  if (score === null || !factors) return null;
  let bonus = 0;
  let cap = 100;
  for (const f of factors) {
    const within =
      (f.factor === 'cold' && feels !== null && tolerance.coldFeelsC !== null && feels >= tolerance.coldFeelsC) ||
      (f.factor === 'wind' && gust !== null && tolerance.gustKmh !== null && gust <= tolerance.gustKmh) ||
      (f.factor === 'heat' && feels !== null && tolerance.heatFeelsC !== null && feels <= tolerance.heatFeelsC);
    if (within) bonus += f.penalty * FORGIVE;
    else if (f.cap !== null) cap = Math.min(cap, f.cap);
  }
  return Math.round(Math.min(cap, 100, score + bonus));
}
