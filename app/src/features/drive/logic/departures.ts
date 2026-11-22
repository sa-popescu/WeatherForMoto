import type { RainBand } from '../../../lib/types';
import type { AheadWarning } from '../../ride/logic/ahead';
import type { RouteOutlook } from './driveWeather';

// "Plecarea perfectă": the same trip at a few departure times, each with the
// traffic TomTom predicts for it and the weather the rider meets on the way
// at the hours they pass, and the one worth recommending. Nobody else puts
// the two together: navigators time the traffic, weather apps time the rain.

/** Later departures: the half hour at least 20 minutes away, the next two half hours, then hourly. */
const LATER_OFFSETS_MIN = [0, 30, 60, 120, 180] as const;
const FIRST_GAP_MIN = 20;
const HALF_HOUR_MS = 30 * 60_000;

/** The departure times to compare after now, UTC ms, on round half hours. */
export function departureTimes(nowMs: number): number[] {
  const first = Math.ceil((nowMs + FIRST_GAP_MIN * 60_000) / HALF_HOUR_MS) * HALF_HOUR_MS;
  return LATER_OFFSETS_MIN.map((m) => first + m * 60_000);
}

export interface DepartureOption {
  /** null for now. */
  departMs: number | null;
  status: 'loading' | 'ready' | 'error';
  /** The ride's time for that departure, with predicted traffic when TomTom gave one. */
  durationS: number | null;
  delayS: number;
  outlook: RouteOutlook | null;
}

/** Minutes a warning weighs against riding time: a frosty or very wet ride is worth waiting a while to avoid. */
const RAIN_COST_MIN: Record<RainBand, number> = { none: 40, urme: 40, slaba: 60, moderata: 120, puternica: 180 };
const FROST_COST_MIN = 240;
const GUST_COST_MIN = 45;
const SCORE_COST_MIN = 60;
const DARK_COST_MIN = 30;
/** Under this score each point lower costs half a minute. */
const COMFORT_SCORE = 70;
/** Waiting counts, a little: between two equal rides, the sooner one wins. */
const WAIT_WEIGHT = 0.25;

function warningCost(w: AheadWarning | null): number {
  if (!w) return 0;
  if (w.kind === 'frost') return FROST_COST_MIN;
  if (w.kind === 'rain') return RAIN_COST_MIN[w.band ?? 'urme'];
  if (w.kind === 'gust') return GUST_COST_MIN;
  return SCORE_COST_MIN;
}

/** What a departure costs, in minutes of riding it is worth; null while it is not known. */
export function departureCost(option: DepartureOption, nowMs: number): number | null {
  const { outlook, durationS } = option;
  if (option.status !== 'ready' || durationS === null || !outlook?.settled || outlook.worstScore === null) return null;
  const waitMin = option.departMs === null ? 0 : Math.max(0, (option.departMs - nowMs) / 60_000);
  return (
    durationS / 60 +
    warningCost(outlook.warning) +
    (outlook.dark ? DARK_COST_MIN : 0) +
    Math.max(0, COMFORT_SCORE - outlook.worstScore) * 0.5 +
    waitMin * WAIT_WEIGHT
  );
}

/** The index of the departure to recommend, or null until every one is known. */
export function bestDeparture(options: readonly DepartureOption[], nowMs: number): number | null {
  if (options.some((o) => o.status === 'loading' || (o.status === 'ready' && !o.outlook?.settled))) return null;
  let best: number | null = null;
  let lowest = Number.POSITIVE_INFINITY;
  options.forEach((o, i) => {
    const cost = departureCost(o, nowMs);
    if (cost !== null && cost < lowest) {
      lowest = cost;
      best = i;
    }
  });
  return best;
}
