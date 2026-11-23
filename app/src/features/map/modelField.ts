import type { ForecastData, GridBounds } from './forecast';
import { HOUR_S, rainWeights, type RainGrid } from './iconEu';
import { mixValues, type GridBox } from './rainPaint';

// A forecast field that can be read at any moment, whatever its source: ICON-EU
// hourly rain totals, or the Open-Meteo grid (rain totals or cloud cover).
// Pure helpers only.

export interface FieldSample {
  /** Identifies the values, so a painted frame can be reused. */
  key: string;
  grid: GridBox;
  values: Float32Array;
}

export interface ModelSource {
  /** Changes whenever the underlying data does. */
  id: string;
  /** How much of the rain falls as snow, 0 to 1, when known (rainfall sources only). */
  snow?: (tSec: number) => FieldSample | null;
  sample: (tSec: number) => FieldSample | null;
}

/** Hourly totals (by the end of their hour) read between the two hours around t. */
function sampleTotals(totals: ReadonlyMap<number, { grid: GridBox; values: Float32Array }>, tSec: number): FieldSample | null {
  const { before, after, weight } = rainWeights(tSec);
  const a = totals.get(before);
  const b = totals.get(after);
  if (a && b && a.values.length === b.values.length) {
    return { key: `${before}+${after}@${weight.toFixed(3)}`, grid: a.grid, values: mixValues(a.values, b.values, weight) };
  }
  const only = a ?? b;
  return only ? { key: `${a ? before : after}`, grid: only.grid, values: only.values } : null;
}

/** Snowfall water (mm) per centimetre of snow, Open-Meteo's own ratio: 7 cm of snow is 10 mm of water. */
const SNOW_WATER_MM_PER_CM = 10 / 7;
/** Under this much water in the hour there is too little to tell rain from snow. */
const MIN_SHARE_MM = 0.05;

/** The share of each point's water that falls as snow, NaN where too little falls to tell. */
export function snowShares(rain: ReadonlyArray<number | null>, snow: ReadonlyArray<number | null>): Float32Array {
  return Float32Array.from(rain, (mm, p) => {
    const cm = snow[p];
    if (mm === null || cm === null || mm < MIN_SHARE_MM) return Number.NaN;
    return Math.max(0, Math.min(1, (cm * SNOW_WATER_MM_PER_CM) / mm));
  });
}

/**
 * How much of the rain falls as snow, from the Open-Meteo grid (ICON-EU on the
 * DWD server has rain only): read between the hours like the rain totals.
 */
export function snowSource(data: ForecastData, bounds: GridBounds): ((tSec: number) => FieldSample | null) | undefined {
  if (data.snow.length === 0) return undefined;
  const grid: GridBox = { ...bounds, cols: data.cols, rows: data.rows };
  const byHour = new Map<number, { grid: GridBox; values: Float32Array }>();
  data.times.forEach((time, i) => {
    const ms = Date.parse(`${time}Z`);
    if (!Number.isNaN(ms) && data.rain[i] && data.snow[i]) byHour.set(Math.round(ms / 1000), { grid, values: snowShares(data.rain[i], data.snow[i]) });
  });
  return (tSec) => sampleTotals(byHour, tSec);
}

/** ICON-EU rain from the hourly totals loaded so far, with the share of snow when the grid gives it. */
export function iconEuSource(grids: ReadonlyMap<number, RainGrid>, id: string, snow?: ModelSource['snow']): ModelSource {
  const totals = new Map<number, { grid: GridBox; values: Float32Array }>();
  for (const [hour, grid] of grids) totals.set(hour, { grid, values: grid.values });
  return { id, snow, sample: (tSec) => sampleTotals(totals, tSec) };
}

function toValues(frame: ReadonlyArray<number | null>): Float32Array {
  return Float32Array.from(frame, (v) => (v === null ? Number.NaN : v));
}

/**
 * The Open-Meteo grid. Its precipitation is the total of the hour before each
 * time stamp, read like ICON-EU; its cloud cover is a value at the hour,
 * interpolated in between.
 */
export function gridSource(data: ForecastData, bounds: GridBounds, kind: 'rain' | 'cloud', id: string): ModelSource {
  const grid: GridBox = { ...bounds, cols: data.cols, rows: data.rows };
  const frames = kind === 'cloud' ? data.cloud : data.rain;
  const byHour = new Map<number, { grid: GridBox; values: Float32Array }>();
  data.times.forEach((time, i) => {
    const ms = Date.parse(`${time}Z`);
    if (!Number.isNaN(ms) && frames[i]) byHour.set(Math.round(ms / 1000), { grid, values: toValues(frames[i]) });
  });

  if (kind === 'rain') return { id, snow: snowSource(data, bounds), sample: (tSec) => sampleTotals(byHour, tSec) };

  return {
    id,
    sample: (tSec) => {
      const hour = Math.floor(tSec / HOUR_S) * HOUR_S;
      const weight = (tSec - hour) / HOUR_S;
      const a = byHour.get(hour);
      const b = byHour.get(hour + HOUR_S);
      if (a && b && weight > 0) return { key: `${hour}@${weight.toFixed(3)}`, grid, values: mixValues(a.values, b.values, weight) };
      const only = a ?? b;
      return only ? { key: `${a ? hour : hour + HOUR_S}`, grid, values: only.values } : null;
    },
  };
}
