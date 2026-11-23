import { describe, expect, it } from 'vitest';
import type { ForecastData } from './forecast';
import type { RainGrid } from './iconEu';
import { gridSource, iconEuSource, snowShares, snowSource } from './modelField';

const T = Date.parse('2026-09-14T08:00:00Z') / 1000;
const HOUR = 3600;

const grid = (value: number): RainGrid => ({ west: 25, south: 44, east: 27, north: 46, cols: 1, rows: 1, values: Float32Array.from([value]) });

describe('iconEuSource', () => {
  it('reads rain between the hourly totals and says when it cannot', () => {
    const source = iconEuSource(new Map([[T, grid(2)], [T + HOUR, grid(4)]]), 'a');
    expect(source.sample(T)?.values[0]).toBe(3);
    expect(source.sample(T - HOUR / 2)?.values[0]).toBe(2);
    expect(source.sample(T + 5 * HOUR)).toBeNull();
  });
});

describe('gridSource', () => {
  const data: ForecastData = {
    cols: 1,
    rows: 1,
    times: ['2026-09-14T08:00', '2026-09-14T09:00'],
    cloud: [[20], [80]],
    rain: [[1], [null]],
    snow: [[0.7], [null]],
  };
  const bounds = { south: 44, west: 25, north: 46, east: 27 };

  it('interpolates cloud cover between the hours', () => {
    const source = gridSource(data, bounds, 'cloud', 'c');
    expect(source.sample(T + HOUR / 2)?.values[0]).toBeCloseTo(50, 5);
    expect(source.sample(T)?.values[0]).toBe(20);
    expect(source.sample(T)?.grid).toMatchObject({ ...bounds, cols: 1, rows: 1 });
  });

  it('reads grid rain like model totals and keeps a missing hour as a gap', () => {
    const source = gridSource(data, bounds, 'rain', 'r');
    expect(source.sample(T - HOUR / 2)?.values[0]).toBe(1);
    // Between a total and a gap the known total is used.
    expect(source.sample(T)?.values[0]).toBe(1);
  });
});

describe('snow in the forecast', () => {
  it('turns snowfall into its share of the water, and says nothing where too little falls', () => {
    // 7 cm of snow is 10 mm of water.
    const shares = snowShares([10, 10, 0.01, null], [7, 0, 1, 1]);
    expect(shares[0]).toBeCloseTo(1, 5);
    expect(shares[1]).toBe(0);
    expect(shares[2]).toBeNaN();
    expect(shares[3]).toBeNaN();
  });

  it('reads the share between the hours, from the grid, for ICON-EU rain too', () => {
    const data: ForecastData = {
      cols: 1,
      rows: 1,
      times: ['2026-09-14T08:00', '2026-09-14T09:00'],
      cloud: [[100], [100]],
      rain: [[2], [2]],
      snow: [[1.4], [0]],
    };
    const bounds = { south: 44, west: 25, north: 46, east: 27 };
    const snow = snowSource(data, bounds);
    // Totals belong to the hour they end: half an hour before 08:00 is all that hour, 08:00 itself halfway.
    expect(snow?.(T - HOUR / 2)?.values[0]).toBeCloseTo(1, 5);
    expect(snow?.(T)?.values[0]).toBeCloseTo(0.5, 5);
    const icon = iconEuSource(new Map([[T, grid(2)]]), 'a', snow);
    expect(icon.snow?.(T - HOUR / 2)?.values[0]).toBeCloseTo(1, 5);
    expect(snowSource({ ...data, snow: [] }, bounds)).toBeUndefined();
  });
});
