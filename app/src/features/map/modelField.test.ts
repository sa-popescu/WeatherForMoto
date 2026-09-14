import { describe, expect, it } from 'vitest';
import { cloudRgba, rainRgba, type ForecastData } from './forecast';
import type { RainGrid } from './iconEu';
import { gridSource, iconEuSource } from './modelField';

const T = Date.parse('2026-09-14T08:00:00Z') / 1000;
const HOUR = 3600;

const grid = (value: number): RainGrid => ({ west: 25, south: 44, east: 27, north: 46, cols: 1, rows: 1, values: Float32Array.from([value]) });

describe('iconEuSource', () => {
  it('reads rain between the hourly totals and says when it cannot', () => {
    const source = iconEuSource(new Map([[T, grid(2)], [T + HOUR, grid(4)]]), 'a');
    expect(source.color).toBe(rainRgba);
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
  };
  const bounds = { south: 44, west: 25, north: 46, east: 27 };

  it('interpolates cloud cover between the hours', () => {
    const source = gridSource(data, bounds, 'cloud', 'c');
    expect(source.color).toBe(cloudRgba);
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
