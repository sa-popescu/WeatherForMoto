import { describe, expect, it } from 'vitest';
import {
  boundsMovedEnough,
  cloudRgba,
  colorFor,
  forecastUrl,
  frameImageData,
  frameOffsetHours,
  gridPoints,
  parseForecast,
  rainRgba,
  utcHourKey,
  type GridBounds,
} from './forecast';

const BOX: GridBounds = { south: 44, west: 25, north: 46, east: 27 };
const NOW = Date.parse('2026-09-12T09:20:00Z');

describe('gridPoints', () => {
  it('samples cell centres from north to south', () => {
    const points = gridPoints(BOX, 2, 2);
    expect(points).toEqual([
      { lat: 45.5, lon: 25.5 },
      { lat: 45.5, lon: 26.5 },
      { lat: 44.5, lon: 25.5 },
      { lat: 44.5, lon: 26.5 },
    ]);
  });

  it('keeps every point inside the box', () => {
    for (const p of gridPoints(BOX)) {
      expect(p.lat).toBeGreaterThan(BOX.south);
      expect(p.lat).toBeLessThan(BOX.north);
      expect(p.lon).toBeGreaterThan(BOX.west);
      expect(p.lon).toBeLessThan(BOX.east);
    }
  });
});

describe('forecastUrl', () => {
  it('asks for both fields on one shared clock', () => {
    const url = new URL(forecastUrl(gridPoints(BOX, 2, 1)));
    expect(url.searchParams.get('latitude')).toBe('45,45');
    expect(url.searchParams.get('longitude')).toBe('25.5,26.5');
    expect(url.searchParams.get('hourly')).toBe('cloud_cover,precipitation');
    expect(url.searchParams.get('timezone')).toBe('UTC');
  });
});

describe('parseForecast', () => {
  const times = ['2026-09-12T08:00', '2026-09-12T09:00', '2026-09-12T10:00', '2026-09-12T11:00'];
  const point = (cloud: number[], rain: number[]) => ({ hourly: { time: times, cloud_cover: cloud, precipitation: rain } });

  it('starts at the current hour and keeps the point order', () => {
    const data = parseForecast(
      [point([10, 20, 30, 40], [0, 0.2, 1, 2]), point([15, 25, 35, 45], [0, 0, 0, 0])],
      { cols: 2, rows: 1 },
      NOW,
    );
    expect(data?.times).toEqual(['2026-09-12T09:00', '2026-09-12T10:00', '2026-09-12T11:00']);
    expect(data?.cloud[0]).toEqual([20, 25]);
    expect(data?.rain[1]).toEqual([1, 0]);
  });

  it('stops after the requested number of hours', () => {
    const data = parseForecast([point([10, 20, 30, 40], [0, 0, 0, 0])], { cols: 1, rows: 1 }, NOW, 2);
    expect(data?.times).toHaveLength(2);
  });

  it('leaves a point that answered short as a gap, not as zero', () => {
    const data = parseForecast(
      [point([10, 20, 30, 40], [0, 0, 0, 0]), { hourly: { time: times, cloud_cover: [10], precipitation: [] } }],
      { cols: 2, rows: 1 },
      NOW,
    );
    expect(data?.cloud[0]).toEqual([20, null]);
    expect(data?.rain[0]).toEqual([0, null]);
  });

  it('accepts a single object and rejects junk', () => {
    expect(parseForecast(point([10, 20, 30, 40], [0, 0, 0, 0]), { cols: 1, rows: 1 }, NOW)?.times[0]).toBe('2026-09-12T09:00');
    expect(parseForecast(null, { cols: 1, rows: 1 }, NOW)).toBeNull();
    expect(parseForecast([], { cols: 1, rows: 1 }, NOW)).toBeNull();
    expect(parseForecast([{ hourly: { time: [] } }], { cols: 1, rows: 1 }, NOW)).toBeNull();
  });
});

describe('colours', () => {
  it('keeps a clear sky and a dry hour invisible', () => {
    expect(cloudRgba(0)[3]).toBe(0);
    expect(cloudRgba(5)[3]).toBe(0);
    expect(cloudRgba(null)[3]).toBe(0);
    expect(rainRgba(0.05)[3]).toBe(0);
    expect(rainRgba(null)[3]).toBe(0);
  });

  it('gets more opaque as the sky fills in, and an overcast sky is plainly visible', () => {
    expect(cloudRgba(40)[3]).toBeLessThan(cloudRgba(90)[3]);
    expect(cloudRgba(100)[3]).toBeGreaterThan(200);
    expect(cloudRgba(20)[3]).toBeGreaterThan(60);
  });

  it('remembers the grid it was sampled on', () => {
    const data = parseForecast(
      [{ hourly: { time: ['2026-09-12T09:00'], cloud_cover: [50], precipitation: [0] } }],
      { cols: 4, rows: 4 },
      NOW,
    );
    expect(data?.cols).toBe(4);
    expect(data?.rows).toBe(4);
  });

  it('changes hue with rain intensity', () => {
    expect(rainRgba(0.2)).not.toEqual(rainRgba(1));
    expect(rainRgba(1)).not.toEqual(rainRgba(5));
    expect(rainRgba(10)[0]).toBeGreaterThan(rainRgba(1)[0]);
  });

  it('paints one pixel per cell', () => {
    const pixels = frameImageData('cloud', [100, null, 0, 50], 2, 2);
    expect(pixels).toHaveLength(16);
    expect(pixels[3]).toBe(colorFor('cloud', 100)[3]);
    expect(pixels[7]).toBe(0);
  });
});

describe('refetching and labels', () => {
  it('refetches when the view moves by a third of its span or zooms', () => {
    expect(boundsMovedEnough(null, BOX)).toBe(true);
    expect(boundsMovedEnough(BOX, { south: 44.1, west: 25.1, north: 46.1, east: 27.1 })).toBe(false);
    expect(boundsMovedEnough(BOX, { south: 45, west: 25, north: 47, east: 27 })).toBe(true);
    expect(boundsMovedEnough(BOX, { south: 43, west: 24, north: 47, east: 28 })).toBe(true);
  });

  it('counts the hours from the current one', () => {
    expect(utcHourKey(NOW)).toBe('2026-09-12T09');
    expect(frameOffsetHours('2026-09-12T09:00', NOW)).toBe(0);
    expect(frameOffsetHours('2026-09-12T15:00', NOW)).toBe(6);
  });
});
