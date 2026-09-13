import { describe, expect, it } from 'vitest';
import {
  boundsMovedEnough,
  cellKm,
  cloudRgba,
  colorFor,
  fieldImageData,
  forecastUrl,
  frameOffsetHours,
  gridPoints,
  gridSizeFor,
  MAX_GRID,
  MIN_GRID,
  parseForecast,
  rainRgba,
  sampleField,
  TARGET_CELL_KM,
  utcHourKey,
  type GridBounds,
  type Rgba,
} from './forecast';
import { RADAR_LEGEND } from './radar';

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
    for (const p of gridPoints(BOX, 4, 4)) {
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

  it('paints one pixel per cell when drawn at grid size', () => {
    const pixels = fieldImageData('cloud', [100, null, 0, 50], 2, 2, 2, 2);
    expect(pixels).toHaveLength(16);
    expect(pixels[3]).toBe(colorFor('cloud', 100)[3]);
    // Exactly on an empty sample there is nothing to draw: that cell is a gap.
    expect(pixels[7]).toBe(0);
  });

  it('borrows from the neighbours between samples, so one gap is not a hole', () => {
    const pixels = fieldImageData('cloud', [100, null, 0, 50], 2, 2, 8, 8);
    // Middle of the top edge, between the full cell and the empty one.
    const i = (0 * 8 + 4) * 4;
    expect(pixels[i + 3]).toBeGreaterThan(0);
  });
});

describe('the forecast field matches the radar it continues', () => {
  const hex = ([r, g, b]: Rgba): string => `#${[r, g, b].map((v) => v.toString(16).padStart(2, '0')).join('')}`;

  it('paints rain in the radar legend colours', () => {
    const radarColors = new Set(RADAR_LEGEND.flatMap((band) => band.colors));
    for (const mm of [0.2, 0.6, 1, 2, 3, 6, 10, 20, 50]) {
      expect(radarColors.has(hex(rainRgba(mm)))).toBe(true);
    }
  });

  it('climbs the scale the same way: blue for light rain, yellow through red for heavy', () => {
    expect(hex(rainRgba(0.2))).toBe('#88ddee');
    expect(hex(rainRgba(1))).toBe('#0077aa');
    expect(hex(rainRgba(3))).toBe('#ffee00');
    expect(hex(rainRgba(10))).toBe('#ff4400');
    expect(hex(rainRgba(50))).toBe('#ffaaff');
  });

  it('keeps every painted pixel on a legend colour, with no blended edges', () => {
    const radarColors = new Set(RADAR_LEGEND.flatMap((band) => band.colors));
    // Light rain beside heavy: the field steps from band to band across the row.
    const pixels = fieldImageData('rain', [0.2, 12], 2, 1, 60, 1);
    const seen = new Set<string>();
    for (let x = 0; x < 60; x += 1) {
      const i = x * 4;
      seen.add(hex([pixels[i], pixels[i + 1], pixels[i + 2], pixels[i + 3]]));
    }
    expect(seen.size).toBeGreaterThan(2);
    for (const colour of seen) expect(radarColors.has(colour)).toBe(true);
  });
});

describe('the grid follows the ground, not the screen', () => {
  it('lands near the target cell size', () => {
    const { cols, rows } = gridSizeFor(BOX);
    const km = cellKm(BOX, cols, rows);
    expect(km).toBeGreaterThanOrEqual(TARGET_CELL_KM * 0.5);
    expect(km).toBeLessThanOrEqual(TARGET_CELL_KM * 1.5);
  });

  it('stays between its bounds for a tiny view and for half a continent', () => {
    const tiny: GridBounds = { south: 44.95, west: 25.95, north: 45.0, east: 26.0 };
    const huge: GridBounds = { south: 35, west: 5, north: 60, east: 40 };
    expect(gridSizeFor(tiny)).toEqual({ cols: MIN_GRID, rows: MIN_GRID });
    expect(gridSizeFor(huge)).toEqual({ cols: MAX_GRID, rows: MAX_GRID });
  });
});

describe('the field is interpolated on values, not on colours', () => {
  it('reads halfway between two samples', () => {
    expect(sampleField([0, 2], 2, 1, 0.5, 0)).toBe(1);
    expect(sampleField([0, 2], 2, 1, 0, 0)).toBe(0);
    expect(sampleField([0, 2], 2, 1, 1, 0)).toBe(2);
  });

  it('drops a missing corner instead of pulling the mix towards zero', () => {
    expect(sampleField([null, 2], 2, 1, 0.5, 0)).toBe(2);
    expect(sampleField([null, null], 2, 1, 0.5, 0)).toBeNull();
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
