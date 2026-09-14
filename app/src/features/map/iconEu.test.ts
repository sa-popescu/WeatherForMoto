import { describe, expect, it } from 'vitest';
import { coverageUrl, HOUR_S, hourEndsFor, ICON_EU_COVERAGE, ICON_EU_MAX_CELLS, iconEuCovers, parseCoverageText, rainWeights } from './iconEu';

const T = Date.parse('2026-09-14T08:00:00Z') / 1000;

// Shape of GeoServer's text/plain answer (trimmed from a real one, September 2026).
const ANSWER = `Grid bounds: GeneralBounds[(27.03125, 43.96875), (27.21875, 44.09375)]
Grid CRS: GEOGCS["WGS84(DD)",
  AXIS["Geodetic longitude", EAST],
  AXIS["Geodetic latitude", NORTH]]
Grid range: GridEnvelope2D[809..811, 424..425]
Contents:
Band 0:
0.0 0.017578125 1.2421875
0.5 -9999.0 0.0
`;

describe('coverageUrl', () => {
  it('asks for one hour of the box as plain numbers', () => {
    const url = new URL(coverageUrl({ south: 44, west: 27, north: 46, east: 30 }, T));
    expect(url.origin + url.pathname).toBe('https://maps.dwd.de/geoserver/dwd/wcs');
    expect(url.searchParams.get('coverageId')).toBe(ICON_EU_COVERAGE);
    expect(url.searchParams.get('format')).toBe('text/plain');
    expect(url.searchParams.getAll('subset')).toEqual(['Lat(44,46)', 'Long(27,30)', 'time("2026-09-14T08:00:00.000Z")']);
    // 48 x 32 cells: small enough to come at the model's own resolution.
    expect(url.searchParams.has('scalesize')).toBe(false);
  });

  it('lets the server resample a whole-country view and stays inside the model domain', () => {
    const url = new URL(coverageUrl({ south: 25, west: 10, north: 55, east: 40 }, T));
    expect(url.searchParams.getAll('subset')[0]).toBe('Lat(29.47,55)');
    const size = /i\((\d+)\),j\((\d+)\)/.exec(url.searchParams.get('scalesize') ?? '');
    expect(size).not.toBeNull();
    expect(Math.max(Number(size?.[1]), Number(size?.[2]))).toBe(ICON_EU_MAX_CELLS);
  });
});

describe('parseCoverageText', () => {
  it('reads the cell edges and the rows, north first', () => {
    const grid = parseCoverageText(ANSWER);
    expect(grid).not.toBeNull();
    expect(grid).toMatchObject({ west: 27.03125, south: 43.96875, east: 27.21875, north: 44.09375, cols: 3, rows: 2 });
    expect(Array.from(grid?.values ?? []).slice(0, 3)).toEqual([0, 0.017578125, 1.2421875]);
    // A negative fill value is no data, not zero rain.
    expect(Number.isNaN(grid?.values[4])).toBe(true);
  });

  it('refuses service exceptions and broken tables', () => {
    expect(parseCoverageText('<ServiceExceptionReport><ServiceException>Dynamic style usage is forbidden</ServiceException></ServiceExceptionReport>')).toBeNull();
    expect(parseCoverageText(ANSWER.replace('0.5 -9999.0 0.0', '0.5 0.0'))).toBeNull();
  });
});

describe('rainWeights', () => {
  it('reads the rain on the hour as the mean of the hour before and the hour after', () => {
    expect(rainWeights(T)).toEqual({ before: T, after: T + HOUR_S, weight: 0.5 });
  });

  it('reads half past as exactly the hour it sits in', () => {
    const { before, weight } = rainWeights(T + HOUR_S / 2);
    expect(before).toBe(T + HOUR_S);
    expect(weight).toBe(0);
  });

  it('lists each needed total once, in order', () => {
    expect(hourEndsFor([T + HOUR_S, T, T + 10 * 60])).toEqual([T, T + HOUR_S, T + 2 * HOUR_S]);
  });
});

describe('iconEuCovers', () => {
  it('covers Romania and not the other side of the Atlantic', () => {
    expect(iconEuCovers({ south: 43, west: 20, north: 48, east: 30 })).toBe(true);
    expect(iconEuCovers({ south: 40, west: -75, north: 42, east: -72 })).toBe(false);
  });
});
