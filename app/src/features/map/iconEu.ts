import type { Box } from './mercator';

// Rain for the hours ahead from ICON-EU, the regional model of the German
// weather service (DWD): hourly precipitation on a 0.0625° grid (about 7 km),
// read as plain numbers from the DWD's open GeoServer (WCS, free to use with
// attribution) and painted in the radar's colours. Pure helpers only.

export const ICON_EU_WCS_URL = 'https://maps.dwd.de/geoserver/dwd/wcs';
export const ICON_EU_COVERAGE = 'dwd__Icon-eu_reg00625_fd_sl_TOTPREC01H';
export const ICON_EU_CELL_DEG = 0.0625;
/** The model's domain (cell edges), from the coverage description. */
export const ICON_EU_DOMAIN: Box = { south: 29.47, west: -23.53, north: 70.53, east: 62.53 };
/**
 * Cells per side at most. A whole-country view is resampled by the server
 * (about 9 km instead of 7) rather than sent at full size: the answer is text.
 */
export const ICON_EU_MAX_CELLS = 256;
export const ICON_EU_FETCH_TIMEOUT_MS = 15_000;
export const HOUR_S = 3600;

export interface RainGrid {
  /** Outer edges of the cells, degrees. */
  west: number;
  south: number;
  east: number;
  north: number;
  cols: number;
  rows: number;
  /** Millimetres in the hour, row-major from north to south; NaN where the model has no value. */
  values: Float32Array;
}

export function iconEuCovers(box: Box): boolean {
  const lat = (box.north + box.south) / 2;
  const lon = (box.east + box.west) / 2;
  return lat > ICON_EU_DOMAIN.south && lat < ICON_EU_DOMAIN.north && lon > ICON_EU_DOMAIN.west && lon < ICON_EU_DOMAIN.east;
}

/** The coverage's time stamp for the hour that ends at `hourEndSec`. */
export function hourIso(hourEndSec: number): string {
  return new Date(hourEndSec * 1000).toISOString();
}

const round3 = (value: number): number => Math.round(value * 1000) / 1000;

/** One hour of ICON-EU rain over the box (clipped to the model domain), as text. */
export function coverageUrl(box: Box, hourEndSec: number): string {
  const south = Math.max(ICON_EU_DOMAIN.south, box.south);
  const north = Math.min(ICON_EU_DOMAIN.north, box.north);
  const west = Math.max(ICON_EU_DOMAIN.west, box.west);
  const east = Math.min(ICON_EU_DOMAIN.east, box.east);
  const params = new URLSearchParams({
    service: 'WCS',
    version: '2.0.1',
    request: 'GetCoverage',
    coverageId: ICON_EU_COVERAGE,
    format: 'text/plain',
  });
  params.append('subset', `Lat(${round3(south)},${round3(north)})`);
  params.append('subset', `Long(${round3(west)},${round3(east)})`);
  params.append('subset', `time("${hourIso(hourEndSec)}")`);
  const cols = (east - west) / ICON_EU_CELL_DEG;
  const rows = (north - south) / ICON_EU_CELL_DEG;
  const scale = Math.min(1, ICON_EU_MAX_CELLS / Math.max(cols, rows));
  if (scale < 1) params.set('scalesize', `i(${Math.max(2, Math.round(cols * scale))}),j(${Math.max(2, Math.round(rows * scale))})`);
  return `${ICON_EU_WCS_URL}?${params.toString()}`;
}

const BOUNDS_RE = /GeneralBounds\[\(\s*(-?[\d.]+),\s*(-?[\d.]+)\),\s*\(\s*(-?[\d.]+),\s*(-?[\d.]+)\)\]/;
const BAND_MARKER = 'Band 0:';

/**
 * Reads GeoServer's text output: a header whose "GeneralBounds" gives the
 * cell edges (longitude first), then "Band 0:" and one line of values per
 * row, north first. Anything else (a service exception) is null.
 */
export function parseCoverageText(text: string): RainGrid | null {
  const bounds = BOUNDS_RE.exec(text);
  const marker = text.indexOf(BAND_MARKER);
  if (!bounds || marker < 0) return null;
  const lines = text
    .slice(marker + BAND_MARKER.length)
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0);
  if (lines.length === 0) return null;
  const cols = lines[0].split(/\s+/).length;
  const values = new Float32Array(cols * lines.length);
  for (let row = 0; row < lines.length; row += 1) {
    const cells = lines[row].split(/\s+/);
    if (cells.length !== cols) return null;
    for (let col = 0; col < cols; col += 1) {
      const value = Number(cells[col]);
      values[row * cols + col] = Number.isFinite(value) && value >= 0 ? value : Number.NaN;
    }
  }
  const [west, south, east, north] = [bounds[1], bounds[2], bounds[3], bounds[4]].map(Number);
  if (!(east > west && north > south)) return null;
  return { west, south, east, north, cols, rows: lines.length, values };
}

/**
 * Which hourly totals describe the rain at time t. A total covers the hour
 * before its time stamp, so it is centred half an hour earlier; the rate at t
 * is read between the two totals whose centres surround it. On the hour this
 * is the mean of the hour before and the hour after.
 */
export function rainWeights(tSec: number): { before: number; after: number; weight: number } {
  const before = Math.floor((tSec + HOUR_S / 2) / HOUR_S) * HOUR_S;
  return { before, after: before + HOUR_S, weight: (tSec - (before - HOUR_S / 2)) / HOUR_S };
}

/** Every hourly total needed to paint these moments, oldest first. */
export function hourEndsFor(times: readonly number[]): number[] {
  const ends = new Set<number>();
  for (const t of times) {
    const { before, after } = rainWeights(t);
    ends.add(before);
    ends.add(after);
  }
  return [...ends].sort((a, b) => a - b);
}
