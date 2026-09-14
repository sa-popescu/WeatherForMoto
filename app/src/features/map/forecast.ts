// Forecast overlay: cloud cover and rain for the hours ahead, sampled from
// Open-Meteo over a grid of points across the visible map and painted as one
// smooth image. Pure helpers only (no Leaflet, no React), so they are testable.

export type ForecastKind = 'cloud' | 'rain';

/**
 * The grid follows the ground, not the screen: a fixed count of points would be
 * 20 km apart when zoomed in and 50 km apart when zoomed out, which is where the
 * field stopped looking like weather and started looking like one soft blob.
 */
export const TARGET_CELL_KM = 20;
/** Never coarser than this, and never more points than one request should carry. */
export const MIN_GRID = 6;
export const MAX_GRID = 12;

/** Degrees of latitude in kilometres; good enough for choosing a grid. */
const KM_PER_DEGREE = 111;

/**
 * Second try when the full grid is refused. Some deployments cap how many
 * coordinates one request may carry, and a coarse field beats no field.
 */
export const FALLBACK_COLS = 4;
export const FALLBACK_ROWS = 4;

/** Hours drawn ahead of the current one. */
export const FORECAST_HOURS = 24;

export const FORECAST_URL = 'https://api.open-meteo.com/v1/forecast';
export const FORECAST_FETCH_TIMEOUT_MS = 12_000;

/** Playback pace; slower than the radar because an hour is a bigger step. */
export const FORECAST_STEP_MS = 700;

export interface GridBounds {
  south: number;
  west: number;
  north: number;
  east: number;
}

export interface GridPoint {
  lat: number;
  lon: number;
}

export interface ForecastData {
  /** Grid size the values were sampled on; the fallback fetch is coarser. */
  cols: number;
  rows: number;
  /** "YYYY-MM-DDTHH:MM" in UTC, one per frame. */
  times: string[];
  /** Cloud cover %, [frame][point] in the same order as the grid. */
  cloud: Array<Array<number | null>>;
  /** Precipitation mm/h, [frame][point]. */
  rain: Array<Array<number | null>>;
}

/** The box in kilometres: width shrinks with latitude, height does not. */
function boxKm(bounds: GridBounds): { widthKm: number; heightKm: number } {
  const midLat = ((bounds.north + bounds.south) / 2) * (Math.PI / 180);
  return {
    widthKm: Math.abs(bounds.east - bounds.west) * KM_PER_DEGREE * Math.cos(midLat),
    heightKm: Math.abs(bounds.north - bounds.south) * KM_PER_DEGREE,
  };
}

function clampGrid(count: number): number {
  if (!Number.isFinite(count)) return MIN_GRID;
  return Math.max(MIN_GRID, Math.min(MAX_GRID, Math.round(count)));
}

/** How many points to sample across this box to land near TARGET_CELL_KM. */
export function gridSizeFor(bounds: GridBounds): { cols: number; rows: number } {
  const { widthKm, heightKm } = boxKm(bounds);
  return {
    cols: clampGrid(widthKm / TARGET_CELL_KM),
    rows: clampGrid(heightKm / TARGET_CELL_KM),
  };
}

/** The side of one cell in kilometres, for the note under the scrubber. */
export function cellKm(bounds: GridBounds, cols: number, rows: number): number {
  const { widthKm, heightKm } = boxKm(bounds);
  return Math.max(1, Math.round((widthKm / cols + heightKm / rows) / 2));
}

/**
 * Grid centres, row-major from north to south. Cell centres rather than corners,
 * so a value covers the area it was sampled in.
 */
export function gridPoints(bounds: GridBounds, cols: number, rows: number): GridPoint[] {
  const latSpan = bounds.north - bounds.south;
  const lonSpan = bounds.east - bounds.west;
  const points: GridPoint[] = [];
  for (let row = 0; row < rows; row += 1) {
    const lat = bounds.north - (latSpan * (row + 0.5)) / rows;
    for (let col = 0; col < cols; col += 1) {
      const lon = bounds.west + (lonSpan * (col + 0.5)) / cols;
      points.push({ lat: round4(lat), lon: round4(lon) });
    }
  }
  return points;
}

function round4(value: number): number {
  return Math.round(value * 10_000) / 10_000;
}

/** One request for every point: Open-Meteo takes coordinate lists. */
export function forecastUrl(points: readonly GridPoint[]): string {
  const params = new URLSearchParams({
    latitude: points.map((p) => p.lat).join(','),
    longitude: points.map((p) => p.lon).join(','),
    hourly: 'cloud_cover,precipitation',
    // One shared clock for the whole grid; the panel shows it in local time.
    timezone: 'UTC',
    forecast_days: '2',
  });
  return `${FORECAST_URL}?${params.toString()}`;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function numbersOf(value: unknown): Array<number | null> {
  if (!Array.isArray(value)) return [];
  return value.map((v) => (typeof v === 'number' && Number.isFinite(v) ? v : null));
}

/** The hour of `nowMs` in UTC, as Open-Meteo writes it. */
export function utcHourKey(nowMs: number): string {
  return new Date(nowMs).toISOString().slice(0, 13);
}

/**
 * Turns the per-point answers into per-hour frames, starting at the current
 * hour. Points that failed or came back short are left as null and painted as
 * a gap rather than as zero.
 */
export function parseForecast(
  raw: unknown,
  grid: { cols: number; rows: number },
  nowMs: number,
  hours: number = FORECAST_HOURS,
): ForecastData | null {
  const pointCount = grid.cols * grid.rows;
  const list = Array.isArray(raw) ? raw : isRecord(raw) ? [raw] : null;
  if (!list || list.length === 0) return null;

  const series = list.map((entry) => {
    const hourly = isRecord(entry) && isRecord(entry.hourly) ? entry.hourly : null;
    return {
      times: hourly && Array.isArray(hourly.time) ? hourly.time.map(String) : [],
      cloud: numbersOf(hourly?.cloud_cover),
      rain: numbersOf(hourly?.precipitation),
    };
  });

  const reference = series.find((s) => s.times.length > 0);
  if (!reference) return null;

  const startKey = utcHourKey(nowMs);
  let start = reference.times.findIndex((t) => t.slice(0, 13) >= startKey);
  if (start < 0) start = 0;
  const end = Math.min(reference.times.length, start + hours);
  if (end <= start) return null;

  const times: string[] = [];
  const cloud: Array<Array<number | null>> = [];
  const rain: Array<Array<number | null>> = [];
  for (let i = start; i < end; i += 1) {
    times.push(reference.times[i]);
    cloud.push(Array.from({ length: pointCount }, (_, p) => series[p]?.cloud[i] ?? null));
    rain.push(Array.from({ length: pointCount }, (_, p) => series[p]?.rain[i] ?? null));
  }
  return { cols: grid.cols, rows: grid.rows, times, cloud, rain };
}

export type Rgba = [number, number, number, number];

const TRANSPARENT: Rgba = [0, 0, 0, 0];

/**
 * A slate veil that thickens as the sky fills in. Mid-grey rather than white,
 * so it reads as cloud over a pale map and still shows over a dark one; a
 * clear sky stays fully transparent.
 */
function cloudColour(cover: number): Rgba {
  if (cover <= 5) return TRANSPARENT;
  const share = Math.min(100, cover) / 100;
  // From a thin haze to a solid deck, never fully opaque: the map stays readable.
  return [148, 163, 184, Math.round(60 + share * 165)];
}

/** One colour per whole percent, built once: this is looked up for every painted pixel. */
const CLOUD_COLOURS: readonly Rgba[] = Array.from({ length: 101 }, (_, cover) => cloudColour(cover));

export function cloudRgba(cover: number | null): Rgba {
  if (cover === null || !Number.isFinite(cover)) return TRANSPARENT;
  return CLOUD_COLOURS[Math.max(0, Math.min(100, Math.round(cover)))];
}

/**
 * Rain in the radar's own colours (RainViewer's Universal Blue, the scheme the
 * tiles are requested in), so a rain area keeps its colour when the scrubber
 * crosses from the observed half of the band into the forecast. Bands are
 * hourly amounts in mm/h, upper bound exclusive; the alpha only rises enough to
 * keep a light shower from looking like a downpour.
 */
const RAIN_BANDS: ReadonlyArray<readonly [number, Rgba]> = [
  [0.3, [136, 221, 238, 170]],
  [0.8, [0, 163, 224, 195]],
  [1.5, [0, 119, 170, 205]],
  [2.5, [0, 85, 136, 210]],
  [5, [255, 238, 0, 215]],
  [7.5, [255, 170, 0, 220]],
  [15, [255, 68, 0, 225]],
  [30, [193, 0, 0, 230]],
];

/** Above the last band: the pink of the radar's extreme end. */
const RAIN_EXTREME: Rgba = [255, 170, 255, 235];

/** Below this there is nothing to paint. */
const RAIN_VISIBLE_MM_H = 0.1;

export function rainRgba(mm: number | null): Rgba {
  if (mm === null || mm < RAIN_VISIBLE_MM_H) return TRANSPARENT;
  for (const [upperBound, color] of RAIN_BANDS) {
    if (mm < upperBound) return color;
  }
  return RAIN_EXTREME;
}

export function colorFor(kind: ForecastKind, value: number | null): Rgba {
  return kind === 'cloud' ? cloudRgba(value) : rainRgba(value);
}

/**
 * Bilinear value at (u, v) in grid coordinates, where whole numbers land on the
 * sampled points. Corners that came back empty simply drop out of the mix, so a
 * hole in the grid does not pull its neighbours towards zero; only a cell with
 * no usable corner at all is a gap.
 */
export function sampleField(
  values: ReadonlyArray<number | null>,
  cols: number,
  rows: number,
  u: number,
  v: number,
): number | null {
  const x0 = Math.max(0, Math.min(cols - 1, Math.floor(u)));
  const y0 = Math.max(0, Math.min(rows - 1, Math.floor(v)));
  const x1 = Math.min(cols - 1, x0 + 1);
  const y1 = Math.min(rows - 1, y0 + 1);
  const fx = Math.max(0, Math.min(1, u - x0));
  const fy = Math.max(0, Math.min(1, v - y0));

  let total = 0;
  let weight = 0;
  const add = (x: number, y: number, w: number): void => {
    const value = values[y * cols + x];
    if (value === null || value === undefined || w <= 0) return;
    total += value * w;
    weight += w;
  };
  add(x0, y0, (1 - fx) * (1 - fy));
  add(x1, y0, fx * (1 - fy));
  add(x0, y1, (1 - fx) * fy);
  add(x1, y1, fx * fy);
  return weight > 0 ? total / weight : null;
}

/**
 * One frame as raw RGBA at the size it will be shown.
 *
 * The values are interpolated first and coloured after, which is the whole
 * point: letting the browser stretch a coloured grid blends the colours
 * themselves, and a light rain fading into transparency over 40 km reads as a
 * halo. Interpolating the millimetres and then looking up the band gives the
 * crisp edges the radar has.
 */
export function fieldImageData(
  kind: ForecastKind,
  values: ReadonlyArray<number | null>,
  cols: number,
  rows: number,
  width: number,
  height: number,
): Uint8ClampedArray {
  const pixels = new Uint8ClampedArray(width * height * 4);
  for (let y = 0; y < height; y += 1) {
    // Pixel centres map onto sample points; the outer half cell keeps the edge value.
    const v = ((y + 0.5) / height) * rows - 0.5;
    for (let x = 0; x < width; x += 1) {
      const u = ((x + 0.5) / width) * cols - 0.5;
      const [r, g, b, a] = colorFor(kind, sampleField(values, cols, rows, u, v));
      const i = (y * width + x) * 4;
      pixels[i] = r;
      pixels[i + 1] = g;
      pixels[i + 2] = b;
      pixels[i + 3] = a;
    }
  }
  return pixels;
}

/**
 * Whether the view moved enough to be worth another request: a third of the
 * span in either direction, or a clear change of zoom (a much wider or
 * narrower box).
 */
export function boundsMovedEnough(previous: GridBounds | null, next: GridBounds): boolean {
  if (!previous) return true;
  const latSpan = Math.max(0.0001, previous.north - previous.south);
  const lonSpan = Math.max(0.0001, previous.east - previous.west);
  const movedLat = Math.abs((next.north + next.south) / 2 - (previous.north + previous.south) / 2);
  const movedLon = Math.abs((next.east + next.west) / 2 - (previous.east + previous.west) / 2);
  if (movedLat > latSpan / 3 || movedLon > lonSpan / 3) return true;
  const nextLatSpan = next.north - next.south;
  return nextLatSpan > latSpan * 1.4 || nextLatSpan < latSpan / 1.4;
}

/** Hours from the current hour to this frame; negative never happens by construction. */
export function frameOffsetHours(time: string, nowMs: number): number {
  const frameMs = Date.parse(`${time}Z`);
  if (Number.isNaN(frameMs)) return 0;
  return Math.round((frameMs - Date.parse(`${utcHourKey(nowMs)}:00:00Z`)) / 3_600_000);
}
