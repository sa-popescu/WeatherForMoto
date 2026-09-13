// Forecast overlay: cloud cover and rain for the hours ahead, sampled from
// Open-Meteo over a grid of points across the visible map and painted as one
// smooth image. Pure helpers only (no Leaflet, no React), so they are testable.

export type ForecastKind = 'cloud' | 'rain';

/** Points across the view. 9 x 9 is one request and still shows a front moving. */
export const GRID_COLS = 9;
export const GRID_ROWS = 9;

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
  /** "YYYY-MM-DDTHH:MM" in UTC, one per frame. */
  times: string[];
  /** Cloud cover %, [frame][point] in the same order as the grid. */
  cloud: Array<Array<number | null>>;
  /** Precipitation mm/h, [frame][point]. */
  rain: Array<Array<number | null>>;
}

/**
 * Grid centres, row-major from north to south. Cell centres rather than corners,
 * so a value covers the area it was sampled in.
 */
export function gridPoints(bounds: GridBounds, cols: number = GRID_COLS, rows: number = GRID_ROWS): GridPoint[] {
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
export function parseForecast(raw: unknown, pointCount: number, nowMs: number, hours: number = FORECAST_HOURS): ForecastData | null {
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
  return { times, cloud, rain };
}

export type Rgba = [number, number, number, number];

const TRANSPARENT: Rgba = [0, 0, 0, 0];

/** Grey veil, opaque where the sky is covered. Clear sky stays invisible. */
export function cloudRgba(cover: number | null): Rgba {
  if (cover === null || cover <= 10) return TRANSPARENT;
  const share = Math.min(100, cover) / 100;
  return [226, 232, 240, Math.round(share * 210)];
}

/** Rain by intensity, the same reading as the score: traces, light, moderate, heavy. */
export function rainRgba(mm: number | null): Rgba {
  if (mm === null || mm < 0.1) return TRANSPARENT;
  if (mm < 0.5) return [96, 165, 250, 110];
  if (mm < 2.5) return [37, 99, 235, 160];
  if (mm < 7.5) return [217, 119, 6, 190];
  return [220, 38, 38, 215];
}

export function colorFor(kind: ForecastKind, value: number | null): Rgba {
  return kind === 'cloud' ? cloudRgba(value) : rainRgba(value);
}

/** One frame as raw RGBA, one pixel per grid cell; the browser smooths it when scaled. */
export function frameImageData(
  kind: ForecastKind,
  values: ReadonlyArray<number | null>,
  cols: number = GRID_COLS,
  rows: number = GRID_ROWS,
): Uint8ClampedArray {
  const pixels = new Uint8ClampedArray(cols * rows * 4);
  for (let i = 0; i < cols * rows; i += 1) {
    const [r, g, b, a] = colorFor(kind, values[i] ?? null);
    pixels[i * 4] = r;
    pixels[i * 4 + 1] = g;
    pixels[i * 4 + 2] = b;
    pixels[i * 4 + 3] = a;
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
