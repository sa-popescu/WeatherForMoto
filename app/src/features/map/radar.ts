import type { Lang } from '../../lib/i18n';

// RainViewer radar: pure helpers (no Leaflet, no React) so they can be unit-tested.

export const RADAR_INDEX_URL = 'https://api.rainviewer.com/public/weather-maps.json';
export const RADAR_REFRESH_MS = 10 * 60_000;
export const RADAR_FETCH_TIMEOUT_MS = 10_000;

/**
 * The public tiles stop at zoom 7: deeper levels come back as a "Zoom Level
 * Not Supported" picture, so Leaflet upscales the zoom 7 tiles instead.
 */
export const RADAR_MAX_NATIVE_ZOOM = 7;

/** 256 px tiles, colour scheme 2 (Universal Blue), smoothed, snow shown. */
const TILE_SUFFIX = '/256/{z}/{x}/{y}/2/1_1.png';

/** Playback pace, and the longer pause on the "now" frame and on the last one. */
export const FRAME_STEP_MS = 550;
export const FRAME_HOLD_MS = 1600;

export interface RadarFrame {
  /** Unix time in seconds (UTC). */
  time: number;
  path: string;
  /** true for RainViewer's short-term forecast frames. */
  nowcast: boolean;
}

export interface RadarIndex {
  host: string;
  frames: RadarFrame[];
}

// The index builds tile URLs, so host and paths are checked before use.
const HOST_RE = /^https:\/\/[a-z0-9.-]+$/i;
const PATH_RE = /^\/[A-Za-z0-9/_-]+$/;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function framesFrom(list: unknown, nowcast: boolean): RadarFrame[] {
  if (!Array.isArray(list)) return [];
  const frames: RadarFrame[] = [];
  for (const item of list) {
    if (!isRecord(item)) continue;
    const { time, path } = item;
    if (typeof time === 'number' && Number.isFinite(time) && typeof path === 'string' && PATH_RE.test(path)) {
      frames.push({ time, path, nowcast });
    }
  }
  return frames;
}

/** Past + nowcast frames of weather-maps.json in time order, or null when there is nothing usable. */
export function parseRadarIndex(raw: unknown): RadarIndex | null {
  if (!isRecord(raw) || typeof raw.host !== 'string' || !HOST_RE.test(raw.host)) return null;
  const radar = isRecord(raw.radar) ? raw.radar : {};
  // Past frames go first, so the stable sort keeps an observed frame over a forecast of the same time.
  const sorted = [...framesFrom(radar.past, false), ...framesFrom(radar.nowcast, true)].sort((a, b) => a.time - b.time);
  const frames = sorted.filter((frame, i) => i === 0 || frame.time !== sorted[i - 1].time);
  return frames.length > 0 ? { host: raw.host, frames } : null;
}

export function radarTileUrl(host: string, frame: RadarFrame): string {
  return `${host}${frame.path}${TILE_SUFFIX}`;
}

/** Index of the frame closest to `targetSec` (ties go to the earlier frame), -1 for an empty list. */
export function closestFrameIndex(frames: readonly RadarFrame[], targetSec: number): number {
  let best = -1;
  let bestGap = Infinity;
  frames.forEach((frame, i) => {
    const gap = Math.abs(frame.time - targetSec);
    if (gap < bestGap) {
      best = i;
      bestGap = gap;
    }
  });
  return best;
}

/**
 * First frame to show: the one closest to now, among observed frames when
 * there are any. A forecast is the start only when nothing was observed.
 */
export function startFrameIndex(frames: readonly RadarFrame[], nowSec: number): number {
  const observed = frames.filter((frame) => !frame.nowcast);
  if (observed.length === 0) return closestFrameIndex(frames, nowSec);
  return frames.indexOf(observed[closestFrameIndex(observed, nowSec)]);
}

/** Index of the newest observed frame, -1 when there is none. */
export function latestObservedIndex(frames: readonly RadarFrame[]): number {
  for (let i = frames.length - 1; i >= 0; i -= 1) if (!frames[i].nowcast) return i;
  return -1;
}

export function nextFrameIndex(index: number, count: number): number {
  return count > 0 ? (index + 1) % count : -1;
}

/** How long frame `index` stays on screen during playback. */
export function frameDelayMs(index: number, count: number, holdIndex: number): number {
  return index === holdIndex || index === count - 1 ? FRAME_HOLD_MS : FRAME_STEP_MS;
}

/** Whole minutes from now to the frame: negative in the past, positive for forecasts. */
export function frameOffsetMinutes(frame: RadarFrame, nowSec: number): number {
  return Math.round((frame.time - nowSec) / 60);
}

const LOCALE: Record<Lang, string> = { ro: 'ro-RO', en: 'en-GB' };

/** Clock time of a frame ("14:20"), in the device timezone unless one is given. */
export function frameClock(timeSec: number, lang: Lang, timeZone?: string): string {
  return new Date(timeSec * 1000).toLocaleTimeString(LOCALE[lang], { hour: '2-digit', minute: '2-digit', hourCycle: 'h23', timeZone });
}

/** Frames that get a time label under the scrubber: the last one and every `step`-th before it. */
export function labelledTicks(count: number, step: number): number[] {
  const out: number[] = [];
  for (let i = 0; i < count; i += 1) if ((count - 1 - i) % step === 0) out.push(i);
  return out;
}

export type LegendKey = 'legendLight' | 'legendModerate' | 'legendHeavy' | 'legendExtreme';

/**
 * Universal Blue colours from RainViewer's colour table, grouped by rain
 * intensity: light under 30 dBZ (under about 2.5 mm/h), moderate 30-39, heavy
 * 40-49, extreme from 50 (cloudbursts, hail). They are the colours painted in
 * the tiles themselves, so they stay the same in both themes.
 */
export const RADAR_LEGEND: ReadonlyArray<{ key: LegendKey; colors: readonly string[] }> = [
  { key: 'legendLight', colors: ['#88ddee', '#00a3e0', '#0077aa'] },
  { key: 'legendModerate', colors: ['#005588', '#ffee00'] },
  { key: 'legendHeavy', colors: ['#ffaa00', '#ff4400'] },
  { key: 'legendExtreme', colors: ['#c10000', '#ffaaff', '#ffffff'] },
];

export function legendGradient(colors: readonly string[]): string {
  return colors.length === 1 ? colors[0] : `linear-gradient(90deg, ${colors.join(', ')})`;
}
