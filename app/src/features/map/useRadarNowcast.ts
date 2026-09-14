import { useEffect, useRef, useState } from 'react';
import { geometryKey, tilesFor, type FieldGeometry } from './mercator';
import { estimateMotion, motionOptions, type MotionField, type RadarPicture } from './nowcast';
import type { MotionReply, MotionRequest } from './nowcastWorker';
import { radarTileAt, type RadarFrame } from './radar';

// Reads the last radar pictures over the canvas and measures how the rain is
// moving. Runs again when a new radar frame arrives or the canvas changes;
// the frames themselves are painted on demand by the rain layer.

/** Pictures used for the motion: three frames give two intervals to agree on. */
const PICTURES = 3;
const TILE_TIMEOUT_MS = 12_000;

export type NowcastStatus = 'idle' | 'loading' | 'ready' | 'error';

export interface RadarNowcast {
  status: NowcastStatus;
  /** Identifies the canvas and pictures the result was computed from. */
  key: string | null;
  /** Time of the picture the extrapolation starts from. */
  baseSec: number | null;
  geometry: FieldGeometry | null;
  source: Uint8ClampedArray | null;
  motion: MotionField | null;
}

const IDLE: RadarNowcast = { status: 'idle', key: null, baseSec: null, geometry: null, source: null, motion: null };

function abortError(): DOMException {
  return new DOMException('Aborted', 'AbortError');
}

function loadTile(url: string, signal: AbortSignal): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.crossOrigin = 'anonymous';
    img.decoding = 'async';
    const timer = window.setTimeout(() => finish(new Error('radar tile timed out')), TILE_TIMEOUT_MS);
    const onAbort = (): void => finish(abortError());
    function finish(error: Error | null): void {
      window.clearTimeout(timer);
      signal.removeEventListener('abort', onAbort);
      img.onload = null;
      img.onerror = null;
      if (error) {
        img.src = '';
        reject(error);
      } else {
        resolve(img);
      }
    }
    signal.addEventListener('abort', onAbort, { once: true });
    img.onload = () => finish(null);
    img.onerror = () => finish(new Error('radar tile failed'));
    img.src = url;
  });
}

/** One radar frame drawn onto a canvas of the geometry, as raw pixels. */
async function pictureOf(host: string, frame: RadarFrame, geometry: FieldGeometry, signal: AbortSignal): Promise<RadarPicture> {
  const canvas = document.createElement('canvas');
  canvas.width = geometry.width;
  canvas.height = geometry.height;
  const ctx = canvas.getContext('2d', { willReadFrequently: true });
  if (!ctx) throw new Error('canvas 2D is not available');
  const slots = tilesFor(geometry);
  const results = await Promise.allSettled(
    slots.map(async (slot) => {
      const img = await loadTile(radarTileAt(host, frame, geometry.z, slot.x, slot.y), signal);
      ctx.drawImage(img, slot.left, slot.top);
    }),
  );
  if (signal.aborted) throw abortError();
  const failed = results.filter((r) => r.status === 'rejected').length;
  if (failed > slots.length / 2) throw new Error(`${failed} of ${slots.length} radar tiles failed`);
  // Throws if a tile came back without CORS: the canvas is then unreadable.
  return { rgba: ctx.getImageData(0, 0, geometry.width, geometry.height).data, timeSec: frame.time };
}

const yieldToBrowser = (): Promise<void> => new Promise((resolve) => window.setTimeout(resolve, 0));

let worker: Worker | null | undefined;
let nextRequest = 0;

/** The shared motion worker, or null where workers are unavailable. */
function motionWorker(): Worker | null {
  if (worker !== undefined) return worker;
  try {
    worker = new Worker(new URL('./nowcastWorker.ts', import.meta.url), { type: 'module' });
  } catch (err) {
    console.warn('[map] motion worker unavailable, measuring on the main thread', err);
    worker = null;
  }
  return worker;
}

/**
 * Motion for the pictures, in the worker when possible. The pictures are sent
 * as copies, so the newest one stays usable here as the extrapolation source.
 */
async function measureMotion(pictures: RadarPicture[], geometry: FieldGeometry, signal: AbortSignal): Promise<MotionField> {
  const options = motionOptions(geometry);
  const target = motionWorker();
  if (!target) {
    await yieldToBrowser();
    return estimateMotion(pictures, geometry.width, geometry.height, options);
  }
  const id = (nextRequest += 1);
  const copies = pictures.map((p) => ({ rgba: p.rgba.slice(), timeSec: p.timeSec }));
  return new Promise<MotionField>((resolve, reject) => {
    const onMessage = (event: MessageEvent<MotionReply>): void => {
      if (event.data.id !== id) return;
      cleanup();
      if ('motion' in event.data) resolve(event.data.motion);
      else reject(new Error(event.data.error));
    };
    const onError = (event: ErrorEvent): void => {
      cleanup();
      // A worker that cannot start (old browser, blocked script) is not retried.
      worker = null;
      reject(new Error(event.message || 'motion worker failed'));
    };
    const onAbort = (): void => {
      cleanup();
      reject(abortError());
    };
    function cleanup(): void {
      target?.removeEventListener('message', onMessage);
      target?.removeEventListener('error', onError);
      signal.removeEventListener('abort', onAbort);
    }
    target.addEventListener('message', onMessage);
    target.addEventListener('error', onError);
    signal.addEventListener('abort', onAbort, { once: true });
    const request: MotionRequest = { id, pictures: copies, width: geometry.width, height: geometry.height, options };
    target.postMessage(request, copies.map((p) => p.rgba.buffer));
  });
}

interface Options {
  enabled: boolean;
  host: string | null;
  /** Observed frames only, oldest first. */
  frames: readonly RadarFrame[];
  geometry: FieldGeometry | null;
}

export function useRadarNowcast({ enabled, host, frames, geometry }: Options): RadarNowcast {
  const [state, setState] = useState<RadarNowcast>(IDLE);
  const latest = frames.slice(-PICTURES);
  const key = enabled && host && geometry && latest.length >= 2 ? `${geometryKey(geometry)}|${latest.map((f) => f.path).join(',')}` : null;
  const inputs = useRef({ host, latest, geometry });
  inputs.current = { host, latest, geometry };

  useEffect(() => {
    if (!key) return undefined;
    const { host: tileHost, latest: pictures, geometry: canvas } = inputs.current;
    if (!tileHost || !canvas) return undefined;
    const controller = new AbortController();
    setState((current) => (current.key === key ? current : { ...current, status: 'loading' }));

    void (async () => {
      try {
        const loaded = await Promise.all(pictures.map((frame) => pictureOf(tileHost, frame, canvas, controller.signal)));
        if (controller.signal.aborted) return;
        const motion = await measureMotion(loaded, canvas, controller.signal);
        if (controller.signal.aborted) return;
        const base = loaded[loaded.length - 1];
        setState({ status: 'ready', key, baseSec: base.timeSec, geometry: canvas, source: base.rgba, motion });
      } catch (err) {
        if (controller.signal.aborted) return;
        console.warn('[map] radar extrapolation unavailable', err);
        setState({ ...IDLE, status: 'error', key });
      }
    })();

    return () => controller.abort();
  }, [key]);

  return enabled ? state : IDLE;
}
