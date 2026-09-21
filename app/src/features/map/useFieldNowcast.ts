import { useEffect, useRef, useState } from 'react';
import type { FieldGeometry } from './mercator';
import { alphaLevel, echoLevel, estimateMotion, motionOptions, type MotionField, type RadarPicture } from './nowcast';
import type { MotionReply, MotionRequest, PictureKind } from './nowcastWorker';
import { abortError } from './pictures';

// Loads the last observed pictures over the canvas and measures how they move:
// radar echoes in the rain view, satellite clouds in the cloud view. Runs again
// when a newer picture arrives or the canvas changes; the extrapolated frames
// themselves are painted on demand by the layer.

export type NowcastStatus = 'idle' | 'loading' | 'ready' | 'error';

export interface FieldNowcast {
  status: NowcastStatus;
  /** Identifies the canvas and pictures the result was computed from. */
  key: string | null;
  /** Time of the picture the extrapolation starts from. */
  baseSec: number | null;
  geometry: FieldGeometry | null;
  source: Uint8ClampedArray | null;
  motion: MotionField | null;
}

export const IDLE_NOWCAST: FieldNowcast = { status: 'idle', key: null, baseSec: null, geometry: null, source: null, motion: null };

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

const yieldToBrowser = (): Promise<void> => new Promise((resolve) => window.setTimeout(resolve, 0));

/**
 * Motion for the pictures, in the worker when possible. The pictures are sent
 * as copies, so the newest one stays usable here as the extrapolation source.
 */
export async function measureMotion(
  kind: PictureKind,
  pictures: RadarPicture[],
  geometry: FieldGeometry,
  maxSpeedKmh: number,
  signal: AbortSignal,
): Promise<MotionField> {
  const options = motionOptions(geometry, maxSpeedKmh);
  const target = motionWorker();
  if (!target) {
    await yieldToBrowser();
    return estimateMotion(pictures, geometry.width, geometry.height, options, kind === 'cloud' ? alphaLevel : echoLevel);
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
      // A worker that cannot start (old browser, blocked script) is not tried again.
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
    const request: MotionRequest = { id, kind, pictures: copies, width: geometry.width, height: geometry.height, options };
    target.postMessage(request, copies.map((p) => p.rgba.buffer));
  });
}

interface Options {
  /** null while there is nothing to compute (layer off, no pictures yet). */
  key: string | null;
  kind: PictureKind;
  geometry: FieldGeometry | null;
  maxSpeedKmh: number;
  /** Loads the pictures for the key, oldest first. */
  load: (signal: AbortSignal) => Promise<RadarPicture[]>;
}

export function useFieldNowcast({ key, kind, geometry, maxSpeedKmh, load }: Options): FieldNowcast {
  const [state, setState] = useState<FieldNowcast>(IDLE_NOWCAST);
  const inputs = useRef({ kind, geometry, maxSpeedKmh, load });
  inputs.current = { kind, geometry, maxSpeedKmh, load };

  useEffect(() => {
    if (!key) return undefined;
    const { kind: pictureKind, geometry: canvas, maxSpeedKmh: speed, load: loadPictures } = inputs.current;
    if (!canvas) return undefined;
    const controller = new AbortController();
    setState((current) => (current.key === key ? current : { ...current, status: 'loading' }));

    void (async () => {
      try {
        const pictures = await loadPictures(controller.signal);
        if (controller.signal.aborted) return;
        if (pictures.length < 2) throw new Error('not enough pictures to measure motion');
        const motion = await measureMotion(pictureKind, pictures, canvas, speed, controller.signal);
        if (controller.signal.aborted) return;
        const base = pictures[pictures.length - 1];
        setState({ status: 'ready', key, baseSec: base.timeSec, geometry: canvas, source: base.rgba, motion });
      } catch (err) {
        if (controller.signal.aborted) return;
        console.warn(`[map] ${pictureKind} extrapolation unavailable`, err);
        setState({ ...IDLE_NOWCAST, status: 'error', key });
      }
    })();

    return () => controller.abort();
  }, [key]);

  return key ? state : IDLE_NOWCAST;
}
