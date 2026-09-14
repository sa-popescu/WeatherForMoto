import { CLOUD_MASK, CLOUD_MASK_SCALE, imageTime, imageUrl, IR_CLOUDS, paintClouds } from './eumetsat';
import { geometryKey, tilesFor, type FieldGeometry } from './mercator';
import type { RadarPicture } from './nowcast';
import { radarTileAt, type RadarFrame } from './radar';

// Loading observed pictures as raw pixels over the canvas: radar frames from
// RainViewer tiles, cloud pictures from the EUMETSAT infrared image and cloud
// mask. Both hosts answer with CORS, so the pixels can be read.

const IMAGE_TIMEOUT_MS = 15_000;

export function abortError(): DOMException {
  return new DOMException('Aborted', 'AbortError');
}

export function loadImage(url: string, signal: AbortSignal): Promise<HTMLImageElement> {
  return new Promise((resolve, reject) => {
    const img = new Image();
    img.crossOrigin = 'anonymous';
    img.decoding = 'async';
    const timer = window.setTimeout(() => finish(new Error('image timed out')), IMAGE_TIMEOUT_MS);
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
    if (signal.aborted) {
      finish(abortError());
      return;
    }
    signal.addEventListener('abort', onAbort, { once: true });
    img.onload = () => finish(null);
    img.onerror = () => finish(new Error('image failed to load'));
    img.src = url;
  });
}

function canvasFor(geometry: FieldGeometry): CanvasRenderingContext2D {
  const canvas = document.createElement('canvas');
  canvas.width = geometry.width;
  canvas.height = geometry.height;
  const ctx = canvas.getContext('2d', { willReadFrequently: true });
  if (!ctx) throw new Error('canvas 2D is not available');
  return ctx;
}

/** One radar frame drawn onto the canvas from its tiles. */
export async function radarPicture(host: string, frame: RadarFrame, geometry: FieldGeometry, signal: AbortSignal): Promise<RadarPicture> {
  const ctx = canvasFor(geometry);
  const slots = tilesFor(geometry);
  const results = await Promise.allSettled(
    slots.map(async (slot) => {
      const img = await loadImage(radarTileAt(host, frame, geometry.z, slot.x, slot.y), signal);
      ctx.drawImage(img, slot.left, slot.top);
    }),
  );
  if (signal.aborted) throw abortError();
  const failed = results.filter((r) => r.status === 'rejected').length;
  if (failed > slots.length / 2) throw new Error(`${failed} of ${slots.length} radar tiles failed`);
  // Throws if a tile came back without CORS: the canvas is then unreadable.
  return { rgba: ctx.getImageData(0, 0, geometry.width, geometry.height).data, timeSec: frame.time };
}

// Cloud pictures are shared by the observed frames and the extrapolation, and
// kept across tab switches. Each is width x height x 4 bytes.
const CLOUD_PICTURES_KEPT = 8;
const cloudPictures = new Map<string, Uint8ClampedArray<ArrayBuffer>>();
const pending = new Map<string, Promise<Uint8ClampedArray<ArrayBuffer>>>();

const cloudKey = (geometry: FieldGeometry, timeSec: number): string => `${geometryKey(geometry)}|${timeSec}`;

export function cachedCloudPicture(geometry: FieldGeometry, timeSec: number): Uint8ClampedArray<ArrayBuffer> | undefined {
  return cloudPictures.get(cloudKey(geometry, timeSec));
}

/** The infrared image for the canvas, turned into clouds (see paintClouds). */
export function cloudPicture(geometry: FieldGeometry, timeSec: number, signal: AbortSignal): Promise<Uint8ClampedArray<ArrayBuffer>> {
  const key = cloudKey(geometry, timeSec);
  const cached = cloudPictures.get(key);
  if (cached) return Promise.resolve(cached);
  let request = pending.get(key);
  if (!request) {
    // Not tied to one caller's signal: another frame may still want the same picture.
    const own = new AbortController();
    request = (async () => {
      try {
        const [ir, mask] = await Promise.all([
          loadImage(imageUrl(IR_CLOUDS, geometry, timeSec), own.signal),
          loadImage(imageUrl(CLOUD_MASK, geometry, imageTime(CLOUD_MASK, timeSec, null), CLOUD_MASK_SCALE), own.signal),
        ]);
        const ctx = canvasFor(geometry);
        ctx.drawImage(ir, 0, 0, geometry.width, geometry.height);
        const pixels = ctx.getImageData(0, 0, geometry.width, geometry.height).data;
        // The small mask is stretched with smoothing, so its coarse cells get soft edges.
        ctx.clearRect(0, 0, geometry.width, geometry.height);
        ctx.imageSmoothingEnabled = true;
        ctx.imageSmoothingQuality = 'high';
        ctx.drawImage(mask, 0, 0, geometry.width, geometry.height);
        paintClouds(pixels, ctx.getImageData(0, 0, geometry.width, geometry.height).data);
        cloudPictures.set(key, pixels);
        while (cloudPictures.size > CLOUD_PICTURES_KEPT) {
          const oldest = cloudPictures.keys().next().value;
          if (oldest === undefined) break;
          cloudPictures.delete(oldest);
        }
        return pixels;
      } finally {
        pending.delete(key);
      }
    })();
    pending.set(key, request);
  }
  return new Promise((resolve, reject) => {
    const onAbort = (): void => reject(abortError());
    if (signal.aborted) {
      onAbort();
      return;
    }
    signal.addEventListener('abort', onAbort, { once: true });
    request.then(
      (pixels) => {
        signal.removeEventListener('abort', onAbort);
        resolve(pixels);
      },
      (err: unknown) => {
        signal.removeEventListener('abort', onAbort);
        reject(err);
      },
    );
  });
}
