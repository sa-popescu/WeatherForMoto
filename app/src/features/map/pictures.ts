import { tilesFor, type FieldGeometry } from './mercator';
import type { RadarPicture } from './nowcast';
import { RADAR_MAX_NATIVE_ZOOM, radarTileAt, type RadarFrame } from './radar';

// Loading observed pictures as raw pixels over the canvas: here radar frames
// from RainViewer tiles for the rain ahead on "Acum", and the image loading and
// canvas the map's own readers share (radarValues.ts, satelliteValues.ts). The
// hosts answer with CORS, so the pixels can be read.

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

/** A canvas of the geometry's size; images stretched onto it are smoothed, not drawn as blocks. */
export function canvasFor(geometry: FieldGeometry): CanvasRenderingContext2D {
  const canvas = document.createElement('canvas');
  canvas.width = geometry.width;
  canvas.height = geometry.height;
  const ctx = canvas.getContext('2d', { willReadFrequently: true });
  if (!ctx) throw new Error('canvas 2D is not available');
  ctx.imageSmoothingEnabled = true;
  ctx.imageSmoothingQuality = 'high';
  return ctx;
}

/**
 * One radar frame drawn onto the canvas from its tiles. RainViewer stops at
 * zoom 7, so a deeper canvas gets those tiles stretched.
 */
export async function radarPicture(host: string, frame: RadarFrame, geometry: FieldGeometry, signal: AbortSignal): Promise<RadarPicture> {
  const ctx = canvasFor(geometry);
  const tileZoom = Math.min(geometry.z, RADAR_MAX_NATIVE_ZOOM);
  const slots = tilesFor(geometry, tileZoom);
  const results = await Promise.allSettled(
    slots.map(async (slot) => {
      const img = await loadImage(radarTileAt(host, frame, tileZoom, slot.x, slot.y), signal);
      ctx.drawImage(img, slot.left, slot.top, slot.size, slot.size);
    }),
  );
  if (signal.aborted) throw abortError();
  const failed = results.filter((r) => r.status === 'rejected').length;
  if (failed > slots.length / 2) throw new Error(`${failed} of ${slots.length} radar tiles failed`);
  // Throws if a tile came back without CORS: the canvas is then unreadable.
  return { rgba: ctx.getImageData(0, 0, geometry.width, geometry.height).data, timeSec: frame.time };
}
