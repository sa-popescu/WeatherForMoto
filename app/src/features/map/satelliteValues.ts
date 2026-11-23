import { cloudField } from './cloudValues';
import { CLOUD_MASK, imageTime, imageUrl, IR_CLOUDS, LIGHTNING } from './eumetsat';
import { lightningField } from './lightningValues';
import { geometryKey, type FieldGeometry } from './mercator';
import { canvasFor, loadImage } from './pictures';
import type { ValueField } from './rainValues';

// EUMETSAT's images as values over the canvas: clouds from the infrared
// picture and the cloud mask (cloudValues.ts), lightning from the flash area
// (lightningValues.ts). Images smaller than the canvas (see pixelKm) are
// stretched with smoothing before they are read. Shared by the map and the
// cloud extrapolation, and kept across tab switches.

export type SatelliteKind = 'clouds' | 'lightning';

/** Fields kept, both kinds together: a band of each and a few spare; each is width x height x 2 bytes. */
const KEPT = 32;
const fields = new Map<string, ValueField>();
const pending = new Map<string, Promise<ValueField>>();

const keyOf = (kind: SatelliteKind, geometry: FieldGeometry, timeSec: number): string => `${kind}|${geometryKey(geometry)}|${timeSec}`;

export function cachedSatelliteField(kind: SatelliteKind, geometry: FieldGeometry, timeSec: number): ValueField | undefined {
  return fields.get(keyOf(kind, geometry, timeSec));
}

/** An image stretched over the whole canvas, read back as RGBA. */
function stretched(ctx: CanvasRenderingContext2D, image: HTMLImageElement, geometry: FieldGeometry): Uint8ClampedArray {
  ctx.clearRect(0, 0, geometry.width, geometry.height);
  ctx.drawImage(image, 0, 0, geometry.width, geometry.height);
  // Throws if the image came back without CORS: the canvas is then unreadable.
  return ctx.getImageData(0, 0, geometry.width, geometry.height).data;
}

async function read(kind: SatelliteKind, geometry: FieldGeometry, timeSec: number, signal: AbortSignal): Promise<ValueField> {
  const ctx = canvasFor(geometry);
  if (kind === 'lightning') {
    const flashes = await loadImage(imageUrl(LIGHTNING, geometry, timeSec), signal);
    return lightningField(stretched(ctx, flashes, geometry), geometry.width, geometry.height);
  }
  const [ir, mask] = await Promise.all([
    loadImage(imageUrl(IR_CLOUDS, geometry, timeSec), signal),
    loadImage(imageUrl(CLOUD_MASK, geometry, imageTime(CLOUD_MASK, timeSec, null)), signal),
  ]);
  return cloudField(stretched(ctx, ir, geometry), stretched(ctx, mask, geometry), geometry.width, geometry.height);
}

/**
 * The field of one image time over the canvas: from the cache, or read once
 * however many ask for it. Not tied to anyone's signal: an image asked for
 * while scrubbing is still worth having when it arrives.
 */
export function satelliteField(kind: SatelliteKind, geometry: FieldGeometry, timeSec: number): Promise<ValueField> {
  const key = keyOf(kind, geometry, timeSec);
  const cached = fields.get(key);
  if (cached) return Promise.resolve(cached);
  let request = pending.get(key);
  if (!request) {
    request = read(kind, geometry, timeSec, new AbortController().signal)
      .then((field) => {
        fields.set(key, field);
        while (fields.size > KEPT) {
          const oldest = fields.keys().next().value;
          if (oldest === undefined) break;
          fields.delete(oldest);
        }
        return field;
      })
      .finally(() => pending.delete(key));
    pending.set(key, request);
  }
  return request;
}
