import { geometryKey, tilesFor, type FieldGeometry } from './mercator';
import { canvasFor, loadImage } from './pictures';
import { RADAR_MAX_NATIVE_ZOOM, radarValueTileAt, type RadarFrame } from './radar';
import { CHANNELS, decodePixels, smoothRadarField, type ValueField } from './rainValues';

// Observed radar frames as rain values over the canvas: RainViewer's tiles
// laid side by side, read back pixel by pixel into rain strength and snow
// (rainValues.ts), and lightly smoothed. Shared by the map and the
// extrapolation, and kept across tab switches.

/** Fields kept: a band of radar frames and a few spare; each is width x height x 2 bytes. */
const KEPT = 16;
const fields = new Map<string, ValueField>();
const pending = new Map<string, Promise<ValueField>>();

const keyOf = (geometry: FieldGeometry, path: string): string => `${geometryKey(geometry)}|${path}`;

export function cachedRadarValues(geometry: FieldGeometry, path: string): ValueField | undefined {
  return fields.get(keyOf(geometry, path));
}

async function readFrame(host: string, frame: RadarFrame, geometry: FieldGeometry, signal: AbortSignal): Promise<ValueField> {
  // The canvas sits on the tiles' own pixels: nothing is stretched, so every colour comes back as drawn.
  if (geometry.z > RADAR_MAX_NATIVE_ZOOM) throw new Error(`radar values are read at zoom ${RADAR_MAX_NATIVE_ZOOM} at most`);
  const ctx = canvasFor(geometry);
  const slots = tilesFor(geometry);
  const results = await Promise.allSettled(
    slots.map(async (slot) => {
      const img = await loadImage(radarValueTileAt(host, frame, geometry.z, slot.x, slot.y), signal);
      ctx.drawImage(img, slot.left, slot.top);
    }),
  );
  const failed = results.filter((r) => r.status === 'rejected').length;
  if (failed > slots.length / 2) throw new Error(`${failed} of ${slots.length} radar tiles failed`);
  // Throws if a tile came back without CORS: the canvas is then unreadable.
  const rgba = ctx.getImageData(0, 0, geometry.width, geometry.height).data;
  const field: ValueField = new Uint8ClampedArray(geometry.width * geometry.height * CHANNELS);
  decodePixels(rgba, field);
  smoothRadarField(field, geometry.width, geometry.height);
  return field;
}

/**
 * The values of one radar frame over the canvas: from the cache, or read once
 * however many ask for it. Not tied to anyone's signal: a frame asked for
 * while scrubbing is still worth having when it arrives.
 */
export function radarValues(host: string, frame: RadarFrame, geometry: FieldGeometry): Promise<ValueField> {
  const key = keyOf(geometry, frame.path);
  const cached = fields.get(key);
  if (cached) return Promise.resolve(cached);
  let request = pending.get(key);
  if (!request) {
    request = readFrame(host, frame, geometry, new AbortController().signal)
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
