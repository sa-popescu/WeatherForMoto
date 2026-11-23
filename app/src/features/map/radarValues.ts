import { readField } from './fieldReader';
import { geometryKey, tilesFor, type FieldGeometry } from './mercator';
import { RADAR_MAX_NATIVE_ZOOM, radarValueTileAt, type RadarFrame } from './radar';
import type { ValueField } from './rainValues';

// Observed radar frames as rain values over the canvas: RainViewer's tiles
// laid side by side, read back pixel by pixel into rain strength and snow
// (rainValues.ts), and lightly smoothed, in a worker where the browser allows
// (fieldReader.ts). Shared by the map and the extrapolation, and kept across
// tab switches.

/** Fields kept: a band of radar frames and a few spare; each is width x height x 2 bytes. */
const KEPT = 16;
const fields = new Map<string, ValueField>();
const pending = new Map<string, Promise<ValueField>>();

const keyOf = (geometry: FieldGeometry, path: string): string => `${geometryKey(geometry)}|${path}`;

export function cachedRadarValues(geometry: FieldGeometry, path: string): ValueField | undefined {
  return fields.get(keyOf(geometry, path));
}

/** The frame's tiles laid side by side on the canvas's own pixels: nothing is stretched, so every colour comes back as drawn. */
function readFrame(host: string, frame: RadarFrame, geometry: FieldGeometry): Promise<ValueField> {
  if (geometry.z > RADAR_MAX_NATIVE_ZOOM) return Promise.reject(new Error(`radar values are read at zoom ${RADAR_MAX_NATIVE_ZOOM} at most`));
  const tiles = tilesFor(geometry).map((slot) => ({
    url: radarValueTileAt(host, frame, geometry.z, slot.x, slot.y),
    left: slot.left,
    top: slot.top,
  }));
  return readField({ kind: 'radar', width: geometry.width, height: geometry.height, tiles });
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
    request = readFrame(host, frame, geometry)
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
