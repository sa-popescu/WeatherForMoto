import type { CanvasSource, MapLibreMap } from 'maplibre-gl';
import { useEffect, useRef } from 'react';
import type { FieldGeometry } from './mercator';
import { addInSlot, removeLayerAndSource, setCredit, slotId, type Slot } from './weatherSlots';

// One canvas placed on the map like an image, painted by whoever supplies the
// frame (extrapolated pictures, model fields, satellite clouds). The few frames
// around the one on screen are kept, so scrubbing back and forth does not
// repaint them. The canvas is a static MapLibre canvas source: it is read
// again only when a new frame is painted on it.

/** Pixels backed by a plain ArrayBuffer, the only kind ImageData accepts. */
export type Pixels = Uint8ClampedArray<ArrayBuffer>;

export interface FieldFrame {
  /** Identifies what is painted; the same key is never painted twice. */
  key: string;
  /** Null while the data for this frame is not there yet. */
  paint: () => Pixels | null;
}

/** Painted frames kept; each is width x height x 4 bytes. */
const KEPT_FRAMES = 4;

interface Options {
  frame: FieldFrame | null;
  geometry: FieldGeometry | null;
  opacity: number;
  slot: Slot;
  attribution: string;
}

type Corners = [[number, number], [number, number], [number, number], [number, number]];

/** The canvas corners, clockwise from the top left, as MapLibre wants them. */
function cornersOf(geometry: FieldGeometry): Corners {
  const { north, south, west, east } = geometry.bounds;
  return [
    [west, north],
    [east, north],
    [east, south],
    [west, south],
  ];
}

export function useFieldLayer(map: MapLibreMap | null, { frame, geometry, opacity, slot, attribution }: Options): void {
  const canvas = useRef<HTMLCanvasElement | null>(null);
  const painted = useRef(new Map<string, Pixels>());
  const onCanvas = useRef<string | null>(null);
  const placedAt = useRef<string | null>(null);
  const id = slotId(slot, 'field');

  useEffect(() => {
    if (!map || !frame || !geometry) {
      if (map) {
        removeLayerAndSource(map, id, id);
        setCredit(map, slot, null);
      }
      onCanvas.current = null;
      placedAt.current = null;
      return;
    }

    let pixels = painted.current.get(frame.key) ?? null;
    if (!pixels) {
      pixels = frame.paint();
      if (pixels) {
        painted.current.set(frame.key, pixels);
        while (painted.current.size > KEPT_FRAMES) {
          const oldest = painted.current.keys().next().value;
          if (oldest === undefined) break;
          painted.current.delete(oldest);
        }
      }
    }

    const element = canvas.current ?? document.createElement('canvas');
    canvas.current = element;
    let repainted = false;
    if (pixels && onCanvas.current !== frame.key) {
      if (element.width !== geometry.width || element.height !== geometry.height) {
        element.width = geometry.width;
        element.height = geometry.height;
      }
      element.getContext('2d')?.putImageData(new ImageData(pixels, geometry.width, geometry.height), 0, 0);
      onCanvas.current = frame.key;
      repainted = true;
    }

    const corners = cornersOf(geometry);
    const where = JSON.stringify(corners);
    const source = map.getSource<CanvasSource>(id);
    if (!source) {
      // A canvas source needs a canvas with a size, even before the first frame.
      if (element.width === 0 || element.height === 0) {
        element.width = geometry.width;
        element.height = geometry.height;
      }
      map.addSource(id, { type: 'canvas', canvas: element, coordinates: corners, animate: false });
      addInSlot(map, slot, { id, type: 'raster', source: id, paint: { 'raster-opacity': 0, 'raster-fade-duration': 0 } });
      placedAt.current = where;
    } else {
      if (placedAt.current !== where) {
        source.setCoordinates(corners);
        placedAt.current = where;
      }
      if (repainted) {
        // A static canvas is read once; play then pause reads the new pixels once more.
        source.play();
        source.pause();
        map.triggerRepaint();
      }
    }
    // Nothing to show yet: keep the layer, hide it.
    map.setPaintProperty(id, 'raster-opacity', pixels ? opacity : 0);
    setCredit(map, slot, pixels ? attribution : null);
  }, [map, frame, geometry, opacity, slot, attribution, id]);

  // Leaving the screen (or the map going away) takes the layer with it.
  useEffect(
    () => () => {
      if (map) {
        removeLayerAndSource(map, id, id);
        setCredit(map, slot, null);
      }
      painted.current.clear();
      onCanvas.current = null;
      placedAt.current = null;
    },
    [map, id, slot],
  );
}
