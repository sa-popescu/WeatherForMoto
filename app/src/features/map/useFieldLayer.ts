import * as L from 'leaflet';
import { useEffect, useRef } from 'react';
import type { FieldGeometry } from './mercator';

// One canvas placed on the map like an image, painted by whoever supplies the
// frame (extrapolated pictures, model fields, satellite clouds). The few frames
// around the one on screen are kept, so scrubbing back and forth does not
// repaint them.

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
  pane: string;
  attribution: string;
}

export function useFieldLayer(map: L.Map | null, { frame, geometry, opacity, pane, attribution }: Options): void {
  const overlay = useRef<L.SVGOverlay | null>(null);
  const canvas = useRef<HTMLCanvasElement | null>(null);
  const painted = useRef(new Map<string, Pixels>());
  const onCanvas = useRef<string | null>(null);

  useEffect(() => {
    if (!map || !frame || !geometry) {
      overlay.current?.remove();
      overlay.current = null;
      onCanvas.current = null;
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
    const bounds = L.latLngBounds([geometry.bounds.south, geometry.bounds.west], [geometry.bounds.north, geometry.bounds.east]);

    if (pixels && onCanvas.current !== frame.key) {
      if (element.width !== geometry.width || element.height !== geometry.height) {
        element.width = geometry.width;
        element.height = geometry.height;
      }
      element.getContext('2d')?.putImageData(new ImageData(pixels, geometry.width, geometry.height), 0, 0);
      onCanvas.current = frame.key;
    }

    // Nothing to show yet: keep the layer, hide it.
    const shownOpacity = pixels ? opacity : 0;
    if (!overlay.current) {
      // SVGOverlay places any element it is given; Leaflet has no canvas overlay of its own.
      overlay.current = L.svgOverlay(element as unknown as SVGElement, bounds, {
        opacity: shownOpacity,
        interactive: false,
        className: 'map-forecast-overlay',
        pane,
        attribution,
      }).addTo(map);
    } else {
      overlay.current.setBounds(bounds);
      overlay.current.setOpacity(shownOpacity);
    }
  }, [map, frame, geometry, opacity, pane, attribution]);

  // Leaving the screen (or the map going away) takes the overlay with it.
  useEffect(
    () => () => {
      overlay.current?.remove();
      overlay.current = null;
      painted.current.clear();
    },
    [],
  );
}
