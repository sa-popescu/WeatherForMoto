import type { CanvasSource, MapLibreMap } from 'maplibre-gl';
import { useCallback, useEffect, useRef } from 'react';
import { Fader } from './fader';
import type { FieldGeometry } from './mercator';
import { addInSlot, removeLayerAndSource, setCredit, slotId, slotLayerOps, type Slot } from './weatherSlots';

// Two canvases placed on the map like images, painted by whoever supplies the
// frame (extrapolated pictures, model fields, satellite clouds). A new frame
// is painted on the canvas not on screen and fades in over the other one
// (fader.ts), so moving along the band never shows an empty map. A frame
// whose data is still on its way leaves the last picture on screen until it
// comes. The few frames around the one on screen are kept, so scrubbing back
// and forth does not repaint them. Each canvas is a static MapLibre canvas
// source: it is read again only when a new frame is painted on it.

/** Pixels backed by a plain ArrayBuffer, the only kind ImageData accepts. */
export type Pixels = Uint8ClampedArray<ArrayBuffer>;

export interface FieldFrame {
  /** Identifies what is painted; the same key is never painted twice. */
  key: string;
  /** Null while the data for this frame is not there yet: the last picture stays meanwhile. */
  paint: () => Pixels | null;
}

/** Painted frames kept; each is width x height x 4 bytes. */
const KEPT_FRAMES = 4;

interface Options {
  /** Null when the layer has nothing to show at this moment of the band: it goes. */
  frame: FieldFrame | null;
  geometry: FieldGeometry | null;
  opacity: number;
  slot: Slot;
  attribution: string;
  /** How long a new frame takes to fade in; 0 swaps at once. */
  fadeMs: number;
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

interface Buffer {
  id: string;
  canvas: HTMLCanvasElement;
  /** The frame painted on it, or null. */
  key: string | null;
  /** Where it sits on the map, as JSON, once its source exists. */
  placedAt: string | null;
}

export type FieldStep =
  | { kind: 'leave' }
  | { kind: 'keep'; buffer: number }
  | { kind: 'reshow'; buffer: number }
  | { kind: 'paint'; buffer: number };

/**
 * What a new frame does to the two canvases: keep the one already showing it,
 * fade back to the other one if it still holds it (scrubbing back a step),
 * paint it on the one not on screen, or, while its pixels are not there yet,
 * leave the screen as it is.
 */
export function fieldStep(keys: readonly [string | null, string | null], shown: number | null, frameKey: string, ready: boolean): FieldStep {
  const holding = keys.findIndex((key) => key === frameKey);
  if (holding >= 0) return holding === shown ? { kind: 'keep', buffer: holding } : { kind: 'reshow', buffer: holding };
  if (!ready) return { kind: 'leave' };
  return { kind: 'paint', buffer: shown === null ? 0 : 1 - shown };
}

export function useFieldLayer(map: MapLibreMap | null, { frame, geometry, opacity, slot, attribution, fadeMs }: Options): void {
  const buffers = useRef<[Buffer, Buffer] | null>(null);
  const painted = useRef(new Map<string, Pixels>());
  const fader = useRef<Fader | null>(null);

  const ids = [slotId(slot, 'field-a'), slotId(slot, 'field-b')] as const;
  const [idA, idB] = ids;

  const removeAll = useCallback(() => {
    if (map) {
      for (const id of [idA, idB]) removeLayerAndSource(map, id, id);
      setCredit(map, slot, null);
    }
    fader.current?.reset();
    for (const buffer of buffers.current ?? []) {
      buffer.key = null;
      buffer.placedAt = null;
    }
  }, [map, idA, idB, slot]);

  // When the layer has gone out of sight (see below), its canvases come off the map.
  const removeWhenHidden = useRef(removeAll);
  removeWhenHidden.current = removeAll;

  useEffect(() => {
    // The satellite clouds sit under the clouds ahead where the band hands over (fader.ts).
    fader.current = map
      ? new Fader(slotLayerOps(map, slot), fadeMs, {
          under: slot === 'clouds',
          onSettled: () => {
            if (fader.current?.current === null) removeWhenHidden.current();
          },
        })
      : null;
    return () => fader.current?.reset();
  }, [map, slot, fadeMs]);

  useEffect(() => {
    const fade = fader.current;
    // Nothing for this layer at this moment of the band: it goes out of sight the way the handover
    // needs, and its canvases come off once it has.
    if (map && fade && !frame && fade.current !== null) {
      fade.hide();
      return;
    }
    if (!map || !fade || !frame || !geometry) {
      if (!fade || ![idA, idB].some((id) => fade.isLeaving(id))) removeAll();
      return;
    }
    buffers.current ??= [
      { id: idA, canvas: document.createElement('canvas'), key: null, placedAt: null },
      { id: idB, canvas: document.createElement('canvas'), key: null, placedAt: null },
    ];
    const pair = buffers.current;

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

    const shown = fade.current === idA ? 0 : fade.current === idB ? 1 : null;
    const step = fieldStep([pair[0].key, pair[1].key], shown, frame.key, pixels !== null);
    if (step.kind === 'leave') return;
    if (step.kind === 'keep' || step.kind === 'reshow') {
      // Same picture (only the opacity slider may have changed), or back to the one just before.
      fade.show(pair[step.buffer].id, opacity);
      setCredit(map, slot, attribution);
      return;
    }

    const buffer = pair[step.buffer];
    const { canvas } = buffer;
    if (canvas.width !== geometry.width || canvas.height !== geometry.height) {
      canvas.width = geometry.width;
      canvas.height = geometry.height;
    }
    canvas.getContext('2d')?.putImageData(new ImageData(pixels as Pixels, geometry.width, geometry.height), 0, 0);
    buffer.key = frame.key;

    const corners = cornersOf(geometry);
    const where = JSON.stringify(corners);
    const source = map.getSource<CanvasSource>(buffer.id);
    if (!source) {
      map.addSource(buffer.id, { type: 'canvas', canvas, coordinates: corners, animate: false });
      addInSlot(map, slot, { id: buffer.id, type: 'raster', source: buffer.id, paint: { 'raster-opacity': 0, 'raster-fade-duration': 0 } });
    } else {
      if (buffer.placedAt !== where) source.setCoordinates(corners);
      // A static canvas is read once; play then pause reads the new pixels once more.
      source.play();
      source.pause();
      map.triggerRepaint();
    }
    buffer.placedAt = where;
    fade.show(buffer.id, opacity);
    setCredit(map, slot, attribution);
  }, [map, frame, geometry, opacity, slot, attribution, idA, idB, removeAll]);

  // Leaving the screen (or the map going away) takes the layers with them.
  useEffect(
    () => () => {
      removeAll();
      painted.current.clear();
    },
    [removeAll],
  );
}
