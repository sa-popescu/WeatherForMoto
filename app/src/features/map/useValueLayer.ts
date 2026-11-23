import type { MapLibreMap } from 'maplibre-gl';
import { useEffect, useRef } from 'react';
import { geometryKey, type FieldGeometry } from './mercator';
import type { MotionField } from './nowcast';
import type { ValueField } from './rainValues';
import type { ValueFrame } from './valueFrames';
import { ValueLayer } from './valueLayer';
import { addInSlot, setCredit, slotId, type Slot } from './weatherSlots';

// Puts one kind of weather of the moment on screen (valueLayer.ts) in its
// slot among the weather layers: clouds, then rain, then lightning on top. A
// moment whose values are still on their way leaves the last one on screen.
// The few moments around the one on screen are kept, so scrubbing back and
// forth does not work them out again.

/** Worked out fields kept; each is width x height x 2 bytes. */
const KEPT_FIELDS = 6;
/**
 * Two moments at most this far apart are joined along the motion: the radar
 * and the frames moved on, ten minutes apart. Model hours further apart move
 * on their own terms, so they blend in place.
 */
const MAX_FLOW_GAP_S = 20 * 60;

/** How the weather of this layer moves, measured on one canvas (useFieldNowcast.ts). */
export interface LayerMotion {
  /** Changes whenever the motion does. */
  key: string;
  geometry: FieldGeometry;
  field: MotionField;
}

/** Seconds from the frame before to this one, when the move between them can follow the motion; else null. */
export function flowGap(before: { timeSec: number; geometry: FieldGeometry } | null, now: { timeSec: number; geometry: FieldGeometry }): number | null {
  if (!before || geometryKey(before.geometry) !== geometryKey(now.geometry)) return null;
  const gap = now.timeSec - before.timeSec;
  return gap !== 0 && Math.abs(gap) <= MAX_FLOW_GAP_S ? gap : null;
}

interface Options {
  /** The layer's own name, also used for its credit. */
  name: string;
  slot: Slot;
  /** 256 x 2 RGBA, built once per map. */
  palette: () => Uint8Array;
  /** Null when there is nothing of this kind to show at this moment of the band. */
  frame: ValueFrame | null;
  opacity: number;
  /** Credit for what is on screen. */
  attribution: string;
  /** How long one moment takes to move into the next; 0 swaps at once. */
  fadeMs: number;
  /** How this layer's weather moves, so a step along the band carries it along its way; null blends in place. */
  motion?: LayerMotion | null;
}

export function useValueLayer(map: MapLibreMap | null, { name, slot, palette, frame, opacity, attribution, fadeMs, motion = null }: Options): void {
  const layer = useRef<ValueLayer | null>(null);
  const fields = useRef(new Map<string, ValueField>());
  /** The frame on screen, for the step to the next one. */
  const shown = useRef<ValueFrame | null>(null);
  const paletteOf = useRef(palette);
  paletteOf.current = palette;
  const id = slotId(slot, name);

  // One layer per map, added on top of its slot.
  useEffect(() => {
    if (!map) return undefined;
    const created = new ValueLayer(id, paletteOf.current(), fadeMs);
    try {
      addInSlot(map, slot, created);
      layer.current = created;
    } catch (err) {
      console.warn(`[map] the ${name} layer could not be added`, err);
    }
    return () => {
      layer.current = null;
      shown.current = null;
      try {
        if (map.getLayer(id)) map.removeLayer(id);
        setCredit(map, name, null);
      } catch {
        // The map is gone, and its layers with it.
      }
    };
    // The fade length is passed on below, without a new layer.
  }, [map, id, slot, name]);

  useEffect(() => {
    layer.current?.setFadeMs(fadeMs);
  }, [fadeMs]);

  useEffect(() => {
    layer.current?.setMotion(motion?.key ?? null, motion?.geometry ?? null, motion?.field ?? null);
    // The key names the motion.
  }, [map, motion?.key]);

  useEffect(() => {
    const target = layer.current;
    if (!map || !target) return;
    if (!frame) {
      target.clear();
      shown.current = null;
      setCredit(map, name, null);
      return;
    }
    let field = fields.current.get(frame.key) ?? null;
    if (!field) {
      field = frame.values();
      // Still on its way: what is on screen stays until it comes.
      if (!field) return;
      fields.current.set(frame.key, field);
      while (fields.current.size > KEPT_FIELDS) {
        const oldest = fields.current.keys().next().value;
        if (oldest === undefined) break;
        fields.current.delete(oldest);
      }
    }
    const before = shown.current;
    target.show(frame.key, frame.geometry, field, opacity, before && before.key !== frame.key ? flowGap(before, frame) : null);
    shown.current = frame;
    setCredit(map, name, attribution);
  }, [map, name, frame, opacity, attribution]);
}
