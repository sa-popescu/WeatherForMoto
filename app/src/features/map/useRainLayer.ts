import type { MapLibreMap } from 'maplibre-gl';
import { useEffect, useRef } from 'react';
import { RainLayer } from './rainLayer';
import type { RainFrame } from './rainFrames';
import type { RainField } from './rainValues';
import { addInSlot, setCredit, slotId } from './weatherSlots';

// Puts the rain of the moment on screen (rainLayer.ts), in the radar's place
// among the weather layers: under lightning, over the satellite clouds. A
// moment whose values are still on their way leaves the last one on screen.
// The few moments around the one on screen are kept, so scrubbing back and
// forth does not work them out again.

/** Worked out fields kept; each is width x height x 2 bytes. */
const KEPT_FIELDS = 6;

interface Options {
  /** Null when there is no rain to show at this moment of the band. */
  frame: RainFrame | null;
  opacity: number;
  /** Credit for what is on screen. */
  attribution: string;
  /** How long one moment takes to move into the next; 0 swaps at once. */
  fadeMs: number;
}

const LAYER_ID = slotId('radar', 'rain');

export function useRainLayer(map: MapLibreMap | null, { frame, opacity, attribution, fadeMs }: Options): void {
  const layer = useRef<RainLayer | null>(null);
  const fields = useRef(new Map<string, RainField>());

  // One layer per map, added on top of the radar's slot.
  useEffect(() => {
    if (!map) return undefined;
    const created = new RainLayer(LAYER_ID, fadeMs);
    try {
      addInSlot(map, 'radar', created);
      layer.current = created;
    } catch (err) {
      console.warn('[map] the rain layer could not be added', err);
    }
    return () => {
      layer.current = null;
      try {
        if (map.getLayer(LAYER_ID)) map.removeLayer(LAYER_ID);
        setCredit(map, 'radar', null);
      } catch {
        // The map is gone, and its layers with it.
      }
    };
    // The fade length is passed on below, without a new layer.
  }, [map]);

  useEffect(() => {
    layer.current?.setFadeMs(fadeMs);
  }, [fadeMs]);

  useEffect(() => {
    const target = layer.current;
    if (!map || !target) return;
    if (!frame) {
      target.clear();
      setCredit(map, 'radar', null);
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
    target.show(frame.key, frame.geometry, field, opacity);
    setCredit(map, 'radar', attribution);
  }, [map, frame, opacity, attribution]);
}
