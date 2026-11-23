import type { MapLibreMap, MapSourceDataEvent } from 'maplibre-gl';
import { useCallback, useEffect, useRef, useState } from 'react';
import { EUMETSAT_ATTRIBUTION, EUMETVIEW_WMS, wmsTime, type SatelliteProduct } from './eumetsat';
import { Fader } from './fader';
import { addInSlot, removeLayerAndSource, slotId, slotLayerOps, type Slot } from './weatherSlots';

// A time-stamped EUMETSAT image layer (lightning) on the map. Like the radar,
// only the image on screen and the next one exist as layers: the next one
// loads invisibly, and while a new one loads the last complete one stays. A
// new image fades in over the old one.

/** Tiles of 512 px: a quarter of the requests of 256 px tiles for the same view. */
const TILE_SIZE = 512;

interface Options {
  product: SatelliteProduct;
  /** Image on screen, unix seconds; null hides the layer. */
  time: number | null;
  /** Image likely to come next, loaded in advance. */
  next: number | null;
  opacity: number;
  slot: Slot;
  /** How long a new image takes to fade in; 0 swaps at once. */
  fadeMs: number;
}

/** A WMS GetMap for one map tile; the map fills in the tile's box in Web Mercator metres. */
export function wmsTileUrl(product: SatelliteProduct, time: number): string {
  const params = new URLSearchParams({
    SERVICE: 'WMS',
    REQUEST: 'GetMap',
    VERSION: '1.3.0',
    LAYERS: product.layer,
    STYLES: product.style,
    FORMAT: 'image/png',
    TRANSPARENT: 'true',
    CRS: 'EPSG:3857',
    WIDTH: String(TILE_SIZE),
    HEIGHT: String(TILE_SIZE),
    TIME: wmsTime(time),
  });
  // The placeholder must stay as it is, so it goes on after the encoded part.
  return `${EUMETVIEW_WMS}?${params.toString()}&BBOX={bbox-epsg-3857}`;
}

export function useWmsTimeLayer(map: MapLibreMap | null, { product, time, next, opacity, slot, fadeMs }: Options): void {
  /** Image time -> layer id (the source has the same id). */
  const layers = useRef(new Map<number, string>());
  const loaded = useRef(new Set<number>());
  const lastComplete = useRef<number | null>(null);
  const fader = useRef<Fader | null>(null);
  const [version, setVersion] = useState(0);
  const bump = useCallback(() => setVersion((v) => v + 1), []);

  // One fader per map: a new image fades in over the old one (fader.ts).
  useEffect(() => {
    fader.current = map ? new Fader(slotLayerOps(map, slot), fadeMs, { under: true, onSettled: bump }) : null;
    return () => fader.current?.reset();
  }, [map, slot, fadeMs, bump]);

  const remove = useCallback(
    (t: number) => {
      const id = layers.current.get(t);
      if (map && id) removeLayerAndSource(map, id, id);
      layers.current.delete(t);
      loaded.current.delete(t);
    },
    [map],
  );

  useEffect(() => {
    if (!map) return undefined;
    const onData = (event: MapSourceDataEvent): void => {
      const entry = [...layers.current].find(([, id]) => id === event.sourceId);
      // A source being taken off still reports once; asking it whether it loaded would throw.
      if (!entry || !map.getSource(event.sourceId)) return;
      const done = map.isSourceLoaded(event.sourceId);
      if (done !== loaded.current.has(entry[0])) {
        if (done) loaded.current.add(entry[0]);
        else loaded.current.delete(entry[0]);
        bump();
      }
    };
    map.on('sourcedata', onData);
    return () => {
      map.off('sourcedata', onData);
    };
  }, [map, bump]);

  useEffect(() => {
    const fade = fader.current;
    // Past the observed part of the band: the image goes after the fade, and the run after takes it off.
    if (map && fade && time === null) {
      if (fade.current !== null) {
        fade.hide();
        return;
      }
      if ([...layers.current.values()].some((id) => fade.isLeaving(id))) return;
    }
    if (!map || !fade || time === null) {
      fade?.reset();
      for (const t of [...layers.current.keys()]) remove(t);
      lastComplete.current = null;
      return;
    }
    const ready = loaded.current.has(time);
    if (ready) lastComplete.current = time;
    const fallback = ready ? null : lastComplete.current;
    const keep = new Set([time, ...(next === null ? [] : [next]), ...(fallback === null ? [] : [fallback])]);
    for (const [t, id] of layers.current) if (fade.isLeaving(id) || fade.current === id) keep.add(t);
    for (const t of [...layers.current.keys()]) if (!keep.has(t)) remove(t);

    for (const t of [time, next]) {
      if (t === null || layers.current.has(t)) continue;
      const id = slotId(slot, `${product.layer.replace(/[^a-zA-Z0-9]/g, '_')}-${t}`);
      map.addSource(id, {
        type: 'raster',
        tiles: [wmsTileUrl(product, t)],
        tileSize: TILE_SIZE,
        // Past this the map stretches the tiles it has, smoothly (see SatelliteProduct.maxZoom).
        ...(product.maxZoom === undefined ? {} : { maxzoom: product.maxZoom }),
        attribution: EUMETSAT_ATTRIBUTION,
      });
      addInSlot(map, slot, { id, type: 'raster', source: id, paint: { 'raster-opacity': 0, 'raster-fade-duration': 0 } });
      layers.current.set(t, id);
    }

    // An image still loading is not shown yet: the last complete one stays until it is in.
    const visible = layers.current.get(fallback ?? time);
    if (visible && (fallback !== null || ready)) fade.show(visible, opacity);
  }, [map, product, time, next, opacity, slot, remove, version]);

  useEffect(() => {
    const own = layers.current;
    return () => {
      if (map) for (const id of own.values()) removeLayerAndSource(map, id, id);
      own.clear();
      loaded.current.clear();
    };
  }, [map]);
}
