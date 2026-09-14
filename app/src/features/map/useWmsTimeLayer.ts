import * as L from 'leaflet';
import { useCallback, useEffect, useRef, useState } from 'react';
import { EUMETSAT_ATTRIBUTION, EUMETVIEW_WMS, wmsTime, type SatelliteProduct } from './eumetsat';

// A time-stamped EUMETSAT image layer (lightning) on the map. Like the radar,
// only the image on screen and the next one exist as layers: the next one
// loads invisibly, and while a new one loads the last complete one stays.

/** Tiles of 512 px: a quarter of the requests of 256 px tiles for the same view. */
const TILE_SIZE = 512;

interface Options {
  product: SatelliteProduct;
  /** Image on screen, unix seconds; null hides the layer. */
  time: number | null;
  /** Image likely to come next, loaded in advance. */
  next: number | null;
  opacity: number;
  pane: string;
}

export function useWmsTimeLayer(map: L.Map | null, { product, time, next, opacity, pane }: Options): void {
  const layers = useRef(new Map<number, L.TileLayer.WMS>());
  const loaded = useRef(new Set<number>());
  const lastComplete = useRef<number | null>(null);
  const [version, setVersion] = useState(0);
  const bump = useCallback(() => setVersion((v) => v + 1), []);

  const remove = useCallback((t: number) => {
    layers.current.get(t)?.remove();
    layers.current.delete(t);
    loaded.current.delete(t);
  }, []);

  useEffect(() => {
    if (!map || time === null) {
      for (const t of [...layers.current.keys()]) remove(t);
      lastComplete.current = null;
      return;
    }
    if (loaded.current.has(time)) lastComplete.current = time;
    const fallback = loaded.current.has(time) ? null : lastComplete.current;
    const keep = new Set([time, ...(next === null ? [] : [next]), ...(fallback === null ? [] : [fallback])]);
    for (const t of [...layers.current.keys()]) if (!keep.has(t)) remove(t);

    for (const t of [time, next]) {
      if (t === null || layers.current.has(t)) continue;
      const layer = L.tileLayer.wms(EUMETVIEW_WMS, {
        layers: product.layer,
        styles: product.style,
        format: 'image/png',
        transparent: true,
        version: '1.3.0',
        tileSize: TILE_SIZE,
        opacity: 0,
        pane,
        attribution: EUMETSAT_ATTRIBUTION,
        // WMS parameters that Leaflet passes through as they are.
        time: wmsTime(t),
      } as L.WMSOptions);
      layer.on('loading', () => {
        loaded.current.delete(t);
        bump();
      });
      layer.on('load', () => {
        loaded.current.add(t);
        bump();
      });
      layers.current.set(t, layer);
      layer.addTo(map);
    }

    const visible = fallback ?? time;
    for (const [t, layer] of layers.current) layer.setOpacity(t === visible ? opacity : 0);
  }, [map, product, time, next, opacity, pane, remove, bump, version]);

  useEffect(() => {
    const own = layers.current;
    return () => {
      for (const layer of own.values()) layer.remove();
      own.clear();
      loaded.current.clear();
    };
  }, [map]);
}
