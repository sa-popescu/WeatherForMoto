import * as L from 'leaflet';
import { useCallback, useEffect, useRef, useState } from 'react';
import { nextFrameIndex, RADAR_MAX_NATIVE_ZOOM, radarTileUrl, type RadarFrame } from './radar';

// Radar tiles on the map. Only the frame on screen and the next one exist as
// layers: the next one loads invisibly, so switching is instant. While a frame
// the user jumped to is still loading, the last complete one stays visible.

const RAINVIEWER_ATTRIBUTION = '<a href="https://www.rainviewer.com/api.html" target="_blank" rel="noopener">RainViewer</a>';
/** Above the base map (zIndex 1) inside the tile pane, below all markers. */
const RADAR_Z_INDEX = 5;
/** Tile errors, with no tile loaded, before the radar counts as unavailable. */
const FAILED_TILES = 4;

interface Options {
  host: string | null;
  frames: readonly RadarFrame[];
  index: number;
  opacity: number;
  shown: boolean;
  /** Frame paths whose tiles finished loading (shared with playback). */
  loaded: Set<string>;
}

export interface RadarLayerState {
  loading: boolean;
  failed: boolean;
}

export function useRadarLayer(map: L.Map | null, { host, frames, index, opacity, shown, loaded }: Options): RadarLayerState {
  const layers = useRef(new Map<string, L.TileLayer>());
  const lastComplete = useRef<string | null>(null);
  const health = useRef({ path: '', errors: 0, loads: 0 });
  const [version, setVersion] = useState(0);
  const [failed, setFailed] = useState(false);

  const bump = useCallback(() => setVersion((v) => v + 1), []);

  const removeLayer = useCallback(
    (path: string) => {
      layers.current.get(path)?.remove();
      layers.current.delete(path);
      loaded.delete(path);
    },
    [loaded],
  );

  const createLayer = useCallback(
    (path: string, url: string): L.TileLayer => {
      const layer = L.tileLayer(url, {
        opacity: 0,
        zIndex: RADAR_Z_INDEX,
        maxNativeZoom: RADAR_MAX_NATIVE_ZOOM,
        maxZoom: 20,
        attribution: RAINVIEWER_ATTRIBUTION,
        className: 'map-radar-tiles',
      });
      layer.on('loading', () => {
        loaded.delete(path);
        bump();
      });
      layer.on('load', () => {
        loaded.add(path);
        bump();
      });
      layer.on('tileload', () => {
        if (health.current.path === path) health.current.loads += 1;
      });
      layer.on('tileerror', () => {
        const h = health.current;
        if (h.path !== path) return;
        h.errors += 1;
        if (h.errors >= FAILED_TILES && h.loads === 0) setFailed(true);
      });
      return layer;
    },
    [loaded, bump],
  );

  // A new frame list gets a fresh chance.
  useEffect(() => {
    setFailed(false);
    health.current = { path: '', errors: 0, loads: 0 };
  }, [frames]);

  useEffect(() => {
    if (!map || !shown || !host || index < 0 || index >= frames.length) {
      for (const path of [...layers.current.keys()]) removeLayer(path);
      lastComplete.current = null;
      return;
    }
    const current = frames[index];
    const next = frames[nextFrameIndex(index, frames.length)];
    const currentReady = loaded.has(current.path);
    if (currentReady) lastComplete.current = current.path;
    const fallback = currentReady ? null : lastComplete.current;

    const keep = new Set([current.path, next.path]);
    if (fallback) keep.add(fallback);
    for (const path of [...layers.current.keys()]) if (!keep.has(path)) removeLayer(path);
    for (const frame of [current, next]) {
      if (layers.current.has(frame.path)) continue;
      const layer = createLayer(frame.path, radarTileUrl(host, frame));
      layers.current.set(frame.path, layer);
      layer.addTo(map);
    }
    if (health.current.path !== current.path) health.current = { path: current.path, errors: 0, loads: 0 };

    const visiblePath = fallback ?? current.path;
    for (const [path, layer] of layers.current) layer.setOpacity(path === visiblePath ? opacity : 0);
  }, [map, shown, host, frames, index, opacity, loaded, removeLayer, createLayer, version]);

  // Unmount (or a new map): take every radar layer off.
  useEffect(() => {
    const own = layers.current;
    return () => {
      for (const layer of own.values()) layer.remove();
      own.clear();
      loaded.clear();
    };
  }, [map, loaded]);

  const current = index >= 0 && index < frames.length ? frames[index] : null;
  return { loading: shown && current !== null && !loaded.has(current.path), failed };
}
