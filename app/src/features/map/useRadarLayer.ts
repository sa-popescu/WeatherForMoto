import type { ErrorEvent, MapLibreMap, MapSourceDataEvent } from 'maplibre-gl';
import { useCallback, useEffect, useRef, useState } from 'react';
import { Fader } from './fader';
import { nextFrameIndex, RADAR_MAX_NATIVE_ZOOM, radarTileUrl, type RadarFrame } from './radar';
import { addInSlot, removeLayerAndSource, slotId, slotLayerOps } from './weatherSlots';

// Radar tiles on the map. Only the frame on screen and the next one exist as
// layers: the next one loads invisibly, so it is ready when the band moves on.
// While a frame the user jumped to is still loading, the last complete one
// stays visible. The new frame fades in over the old one (fader.ts), so the
// map never goes blank between two frames. Past the observed part of the band
// the layers stay, hidden, for the way back.

const RAINVIEWER_ATTRIBUTION = '<a href="https://www.rainviewer.com/api.html" target="_blank" rel="noopener">RainViewer</a>';
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
  /** How long a new frame takes to fade in; 0 swaps at once. */
  fadeMs: number;
}

export interface RadarLayerState {
  loading: boolean;
  failed: boolean;
}

/** One id per frame; the path's slashes are not welcome in ids. */
const frameId = (path: string): string => slotId('radar', path.replace(/[^a-zA-Z0-9]/g, '_'));

export function useRadarLayer(map: MapLibreMap | null, { host, frames, index, opacity, shown, loaded, fadeMs }: Options): RadarLayerState {
  /** Frame path -> layer id (the source has the same id). */
  const layers = useRef(new Map<string, string>());
  const lastComplete = useRef<string | null>(null);
  const health = useRef({ path: '', errors: 0, loads: 0 });
  const fader = useRef<Fader | null>(null);
  const [version, setVersion] = useState(0);
  const [failed, setFailed] = useState(false);

  const bump = useCallback(() => setVersion((v) => v + 1), []);

  // One fader per map; when a fade is done, the frames left underneath can go.
  useEffect(() => {
    fader.current = map ? new Fader(slotLayerOps(map, 'radar'), fadeMs, { under: true, onSettled: bump }) : null;
    return () => fader.current?.reset();
  }, [map, fadeMs, bump]);

  const removeFrame = useCallback(
    (path: string) => {
      const id = layers.current.get(path);
      if (map && id) removeLayerAndSource(map, id, id);
      layers.current.delete(path);
      loaded.delete(path);
    },
    [map, loaded],
  );

  // Which frames have all their tiles in, and which keep failing.
  useEffect(() => {
    if (!map) return undefined;
    const pathOf = (sourceId: string | undefined): string | undefined =>
      sourceId === undefined ? undefined : [...layers.current].find(([, id]) => id === sourceId)?.[0];
    const onData = (event: MapSourceDataEvent): void => {
      const path = pathOf(event.sourceId);
      if (!path) return;
      if (event.tile && health.current.path === path) health.current.loads += 1;
      const done = map.isSourceLoaded(event.sourceId);
      if (done !== loaded.has(path)) {
        if (done) loaded.add(path);
        else loaded.delete(path);
        bump();
      }
    };
    // Tile errors carry the source they came from, though the event type does not say so.
    const onError = (event: ErrorEvent): void => {
      const path = pathOf((event as ErrorEvent & { sourceId?: string }).sourceId);
      const h = health.current;
      if (!path || h.path !== path) return;
      h.errors += 1;
      if (h.errors >= FAILED_TILES && h.loads === 0) setFailed(true);
    };
    map.on('sourcedata', onData);
    map.on('error', onError);
    return () => {
      map.off('sourcedata', onData);
      map.off('error', onError);
    };
  }, [map, loaded, bump]);

  // A new frame list gets a fresh chance.
  useEffect(() => {
    setFailed(false);
    health.current = { path: '', errors: 0, loads: 0 };
  }, [frames]);

  useEffect(() => {
    const fade = fader.current;
    if (!map || !fade || !host || index < 0 || index >= frames.length) {
      fade?.reset();
      for (const path of [...layers.current.keys()]) removeFrame(path);
      lastComplete.current = null;
      return;
    }
    const current = frames[index];
    const next = frames[nextFrameIndex(index, frames.length)];
    const currentReady = loaded.has(current.path);
    if (currentReady) lastComplete.current = current.path;
    const fallback = currentReady ? null : lastComplete.current;

    // The frame on screen, the next, the last complete one while waiting, and any still under a fade.
    const keep = new Set([current.path, next.path]);
    if (fallback) keep.add(fallback);
    for (const [path, id] of layers.current) if (fade.isLeaving(id) || fade.current === id) keep.add(path);
    for (const path of [...layers.current.keys()]) if (!keep.has(path)) removeFrame(path);
    for (const frame of [current, next]) {
      if (layers.current.has(frame.path)) continue;
      const id = frameId(frame.path);
      map.addSource(id, {
        type: 'raster',
        tiles: [radarTileUrl(host, frame)],
        tileSize: 256,
        // Beyond this the map stretches the last tiles, as Leaflet's maxNativeZoom did.
        maxzoom: RADAR_MAX_NATIVE_ZOOM,
        attribution: RAINVIEWER_ATTRIBUTION,
      });
      addInSlot(map, 'radar', { id, type: 'raster', source: id, paint: { 'raster-opacity': 0, 'raster-fade-duration': 0 } });
      layers.current.set(frame.path, id);
    }
    if (health.current.path !== current.path) health.current = { path: current.path, errors: 0, loads: 0 };

    // Past the observed part of the band the rain ahead takes over, on top. The frames stay
    // loaded and hidden, so the radar is there at once when the band comes back to it.
    if (!shown) {
      fade.hide();
      return;
    }

    // A frame still loading is not shown yet: the last complete one stays until it is in.
    const visible = layers.current.get(fallback ?? current.path);
    if (visible && (fallback !== null || currentReady)) fade.show(visible, opacity);
  }, [map, shown, host, frames, index, opacity, loaded, removeFrame, version]);

  // Unmount (or a new map): take every radar layer off.
  useEffect(() => {
    const own = layers.current;
    return () => {
      if (map) for (const id of own.values()) removeLayerAndSource(map, id, id);
      own.clear();
      loaded.clear();
    };
  }, [map, loaded]);

  const current = index >= 0 && index < frames.length ? frames[index] : null;
  return { loading: shown && current !== null && !loaded.has(current.path), failed };
}
