import type { Map as LeafletMap } from 'leaflet';
import { useCallback, useState } from 'react';
import type { RadarFrame } from './radar';
import { useRadarFrames, type RadarStatus } from './useRadarFrames';
import { useRadarLayer } from './useRadarLayer';
import { useRadarPlayback } from './useRadarPlayback';

// The radar feature in one hook: frame list, playback and map layers.

const NO_FRAMES: readonly RadarFrame[] = [];

export interface RadarOptions {
  /** Fetch, refresh and animate (tab and page on screen, layer on). */
  fetching: boolean;
  /** Layer switched on in the layers panel. */
  shown: boolean;
  opacity: number;
  autoPlay: boolean;
}

export interface RadarState {
  status: RadarStatus;
  frames: readonly RadarFrame[];
  index: number;
  playing: boolean;
  tilesLoading: boolean;
  tilesFailed: boolean;
  select: (index: number) => void;
  togglePlay: () => void;
  retry: () => void;
}

export function useRadar(map: LeafletMap | null, { fetching, shown, opacity, autoPlay }: RadarOptions): RadarState {
  const { status, data, reload } = useRadarFrames(fetching);
  const frames = data?.frames ?? NO_FRAMES;
  // Frame paths whose tiles finished loading: the layer fills it, playback waits on it.
  const [loaded] = useState(() => new Set<string>());

  const isReady = useCallback((i: number) => i >= 0 && i < frames.length && loaded.has(frames[i].path), [frames, loaded]);
  const playback = useRadarPlayback(frames, fetching, autoPlay, isReady);
  const layer = useRadarLayer(map, { host: data?.host ?? null, frames, index: playback.index, opacity, shown, loaded });

  return {
    status,
    frames,
    index: playback.index,
    playing: playback.playing,
    tilesLoading: layer.loading,
    tilesFailed: layer.failed,
    select: playback.select,
    togglePlay: playback.toggle,
    retry: reload,
  };
}
