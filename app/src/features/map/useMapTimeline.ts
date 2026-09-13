import type { Map as LeafletMap } from 'leaflet';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ForecastKind } from './forecast';
import { entryDelayMs, buildTimeline, nextIndex, startIndex, type TimelineEntry } from './timeline';
import { useForecastData, type ForecastGrid } from './useForecastData';
import { useForecastLayer } from './useForecastLayer';
import { useRadarFrames, type RadarStatus } from './useRadarFrames';
import { useRadarLayer } from './useRadarLayer';

// The map's single band of time. Observed radar and the forecast hours are two
// sources behind one scrubber: the entry on screen decides which layer is
// visible, so moving past the last radar frame simply continues into the
// forecast instead of switching anything by hand.

export interface MapTimeline {
  entries: readonly TimelineEntry[];
  index: number;
  current: TimelineEntry | null;
  playing: boolean;
  /** What the forecast part of the band paints. */
  kind: ForecastKind;
  radarStatus: RadarStatus;
  forecast: ForecastGrid;
  tilesLoading: boolean;
  tilesFailed: boolean;
  select: (index: number) => void;
  togglePlay: () => void;
  retry: () => void;
}

interface Options {
  /** Fetch, refresh and animate (tab and page on screen). */
  fetching: boolean;
  /** Observed radar switched on in the layers panel. */
  radar: boolean;
  kind: ForecastKind;
  opacity: number;
  autoPlay: boolean;
}

export function useMapTimeline(map: LeafletMap | null, { fetching, radar, kind, opacity, autoPlay }: Options): MapTimeline {
  const frames = useRadarFrames(fetching && radar);
  const forecast = useForecastData(map, fetching);
  const radarFrames = useMemo(() => (radar ? (frames.data?.frames ?? []) : []), [radar, frames.data]);
  const forecastTimes = forecast.data?.times ?? EMPTY_TIMES;

  const entries = useMemo(() => buildTimeline(radarFrames, forecastTimes), [radarFrames, forecastTimes]);

  const [index, setIndex] = useState(-1);
  const [playing, setPlaying] = useState(false);
  const holdIndex = useRef(-1);
  const shownTime = useRef<number | null>(null);
  const autoStarted = useRef(false);
  // Radar frame paths whose tiles finished loading; the layer fills this in.
  const [loaded] = useState(() => new Set<string>());

  // A new band keeps the moment the user was looking at, or opens on the present.
  useEffect(() => {
    if (entries.length === 0) {
      setIndex(-1);
      return;
    }
    const start = startIndex(entries, Date.now() / 1000);
    holdIndex.current = start;
    setIndex((current) => {
      if (current < 0 || shownTime.current === null) return start;
      const same = entries.findIndex((entry) => entry.timeSec === shownTime.current);
      return same >= 0 ? same : start;
    });
  }, [entries]);

  useEffect(() => {
    shownTime.current = index >= 0 && index < entries.length ? entries[index].timeSec : null;
  }, [entries, index]);

  // Play once by itself when the band first fills in, unless motion is reduced.
  useEffect(() => {
    if (!autoPlay || autoStarted.current || entries.length < 2 || !fetching) return;
    autoStarted.current = true;
    setPlaying(true);
  }, [autoPlay, entries.length, fetching]);

  useEffect(() => {
    if (!playing || !fetching || entries.length < 2 || index < 0) return undefined;
    const timer = window.setTimeout(
      () => setIndex((current) => nextIndex(current, entries.length)),
      entryDelayMs(entries, index, holdIndex.current),
    );
    return () => window.clearTimeout(timer);
  }, [playing, fetching, entries, index]);

  // Nothing animates while the map is off screen.
  useEffect(() => {
    if (!fetching) setPlaying(false);
  }, [fetching]);

  const current = index >= 0 && index < entries.length ? entries[index] : null;
  const onRadar = current?.source === 'radar';

  const radarLayer = useRadarLayer(map, {
    host: frames.data?.host ?? null,
    frames: radarFrames,
    index: onRadar ? current.index : -1,
    opacity,
    shown: radar && onRadar,
    loaded,
  });

  useForecastLayer(map, {
    kind: current?.source === 'forecast' ? kind : null,
    data: forecast.data,
    bounds: forecast.bounds,
    index: current?.source === 'forecast' ? current.index : 0,
    opacity,
  });

  const select = useCallback((next: number) => {
    setPlaying(false);
    setIndex(next);
  }, []);

  const togglePlay = useCallback(() => setPlaying((on) => !on), []);

  const retry = useCallback(() => {
    frames.reload();
    forecast.reload();
  }, [frames, forecast]);

  return {
    entries,
    index,
    current,
    playing,
    kind,
    radarStatus: frames.status,
    forecast,
    tilesLoading: radarLayer.loading,
    tilesFailed: radarLayer.failed,
    select,
    togglePlay,
    retry,
  };
}

const EMPTY_TIMES: readonly string[] = [];
