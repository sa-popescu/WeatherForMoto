import type { Map as LeafletMap } from 'leaflet';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import type { ForecastKind } from './forecast';
import { iconEuCovers } from './iconEu';
import { entryDelayMs, buildTimeline, nextIndex, planRainBand, startIndex, utcTimeSec, type TimelineEntry } from './timeline';
import { useFieldGeometry } from './useFieldGeometry';
import { useForecastData, type ForecastGrid } from './useForecastData';
import { useForecastLayer } from './useForecastLayer';
import { useIconEuRain, type ModelStatus } from './useIconEuRain';
import { useRadarFrames, type RadarStatus } from './useRadarFrames';
import { useRadarLayer } from './useRadarLayer';
import { useRadarNowcast, type NowcastStatus } from './useRadarNowcast';
import { useRainFieldLayer } from './useRainFieldLayer';

// The map's single band of time. Observed radar, the radar extrapolated for
// the next hour and a half, and the forecast hours are sources behind one
// scrubber: the entry on screen decides which layer is visible.
//
// Rain ahead comes from ICON-EU (DWD, ~7 km) wherever that model covers the
// map, painted in the radar's colours; the extrapolated radar fades into it,
// so there is no jump from the last radar frame to the first forecast hour.
// Clouds, and rain outside ICON-EU or when the DWD is unreachable, still come
// from the coarser Open-Meteo grid.

export type RainSource = 'model' | 'grid';

export interface MapTimeline {
  entries: readonly TimelineEntry[];
  index: number;
  current: TimelineEntry | null;
  playing: boolean;
  /** What the forecast part of the band paints. */
  kind: ForecastKind;
  /** Where rain ahead comes from right now. */
  rainSource: RainSource;
  radarStatus: RadarStatus;
  nowcastStatus: NowcastStatus;
  modelStatus: ModelStatus;
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

/** The forecast window slides with the clock; checking every few minutes is plenty. */
const CLOCK_MS = 5 * 60_000;

function useClock(running: boolean): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!running) return undefined;
    setNow(Date.now());
    const timer = window.setInterval(() => setNow(Date.now()), CLOCK_MS);
    return () => window.clearInterval(timer);
  }, [running]);
  return now;
}

export function useMapTimeline(map: LeafletMap | null, { fetching, radar, kind, opacity, autoPlay }: Options): MapTimeline {
  const nowMs = useClock(fetching);
  const frames = useRadarFrames(fetching && radar);
  const radarFrames = useMemo(() => (radar ? (frames.data?.frames ?? []) : []), [radar, frames.data]);
  const observed = useMemo(() => radarFrames.filter((frame) => !frame.nowcast), [radarFrames]);

  const rainAhead = kind === 'rain';
  const geometry = useFieldGeometry(map, fetching && rainAhead);
  const modelCovers = geometry !== null && iconEuCovers(geometry.bounds);

  const nowcast = useRadarNowcast({ enabled: fetching && radar && rainAhead, host: frames.data?.host ?? null, frames: observed, geometry });

  const plan = useMemo(
    () =>
      planRainBand({
        nowSec: nowMs / 1000,
        lastObservedSec: observed.length > 0 ? observed[observed.length - 1].time : null,
        lastRadarSec: radarFrames.length > 0 ? radarFrames[radarFrames.length - 1].time : null,
        nowcastBaseSec: nowcast.status === 'ready' ? nowcast.baseSec : null,
        nowcastExpected: radar && observed.length >= 2 && nowcast.status !== 'error',
      }),
    [nowMs, observed, radarFrames, nowcast.status, nowcast.baseSec, radar],
  );

  const model = useIconEuRain({ enabled: fetching && rainAhead && modelCovers, geometry, hourEnds: plan.hourEnds });
  // Until the canvas is sized the model is assumed, so the coarse grid is not fetched for nothing.
  const rainSource: RainSource = rainAhead && (geometry === null || modelCovers) && model.status !== 'error' ? 'model' : 'grid';
  const useGrid = !rainAhead || rainSource === 'grid';

  const forecast = useForecastData(map, fetching && useGrid);
  const gridTimes = useMemo(() => (forecast.data?.times ?? []).map(utcTimeSec), [forecast.data]);

  const entries = useMemo(() => {
    const nowcastTimes = rainAhead && nowcast.status === 'ready' ? plan.nowcastTimes : EMPTY;
    return buildTimeline(radarFrames, nowcastTimes, useGrid ? gridTimes : plan.modelTimes);
  }, [radarFrames, rainAhead, nowcast.status, plan, useGrid, gridTimes]);

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

  // Extrapolated frames are always painted here; forecast hours only when they come from the model.
  const paintsRain = rainAhead && current !== null && (current.source === 'nowcast' || (current.source === 'forecast' && !useGrid));
  useRainFieldLayer(map, { entry: paintsRain ? current : null, geometry, nowcast, rain: model, opacity });

  useForecastLayer(map, {
    kind: current?.source === 'forecast' && useGrid ? kind : null,
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
    model.reload();
  }, [frames, forecast, model]);

  return {
    entries,
    index,
    current,
    playing,
    kind,
    rainSource,
    radarStatus: frames.status,
    nowcastStatus: nowcast.status,
    modelStatus: model.status,
    forecast,
    tilesLoading: radarLayer.loading,
    tilesFailed: radarLayer.failed,
    select,
    togglePlay,
    retry,
  };
}

const EMPTY: readonly number[] = [];
