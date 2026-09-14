import type { Map as LeafletMap } from 'leaflet';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { modelFrame, nowcastFrame, pictureFrame } from './bandFrames';
import { EUMETSAT_ATTRIBUTION, imageTime, IR_CLOUDS, LIGHTNING, recentTimes } from './eumetsat';
import type { ForecastKind } from './forecast';
import { HOUR_S, iconEuCovers } from './iconEu';
import { geometryKey } from './mercator';
import { gridSource, iconEuSource, type ModelSource } from './modelField';
import { CLOUD_MAX_SPEED_KMH, RAIN_MAX_SPEED_KMH } from './nowcast';
import { PANES } from './panes';
import { cachedCloudPicture, cloudPicture, radarPicture } from './pictures';
import type { RadarFrame } from './radar';
import { buildTimeline, entryDelayMs, nextIndex, planBand, startIndex, utcTimeSec, type TimelineEntry } from './timeline';
import { useCloudPictures } from './useCloudPictures';
import { useFieldGeometry } from './useFieldGeometry';
import { useFieldLayer } from './useFieldLayer';
import { useFieldNowcast, type NowcastStatus } from './useFieldNowcast';
import { useForecastData, type ForecastGrid } from './useForecastData';
import { useIconEuRain, type ModelStatus } from './useIconEuRain';
import { useLatestImageTime } from './useLatestImageTime';
import { useRadarFrames, type RadarStatus } from './useRadarFrames';
import { useRadarLayer } from './useRadarLayer';
import { useWmsTimeLayer } from './useWmsTimeLayer';

// The map's single band of time, in one of two views.
//
// Rain: observed radar (with lightning), the radar extrapolated for an hour
// and a half, then ICON-EU rain (DWD, ~7 km) where that model covers the map.
//
// Clouds: satellite infrared clouds (EUMETSAT, ~2 km, with the radar on top
// when it is on), the clouds extrapolated the same way, then the Open-Meteo
// cloud cover forecast.
//
// The extrapolated frames fade into the model, so there is no jump where the
// observations end. The entry on screen decides which layers are visible.

export type RainSource = 'model' | 'grid';

export interface MapTimeline {
  entries: readonly TimelineEntry[];
  index: number;
  current: TimelineEntry | null;
  playing: boolean;
  /** Which view the band is in. */
  kind: ForecastKind;
  /** Observed entries come from the radar (false: from the satellite, cloud view only). */
  radarOn: boolean;
  /** Where rain ahead comes from right now. */
  rainSource: RainSource;
  /** Lightning is drawn for the moment on screen. */
  lightningShown: boolean;
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
  lightning: boolean;
  kind: ForecastKind;
  opacity: number;
  autoPlay: boolean;
}

/** The forecast window slides with the clock; checking every few minutes is plenty. */
const CLOCK_MS = 5 * 60_000;
/** Satellite pictures in the observed part of the band when the radar is off: two hours. */
const SATELLITE_FRAMES = 13;
/** Pictures the motion is measured on. */
const MOTION_PICTURES = 3;
/** Model hours loaded around the moment on screen; later ones load as the band moves on. */
const MODEL_HOURS_BEHIND = 2 * HOUR_S;
const MODEL_HOURS_AHEAD = 7 * HOUR_S;

const RAIN_AHEAD_ATTRIBUTION =
  '<a href="https://www.rainviewer.com/api.html" target="_blank" rel="noopener">RainViewer</a>, ' +
  '<a href="https://www.dwd.de/" target="_blank" rel="noopener">DWD ICON-EU</a>';
const CLOUDS_AHEAD_ATTRIBUTION = `${EUMETSAT_ATTRIBUTION}, <a href="https://open-meteo.com/" target="_blank" rel="noopener">Open-Meteo</a>`;

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

export function useMapTimeline(map: LeafletMap | null, { fetching, radar, lightning, kind, opacity, autoPlay }: Options): MapTimeline {
  const nowMs = useClock(fetching);
  const cloudView = kind === 'cloud';
  const rainView = kind === 'rain';

  const frames = useRadarFrames(fetching && radar);
  const radarFrames = useMemo(() => (radar ? (frames.data?.frames ?? []) : []), [radar, frames.data]);
  const observedRadar = useMemo(() => radarFrames.filter((frame) => !frame.nowcast), [radarFrames]);
  const geometry = useFieldGeometry(map, fetching);
  const gkey = geometry ? geometryKey(geometry) : '';

  const satelliteLatest = useLatestImageTime(IR_CLOUDS, fetching && cloudView);
  const lightningLatest = useLatestImageTime(LIGHTNING, fetching && lightning);

  // The observed part of the band: radar frames, or satellite times in the cloud view without radar.
  const bandFrames = useMemo<readonly RadarFrame[]>(() => {
    if (radar) return radarFrames;
    if (!cloudView || satelliteLatest === null) return EMPTY_FRAMES;
    return recentTimes(IR_CLOUDS, satelliteLatest, SATELLITE_FRAMES).map((time) => ({ time, path: `satellite:${time}`, nowcast: false }));
  }, [radar, radarFrames, cloudView, satelliteLatest]);

  // ---- Extrapolation ----------------------------------------------------------

  const radarHost = frames.data?.host ?? null;
  const motionRadar = observedRadar.slice(-MOTION_PICTURES);
  const radarKey =
    rainView && radar && radarHost && geometry && motionRadar.length >= 2 ? `radar|${gkey}|${motionRadar.map((f) => f.path).join(',')}` : null;
  const cloudKey = cloudView && geometry && satelliteLatest !== null ? `cloud|${gkey}|${satelliteLatest}` : null;

  const nowcast = useFieldNowcast({
    key: radarKey ?? cloudKey,
    kind: rainView ? 'radar' : 'cloud',
    geometry,
    maxSpeedKmh: rainView ? RAIN_MAX_SPEED_KMH : CLOUD_MAX_SPEED_KMH,
    load: (signal) => {
      if (!geometry) return Promise.resolve([]);
      if (rainView && radarHost) return Promise.all(motionRadar.map((frame) => radarPicture(radarHost, frame, geometry, signal)));
      if (satelliteLatest === null) return Promise.resolve([]);
      return Promise.all(
        recentTimes(IR_CLOUDS, satelliteLatest, MOTION_PICTURES).map(async (time) => ({ rgba: await cloudPicture(geometry, time, signal), timeSec: time })),
      );
    },
  });

  const plan = useMemo(() => {
    const lastObservedSec = rainView ? (observedRadar.length > 0 ? observedRadar[observedRadar.length - 1].time : null) : satelliteLatest;
    return planBand({
      nowSec: nowMs / 1000,
      lastObservedSec,
      lastRadarSec: bandFrames.length > 0 ? bandFrames[bandFrames.length - 1].time : null,
      nowcastBaseSec: nowcast.status === 'ready' ? nowcast.baseSec : null,
      nowcastExpected: nowcast.status !== 'error' && (rainView ? radar && observedRadar.length >= 2 : satelliteLatest !== null),
    });
  }, [nowMs, rainView, observedRadar, satelliteLatest, bandFrames, nowcast.status, nowcast.baseSec, radar]);

  // ---- Forecast -----------------------------------------------------------------

  const [focusHour, setFocusHour] = useState(() => Math.floor(Date.now() / 1000 / HOUR_S) * HOUR_S);
  const neededHours = useMemo(
    () => plan.hourEnds.filter((hour) => hour >= focusHour - MODEL_HOURS_BEHIND && hour <= focusHour + MODEL_HOURS_AHEAD),
    [plan.hourEnds, focusHour],
  );
  const modelCovers = geometry !== null && iconEuCovers(geometry.bounds);
  const iconEu = useIconEuRain({ enabled: fetching && rainView && modelCovers, geometry, hourEnds: neededHours });
  // Until the canvas is sized the model is assumed, so the coarse grid is not fetched for nothing.
  const rainSource: RainSource = rainView && (geometry === null || modelCovers) && iconEu.status !== 'error' ? 'model' : 'grid';
  const useGrid = cloudView || rainSource === 'grid';

  const forecast = useForecastData(map, fetching && useGrid, geometry ? geometry.bounds : null);
  const gridTimes = useMemo(() => (forecast.data?.times ?? []).map(utcTimeSec), [forecast.data]);

  const model = useMemo<ModelSource | null>(() => {
    if (!useGrid) return iconEuSource(iconEu.grids, `icon|${gkey}|${[...iconEu.grids.keys()].join(',')}`);
    if (!forecast.data || !forecast.bounds) return null;
    const b = forecast.bounds;
    return gridSource(forecast.data, b, kind, `grid|${kind}|${forecast.data.times[0]}|${b.south},${b.west},${b.north},${b.east}`);
  }, [useGrid, iconEu.grids, gkey, forecast.data, forecast.bounds, kind]);

  const entries = useMemo(() => {
    const nowcastTimes = nowcast.status === 'ready' ? plan.nowcastTimes : EMPTY_TIMES;
    return buildTimeline(bandFrames, nowcastTimes, useGrid ? gridTimes : plan.modelTimes);
  }, [bandFrames, nowcast.status, plan, useGrid, gridTimes]);

  // ---- Playback -----------------------------------------------------------------

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
    const time = index >= 0 && index < entries.length ? entries[index].timeSec : null;
    shownTime.current = time;
    if (time !== null) setFocusHour(Math.floor(time / HOUR_S) * HOUR_S);
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

  // ---- Layers ---------------------------------------------------------------------

  const current = index >= 0 && index < entries.length ? entries[index] : null;
  const following = entries.length > 1 && index >= 0 ? entries[nextIndex(index, entries.length)] : null;
  const onObserved = current?.source === 'radar';
  const nextObserved = following?.source === 'radar' ? following : null;

  const radarLayer = useRadarLayer(map, {
    host: radarHost,
    frames: radarFrames,
    index: onObserved && radar ? current.index : -1,
    opacity,
    shown: radar && onObserved,
    loaded,
  });

  const lightningShown = lightning && onObserved;
  useWmsTimeLayer(map, {
    product: LIGHTNING,
    time: lightningShown ? imageTime(LIGHTNING, current.timeSec, lightningLatest) : null,
    next: lightning && nextObserved ? imageTime(LIGHTNING, nextObserved.timeSec, lightningLatest) : null,
    opacity: 1,
    pane: PANES.lightning,
  });

  const satelliteTime = cloudView && onObserved && satelliteLatest !== null ? imageTime(IR_CLOUDS, current.timeSec, satelliteLatest) : null;
  const nextSatelliteTime = cloudView && nextObserved && satelliteLatest !== null ? imageTime(IR_CLOUDS, nextObserved.timeSec, satelliteLatest) : null;
  const cloudVersion = useCloudPictures(cloudView && fetching ? geometry : null, [satelliteTime, nextSatelliteTime]);
  const observedClouds = useMemo(
    () => (geometry && satelliteTime !== null ? pictureFrame(`${gkey}|satellite|${satelliteTime}`, cachedCloudPicture(geometry, satelliteTime)) : null),
    // cloudVersion marks pictures arriving in the shared cache.
    [geometry, gkey, satelliteTime, cloudVersion],
  );
  useFieldLayer(map, { frame: observedClouds, geometry, opacity, pane: PANES.clouds, attribution: EUMETSAT_ATTRIBUTION });

  const aheadFrame = useMemo(() => {
    if (!geometry || !current || current.source === 'radar') return null;
    return current.source === 'nowcast' ? nowcastFrame(current.timeSec, geometry, nowcast, model) : modelFrame(current.timeSec, geometry, model);
  }, [geometry, current, nowcast, model]);
  useFieldLayer(map, {
    frame: aheadFrame,
    geometry,
    opacity,
    pane: PANES.ahead,
    attribution: rainView ? RAIN_AHEAD_ATTRIBUTION : CLOUDS_AHEAD_ATTRIBUTION,
  });

  const select = useCallback((next: number) => {
    setPlaying(false);
    setIndex(next);
  }, []);

  const togglePlay = useCallback(() => setPlaying((on) => !on), []);

  const retry = useCallback(() => {
    frames.reload();
    forecast.reload();
    iconEu.reload();
  }, [frames, forecast, iconEu]);

  return {
    entries,
    index,
    current,
    playing,
    kind,
    radarOn: radar,
    rainSource,
    lightningShown,
    radarStatus: frames.status,
    nowcastStatus: nowcast.status,
    modelStatus: iconEu.status,
    forecast,
    tilesLoading: radarLayer.loading,
    tilesFailed: radarLayer.failed,
    select,
    togglePlay,
    retry,
  };
}

const EMPTY_TIMES: readonly number[] = [];
const EMPTY_FRAMES: readonly RadarFrame[] = [];
