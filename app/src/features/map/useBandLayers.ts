import type { MapLibreMap } from 'maplibre-gl';
import { useMemo } from 'react';
import { cloudPalette } from './cloudValues';
import { EUMETSAT_ATTRIBUTION, imageTime, IR_CLOUDS, LIGHTNING } from './eumetsat';
import { lightningPalette } from './lightningValues';
import { geometryKey, type FieldGeometry } from './mercator';
import type { ModelSource } from './modelField';
import type { RadarFrame } from './radar';
import { cachedRadarValues, radarValues } from './radarValues';
import { paletteTexture } from './rainPalette';
import { cachedSatelliteField, satelliteField } from './satelliteValues';
import type { TimelineEntry } from './timeline';
import { useBandFields } from './useBandFields';
import type { FieldNowcast } from './useFieldNowcast';
import { useValueLayer } from './useValueLayer';
import { CLOUD_FIELD, modelFrame, nowcastFrame, observedFrame, RAIN_FIELD, type ValueFrame } from './valueFrames';

// The band's three weather layers, all drawn from values (valueLayer.ts):
// clouds at the bottom, rain over them, lightning on top. Which field each
// shows follows the moment on screen: observed (radar, satellite clouds,
// lightning), moved on, or the model. The observed fields of the whole band
// are read ahead (useBandFields.ts).

const RAINVIEWER_ATTRIBUTION = '<a href="https://www.rainviewer.com/api.html" target="_blank" rel="noopener">RainViewer</a>';
const RAIN_AHEAD_ATTRIBUTION = `${RAINVIEWER_ATTRIBUTION}, <a href="https://www.dwd.de/" target="_blank" rel="noopener">DWD ICON-EU</a>`;
const CLOUDS_AHEAD_ATTRIBUTION = `${EUMETSAT_ATTRIBUTION}, <a href="https://open-meteo.com/" target="_blank" rel="noopener">Open-Meteo</a>`;

interface Options {
  geometry: FieldGeometry | null;
  current: TimelineEntry | null;
  following: TimelineEntry | null;
  view: 'rain' | 'cloud';
  radar: { on: boolean; host: string | null; frames: readonly RadarFrame[] };
  /** Times of the observed part of the band (radar frames, or satellite pictures without radar). */
  observedTimes: readonly number[];
  satellite: { latest: number | null; band: readonly number[] };
  lightning: { on: boolean; latest: number | null };
  nowcast: FieldNowcast;
  model: ModelSource | null;
  opacity: number;
  fadeMs: number;
  /** Read the observed fields of the whole band ahead, or only those on screen and next. */
  readAhead: boolean;
}

export interface BandLayers {
  /** Lightning is drawn for the moment on screen. */
  lightningShown: boolean;
  /** The observed field on screen is still being read. */
  loading: boolean;
  /** The radar frame on screen could not be read. */
  radarFailed: boolean;
}

const NOTHING: readonly string[] = [];

const newestFirst = (times: Iterable<number>): string[] => [...new Set(times)].sort((a, b) => b - a).map(String);

export function useBandLayers(map: MapLibreMap | null, options: Options): BandLayers {
  const { geometry, current, following, view, radar, observedTimes, satellite, lightning, nowcast, model, opacity, fadeMs, readAhead } = options;
  const gkey = geometry ? geometryKey(geometry) : '';
  const onObserved = current?.source === 'radar';
  const nextObserved = following?.source === 'radar' ? following : null;

  // ---- Radar ----------------------------------------------------------------------
  const radarOnScreen = radar.on && onObserved ? (radar.frames[current.index] ?? null) : null;
  const radarNext = radar.on && nextObserved ? (radar.frames[nextObserved.index] ?? null) : null;
  const byPath = useMemo(() => new Map(radar.frames.map((frame) => [frame.path, frame])), [radar.frames]);
  const radarBand = useMemo(() => [...radar.frames].sort((a, b) => b.time - a.time).map((frame) => frame.path), [radar.frames]);
  const radarState = useBandFields({
    scope: radar.on && radar.host && geometry ? `radar|${radar.host}|${gkey}` : null,
    band: readAhead ? radarBand : NOTHING,
    onScreen: [radarOnScreen?.path ?? null, radarNext?.path ?? null],
    read: (path) => {
      const frame = byPath.get(path);
      if (!frame || !radar.host || !geometry) return Promise.reject(new Error('the radar frame is no longer in the band'));
      return radarValues(radar.host, frame, geometry);
    },
    cached: (path) => geometry !== null && cachedRadarValues(geometry, path) !== undefined,
    label: 'radar frame',
  });

  // ---- Satellite clouds (cloud view) ------------------------------------------------
  const cloudView = view === 'cloud';
  const cloudTimeOf = (t: number): number | null => (satellite.latest === null ? null : imageTime(IR_CLOUDS, t, satellite.latest));
  const cloudTime = cloudView && onObserved ? cloudTimeOf(current.timeSec) : null;
  const nextCloudTime = cloudView && nextObserved ? cloudTimeOf(nextObserved.timeSec) : null;
  const cloudBand = useMemo(() => (cloudView ? newestFirst(satellite.band) : []), [cloudView, satellite.band]);
  const cloudState = useBandFields({
    scope: cloudView && geometry ? `clouds|${gkey}` : null,
    band: readAhead ? cloudBand : NOTHING,
    onScreen: [cloudTime, nextCloudTime].map((t) => (t === null ? null : String(t))),
    read: (id) => (geometry ? satelliteField('clouds', geometry, Number(id)) : Promise.reject(new Error('no canvas'))),
    cached: (id) => geometry !== null && cachedSatelliteField('clouds', geometry, Number(id)) !== undefined,
    label: 'satellite clouds',
  });

  // ---- Lightning (observed moments, both views) -------------------------------------
  const lightningShown = lightning.on && onObserved;
  const lightningTimeOf = (t: number): number => imageTime(LIGHTNING, t, lightning.latest);
  const lightningTime = lightningShown ? lightningTimeOf(current.timeSec) : null;
  const nextLightningTime = lightning.on && nextObserved ? lightningTimeOf(nextObserved.timeSec) : null;
  const lightningBand = useMemo(
    () => (lightning.on ? newestFirst(observedTimes.map((t) => imageTime(LIGHTNING, t, lightning.latest))) : []),
    [lightning.on, lightning.latest, observedTimes],
  );
  const lightningState = useBandFields({
    scope: lightning.on && geometry ? `lightning|${gkey}` : null,
    band: readAhead ? lightningBand : NOTHING,
    onScreen: [lightningTime, nextLightningTime].map((t) => (t === null ? null : String(t))),
    read: (id) => (geometry ? satelliteField('lightning', geometry, Number(id)) : Promise.reject(new Error('no canvas'))),
    cached: (id) => geometry !== null && cachedSatelliteField('lightning', geometry, Number(id)) !== undefined,
    label: 'lightning',
  });

  // ---- The fields of the moment ---------------------------------------------------------
  const rainView = view === 'rain';
  const rainFrame = useMemo<ValueFrame | null>(() => {
    if (!geometry || !current) return null;
    if (current.source === 'radar') {
      return radarOnScreen ? observedFrame('rain', geometry, radarOnScreen.path, cachedRadarValues(geometry, radarOnScreen.path)) : null;
    }
    if (!rainView) return null;
    return current.source === 'nowcast'
      ? nowcastFrame(RAIN_FIELD, current.timeSec, geometry, nowcast, model)
      : modelFrame(RAIN_FIELD, current.timeSec, geometry, model);
    // The version marks frames arriving in the shared cache.
  }, [geometry, current, radarOnScreen, rainView, nowcast, model, radarState.version]);

  const cloudFrame = useMemo<ValueFrame | null>(() => {
    if (!cloudView || !geometry || !current) return null;
    if (current.source === 'radar') {
      return cloudTime === null ? null : observedFrame('clouds', geometry, String(cloudTime), cachedSatelliteField('clouds', geometry, cloudTime));
    }
    return current.source === 'nowcast'
      ? nowcastFrame(CLOUD_FIELD, current.timeSec, geometry, nowcast, model)
      : modelFrame(CLOUD_FIELD, current.timeSec, geometry, model);
  }, [cloudView, geometry, current, cloudTime, nowcast, model, cloudState.version]);

  const lightningFrame = useMemo<ValueFrame | null>(
    () =>
      lightningTime === null || !geometry
        ? null
        : observedFrame('lightning', geometry, String(lightningTime), cachedSatelliteField('lightning', geometry, lightningTime)),
    [geometry, lightningTime, lightningState.version],
  );

  const observedNow = current?.source === 'radar';
  useValueLayer(map, {
    name: 'clouds',
    slot: 'clouds',
    palette: cloudPalette,
    frame: cloudFrame,
    opacity,
    attribution: observedNow ? EUMETSAT_ATTRIBUTION : CLOUDS_AHEAD_ATTRIBUTION,
    fadeMs,
  });
  useValueLayer(map, {
    name: 'rain',
    slot: 'radar',
    palette: paletteTexture,
    frame: rainFrame,
    opacity,
    attribution: observedNow ? RAINVIEWER_ATTRIBUTION : RAIN_AHEAD_ATTRIBUTION,
    fadeMs,
  });
  useValueLayer(map, {
    name: 'lightning',
    slot: 'lightning',
    palette: lightningPalette,
    frame: lightningFrame,
    opacity: 1,
    attribution: EUMETSAT_ATTRIBUTION,
    fadeMs,
  });

  return { lightningShown, loading: radarState.loading || cloudState.loading, radarFailed: radarState.failed };
}
