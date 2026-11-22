import 'maplibre-gl/dist/maplibre-gl.css';
import type { MapLayerMouseEvent, MapLibreMap } from 'maplibre-gl';
import { useEffect, useRef, useState, type RefObject } from 'react';
import { createVectorMap, foldAttribution } from '../../lib/vectorMap';
import type { Theme } from '../../state/settings';
import { addDriveLayers, HIT_LAYER } from './driveMapLayers';

// The map of "Drum": created once the screen is first shown, again when the
// theme changes, and resized whenever the screen comes back into view (a
// hidden screen has no size). A tap on a grey route picks it.

export interface DriveMap {
  map: MapLibreMap | null;
  /** The base map and the app's layers are in place. */
  ready: boolean;
  failed: boolean;
}

export function useDriveMap(containerRef: RefObject<HTMLDivElement | null>, theme: Theme, active: boolean, onPick: (routeId: string) => void): DriveMap {
  const [map, setMap] = useState<MapLibreMap | null>(null);
  const [ready, setReady] = useState(false);
  const [failed, setFailed] = useState(false);
  const pickRef = useRef(onPick);
  pickRef.current = onPick;

  useEffect(() => {
    const el = containerRef.current;
    if (!el) return undefined;
    let created: MapLibreMap;
    try {
      created = createVectorMap(el, theme, { dragRotate: false, pitchWithRotate: false, touchPitch: false, maxPitch: 0 });
    } catch (err) {
      console.warn('[drive] the map cannot be drawn on this device', err);
      setFailed(true);
      return undefined;
    }
    created.touchZoomRotate.disableRotation();
    const onTap = (event: MapLayerMouseEvent): void => {
      const id = event.features?.[0]?.properties?.id;
      if (typeof id === 'string') pickRef.current(id);
    };
    created.on('load', () => {
      addDriveLayers(created);
      foldAttribution(created);
      created.on('click', HIT_LAYER, onTap);
      setReady(true);
    });
    setMap(created);
    return () => {
      created.remove();
      setMap(null);
      setReady(false);
    };
  }, [containerRef, theme]);

  useEffect(() => {
    if (!map || !active) return undefined;
    const frame = window.requestAnimationFrame(() => map.resize());
    return () => window.cancelAnimationFrame(frame);
  }, [map, active]);

  return { map, ready, failed };
}
