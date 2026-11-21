import 'maplibre-gl/dist/maplibre-gl.css';
import { AttributionControl, type MapLibreMap } from 'maplibre-gl';
import { useEffect, useRef, useState, type RefObject } from 'react';
import { createVectorMap, foldAttribution } from '../../lib/vectorMap';
import type { Theme } from '../../state/settings';

// Owns the Hartă map: the app's vector base map (lib/vectorMap.ts), created
// once per mount (and again when the theme changes), flat and north up so the
// weather canvases stay aligned to the tile grid, and resized whenever the tab
// comes back (a hidden map measures 0 x 0).
//
// The map is handed out only once its style has loaded: every weather layer
// is a source and a layer in that style, which cannot be added before.

/** Regional zoom: a county or two around the place (MapLibre zoom, 512 px tiles). */
export const REGIONAL_ZOOM = 7;
const MIN_ZOOM = 2;
const MAX_ZOOM = 17;

/**
 * The zoom of 256 px tiles at the map's scale: what RainViewer, the WMS
 * images and the painted canvases (mercator.ts) count in. MapLibre counts in
 * 512 px tiles, one level less for the same scale.
 */
export function tileZoom(map: MapLibreMap): number {
  return map.getZoom() + 1;
}

interface Options {
  center: { lat: number; lon: number };
  active: boolean;
  theme: Theme;
}

export interface MainMapState {
  map: MapLibreMap | null;
  /** true while base map tiles are still arriving. */
  baseLoading: boolean;
}

export function useMainMap(containerRef: RefObject<HTMLDivElement | null>, { center, active, theme }: Options): MainMapState {
  const [map, setMap] = useState<MapLibreMap | null>(null);
  const [baseLoading, setBaseLoading] = useState(true);
  // Only the first centre builds the map; later place changes go through usePlaceView.
  const initialCenter = useRef(center);
  const instance = useRef<{ map: MapLibreMap; theme: Theme } | null>(null);
  const removal = useRef(0);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return undefined;
    // React can disconnect and reconnect effects without unmounting: StrictMode,
    // or the shell's shared Suspense hiding this tab while another tab loads.
    // Removal waits one tick, so a reconnect keeps the same live map instead of
    // leaving the other hooks holding a removed one.
    window.clearTimeout(removal.current);
    if (instance.current && instance.current.theme !== theme) {
      instance.current.map.remove();
      instance.current = null;
      setMap(null);
    }
    if (!instance.current) {
      let created: MapLibreMap;
      try {
        created = createVectorMap(element, theme, {
          center: [initialCenter.current.lon, initialCenter.current.lat],
          zoom: REGIONAL_ZOOM,
          minZoom: MIN_ZOOM,
          maxZoom: MAX_ZOOM,
          dragRotate: false,
          pitchWithRotate: false,
          touchPitch: false,
          maxPitch: 0,
          attributionControl: false,
        });
      } catch (err) {
        console.warn('[map] the map cannot be drawn on this device', err);
        setBaseLoading(false);
        return undefined;
      }
      created.touchZoomRotate.disableRotation();
      // Top left, under the place chip, where no panel covers it (map.css).
      created.addControl(new AttributionControl({ compact: true }), 'top-left');
      created.on('load', () => {
        // Folded to its (i) button, the credits do not run under the controls on a phone.
        foldAttribution(created);
        setMap(created);
      });
      created.on('dataloading', () => setBaseLoading(true));
      created.on('idle', () => setBaseLoading(false));
      instance.current = { map: created, theme };
    }
    return () => {
      removal.current = window.setTimeout(() => {
        instance.current?.map.remove();
        instance.current = null;
        setMap(null);
      }, 0);
    };
  }, [containerRef, theme]);

  // Back on screen: measure again, keeping the same centre.
  useEffect(() => {
    if (active && map) map.resize();
  }, [active, map]);

  return { map, baseLoading };
}
