import * as L from 'leaflet';
import 'leaflet/dist/leaflet.css';
import { useEffect, useRef, useState, type RefObject } from 'react';

// Owns the Leaflet map: created once per mount, OpenStreetMap base tiles, and
// a size refresh whenever the tab becomes visible again (Leaflet measures
// 0 x 0 while its container is display:none).
//
// Both themes use the same OSM tiles. The dark theme darkens them with a CSS
// filter on the base layer only (see map.css, ".map-base"), so the radar,
// markers and controls keep their true colours. CARTO basemaps are not used:
// they now come back watermarked "API KEY REQUIRED".

const OSM_URL = 'https://tile.openstreetmap.org/{z}/{x}/{y}.png';
const OSM_ATTRIBUTION = '&copy; <a href="https://www.openstreetmap.org/copyright" target="_blank" rel="noopener">OpenStreetMap</a>';

/** Regional zoom: a county or two around the place. */
export const REGIONAL_ZOOM = 8;

interface Options {
  center: { lat: number; lon: number };
  active: boolean;
}

export interface LeafletMapState {
  map: L.Map | null;
  /** true while base map tiles are still arriving. */
  baseLoading: boolean;
}

function createMap(element: HTMLElement, center: { lat: number; lon: number }, onLoading: (loading: boolean) => void): L.Map {
  const map = L.map(element, { zoomControl: false, attributionControl: false, minZoom: 3, maxZoom: 18, worldCopyJump: true });
  map.setView([center.lat, center.lon], REGIONAL_ZOOM);
  L.control.attribution({ position: 'topleft', prefix: false }).addTo(map);
  const base = L.tileLayer(OSM_URL, { maxZoom: 19, attribution: OSM_ATTRIBUTION, className: 'map-base' });
  base.on('loading', () => onLoading(true));
  base.on('load', () => onLoading(false));
  base.addTo(map);
  return map;
}

export function useLeafletMap(containerRef: RefObject<HTMLDivElement | null>, { center, active }: Options): LeafletMapState {
  const [map, setMap] = useState<L.Map | null>(null);
  const [baseLoading, setBaseLoading] = useState(true);
  // Only the first centre builds the map; later place changes go through usePlaceView.
  const initialCenter = useRef(center);
  const instance = useRef<L.Map | null>(null);
  const removal = useRef(0);

  useEffect(() => {
    const element = containerRef.current;
    if (!element) return undefined;
    // React can disconnect and reconnect effects without unmounting: StrictMode,
    // or the shell's shared Suspense hiding this tab while another tab loads.
    // Removal waits one tick, so a reconnect keeps the same live map instead of
    // leaving the other hooks holding a removed one (which crashes on addTo).
    window.clearTimeout(removal.current);
    if (!instance.current) {
      instance.current = createMap(element, initialCenter.current, setBaseLoading);
      setMap(instance.current);
    }
    return () => {
      removal.current = window.setTimeout(() => {
        instance.current?.remove();
        instance.current = null;
      }, 0);
    };
  }, [containerRef]);

  // Back on screen: measure again, keeping the same centre.
  useEffect(() => {
    if (active && map) map.invalidateSize();
  }, [active, map]);

  return { map, baseLoading };
}
