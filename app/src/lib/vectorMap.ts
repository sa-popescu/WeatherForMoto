import { MapLibreMap, setWorkerUrl, type MapOptions } from 'maplibre-gl';
import workerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url';
import type { Theme } from '../state/settings';
import { appStyle } from './mapStyle';

// The app's base map, shared by the ride screen, Traseu and Hartă: MapLibre
// with OpenFreeMap vector tiles (OpenStreetMap data, free, no key), dark with
// the Cockpit theme and Liberty with the Day theme, with local place names.

// The worker is a file of the app itself, so the CSP needs no blob: for it.
setWorkerUrl(workerUrl);

const STYLE_URL: Record<Theme, string> = {
  dark: 'https://tiles.openfreemap.org/styles/dark',
  day: 'https://tiles.openfreemap.org/styles/liberty',
};

/** Romania, as the first view before anything else frames the map. */
export const ROMANIA_CENTER: [number, number] = [24.9, 45.9];

export function createVectorMap(container: HTMLElement, theme: Theme, options: Partial<MapOptions> = {}): MapLibreMap {
  return new MapLibreMap({
    container,
    style: STYLE_URL[theme],
    transformStyle: (_previous, next) => appStyle(next, theme),
    center: ROMANIA_CENTER,
    zoom: 6,
    attributionControl: { compact: true },
    ...options,
  });
}

/**
 * The layer the app's own lines go under: the first label drawn after the
 * last road line, so they cover the roads but not the street names. The
 * styles draw some labels (water names) before the roads; going under those
 * would put the lines under the roads too.
 */
export function labelsAboveRoads(map: MapLibreMap): string | undefined {
  const layers = map.getStyle().layers;
  const lastLine = layers.reduce((last, layer, i) => (layer.type === 'line' ? i : last), -1);
  return layers.slice(lastLine + 1).find((layer) => layer.type === 'symbol')?.id;
}

/**
 * The compact attribution opens expanded and only folds after a drag; folded
 * from the start, it stays one tap away without covering the map.
 */
export function foldAttribution(map: MapLibreMap): void {
  map.getContainer().querySelector('.maplibregl-ctrl-attrib')?.classList.remove('maplibregl-compact-show');
}

/** A theme colour for a map layer: MapLibre paints with values, not CSS variables. */
export function cssVar(name: string, fallback: string): string {
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}
