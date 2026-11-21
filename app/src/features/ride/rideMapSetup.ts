import { MapLibreMap, setWorkerUrl, type GeoJSONSource } from 'maplibre-gl';
import workerUrl from 'maplibre-gl/dist/maplibre-gl-worker.mjs?worker&url';
import type { Theme } from '../../state/settings';
import { localNames, multiLine, type Coord } from './rideMapData';

// The ride map's MapLibre side: a vector map from OpenFreeMap (OpenStreetMap
// data, free, no key), dark at night like the app, and the ride's own lines
// on top of the roads but under the labels.

// The worker is a file of the app itself, so the CSP needs no blob: for it.
setWorkerUrl(workerUrl);

const STYLE_URL: Record<Theme, string> = {
  dark: 'https://tiles.openfreemap.org/styles/dark',
  day: 'https://tiles.openfreemap.org/styles/liberty',
};

/** The route ahead, in the blue riders know from navigation apps, per theme. */
const ROUTE_BLUE: Record<Theme, { line: string; casing: string }> = {
  dark: { line: '#4c8dff', casing: '#163a7a' },
  day: { line: '#1a73e8', casing: '#0d47a1' },
};

const ROMANIA: Coord = [24.9, 45.9];

export type RideLineId = 'ride-plan-done' | 'ride-plan-ahead' | 'ride-track' | 'ride-gaps';
const SOURCES: readonly RideLineId[] = ['ride-plan-done', 'ride-plan-ahead', 'ride-track', 'ride-gaps'];

export function createRideMap(container: HTMLElement, theme: Theme): MapLibreMap {
  return new MapLibreMap({
    container,
    style: STYLE_URL[theme],
    transformStyle: (_previous, next) => localNames(next),
    center: ROMANIA,
    zoom: 6,
    maxPitch: 60,
    attributionControl: { compact: true },
  });
}

function cssVar(name: string, fallback: string): string {
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

/**
 * The layer the ride's lines go under: the first label drawn after the last
 * road line, so the lines cover the roads but not the street names. The
 * styles draw some labels (water names) before the roads; going under those
 * would put the route under the roads too.
 */
function labelsAboveRoads(map: MapLibreMap): string | undefined {
  const layers = map.getStyle().layers;
  const lastLine = layers.reduce((last, layer, i) => (layer.type === 'line' ? i : last), -1);
  return layers.slice(lastLine + 1).find((layer) => layer.type === 'symbol')?.id;
}

/** The ride's sources and layers, once the style is in. On a route, the line ridden gets thinner. */
export function addRideLayers(map: MapLibreMap, theme: Theme, onRoute: boolean): void {
  for (const id of SOURCES) map.addSource(id, { type: 'geojson', data: multiLine([]) });
  const beforeLabels = labelsAboveRoads(map);
  const accent = cssVar('--accent', '#b4f03c');
  const blue = ROUTE_BLUE[theme];
  const round = { 'line-cap': 'round', 'line-join': 'round' } as const;
  map.addLayer(
    { id: 'ride-plan-done', type: 'line', source: 'ride-plan-done', layout: round, paint: { 'line-color': '#8a8f98', 'line-width': 7, 'line-opacity': 0.55 } },
    beforeLabels,
  );
  map.addLayer({ id: 'ride-plan-casing', type: 'line', source: 'ride-plan-ahead', layout: round, paint: { 'line-color': blue.casing, 'line-width': 11 } }, beforeLabels);
  map.addLayer({ id: 'ride-plan-ahead', type: 'line', source: 'ride-plan-ahead', layout: round, paint: { 'line-color': blue.line, 'line-width': 7 } }, beforeLabels);
  map.addLayer(
    // Dots: almost no dash, round caps, wide gaps. A gap is reconstructed, not measured.
    { id: 'ride-gaps', type: 'line', source: 'ride-gaps', layout: round, paint: { 'line-color': accent, 'line-width': 4, 'line-dasharray': [0.1, 2] } },
    beforeLabels,
  );
  map.addLayer(
    { id: 'ride-track', type: 'line', source: 'ride-track', layout: round, paint: { 'line-color': accent, 'line-width': onRoute ? 3 : 5, 'line-opacity': onRoute ? 0.9 : 1 } },
    beforeLabels,
  );
}

/**
 * The compact attribution opens expanded and only folds after a drag; folded
 * from the start, it stays one tap away without covering the road ahead.
 */
export function foldAttribution(map: MapLibreMap): void {
  map.getContainer().querySelector('.maplibregl-ctrl-attrib')?.classList.remove('maplibregl-compact-show');
}

export function setLines(map: MapLibreMap, id: RideLineId, lines: readonly Coord[][]): void {
  const source = map.getSource<GeoJSONSource>(id);
  source?.setData(multiLine(lines)).catch((err: unknown) => console.warn(`[ride] could not draw ${id}`, err));
}
