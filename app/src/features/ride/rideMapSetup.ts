import type { ExpressionSpecification, GeoJSONSource, MapLibreMap } from 'maplibre-gl';
import { createVectorMap, cssVar, labelsAboveRoads } from '../../lib/vectorMap';
import type { Theme } from '../../state/settings';
import { multiLine, type Coord } from './rideMapData';

// The ride map's own layers over the app's base map (lib/vectorMap.ts): the
// route, the line ridden and the gaps, on top of the roads but under the labels.

/** The route ahead, in the blue riders know from navigation apps, per theme. */
const ROUTE_BLUE: Record<Theme, { line: string; casing: string }> = {
  dark: { line: '#4c8dff', casing: '#163a7a' },
  day: { line: '#1a73e8', casing: '#0d47a1' },
};

/** Line widths grow with the zoom, as on a navigation map: thin over a county, wide at a junction. */
function widthByZoom(atCounty: number, atStreet: number, atJunction: number): ExpressionSpecification {
  return ['interpolate', ['linear'], ['zoom'], 10, atCounty, 16, atStreet, 19, atJunction];
}

export type RideLineId = 'ride-plan-done' | 'ride-plan-ahead' | 'ride-track' | 'ride-gaps';
const SOURCES: readonly RideLineId[] = ['ride-plan-done', 'ride-plan-ahead', 'ride-track', 'ride-gaps'];

export function createRideMap(container: HTMLElement, theme: Theme): MapLibreMap {
  return createVectorMap(container, theme, { maxPitch: 60 });
}

/** The ride's sources and layers, once the style is in. On a route, the line ridden gets thinner. */
export function addRideLayers(map: MapLibreMap, theme: Theme, onRoute: boolean): void {
  for (const id of SOURCES) map.addSource(id, { type: 'geojson', data: multiLine([]) });
  const beforeLabels = labelsAboveRoads(map);
  const accent = cssVar('--accent', '#b4f03c');
  const blue = ROUTE_BLUE[theme];
  const round = { 'line-cap': 'round', 'line-join': 'round' } as const;
  map.addLayer(
    {
      id: 'ride-plan-done',
      type: 'line',
      source: 'ride-plan-done',
      layout: round,
      paint: { 'line-color': '#8a8f98', 'line-width': widthByZoom(5, 9, 14), 'line-opacity': 0.55 },
    },
    beforeLabels,
  );
  map.addLayer(
    { id: 'ride-plan-casing', type: 'line', source: 'ride-plan-ahead', layout: round, paint: { 'line-color': blue.casing, 'line-width': widthByZoom(8, 13, 19) } },
    beforeLabels,
  );
  map.addLayer(
    { id: 'ride-plan-ahead', type: 'line', source: 'ride-plan-ahead', layout: round, paint: { 'line-color': blue.line, 'line-width': widthByZoom(5, 9, 14) } },
    beforeLabels,
  );
  map.addLayer(
    // Dots: almost no dash, round caps, wide gaps. A gap is reconstructed, not measured.
    { id: 'ride-gaps', type: 'line', source: 'ride-gaps', layout: round, paint: { 'line-color': accent, 'line-width': widthByZoom(3, 5, 7), 'line-dasharray': [0.1, 2] } },
    beforeLabels,
  );
  map.addLayer(
    {
      id: 'ride-track',
      type: 'line',
      source: 'ride-track',
      layout: round,
      paint: { 'line-color': accent, 'line-width': onRoute ? widthByZoom(3, 4, 6) : widthByZoom(4, 7, 10), 'line-opacity': onRoute ? 0.9 : 1 },
    },
    beforeLabels,
  );
}

export function setLines(map: MapLibreMap, id: RideLineId, lines: readonly Coord[][]): void {
  const source = map.getSource<GeoJSONSource>(id);
  source?.setData(multiLine(lines)).catch((err: unknown) => console.warn(`[ride] could not draw ${id}`, err));
}
