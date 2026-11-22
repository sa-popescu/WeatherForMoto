import type { FeatureCollection } from 'geojson';
import type { ExpressionSpecification, GeoJSONSource, MapLibreMap } from 'maplibre-gl';
import { cssVar, labelsAboveRoads } from '../../lib/vectorMap';
import type { RouteHazard } from './hazards';
import { routePoints, routeStretches } from './routeMapData';
import type { TimelineRow } from './plan';
import type { RouteLine } from './types';

// The Traseu map's own layers on the app's base map: the route in stretches
// coloured by their worst score, with a dark edge, and the stops and sample
// points on it. Colours come from the theme tokens, read once per map.

const empty = (): FeatureCollection => ({ type: 'FeatureCollection', features: [] });

/** One colour per score tier, as MapLibre wants it: a match on the "tier" property. */
function tierColor(): ExpressionSpecification {
  return [
    'match',
    ['get', 'tier'],
    'ideal',
    cssVar('--t-ideal', '#b4f03c'),
    'ok',
    cssVar('--t-ok', '#f2c94c'),
    'atentie',
    cssVar('--t-atentie', '#ff8f3a'),
    'evita',
    cssVar('--t-evita', '#ff5252'),
    cssVar('--dim', '#5c6157'),
  ];
}

export function addRouteLayers(map: MapLibreMap): void {
  map.addSource('route-stretches', { type: 'geojson', data: empty() });
  map.addSource('route-points', { type: 'geojson', data: empty() });
  const beforeLabels = labelsAboveRoads(map);
  const bg = cssVar('--bg', '#0b0c0a');
  const ink = cssVar('--ink', '#eef1e8');
  const round = { 'line-cap': 'round', 'line-join': 'round' } as const;
  const kind: ExpressionSpecification = ['get', 'kind'];
  map.addLayer(
    {
      id: 'route-casing',
      type: 'line',
      source: 'route-stretches',
      layout: round,
      paint: { 'line-color': bg, 'line-opacity': 0.9, 'line-width': ['interpolate', ['linear'], ['zoom'], 5, 8, 12, 12] },
    },
    beforeLabels,
  );
  map.addLayer(
    {
      id: 'route-stretches',
      type: 'line',
      source: 'route-stretches',
      layout: round,
      paint: { 'line-color': tierColor(), 'line-width': ['interpolate', ['linear'], ['zoom'], 5, 4.5, 12, 7] },
    },
    beforeLabels,
  );
  // The points sit above the labels: they are what the route is about.
  map.addLayer({
    id: 'route-points',
    type: 'circle',
    source: 'route-points',
    paint: {
      'circle-radius': ['match', kind, 'sample', 4, 'via', 6, 8],
      'circle-color': ['match', kind, 'destination', ink, bg],
      'circle-stroke-width': ['match', kind, 'sample', 2, 3],
      'circle-stroke-color': ['match', kind, 'origin', cssVar('--accent', '#b4f03c'), 'destination', bg, 'via', ink, tierColor()],
    },
  });
}

export function setRouteData(map: MapLibreMap, route: RouteLine, rows: readonly TimelineRow[], segments: ReadonlyArray<number | null>): void {
  const warn = (err: unknown): void => console.warn('[route] could not draw the route', err);
  map.getSource<GeoJSONSource>('route-stretches')?.setData(routeStretches(route, rows, segments)).catch(warn);
  map.getSource<GeoJSONSource>('route-points')?.setData(routePoints(rows)).catch(warn);
}

// Static markup only; the hazard's text goes through textContent and aria-label.
const HAZARD_SVG =
  '<svg width="26" height="26" viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3 22 20H2Z" fill="currentColor" stroke="var(--bg)" stroke-width="1.6" stroke-linejoin="round"/><path d="M12 10v4M12 17v.01" stroke="var(--bg)" stroke-width="2.2" stroke-linecap="round"/></svg>';

/** A hazard on the route: a warning sign that names itself on hover and keyboard focus. */
export function hazardElement(h: RouteHazard, text: string): HTMLElement {
  const el = document.createElement('button');
  el.type = 'button';
  el.className = `route-map__hz route-map__hz--sev${Math.min(5, Math.max(1, h.severity))}`;
  el.innerHTML = HAZARD_SVG;
  el.setAttribute('aria-label', text);
  el.title = text;
  return el;
}

// Static markup only, like the hazards: a pump in a round badge.
const FUEL_SVG =
  '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"><path d="M5 20.5V5a1.5 1.5 0 0 1 1.5-1.5h6A1.5 1.5 0 0 1 14 5v15.5M3.5 20.5h12M7.5 8h4"/><path d="M14 10h1.5a1.5 1.5 0 0 1 1.5 1.5v5a1.5 1.5 0 0 0 3 0V8l-2.5-2.5"/></svg>';

/** A suggested fuel stop: a pump badge that names the station on hover and keyboard focus. */
export function fuelElement(text: string): HTMLElement {
  const el = document.createElement('button');
  el.type = 'button';
  el.className = 'route-map__fuel';
  el.innerHTML = FUEL_SVG;
  el.setAttribute('aria-label', text);
  el.title = text;
  return el;
}
