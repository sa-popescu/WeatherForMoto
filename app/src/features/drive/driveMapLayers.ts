import type { Feature, FeatureCollection, LineString } from 'geojson';
import type { ExpressionSpecification, GeoJSONSource, MapLibreMap } from 'maplibre-gl';
import { cssVar, labelsAboveRoads } from '../../lib/vectorMap';
import { splitLineAtKm } from '../route/geometry';
import type { LatLon } from '../route/types';
import type { DriveRoute } from './logic/driveRoute';

// The layers "Drum" draws on the base map: the other routes in grey, the
// chosen one in the accent colour over a dark edge, its traffic in yellow,
// orange and red on top, and the two ends. A wide invisible line under each
// route catches the taps that pick it.

export const HIT_LAYER = 'drive-hit';

const empty = (): FeatureCollection => ({ type: 'FeatureCollection', features: [] });

function line(coords: readonly LatLon[], properties: Record<string, unknown>): Feature<LineString> {
  return { type: 'Feature', properties, geometry: { type: 'LineString', coordinates: coords.map((p) => [p.lon, p.lat]) } };
}

/** Yellow for a little traffic, orange, red, and dark red for a closed road. */
function trafficColor(): ExpressionSpecification {
  return [
    'match',
    ['get', 'magnitude'],
    3,
    cssVar('--t-evita', '#ff5252'),
    4,
    '#9b1c1c',
    2,
    cssVar('--t-atentie', '#ff8f3a'),
    cssVar('--t-ok', '#f2c94c'),
  ];
}

const width = (low: number, high: number): ExpressionSpecification => ['interpolate', ['linear'], ['zoom'], 6, low, 15, high];

export function addDriveLayers(map: MapLibreMap): void {
  map.addSource('drive-routes', { type: 'geojson', data: empty() });
  map.addSource('drive-traffic', { type: 'geojson', data: empty() });
  map.addSource('drive-ends', { type: 'geojson', data: empty() });
  const before = labelsAboveRoads(map);
  const round = { 'line-cap': 'round', 'line-join': 'round' } as const;
  const chosen: ExpressionSpecification = ['==', ['get', 'selected'], true];
  const other: ExpressionSpecification = ['==', ['get', 'selected'], false];
  map.addLayer({ id: HIT_LAYER, type: 'line', source: 'drive-routes', filter: other, layout: round, paint: { 'line-color': '#000', 'line-opacity': 0, 'line-width': 28 } }, before);
  map.addLayer(
    { id: 'drive-other', type: 'line', source: 'drive-routes', filter: other, layout: round, paint: { 'line-color': cssVar('--muted-2', '#8a8f85'), 'line-opacity': 0.85, 'line-width': width(4, 7) } },
    before,
  );
  map.addLayer(
    { id: 'drive-casing', type: 'line', source: 'drive-routes', filter: chosen, layout: round, paint: { 'line-color': cssVar('--bg', '#0b0c0a'), 'line-width': width(8, 13) } },
    before,
  );
  map.addLayer(
    { id: 'drive-chosen', type: 'line', source: 'drive-routes', filter: chosen, layout: round, paint: { 'line-color': cssVar('--accent', '#b4f03c'), 'line-width': width(5, 8) } },
    before,
  );
  map.addLayer(
    { id: 'drive-traffic', type: 'line', source: 'drive-traffic', layout: round, paint: { 'line-color': trafficColor(), 'line-width': width(5, 8) } },
    before,
  );
  // The ends sit above the labels: they are what the screen is about. The
  // rider's own position looks like it does in map apps, a solid dot with a
  // white ring and a soft halo; a place picked by name is a hollow ring, so
  // the two are never confused; the destination is a light dot.
  const accent = cssVar('--accent', '#b4f03c');
  const kind: ExpressionSpecification = ['get', 'kind'];
  map.addLayer({
    id: 'drive-you-halo',
    type: 'circle',
    source: 'drive-ends',
    filter: ['==', kind, 'you'],
    paint: { 'circle-radius': 22, 'circle-color': accent, 'circle-opacity': 0.2, 'circle-blur': 0.3 },
  });
  map.addLayer({
    id: 'drive-ends',
    type: 'circle',
    source: 'drive-ends',
    paint: {
      'circle-radius': ['match', kind, 'you', 8, 'origin', 7, 8],
      'circle-color': ['match', kind, 'you', accent, 'origin', cssVar('--bg', '#0b0c0a'), cssVar('--ink', '#eef1e8')],
      'circle-stroke-width': ['match', kind, 'origin', 4, 3],
      'circle-stroke-color': ['match', kind, 'you', '#ffffff', 'origin', accent, cssVar('--bg', '#0b0c0a')],
    },
  });
}

/**
 * The routes, the traffic of the chosen one, and the ends. The chosen route is drawn last, on top.
 * `originIsYou`: the origin is the rider's GPS position, not a place picked by name.
 */
export function setDriveData(
  map: MapLibreMap,
  routes: readonly DriveRoute[],
  selectedId: string | null,
  origin: LatLon | null,
  destination: LatLon | null,
  originIsYou = false,
): void {
  const warn = (err: unknown): void => console.warn('[drive] could not draw the routes', err);
  const ordered = [...routes].sort((a, b) => Number(a.id === selectedId) - Number(b.id === selectedId));
  const routeFeatures = ordered.map((r) => line(r.line.coords, { id: r.id, selected: r.id === selectedId }));
  const chosen = routes.find((r) => r.id === selectedId);
  const trafficFeatures = (chosen?.traffic ?? []).map((t) => {
    const piece = splitLineAtKm(chosen!.line.coords, chosen!.line.cumKm, [t.fromKm, t.toKm])[1] ?? [];
    return line(piece, { magnitude: t.magnitude });
  });
  const ends: FeatureCollection = {
    type: 'FeatureCollection',
    features: [
      ...(origin ? [{ type: 'Feature' as const, properties: { kind: originIsYou ? 'you' : 'origin' }, geometry: { type: 'Point' as const, coordinates: [origin.lon, origin.lat] } }] : []),
      ...(destination ? [{ type: 'Feature' as const, properties: { kind: 'destination' }, geometry: { type: 'Point' as const, coordinates: [destination.lon, destination.lat] } }] : []),
    ],
  };
  map.getSource<GeoJSONSource>('drive-routes')?.setData({ type: 'FeatureCollection', features: routeFeatures }).catch(warn);
  map.getSource<GeoJSONSource>('drive-traffic')?.setData({ type: 'FeatureCollection', features: trafficFeatures.filter((f) => f.geometry.coordinates.length >= 2) }).catch(warn);
  map.getSource<GeoJSONSource>('drive-ends')?.setData(ends).catch(warn);
}

/** West, south, east, north around every route and both ends. */
export function boundsOf(routes: readonly DriveRoute[], extra: ReadonlyArray<LatLon | null>): [number, number, number, number] | null {
  const all = [...routes.flatMap((r) => r.line.coords), ...extra.filter((p): p is LatLon => p !== null)];
  if (all.length === 0) return null;
  let [w, s, e, n] = [all[0].lon, all[0].lat, all[0].lon, all[0].lat];
  for (const p of all) {
    w = Math.min(w, p.lon);
    e = Math.max(e, p.lon);
    s = Math.min(s, p.lat);
    n = Math.max(n, p.lat);
  }
  return [w, s, e, n];
}
