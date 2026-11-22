import type { ExpressionSpecification, LayerSpecification, StyleSpecification } from 'maplibre-gl';
import type { Theme } from '../state/settings';

// Changes the app makes to the OpenFreeMap styles before a map draws them.
// Pure functions over the style JSON, so they are tested without a map.

const LOCAL_NAME = ['coalesce', ['get', 'name:latin'], ['get', 'name']];

/**
 * The OpenFreeMap styles label places in English when they can ("Bucharest");
 * the app uses the local names, the ones on the road signs ("București").
 */
export function localNames(style: StyleSpecification): StyleSpecification {
  return {
    ...style,
    layers: style.layers.map((layer) => {
      if (layer.type !== 'symbol' || !layer.layout) return layer;
      const field = layer.layout['text-field'];
      if (field === undefined || !JSON.stringify(field).includes('name_en')) return layer;
      return { ...layer, layout: { ...layer.layout, 'text-field': LOCAL_NAME } } as LayerSpecification;
    }),
  };
}

/**
 * Road colours for the dark style, by layer id. As published it draws minor
 * roads almost in the colour of the ground, which reads as an empty map on a
 * handlebar. These keep it dark but give every road an edge: streets in
 * grey, main roads lighter, motorways a muted amber like the day map's.
 */
const DARK_ROADS: Readonly<Record<string, { color: string; opacity?: number }>> = {
  highway_path: { color: '#2c2f35' },
  highway_minor: { color: '#353941', opacity: 1 },
  highway_major_casing: { color: 'rgba(125, 130, 140, 0.9)' },
  highway_major_inner: { color: '#454a54' },
  highway_major_subtle: { color: '#454a54' },
  highway_motorway_casing: { color: 'rgba(170, 135, 75, 0.9)' },
  highway_motorway_inner: { color: '#5c4a2c' },
  highway_motorway_subtle: { color: '#5c4a2c' },
};

export function brighterDarkRoads(style: StyleSpecification): StyleSpecification {
  return {
    ...style,
    layers: style.layers.map((layer) => {
      const road = DARK_ROADS[layer.id];
      if (!road || layer.type !== 'line') return layer;
      const paint = { ...layer.paint, 'line-color': road.color, ...(road.opacity !== undefined ? { 'line-opacity': road.opacity } : {}) };
      return { ...layer, paint } as LayerSpecification;
    }),
  };
}

interface LabelLook {
  color: string;
  halo: string;
  haloWidth: number;
  /** Text size, px, or one that grows with the zoom. */
  size?: number | ExpressionSpecification;
}

/** Street names: a touch larger as the map zooms in, readable at a glance on the handlebar. */
const STREET_NAME_SIZE: ExpressionSpecification = ['interpolate', ['linear'], ['zoom'], 13, 12, 16, 14, 19, 16];

/**
 * Labels of the dark style. As published, street names are dark grey 10 px on
 * black and place names barely lighter, so on a phone they do not show at all.
 */
function darkLabel(id: string): LabelLook | null {
  if (id === 'highway_name_other') return { color: '#d4d7dd', halo: '#0c0c0c', haloWidth: 1.6, size: STREET_NAME_SIZE };
  if (id === 'highway_name_motorway') return { color: '#e2cc9c', halo: '#0c0c0c', haloWidth: 1.6, size: 11 };
  if (id === 'water_name') return { color: '#8fa3bf', halo: '#0c0c0c', haloWidth: 1.4 };
  if (id.startsWith('place_')) return { color: '#c3c7ce', halo: 'rgba(0, 0, 0, 0.85)', haloWidth: 1.6 };
  return null;
}

export function readableDarkLabels(style: StyleSpecification): StyleSpecification {
  return {
    ...style,
    layers: style.layers.map((layer) => {
      const look = layer.type === 'symbol' ? darkLabel(layer.id) : null;
      if (!look || layer.type !== 'symbol') return layer;
      const paint = { ...layer.paint, 'text-color': look.color, 'text-halo-color': look.halo, 'text-halo-width': look.haloWidth };
      const layout = look.size === undefined ? layer.layout : { ...layer.layout, 'text-size': look.size };
      return { ...layer, paint, layout } as LayerSpecification;
    }),
  };
}

/** The OpenMapTiles source layer of roads, railways and their bridges. */
const ROADS = 'transportation';

/**
 * The label layer the app's own lines (routes, the ride, radar) go under: the
 * first label drawn after the last road or railway line, so they cover the
 * roads but not the street names.
 *
 * Counted from the last road, not the last line of any kind: the dark style
 * draws the street names before the county and country borders, and going
 * under the first label after a border put the route over the street names.
 * Some labels (water names) come before the roads, and going under those
 * would put the lines under the roads too. A style without road layers falls
 * back to the last line.
 */
export function labelLayerAboveRoads(layers: readonly LayerSpecification[]): string | undefined {
  let lastRoad = -1;
  let lastLine = -1;
  layers.forEach((layer, i) => {
    if (layer.type !== 'line') return;
    lastLine = i;
    if (layer['source-layer'] === ROADS) lastRoad = i;
  });
  const after = lastRoad >= 0 ? lastRoad : lastLine;
  return layers.slice(after + 1).find((layer) => layer.type === 'symbol')?.id;
}

/** Everything the app changes in a style, for the given theme. */
export function appStyle(style: StyleSpecification, theme: Theme): StyleSpecification {
  const named = localNames(style);
  return theme === 'dark' ? readableDarkLabels(brighterDarkRoads(named)) : named;
}
