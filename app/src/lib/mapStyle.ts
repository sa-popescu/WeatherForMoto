import type { LayerSpecification, StyleSpecification } from 'maplibre-gl';
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

/** Everything the app changes in a style, for the given theme. */
export function appStyle(style: StyleSpecification, theme: Theme): StyleSpecification {
  const named = localNames(style);
  return theme === 'dark' ? brighterDarkRoads(named) : named;
}
