import type { LayerSpecification, StyleSpecification } from 'maplibre-gl';
import { describe, expect, it } from 'vitest';
import { appStyle, brighterDarkRoads, labelLayerAboveRoads, localNames, readableDarkLabels } from './mapStyle';

const style = {
  version: 8,
  sources: {},
  layers: [
    { id: 'place', type: 'symbol', source: 'x', layout: { 'text-field': ['coalesce', ['get', 'name_en'], ['get', 'name']] } },
    { id: 'shield', type: 'symbol', source: 'x', layout: { 'text-field': ['get', 'ref'] } },
    { id: 'highway_minor', type: 'line', source: 'x', paint: { 'line-color': '#181818', 'line-width': 2, 'line-opacity': 0.9 } },
    { id: 'water', type: 'fill', source: 'x' },
  ],
} as unknown as StyleSpecification;

describe('map style', () => {
  it('labels places by their local name instead of the English one', () => {
    const out = localNames(style);
    expect(JSON.stringify(out.layers[0])).not.toContain('name_en');
    expect(out.layers[1]).toBe(style.layers[1]);
    expect(out.layers[3]).toBe(style.layers[3]);
  });

  it('lightens the roads of the dark style and keeps their widths', () => {
    const road = brighterDarkRoads(style).layers[2] as { paint: Record<string, unknown> };
    expect(road.paint['line-color']).not.toBe('#181818');
    expect(road.paint['line-opacity']).toBe(1);
    expect(road.paint['line-width']).toBe(2);
  });

  it('makes the street and place names of the dark style readable', () => {
    const dark = {
      version: 8,
      sources: {},
      layers: [
        { id: 'highway_name_other', type: 'symbol', source: 'x', layout: { 'text-size': 10 }, paint: { 'text-color': 'rgba(80, 78, 78, 1)' } },
        { id: 'place_city', type: 'symbol', source: 'x', layout: { 'text-size': 10 }, paint: { 'text-color': 'rgb(101,101,101)' } },
      ],
    } as unknown as StyleSpecification;
    const [street, city] = readableDarkLabels(dark).layers as Array<{ paint: Record<string, unknown>; layout: Record<string, unknown> }>;
    expect(street.paint['text-color']).not.toBe('rgba(80, 78, 78, 1)');
    expect(street.layout['text-size']).not.toBe(10);
    expect(city.paint['text-color']).not.toBe('rgb(101,101,101)');
    // Place names keep their own sizes.
    expect(city.layout['text-size']).toBe(10);
  });

  it('changes the roads only in the dark theme', () => {
    expect(appStyle(style, 'day').layers[2]).toBe(style.layers[2]);
    expect(appStyle(style, 'dark').layers[2]).not.toBe(style.layers[2]);
  });
});

describe('where the app draws its lines', () => {
  type Row = [id: string, type: string, sourceLayer?: string];
  const layers = (rows: Row[]) =>
    rows.map(([id, type, sourceLayer]) => ({ id, type, source: 'x', ...(sourceLayer ? { 'source-layer': sourceLayer } : {}) })) as unknown as LayerSpecification[];

  it('goes under the street names of the dark style, which come before the borders', () => {
    // The order of OpenFreeMap "dark", trimmed.
    const dark = layers([
      ['water_name', 'symbol', 'water_name'],
      ['highway_minor', 'line', 'transportation'],
      ['road_oneway', 'symbol', 'transportation'],
      ['railway_dashline', 'line', 'transportation'],
      ['highway_name_other', 'symbol', 'transportation_name'],
      ['highway_name_motorway', 'symbol', 'transportation_name'],
      ['boundary_state', 'line', 'boundary'],
      ['boundary_country_z5-', 'line', 'boundary'],
      ['place_other', 'symbol', 'place'],
    ]);
    expect(labelLayerAboveRoads(dark)).toBe('highway_name_other');
  });

  it('keeps the day style as it was: under the first label after the bridges', () => {
    // The order of OpenFreeMap "liberty", trimmed.
    const liberty = layers([
      ['highway_minor', 'line', 'transportation'],
      ['road_one_way_arrow', 'symbol', 'transportation'],
      ['bridge_transit_rail_hatching', 'line', 'transportation'],
      ['building', 'fill', 'building'],
      ['boundary_disputed', 'line', 'boundary'],
      ['waterway_line_label', 'symbol', 'waterway'],
      ['highway-name-path', 'symbol', 'transportation_name'],
    ]);
    expect(labelLayerAboveRoads(liberty)).toBe('waterway_line_label');
  });

  it('falls back to the last line in a style without roads, and to the top with no label after it', () => {
    expect(labelLayerAboveRoads(layers([['coast', 'line'], ['label', 'symbol']]))).toBe('label');
    expect(labelLayerAboveRoads(layers([['label', 'symbol'], ['highway_minor', 'line', 'transportation']]))).toBeUndefined();
  });
});
