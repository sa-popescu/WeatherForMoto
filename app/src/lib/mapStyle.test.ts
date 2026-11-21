import type { StyleSpecification } from 'maplibre-gl';
import { describe, expect, it } from 'vitest';
import { appStyle, brighterDarkRoads, localNames } from './mapStyle';

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

  it('changes the roads only in the dark theme', () => {
    expect(appStyle(style, 'day').layers[2]).toBe(style.layers[2]);
    expect(appStyle(style, 'dark').layers[2]).not.toBe(style.layers[2]);
  });
});
