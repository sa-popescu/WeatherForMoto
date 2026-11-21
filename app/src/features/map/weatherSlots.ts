import type { FeatureCollection } from 'geojson';
import type { AddLayerObject, MapLibreMap } from 'maplibre-gl';
import { labelsAboveRoads } from '../../lib/vectorMap';

// Where each weather layer goes in the map's style, so they stack the same way
// whatever order they are added in: satellite clouds under the radar,
// lightning on top of it, the rain and clouds ahead above those, and all of
// them under the place and street names, which stay readable over the rain.

export const SLOTS = ['clouds', 'radar', 'lightning', 'ahead'] as const;
export type Slot = (typeof SLOTS)[number];

const PREFIX = 'wx-';

/** A layer id that carries its slot, so the order can be read back from the style. */
export function slotId(slot: Slot, name: string): string {
  return `${PREFIX}${slot}-${name}`;
}

function slotOf(id: string): number {
  if (!id.startsWith(PREFIX)) return -1;
  return SLOTS.findIndex((slot) => id.startsWith(`${PREFIX}${slot}-`));
}

/** Adds a layer in its slot: under the first weather layer of a higher slot, else under the labels. */
export function addInSlot(map: MapLibreMap, slot: Slot, layer: AddLayerObject): void {
  const rank = SLOTS.indexOf(slot);
  const above = map.getStyle().layers.find((l) => slotOf(l.id) > rank);
  map.addLayer(layer, above?.id ?? labelsAboveRoads(map));
}

/** Takes a layer and its source off; a map already removed (theme switch, unmount) is left alone. */
export function removeLayerAndSource(map: MapLibreMap, layerId: string, sourceId: string): void {
  try {
    if (map.getLayer(layerId)) map.removeLayer(layerId);
    if (map.getSource(sourceId)) map.removeSource(sourceId);
  } catch {
    // The map is gone, and its layers with it.
  }
}

const EMPTY: FeatureCollection = { type: 'FeatureCollection', features: [] };

/**
 * Credits a source in the map's attribution while its layer is shown. Canvas
 * sources carry no attribution of their own, so an empty GeoJSON source (and
 * an invisible layer, which makes the map count it as used) carries it.
 */
export function setCredit(map: MapLibreMap, name: string, html: string | null): void {
  const sourceId = `${PREFIX}credit-${name}`;
  const layerId = `${sourceId}-layer`;
  const existing = map.getSource(sourceId) as { attribution?: string } | undefined;
  if (html === null || (existing && existing.attribution !== html)) removeLayerAndSource(map, layerId, sourceId);
  if (html === null || map.getSource(sourceId)) return;
  map.addSource(sourceId, { type: 'geojson', data: EMPTY, attribution: html });
  map.addLayer({ id: layerId, type: 'circle', source: sourceId, paint: { 'circle-opacity': 0, 'circle-radius': 0 } });
}
