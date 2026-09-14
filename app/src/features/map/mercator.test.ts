import { describe, expect, it } from 'vitest';
import {
  FIELD_MAX_PX,
  FIELD_MAX_ZOOM,
  FIELD_MIN_SPAN_KM,
  fieldGeometry,
  geometryServes,
  latToY,
  lonToX,
  TILE_SIZE,
  tilesFor,
  xToLon,
  yToLat,
} from './mercator';

// The view in the report: Romania and its neighbours on a phone, zoom 6.
const COUNTRY = { south: 41.5, west: 14, north: 52, east: 33 };

describe('Web Mercator', () => {
  it('goes back and forth between degrees and world pixels', () => {
    expect(lonToX(0, 0)).toBe(TILE_SIZE / 2);
    expect(latToY(0, 3)).toBeCloseTo(TILE_SIZE * 4, 6);
    expect(xToLon(lonToX(26.1, 7), 7)).toBeCloseTo(26.1, 9);
    expect(yToLat(latToY(44.43, 7), 7)).toBeCloseTo(44.43, 9);
  });
});

describe('fieldGeometry', () => {
  it('paints a country view at the zoom of the radar tiles, around the whole view', () => {
    const g = fieldGeometry(COUNTRY, 6);
    expect(g.z).toBe(6);
    expect(g.width).toBeLessThanOrEqual(FIELD_MAX_PX);
    expect(g.height).toBeLessThanOrEqual(FIELD_MAX_PX);
    expect(g.bounds.south).toBeLessThan(COUNTRY.south);
    expect(g.bounds.north).toBeGreaterThan(COUNTRY.north);
    expect(g.bounds.west).toBeLessThan(COUNTRY.west);
    expect(g.bounds.east).toBeGreaterThan(COUNTRY.east);
    // Corners are whole world pixels, so the canvas and the tiles line up exactly.
    expect(lonToX(g.bounds.west, g.z)).toBeCloseTo(g.x0, 6);
    expect(latToY(g.bounds.south, g.z)).toBeCloseTo(g.y0 + g.height, 6);
  });

  it('keeps enough room around a city view for rain to arrive from', () => {
    const g = fieldGeometry({ south: 44.4, west: 26.05, north: 44.46, east: 26.15 }, 12);
    expect(g.z).toBe(FIELD_MAX_ZOOM);
    expect((g.bounds.north - g.bounds.south) * 111.32).toBeGreaterThanOrEqual(FIELD_MIN_SPAN_KM - 1);
  });

  it('steps the zoom down rather than paint a canvas too large to redraw', () => {
    const g = fieldGeometry({ south: 30, west: -20, north: 65, east: 45 }, 6);
    expect(g.z).toBeLessThan(6);
    expect(Math.max(g.width, g.height)).toBeLessThanOrEqual(FIELD_MAX_PX);
  });
});

describe('geometryServes', () => {
  const g = fieldGeometry(COUNTRY, 6);

  it('keeps the canvas for a small pan and replaces it for a new zoom or a long pan', () => {
    expect(geometryServes(g, { ...COUNTRY, west: COUNTRY.west + 1, east: COUNTRY.east + 1 }, 6)).toBe(true);
    expect(geometryServes(g, { south: 44, west: 22, north: 48, east: 28 }, 7)).toBe(false);
    expect(geometryServes(g, { ...COUNTRY, west: COUNTRY.west + 10, east: COUNTRY.east + 10 }, 6)).toBe(false);
  });
});

describe('tilesFor', () => {
  it('covers every pixel of the canvas once', () => {
    const g = fieldGeometry(COUNTRY, 6);
    const slots = tilesFor(g);
    const cols = Math.ceil((g.x0 + g.width) / TILE_SIZE) - Math.floor(g.x0 / TILE_SIZE);
    const rows = Math.ceil((g.y0 + g.height) / TILE_SIZE) - Math.floor(g.y0 / TILE_SIZE);
    expect(slots).toHaveLength(cols * rows);
    for (const slot of slots) {
      expect(slot.left).toBeGreaterThan(-TILE_SIZE);
      expect(slot.left).toBeLessThan(g.width);
      expect(slot.top).toBeGreaterThan(-TILE_SIZE);
      expect(slot.top).toBeLessThan(g.height);
    }
  });
});
