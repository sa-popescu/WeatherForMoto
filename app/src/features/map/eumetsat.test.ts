import { describe, expect, it } from 'vitest';
import {
  capabilitiesUrl,
  cloudShare,
  imageTime,
  imageUrl,
  IR_CLOUDS,
  latestFromCapabilities,
  LIGHTNING,
  maskShare,
  mercatorBbox,
  paintClouds,
  recentTimes,
  wmsTime,
} from './eumetsat';
import { fieldGeometry } from './mercator';

const T = Date.parse('2026-09-14T07:40:00Z') / 1000;

// Trimmed from the real per-layer capabilities (September 2026).
const CAPABILITIES = `<WMS_Capabilities version="1.3.0"><Capability><Layer><Layer queryable="1" opaque="0">
<Name>li_afa</Name><Title>LI Accumulated Flash Area - MTG-I - 0 degree</Title>
<Dimension name="time" default="2026-09-14T07:40:00Z" units="ISO8601" nearestValue="1">2025-05-30T15:00:00.000Z/2026-09-14T07:40:00.000Z/PT5M</Dimension>
</Layer></Layer></Capability></WMS_Capabilities>`;

describe('satellite times', () => {
  it('reads the newest image from the layer capabilities', () => {
    expect(capabilitiesUrl(LIGHTNING)).toBe('https://view.eumetsat.int/geoserver/mtg_fd/li_afa/ows?service=WMS&version=1.3.0&request=GetCapabilities');
    expect(latestFromCapabilities(CAPABILITIES)).toBe(T);
    expect(latestFromCapabilities('<Dimension name="elevation" default="2"/>')).toBeNull();
  });

  it('shows the last image at or before a moment, never one that does not exist yet', () => {
    expect(imageTime(LIGHTNING, T + 4 * 60, null)).toBe(T);
    expect(imageTime(IR_CLOUDS, T + 9 * 60, T)).toBe(T);
    expect(imageTime(IR_CLOUDS, T + 25 * 60, T)).toBe(T);
    expect(recentTimes(IR_CLOUDS, T, 3)).toEqual([T - 1200, T - 600, T]);
    expect(wmsTime(T)).toBe('2026-09-14T07:40:00Z');
  });
});

describe('imageUrl', () => {
  it('asks for exactly the canvas, in the map projection', () => {
    const g = fieldGeometry({ south: 41.5, west: 14, north: 52, east: 33 }, 6);
    const url = new URL(imageUrl(IR_CLOUDS, g, T));
    expect(url.searchParams.get('layers')).toBe('mtg_fd:ir105_hrfi');
    expect(url.searchParams.get('crs')).toBe('EPSG:3857');
    expect([url.searchParams.get('width'), url.searchParams.get('height')]).toEqual([String(g.width), String(g.height)]);
    expect(url.searchParams.get('time')).toBe('2026-09-14T07:40:00Z');
    const [west, south, east, north] = mercatorBbox(g).split(',').map(Number);
    expect(west).toBeCloseTo((g.bounds.west * 20037508.342789244) / 180, 0);
    expect(east).toBeGreaterThan(west);
    expect(north).toBeGreaterThan(south);
  });
});

describe('clouds from the satellite pictures', () => {
  it('reads cloud height from infrared and cloud presence from the mask colours', () => {
    expect(cloudShare(40)).toBe(0);
    expect(cloudShare(120)).toBeGreaterThan(0.3);
    expect(cloudShare(200)).toBe(1);
    expect(maskShare(255, 255, 255, 255)).toBe(1);
    expect(maskShare(0, 190, 0, 255)).toBe(0);
    expect(maskShare(0, 0, 255, 255)).toBe(0);
  });

  it('draws clear ground transparent, low cloud as a veil, high cloud near white', () => {
    const ir = Uint8ClampedArray.from([
      40, 40, 40, 255, // clear land
      45, 45, 45, 255, // low warm cloud: infrared alone would call it ground
      190, 190, 190, 255, // high cold top
      190, 190, 190, 0, // no infrared value, but the mask says cloud
    ]);
    const mask = Uint8ClampedArray.from([0, 190, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255]);
    paintClouds(ir, mask);
    expect(ir[3]).toBe(0);
    expect(ir[7]).toBe(90);
    expect(ir[4]).toBe(148);
    expect(ir[11]).toBe(230);
    expect(ir[8]).toBeGreaterThan(230);
    expect(ir[15]).toBe(90);
  });
});
