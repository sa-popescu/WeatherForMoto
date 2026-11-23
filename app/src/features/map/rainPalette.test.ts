import { describe, expect, it } from 'vitest';
import { colourAt, legendGradient, paletteTexture, RAIN_LEGEND, RAIN_STOPS, SNOW_LEGEND, SNOW_STOPS } from './rainPalette';
import { dbzToCode, mmToDbz } from './rainValues';

describe('rain palette', () => {
  it('draws nothing for clutter and drizzle, and light rain see-through', () => {
    expect(colourAt(RAIN_STOPS, 5)[3]).toBe(0);
    expect(colourAt(RAIN_STOPS, mmToDbz(0.15))[3]).toBe(0);
    const light = colourAt(RAIN_STOPS, mmToDbz(0.6))[3];
    expect(light).toBeGreaterThan(0.5);
    expect(light).toBeLessThan(1);
    expect(colourAt(RAIN_STOPS, mmToDbz(20))[3]).toBe(1);
  });

  it('turns from blue to warm colours as the rain gets heavy', () => {
    const [r1, , b1] = colourAt(RAIN_STOPS, mmToDbz(1));
    const [r2, , b2] = colourAt(RAIN_STOPS, mmToDbz(15));
    expect(b1).toBeGreaterThan(r1);
    expect(r2).toBeGreaterThan(b2);
  });

  it('lays rain and snow out in two rows of the texture, one column per code', () => {
    const texture = paletteTexture();
    expect(texture).toHaveLength(256 * 2 * 4);
    expect(Array.from(texture.slice(0, 4))).toEqual([0, 0, 0, 0]);
    const heavy = dbzToCode(45) * 4;
    expect(texture[heavy + 3]).toBe(255);
    const snow = (256 + dbzToCode(30)) * 4;
    const [r, g, b] = colourAt(SNOW_STOPS, 30);
    expect(Array.from(texture.slice(snow, snow + 3))).toEqual([r, g, b].map(Math.round));
  });

  it('shows every band of the legend in the colours it is drawn in', () => {
    expect(RAIN_LEGEND.map((band) => band.key)).toEqual(['legendLight', 'legendModerate', 'legendHeavy', 'legendExtreme']);
    for (const band of [...RAIN_LEGEND, SNOW_LEGEND]) {
      expect(band.toDbz).toBeGreaterThan(band.fromDbz);
      expect(legendGradient(band.stops, band.fromDbz, band.toDbz)).toMatch(/^linear-gradient\(90deg, rgb\(/);
    }
  });
});
