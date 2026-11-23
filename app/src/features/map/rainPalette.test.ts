import { describe, expect, it } from 'vitest';
import { colourAt, legendGradient, paletteTexture, rainLegend, RAIN_STOPS, RAIN_STOPS_DAY, snowLegend, SNOW_STOPS, SNOW_STOPS_DAY } from './rainPalette';
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

  it('draws light rain and snow deeper and less see-through on the day map', () => {
    const at = mmToDbz(0.6);
    const [nr, ng, nb, na] = colourAt(RAIN_STOPS, at);
    const [dr, dg, db, da] = colourAt(RAIN_STOPS_DAY, at);
    expect(dr + dg + db).toBeLessThan(nr + ng + nb);
    expect(da).toBeGreaterThan(na);
    expect(colourAt(SNOW_STOPS_DAY, 20)[3]).toBeGreaterThan(colourAt(SNOW_STOPS, 20)[3]);
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
    const day = paletteTexture('day');
    const [dr, dg, dbl] = colourAt(SNOW_STOPS_DAY, 30);
    expect(Array.from(day.slice(snow, snow + 3))).toEqual([dr, dg, dbl].map(Math.round));
  });

  it('shows every band of the legend in the colours it is drawn in', () => {
    for (const theme of ['dark', 'day'] as const) {
      const bands = rainLegend(theme);
      expect(bands.map((band) => band.key)).toEqual(['legendLight', 'legendModerate', 'legendHeavy', 'legendExtreme']);
      for (const band of [...bands, snowLegend(theme)]) {
        expect(band.toDbz).toBeGreaterThan(band.fromDbz);
        expect(legendGradient(band.stops, band.fromDbz, band.toDbz)).toMatch(/^linear-gradient\(90deg, rgb\(/);
      }
    }
    expect(rainLegend('day')[0].stops).toBe(RAIN_STOPS_DAY);
  });
});
