import { describe, expect, it } from 'vitest';
import { lightningField, lightningPalette } from './lightningValues';
import { CHANNELS } from './rainValues';

describe('lightningField', () => {
  /** A width x width image with one square of lit pixels in the middle. */
  function image(width: number, side: number, rgba: [number, number, number, number]): Uint8ClampedArray {
    const out = new Uint8ClampedArray(width * width * 4);
    const from = Math.floor((width - side) / 2);
    for (let y = from; y < from + side; y += 1) for (let x = from; x < from + side; x += 1) out.set(rgba, (y * width + x) * 4);
    return out;
  }

  it('reads more flashes from the redder colours, and nothing where there is none', () => {
    const few = lightningField(image(9, 3, [0xfe, 0xf9, 0xbd, 255]), 9, 9);
    const many = lightningField(image(9, 3, [0xb1, 0x00, 0x26, 255]), 9, 9);
    const centre = (4 * 9 + 4) * CHANNELS;
    expect(few[centre]).toBeGreaterThan(50);
    expect(many[centre]).toBeGreaterThan(few[centre]);
    expect(few[0]).toBe(0);
  });

  it('keeps a lone flash visible once softened', () => {
    const lone = lightningField(image(5, 1, [0xfe, 0xf9, 0xbd, 255]), 5, 5);
    const value = lone[(2 * 5 + 2) * CHANNELS];
    const palette = lightningPalette();
    expect(palette[value * 4 + 3]).toBeGreaterThan(0);
  });
});

describe('lightningPalette', () => {
  it('rims a patch in amber and fills its core with a see-through cream', () => {
    const palette = lightningPalette();
    const at = (level: number): number[] => Array.from(palette.slice(level * 4, level * 4 + 4));
    expect(at(0)[3]).toBe(0);
    const rim = at(16);
    const core = at(120);
    expect(rim[2]).toBeLessThan(core[2]);
    expect(rim[3]).toBeGreaterThan(core[3]);
    expect(core[3]).toBeGreaterThan(120);
    expect(core[3]).toBeLessThan(200);
  });
});
