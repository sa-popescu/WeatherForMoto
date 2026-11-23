import { describe, expect, it } from 'vitest';
import { amountLevel, cloudField, cloudPalette, coverAmount } from './cloudValues';
import { CHANNELS, fieldToRgba } from './rainValues';

const rgba = (pixels: number[][]): Uint8ClampedArray => Uint8ClampedArray.from(pixels.flat());

describe('cloudField', () => {
  it('shows clear ground as nothing, low cloud as a veil and a high cold top near white', () => {
    // One row of four, each pixel alone so the mask's blur does not mix them up: far apart.
    const width = 31;
    const ir: number[][] = Array.from({ length: width }, () => [40, 40, 40, 255]);
    const mask: number[][] = Array.from({ length: width }, () => [0, 190, 0, 255]);
    // Low warm cloud: infrared alone would call it ground, the mask says cloud (a wide patch, so the blur keeps it).
    for (let x = 8; x <= 14; x += 1) mask[x] = [255, 255, 255, 255];
    // A high cold top the mask has not caught yet.
    ir[26] = [190, 190, 190, 255];
    const field = cloudField(rgba(ir), rgba(mask), width, 1);
    const at = (x: number): [number, number] => [field[x * CHANNELS], field[x * CHANNELS + 1]];
    expect(at(0)).toEqual([0, 0]);
    expect(at(11)[0]).toBeGreaterThan(60);
    expect(at(11)[1]).toBe(0);
    expect(at(26)[0]).toBeGreaterThan(200);
    expect(at(26)[1]).toBe(255);
  });

  it('turns the edge of the mask into a slope, not a step', () => {
    const width = 20;
    const ir = rgba(Array.from({ length: width }, () => [40, 40, 40, 255]));
    const mask = rgba(Array.from({ length: width }, (_, x) => (x < 10 ? [255, 255, 255, 255] : [0, 0, 255, 255])));
    const field = cloudField(ir, mask, width, 1);
    const amounts = Array.from({ length: width }, (_, x) => field[x * CHANNELS]);
    expect(amounts[2]).toBeGreaterThan(amounts[9]);
    expect(amounts[9]).toBeGreaterThan(amounts[11]);
    expect(amounts[11]).toBeGreaterThan(0);
  });
});

describe('forecast cloud cover', () => {
  it('starts gently above a few percent and never quite covers the map', () => {
    expect(coverAmount(3)).toBe(0);
    expect(coverAmount(8)).toBeLessThan(coverAmount(15));
    expect(coverAmount(100)).toBeLessThan(255);
    expect(coverAmount(100)).toBeGreaterThan(200);
  });
});

describe('clouds on the move and on screen', () => {
  it('follows the cloud amount, thin haze left out', () => {
    const moving = fieldToRgba(Uint8ClampedArray.from([10, 0, 150, 200]));
    expect(amountLevel(moving[0], moving[1], moving[2], moving[3])).toBe(0);
    expect(amountLevel(moving[4], moving[5], moving[6], moving[7])).toBe(25);
  });

  it('colours low cloud slate and high cloud white, opaque as much as the amount', () => {
    const palette = cloudPalette();
    expect(palette[3]).toBe(0);
    expect(Array.from(palette.slice(200 * 4, 200 * 4 + 4))).toEqual([148, 163, 184, 200]);
    expect(Array.from(palette.slice((256 + 200) * 4, (256 + 200) * 4 + 4))).toEqual([241, 245, 249, 200]);
    // The edge of a cloud fades in.
    expect(palette[12 * 4 + 3]).toBeLessThan(12);
  });
});
