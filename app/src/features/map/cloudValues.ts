import { cloudShare, maskShare } from './eumetsat';
import type { FieldGeometry } from './mercator';
import type { LevelOf } from './nowcast';
import type { GridBox } from './rainPaint';
import { blurPlane, CHANNELS, gridToField, type ValueField } from './rainValues';

// Clouds as values (rainValues.ts), drawn by the same layer as rain
// (valueLayer.ts): the first byte is how much cloud a pixel shows (its
// opacity, 0 to 255), the second how high and cold its top is (0 low, a slate
// veil; 255 high, near white). Pure helpers only.

/** A low cloud the infrared barely sees still shows this opaque; a high cold top up to the second. */
const LOW_CLOUD_AMOUNT = 90;
const HIGH_CLOUD_AMOUNT = 230;
/** Less than this is a clear sky. */
const CLEAR = 8;
/** The mask's cells are 4 to 6 km: its share is blurred over a few kilometres, or its squares would show. */
const MASK_BLUR_RADIUS = 3;

/**
 * Clouds from the satellite images, both stretched over the canvas. The mask
 * decides where cloud is, including the low cloud the infrared misses; the
 * infrared decides how high it is. A cold top the mask has not caught yet
 * still shows.
 */
export function cloudField(ir: Uint8ClampedArray, mask: Uint8ClampedArray, width: number, height: number): ValueField {
  const count = width * height;
  const presence = new Uint8ClampedArray(count);
  for (let p = 0; p < count; p += 1) {
    const i = p * 4;
    presence[p] = maskShare(mask[i], mask[i + 1], mask[i + 2], mask[i + 3]) * 255;
  }
  blurPlane(presence, width, height, 1, 0, MASK_BLUR_RADIUS, 2);

  const field: ValueField = new Uint8ClampedArray(count * CHANNELS);
  for (let p = 0; p < count; p += 1) {
    const i = p * 4;
    const height01 = ir[i + 3] === 0 ? 0 : cloudShare(ir[i]);
    const there = Math.max(presence[p] / 255, height01 * height01);
    const amount = there * (LOW_CLOUD_AMOUNT + (HIGH_CLOUD_AMOUNT - LOW_CLOUD_AMOUNT) * height01);
    if (amount < CLEAR) continue;
    field[p * CHANNELS] = amount;
    field[p * CHANNELS + 1] = height01 * 255;
  }
  return field;
}

/**
 * How much a forecast cloud cover (percent) shows: nothing under a few
 * percent, a thin haze rising to a solid deck, never fully opaque so the map
 * stays readable. It starts gently, so a clear patch has a soft edge.
 */
export function coverAmount(cover: number): number {
  if (!(cover > 5)) return 0;
  const share = Math.min(100, cover) / 100;
  return Math.min(1, (cover - 5) / 10) * (60 + share * 165);
}

/** Forecast cloud cover as a field: the model knows no cloud height, so it is the slate veil. */
export function coverField(grid: GridBox, values: Float32Array, geometry: FieldGeometry): ValueField {
  return gridToField(grid, values, geometry, coverAmount);
}

/** Motion level of a cloud field in RGBA form (rainValues.fieldToRgba): its amount, thin haze left out. */
export const amountLevel: LevelOf = (r, _g, _b, a) => (a === 0 || r < 20 ? 0 : r / 6);

const SLATE = [148, 163, 184] as const;
const WHITE = [241, 245, 249] as const;
/** From CLEAR up to here the opacity rises from nothing, so the edge of a cloud is soft. */
const SOFT_EDGE = 24;

/** 256 x 2 RGBA for the layer: slate in the first row, white in the second, opaque as much as the amount. */
export function cloudPalette(): Uint8Array {
  const out = new Uint8Array(256 * 2 * 4);
  [SLATE, WHITE].forEach((rgb, row) => {
    for (let amount = 0; amount < 256; amount += 1) {
      const alpha = amount < CLEAR ? 0 : amount < SOFT_EDGE ? (amount * (amount - CLEAR)) / (SOFT_EDGE - CLEAR) : amount;
      out.set([rgb[0], rgb[1], rgb[2], Math.round(alpha)], (row * 256 + amount) * 4);
    }
  });
  return out;
}
