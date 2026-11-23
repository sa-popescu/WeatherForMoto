import { blurChannel, CHANNELS, type ValueField } from './rainValues';
import { colourAt, type ColourStop } from './rainPalette';

// Lightning as values (rainValues.ts), drawn by the same layer as rain
// (valueLayer.ts) as soft glowing patches: a cream core with an amber rim,
// which reads over blue rain and over both map themes. The source is
// EUMETSAT's accumulated flash area, an image in 20 colours from few flashes
// to many; the first byte of the field is how many, the second is unused.
// Pure helpers only.

/** EUMETSAT's lightning colours, from few flashes to many (YlOrRd). */
const FLASH_COLOURS = [
  'fef9bd', 'fef3ad', 'ffec9e', 'fee58f', 'fede80', 'fed571', 'fec862', 'feba53', 'fead49', 'fea042',
  'fd933e', 'fd8236', 'fd6e2f', 'fc562a', 'f64426', 'ed3321', 'e41e1d', 'd41021', 'c30624', 'b10026',
].map((hex) => [0, 2, 4].map((i) => Number.parseInt(hex.slice(i, i + 2), 16)));

/** The fewest flashes still start well up the scale, so a lone flash keeps its glow once softened. */
const FIRST_LEVEL = 110;
const LEVEL_STEP = 7;

const levelOf = new Map<number, number>();

function flashLevel(r: number, g: number, b: number): number {
  const key = (r << 16) | (g << 8) | b;
  let level = levelOf.get(key);
  if (level === undefined) {
    let best = 0;
    let bestDistance = Number.POSITIVE_INFINITY;
    FLASH_COLOURS.forEach(([cr, cg, cb], i) => {
      const distance = (r - cr) ** 2 + (g - cg) ** 2 + (b - cb) ** 2;
      if (distance < bestDistance) {
        bestDistance = distance;
        best = i;
      }
    });
    level = FIRST_LEVEL + best * LEVEL_STEP;
    levelOf.set(key, level);
  }
  return level;
}

/**
 * Lightning from EUMETSAT's image stretched over the canvas. The stretch
 * leaves see-through pixels at the edges; they count as much as they show.
 * A light blur then makes each flash area a soft patch.
 */
export function lightningField(rgba: Uint8ClampedArray, width: number, height: number): ValueField {
  const count = width * height;
  const field: ValueField = new Uint8ClampedArray(count * CHANNELS);
  for (let p = 0; p < count; p += 1) {
    const i = p * 4;
    const a = rgba[i + 3];
    if (a === 0) continue;
    field[p * CHANNELS] = (flashLevel(rgba[i], rgba[i + 1], rgba[i + 2]) * a) / 255;
  }
  blurChannel(field, width, height, 0, 1, 2);
  return field;
}

/**
 * By the field's first byte (not dBZ): an amber rim, then a see-through cream
 * core, so a large storm does not hide the rain under it.
 */
const LIGHTNING_STOPS: readonly ColourStop[] = [
  { dbz: 4, rgb: [217, 119, 6], alpha: 0 },
  { dbz: 16, rgb: [245, 158, 11], alpha: 0.85 },
  { dbz: 40, rgb: [254, 243, 199], alpha: 0.55 },
  { dbz: 255, rgb: [255, 251, 235], alpha: 0.65 },
];

/** 256 x 2 RGBA for the layer, both rows the same. */
export function lightningPalette(): Uint8Array {
  const out = new Uint8Array(256 * 2 * 4);
  for (let level = 0; level < 256; level += 1) {
    const [r, g, b, a] = colourAt(LIGHTNING_STOPS, level);
    const rgba = [r, g, b, a * 255].map(Math.round);
    out.set(rgba, level * 4);
    out.set(rgba, (256 + level) * 4);
  }
  return out;
}

/** The legend's swatch: a cream core with the amber rim. */
export const LIGHTNING_SWATCH = 'radial-gradient(circle, rgb(255 251 235) 0 40%, rgb(245 158 11) 100%)';
