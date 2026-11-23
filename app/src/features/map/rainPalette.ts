import type { Theme } from '../../state/settings';
import { codeToDbz, mmToDbz } from './rainValues';

// The colours rain is drawn in, the same for the observed radar, the radar
// moved on and the model. Light rain is a see-through sky blue, so the roads
// show through it; it deepens to blue, then yellow, orange and red as it gets
// heavier, and magenta for cloudbursts and hail. Snow has its own violet.
// Below about 0.2 mm/h nothing is drawn: radar clutter and drizzle no one
// rides around. The day map is pale, so there every colour is a little
// deeper and less see-through, or light rain and snow would fade into it.
// Pure helpers only.

export interface ColourStop {
  dbz: number;
  rgb: readonly [number, number, number];
  /** 0 to 1. */
  alpha: number;
}

const SKY: ColourStop['rgb'] = [56, 189, 248];

export const RAIN_STOPS: readonly ColourStop[] = [
  { dbz: 12, rgb: SKY, alpha: 0 },
  { dbz: 17, rgb: SKY, alpha: 0.72 },
  { dbz: 25, rgb: [59, 130, 246], alpha: 0.85 },
  { dbz: 31, rgb: [37, 99, 235], alpha: 0.92 },
  { dbz: 36, rgb: [250, 204, 21], alpha: 1 },
  { dbz: 41, rgb: [249, 115, 22], alpha: 1 },
  { dbz: 47, rgb: [220, 38, 38], alpha: 1 },
  { dbz: 53, rgb: [217, 70, 239], alpha: 1 },
  { dbz: 60, rgb: [250, 232, 255], alpha: 1 },
];

/**
 * Snow shows weaker on radar than rain of the same amount, so its scale starts
 * a little lower, but not much: where snow turns to rain the two scales meet,
 * and a far lower start would draw the edge of RainViewer's coarse snow grid.
 */
export const SNOW_STOPS: readonly ColourStop[] = [
  { dbz: 9, rgb: [196, 181, 253], alpha: 0 },
  { dbz: 14, rgb: [196, 181, 253], alpha: 0.72 },
  { dbz: 22, rgb: [167, 139, 250], alpha: 0.85 },
  { dbz: 32, rgb: [139, 92, 246], alpha: 0.95 },
  { dbz: 42, rgb: [109, 40, 217], alpha: 1 },
];

const DAY_SKY: ColourStop['rgb'] = [14, 165, 233];

/** Rain on the day map: the same steps, deeper. */
export const RAIN_STOPS_DAY: readonly ColourStop[] = [
  { dbz: 12, rgb: DAY_SKY, alpha: 0 },
  { dbz: 17, rgb: DAY_SKY, alpha: 0.82 },
  { dbz: 25, rgb: [37, 99, 235], alpha: 0.9 },
  { dbz: 31, rgb: [29, 78, 216], alpha: 0.95 },
  { dbz: 36, rgb: [234, 179, 8], alpha: 1 },
  { dbz: 41, rgb: [234, 88, 12], alpha: 1 },
  { dbz: 47, rgb: [220, 38, 38], alpha: 1 },
  { dbz: 53, rgb: [192, 38, 211], alpha: 1 },
  { dbz: 60, rgb: [126, 34, 206], alpha: 1 },
];

/** Snow on the day map: a deeper violet. */
export const SNOW_STOPS_DAY: readonly ColourStop[] = [
  { dbz: 9, rgb: [139, 92, 246], alpha: 0 },
  { dbz: 14, rgb: [139, 92, 246], alpha: 0.78 },
  { dbz: 22, rgb: [124, 58, 237], alpha: 0.88 },
  { dbz: 32, rgb: [109, 40, 217], alpha: 0.95 },
  { dbz: 42, rgb: [91, 33, 182], alpha: 1 },
];

export const rainStops = (theme: Theme): readonly ColourStop[] => (theme === 'day' ? RAIN_STOPS_DAY : RAIN_STOPS);
export const snowStops = (theme: Theme): readonly ColourStop[] => (theme === 'day' ? SNOW_STOPS_DAY : SNOW_STOPS);

/** Colour and opacity at a reflectivity, straight between stops; nothing below the first. */
export function colourAt(stops: readonly ColourStop[], dbz: number): [number, number, number, number] {
  const first = stops[0];
  const last = stops[stops.length - 1];
  if (!(dbz > first.dbz)) return [...first.rgb, 0];
  if (dbz >= last.dbz) return [...last.rgb, last.alpha];
  let i = 0;
  while (dbz > stops[i + 1].dbz) i += 1;
  const a = stops[i];
  const b = stops[i + 1];
  const t = (dbz - a.dbz) / (b.dbz - a.dbz);
  const mix = (x: number, y: number): number => x + (y - x) * t;
  return [mix(a.rgb[0], b.rgb[0]), mix(a.rgb[1], b.rgb[1]), mix(a.rgb[2], b.rgb[2]), mix(a.alpha, b.alpha)];
}

/** 256 x 2 RGBA, one column per strength code: rain in the first row, snow in the second. */
export function paletteTexture(theme: Theme = 'dark'): Uint8Array {
  const out = new Uint8Array(256 * 2 * 4);
  [rainStops(theme), snowStops(theme)].forEach((stops, row) => {
    for (let code = 0; code < 256; code += 1) {
      const [r, g, b, a] = code === 0 ? [0, 0, 0, 0] : colourAt(stops, codeToDbz(code));
      out.set([r, g, b, a * 255].map(Math.round), (row * 256 + code) * 4);
    }
  });
  return out;
}

// ---- Legend ---------------------------------------------------------------------

export type LegendKey = 'legendLight' | 'legendModerate' | 'legendHeavy' | 'legendExtreme';

export interface LegendBand {
  stops: readonly ColourStop[];
  fromDbz: number;
  toDbz: number;
}

/**
 * The bands under the scrubber, by rain rate: light under 2.5 mm/h, moderate
 * to 10, heavy to 50, extreme above. Each shows the colours its range is
 * drawn in on the map of that theme.
 */
export function rainLegend(theme: Theme): ReadonlyArray<LegendBand & { key: LegendKey }> {
  const stops = rainStops(theme);
  return [
    { key: 'legendLight', stops, fromDbz: 17, toDbz: mmToDbz(2.5) },
    { key: 'legendModerate', stops, fromDbz: mmToDbz(2.5), toDbz: mmToDbz(10) },
    { key: 'legendHeavy', stops, fromDbz: mmToDbz(10), toDbz: mmToDbz(50) },
    { key: 'legendExtreme', stops, fromDbz: mmToDbz(50), toDbz: 60 },
  ];
}

/** Snow, from light to heavy, on its own line under the rain bands. */
export function snowLegend(theme: Theme): LegendBand {
  return { stops: snowStops(theme), fromDbz: 14, toDbz: 42 };
}

/** A CSS gradient through a band's colours, fully opaque so it reads on the panel. */
export function legendGradient(stops: readonly ColourStop[], fromDbz: number, toDbz: number): string {
  const samples = 5;
  const colours = Array.from({ length: samples }, (_, i) => {
    const [r, g, b] = colourAt(stops, fromDbz + ((toDbz - fromDbz) * i) / (samples - 1));
    return `rgb(${Math.round(r)} ${Math.round(g)} ${Math.round(b)})`;
  });
  return `linear-gradient(90deg, ${colours.join(', ')})`;
}
