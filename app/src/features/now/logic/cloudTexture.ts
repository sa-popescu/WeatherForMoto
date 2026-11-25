// Clouds that look like clouds: fractal value noise, drawn once into a small
// canvas (WeatherSky then paints it, scaled, into the sky and drifts it).
// The noise wraps left to right, so copies side by side join without a
// seam and the drift can loop. A few tens of milliseconds, once per kind
// of sky.

export interface CloudLook {
  /** Share of the sky covered, 0 to 1. */
  cover: number;
  /** Colour of the lit tops and of the dense, shadowed parts, as [r, g, b]. */
  light: [number, number, number];
  dark: [number, number, number];
  /** Peak opacity of the thickest cloud. */
  alpha: number;
  seed: number;
}

const W = 384;
const H = 192;
const cache = new Map<string, HTMLCanvasElement | null>();

function hash(x: number, y: number, seed: number): number {
  let h = (x * 374761393 + y * 668265263 + seed * 2147483647) | 0;
  h = Math.imul(h ^ (h >>> 13), 1274126177);
  return ((h ^ (h >>> 16)) >>> 0) / 4294967295;
}

/** Value noise on a lattice `period` cells wide, wrapping in x. */
function noise(x: number, y: number, period: number, seed: number): number {
  const x0 = Math.floor(x);
  const y0 = Math.floor(y);
  const fx = x - x0;
  const fy = y - y0;
  const sx = fx * fx * (3 - 2 * fx);
  const sy = fy * fy * (3 - 2 * fy);
  const xa = ((x0 % period) + period) % period;
  const xb = (xa + 1) % period;
  const a = hash(xa, y0, seed);
  const b = hash(xb, y0, seed);
  const c = hash(xa, y0 + 1, seed);
  const d = hash(xb, y0 + 1, seed);
  return a + (b - a) * sx + (c - a) * sy + (a - b - c + d) * sx * sy;
}

function fbm(u: number, v: number, seed: number): number {
  let sum = 0;
  let amp = 0.5;
  let norm = 0;
  for (let o = 0; o < 5; o += 1) {
    const period = 4 << o;
    sum += amp * noise(u * period, v * period, period, seed + o * 17);
    norm += amp;
    amp *= 0.5;
  }
  return sum / norm;
}

const smooth = (e0: number, e1: number, x: number): number => {
  const t = Math.min(1, Math.max(0, (x - e0) / (e1 - e0)));
  return t * t * (3 - 2 * t);
};

/** The clouds, twice as wide as tall, or null where there is no canvas (tests). */
export function cloudTexture(look: CloudLook): HTMLCanvasElement | null {
  const key = JSON.stringify(look);
  if (cache.has(key)) return cache.get(key) ?? null;
  let out: HTMLCanvasElement | null = null;
  try {
    const canvas = document.createElement('canvas');
    canvas.width = W;
    canvas.height = H;
    const ctx = canvas.getContext('2d');
    if (ctx) {
      const img = ctx.createImageData(W, H);
      // Higher cover lowers the density a pixel needs to show cloud.
      const threshold = 0.62 - look.cover * 0.42;
      for (let y = 0; y < H; y += 1) {
        const v = y / H;
        // Thinner towards the bottom of the band, so it fades into the sky.
        const fade = 1 - smooth(0.55, 1, v);
        for (let x = 0; x < W; x += 1) {
          const u = x / W;
          // Cells a little wider than tall: clouds lie in layers.
          const d = fbm(u, v * 0.7, look.seed);
          const body = smooth(threshold, threshold + 0.22, d) * fade;
          // Denser parts are darker underneath: shade by density and by depth.
          const shade = smooth(threshold + 0.05, threshold + 0.4, d) * (0.45 + 0.55 * v);
          const i = (y * W + x) * 4;
          img.data[i] = look.light[0] + (look.dark[0] - look.light[0]) * shade;
          img.data[i + 1] = look.light[1] + (look.dark[1] - look.light[1]) * shade;
          img.data[i + 2] = look.light[2] + (look.dark[2] - look.light[2]) * shade;
          img.data[i + 3] = 255 * look.alpha * body;
        }
      }
      ctx.putImageData(img, 0, 0);
      out = canvas;
    }
  } catch {
    out = null;
  }
  cache.set(key, out);
  return out;
}
