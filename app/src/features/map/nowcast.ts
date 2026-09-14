import { TILE_SIZE, type FieldGeometry } from './mercator';

// Radar extrapolation ("nowcast"): the newest radar picture moved on along the
// direction and speed its rain areas travelled over the last frames. This is
// what keeps the band continuous: ten minutes after the last radar frame the
// map shows the same rain a little further on, not a different picture from a
// model. The model takes over gradually (see modelWeight). Pure helpers only,
// working on raw RGBA buffers.

/** One radar interval, and how far the extrapolation reaches. */
export const NOWCAST_STEP_S = 600;
export const NOWCAST_STEPS = 9;

/** The model starts to show through after 20 minutes and is on its own after 100. */
const MODEL_FADE_START_S = 20 * 60;
const MODEL_FADE_S = 80 * 60;

/** Share of the model in a frame extrapolated `leadSec` ahead of the last radar picture. */
export function modelWeight(leadSec: number): number {
  return Math.max(0, Math.min(1, (leadSec - MODEL_FADE_START_S) / MODEL_FADE_S));
}

// ---- Radar colours back to echo strength -------------------------------------

/** RainViewer "Universal Blue" (colour scheme 2), dBZ 15 to 65, from its public colour table. */
const UNIVERSAL_BLUE = [
  '88ddee', '6cd1eb', '51c5e8', '36bae5', '1baee2', '00a3e0', '009ad5', '0091ca', '0088bf', '007fb4',
  '0077aa', '0070a3', '00699c', '006295', '005b8e', '005588', '005180', '004e78', '004a70', '004768',
  'ffee00', 'ffe000', 'ffd200', 'ffc500', 'ffb700', 'ffaa00', 'ff9f00', 'ff9500', 'ff8b00', 'ff8100',
  'ff4400', 'f23600', 'e62800', 'd91b00', 'cd0d00', 'c10000', 'a80000', '8f0000', '760000', '5d0000',
  'ffaaff', 'ff9fff', 'ff95ff', 'ff8bff', 'ff81ff', 'ff77ff', 'ff6cff', 'ff62ff', 'ff58ff', 'ff4eff',
  'ffffff',
];
const FIRST_DBZ = 15;
const PALETTE = UNIVERSAL_BLUE.map((hex) => [0, 2, 4].map((i) => Number.parseInt(hex.slice(i, i + 2), 16)));
/** The table's semi-transparent greys below 15 dBZ are drizzle and clutter, not rain to follow. */
const MIN_ALPHA = 160;
/**
 * Nearest palette entry per colour quantised to 5 bits a channel, filled on
 * first use. Smoothed tiles hold thousands of in-between shades, so a cache of
 * exact colours would miss constantly; 32 768 entries cover them all.
 */
const LEVELS = new Int8Array(1 << 15).fill(-1);

function nearestLevel(r: number, g: number, b: number): number {
  let best = 0;
  let bestDistance = Number.POSITIVE_INFINITY;
  for (let i = 0; i < PALETTE.length; i += 1) {
    const [pr, pg, pb] = PALETTE[i];
    const distance = (r - pr) ** 2 + (g - pg) ** 2 + (b - pb) ** 2;
    if (distance < bestDistance) {
      bestDistance = distance;
      best = i;
    }
  }
  return FIRST_DBZ - 10 + best;
}

/** Echo strength behind a tile pixel as dBZ above 10 (15 dBZ is 5), or 0 for no rain. */
export function echoLevel(r: number, g: number, b: number, a: number): number {
  if (a < MIN_ALPHA) return 0;
  const key = ((r >> 3) << 10) | ((g >> 3) << 5) | (b >> 3);
  let level = LEVELS[key];
  if (level < 0) {
    // The centre of the quantised cell stands for every colour in it.
    level = nearestLevel(((r >> 3) << 3) + 4, ((g >> 3) << 3) + 4, ((b >> 3) << 3) + 4);
    LEVELS[key] = level;
  }
  return level;
}

export interface EchoField {
  data: Float32Array;
  width: number;
  height: number;
}

/** Echo levels averaged over factor x factor pixels: the grid motion is measured on. */
export function echoField(rgba: Uint8ClampedArray, width: number, height: number, factor: number): EchoField {
  const w = Math.floor(width / factor);
  const h = Math.floor(height / factor);
  const data = new Float32Array(w * h);
  const area = factor * factor;
  for (let y = 0; y < h; y += 1) {
    for (let x = 0; x < w; x += 1) {
      let sum = 0;
      for (let fy = 0; fy < factor; fy += 1) {
        const row = (y * factor + fy) * width;
        for (let fx = 0; fx < factor; fx += 1) {
          const i = (row + x * factor + fx) * 4;
          if (rgba[i + 3] !== 0) sum += echoLevel(rgba[i], rgba[i + 1], rgba[i + 2], rgba[i + 3]);
        }
      }
      data[y * w + x] = sum / area;
    }
  }
  return { data, width: w, height: h };
}

// ---- Motion ---------------------------------------------------------------

export interface MotionOptions {
  /** Pixels per working cell. */
  factor: number;
  /** Block side in working cells. */
  blockCells: number;
  /** Farthest shift tried per radar interval, in working cells. */
  radiusCells: number;
}

const EARTH_KM = 40_075;
/** Fastest rain area followed; faster ones are rare and would widen the search a lot. */
const MAX_SPEED_KMH = 120;
const TARGET_CELL_KM = 3.5;
const BLOCK_KM = 40;

/** Working grid and search sizes in kilometres, whatever the zoom. */
export function motionOptions(geometry: FieldGeometry): MotionOptions {
  const midLat = (geometry.bounds.north + geometry.bounds.south) / 2;
  const kmPerPx = (EARTH_KM * Math.cos((midLat * Math.PI) / 180)) / (TILE_SIZE * 2 ** geometry.z);
  const factor = Math.max(1, Math.round(TARGET_CELL_KM / kmPerPx));
  const cellKm = kmPerPx * factor;
  return {
    factor,
    blockCells: Math.max(6, Math.round(BLOCK_KM / cellKm)),
    radiusCells: Math.max(2, Math.ceil((MAX_SPEED_KMH * NOWCAST_STEP_S) / 3600 / cellKm)),
  };
}

export interface BlockMotion {
  cols: number;
  rows: number;
  /** Shift per interval, working cells, per block. */
  dx: Float32Array;
  dy: Float32Array;
  valid: Uint8Array;
}

/** A block is matched only when this share of it is rain. */
const MIN_RAIN_SHARE = 0.08;
/** And only when this share of it can still be compared after the shift. */
const MIN_OVERLAP = 0.6;
/** A shift has to beat standing still clearly, or the match is ambiguous (even rain everywhere). */
const CLEAR_WIN = 0.97;

/**
 * Mean absolute difference between a block of `curr` and the same block of
 * `prev` moved back by (dx, dy). Only the part whose source stays on the
 * picture is compared. Stops early (Infinity) once the mean cannot come in
 * under `limit`, which is what keeps a full search affordable.
 */
function blockCost(
  prev: EchoField,
  curr: EchoField,
  x0: number,
  y0: number,
  x1: number,
  y1: number,
  dx: number,
  dy: number,
  limit: number = Number.POSITIVE_INFINITY,
): number {
  const ys = Math.max(y0, dy);
  const ye = Math.min(y1, prev.height + dy);
  const xs = Math.max(x0, dx);
  const xe = Math.min(x1, prev.width + dx);
  const count = Math.max(0, ye - ys) * Math.max(0, xe - xs);
  if (count < (x1 - x0) * (y1 - y0) * MIN_OVERLAP) return Number.POSITIVE_INFINITY;
  const budget = limit * count;
  const a = curr.data;
  const b = prev.data;
  let sum = 0;
  for (let y = ys; y < ye; y += 1) {
    let ci = y * curr.width + xs;
    let pi = (y - dy) * prev.width + (xs - dx);
    for (let x = xs; x < xe; x += 1, ci += 1, pi += 1) {
      const d = a[ci] - b[pi];
      sum += d < 0 ? -d : d;
    }
    if (sum > budget) return Number.POSITIVE_INFINITY;
  }
  return sum / count;
}

/** Where the minimum of a parabola through three costs sits, relative to the middle one. */
function parabolaOffset(minus: number, centre: number, plus: number): number {
  if (!Number.isFinite(minus) || !Number.isFinite(plus)) return 0;
  const curvature = minus - 2 * centre + plus;
  return curvature > 1e-9 ? Math.max(-0.5, Math.min(0.5, (minus - plus) / (2 * curvature))) : 0;
}

/**
 * Block matching: for each rainy block of `curr`, the shift that best lines it
 * up with `prev` (content at x - shift moved to x), refined below one cell.
 */
export function matchBlocks(prev: EchoField, curr: EchoField, blockCells: number, radius: number): BlockMotion {
  const cols = Math.max(1, Math.ceil(curr.width / blockCells));
  const rows = Math.max(1, Math.ceil(curr.height / blockCells));
  const dx = new Float32Array(cols * rows);
  const dy = new Float32Array(cols * rows);
  const valid = new Uint8Array(cols * rows);

  for (let by = 0; by < rows; by += 1) {
    for (let bx = 0; bx < cols; bx += 1) {
      const x0 = bx * blockCells;
      const y0 = by * blockCells;
      const x1 = Math.min(curr.width, x0 + blockCells);
      const y1 = Math.min(curr.height, y0 + blockCells);
      let rainy = 0;
      for (let y = y0; y < y1; y += 1) for (let x = x0; x < x1; x += 1) if (curr.data[y * curr.width + x] > 0) rainy += 1;
      if (rainy < (x1 - x0) * (y1 - y0) * MIN_RAIN_SHARE) continue;

      // Standing still is measured in full: it is the bar every shift has to clear.
      const still = blockCost(prev, curr, x0, y0, x1, y1, 0, 0);
      let best = still;
      let bestX = 0;
      let bestY = 0;
      for (let sy = -radius; sy <= radius; sy += 1) {
        for (let sx = -radius; sx <= radius; sx += 1) {
          if (sx === 0 && sy === 0) continue;
          const cost = blockCost(prev, curr, x0, y0, x1, y1, sx, sy, best);
          if (cost < best) {
            best = cost;
            bestX = sx;
            bestY = sy;
          }
        }
      }
      if (!Number.isFinite(best)) continue;
      if ((bestX !== 0 || bestY !== 0) && best > still * CLEAR_WIN) continue;
      // The neighbours of the best shift, measured in full for the sub-cell fit.
      const at = (sx: number, sy: number): number =>
        Math.abs(sx) <= radius && Math.abs(sy) <= radius ? blockCost(prev, curr, x0, y0, x1, y1, sx, sy) : Number.POSITIVE_INFINITY;
      const i = by * cols + bx;
      dx[i] = bestX + parabolaOffset(at(bestX - 1, bestY), best, at(bestX + 1, bestY));
      dy[i] = bestY + parabolaOffset(at(bestX, bestY - 1), best, at(bestX, bestY + 1));
      valid[i] = 1;
    }
  }
  return { cols, rows, dx, dy, valid };
}

function median(values: number[]): number {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

/** A vector further than this (cells per interval) from its neighbours is an outlier. */
const OUTLIER_CELLS = 1.5;
/** Blocks without their own match borrow from matched blocks this close. */
const FILL_BLOCKS = 2;

/**
 * Turns block matches into one smooth field: outliers replaced by their
 * neighbours' median, empty blocks filled from nearby matches (or the overall
 * median), then smoothed. No match at all means standing still.
 */
export function regularize(motion: BlockMotion): { dx: Float32Array; dy: Float32Array } {
  const { cols, rows, valid } = motion;
  const size = cols * rows;
  const dx = new Float32Array(size);
  const dy = new Float32Array(size);
  const allX: number[] = [];
  const allY: number[] = [];
  for (let i = 0; i < size; i += 1) {
    if (!valid[i]) continue;
    allX.push(motion.dx[i]);
    allY.push(motion.dy[i]);
  }
  if (allX.length === 0) return { dx, dy };
  const globalX = median(allX);
  const globalY = median(allY);

  const cleanX = Float32Array.from(motion.dx);
  const cleanY = Float32Array.from(motion.dy);
  for (let by = 0; by < rows; by += 1) {
    for (let bx = 0; bx < cols; bx += 1) {
      const i = by * cols + bx;
      if (!valid[i]) continue;
      const nx: number[] = [];
      const ny: number[] = [];
      for (let oy = -1; oy <= 1; oy += 1) {
        for (let ox = -1; ox <= 1; ox += 1) {
          const x = bx + ox;
          const y = by + oy;
          if (x < 0 || y < 0 || x >= cols || y >= rows || !valid[y * cols + x]) continue;
          nx.push(motion.dx[y * cols + x]);
          ny.push(motion.dy[y * cols + x]);
        }
      }
      if (nx.length < 3) continue;
      const mx = median(nx);
      const my = median(ny);
      if (Math.hypot(motion.dx[i] - mx, motion.dy[i] - my) > OUTLIER_CELLS) {
        cleanX[i] = mx;
        cleanY[i] = my;
      }
    }
  }

  for (let by = 0; by < rows; by += 1) {
    for (let bx = 0; bx < cols; bx += 1) {
      const i = by * cols + bx;
      if (valid[i]) {
        dx[i] = cleanX[i];
        dy[i] = cleanY[i];
        continue;
      }
      let sumX = 0;
      let sumY = 0;
      let weight = 0;
      for (let oy = -FILL_BLOCKS; oy <= FILL_BLOCKS; oy += 1) {
        for (let ox = -FILL_BLOCKS; ox <= FILL_BLOCKS; ox += 1) {
          const x = bx + ox;
          const y = by + oy;
          if (x < 0 || y < 0 || x >= cols || y >= rows || !valid[y * cols + x]) continue;
          const w = 1 / Math.hypot(ox, oy);
          sumX += cleanX[y * cols + x] * w;
          sumY += cleanY[y * cols + x] * w;
          weight += w;
        }
      }
      dx[i] = weight > 0 ? sumX / weight : globalX;
      dy[i] = weight > 0 ? sumY / weight : globalY;
    }
  }

  for (let pass = 0; pass < 2; pass += 1) {
    const srcX = Float32Array.from(dx);
    const srcY = Float32Array.from(dy);
    for (let by = 0; by < rows; by += 1) {
      for (let bx = 0; bx < cols; bx += 1) {
        let sumX = 0;
        let sumY = 0;
        let count = 0;
        for (let oy = -1; oy <= 1; oy += 1) {
          for (let ox = -1; ox <= 1; ox += 1) {
            const x = bx + ox;
            const y = by + oy;
            if (x < 0 || y < 0 || x >= cols || y >= rows) continue;
            sumX += srcX[y * cols + x];
            sumY += srcY[y * cols + x];
            count += 1;
          }
        }
        dx[by * cols + bx] = sumX / count;
        dy[by * cols + bx] = sumY / count;
      }
    }
  }
  return { dx, dy };
}

export interface RadarPicture {
  rgba: Uint8ClampedArray;
  timeSec: number;
}

/** Smooth motion per block, in canvas pixels per radar interval. */
export interface MotionField {
  cols: number;
  rows: number;
  blockPx: number;
  dx: Float32Array;
  dy: Float32Array;
}

/**
 * Motion from consecutive radar pictures (oldest first). Each pair is matched
 * on its own and scaled to one interval; the newest pair counts double, since
 * it is the best guess for what comes next.
 */
export function estimateMotion(pictures: readonly RadarPicture[], width: number, height: number, options: MotionOptions): MotionField {
  const fields = pictures.map((picture) => echoField(picture.rgba, width, height, options.factor));
  const cols = Math.max(1, Math.ceil((fields[0]?.width ?? 1) / options.blockCells));
  const rows = Math.max(1, Math.ceil((fields[0]?.height ?? 1) / options.blockCells));
  const sumX = new Float32Array(cols * rows);
  const sumY = new Float32Array(cols * rows);
  const weight = new Float32Array(cols * rows);

  for (let i = 1; i < fields.length; i += 1) {
    const gap = pictures[i].timeSec - pictures[i - 1].timeSec;
    if (gap < NOWCAST_STEP_S / 2 || gap > NOWCAST_STEP_S * 3) continue;
    const pair = matchBlocks(fields[i - 1], fields[i], options.blockCells, Math.ceil((options.radiusCells * gap) / NOWCAST_STEP_S));
    const scale = NOWCAST_STEP_S / gap;
    const w = i === fields.length - 1 ? 2 : 1;
    for (let b = 0; b < cols * rows; b += 1) {
      if (!pair.valid[b]) continue;
      sumX[b] += pair.dx[b] * scale * w;
      sumY[b] += pair.dy[b] * scale * w;
      weight[b] += w;
    }
  }

  const combined: BlockMotion = {
    cols,
    rows,
    dx: sumX.map((v, b) => (weight[b] > 0 ? v / weight[b] : 0)),
    dy: sumY.map((v, b) => (weight[b] > 0 ? v / weight[b] : 0)),
    valid: Uint8Array.from(weight, (w) => (w > 0 ? 1 : 0)),
  };
  const smooth = regularize(combined);
  return {
    cols,
    rows,
    blockPx: options.blockCells * options.factor,
    dx: smooth.dx.map((v) => v * options.factor),
    dy: smooth.dy.map((v) => v * options.factor),
  };
}

// ---- Moving the picture on ---------------------------------------------------

/** Motion at a canvas point, read between block centres. */
function velocityAt(motion: MotionField, x: number, y: number, out: { x: number; y: number }): void {
  const u = Math.max(0, Math.min(motion.cols - 1, x / motion.blockPx - 0.5));
  const v = Math.max(0, Math.min(motion.rows - 1, y / motion.blockPx - 0.5));
  const c0 = Math.floor(u);
  const r0 = Math.floor(v);
  const c1 = Math.min(motion.cols - 1, c0 + 1);
  const r1 = Math.min(motion.rows - 1, r0 + 1);
  const fx = u - c0;
  const fy = v - r0;
  const w00 = (1 - fx) * (1 - fy);
  const w10 = fx * (1 - fy);
  const w01 = (1 - fx) * fy;
  const w11 = fx * fy;
  const i00 = r0 * motion.cols + c0;
  const i10 = r0 * motion.cols + c1;
  const i01 = r1 * motion.cols + c0;
  const i11 = r1 * motion.cols + c1;
  out.x = motion.dx[i00] * w00 + motion.dx[i10] * w10 + motion.dx[i01] * w01 + motion.dx[i11] * w11;
  out.y = motion.dy[i00] * w00 + motion.dy[i10] * w10 + motion.dy[i01] * w01 + motion.dy[i11] * w11;
}

/** Trajectories are traced on this lattice and interpolated in between. */
const LATTICE_PX = 4;

/**
 * The radar picture `steps` intervals later. Each pixel follows the motion
 * back, one interval at a time, and takes the colour it finds there. Colours
 * are copied, never mixed, so the picture keeps the radar's own palette; a
 * trail that leaves the canvas finds no rain.
 */
export function extrapolate(
  source: Uint8ClampedArray,
  width: number,
  height: number,
  motion: MotionField,
  steps: number,
): Uint8ClampedArray<ArrayBuffer> {
  const pixels = new Uint8ClampedArray(width * height * 4);
  const lw = Math.floor(width / LATTICE_PX) + 2;
  const lh = Math.floor(height / LATTICE_PX) + 2;
  const backX = new Float32Array(lw * lh);
  const backY = new Float32Array(lw * lh);
  const velocity = { x: 0, y: 0 };
  for (let j = 0; j < lh; j += 1) {
    for (let i = 0; i < lw; i += 1) {
      let x = i * LATTICE_PX;
      let y = j * LATTICE_PX;
      for (let s = 0; s < steps; s += 1) {
        velocityAt(motion, x, y, velocity);
        x -= velocity.x;
        y -= velocity.y;
      }
      backX[j * lw + i] = i * LATTICE_PX - x;
      backY[j * lw + i] = j * LATTICE_PX - y;
    }
  }
  for (let py = 0; py < height; py += 1) {
    const gy = py / LATTICE_PX;
    const j0 = Math.floor(gy);
    const fy = gy - j0;
    for (let px = 0; px < width; px += 1) {
      const gx = px / LATTICE_PX;
      const i0 = Math.floor(gx);
      const fx = gx - i0;
      const a = j0 * lw + i0;
      const b = a + 1;
      const c = a + lw;
      const d = c + 1;
      const bx = (backX[a] * (1 - fx) + backX[b] * fx) * (1 - fy) + (backX[c] * (1 - fx) + backX[d] * fx) * fy;
      const by = (backY[a] * (1 - fx) + backY[b] * fx) * (1 - fy) + (backY[c] * (1 - fx) + backY[d] * fx) * fy;
      const sx = Math.round(px - bx);
      const sy = Math.round(py - by);
      if (sx < 0 || sy < 0 || sx >= width || sy >= height) continue;
      const from = (sy * width + sx) * 4;
      const to = (py * width + px) * 4;
      pixels[to] = source[from];
      pixels[to + 1] = source[from + 1];
      pixels[to + 2] = source[from + 2];
      pixels[to + 3] = source[from + 3];
    }
  }
  return pixels;
}
