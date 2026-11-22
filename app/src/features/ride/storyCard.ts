// The picture of a ride to share: its line, its numbers and its story, drawn
// on a canvas in the app's colours, in the 4:5 shape social apps show whole.
// Nothing leaves the phone until the rider shares it.

export interface CardInput {
  /** "Spre Brașov" or "Tură liberă". */
  title: string;
  /** "Duminică 22 nov · 09:30". */
  when: string;
  points: ReadonlyArray<{ lat: number; lon: number }>;
  stats: ReadonlyArray<{ label: string; value: string }>;
  lines: readonly string[];
  brand: string;
}

const W = 1080;
const H = 1350;
const PAD = 64;
const MAP = { x: PAD, y: 250, w: W - 2 * PAD, h: 520 };
/** More points than this only cost time: the line is thinned evenly. */
const MAX_LINE_POINTS = 1500;

function cssColor(name: string, fallback: string): string {
  const value = getComputedStyle(document.documentElement).getPropertyValue(name).trim();
  return value || fallback;
}

function cssFont(name: string, fallback: string): string {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim() || fallback;
}

/** The ride's line fitted into the box, north up, kept to scale. */
function project(points: CardInput['points']): Array<[number, number]> {
  const step = Math.max(1, Math.ceil(points.length / MAX_LINE_POINTS));
  const thin = points.filter((_, i) => i % step === 0 || i === points.length - 1);
  const midLat = thin.reduce((s, p) => s + p.lat, 0) / Math.max(1, thin.length);
  const kx = Math.cos((midLat * Math.PI) / 180);
  const xs = thin.map((p) => p.lon * kx);
  const ys = thin.map((p) => -p.lat);
  const [minX, maxX, minY, maxY] = [Math.min(...xs), Math.max(...xs), Math.min(...ys), Math.max(...ys)];
  const inner = 40;
  const scale = Math.min((MAP.w - 2 * inner) / Math.max(maxX - minX, 1e-9), (MAP.h - 2 * inner) / Math.max(maxY - minY, 1e-9));
  const offX = MAP.x + (MAP.w - (maxX - minX) * scale) / 2;
  const offY = MAP.y + (MAP.h - (maxY - minY) * scale) / 2;
  return xs.map((x, i) => [offX + (x - minX) * scale, offY + (ys[i] - minY) * scale]);
}

/** Words onto lines no wider than `width`, at most `max` lines, the last one cut with an ellipsis. */
function wrap(ctx: CanvasRenderingContext2D, text: string, width: number, max: number): string[] {
  const words = text.split(' ');
  const out: string[] = [];
  let line = '';
  for (const word of words) {
    const next = line ? `${line} ${word}` : word;
    if (ctx.measureText(next).width <= width || !line) line = next;
    else {
      out.push(line);
      line = word;
    }
  }
  if (line) out.push(line);
  if (out.length <= max) return out;
  const kept = out.slice(0, max);
  let last = kept[max - 1];
  while (last.length > 1 && ctx.measureText(`${last}…`).width > width) last = last.slice(0, -1);
  kept[max - 1] = `${last}…`;
  return kept;
}

export async function drawStoryCard(input: CardInput): Promise<Blob> {
  await document.fonts?.ready;
  const canvas = document.createElement('canvas');
  canvas.width = W;
  canvas.height = H;
  const ctx = canvas.getContext('2d');
  if (!ctx) throw new Error('No 2D canvas on this device');
  const bg = '#0b0c0a';
  const accent = cssColor('--accent', '#b4f03c');
  const ink = '#eef1e8';
  const muted = '#9aa093';
  const numFont = cssFont('--font-num', 'system-ui, sans-serif');
  const bodyFont = cssFont('--font-body', 'system-ui, sans-serif');

  ctx.fillStyle = bg;
  ctx.fillRect(0, 0, W, H);

  ctx.fillStyle = accent;
  ctx.font = `700 30px ${numFont}`;
  ctx.fillText(input.brand.toUpperCase().split('').join(' '), PAD, 96);
  ctx.fillStyle = ink;
  ctx.font = `700 64px ${numFont}`;
  ctx.fillText(wrap(ctx, input.title, W - 2 * PAD, 1)[0], PAD, 174);
  ctx.fillStyle = muted;
  ctx.font = `500 32px ${bodyFont}`;
  ctx.fillText(input.when, PAD, 222);

  // The line, on a darker panel, from a ringed start to a filled end.
  ctx.fillStyle = '#151713';
  ctx.beginPath();
  ctx.roundRect(MAP.x, MAP.y, MAP.w, MAP.h, 28);
  ctx.fill();
  const xy = project(input.points);
  if (xy.length >= 2) {
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.strokeStyle = bg;
    ctx.lineWidth = 18;
    ctx.beginPath();
    xy.forEach(([x, y], i) => (i === 0 ? ctx.moveTo(x, y) : ctx.lineTo(x, y)));
    ctx.stroke();
    ctx.strokeStyle = accent;
    ctx.lineWidth = 10;
    ctx.stroke();
    const [sx, sy] = xy[0];
    const [ex, ey] = xy[xy.length - 1];
    ctx.lineWidth = 6;
    ctx.fillStyle = bg;
    ctx.beginPath();
    ctx.arc(sx, sy, 14, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
    ctx.fillStyle = ink;
    ctx.beginPath();
    ctx.arc(ex, ey, 14, 0, Math.PI * 2);
    ctx.fill();
  }

  // The numbers, side by side.
  const cols = Math.max(1, input.stats.length);
  const colW = (W - 2 * PAD) / cols;
  input.stats.forEach((stat, i) => {
    const x = PAD + i * colW;
    ctx.fillStyle = ink;
    ctx.font = `700 ${cols > 4 ? 50 : 58}px ${numFont}`;
    ctx.fillText(wrap(ctx, stat.value, colW - 16, 1)[0], x, 870);
    ctx.fillStyle = muted;
    ctx.font = `600 24px ${numFont}`;
    ctx.fillText(wrap(ctx, stat.label.toUpperCase(), colW - 16, 1)[0], x, 910);
  });

  // The story, a dot in front of each line.
  let y = 990;
  ctx.font = `500 34px ${bodyFont}`;
  for (const text of input.lines.slice(0, 5)) {
    const rows = wrap(ctx, text, W - 2 * PAD - 40, 2);
    if (y + rows.length * 44 > H - 90) break;
    ctx.fillStyle = accent;
    ctx.beginPath();
    ctx.arc(PAD + 10, y - 11, 8, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = ink;
    rows.forEach((row, i) => ctx.fillText(row, PAD + 40, y + i * 44));
    y += rows.length * 44 + 22;
  }

  ctx.fillStyle = muted;
  ctx.font = `600 26px ${numFont}`;
  ctx.textAlign = 'right';
  ctx.fillText(input.brand, W - PAD, H - 48);

  return new Promise((resolve, reject) => {
    canvas.toBlob((blob) => (blob ? resolve(blob) : reject(new Error('The picture could not be made'))), 'image/png');
  });
}
