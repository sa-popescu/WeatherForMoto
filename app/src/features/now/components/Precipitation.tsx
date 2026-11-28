import { useEffect, useRef } from 'react';
import type { Precip } from '../logic/sky';

// Rain or snow falling past, in depth, on one canvas. Far drops and flakes
// are small, faint and slow; near ones large and quick, and a few right past
// the eye are out of focus. The wind tilts all of them and its gusts come and
// go over a few seconds. Snowflakes are ragged clumps, the nearest single
// six-armed crystals; they sway, turn and tumble as they fall. Raindrops are streaks
// as long as the distance they fall while the eye takes them in, so a
// downpour looks like a downpour and drizzle like a fine mist.
// A frame is a few hundred drawImage calls of small sprites made once.
// Stops while the screen is hidden, stands still under reduced motion.

export type PrecipKind = 'snow' | 'rain' | 'drizzle' | 'storm';

interface Particle {
  x: number;
  y: number;
  /** Depth, 0 far to 1 near. */
  z: number;
  /** Fall speed, px/s. */
  vy: number;
  size: number;
  alpha: number;
  /** Snow: sway phase, how far (px) and how fast (rad/s); which sprite. */
  phase: number;
  sway: number;
  freq: number;
  sprite: number;
  /** Snow: turn (rad), how fast it turns (rad/s) and tumbles (rad/s). */
  angle: number;
  spin: number;
  tumble: number;
}

const rand = (min: number, max: number): number => min + Math.random() * (max - min);

function canvasOf(d: number): [HTMLCanvasElement, CanvasRenderingContext2D | null] {
  const c = document.createElement('canvas');
  c.width = d;
  c.height = d;
  return [c, c.getContext('2d')];
}

/**
 * A flake as most of them really are: a clump of crystals stuck together,
 * an irregular, fluffy shape, brighter where it is thicker.
 */
function clumpSprite(colour: string, seed: number): HTMLCanvasElement {
  const d = 32;
  const [c, ctx] = canvasOf(d);
  if (!ctx) return c;
  let x = seed * 9301 + 49297;
  const rnd = (): number => {
    x = (x * 16807) % 2147483647;
    return (x % 10000) / 10000;
  };
  const blobs = 5 + Math.floor(rnd() * 5);
  for (let i = 0; i < blobs; i++) {
    const a = rnd() * Math.PI * 2;
    const r = rnd() * 6.5;
    const bx = d / 2 + Math.cos(a) * r;
    const by = d / 2 + Math.sin(a) * r * 0.8;
    const br = 3 + rnd() * 4.5;
    const g = ctx.createRadialGradient(bx, by, 0, bx, by, br);
    g.addColorStop(0, colour);
    g.addColorStop(0.45, colour);
    g.addColorStop(1, 'rgba(255,255,255,0)');
    ctx.globalAlpha = 0.6 + rnd() * 0.4;
    ctx.fillStyle = g;
    ctx.beginPath();
    ctx.arc(bx, by, br, 0, Math.PI * 2);
    ctx.fill();
  }
  // A few loose crystal needles sticking out of the clump.
  ctx.globalAlpha = 0.7;
  ctx.strokeStyle = colour;
  ctx.lineWidth = 0.9;
  ctx.lineCap = 'round';
  for (let i = 0; i < 4; i++) {
    const a = rnd() * Math.PI * 2;
    ctx.beginPath();
    ctx.moveTo(d / 2 + Math.cos(a) * 4, d / 2 + Math.sin(a) * 4);
    ctx.lineTo(d / 2 + Math.cos(a) * (9 + rnd() * 4), d / 2 + Math.sin(a) * (9 + rnd() * 4));
    ctx.stroke();
  }
  return c;
}

/**
 * A single crystal, as the near flakes show it: six arms with side branches
 * (a stellar dendrite) around a small hexagonal plate, with a faint glow.
 * `blur` draws it out of focus, for the flakes right past the eye.
 */
function crystalSprite(colour: string, seed: number, blur: number): HTMLCanvasElement {
  const d = 64;
  const R = 26;
  const [c, ctx] = canvasOf(d);
  if (!ctx) return c;
  let x = seed * 7919 + 104729;
  const rnd = (): number => {
    x = (x * 16807) % 2147483647;
    return (x % 10000) / 10000;
  };
  const branches = 2 + Math.floor(rnd() * 2);
  const branchAt = Array.from({ length: branches }, (_, i) => 0.3 + (i / branches) * 0.55 + rnd() * 0.08);
  const branchLen = branchAt.map((at) => (1 - at) * (0.45 + rnd() * 0.35));
  const plate = 0.12 + rnd() * 0.12;
  ctx.translate(d / 2, d / 2);
  if (blur > 0 && 'filter' in ctx) ctx.filter = `blur(${blur}px)`;
  ctx.strokeStyle = colour;
  ctx.fillStyle = colour;
  ctx.lineCap = 'round';
  ctx.shadowColor = colour;
  ctx.shadowBlur = 3;
  // The plate in the middle.
  ctx.globalAlpha = 0.55;
  ctx.beginPath();
  for (let k = 0; k < 6; k++) {
    const a = (k * Math.PI) / 3;
    const px = Math.cos(a) * R * plate;
    const py = Math.sin(a) * R * plate;
    if (k === 0) ctx.moveTo(px, py);
    else ctx.lineTo(px, py);
  }
  ctx.closePath();
  ctx.fill();
  // The six arms, each the same, as a real crystal grows.
  ctx.globalAlpha = 0.95;
  for (let k = 0; k < 6; k++) {
    ctx.save();
    ctx.rotate((k * Math.PI) / 3);
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(0, 0);
    ctx.lineTo(R, 0);
    ctx.stroke();
    ctx.lineWidth = 1.4;
    branchAt.forEach((at, i) => {
      const bx = R * at;
      const len = R * branchLen[i];
      for (const side of [-1, 1]) {
        ctx.beginPath();
        ctx.moveTo(bx, 0);
        ctx.lineTo(bx + len * 0.5, side * len * 0.866);
        ctx.stroke();
      }
    });
    ctx.restore();
  }
  return c;
}

/** Sprites: clumps, then sharp crystals, then crystals out of focus. */
const CLUMPS = 8;
const CRYSTALS = 5;

function snowSprites(colour: string): HTMLCanvasElement[] {
  return [
    ...Array.from({ length: CLUMPS }, (_, i) => clumpSprite(colour, i + 1)),
    ...Array.from({ length: CRYSTALS }, (_, i) => crystalSprite(colour, i + 1, 0)),
    ...Array.from({ length: CRYSTALS }, (_, i) => crystalSprite(colour, i + 11, 3)),
  ];
}

const pick = (n: number): number => Math.floor(Math.random() * n);

function flake(w: number, h: number, anywhere: boolean): Particle {
  // Most flakes are far: small clumps that read as specks. Nearer, the clumps
  // show their ragged shape, and the nearest few are single crystals whose
  // six arms you can see. One in twenty-five passes right by the eye, large
  // and out of focus.
  const close = Math.random() < 0.04;
  const z = close ? 1 : Math.random() ** 1.5 * 0.95;
  const crystal = close ? Math.random() < 0.6 : z > 0.72 && Math.random() < 0.45;
  const sprite = crystal ? CLUMPS + (close ? CRYSTALS : 0) + pick(CRYSTALS) : pick(CLUMPS);
  const size = close ? rand(14, 24) : crystal ? rand(7, 12) : 2.6 + z * z * 11 + rand(0, 1.6);
  return {
    x: rand(-0.1, 1.1) * w,
    y: anywhere ? rand(0, h) : rand(-40, -10),
    z,
    vy: close ? rand(150, 190) : 26 + z * 95 + rand(-6, 10),
    size,
    alpha: close ? 0.5 : 0.55 + z * 0.45,
    phase: rand(0, Math.PI * 2),
    sway: 4 + z * 22 * rand(0.5, 1),
    freq: rand(0.6, 1.6),
    sprite,
    angle: rand(0, Math.PI * 2),
    spin: rand(-1.5, 1.5),
    tumble: rand(0.8, 2.2),
  };
}

/** A streak: faint at its tail, brightest just behind the drop's head. */
function streakSprite(colour: string): HTMLCanvasElement {
  const c = document.createElement('canvas');
  c.width = 4;
  c.height = 64;
  const ctx = c.getContext('2d');
  if (!ctx) return c;
  const g = ctx.createLinearGradient(0, 0, 0, 64);
  g.addColorStop(0, 'rgba(255,255,255,0)');
  g.addColorStop(0.75, colour);
  g.addColorStop(1, 'rgba(255,255,255,0)');
  ctx.fillStyle = g;
  ctx.fillRect(1, 0, 2, 64);
  ctx.globalAlpha = 0.4;
  ctx.fillRect(0, 0, 4, 64);
  return c;
}

/** Raindrops in three depths: far, middle, near. */
const RAIN_LAYERS = [
  { width: 1.6, alpha: 0.3 },
  { width: 2.2, alpha: 0.45 },
  { width: 3.4, alpha: 0.5 },
];

function drop(w: number, h: number, anywhere: boolean, fine: boolean): Particle {
  const z = Math.random() ** 1.3;
  const layer = z < 0.45 ? 0 : z < 0.85 ? 1 : 2;
  // Drizzle: fine drops, falling slower. Rain: 6 to 9 m/s, seen as fast.
  const vy = fine ? 260 + z * 260 : 700 + z * 700;
  return {
    x: rand(-0.2, 1.2) * w,
    y: anywhere ? rand(-h * 0.1, h) : rand(-120, -20),
    z,
    vy: vy * rand(0.9, 1.1),
    size: fine ? 0.5 + z * 0.4 : 1,
    alpha: 1,
    phase: 0,
    sway: 0,
    freq: 0,
    sprite: layer,
    angle: 0,
    spin: 0,
    tumble: 0,
  };
}

const prefersStill = (): boolean => typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;

export function Precipitation({ kind, precip, active }: { kind: PrecipKind; precip: Precip; active: boolean }) {
  const ref = useRef<HTMLCanvasElement>(null);
  const { amount, wind, gust } = precip;

  useEffect(() => {
    const canvas = ref.current;
    const ctx = canvas?.getContext('2d');
    if (!canvas || !ctx || !active) return;
    const snow = kind === 'snow';
    const fine = kind === 'drizzle';
    const style = getComputedStyle(canvas);
    const flakeColour = style.getPropertyValue('--flake').trim() || 'rgb(255 255 255 / 0.9)';
    const dropColour = style.getPropertyValue('--drop').trim() || 'rgb(210 222 240)';
    const sprites = snow ? snowSprites(flakeColour) : [streakSprite(dropColour)];
    const dpr = Math.min(2, window.devicePixelRatio || 1);
    const make = (w: number, h: number, anywhere: boolean): Particle => (snow ? flake(w, h, anywhere) : drop(w, h, anywhere, fine));
    let parts: Particle[] = [];
    let w = 0;
    let h = 0;

    const fit = (): void => {
      const cw = canvas.clientWidth;
      const ch = canvas.clientHeight;
      if (cw === w && ch === h) return;
      w = cw;
      h = ch;
      canvas.width = Math.round(w * dpr);
      canvas.height = Math.round(h * dpr);
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      // On a phone screen: snow about 300 flakes in a moderate fall, 450 in a
      // heavy one; rain 120 to 330 streaks; drizzle many fine ones.
      const per = snow ? 800 : fine ? 650 : 650;
      const count = Math.round(((w * h) / per) * (0.2 + amount * 0.8));
      parts = Array.from({ length: Math.min(900, count) }, () => make(w, h, true));
    };

    let t = 0;
    let push = 0;

    const paintSnow = (): void => {
      for (const f of parts) {
        const x = f.x + Math.sin(t * f.freq + f.phase) * f.sway;
        const s = f.size;
        // Turning as it falls, and tumbling: seen edge-on now and then.
        const turn = f.angle + t * f.spin;
        const face = 0.35 + 0.65 * Math.abs(Math.cos(t * f.tumble + f.phase));
        const cos = Math.cos(turn) * dpr;
        const sin = Math.sin(turn) * dpr;
        // The near, quick flakes leave a short blur behind them.
        if (f.z > 0.6) {
          ctx.globalAlpha = f.alpha * 0.3;
          ctx.setTransform(cos * face, sin * face, -sin, cos, x * dpr, (f.y - f.vy * 0.018) * dpr);
          ctx.drawImage(sprites[f.sprite], -s, -s, s * 2, s * 2);
        }
        ctx.globalAlpha = f.alpha;
        ctx.setTransform(cos * face, sin * face, -sin, cos, x * dpr, f.y * dpr);
        ctx.drawImage(sprites[f.sprite], -s, -s, s * 2, s * 2);
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    // A streak is the path the drop travels in a moment of seeing, so it
    // leans with the wind and is longer for the near, faster drops.
    const exposure = fine ? 0.022 : 0.03;
    const paintRain = (): void => {
      const streak = sprites[0];
      for (const d of parts) {
        const layer = RAIN_LAYERS[d.sprite];
        const vx = push * (0.4 + d.z * 0.6);
        const len = Math.hypot(vx, d.vy) * exposure;
        const lw = layer.width * d.size;
        ctx.globalAlpha = layer.alpha * (fine ? 0.9 : 1);
        ctx.setTransform(dpr, 0, 0, dpr, d.x * dpr, d.y * dpr);
        ctx.rotate(-Math.atan2(vx, d.vy));
        ctx.drawImage(streak, -lw / 2, -len, lw, len);
      }
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    const paint = (): void => {
      ctx.clearRect(0, 0, w, h);
      if (snow) paintSnow();
      else paintRain();
      ctx.globalAlpha = 1;
    };

    const step = (dt: number): void => {
      t += dt;
      // The wind's push, px/s at the nearest depth: a steady part and gusts
      // that rise and fall over a few seconds. Rain is heavier than snow but
      // falls much faster, so it leans more.
      const gustNow = gust * Math.max(0, Math.sin(t * 0.7) * 0.6 + Math.sin(t * 1.9 + 1) * 0.4);
      const base = snow ? 70 : fine ? 120 : 380;
      push = wind * base * (1 + 1.6 * gustNow) + Math.sign(wind || 1) * base * 0.4 * gustNow;
      for (const p of parts) {
        p.y += p.vy * dt;
        p.x += push * (0.4 + p.z * 0.6) * dt;
        if (p.y - p.size > h) Object.assign(p, make(w, h, false));
        else if (p.x < -0.2 * w) p.x += 1.4 * w;
        else if (p.x > 1.2 * w) p.x -= 1.4 * w;
      }
    };

    fit();
    if (prefersStill()) {
      step(0);
      paint();
      return;
    }
    let frame = 0;
    let last = performance.now();
    const loop = (now: number): void => {
      // Up to 60 frames a second; a long pause (a hidden tab) does not jump.
      if (now - last >= 15) {
        fit();
        step(Math.min(0.05, (now - last) / 1000));
        paint();
        last = now;
      }
      frame = requestAnimationFrame(loop);
    };
    frame = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(frame);
  }, [kind, amount, wind, gust, active]);

  return <canvas ref={ref} className="sky__fall" />;
}
