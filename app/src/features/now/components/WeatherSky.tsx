import { memo, useEffect, useMemo, useRef, type CSSProperties } from 'react';
import { cloudTexture, type CloudLook } from '../logic/cloudTexture';
import type { Precip, Sky, SkyKind } from '../logic/sky';
import { Precipitation, type PrecipKind } from './Precipitation';
import '../sky.css';

// The weather right now, drawn behind the forecast: a sky for the hour of
// the day, clouds from fractal noise (logic/cloudTexture.ts) drifting
// slowly, rain or snow falling past in depth and leaning with the wind
// (Precipitation.tsx), drops that run down the glass, fog, a storm's
// flashes. The CSS layers animate only transform and opacity, so a phone
// composits them without repainting; it is decoration, hidden from screen
// readers.
// Reduced motion keeps the scene and stops it moving (sky.css).

/** Same drops on every render: positions from a small fixed-seed generator. */
function seeded(seed: number): () => number {
  let x = seed;
  return () => {
    x = (x * 16807) % 2147483647;
    return (x - 1) / 2147483646;
  };
}

type Vars = CSSProperties & Record<`--${string}`, string>;

function glassDrops(count: number, seed: number): Vars[] {
  const rnd = seeded(seed);
  return Array.from({ length: count }, () => ({
    '--x': `${(rnd() * 100).toFixed(1)}%`,
    '--y': `${(rnd() * 70).toFixed(1)}%`,
    '--s': `${(4 + rnd() * 8).toFixed(1)}px`,
    '--dur': `${(7 + rnd() * 9).toFixed(1)}s`,
    '--delay': `${(-rnd() * 16).toFixed(1)}s`,
  }));
}

/** How each sky's clouds look: cover, lit and shadowed colours, opacity. */
const LOOKS: Record<SkyKind, Omit<CloudLook, 'seed'> | null> = {
  clear: { cover: 0.1, light: [240, 244, 250], dark: [200, 208, 220], alpha: 0.45 },
  partly: { cover: 0.42, light: [238, 242, 248], dark: [150, 160, 176], alpha: 0.92 },
  overcast: { cover: 0.85, light: [190, 196, 206], dark: [92, 100, 114], alpha: 0.95 },
  fog: null,
  drizzle: { cover: 0.9, light: [168, 175, 186], dark: [80, 87, 100], alpha: 0.95 },
  rain: { cover: 0.96, light: [142, 150, 162], dark: [58, 64, 77], alpha: 0.97 },
  storm: { cover: 1, light: [112, 118, 130], dark: [32, 35, 45], alpha: 1 },
  // Snow clouds a flat, mid grey, so the white flakes show against them.
  snow: { cover: 0.95, light: [150, 158, 170], dark: [86, 94, 108], alpha: 0.95 },
};

const prefersStill = (): boolean => typeof matchMedia === 'function' && matchMedia('(prefers-reduced-motion: reduce)').matches;

/**
 * The clouds, painted into one canvas at one pixel per CSS pixel (they are
 * soft, so the browser's upscaling on a dense screen costs nothing visible)
 * and moved a few times a second: two layers, the near one faster. Stops
 * while the screen is hidden or the page is in the background, and stands
 * still under reduced motion.
 */
function CloudCanvas({ kind, active }: { kind: SkyKind; active: boolean }) {
  const ref = useRef<HTMLCanvasElement>(null);
  const look = LOOKS[kind];

  useEffect(() => {
    const canvas = ref.current;
    const ctx = canvas?.getContext('2d');
    if (!canvas || !ctx || !look || !active) return;
    const far = cloudTexture({ ...look, seed: 3 });
    const near = look.cover > 0.3 ? cloudTexture({ ...look, cover: look.cover * 0.8, seed: 11 }) : null;
    if (!far) return;
    const layers = [
      { tex: far, speed: 4, scale: 1, alpha: 1 },
      ...(near ? [{ tex: near, speed: 9, scale: 0.8, alpha: 0.8 }] : []),
    ];
    const start = performance.now();
    let frame = 0;
    let last = 0;

    const paint = (now: number): void => {
      const w = canvas.clientWidth;
      const h = canvas.clientHeight;
      if (canvas.width !== w || canvas.height !== h) {
        canvas.width = w;
        canvas.height = h;
      }
      ctx.clearRect(0, 0, w, h);
      const t = (now - start) / 1000;
      for (const layer of layers) {
        const th = h * layer.scale;
        const tw = th * (layer.tex.width / layer.tex.height);
        const x0 = -((t * layer.speed) % tw);
        ctx.globalAlpha = layer.alpha;
        for (let x = x0; x < w; x += tw) ctx.drawImage(layer.tex, x, 0, tw, th);
      }
      ctx.globalAlpha = 1;
    };

    if (prefersStill()) {
      paint(start);
      return;
    }
    // About 15 frames a second: the clouds move a few pixels a second.
    const loop = (now: number): void => {
      if (now - last > 66) {
        last = now;
        paint(now);
      }
      frame = requestAnimationFrame(loop);
    };
    frame = requestAnimationFrame(loop);
    return () => cancelAnimationFrame(frame);
  }, [look, active]);

  return look ? <canvas ref={ref} className="sky__clouds" /> : null;
}

const STEADY: Precip = { amount: 0.5, wind: 0.2, gust: 0.2 };

export const WeatherSky = memo(function WeatherSky({ sky, active, precip }: { sky: Sky; active: boolean; precip?: Precip }) {
  const { kind, night } = sky;
  const wet = kind === 'drizzle' || kind === 'rain' || kind === 'storm';
  const drops = useMemo(() => (wet ? glassDrops(kind === 'drizzle' ? 8 : 14, 7) : []), [wet, kind]);

  return (
    <div className="sky" data-sky={kind} data-night={night || undefined} aria-hidden="true">
      {kind === 'clear' && !night && <div className="sky__sun" />}
      {night && (kind === 'clear' || kind === 'partly') && (
        <>
          <div className="sky__stars" />
          <div className="sky__stars sky__stars--b" />
        </>
      )}
      {kind === 'partly' && !night && <div className="sky__sun sky__sun--behind" />}
      <CloudCanvas kind={kind} active={active} />
      {kind === 'fog' && (
        <>
          <div className="sky__fog" />
          <div className="sky__fog sky__fog--b" />
          <div className="sky__fog sky__fog--c" />
        </>
      )}
      {wet && (
        <>
          <div className="sky__veil" />
          <Precipitation kind={kind as PrecipKind} precip={precip ?? STEADY} active={active} />
          <div className="sky__glass" />
          {drops.map((style, i) => (
            <span key={i} className="sky__drop" style={style} />
          ))}
        </>
      )}
      {kind === 'storm' && (
        <>
          <div className="sky__flash" />
          <div className="sky__flash sky__flash--b" />
        </>
      )}
      {kind === 'snow' && (
        <>
          <div className="sky__veil" />
          <Precipitation kind="snow" precip={precip ?? STEADY} active={active} />
        </>
      )}
      <div className="sky__scrim" />
    </div>
  );
});
