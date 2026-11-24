import { memo, useMemo, type CSSProperties } from 'react';
import type { Sky } from '../logic/sky';
import '../sky.css';

// The weather right now, drawn behind the forecast: a sky for the hour of
// the day, clouds that drift, rain that falls past and drops that run down
// the glass, snow, fog, a storm's flashes. Plain CSS on a few dozen
// elements, every animation a transform or an opacity, so a phone composits
// it without repainting; it is decoration, hidden from screen readers.
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
    '--s': `${(6 + rnd() * 10).toFixed(1)}px`,
    '--dur': `${(7 + rnd() * 9).toFixed(1)}s`,
    '--delay': `${(-rnd() * 16).toFixed(1)}s`,
  }));
}

const CLOUDS = [
  { '--top': '-4%', '--w': '78vw', '--dur': '150s', '--delay': '-40s', '--o': '0.95' },
  { '--top': '10%', '--w': '64vw', '--dur': '190s', '--delay': '-120s', '--o': '0.75' },
  { '--top': '22%', '--w': '90vw', '--dur': '230s', '--delay': '-10s', '--o': '0.6' },
  { '--top': '4%', '--w': '56vw', '--dur': '170s', '--delay': '-90s', '--o': '0.85' },
] as Vars[];

const CLOUDS_FOR = { clear: 0, partly: 2, overcast: 4, fog: 0, drizzle: 4, rain: 4, storm: 4, snow: 3 } as const;

export const WeatherSky = memo(function WeatherSky({ sky }: { sky: Sky }) {
  const { kind, night } = sky;
  const wet = kind === 'drizzle' || kind === 'rain' || kind === 'storm';
  const drops = useMemo(() => (wet ? glassDrops(kind === 'drizzle' ? 9 : 16, 7) : []), [wet, kind]);
  const clouds = CLOUDS.slice(0, CLOUDS_FOR[kind]);

  return (
    <div className="sky" data-sky={kind} data-night={night || undefined} aria-hidden="true">
      {kind === 'clear' && !night && (
        <>
          <div className="sky__sun" />
          <div className="sky__rays" />
        </>
      )}
      {night && (kind === 'clear' || kind === 'partly') && (
        <>
          <div className="sky__stars" />
          <div className="sky__stars sky__stars--b" />
        </>
      )}
      {kind === 'partly' && !night && <div className="sky__sun sky__sun--behind" />}
      {clouds.map((style, i) => (
        <div key={i} className="sky__cloud" style={style} />
      ))}
      {kind === 'fog' && (
        <>
          <div className="sky__fog" />
          <div className="sky__fog sky__fog--b" />
        </>
      )}
      {wet && (
        <>
          <div className="sky__rain" />
          {kind !== 'drizzle' && <div className="sky__rain sky__rain--near" />}
          <div className="sky__glass" />
          {drops.map((style, i) => (
            <span key={i} className="sky__drop" style={style} />
          ))}
        </>
      )}
      {kind === 'storm' && <div className="sky__flash" />}
      {kind === 'snow' && (
        <>
          <div className="sky__snow" />
          <div className="sky__snow sky__snow--near" />
        </>
      )}
      <div className="sky__scrim" />
    </div>
  );
});
