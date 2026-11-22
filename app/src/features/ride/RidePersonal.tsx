import { fmt, useStrings } from '../../lib/i18n';
import { MIN_RIDES } from './logic/personal';
import { useLearned } from './personalStore';
import { STORY_S } from './storyStrings';

// "Scorul tău" on "Ture": how far the learning is, then what it learned (the
// cold, gusts and heat this rider has ridden in), in one sentence.

export function RidePersonal() {
  const s = useStrings(STORY_S);
  const { rides, tolerance } = useLearned();

  let text: string;
  if (!tolerance) {
    text = fmt(s.mineLearning, { n: Math.min(rides, MIN_RIDES), need: MIN_RIDES });
  } else {
    const parts = [
      tolerance.coldFeelsC !== null ? fmt(s.mineCold, { c: Math.round(tolerance.coldFeelsC) }) : null,
      tolerance.gustKmh !== null ? fmt(s.mineGust, { g: Math.round(tolerance.gustKmh) }) : null,
      tolerance.heatFeelsC !== null ? fmt(s.mineHeat, { h: Math.round(tolerance.heatFeelsC) }) : null,
    ].filter((p): p is string => p !== null);
    const joined = parts.length > 1 ? `${parts.slice(0, -1).join(', ')}${s.and}${parts[parts.length - 1]}` : (parts[0] ?? '');
    text = fmt(s.mineReady, { parts: joined });
  }

  return (
    <section className="ride-mine" aria-labelledby="ride-mine-title">
      <h2 id="ride-mine-title" className="eyebrow">
        {s.mineTitle}
      </h2>
      {!tolerance && (
        <div className="ride-mine__bar" role="progressbar" aria-valuemin={0} aria-valuemax={MIN_RIDES} aria-valuenow={Math.min(rides, MIN_RIDES)}>
          <span style={{ width: `${(Math.min(rides, MIN_RIDES) / MIN_RIDES) * 100}%` }} />
        </div>
      )}
      <p className="ride-mine__text">{text}</p>
    </section>
  );
}
