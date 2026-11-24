import { CORE } from '../../../i18n/core';
import { fmtTemp, hourOf } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { tierColor, tierOf } from '../../../lib/scoring';
import { Banner } from '../../../ui/primitives';
import type { CurrentWeather } from '../../../lib/types';
import type { RideWindow } from '../logic/bestWindow';
import { skyOf, type SkyKind } from '../logic/sky';
import { warningText, type WarningKind } from '../logic/warnings';
import { S_NOW } from '../strings';
import type { NowModel } from '../useNowModel';
import { tierWord } from './tierWord';

const SEVERE: ReadonlySet<WarningKind> = new Set(['storm', 'ice', 'frost']);

interface HeroProps {
  score: number | null;
  /** "Scorul tău", when the rider's own rides make it differ from the general score. */
  personal: number | null;
  model: NowModel;
  current: CurrentWeather;
  onScore: () => void;
}

// The weather first, as on the sky behind it: the temperature and what the
// sky is doing, the verdict, then the score on one row with a bar, which
// opens its explanation.

export function Hero({ score, personal, model, current, onScore }: HeroProps) {
  const s = useStrings(S_NOW);
  const core = useStrings(CORE);
  const lang = useLang();
  const word = tierWord(tierOf(score), core);
  const aria = score == null ? s.scoreAriaNone : fmt(s.scoreAria, { score, tier: word });
  const { title, sub } = model.headline;
  const skyWord: Record<SkyKind, string> = {
    clear: s.skyClear,
    partly: s.skyPartly,
    overcast: s.skyOvercast,
    fog: s.skyFog,
    drizzle: s.skyDrizzle,
    rain: s.skyRain,
    storm: s.skyStorm,
    snow: s.skySnow,
  };
  const value = score == null ? 0 : Math.max(0, Math.min(100, score));
  const color = tierColor(score);

  return (
    <section className="now-hero" aria-labelledby="now-verdict">
      <p className="now-hero__temp">{fmtTemp(current.temperature, lang)}</p>
      <p className="now-hero__sky">{skyWord[skyOf(current).kind]}</p>
      <h1 id="now-verdict" className="now-hero__verdict">
        {title}
      </h1>
      {sub && <p className="now-hero__sub">{sub}</p>}
      <button type="button" className="now-score" onClick={onScore} aria-label={aria} aria-haspopup="dialog">
        <span className="now-score__label">{s.scoreLabel}</span>
        <span className="now-score__num">{score ?? '–'}</span>
        <span className="now-score__bar" aria-hidden="true">
          <span className="now-score__fill" style={{ width: `${value}%`, background: color }} />
        </span>
        <span className="now-score__tier" style={{ color }}>
          {word}
        </span>
      </button>
      {personal !== null && score !== null && (
        <p className="now-hero__mine num" title={s.mineHint}>
          <span className="now-pill__dot" style={{ background: tierColor(personal) }} aria-hidden="true" />
          {fmt(s.mineLine, { score: personal, general: score })}
        </p>
      )}
      {model.warnings.length > 0 && (
        <div className="now-hero__warnings">
          {model.warnings.map((w) => (
            <Banner key={w.kind} tone={SEVERE.has(w.kind) ? 'error' : 'warn'} icon="alert">
              {warningText(w, model.nowIso, lang)}
            </Banner>
          ))}
        </div>
      )}
      <WindowPills today={model.windows.today} tomorrow={model.windows.tomorrow} />
    </section>
  );
}

function WindowPills({ today, tomorrow }: { today: RideWindow | null; tomorrow: RideWindow | null }) {
  const s = useStrings(S_NOW);
  const items = [
    today ? { key: 'today', w: today, template: s.windowToday } : null,
    tomorrow ? { key: 'tomorrow', w: tomorrow, template: s.windowTomorrow } : null,
  ].filter((x): x is { key: string; w: RideWindow; template: string } => x !== null);
  if (!items.length) return null;
  return (
    <div className="now-pills">
      <span className="eyebrow">{s.windowLabel}</span>
      <ul className="now-pills__row">
        {items.map(({ key, w, template }) => (
          <li key={key} className="now-pill num">
            <span className="now-pill__dot" style={{ background: tierColor(w.avg) }} aria-hidden="true" />
            {fmt(template, { from: hourOf(w.from), to: hourOf(w.to), score: w.avg })}
          </li>
        ))}
      </ul>
    </div>
  );
}
