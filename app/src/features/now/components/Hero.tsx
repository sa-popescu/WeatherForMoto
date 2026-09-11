import { CORE } from '../../../i18n/core';
import { hourOf } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { tierColor, tierOf } from '../../../lib/scoring';
import { Gauge } from '../../../ui/Gauge';
import { Banner } from '../../../ui/primitives';
import type { RideWindow } from '../logic/bestWindow';
import { warningText, type WarningKind } from '../logic/warnings';
import { S_NOW } from '../strings';
import type { NowModel } from '../useNowModel';
import { tierWord } from './tierWord';

const SEVERE: ReadonlySet<WarningKind> = new Set(['storm', 'ice', 'frost']);

interface HeroProps {
  score: number | null;
  model: NowModel;
  onScore: () => void;
}

export function Hero({ score, model, onScore }: HeroProps) {
  const s = useStrings(S_NOW);
  const core = useStrings(CORE);
  const lang = useLang();
  const word = tierWord(tierOf(score), core);
  const aria = score == null ? s.scoreAriaNone : fmt(s.scoreAria, { score, tier: word });
  const { title, sub } = model.headline;

  return (
    <section className="now-hero" aria-labelledby="now-verdict">
      <button type="button" className="now-hero__gauge" onClick={onScore} aria-label={aria} aria-haspopup="dialog">
        <Gauge score={score} label={word} ariaLabel={aria} />
      </button>
      <h1 id="now-verdict" className="now-hero__verdict num">
        {title}
      </h1>
      {sub && <p className="now-hero__sub">{sub}</p>}
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
