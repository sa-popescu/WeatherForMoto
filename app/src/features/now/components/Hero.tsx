import type { CSSProperties } from 'react';
import { CORE } from '../../../i18n/core';
import { fmtNumber, hourOf } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { tierColor, tierOf } from '../../../lib/scoring';
import type { CurrentWeather } from '../../../lib/types';
import { Banner } from '../../../ui/primitives';
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

// The weather right now on the sky behind it (the temperature and the sky
// in words), then one card like the rest of the screen: the score in a
// small ring next to the verdict, which opens the score's explanation, and
// under it the best windows to ride.

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
  const ring = { '--p': String(value), '--c': color } as CSSProperties;

  return (
    <section className="now-hero" aria-labelledby="now-verdict">
      <div className="now-hero__weather">
        <div className="now-hero__temps">
          <p className="now-hero__temp" title={tempTitle(current, lang)}>
            {temp1(current.temperature, lang)}
          </p>
          {current.feels_like != null && (
            <p className="now-hero__feels">
              <span>{s.feelsLabel}</span>
              <span className="now-hero__feels-value">{temp1(current.feels_like, lang)}</span>
            </p>
          )}
        </div>
        <p className="now-hero__sky">{skyWord[skyOf(current).kind]}</p>
        <Why current={current} lang={lang} />
      </div>
      <div className="now-hero__card">
        <div className="now-score">
          <button type="button" className="now-score__ring" style={ring} onClick={onScore} aria-label={aria} aria-haspopup="dialog">
            <span className="now-score__num" aria-hidden="true">
              {score ?? '–'}
            </span>
          </button>
          <div className="now-score__text">
            <p className="now-score__eyebrow">
              {s.scoreLabel} · <span style={{ color }}>{word}</span>
            </p>
            <h1 id="now-verdict" className="now-hero__verdict">
              {title}
            </h1>
            {sub && <p className="now-hero__sub">{sub}</p>}
          </div>
        </div>
        {personal !== null && score !== null && (
          <p className="now-hero__mine" title={s.mineHint}>
            <span className="now-dot" style={{ background: tierColor(personal) }} aria-hidden="true" />
            {fmt(s.mineLine, { score: personal, general: score })}
          </p>
        )}
        <Windows today={model.windows.today} tomorrow={model.windows.tomorrow} />
      </div>
      {model.warnings.length > 0 && (
        <div className="now-hero__warnings">
          {model.warnings.map((w) => (
            <Banner key={w.kind} tone={SEVERE.has(w.kind) ? 'error' : 'warn'} icon="alert">
              {warningText(w, model.nowIso, lang)}
            </Banner>
          ))}
        </div>
      )}
    </section>
  );
}

/**
 * Why the sky reads as it does: the arbiter's sentence on rain, and on fog or
 * a storm when someone mentioned one ("Nu plouă: aeroportul Băneasa și 10
 * din 11 surse spun uscat."). Nothing from older servers or the fallback.
 */
function Why({ current, lang }: { current: CurrentWeather; lang: 'ro' | 'en' }) {
  const v = current.verdict;
  if (!v) return null;
  const pick = (p: { explanation: string; explanation_en: string }) => (lang === 'en' ? p.explanation_en : p.explanation);
  // The temperature's sentence only when a station moved it or was set aside.
  const temperature = v.values?.temperature;
  const lines = [
    pick(v.precipitation),
    v.fog && pick(v.fog),
    v.storm && pick(v.storm),
    temperature?.notable && pick(temperature),
  ].filter(
    (x): x is string => Boolean(x),
  );
  if (!lines.length) return null;
  return (
    <p className="now-hero__why" title={`${v.precipitation.confidence}%`}>
      {lines.join(' ')}
    </p>
  );
}

/** How the temperature was decided, for the tooltip; undefined on older servers. */
function tempTitle(current: CurrentWeather, lang: 'ro' | 'en'): string | undefined {
  const t = current.verdict?.values?.temperature;
  if (!t) return undefined;
  return lang === 'en' ? t.explanation_en : t.explanation;
}

/** A temperature to a tenth of a degree: "1,4°", "-3,6°". */
function temp1(value: number | null, lang: 'ro' | 'en'): string {
  return value == null ? '–' : `${fmtNumber(value, lang, 1)}°`;
}

/** The best windows to ride, today and tomorrow, as rows of the card. */
function Windows({ today, tomorrow }: { today: RideWindow | null; tomorrow: RideWindow | null }) {
  const s = useStrings(S_NOW);
  const items = [
    today ? { key: 'today', w: today, day: s.windowToday } : null,
    tomorrow ? { key: 'tomorrow', w: tomorrow, day: s.windowTomorrow } : null,
  ].filter((x): x is { key: string; w: RideWindow; day: string } => x !== null);
  if (!items.length) return null;
  return (
    <div className="now-windows">
      <p className="eyebrow">{s.windowLabel}</p>
      <ul className="now-windows__list">
        {items.map(({ key, w, day }) => (
          <li key={key} className="now-windows__row">
            <span className="now-windows__day">{day}</span>
            <span className="now-windows__range num">{fmt(s.windowRange, { from: hourOf(w.from), to: hourOf(w.to) })}</span>
            <span className="now-windows__score num">
              <span className="now-dot" style={{ background: tierColor(w.avg) }} aria-hidden="true" />
              {w.avg}
            </span>
          </li>
        ))}
      </ul>
    </div>
  );
}
