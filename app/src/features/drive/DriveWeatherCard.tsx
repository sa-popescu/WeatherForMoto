import type { CSSProperties } from 'react';
import { CORE } from '../../i18n/core';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { tierColor, tierOf } from '../../lib/scoring';
import { usePlace } from '../../state/place';
import { useWeather } from '../../state/weather';
import { tierWord } from '../now/components/tierWord';
import { usePersonalScore } from '../ride/personalStore';
import { warningText } from '../now/logic/warnings';
import { useClock } from '../now/useClock';
import { useNowModel } from '../now/useNowModel';
import { DRIVE_S } from './strings';

// The weather in front of the map before a destination is picked: the moto
// score of the place on screen, the verdict of "Vreme" in one line, and the
// first warning. The whole card opens the forecast.

export function DriveWeatherCard({ active }: { active: boolean }) {
  const s = useStrings(DRIVE_S);
  const core = useStrings(CORE);
  const lang = useLang();
  const { place } = usePlace();
  const weather = useWeather();
  const nowMs = useClock(active);
  const model = useNowModel(weather.data, nowMs, lang);
  const data = weather.data;
  // The rider's own score when their rides have taught the app one (ride/personalStore.ts).
  const personal = usePersonalScore(data?.current);
  const score = personal ?? (data ? (data.current.moto_score ?? model?.bars[0]?.score ?? null) : null);
  const warning = model?.warnings[0] ?? null;
  const style = { '--drive-tier': tierColor(score) } as CSSProperties;

  return (
    <a className="drive-weather" href="#/acum" style={style}>
      <span className="drive-weather__score num" aria-hidden={score === null}>
        <span className="drive-weather__value">{score ?? '–'}</span>
        <span className="drive-weather__tier">{tierWord(tierOf(score), core)}</span>
      </span>
      <span className="drive-weather__text">
        <span className="drive-weather__eyebrow">{fmt(personal !== null ? s.weatherMine : s.weatherAt, { place: place.name })}</span>
        {model ? (
          <>
            <span className="drive-weather__title">{model.headline.title}</span>
            {warning ? (
              <span className="drive-weather__sub drive-weather__sub--warn">{warningText(warning, model.nowIso, lang)}</span>
            ) : (
              model.headline.sub && <span className="drive-weather__sub">{model.headline.sub}</span>
            )}
          </>
        ) : (
          <span className="drive-weather__sub">{s.weatherLoading}</span>
        )}
      </span>
      <span className="drive-weather__open">{s.weatherOpen}</span>
    </a>
  );
}
