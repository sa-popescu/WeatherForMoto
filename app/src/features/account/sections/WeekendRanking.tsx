import { useEffect, useRef, useState } from 'react';
import { CORE } from '../../../i18n/core';
import { api } from '../../../lib/api';
import { dayName, localNowIso } from '../../../lib/format';
import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { tierColor, tierOf, type Tier } from '../../../lib/scoring';
import { usePlace } from '../../../state/place';
import { Banner, Button, Card, Spinner } from '../../../ui/primitives';
import { mapPool } from '../lib/pool';
import { pickWeekend, rainLine, rankWeekend, type RainWords, type RankedPlace, type WeekendDay } from '../lib/weekend';
import { APP } from '../strings/app';

const CONCURRENCY = 2;
/** Covers any coming weekend (today + 6 days). */
const FORECAST_DAYS = 7;

type Phase = 'idle' | 'loading' | 'done';
type TierKey = 'tierIdeal' | 'tierOk' | 'tierAtentie' | 'tierEvita';
const TIER_KEY: Record<Tier, TierKey> = { ideal: 'tierIdeal', ok: 'tierOk', atentie: 'tierAtentie', evita: 'tierEvita' };

export function WeekendRanking() {
  const s = useStrings(APP);
  const core = useStrings(CORE);
  const lang = useLang();
  const { favorites } = usePlace();
  const [phase, setPhase] = useState<Phase>('idle');
  const [done, setDone] = useState(0);
  const [failed, setFailed] = useState(0);
  const [ranked, setRanked] = useState<RankedPlace[]>([]);
  const controllerRef = useRef<AbortController | null>(null);
  const favKey = favorites.map((f) => `${f.lat.toFixed(3)},${f.lon.toFixed(3)}`).join('|');

  // Results belong to one list of favourites; any change starts over.
  useEffect(() => {
    controllerRef.current?.abort();
    setPhase('idle');
    setRanked([]);
    setFailed(0);
  }, [favKey]);
  useEffect(() => () => controllerRef.current?.abort(), []);

  const words: RainWords = {
    noRain: s.noRain,
    bands: { urme: core.bandUrme, slaba: core.bandSlaba, moderata: core.bandModerata, puternica: core.bandPuternica },
  };

  const run = async (): Promise<void> => {
    controllerRef.current?.abort();
    const controller = new AbortController();
    controllerRef.current = controller;
    const list = [...favorites];
    setPhase('loading');
    setDone(0);
    setFailed(0);
    setRanked([]);
    const results = await mapPool(
      list,
      CONCURRENCY,
      async (p, _index, signal) => {
        const { data } = await api.weather(p, signal, FORECAST_DAYS);
        return pickWeekend(data.daily, localNowIso(data.utc_offset_seconds).slice(0, 10));
      },
      controller.signal,
      () => setDone((n) => n + 1),
    );
    if (controller.signal.aborted) return;
    results.forEach((r) => r?.status === 'rejected' && console.warn('[account] weekend forecast failed', r.reason));
    setFailed(results.filter((r) => r?.status === 'rejected').length);
    setRanked(rankWeekend(list.map((place, i) => ({ place, days: results[i]?.status === 'fulfilled' ? results[i].value : [] }))));
    setPhase('done');
  };

  const cancel = (): void => {
    controllerRef.current?.abort();
    setPhase('idle');
  };

  const dayLine = (d: WeekendDay) => (
    <span key={d.date} className="acct-rank__day">
      <span className="num" style={{ color: tierColor(d.score) }}>
        {`${dayName(d.date, lang)} ${d.score ?? '–'}`}
      </span>
      {' · '}
      <span className="acct-rank__rain">{rainLine(d.probability, d.band, words)}</span>
    </span>
  );

  return (
    <Card className="acct-stack">
      <h3 className="acct-sub">{s.weekendTitle}</h3>
      <p className="acct-hint">{favorites.length ? s.weekendLead : s.weekendNeedsFav}</p>
      {phase === 'loading' ? (
        <div className="acct-progress" role="status">
          <Spinner size={18} />
          <span className="acct-progress__text">{fmt(s.weekendProgress, { done, total: favorites.length })}</span>
          <Button variant="ghost" onClick={cancel}>
            {s.weekendCancel}
          </Button>
        </div>
      ) : (
        <Button full icon="flag" disabled={favorites.length === 0} onClick={() => void run()}>
          {phase === 'done' ? s.weekendRerun : s.weekendRun}
        </Button>
      )}
      {failed > 0 && <Banner tone="warn" icon="alert">{fmt(s.weekendFailed, { count: failed })}</Banner>}
      {ranked.length > 0 && (
        <ol className="acct-rank">
          {ranked.map((r, i) => {
            const tier = tierOf(r.best?.score);
            return (
              <li key={`${r.place.name}|${r.place.lat}`} className="acct-rank__item">
                <span className="acct-rank__pos num">{i + 1}</span>
                <div className="acct-rank__body">
                  <span className="acct-rank__name">{r.place.name}</span>
                  {r.days.length ? r.days.map(dayLine) : <span className="acct-rank__day">{s.weekendNoData}</span>}
                </div>
                <span className="acct-rank__score num" style={{ color: tierColor(r.best?.score) }}>
                  {r.best?.score ?? '–'}
                  {tier && <span className="acct-rank__tier">{core[TIER_KEY[tier]]}</span>}
                </span>
              </li>
            );
          })}
        </ol>
      )}
    </Card>
  );
}
