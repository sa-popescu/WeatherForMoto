import { useEffect, useState } from 'react';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { Spinner } from '../../ui/primitives';
import { fmtClockTime } from '../ride/logic/rideFormat';
import { LiveMap } from './LiveMap';
import { LIVE_VIEW_S } from './strings';
import { useLiveState } from './useLiveState';
import './live.css';

// What a live location link opens (#/live/<id>): the rider on a map, how
// fresh the position is, the speed and, on a route, where to and when. It
// covers the app; the link back opens Motonor itself.

/** A position older than this probably means no signal on the rider's side. */
const STALE_MS = 5 * 60_000;
const TICK_MS = 15_000;

function useNow(): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), TICK_MS);
    return () => window.clearInterval(timer);
  }, []);
  return now;
}

export default function LiveView({ id }: { id: string }) {
  const s = useStrings(LIVE_VIEW_S);
  const lang = useLang();
  const state = useLiveState(id);
  const now = useNow();

  if (state.status === 'gone') {
    return (
      <div className="live live--gone">
        <Icon name="flag" size={40} />
        <h1 className="live__gone-title num">{s.gone}</h1>
        <p className="live__note">{s.goneNote}</p>
        <a className="btn btn--primary btn--lg" href="#/drum">
          {s.openApp}
        </a>
      </div>
    );
  }

  const data = state.status === 'ready' ? state.data : null;
  const pos = data?.pos ?? null;
  const ageMin = pos ? Math.floor((now - pos.at) / 60_000) : 0;

  return (
    <div className="live">
      {data ? <LiveMap data={data} /> : <div className="live-map" />}
      <section className="live__card" aria-labelledby="live-title" aria-live="polite">
        <header className="live__head">
          <span className="live__dot" aria-hidden="true" />
          <h1 id="live-title" className="live__title num">
            {s.title}
          </h1>
          <a className="live__app" href="#/drum">
            {s.openApp}
          </a>
        </header>
        {state.status === 'loading' && (
          <p className="live__line">
            <Spinner size={16} /> {s.loading}
          </p>
        )}
        {data && !pos && <p className="live__line">{s.waiting}</p>}
        {pos && (
          <>
            <p className="live__line">
              {ageMin < 1 ? s.updatedNow : fmt(s.updatedMin, { n: ageMin })}
              {pos.speedKmh !== null && pos.speedKmh >= 3 && ageMin < 1 && <span className="live__speed num"> · {fmt(s.speed, { v: pos.speedKmh })}</span>}
            </p>
            {now - pos.at > STALE_MS && <p className="live__line live__line--warn">{s.stale}</p>}
            {pos.dest && (
              <p className="live__line live__line--strong">
                {fmt(s.towards, { dest: pos.dest })}
                {pos.etaMs !== null && <> · {fmt(s.eta, { time: fmtClockTime(pos.etaMs, lang) })}</>}
              </p>
            )}
          </>
        )}
        {state.status === 'ready' && state.failed && <p className="live__line live__line--warn">{s.error}</p>}
        {data && <p className="live__note">{fmt(s.expires, { time: fmtClockTime(data.expiresAt, lang) })}</p>}
      </section>
    </div>
  );
}
