import { useState } from 'react';
import { fmt, useLang, useStrings, type Lang } from '../../lib/i18n';
import { useToast } from '../../state/toast';
import { Button, Skeleton } from '../../ui/primitives';
import { averageKmh } from './logic/clock';
import { fmtClockTime, fmtDuration, fmtKm, fmtKmh } from './logic/rideFormat';
import { rideRecorder } from './recorder';
import { deleteRide, type RideListItem } from './store';
import { RIDE_S } from './strings';
import type { RideHistory } from './useRideHistory';

// The rides kept on this phone, on "Ture": open one on the ride screen (map,
// summary, GPX, saving to the account), or delete it after a confirmation.

function rideDay(startedAt: number, lang: Lang): string {
  const day = new Date(startedAt).toLocaleDateString(lang === 'en' ? 'en-GB' : 'ro-RO', { weekday: 'short', day: 'numeric', month: 'short' }).replace(/\./g, '');
  return `${day} · ${fmtClockTime(startedAt, lang)}`;
}

interface RowProps {
  item: RideListItem;
  onOpen: () => void;
  onDeleted: () => void;
}

function RideRow({ item, onOpen, onDeleted }: RowProps) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const toast = useToast();
  const [asking, setAsking] = useState(false);
  const { summary } = item;
  const avg = averageKmh(summary.movingDistanceM, summary.movingMs);

  const remove = async (): Promise<void> => {
    try {
      await deleteRide(item.id);
      toast(s.deleted, { tone: 'success' });
      onDeleted();
    } catch (err) {
      console.warn('[ride] could not delete a ride', err);
      toast(s.deleteErr, { tone: 'error' });
    }
  };

  return (
    <li className="ride-history__row">
      <div className="ride-history__text">
        <span className="ride-history__day num">
          {rideDay(item.startedAt, lang)}
          {item.loggedId !== null && <span className="ride-history__tag">{s.historyLogged}</span>}
        </span>
        {item.routeName && <span className="ride-history__route">{`${s.towards} ${item.routeName}`}</span>}
        <span className="ride-history__meta num">
          {fmt(s.historyRow, { km: fmtKm(summary.distanceM, lang), time: fmtDuration(summary.elapsedMs), avg: fmtKmh(avg, lang) })}
        </span>
      </div>
      {asking ? (
        <div className="ride-history__confirm" role="group" aria-label={s.deleteAsk}>
          <p>{s.deleteAsk}</p>
          <Button variant="danger" onClick={() => void remove()}>
            {s.deleteYes}
          </Button>
          <Button variant="ghost" onClick={() => setAsking(false)}>
            {s.deleteNo}
          </Button>
        </div>
      ) : (
        <div className="ride-history__actions">
          <Button icon="map" onClick={onOpen}>
            {s.review}
          </Button>
          <Button variant="ghost" icon="trash" onClick={() => setAsking(true)}>
            {s.deleteRide}
          </Button>
        </div>
      )}
    </li>
  );
}

/** The rides on this phone, newest first, as a section of "Ture". */
export function RideHistoryList({ history }: { history: RideHistory }) {
  const s = useStrings(RIDE_S);
  const toast = useToast();

  const openRide = async (id: string): Promise<void> => {
    if (!(await rideRecorder.review(id))) toast(s.reviewErr, { tone: 'error' });
  };

  return (
    <section className="ride-history-section" aria-labelledby="ride-history-title">
      <h2 className="eyebrow" id="ride-history-title">
        {s.historyTitle}
      </h2>
      <p className="ride-history__lead">{s.historyLead}</p>
      {history.status === 'loading' && history.items.length === 0 && <Skeleton height={96} radius={14} />}
      {history.status === 'error' && <p className="ride-history__empty">{s.historyFailed}</p>}
      {history.status === 'ready' && history.items.length === 0 && <p className="ride-history__empty">{s.historyEmpty}</p>}
      <ul className="ride-history">
        {history.items.map((item) => (
          <RideRow key={item.id} item={item} onOpen={() => void openRide(item.id)} onDeleted={history.reload} />
        ))}
      </ul>
    </section>
  );
}
