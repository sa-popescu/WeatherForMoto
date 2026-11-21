import { fmtNumber } from '../../lib/format';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { cx } from '../../ui/primitives';
import type { RideSnapshot } from './recorder';
import { RIDE_S } from './strings';

// The band at the top of the ride screen: the GPS state on one line, then only
// the notes that need doing something (location refused, screen may go dark,
// the ride cannot be saved) or that change how to read the numbers (gaps).

/** Gaps shorter than this in total are not worth a note. */
const GAP_NOTE_MIN_MS = 60_000;

function GpsLine({ ride }: { ride: RideSnapshot }) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const metres = ride.accuracyM === null ? '' : fmtNumber(Math.round(ride.accuracyM), lang);
  const text =
    ride.gps === 'ok'
      ? fmt(s.gpsOk, { m: metres })
      : ride.gps === 'weak'
        ? fmt(s.gpsWeak, { m: metres })
        : ride.gps === 'unavailable'
          ? s.gpsUnavailable
          : s.gpsWaiting;
  return (
    <p className={cx('ride-notes__gps', `ride-notes__gps--${ride.gps}`)} role="status">
      <span className="ride-notes__dot" aria-hidden="true" />
      {text}
    </p>
  );
}

export function RideNotes({ ride }: { ride: RideSnapshot }) {
  const s = useStrings(RIDE_S);
  const live = ride.phase === 'recording' || ride.phase === 'paused';
  const gapMinutes = Math.round(ride.totals.gapMs / 60_000);
  const notes: Array<{ key: string; tone: 'warn' | 'error' | 'info'; text: string }> = [];
  if (ride.gps === 'denied') notes.push({ key: 'denied', tone: 'error', text: s.gpsDenied });
  if (ride.gps === 'unsupported') notes.push({ key: 'unsupported', tone: 'error', text: s.gpsUnsupported });
  if (ride.saveFailed) notes.push({ key: 'save', tone: 'error', text: s.saveFailed });
  if (live && ride.wake !== 'on') notes.push({ key: 'wake', tone: 'warn', text: s.wakeOff });
  if (ride.totals.gapMs >= GAP_NOTE_MIN_MS) notes.push({ key: 'gap', tone: 'info', text: fmt(s.gapNote, { min: gapMinutes }) });

  const showGps = live && ride.gps !== 'denied' && ride.gps !== 'unsupported';
  return (
    <div className="ride-notes">
      <div className="ride-notes__row">
        {showGps ? <GpsLine ride={ride} /> : <span />}
        {ride.phase === 'paused' && <span className="ride-notes__tag num">{s.pausedTag}</span>}
      </div>
      {notes.map((n) => (
        <p key={n.key} className={cx('ride-notes__note', `ride-notes__note--${n.tone}`)} role={n.tone === 'error' ? 'alert' : undefined}>
          {n.text}
        </p>
      ))}
    </div>
  );
}
