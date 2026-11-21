import { fmtNumber } from '../../lib/format';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { cx, IconButton } from '../../ui/primitives';
import { GAP_MS } from './logic/track';
import { rideRecorder, type RideSnapshot } from './recorder';
import { RIDE_S } from './strings';
import { voiceAvailable } from './voice';

// The band at the top of the ride screen: the GPS state and the voice switch
// on one line, then only the notes that need doing something (off the route,
// location refused, screen may go dark, the ride cannot be saved) or that
// change how to read the numbers (gaps, arrived).

/** Gaps shorter than this in total are not worth a note. */
const GAP_NOTE_MIN_MS = 60_000;

function GpsLine({ ride, now }: { ride: RideSnapshot; now: number }) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const metres = ride.accuracyM === null ? '' : fmtNumber(Math.round(ride.accuracyM), lang);
  // Some phones simply stop sending positions instead of reporting an error.
  const stale = ride.lastFixAt !== null && now - ride.lastFixAt > GAP_MS;
  const state = stale && (ride.gps === 'ok' || ride.gps === 'weak') ? 'waiting' : ride.gps;
  const text =
    state === 'ok'
      ? fmt(s.gpsOk, { m: metres })
      : state === 'weak'
        ? fmt(s.gpsWeak, { m: metres })
        : state === 'unavailable'
          ? s.gpsUnavailable
          : s.gpsWaiting;
  return (
    <p className={cx('ride-notes__gps', `ride-notes__gps--${state}`)} role="status">
      <span className="ride-notes__dot" aria-hidden="true" />
      {text}
    </p>
  );
}

export function RideNotes({ ride, now }: { ride: RideSnapshot; now: number }) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const live = ride.phase === 'recording' || ride.phase === 'paused';
  const gapMinutes = Math.round(ride.totals.gapMs / 60_000);
  const notes: Array<{ key: string; tone: 'off' | 'warn' | 'error' | 'info'; text: string }> = [];
  const follow = ride.follow;
  if (live && follow?.off && follow.offM !== null) {
    notes.push({ key: 'off', tone: 'off', text: fmt(s.offRoute, { m: fmtNumber(Math.round(follow.offM / 10) * 10, lang) }) });
  }
  if (follow?.arrived && ride.route) notes.push({ key: 'arrived', tone: 'info', text: fmt(s.arrivedNote, { name: ride.route.name }) });
  if (ride.gps === 'denied') notes.push({ key: 'denied', tone: 'error', text: s.gpsDenied });
  if (ride.gps === 'unsupported') notes.push({ key: 'unsupported', tone: 'error', text: s.gpsUnsupported });
  if (ride.saveFailed) notes.push({ key: 'save', tone: 'error', text: s.saveFailed });
  if (live && ride.wake !== 'on') notes.push({ key: 'wake', tone: 'warn', text: s.wakeOff });
  if (ride.totals.gapMs >= GAP_NOTE_MIN_MS) notes.push({ key: 'gap', tone: 'info', text: fmt(s.gapNote, { min: gapMinutes }) });

  const showGps = live && ride.gps !== 'denied' && ride.gps !== 'unsupported';
  // A finished ride usually has nothing to say here: leave the room to the map.
  if (!showGps && notes.length === 0) return null;
  return (
    <div className="ride-notes">
      <div className="ride-notes__row">
        {showGps ? <GpsLine ride={ride} now={now} /> : <span />}
        <span className="ride-notes__side">
          {ride.phase === 'paused' && <span className="ride-notes__tag num">{s.pausedTag}</span>}
          {live && voiceAvailable() && (
            <IconButton
              icon="bell"
              label={ride.voice ? s.voiceOn : s.voiceOff}
              aria-pressed={ride.voice}
              className={cx('ride-notes__voice', !ride.voice && 'ride-notes__voice--off')}
              onClick={() => rideRecorder.setVoice(!ride.voice)}
            />
          )}
        </span>
      </div>
      {notes.map((n) => (
        <p key={n.key} className={cx('ride-notes__note', `ride-notes__note--${n.tone}`)} role={n.tone === 'error' || n.tone === 'off' ? 'alert' : undefined}>
          {n.text}
        </p>
      ))}
    </div>
  );
}
