import { useEffect, useRef } from 'react';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { Button } from '../../ui/primitives';
import { useClock } from '../now/useClock';
import { fmtClockTime, fmtKm } from './logic/rideFormat';
import { rideRecorder, type RideSnapshot } from './recorder';
import { LiveShareControl } from './LiveShareControl';
import { RideControls } from './RideControls';
import { RideGauges } from './RideGauges';
import { RideMap } from './RideMap';
import { RideNotes } from './RideNotes';
import { RideStory } from './RideStory';
import { RideTurn } from './RideTurn';
import { RIDE_S } from './strings';
import { useRide } from './useRide';
import './ride.css';

// The ride screen: over the whole app, with no tab bar, while a ride is
// recorded, paused, just finished, or waiting to be picked up after the app
// was closed. The app shell behind it is inert (App.tsx).

/** Live numbers refresh this often, even when no GPS fix comes in. */
const TICK_MS = 1_000;

function ResumePrompt({ ride }: { ride: RideSnapshot }) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const last = ride.lastFixAt ?? Date.now();
  const start = ride.clock.startedAt ?? last;
  return (
    <div className="ride-resume">
      <Icon name="bike" size={44} />
      <h2 className="ride-resume__title num">{s.resumeTitle}</h2>
      <p className="ride-resume__lead">
        {fmt(s.resumeLead, { start: fmtClockTime(start, lang), last: fmtClockTime(last, lang), km: fmtKm(ride.totals.distanceM, lang) })}
      </p>
      <div className="ride-resume__actions">
        <Button variant="primary" size="lg" full icon="play" onClick={() => rideRecorder.continueUnfinished()}>
          {s.resumeContinue}
        </Button>
        <Button size="lg" full icon="flag" onClick={() => rideRecorder.finishUnfinished()}>
          {s.resumeFinish}
        </Button>
      </div>
    </div>
  );
}

export default function RideScreen() {
  const s = useStrings(RIDE_S);
  const ride = useRide();
  const live = ride.phase === 'recording' || ride.phase === 'paused';
  const now = useClock(live, TICK_MS);
  const rootRef = useRef<HTMLDivElement>(null);

  // Focus moves into the screen, so a screen reader starts here.
  useEffect(() => {
    rootRef.current?.focus();
  }, []);

  // What is said follows the app's language, also for a ride picked up after a restart.
  const lang = useLang();
  useEffect(() => {
    rideRecorder.setLang(lang);
  }, [lang]);

  return (
    <div ref={rootRef} className={ride.phase === 'finished' ? 'ride ride--done' : 'ride'} role="dialog" aria-modal="true" aria-label={s.dialogLabel} tabIndex={-1}>
      {ride.phase === 'resume' ? (
        <ResumePrompt ride={ride} />
      ) : (
        <>
          <RideNotes ride={ride} now={now} />
          <RideTurn ride={ride} />
          <RideMap ride={ride} />
          <div className="ride-panel">
            <RideGauges ride={ride} now={now} />
            {ride.phase === 'finished' && <RideStory ride={ride} />}
            <RideControls phase={ride.phase} />
            {live && <LiveShareControl now={now} />}
          </div>
        </>
      )}
    </div>
  );
}
