import { useEffect, useId, useLayoutEffect, useRef, useState, type CSSProperties, type RefObject } from 'react';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { Button } from '../../ui/primitives';
import { useClock } from '../now/useClock';
import { fmtClockTime, fmtKm } from './logic/rideFormat';
import { rideRecorder, type RideSnapshot } from './recorder';
import { LiveShareButton } from './LiveShareControl';
import { RideControls } from './RideControls';
import { RideGauges } from './RideGauges';
import { RideDetails, RideSpeed, RideSummary } from './RideLive';
import { RideMap, type MapInsets } from './RideMap';
import { RideNotes } from './RideNotes';
import { RideStory } from './RideStory';
import { RideTurn } from './RideTurn';
import { RIDE_S } from './strings';
import { useRide } from './useRide';
import './ride.css';

// The ride screen: over the whole app, with no tab bar, while a ride is
// recorded, paused, just finished, or waiting to be picked up after the app
// was closed. The app shell behind it is inert (App.tsx).
//
// While riding, the map takes the whole screen and the rest lies over it: the
// next turn and the notes at the top, the speed and a bar with what is left
// and the buttons at the bottom (in a column on the left when the phone is
// sideways). The map keeps the rider in the free part between them. A
// finished ride is a report that scrolls, with the map at a fixed height.

/** Live numbers refresh this often, even when no GPS fix comes in. */
const TICK_MS = 1_000;
/** Sideways, the overlays stand in a column on the left (ride.css, same query). */
const SIDEWAYS_QUERY = '(orientation: landscape) and (max-height: 520px)';

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

/**
 * How much of the screen the overlays cover, kept up to date as they grow and
 * shrink, and where the top one ends (the bottom one may not reach past it).
 */
function useInsets(top: RefObject<HTMLElement | null>, bottom: RefObject<HTMLElement | null>, active: boolean): MapInsets & { topEdge: number } {
  const [insets, setInsets] = useState<MapInsets & { topEdge: number }>({ top: 0, bottom: 0, left: 0, topEdge: 0 });
  useLayoutEffect(() => {
    if (!active) return undefined;
    const measure = (): void => {
      const screenH = window.innerHeight;
      const t = top.current?.getBoundingClientRect();
      const b = bottom.current?.getBoundingClientRect();
      const topEdge = t ? Math.round(t.bottom) : 0;
      const next = window.matchMedia(SIDEWAYS_QUERY).matches
        ? { top: 0, bottom: 0, left: Math.max(t?.right ?? 0, b?.right ?? 0), topEdge }
        : { top: topEdge, bottom: b ? Math.round(screenH - b.top) : 0, left: 0, topEdge };
      setInsets((prev) =>
        prev.top === next.top && prev.bottom === next.bottom && prev.left === next.left && prev.topEdge === next.topEdge ? prev : next,
      );
    };
    measure();
    const observer = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(measure);
    if (top.current) observer?.observe(top.current);
    if (bottom.current) observer?.observe(bottom.current);
    window.addEventListener('resize', measure);
    return () => {
      observer?.disconnect();
      window.removeEventListener('resize', measure);
    };
  }, [top, bottom, active]);
  return insets;
}

export default function RideScreen() {
  const s = useStrings(RIDE_S);
  const ride = useRide();
  const live = ride.phase === 'recording' || ride.phase === 'paused';
  const now = useClock(live, TICK_MS);
  const rootRef = useRef<HTMLDivElement>(null);
  const topRef = useRef<HTMLDivElement>(null);
  const bottomRef = useRef<HTMLDivElement>(null);
  const insets = useInsets(topRef, bottomRef, live);
  const [details, setDetails] = useState(false);
  const detailsId = useId();

  // Focus moves into the screen, so a screen reader starts here.
  useEffect(() => {
    rootRef.current?.focus();
  }, []);

  // What is said follows the app's language, also for a ride picked up after a restart.
  const lang = useLang();
  useEffect(() => {
    rideRecorder.setLang(lang);
  }, [lang]);

  if (ride.phase === 'resume') {
    return (
      <div ref={rootRef} className="ride" role="dialog" aria-modal="true" aria-label={s.dialogLabel} tabIndex={-1}>
        <ResumePrompt ride={ride} />
      </div>
    );
  }

  if (!live) {
    return (
      <div ref={rootRef} className="ride ride--done" role="dialog" aria-modal="true" aria-label={s.dialogLabel} tabIndex={-1}>
        <RideNotes ride={ride} now={now} />
        <RideMap ride={ride} />
        <div className="ride-panel">
          <RideGauges ride={ride} now={now} />
          <RideStory ride={ride} />
          <RideControls phase={ride.phase} />
        </div>
      </div>
    );
  }

  // The overlays' size, for the map's controls (ride.css).
  const style = {
    '--ride-top': `${insets.top}px`,
    '--ride-bottom': `${insets.bottom}px`,
    '--ride-left': `${insets.left}px`,
    '--ride-top-edge': `${insets.topEdge}px`,
  } as CSSProperties;
  return (
    <div ref={rootRef} className="ride ride--live" style={style} role="dialog" aria-modal="true" aria-label={s.dialogLabel} tabIndex={-1}>
      <RideMap ride={ride} insets={insets} />
      <div ref={topRef} className="ride-top">
        <RideTurn ride={ride} />
        <RideNotes ride={ride} now={now} />
      </div>
      <div ref={bottomRef} className="ride-bottom">
        <RideSpeed ride={ride} now={now} />
        <div className="ride-bar">
          <RideSummary ride={ride} now={now} open={details} onToggle={() => setDetails((open) => !open)} detailsId={detailsId} />
          {details && <RideDetails ride={ride} now={now} id={detailsId} />}
          <div className="ride-actions">
            <RideControls phase={ride.phase} />
            <LiveShareButton />
          </div>
        </div>
      </div>
    </div>
  );
}
