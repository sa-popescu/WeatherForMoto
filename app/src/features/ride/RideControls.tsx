import { useEffect, useRef, useState, type KeyboardEvent } from 'react';
import { useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { Button, cx } from '../../ui/primitives';
import { rideRecorder, type RidePhase } from './recorder';
import { RideSummaryActions } from './RideSummaryActions';
import { RIDE_S } from './strings';

// The buttons under the numbers: large, for gloves. Stop has to be held for a
// second, so a bump on the handlebar cannot end the ride.

/** How long Stop is held. The fill in ride.css (.ride-hold--on) takes as long. */
const HOLD_MS = 1_000;

function HoldToStop() {
  const s = useStrings(RIDE_S);
  const [holding, setHolding] = useState(false);
  const timer = useRef<number | null>(null);

  const cancel = (): void => {
    if (timer.current !== null) window.clearTimeout(timer.current);
    timer.current = null;
    setHolding(false);
  };
  const begin = (): void => {
    if (timer.current !== null) return;
    setHolding(true);
    timer.current = window.setTimeout(() => {
      timer.current = null;
      setHolding(false);
      rideRecorder.stop();
    }, HOLD_MS);
  };
  useEffect(() => cancel, []);

  const onKeyDown = (e: KeyboardEvent<HTMLButtonElement>): void => {
    if ((e.key === 'Enter' || e.key === ' ') && !e.repeat) {
      e.preventDefault();
      begin();
    }
  };
  const onKeyUp = (e: KeyboardEvent<HTMLButtonElement>): void => {
    if (e.key === 'Enter' || e.key === ' ') cancel();
  };

  return (
    <button
      type="button"
      className={cx('ride-hold', holding && 'ride-hold--on')}
      aria-label={s.stopAria}
      onPointerDown={(e) => {
        // Captured, so lifting the finger anywhere ends the hold, not only on the button.
        try {
          e.currentTarget.setPointerCapture(e.pointerId);
        } catch (err) {
          console.warn('[ride] no pointer capture for the stop button', err);
        }
        begin();
      }}
      onPointerUp={cancel}
      onPointerCancel={cancel}
      onLostPointerCapture={cancel}
      onKeyDown={onKeyDown}
      onKeyUp={onKeyUp}
      onBlur={cancel}
      onContextMenu={(e) => e.preventDefault()}
    >
      <span className="ride-hold__fill" aria-hidden="true" />
      <span className="ride-hold__text">
        <Icon name="flag" size={20} />
        <span className="num">{s.stop}</span>
        <span className="ride-hold__hint">{s.stopHint}</span>
      </span>
    </button>
  );
}

export function RideControls({ phase }: { phase: RidePhase }) {
  const s = useStrings(RIDE_S);
  if (phase === 'finished') return <RideSummaryActions />;
  return (
    <div className="ride-controls ride-controls--two">
      {phase === 'paused' ? (
        <Button variant="primary" size="lg" full icon="play" onClick={() => rideRecorder.resume()}>
          {s.resume}
        </Button>
      ) : (
        <Button size="lg" full icon="pause" onClick={() => rideRecorder.pause()}>
          {s.pause}
        </Button>
      )}
      <HoldToStop />
    </div>
  );
}
