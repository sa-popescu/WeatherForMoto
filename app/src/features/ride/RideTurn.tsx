import { useLang, useStrings } from '../../lib/i18n';
import type { RideSnapshot } from './recorder';
import { TurnIcon } from './TurnIcon';
import { capitalize, turnDistanceText, turnPhrase, turnShape } from './turnText';
import { TURN_S } from './turnStrings';

// The next turn, above the map, on a route from "Drum": the arrow, how far,
// and what to do onto which road, large enough to read at a glance. A turn
// right after it shows as a small arrow on the side. The voice says it too
// (guide.ts), so this is not read out by a screen reader on every fix.

export function RideTurn({ ride }: { ride: RideSnapshot }) {
  const s = useStrings(TURN_S);
  const lang = useLang();
  const next = ride.turn;
  const live = ride.phase === 'recording' || ride.phase === 'paused';
  if (!live || !next || ride.follow?.off || ride.follow?.arrived) return null;
  return (
    <section className="ride-turn" aria-label={s.nextTurn}>
      <span className="ride-turn__arrow">
        <TurnIcon shape={turnShape(next.turn.maneuver)} />
      </span>
      <span className="ride-turn__text">
        <span className="ride-turn__dist num">{turnDistanceText(next.distanceM, lang)}</span>
        <span className="ride-turn__what">{capitalize(turnPhrase(next.turn, lang))}</span>
      </span>
      {next.then && (
        <span className="ride-turn__then" aria-hidden="true">
          <TurnIcon shape={turnShape(next.then.maneuver)} size={24} />
        </span>
      )}
    </section>
  );
}
