import { useStrings } from '../../lib/i18n';
import { Button, Card } from '../../ui/primitives';
import { rideRecorder } from './recorder';
import { RIDE_S } from './strings';
import './ride-entry.css';

/** The way into a free ride, on "Traseu". The ride screen itself opens over the app (App.tsx). */
export function RideEntry() {
  const s = useStrings(RIDE_S);
  return (
    <Card className="ride-entry">
      <div>
        <h2 className="ride-entry__title num">{s.entryTitle}</h2>
        <p className="ride-entry__lead">{s.entryLead}</p>
      </div>
      <Button size="lg" full icon="play" onClick={() => rideRecorder.start()}>
        {s.entryStart}
      </Button>
    </Card>
  );
}
