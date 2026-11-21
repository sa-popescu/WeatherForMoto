import { useLang, useStrings } from '../../lib/i18n';
import { Button, Card } from '../../ui/primitives';
import type { RouteLine } from '../route/types';
import { rideRecorder } from './recorder';
import { RIDE_S } from './strings';
import './ride-entry.css';

// The ways into a ride, on "Traseu". The ride screen itself opens over the
// app (App.tsx). Both start from a tap, which iPhone needs before it speaks.

/** A ride with no route: speed, distance and time. */
export function RideEntry() {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  return (
    <Card className="ride-entry">
      <div>
        <h2 className="ride-entry__title num">{s.entryTitle}</h2>
        <p className="ride-entry__lead">{s.entryLead}</p>
      </div>
      <Button size="lg" full icon="play" onClick={() => rideRecorder.start({ lang })}>
        {s.entryStart}
      </Button>
    </Card>
  );
}

interface RouteRideStartProps {
  route: RouteLine;
  /** Where the route ends. */
  destination: string;
  /** The planner's average speed, km/h. */
  speed: number;
}

/** A ride along the route on screen, planned or imported from a GPX. */
export function RouteRideStart({ route, destination, speed }: RouteRideStartProps) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const start = (): void =>
    rideRecorder.start({
      lang,
      route: { name: destination, coords: route.coords, cumKm: route.cumKm, distanceKm: route.distanceKm, plannedKmh: speed },
    });
  return (
    <div className="ride-entry ride-entry--route">
      <Button variant="primary" size="lg" full icon="play" onClick={start}>
        {s.routeStart}
      </Button>
      <p className="ride-entry__lead">{s.routeStartHint}</p>
    </div>
  );
}
