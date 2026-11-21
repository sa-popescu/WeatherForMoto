import { useState } from 'react';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { Button, Card } from '../../ui/primitives';
import type { PointWeather, RouteLine, SamplePoint } from '../route/types';
import { aheadPoints } from './logic/ahead';
import { rideRecorder } from './recorder';
import { RideHistorySheet } from './RideHistorySheet';
import { RIDE_S } from './strings';
import { useRideHistory } from './useRideHistory';
import './ride-entry.css';

// The ways into a ride, on "Traseu". The ride screen itself opens over the
// app (App.tsx). Both start from a tap, which iPhone needs before it speaks.

/** A ride with no route: speed, distance and time. And the way to the rides already kept. */
export function RideEntry() {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const history = useRideHistory();
  const [historyOpen, setHistoryOpen] = useState(false);
  return (
    <Card className="ride-entry">
      <div>
        <h2 className="ride-entry__title num">{s.entryTitle}</h2>
        <p className="ride-entry__lead">{s.entryLead}</p>
      </div>
      <Button size="lg" full icon="play" onClick={() => rideRecorder.start({ lang })}>
        {s.entryStart}
      </Button>
      {history.items.length > 0 && (
        <Button variant="ghost" full icon="clock" onClick={() => setHistoryOpen(true)}>
          {fmt(s.historyButton, { n: history.items.length })}
        </Button>
      )}
      <RideHistorySheet open={historyOpen} onClose={() => setHistoryOpen(false)} history={history} />
    </Card>
  );
}

interface RouteRideStartProps {
  route: RouteLine;
  /** Where the route ends. */
  destination: string;
  /** The planner's average speed, km/h. */
  speed: number;
  /** The plan's weather points (with their final names) and their forecasts, for the warnings ahead. */
  points: readonly SamplePoint[];
  weather: Readonly<Record<string, PointWeather | undefined>>;
}

/** A ride along the route on screen, planned or imported from a GPX. */
export function RouteRideStart({ route, destination, speed, points, weather }: RouteRideStartProps) {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const start = (): void =>
    rideRecorder.start({
      lang,
      route: {
        name: destination,
        coords: route.coords,
        cumKm: route.cumKm,
        distanceKm: route.distanceKm,
        plannedKmh: speed,
        // The forecasts go along as they are now, so the warnings work without a connection.
        ahead: aheadPoints(points, weather, Date.now()),
      },
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
