import { useStrings } from '../../lib/i18n';
import { useDesktop } from '../../lib/useMediaQuery';
import { useAuth } from '../../state/auth';
import { Icon } from '../../ui/icons';
import { Card } from '../../ui/primitives';
import { RideStatsCard } from '../route/RideStats';
import { useRideStats } from '../route/useAccountData';
import type { ScreenProps } from '../types';
import { RideEntry } from './RideEntry';
import { RideHistoryList } from './RideHistoryList';
import { RidePersonal } from './RidePersonal';
import { RideTotals } from './RideTotals';
import { RIDE_S } from './strings';
import { useRideHistory } from './useRideHistory';
import './ride-entry.css';

// "Ture": a ride with no route, the statistics and records of the rides kept
// on this phone, the rides themselves and, signed in, the totals of the
// account's ride log. A computer shows where rides come from instead of its
// empty list.

export default function RidesScreen({ active }: ScreenProps) {
  const s = useStrings(RIDE_S);
  const { status } = useAuth();
  const desktop = useDesktop();
  const history = useRideHistory();
  const stats = useRideStats(active);
  // A computer records no rides, so its own list stays empty: the screen says
  // where rides come from instead of speaking of "this phone".
  const phoneOnly = desktop && history.status === 'ready' && history.items.length === 0;

  return (
    <div className="screen rides" hidden={!active}>
      <h1 className="rides__title num">{s.ridesTitle}</h1>
      {/* A ride is recorded by a phone on the handlebar, not by a computer. */}
      {!desktop && <RideEntry />}
      {phoneOnly ? (
        <Card className="rides-desktop">
          <span className="rides-desktop__icon" aria-hidden="true">
            <Icon name="bike" size={26} />
          </span>
          <h2 className="rides-desktop__title num">{s.desktopTitle}</h2>
          <p className="rides-desktop__lead">{s.desktopLead}</p>
          {status === 'signed-in' ? (
            <p className="rides-desktop__lead">{s.desktopSignedIn}</p>
          ) : (
            <a className="btn btn--secondary rides-desktop__signin" href="#/eu">
              <Icon name="user" size={20} />
              <span>{s.desktopSignIn}</span>
            </a>
          )}
        </Card>
      ) : (
        <>
          {history.status === 'ready' && <RideTotals items={history.items} />}
          <RidePersonal />
          <RideHistoryList history={history} />
        </>
      )}
      {status === 'signed-in' && <RideStatsCard state={stats} onRetry={stats.reload} />}
    </div>
  );
}
