import { useStrings } from '../../lib/i18n';
import { useDesktop } from '../../lib/useMediaQuery';
import { useAuth } from '../../state/auth';
import { RideStatsCard } from '../route/RideStats';
import { useRideStats } from '../route/useAccountData';
import type { ScreenProps } from '../types';
import { RideEntry } from './RideEntry';
import { RideHistoryList } from './RideHistoryList';
import { RideTotals } from './RideTotals';
import { RIDE_S } from './strings';
import { useRideHistory } from './useRideHistory';
import './ride-entry.css';

// "Ture": a ride with no route, the statistics and records of the rides kept
// on this phone, the rides themselves and, signed in, the totals of the
// account's ride log.

export default function RidesScreen({ active }: ScreenProps) {
  const s = useStrings(RIDE_S);
  const { status } = useAuth();
  const desktop = useDesktop();
  const history = useRideHistory();
  const stats = useRideStats(active);

  return (
    <div className="screen rides" hidden={!active}>
      <h1 className="rides__title num">{s.ridesTitle}</h1>
      {/* A ride is recorded by a phone on the handlebar, not by a computer. */}
      {!desktop && <RideEntry />}
      {history.status === 'ready' && <RideTotals items={history.items} />}
      <RideHistoryList history={history} />
      {status === 'signed-in' && <RideStatsCard state={stats} onRetry={stats.reload} />}
    </div>
  );
}
