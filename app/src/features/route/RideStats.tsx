import { dayMonth, fmtNumber } from '../../lib/format';
import { useLang, useStrings } from '../../lib/i18n';
import type { RideStats } from '../../lib/types';
import { CORE } from '../../i18n/core';
import { Button, Card, Skeleton } from '../../ui/primitives';
import { AS } from './actionStrings';
import { serverTimeMs } from './hazards';
import { fmtDuration, fmtKm } from './routeFormat';
import { RS } from './strings';
import type { Loadable } from './useAccountData';

// Totals and the last few rides of the signed-in rider.

const RECENT = 5;

export function RideStatsCard({ state, onRetry }: { state: Loadable<RideStats>; onRetry: () => void }) {
  const as = useStrings(AS);
  const s = useStrings(RS);
  const core = useStrings(CORE);
  const lang = useLang();
  const stats = state.data;

  return (
    <Card className="route-stats" aria-labelledby="route-stats-title">
      <h2 className="eyebrow" id="route-stats-title">
        {as.statsTitle}
      </h2>
      {state.status === 'loading' && !stats && <Skeleton height={120} radius={14} />}
      {state.status === 'error' && !stats && (
        <div className="route-inline-error">
          <p className="route-note">{as.statsErr}</p>
          <Button icon="refresh" onClick={onRetry}>
            {core.retry}
          </Button>
        </div>
      )}
      {stats && stats.rides === 0 && <p className="route-note">{as.statsEmpty}</p>}
      {stats && stats.rides > 0 && (
        <>
          <dl className="route-stats__grid">
            <div className="route-stat">
              <dt>{as.statsRides}</dt>
              <dd className="num">{fmtNumber(stats.rides, lang)}</dd>
            </div>
            <div className="route-stat">
              <dt>{as.statsDistance}</dt>
              <dd className="num">{fmtKm(stats.total_distance_km, lang)} km</dd>
            </div>
            <div className="route-stat">
              <dt>{as.statsTime}</dt>
              <dd className="num">{fmtDuration(stats.total_duration_min, s)}</dd>
            </div>
            <div className="route-stat">
              <dt>{as.statsAvg}</dt>
              <dd className="num">{fmtNumber(stats.avg_score, lang)}</dd>
            </div>
          </dl>
          {stats.recent.length > 0 && (
            <>
              <h3 className="route-stats__sub eyebrow">{as.statsRecent}</h3>
              <ul className="route-rides">
                {stats.recent.slice(0, RECENT).map((ride) => (
                  <li key={ride.id} className="route-rides__row">
                    <span className="route-rides__name">
                      {ride.start_city} → {ride.end_city}
                    </span>
                    <span className="route-rides__meta num">
                      {dayMonth(new Date(serverTimeMs(ride.created_at)).toISOString().slice(0, 10), lang)} · {fmtKm(ride.distance_km, lang)} km ·{' '}
                      {fmtDuration(ride.duration_min, s)}
                      {ride.avg_moto_score != null && ` · ${ride.avg_moto_score}`}
                    </span>
                  </li>
                ))}
              </ul>
            </>
          )}
        </>
      )}
    </Card>
  );
}
