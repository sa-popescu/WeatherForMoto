import { useMemo } from 'react';
import { fmt, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { SectionLabel } from '../../ui/primitives';
import { LIVE_CLOSURES_URL, seasonalRoadsOnRoute, seasonRisk } from './seasonalClosures';
import { SEASONAL_S } from './seasonalStrings';
import type { RouteLine } from './types';

// In the planner: the high roads on the route that are usually closed, or may
// be, on the day of the ride, with a link to the closures right now.

interface Props {
  route: RouteLine;
  /** The day of the ride, "YYYY-MM-DD". */
  date: string;
}

export function SeasonalRoads({ route, date }: Props) {
  const s = useStrings(SEASONAL_S);
  const roads = useMemo(() => seasonalRoadsOnRoute(route.coords), [route]);
  const risk = seasonRisk(date);
  if (roads.length === 0 || risk === 'open') return null;

  return (
    <section className="route-hz" aria-labelledby="route-season-title">
      <SectionLabel id="route-season-title">{s.title}</SectionLabel>
      <ul className="route-hz__list">
        {roads.map((road) => (
          <li key={road.id} className="route-hz__item">
            <span className="route-hz__icon" style={{ color: risk === 'closed' ? 'var(--t-evita)' : 'var(--t-atentie)' }}>
              <Icon name="alert" size={26} />
            </span>
            <span className="route-hz__text">
              <span className="route-hz__title">{road.name}</span>
              <span className="route-hz__desc">{fmt(risk === 'closed' ? s.closed : s.maybe, { from: road.from, to: road.to })}</span>
              <a className="route-season__link" href={LIVE_CLOSURES_URL} target="_blank" rel="noopener noreferrer">
                {s.check}
              </a>
            </span>
          </li>
        ))}
      </ul>
    </section>
  );
}
