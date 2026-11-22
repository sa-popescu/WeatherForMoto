import { fmtNumber } from '../../lib/format';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { SectionLabel, Spinner } from '../../ui/primitives';
import { USABLE_SHARE, type FuelGap, type FuelPlan, type StationOnRoute } from './fuel';
import { FUEL_S } from './fuelStrings';
import { fmtKm } from './routeFormat';
import type { RouteLine } from './types';
import type { FuelPlanState } from './useFuel';

// In the planner: where to fill up on the route, and the stretches with no
// station within reach. Without a range set, a short pointer to the setting
// on routes long enough to need it.

/** Below this, no route needs a word about fuel, whatever the range. */
const HINT_FROM_KM = 120;

type Item = { kind: 'stop'; km: number; stop: StationOnRoute } | { kind: 'gap'; km: number; gap: FuelGap };

/** Stops and warnings in the order the rider meets them. */
function alongTheRoad(plan: FuelPlan): Item[] {
  const items: Item[] = [
    ...plan.stops.map((stop): Item => ({ kind: 'stop', km: stop.km, stop })),
    ...plan.gaps.map((gap): Item => ({ kind: 'gap', km: gap.fromKm, gap })),
  ];
  // A gap starts where the last fill-up was: at the same point, the stop comes first.
  return items.sort((a, b) => a.km - b.km || (a.kind === 'stop' ? -1 : 1));
}

interface Props {
  route: RouteLine;
  state: FuelPlanState;
}

export function FuelStops({ route, state }: Props) {
  const s = useStrings(FUEL_S);
  const lang = useLang();
  if (state.status === 'short' || (state.status === 'off' && route.distanceKm < HINT_FROM_KM)) return null;
  const km = (value: number): string => fmtKm(value, lang);

  return (
    <section className="route-fuel" aria-labelledby="route-fuel-title" aria-busy={state.status === 'loading' || undefined}>
      <SectionLabel id="route-fuel-title">{s.title}</SectionLabel>
      {state.status === 'off' && (
        <p className="route-note">
          {s.noRange}{' '}
          <a className="route-fuel__link" href="#/eu">
            {s.setRange}
          </a>
        </p>
      )}
      {state.status === 'loading' && (
        <p className="route-note">
          <Spinner size={16} /> {s.loading}
        </p>
      )}
      {state.status === 'error' && <p className="route-note">{s.error}</p>}
      {state.status === 'ready' && (
        <>
          <ul className="route-fuel__list">
            {alongTheRoad(state.plan).map((item) =>
              item.kind === 'gap' ? (
                <li key={`gap-${item.gap.fromKm}`} className="route-fuel__item route-fuel__item--gap">
                  <Icon name="alert" size={22} />
                  <span>{fmt(item.gap.tooLong ? s.gapTooLong : s.gapTight, { from: km(item.gap.fromKm), to: km(item.gap.toKm), range: state.range })}</span>
                </li>
              ) : (
                <li key={`stop-${item.stop.km}`} className="route-fuel__item">
                  <Icon name="fuel" size={22} />
                  <span className="route-fuel__text">
                    <span className="route-fuel__title">{fmt(s.stop, { km: km(item.stop.km), name: item.stop.label || s.anyStation })}</span>
                    <span className="route-fuel__meta">
                      {item.stop.offKm < 0.2 ? s.nearRoad : fmt(s.offRoad, { d: fmtNumber(item.stop.offKm, lang, 1) })}
                    </span>
                  </span>
                </li>
              ),
            )}
          </ul>
          <p className="route-note route-note--small">{fmt(s.note, { pct: Math.round(USABLE_SHARE * 100), range: state.range })}</p>
        </>
      )}
    </section>
  );
}
