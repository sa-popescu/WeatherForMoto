import { fmt, useLang, useStrings } from '../../lib/i18n';
import { DayCompare } from './DayCompare';
import type { StationOnRoute } from './fuel';
import { DepartureSweep } from './DepartureSweep';
import { FuelStops } from './FuelStops';
import { HazardList, useHazardText } from './HazardList';
import { OfflineMapPanel } from './OfflineMapPanel';
import { RouteActions, type ActionKind } from './RouteActions';
import { fmtDuration, fmtKm } from './routeFormat';
import { RouteMap } from './RouteMap';
import { SeasonalRoads } from './SeasonalRoads';
import { RS } from './strings';
import { rideMinutes } from './timing';
import type { Departure, RouteLine } from './types';
import type { DerivedPlan } from './useDerivedPlan';
import type { FuelPlanState } from './useFuel';
import type { HazardsState } from './useRouteHazards';
import { WaypointList } from './WaypointList';

// Everything below the calculate button once a route exists, in the order of
// the Cockpit design: when to leave, map, weather on the road, hazards, actions.
// It comes in two parts because the desktop layout puts them in different
// columns (route-desktop.css): the overview (when to leave and the map) and
// the details along the road. On a phone they simply follow each other.

const NO_STOPS: readonly StationOnRoute[] = [];

/** "From → to", for the offline map's name in Eu. */
function routeName(derived: DerivedPlan): string {
  const names = derived.rows.filter((row) => row.point.stopIndex !== null).map((row) => row.point.name);
  return names.length >= 2 ? `${names[0]} → ${names[names.length - 1]}` : (names[0] ?? '');
}

interface RouteOverviewProps {
  active: boolean;
  route: RouteLine;
  derived: DerivedPlan;
  /** The days the planner offers, for the day comparison. */
  dates: readonly string[];
  departure: Departure;
  speed: number;
  weatherDone: boolean;
  hazards: HazardsState;
  fuel: FuelPlanState;
  onAdopt: (time: string) => void;
  /** A day picked in the comparison, with its best departure. */
  onPickDay: (departure: Departure) => void;
  onNeedAllDays: () => void;
}

/** When to leave (with the day comparison) and the map of the route. */
export function RouteOverview(props: RouteOverviewProps) {
  const { active, route, derived, dates, departure, speed, weatherDone, hazards, fuel, onAdopt, onPickDay, onNeedAllDays } = props;
  const s = useStrings(RS);
  const lang = useLang();
  const { summary: hazardText } = useHazardText();
  const summary = fmt(s.summary, { km: fmtKm(route.distanceKm, lang), dur: fmtDuration(rideMinutes(route.distanceKm, speed), s) });

  return (
    <>
      <DepartureSweep
        bars={derived.bars}
        best={derived.best}
        chosenTime={departure.time}
        chosenWorst={derived.stats.worstScore}
        chosenWorstName={derived.stats.worstName}
        ready={weatherDone}
        onAdopt={onAdopt}
        footer={
          <DayCompare
            dates={dates}
            departure={departure}
            speed={speed}
            points={derived.sweepPoints}
            originOffsetSec={derived.originOffset}
            loading={!weatherDone}
            onNeedAllDays={onNeedAllDays}
            onPick={onPickDay}
          />
        }
      />
      <RouteMap
        route={route}
        rows={derived.rows}
        segments={derived.segments}
        hazards={hazards.list}
        hazardText={hazardText}
        fuelStops={fuel.status === 'ready' ? fuel.plan.stops : NO_STOPS}
        active={active}
        summary={summary}
      />
    </>
  );
}

interface RouteDetailsProps {
  route: RouteLine;
  derived: DerivedPlan;
  hazards: HazardsState;
  fuel: FuelPlanState;
  /** The day of the ride, "YYYY-MM-DD", for the roads closed in winter. */
  date: string;
  signedIn: boolean;
  onAction: (kind: ActionKind) => void;
}

/** The roads closed in winter, the weather along the road, where to fill up, the hazards and the actions. */
export function RouteDetails({ route, derived, hazards, fuel, date, signedIn, onAction }: RouteDetailsProps) {
  return (
    <>
      <SeasonalRoads route={route} date={date} />
      <WaypointList rows={derived.rows} route={route} />
      <FuelStops route={route} state={fuel} />
      <HazardList state={hazards} />
      <OfflineMapPanel line={route.coords} name={routeName(derived)} />
      <RouteActions signedIn={signedIn} onAction={onAction} />
    </>
  );
}
