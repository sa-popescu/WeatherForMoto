import { fmt, useLang, useStrings } from '../../lib/i18n';
import { DayCompare } from './DayCompare';
import { DepartureSweep } from './DepartureSweep';
import { HazardList, useHazardText } from './HazardList';
import { RouteActions, type ActionKind } from './RouteActions';
import { fmtDuration, fmtKm } from './routeFormat';
import { RouteMap } from './RouteMap';
import { SeasonalRoads } from './SeasonalRoads';
import { RS } from './strings';
import { rideMinutes } from './timing';
import type { Departure, RouteLine } from './types';
import type { DerivedPlan } from './useDerivedPlan';
import type { HazardsState } from './useRouteHazards';
import { WaypointList } from './WaypointList';

// Everything below the calculate button once a route exists, in the order of
// the Cockpit design: when to leave, map, weather on the road, hazards, actions.
// It comes in two parts because the desktop layout puts them in different
// columns (route-desktop.css): the overview (when to leave and the map) and
// the details along the road. On a phone they simply follow each other.

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
  onAdopt: (time: string) => void;
  /** A day picked in the comparison, with its best departure. */
  onPickDay: (departure: Departure) => void;
  onNeedAllDays: () => void;
}

/** When to leave (with the day comparison) and the map of the route. */
export function RouteOverview(props: RouteOverviewProps) {
  const { active, route, derived, dates, departure, speed, weatherDone, hazards, onAdopt, onPickDay, onNeedAllDays } = props;
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
  /** The day of the ride, "YYYY-MM-DD", for the roads closed in winter. */
  date: string;
  signedIn: boolean;
  onAction: (kind: ActionKind) => void;
}

/** The roads closed in winter, the weather at every point along the road, the hazards on it and the actions. */
export function RouteDetails({ route, derived, hazards, date, signedIn, onAction }: RouteDetailsProps) {
  return (
    <>
      <SeasonalRoads route={route} date={date} />
      <WaypointList rows={derived.rows} route={route} />
      <HazardList state={hazards} />
      <RouteActions signedIn={signedIn} onAction={onAction} />
    </>
  );
}
