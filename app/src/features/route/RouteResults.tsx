import { fmt, useLang, useStrings } from '../../lib/i18n';
import { DayCompare } from './DayCompare';
import { DepartureSweep } from './DepartureSweep';
import { HazardList, useHazardText } from './HazardList';
import { RouteActions, type ActionKind } from './RouteActions';
import { fmtDuration, fmtKm } from './routeFormat';
import { RouteMap } from './RouteMap';
import { RS } from './strings';
import { rideMinutes } from './timing';
import type { Departure, RouteLine } from './types';
import type { DerivedPlan } from './useDerivedPlan';
import type { HazardsState } from './useRouteHazards';
import { WaypointList } from './WaypointList';

// Everything below the calculate button once a route exists, in the order of
// the Cockpit design: when to leave, map, weather on the road, hazards, actions.

interface RouteResultsProps {
  active: boolean;
  route: RouteLine;
  derived: DerivedPlan;
  /** The days the planner offers, for the day comparison. */
  dates: readonly string[];
  departure: Departure;
  speed: number;
  weatherDone: boolean;
  hazards: HazardsState;
  signedIn: boolean;
  onAdopt: (time: string) => void;
  /** A day picked in the comparison, with its best departure. */
  onPickDay: (departure: Departure) => void;
  onNeedAllDays: () => void;
  onAction: (kind: ActionKind) => void;
}

export function RouteResults(props: RouteResultsProps) {
  const { active, route, derived, dates, departure, speed, weatherDone, hazards, signedIn, onAdopt, onPickDay, onNeedAllDays, onAction } = props;
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
      <WaypointList rows={derived.rows} route={route} />
      <HazardList state={hazards} />
      <RouteActions signedIn={signedIn} onAction={onAction} />
    </>
  );
}
