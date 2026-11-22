import { useMemo } from 'react';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { cx } from '../../ui/primitives';
import type { FuelPlanState } from '../route/useFuel';
import { FUEL_S } from '../route/fuelStrings';
import { fmtKm } from '../route/routeFormat';
import { seasonalRoadsOnRoute, type SeasonRisk } from '../route/seasonalClosures';
import { SEASONAL_S } from '../route/seasonalStrings';
import { useFuelPlan } from '../route/useFuel';
import type { DriveRoute } from './logic/driveRoute';

// The short lines under a Drum route card, after the weather: a high road
// that is usually closed that day, and where to fill up on a route longer
// than one tank (the planner has the details).

/** The high roads on the route that are usually closed, or may be, that day: one line, or null. */
function useSeasonalLine(route: DriveRoute, risk: SeasonRisk): string | null {
  const s = useStrings(SEASONAL_S);
  const roads = useMemo(() => seasonalRoadsOnRoute(route.line.coords), [route]);
  if (risk === 'open' || roads.length === 0) return null;
  return roads.map((road) => fmt(risk === 'closed' ? s.shortClosed : s.shortMaybe, { name: road.name })).join(' · ');
}

/** Where to fill up, or the stretch with no station in reach: one line, or null. */
function useFuelLine(fuel: FuelPlanState): { text: string; warn: boolean } | null {
  const s = useStrings(FUEL_S);
  const lang = useLang();
  if (fuel.status !== 'ready') return null;
  const km = (value: number): string => fmtKm(value, lang);
  const gap = fuel.plan.gaps.find((g) => g.tooLong);
  if (gap) return { text: fmt(s.driveGap, { from: km(gap.fromKm), to: km(gap.toKm) }), warn: true };
  const [first, ...rest] = fuel.plan.stops;
  if (!first) return null;
  const text = fmt(s.driveStop, { km: km(first.km), name: first.label || s.anyStation });
  return { text: rest.length > 0 ? fmt(s.driveMore, { text, n: rest.length }) : text, warn: false };
}

export function RouteNotes({ route, season }: { route: DriveRoute; season: SeasonRisk }) {
  const seasonal = useSeasonalLine(route, season);
  const fuel = useFuelLine(useFuelPlan(route.line));
  return (
    <>
      {seasonal && (
        <span className={cx('drive-option__warning', season === 'closed' && 'drive-option__warning--closed')}>
          <Icon name="alert" size={14} />
          {seasonal}
        </span>
      )}
      {fuel && (
        <span className={cx('drive-option__warning', !fuel.warn && 'drive-option__warning--info')}>
          <Icon name="fuel" size={14} />
          {fuel.text}
        </span>
      )}
    </>
  );
}
