import { useCallback, useEffect, useMemo, useState } from 'react';
import { fmt, useStrings } from '../../lib/i18n';
import { useAuth } from '../../state/auth';
import { Icon } from '../../ui/icons';
import { Banner, Button } from '../../ui/primitives';
import type { ScreenProps } from '../types';
import { DepartureControls } from './DepartureControls';
import { HazardSheet } from './HazardSheet';
import { RideStatsCard } from './RideStats';
import type { ActionKind } from './RouteActions';
import { RouteResults } from './RouteResults';
import { RouteStatus } from './RouteStatus';
import { SavedRoutesSheet } from './SavedRoutesSheet';
import { LogRideSheet, SaveRouteSheet, SignInSheet } from './SimpleSheets';
import { StopsEditor } from './StopsEditor';
import { RS } from './strings';
import { SWEEP_LAST } from './sweep';
import { daysBetween, defaultDeparture, DEFAULT_SPEED, deviceOffsetSec, forecastDaysFor, minutesOf, nextDates, rideMinutes, type Speed } from './timing';
import type { Departure } from './types';
import { useRideStats, useSavedRoutes } from './useAccountData';
import { useCalculate } from './useCalculate';
import { useDerivedPlan } from './useDerivedPlan';
import { useRouteActions } from './useRouteActions';
import { useRouteHazards } from './useRouteHazards';
import { useRoutePlan } from './useRoutePlan';
import { useStopsState } from './useStopsState';
import './route.css';
import './route-results.css';

// "Traseu": plan a ride of 2 to 5 stops and see the weather at every point
// at the moment you get there, plus the best time to leave.

type SheetKind = 'none' | 'save' | 'log' | 'hazard' | 'signin' | 'saved';

export default function RouteScreen({ active }: ScreenProps) {
  const s = useStrings(RS);
  const { status: authStatus } = useAuth();
  const signedIn = authStatus === 'signed-in';
  // `active` is a deliberate dependency: returning to the tab refreshes "today".
  const dates = useMemo(() => nextDates(Date.now(), deviceOffsetSec()), [active]);
  const [departure, setDeparture] = useState<Departure>(() => defaultDeparture(Date.now(), deviceOffsetSec()));
  const [speed, setSpeed] = useState<Speed>(DEFAULT_SPEED);
  const [sheet, setSheet] = useState<SheetKind>('none');

  const stopsState = useStopsState();
  const { state: plan, calculate, ensureDays, retryFailed } = useRoutePlan();
  const derived = useDerivedPlan(plan, departure, speed);
  const hazards = useRouteHazards(plan.route, plan.points);
  const saved = useSavedRoutes(active);
  const rides = useRideStats(active);
  const calc = useCalculate({ stopsState, calculate, hasRoute: plan.route !== null, departure, speed, today: dates[0] });
  const actions = useRouteActions({
    route: plan.route,
    points: plan.points,
    stops: stopsState.stops,
    stats: derived.stats,
    speed,
    onSavedChange: saved.reload,
    onLogged: rides.reload,
    onReported: hazards.reload,
  });

  // A departure further out needs the longer forecast: reload the points once.
  useEffect(() => {
    if (!plan.route) return;
    const latest = Math.max(minutesOf(departure.time), minutesOf(SWEEP_LAST));
    ensureDays(forecastDaysFor(daysBetween(dates[0], departure.date), latest, plan.route.distanceKm / speed));
  }, [plan.route, departure, speed, dates, ensureDays]);

  const onAction = useCallback(
    (kind: ActionKind) => {
      if (kind === 'gpx') actions.exportGpx();
      else setSheet(signedIn ? kind : 'signin');
    },
    [actions, signedIn],
  );

  const close = useCallback(() => setSheet('none'), []);
  const busy = plan.step === 'geocoding' || plan.step === 'routing';

  return (
    <div className="screen route" hidden={!active}>
      <header className="route-head">
        <h1 className="route-head__title num">{s.title}</h1>
        {signedIn && (
          <button type="button" className="route-head__saved" onClick={() => setSheet('saved')}>
            <Icon name="bookmark" size={18} />
            {fmt(s.savedButton, { n: saved.data?.length ?? 0 })}
          </button>
        )}
      </header>

      <StopsEditor state={stopsState} />
      {calc.formError && (
        <Banner tone="error" icon="alert">
          {calc.formError}
        </Banner>
      )}
      <DepartureControls dates={dates} departure={departure} onDeparture={setDeparture} speed={speed} onSpeed={setSpeed} />
      <Button variant="primary" size="lg" full icon="route" busy={busy} onClick={() => calc.run(stopsState.stops)}>
        {plan.route ? s.recalculate : s.calculate}
      </Button>

      <RouteStatus
        state={plan}
        settled={derived.settled}
        failed={derived.failed}
        stale={calc.stale}
        onRetry={() => calc.run(stopsState.stops)}
        onRetryWeather={retryFailed}
        onExample={calc.runExample}
      />

      {plan.route && (
        <RouteResults
          active={active}
          route={plan.route}
          derived={derived}
          departure={departure}
          speed={speed}
          weatherDone={plan.step === 'ready'}
          hazards={hazards}
          signedIn={signedIn}
          onAdopt={(time) => setDeparture((d) => ({ ...d, time }))}
          onAction={onAction}
        />
      )}

      {signedIn && <RideStatsCard state={rides} onRetry={rides.reload} />}

      <SignInSheet open={sheet === 'signin'} onClose={close} />
      <SaveRouteSheet open={sheet === 'save'} onClose={close} defaultName={actions.routeName} onSave={actions.saveRoute} />
      {plan.route && (
        <LogRideSheet
          open={sheet === 'log'}
          onClose={close}
          distanceKm={plan.route.distanceKm}
          durationMin={rideMinutes(plan.route.distanceKm, speed)}
          speed={speed}
          stats={derived.stats}
          onConfirm={actions.logRide}
        />
      )}
      <HazardSheet open={sheet === 'hazard'} onClose={close} points={plan.points} onSubmit={actions.reportHazard} />
      <SavedRoutesSheet
        open={sheet === 'saved'}
        onClose={close}
        saved={saved}
        onRetry={saved.reload}
        onLoad={(route) => void calc.loadSaved(route)}
        onDelete={actions.deleteRoute}
      />
    </div>
  );
}
