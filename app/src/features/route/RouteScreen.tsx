import { useCallback, useEffect, useMemo, useState } from 'react';
import { fmt, useStrings } from '../../lib/i18n';
import { useComputer, useWideLayout } from '../../lib/useMediaQuery';
import { useAuth } from '../../state/auth';
import { Icon } from '../../ui/icons';
import { Banner, Button } from '../../ui/primitives';
import { RouteRideStart } from '../ride/RideEntry';
import type { ScreenProps } from '../types';
import { DepartureControls } from './DepartureControls';
import { HazardSheet } from './HazardSheet';
import type { ActionKind } from './RouteActions';
import { RoutePreviewMap } from './RoutePreviewMap';
import { RouteDetails, RouteOverview } from './RouteResults';
import { RouteStatus } from './RouteStatus';
import { SavedRoutesSheet } from './SavedRoutesSheet';
import { LogRideSheet, SaveRouteSheet, SignInSheet } from './SimpleSheets';
import { RouteOptionsButton } from './RouteOptionsSheet';
import { StopsEditor } from './StopsEditor';
import { RS } from './strings';
import { SWEEP_LAST } from './sweep';
import {
  daysBetween,
  defaultDeparture,
  DEFAULT_SPEED,
  deviceOffsetSec,
  forecastDaysFor,
  MAX_FORECAST_DAYS,
  minutesOf,
  nextDates,
  rideMinutes,
  type Speed,
} from './timing';
import type { Departure } from './types';
import { useSavedRoutes } from './useAccountData';
import { useCalculate } from './useCalculate';
import { useDerivedPlan } from './useDerivedPlan';
import { useFileDrop } from './useFileDrop';
import { useFuelPlan } from './useFuel';
import { useOpenRouteLink } from './useOpenRouteLink';
import { useRouteActions } from './useRouteActions';
import { useRouteHazards } from './useRouteHazards';
import { useRoutePlan } from './useRoutePlan';
import { useStopsState } from './useStopsState';
import './route.css';
import './route-results.css';
import './route-desktop.css';

// "Traseu": plan a ride of 2 to 10 stops and see the weather at every point
// at the moment you get there, plus the best time to leave.

type SheetKind = 'none' | 'save' | 'log' | 'hazard' | 'signin' | 'saved';

// The ride log's totals live on "Ture", which reloads them whenever it opens.
const NOTHING_TO_RELOAD = (): void => {};

export default function RouteScreen({ active }: ScreenProps) {
  const s = useStrings(RS);
  const { status: authStatus } = useAuth();
  const signedIn = authStatus === 'signed-in';
  // `active` is a deliberate dependency: returning to the tab refreshes "today".
  const dates = useMemo(() => nextDates(Date.now(), deviceOffsetSec()), [active]);
  const [departure, setDeparture] = useState<Departure>(() => defaultDeparture(Date.now(), deviceOffsetSec()));
  const [speed, setSpeed] = useState<Speed>(DEFAULT_SPEED);
  const [sheet, setSheet] = useState<SheetKind>('none');
  // The layout (a desktop window or a phone turned sideways) and the device (a computer rides nowhere).
  const wide = useWideLayout();
  const computer = useComputer();

  const stopsState = useStopsState();
  const { state: plan, calculate, calculateFromTrack, ensureDays, retryFailed } = useRoutePlan();
  const derived = useDerivedPlan(plan, departure, speed);
  const hazards = useRouteHazards(plan.route, plan.points);
  const fuel = useFuelPlan(plan.route);
  const saved = useSavedRoutes(active);
  const calc = useCalculate({ stopsState, calculate, calculateFromTrack, hasRoute: plan.route !== null, departure, speed, today: dates[0] });
  const actions = useRouteActions({
    route: plan.route,
    points: plan.points,
    stops: stopsState.stops,
    stats: derived.stats,
    speed,
    onSavedChange: saved.reload,
    onLogged: NOTHING_TO_RELOAD,
    onReported: hazards.reload,
  });
  useOpenRouteLink({ dates, speed, setDeparture, setSpeed, openShared: calc.openShared });

  // A departure further out needs the longer forecast: reload the points once.
  useEffect(() => {
    if (!plan.route) return;
    const latest = Math.max(minutesOf(departure.time), minutesOf(SWEEP_LAST));
    ensureDays(forecastDaysFor(daysBetween(dates[0], departure.date), latest, plan.route.distanceKm / speed));
  }, [plan.route, departure, speed, dates, ensureDays]);

  const onAction = useCallback(
    (kind: ActionKind) => {
      if (kind === 'share') void actions.shareRoute({ departure, track: calc.sharedTrack(), stale: calc.stale });
      else if (kind === 'gpx') actions.exportGpx();
      else setSheet(signedIn ? kind : 'signin');
    },
    [actions, calc, departure, signedIn],
  );

  // A GPX file dragged onto the screen imports like the button does.
  const dragging = useFileDrop(active, (file) => void calc.importGpx(file));

  // Comparing days needs the forecast of every planner day at every point.
  const needAllDays = useCallback(() => ensureDays(MAX_FORECAST_DAYS), [ensureDays]);

  const close = useCallback(() => setSheet('none'), []);
  const busy = plan.step === 'geocoding' || plan.step === 'routing';
  // New options redraw a routed road at once; an imported track is its own road. On a phone they
  // sit above the calculate button; in the wide layout over the road they shape, so the plan
  // column keeps its height and its button in reach.
  const optionsButton = <RouteOptionsButton onChanged={() => plan.routeInfo && calc.run(stopsState.stops)} />;

  // Three column groups for the desktop layout (route-desktop.css): the plan,
  // the map with when to leave, and the road. On a phone the groups add no box
  // (display: contents), so the order below is the order on screen.
  return (
    <div className="screen route" hidden={!active}>
      <div className="route-col route-col--plan">
        <header className="route-head">
          <h1 className="route-head__title num">{s.title}</h1>
          {signedIn && (
            <button type="button" className="route-head__saved" onClick={() => setSheet('saved')}>
              <Icon name="bookmark" size={18} />
              {fmt(s.savedButton, { n: saved.data?.length ?? 0 })}
            </button>
          )}
        </header>

        <StopsEditor state={stopsState} onImportGpx={(file) => void calc.importGpx(file)} />
        {calc.formError && (
          <Banner tone="error" icon="alert">
            {calc.formError}
          </Banner>
        )}
        <DepartureControls dates={dates} departure={departure} onDeparture={setDeparture} speed={speed} onSpeed={setSpeed} />
        {!wide && optionsButton}
        <Button variant="primary" size="lg" full icon="route" busy={busy} onClick={() => calc.run(stopsState.stops)}>
          {plan.route ? s.recalculate : s.calculate}
        </Button>
      </div>

      <div className="route-col route-col--map">
        {wide && optionsButton}
        <RouteStatus
          state={plan}
          settled={derived.settled}
          failed={derived.failed}
          stale={calc.stale}
          onRetry={() => calc.run(stopsState.stops)}
          onRetryWeather={retryFailed}
          onExample={calc.runExample}
        />
        {/* A desktop column would be empty before the first route: the area and the stops so far. */}
        {!plan.route && wide && <RoutePreviewMap stops={stopsState.stops} appPlace={stopsState.appPlace} active={active} />}
        {plan.route && (
          <RouteOverview
            active={active}
            route={plan.route}
            derived={derived}
            dates={dates}
            departure={departure}
            speed={speed}
            weatherDone={plan.step === 'ready'}
            hazards={hazards}
            fuel={fuel}
            onAdopt={(time) => setDeparture((d) => ({ ...d, time }))}
            onPickDay={setDeparture}
            onNeedAllDays={needAllDays}
          />
        )}
        {plan.route && !computer && (
          <RouteRideStart
            route={plan.route}
            destination={derived.rows[derived.rows.length - 1]?.point.name ?? s.title}
            speed={speed}
            points={derived.rows.map((row) => row.point)}
            weather={plan.weather}
          />
        )}
      </div>

      <div className="route-col route-col--road">
        {plan.route && (
          <RouteDetails route={plan.route} derived={derived} hazards={hazards} fuel={fuel} date={departure.date} signedIn={signedIn} onAction={onAction} />
        )}
        {!plan.route && wide && (
          <section className="route-road-hint" aria-labelledby="route-road-hint-title">
            <h2 className="eyebrow" id="route-road-hint-title">
              {s.roadHintTitle}
            </h2>
            <p>{s.roadHint}</p>
          </section>
        )}
      </div>

      {dragging && (
        <div className="route-drop" aria-hidden="true">
          <div className="route-drop__box">
            <Icon name="route" size={40} />
            <span>{s.dropGpx}</span>
          </div>
        </div>
      )}

      <SignInSheet open={sheet === 'signin'} onClose={close} />
      <SaveRouteSheet
        open={sheet === 'save'}
        onClose={close}
        defaultName={actions.routeName}
        // Edited stops no longer sit on the imported road: save them by name only.
        onSave={(name) => actions.saveRoute(name, calc.stale ? null : calc.sharedTrack())}
      />
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
