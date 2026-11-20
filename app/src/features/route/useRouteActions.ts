import { useCallback, useMemo } from 'react';
import { api, ApiError, type HazardInput } from '../../lib/api';
import { dayMonth, dayName } from '../../lib/format';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { shareLink } from '../../lib/share';
import type { SavedRoute } from '../../lib/types';
import { CORE } from '../../i18n/core';
import { useAuth } from '../../state/auth';
import { useToast } from '../../state/toast';
import { AS } from './actionStrings';
import { buildGpx, gpxFileName } from './gpx';
import type { RouteStats } from './plan';
import { buildRouteLink, type ShareStop } from './routeLink';
import { fmtKm } from './routeFormat';
import { savedTrackOf } from './savedTrack';
import { shortRouteLink } from './shortLink';
import { savedLabel } from './stops';
import { rideMinutes } from './timing';
import type { Departure, RouteLine, SamplePoint, StopDraft } from './types';

// The write actions of the screen. Each returns true when it succeeded, so
// the sheet that started it knows whether to close.

/** What sharing needs beyond the route itself. */
export interface ShareRequest {
  departure: Departure;
  /** The imported road when the route came from a GPX file, else null. */
  track: RouteLine | null;
  /** The stops were edited after the route was calculated. */
  stale: boolean;
}

interface Options {
  route: RouteLine | null;
  points: readonly SamplePoint[];
  stops: readonly StopDraft[];
  stats: RouteStats;
  speed: number;
  onSavedChange: () => void;
  onLogged: () => void;
  onReported: () => void;
}

const round1 = (v: number): number => Math.round(v * 10) / 10;

export function useRouteActions({ route, points, stops, stats, speed, onSavedChange, onLogged, onReported }: Options) {
  const { token, handleAuthError } = useAuth();
  const toast = useToast();
  const lang = useLang();
  const as = useStrings(AS);
  const core = useStrings(CORE);

  const names = useMemo(() => stops.map((st) => st.place?.name ?? st.text.trim()), [stops]);
  const routeName = `${names[0] ?? ''} → ${names[names.length - 1] ?? ''}`;

  const fail = useCallback(
    (err: unknown, fallback: string) => {
      console.warn('[route] action failed', err);
      handleAuthError(err);
      toast(err instanceof ApiError && err.isNetwork ? core.networkError : fallback, { tone: 'error' });
    },
    [core.networkError, handleAuthError, toast],
  );

  /** `road` is the imported GPX road the stops were calculated on, else null. */
  const saveRoute = useCallback(
    async (name: string, road: RouteLine | null): Promise<boolean> => {
      if (!token || !route) return false;
      try {
        await api.saveRoute(token, {
          name: name.trim(),
          stops: stops.map(savedLabel).filter(Boolean),
          total_distance_km: round1(route.distanceKm),
          track: road ? savedTrackOf(road, stops) : null,
        });
        toast(as.saveOk, { tone: 'success' });
        onSavedChange();
        return true;
      } catch (err) {
        fail(err, as.saveErr);
        return false;
      }
    },
    [as.saveErr, as.saveOk, fail, onSavedChange, route, stops, toast, token],
  );

  const deleteRoute = useCallback(
    async (saved: SavedRoute): Promise<boolean> => {
      if (!token) return false;
      try {
        await api.deleteRoute(token, saved.id);
        toast(as.deleteOk, { tone: 'success' });
        onSavedChange();
        return true;
      } catch (err) {
        fail(err, as.deleteErr);
        return false;
      }
    },
    [as.deleteErr, as.deleteOk, fail, onSavedChange, toast, token],
  );

  const exportGpx = useCallback(() => {
    if (!route) return;
    try {
      const waypoints = points.filter((p) => p.stopIndex !== null).map((p) => ({ lat: p.lat, lon: p.lon, name: p.name }));
      const gpx = buildGpx({ name: routeName, line: route.coords, waypoints, createdIso: new Date().toISOString() });
      const url = URL.createObjectURL(new Blob([gpx], { type: 'application/gpx+xml' }));
      const link = document.createElement('a');
      link.href = url;
      link.download = gpxFileName(routeName);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.setTimeout(() => URL.revokeObjectURL(url), 1000);
      toast(as.gpxOk, { tone: 'success' });
    } catch (err) {
      console.warn('[route] GPX export failed', err);
      toast(as.gpxErr, { tone: 'error' });
    }
  }, [as.gpxErr, as.gpxOk, points, route, routeName, toast]);

  const logRide = useCallback(async (): Promise<boolean> => {
    if (!token || !route) return false;
    try {
      await api.logRide(token, {
        route_name: routeName.slice(0, 80),
        start_city: (names[0] ?? '').slice(0, 120),
        end_city: (names[names.length - 1] ?? '').slice(0, 120),
        distance_km: Math.max(0.1, round1(route.distanceKm)),
        duration_min: Math.min(24 * 60, rideMinutes(route.distanceKm, speed)),
        avg_moto_score: stats.avgScore,
        max_wind_gust: stats.maxGustKmh,
        max_precip: stats.maxPrecipMm,
      });
      toast(as.logOk, { tone: 'success' });
      onLogged();
      return true;
    } catch (err) {
      fail(err, as.logErr);
      return false;
    }
  }, [as.logErr, as.logOk, fail, names, onLogged, route, routeName, speed, stats, toast, token]);

  const reportHazard = useCallback(
    async (input: HazardInput): Promise<boolean> => {
      if (!token) return false;
      try {
        await api.reportHazard(token, input);
        toast(as.hzOk, { tone: 'success' });
        onReported();
        return true;
      } catch (err) {
        if (err instanceof ApiError && err.status === 429) {
          toast(as.hzRate, { tone: 'error' });
          return false;
        }
        fail(err, as.hzErr);
        return false;
      }
    },
    [as.hzErr, as.hzOk, as.hzRate, fail, onReported, toast, token],
  );

  const shareRoute = useCallback(
    async ({ departure, track, stale }: ShareRequest): Promise<void> => {
      if (!route) return;
      const shared: ShareStop[] = [];
      for (const st of stops) {
        if (!st.place) break;
        shared.push({ name: st.place.name, lat: st.place.lat, lon: st.place.lon, gps: st.gps });
      }
      // A link made from edited stops would not open the route on screen.
      if (stale || shared.length !== stops.length) {
        toast(as.shareStale, { tone: 'error' });
        return;
      }
      // A long link (an imported road) goes out as a short one when the Worker answers in time.
      const url = await shortRouteLink(buildRouteLink({ stops: shared, departure, speed, line: track ? track.coords : null }));
      const text = fmt(as.shareText, {
        route: routeName,
        day: `${dayName(departure.date, lang)} ${dayMonth(departure.date, lang)}`,
        time: departure.time,
        km: fmtKm(route.distanceKm, lang),
      });
      try {
        if ((await shareLink(url, text)) === 'copied') toast(as.shareCopied, { tone: 'success' });
      } catch (err) {
        console.warn('[route] share failed', err);
        toast(as.shareErr, { tone: 'error' });
      }
    },
    [as.shareCopied, as.shareErr, as.shareStale, as.shareText, lang, route, routeName, speed, stops, toast],
  );

  return { routeName, saveRoute, deleteRoute, exportGpx, logRide, reportHazard, shareRoute };
}

export type RouteActionsApi = ReturnType<typeof useRouteActions>;
