import { useCallback, useEffect, useState } from 'react';
import { api } from '../../lib/api';
import type { RideStats, SavedRoute } from '../../lib/types';
import { useAuth } from '../../state/auth';

// Signed-in data shown on the route screen: saved routes and ride stats.
// Loaded when the tab is visible, refreshed after saves and logs.

export interface Loadable<T> {
  status: 'idle' | 'loading' | 'ready' | 'error';
  data: T | null;
}

function useLoadable<T>(load: ((token: string) => Promise<T>) | null, enabled: boolean) {
  const { token, status: authStatus, handleAuthError } = useAuth();
  const [state, setState] = useState<Loadable<T>>({ status: 'idle', data: null });
  const [version, setVersion] = useState(0);
  const signedIn = authStatus === 'signed-in' && Boolean(token);

  useEffect(() => {
    if (!signedIn || !token || !enabled || !load) {
      if (!signedIn) setState({ status: 'idle', data: null });
      return undefined;
    }
    let cancelled = false;
    setState((s) => ({ status: 'loading', data: s.data }));
    load(token)
      .then((data) => {
        if (!cancelled) setState({ status: 'ready', data });
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        console.warn('[route] account data failed', err);
        handleAuthError(err);
        setState((s) => ({ status: 'error', data: s.data }));
      });
    return () => {
      cancelled = true;
    };
  }, [signedIn, token, enabled, load, version, handleAuthError]);

  const reload = useCallback(() => setVersion((v) => v + 1), []);
  return { ...state, reload };
}

export function useSavedRoutes(enabled: boolean) {
  return useLoadable<SavedRoute[]>(api.routes, enabled);
}

export function useRideStats(enabled: boolean) {
  return useLoadable<RideStats>(api.rideStats, enabled);
}
