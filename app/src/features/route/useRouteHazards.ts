import { useCallback, useEffect, useState } from 'react';
import { api, isAbort } from '../../lib/api';
import type { Hazard } from '../../lib/types';
import { HAZARD_RADIUS_KM, placeHazards, type RouteHazard } from './hazards';
import { runPool } from './pool';
import type { RouteLine, SamplePoint } from './types';

// Rider-reported hazards near the route: one small query around each sample
// point (25 km), merged, deduped and ordered along the road.

const HAZARD_CONCURRENCY = 2;

export interface HazardsState {
  status: 'idle' | 'loading' | 'ready' | 'error';
  list: RouteHazard[];
}

export function useRouteHazards(route: RouteLine | null, points: readonly SamplePoint[]) {
  const [state, setState] = useState<HazardsState>({ status: 'idle', list: [] });
  const [version, setVersion] = useState(0);

  useEffect(() => {
    if (!route || points.length === 0) {
      setState({ status: 'idle', list: [] });
      return undefined;
    }
    const controller = new AbortController();
    const found: Hazard[] = [];
    let failures = 0;
    setState((s) => ({ status: 'loading', list: s.list }));
    void runPool(
      points,
      HAZARD_CONCURRENCY,
      async (p) => {
        try {
          found.push(...(await api.hazards(p.lat, p.lon, HAZARD_RADIUS_KM, controller.signal)));
        } catch (err) {
          if (isAbort(err) || controller.signal.aborted) return;
          failures += 1;
          console.warn('[route] hazards failed near', p.name, err);
        }
      },
      controller.signal,
    ).then(() => {
      if (controller.signal.aborted) return;
      // Partial answers are still useful; only a total failure is an error.
      if (failures === points.length) setState({ status: 'error', list: [] });
      else setState({ status: 'ready', list: placeHazards(found, route) });
    });
    return () => controller.abort();
  }, [route, points, version]);

  const reload = useCallback(() => setVersion((v) => v + 1), []);
  return { ...state, reload };
}
