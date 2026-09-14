import { useEffect, useState } from 'react';
import { isAbort } from '../../lib/api';
import { searchPlaces, type PlaceSuggestion } from '../../lib/geo';
import type { Lang } from '../../lib/i18n';

// Search-as-you-type: debounced, at least two letters, and every new keystroke
// aborts the previous request so late answers never overwrite newer ones.
// Results near `near` (the place on screen) rank first.

const DEBOUNCE_MS = 350;
export const MIN_QUERY_CHARS = 2;

export interface PlaceSearchState {
  status: 'idle' | 'loading' | 'done' | 'error';
  results: PlaceSuggestion[];
}

const IDLE: PlaceSearchState = { status: 'idle', results: [] };

export function usePlaceSearch(query: string, lang: Lang, near?: { lat: number; lon: number }): PlaceSearchState {
  const [state, setState] = useState<PlaceSearchState>(IDLE);
  const nearLat = near?.lat;
  const nearLon = near?.lon;

  useEffect(() => {
    const trimmed = query.trim();
    if ((trimmed.split(',')[0] ?? '').trim().length < MIN_QUERY_CHARS) {
      setState(IDLE);
      return undefined;
    }
    const controller = new AbortController();
    const bias = nearLat === undefined || nearLon === undefined ? undefined : { lat: nearLat, lon: nearLon };
    setState((prev) => ({ status: 'loading', results: prev.results }));
    const timer = window.setTimeout(() => {
      searchPlaces(trimmed, lang, controller.signal, bias)
        .then((results) => setState({ status: 'done', results }))
        .catch((err: unknown) => {
          if (isAbort(err) || controller.signal.aborted) return;
          console.warn('[place] search failed', err);
          setState({ status: 'error', results: [] });
        });
    }, DEBOUNCE_MS);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [query, lang, nearLat, nearLon]);

  return state;
}
