import { useEffect, useState } from 'react';
import { isAbort } from '../../lib/api';
import { searchPlaces, type NearPoint, type PlaceSuggestion } from '../../lib/geo';
import type { Lang } from '../../lib/i18n';

// Debounced search-as-you-type over the place search (lib/geo); every new keystroke
// cancels the previous request.

const DEBOUNCE_MS = 300;

export interface PlaceSearch {
  status: 'idle' | 'loading' | 'ready' | 'error';
  results: PlaceSuggestion[];
}

export function usePlaceSearch(query: string, lang: Lang, enabled: boolean, near?: NearPoint | null): PlaceSearch {
  const [state, setState] = useState<PlaceSearch>({ status: 'idle', results: [] });
  // Kept as two numbers: a fresh { lat, lon } object on every render would
  // restart the search on every render.
  const nearLat = near?.lat ?? null;
  const nearLon = near?.lon ?? null;

  useEffect(() => {
    const text = query.trim();
    if (!enabled || text.split(',')[0].trim().length < 2) {
      setState({ status: 'idle', results: [] });
      return undefined;
    }
    const controller = new AbortController();
    setState((s) => ({ status: 'loading', results: s.results }));
    const timer = window.setTimeout(() => {
      searchPlaces(text, lang, controller.signal, nearLat !== null && nearLon !== null ? { lat: nearLat, lon: nearLon } : null)
        .then((results) => {
          if (!controller.signal.aborted) setState({ status: 'ready', results });
        })
        .catch((err: unknown) => {
          if (isAbort(err) || controller.signal.aborted) return;
          console.warn('[route] place search failed', err);
          setState({ status: 'error', results: [] });
        });
    }, DEBOUNCE_MS);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  }, [query, lang, enabled, nearLat, nearLon]);

  return state;
}
