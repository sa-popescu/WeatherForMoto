import { useCallback, useEffect, useMemo, useState } from 'react';
import { coverageUrl, ICON_EU_FETCH_TIMEOUT_MS, iconEuCovers, parseCoverageText, type RainGrid } from './iconEu';
import { geometryKey, type FieldGeometry } from './mercator';

// Loads the hourly ICON-EU totals the band needs for the canvas, earliest
// first, a few at a time. Hours already loaded for the same canvas are kept
// (also across tab switches), so moving the band along only asks for new hours.

const CONCURRENCY = 3;
const CACHE_LIMIT = 90;

export type ModelStatus = 'idle' | 'unsupported' | 'loading' | 'ready' | 'error';

export interface IconEuRain {
  status: ModelStatus;
  /** Loaded totals by the end of their hour (unix seconds). */
  grids: ReadonlyMap<number, RainGrid>;
  reload: () => void;
}

const cache = new Map<string, RainGrid>();

function remember(key: string, grid: RainGrid): void {
  cache.delete(key);
  cache.set(key, grid);
  while (cache.size > CACHE_LIMIT) {
    const oldest = cache.keys().next().value;
    if (oldest === undefined) break;
    cache.delete(oldest);
  }
}

async function fetchGrid(geometry: FieldGeometry, hourEnd: number, signal: AbortSignal): Promise<RainGrid> {
  const timeout = new AbortController();
  const onAbort = (): void => timeout.abort();
  signal.addEventListener('abort', onAbort, { once: true });
  const timer = window.setTimeout(() => timeout.abort(), ICON_EU_FETCH_TIMEOUT_MS);
  try {
    const res = await fetch(coverageUrl(geometry.bounds, hourEnd), { signal: timeout.signal });
    if (!res.ok) throw new Error(`DWD HTTP ${res.status}`);
    const text = await res.text();
    const grid = parseCoverageText(text);
    // A service exception comes back as XML with status 200; keep its message for the console.
    if (!grid) throw new Error(`DWD answer not readable: ${text.replace(/\s+/g, ' ').slice(0, 160)}`);
    return grid;
  } finally {
    window.clearTimeout(timer);
    signal.removeEventListener('abort', onAbort);
  }
}

interface Options {
  enabled: boolean;
  geometry: FieldGeometry | null;
  hourEnds: readonly number[];
}

export function useIconEuRain({ enabled, geometry, hourEnds }: Options): IconEuRain {
  const [status, setStatus] = useState<ModelStatus>('idle');
  const [version, setVersion] = useState(0);
  const [attempt, setAttempt] = useState(0);
  const gkey = geometry ? geometryKey(geometry) : '';
  const hoursKey = hourEnds.join(',');
  const supported = geometry !== null && iconEuCovers(geometry.bounds);

  useEffect(() => {
    if (!enabled || !geometry) return undefined;
    if (!supported) {
      setStatus('unsupported');
      return undefined;
    }
    const hours = hoursKey ? hoursKey.split(',').map(Number) : [];
    const queue = hours.filter((hour) => !cache.has(`${gkey}|${hour}`));
    if (queue.length === 0) {
      setStatus(hours.length > 0 ? 'ready' : 'idle');
      return undefined;
    }
    const controller = new AbortController();
    setStatus('loading');

    const worker = async (): Promise<void> => {
      for (let hour = queue.shift(); hour !== undefined && !controller.signal.aborted; hour = queue.shift()) {
        try {
          remember(`${gkey}|${hour}`, await fetchGrid(geometry, hour, controller.signal));
          setVersion((v) => v + 1);
        } catch (err) {
          if (controller.signal.aborted) return;
          console.warn('[map] ICON-EU hour failed', new Date(hour * 1000).toISOString(), err);
        }
      }
    };
    void Promise.all(Array.from({ length: CONCURRENCY }, () => worker())).then(() => {
      if (controller.signal.aborted) return;
      setStatus(hours.some((hour) => cache.has(`${gkey}|${hour}`)) ? 'ready' : 'error');
    });
    return () => controller.abort();
    // The geometry is identified by gkey: the object itself only changes with it.
  }, [enabled, supported, gkey, hoursKey, attempt]);

  const grids = useMemo(() => {
    const found = new Map<number, RainGrid>();
    if (!gkey || !hoursKey) return found;
    for (const hour of hoursKey.split(',').map(Number)) {
      const grid = cache.get(`${gkey}|${hour}`);
      if (grid) found.set(hour, grid);
    }
    return found;
    // version marks new arrivals in the shared cache.
  }, [gkey, hoursKey, version]);

  const reload = useCallback(() => setAttempt((n) => n + 1), []);

  return { status: enabled ? status : 'idle', grids, reload };
}
