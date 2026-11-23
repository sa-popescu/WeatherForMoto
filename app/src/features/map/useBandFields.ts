import { useEffect, useRef, useState } from 'react';
import type { ValueField } from './rainValues';

// Keeps the observed fields of the band ready: radar frames, satellite
// clouds, lightning. The one on screen and the next are read first, and a
// move of the cursor does not cancel them. The rest of the band follows one
// at a time, in the order given (newest first), so playing or scrubbing back
// finds them ready. Returns a counter that moves whenever a field arrives, so
// the map knows to look again.

export interface BandFieldsState {
  /** Moves whenever a field arrives. */
  version: number;
  /** The field on screen is still being read. */
  loading: boolean;
  /** The field on screen could not be read. */
  failed: boolean;
}

interface Options {
  /** What the ids are read for (the source and the canvas); null switches it all off. */
  scope: string | null;
  /** Every id of the band, in the order to read them ahead. */
  band: readonly string[];
  /** On screen first, then next; null where there is none. */
  onScreen: readonly (string | null)[];
  read: (id: string) => Promise<ValueField>;
  cached: (id: string) => boolean;
  /** Name for the console when a read fails. */
  label: string;
}

export function useBandFields({ scope, band, onScreen, read, cached, label }: Options): BandFieldsState {
  const [version, setVersion] = useState(0);
  const [failedKey, setFailedKey] = useState<string | null>(null);
  const reader = useRef(read);
  reader.current = read;
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const wanted = onScreen.filter((id): id is string => id !== null);
  const screenKey = scope ? `${scope}|${wanted.join(',')}` : '';

  useEffect(() => {
    if (!scope) return;
    for (const id of wanted) {
      reader.current(id).then(
        () => {
          if (mounted.current) setVersion((v) => v + 1);
        },
        (err: unknown) => {
          console.warn(`[map] ${label} could not be read`, err);
          if (mounted.current) setFailedKey(`${scope}|${id}`);
        },
      );
    }
    // The key names the scope and the ids on screen.
  }, [screenKey]);

  const bandKey = scope ? `${scope}|${band.join(',')}` : '';

  useEffect(() => {
    if (!scope || band.length === 0) return undefined;
    // A new canvas or a new band stops what was being read for the old one.
    let stopped = false;
    void (async () => {
      for (const id of band) {
        if (stopped) return;
        try {
          await reader.current(id);
          if (mounted.current && !stopped) setVersion((v) => v + 1);
        } catch (err) {
          if (stopped) return;
          console.warn(`[map] ${label} could not be read ahead`, err);
        }
      }
    })();
    return () => {
      stopped = true;
    };
    // The key names the scope and the band's ids.
  }, [bandKey]);

  const current = wanted[0] ?? null;
  const ready = current !== null && scope !== null && cached(current);
  const failed = current !== null && failedKey === `${scope}|${current}`;
  return { version, loading: current !== null && !ready && !failed, failed };
}
