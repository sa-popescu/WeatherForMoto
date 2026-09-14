import { useEffect, useState } from 'react';
import { geometryKey, type FieldGeometry } from './mercator';
import { cloudPicture } from './pictures';

// Keeps the satellite cloud pictures the band is about to show loaded: the one
// on screen and the next. Returns a counter that moves when one arrives, so the
// layer knows to look again.

export function useCloudPictures(geometry: FieldGeometry | null, times: ReadonlyArray<number | null>): number {
  const [version, setVersion] = useState(0);
  const wanted = times.filter((t): t is number => t !== null);
  const key = geometry ? `${geometryKey(geometry)}|${[...new Set(wanted)].join(',')}` : '';

  useEffect(() => {
    if (!geometry || wanted.length === 0) return undefined;
    const controller = new AbortController();
    for (const time of new Set(wanted)) {
      cloudPicture(geometry, time, controller.signal)
        .then(() => {
          if (!controller.signal.aborted) setVersion((v) => v + 1);
        })
        .catch((err: unknown) => {
          if (!controller.signal.aborted) console.warn('[map] satellite cloud picture failed', err);
        });
    }
    return () => controller.abort();
    // The key names the geometry and the times.
  }, [key]);

  return version;
}
