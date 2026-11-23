import { useEffect, useRef, useState } from 'react';
import { geometryKey, type FieldGeometry } from './mercator';
import { cloudPicture, prefetchCloudImages } from './pictures';

// Keeps the satellite cloud pictures of the band ready. The one on screen and
// the next are turned into pixels; a move of the cursor does not cancel them,
// so a picture asked for while scrubbing still arrives. The rest of the band's
// images are fetched one after the other into the browser's cache, so moving
// to them finds them there. Returns a counter that moves whenever a picture
// arrives, so the layer knows to look again.

/** A signal nothing aborts: the pictures on screen are always worth finishing. */
const NEVER = new AbortController().signal;

export function useCloudPictures(geometry: FieldGeometry | null, onScreen: ReadonlyArray<number | null>, band: readonly number[]): number {
  const [version, setVersion] = useState(0);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const wanted = [...new Set(onScreen.filter((t): t is number => t !== null))];
  const screenKey = geometry ? `${geometryKey(geometry)}|${wanted.join(',')}` : '';

  useEffect(() => {
    if (!geometry) return;
    for (const time of wanted) {
      cloudPicture(geometry, time, NEVER)
        .then(() => {
          if (mounted.current) setVersion((v) => v + 1);
        })
        .catch((err: unknown) => console.warn('[map] satellite cloud picture failed', err));
    }
    // The key names the geometry and the times.
  }, [screenKey]);

  // Newest first: the band opens on the present and is scrubbed back from there.
  const ahead = [...band].sort((a, b) => b - a);
  const bandKey = geometry ? `${geometryKey(geometry)}|${ahead.join(',')}` : '';

  useEffect(() => {
    if (!geometry || ahead.length === 0) return undefined;
    // A new view of the map (other images) stops what was fetched for the old one.
    const controller = new AbortController();
    void (async () => {
      for (const time of ahead) {
        if (controller.signal.aborted) return;
        try {
          await prefetchCloudImages(geometry, time, controller.signal);
        } catch (err) {
          if (controller.signal.aborted) return;
          console.warn('[map] satellite images could not be fetched ahead', err);
        }
      }
    })();
    return () => controller.abort();
    // The key names the geometry and the band's times.
  }, [bandKey]);

  return version;
}
