import { useEffect, useRef, useState } from 'react';
import { geometryKey, type FieldGeometry } from './mercator';
import type { RadarFrame } from './radar';
import { cachedRadarValues, radarValues } from './radarValues';

// Keeps the observed radar frames of the band ready as rain values. The frame
// on screen and the next are read first; a move of the cursor does not cancel
// them. The rest of the band follows one frame at a time, newest first, so
// playing or scrubbing back finds them ready. Returns a counter that moves
// whenever a frame arrives, so the map knows to look again.

export interface RadarValuesState {
  /** Moves whenever a frame arrives. */
  version: number;
  /** The frame on screen is still being read. */
  loading: boolean;
  /** The frame on screen could not be read (its tiles keep failing). */
  failed: boolean;
}

export function useRadarValues(
  host: string | null,
  frames: readonly RadarFrame[],
  geometry: FieldGeometry | null,
  onScreen: readonly (RadarFrame | null)[],
): RadarValuesState {
  const [version, setVersion] = useState(0);
  const [failedPath, setFailedPath] = useState<string | null>(null);
  const mounted = useRef(true);
  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
    };
  }, []);

  const wanted = onScreen.filter((frame): frame is RadarFrame => frame !== null);
  const gkey = geometry ? geometryKey(geometry) : '';
  const screenKey = host && geometry ? `${gkey}|${wanted.map((f) => f.path).join(',')}` : '';

  useEffect(() => {
    if (!host || !geometry) return;
    for (const frame of wanted) {
      radarValues(host, frame, geometry)
        .then(() => {
          if (mounted.current) setVersion((v) => v + 1);
        })
        .catch((err: unknown) => {
          console.warn('[map] radar frame could not be read', err);
          if (mounted.current) setFailedPath(`${gkey}|${frame.path}`);
        });
    }
    // The key names the host, the canvas and the frames.
  }, [screenKey]);

  // Newest first: the band opens on the present and is scrubbed back from there.
  const band = [...frames].sort((a, b) => b.time - a.time);
  const bandKey = host && geometry ? `${gkey}|${band.map((f) => f.path).join(',')}` : '';

  useEffect(() => {
    if (!host || !geometry || band.length === 0) return undefined;
    // A new canvas or a new band stops what was being read for the old one.
    let stopped = false;
    void (async () => {
      for (const frame of band) {
        if (stopped) return;
        try {
          await radarValues(host, frame, geometry);
          if (mounted.current && !stopped) setVersion((v) => v + 1);
        } catch (err) {
          if (stopped) return;
          console.warn('[map] radar frame could not be read ahead', err);
        }
      }
    })();
    return () => {
      stopped = true;
    };
    // The key names the host, the canvas and the band's frames.
  }, [bandKey]);

  const current = wanted[0] ?? null;
  const ready = current !== null && geometry !== null && cachedRadarValues(geometry, current.path) !== undefined;
  const failed = current !== null && failedPath === `${gkey}|${current.path}`;
  return { version, loading: current !== null && !ready && !failed, failed };
}
