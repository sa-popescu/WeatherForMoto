import { NOMINATIM_GAP_MS, reverseGeocode } from '../../lib/geo';
import type { Lang } from '../../lib/i18n';
import { SUMMIT_POINT_ID } from './samples';
import type { SamplePoint } from './types';

// Names for the points between the stops of an imported GPX track. A routed
// road names them after itself ("A1", "DN7"); a track carries no road names,
// so without this every point would read "after <first stop>". Each point is
// named after the nearest locality instead, looked up one by one within
// Nominatim's limit while the weather loads; a lookup that fails leaves the
// point with the name it already has.

/** The points that get a locality name: those between stops, except the summit, which has its own. */
export function pointsToName(points: readonly SamplePoint[]): SamplePoint[] {
  return points.filter((p) => p.stopIndex === null && p.id !== SUMMIT_POINT_ID);
}

const pause = (ms: number, signal: AbortSignal): Promise<void> =>
  new Promise((resolve) => {
    const timer = setTimeout(resolve, ms);
    signal.addEventListener('abort', () => {
      clearTimeout(timer);
      resolve();
    }, { once: true });
  });

/**
 * Looks up the locality of every point to name, in riding order, and hands
 * each one found to `onName`. Stops quietly when `signal` aborts (a newer
 * calculation took over). The first lookup also waits its turn: the ends of
 * the track were looked up just before.
 */
export async function nameTrackPoints(
  points: readonly SamplePoint[],
  lang: Lang,
  signal: AbortSignal,
  onName: (pointId: string, locality: string) => void,
  gapMs: number = NOMINATIM_GAP_MS,
): Promise<void> {
  for (const point of pointsToName(points)) {
    await pause(gapMs, signal);
    if (signal.aborted) return;
    const locality = await reverseGeocode(point.lat, point.lon, lang, signal);
    if (signal.aborted) return;
    if (locality) onName(point.id, locality);
  }
}
