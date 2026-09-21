import { useEffect, useMemo, useState } from 'react';
import { RAIN_MAX_SPEED_KMH } from '../map/nowcast';
import { radarPicture } from '../map/pictures';
import { useRadarFrames } from '../map/useRadarFrames';
import { measureMotion } from '../map/useFieldNowcast';
import { pointGeometry, rainAlongTrail, summarizeRadar, type RadarRain, type RadarStep } from './logic/radarRain';

// Radar rain over the place for the next ~90 minutes. Loads the last observed
// radar pictures around the point when the screen is on and RainViewer has a
// newer frame (every 10 minutes), measures their motion in the map's worker,
// and follows it back from the point. Without radar (offline, blocked tiles)
// the screen simply goes on with the forecast.

/** Pictures used to measure motion: the newest three give two intervals. */
const PICTURES = 3;

interface Place {
  lat: number;
  lon: number;
}

export function useRadarRain(place: Place | null, active: boolean, nowMs: number): RadarRain | null {
  const frames = useRadarFrames(active && place !== null);
  const [steps, setSteps] = useState<{ key: string; steps: RadarStep[] } | null>(null);

  const past = useMemo(() => (frames.data ? frames.data.frames.filter((f) => !f.nowcast).slice(-PICTURES) : []), [frames.data]);
  const key = place && past.length >= 2 ? `${place.lat.toFixed(3)},${place.lon.toFixed(3)}|${past[past.length - 1].time}` : null;

  useEffect(() => {
    if (!key || !place || !frames.data) return undefined;
    const host = frames.data.host;
    const geometry = pointGeometry(place.lat, place.lon);
    const controller = new AbortController();
    void (async () => {
      try {
        const pictures = await Promise.all(past.map((frame) => radarPicture(host, frame, geometry, controller.signal)));
        const motion = await measureMotion('radar', pictures, geometry, RAIN_MAX_SPEED_KMH, controller.signal);
        if (controller.signal.aborted) return;
        const base = pictures[pictures.length - 1];
        setSteps({ key, steps: rainAlongTrail(base.rgba, geometry, motion, place.lat, place.lon, base.timeSec) });
      } catch (err) {
        if (controller.signal.aborted) return;
        console.warn('[now] radar rain unavailable', err);
        setSteps(null);
      }
    })();
    return () => controller.abort();
    // `key` stands for the place and the newest frame, so it alone decides a reload.
  }, [key]);

  // Minutes are counted from the clock, so an arrival keeps counting down between radar frames.
  return useMemo(() => (steps && steps.key === key ? summarizeRadar(steps.steps, Math.floor(nowMs / 1000)) : null), [steps, key, nowMs]);
}
