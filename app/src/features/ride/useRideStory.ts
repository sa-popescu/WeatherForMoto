import { useCallback, useEffect, useState } from 'react';
import { isAbort } from '../../lib/api';
import { buildStory, highestAltitude, storySamples, type RideStory } from './logic/story';
import { rideRecorder, type RideSnapshot } from './recorder';
import { fetchStoryWeather } from './storyWeather';

// The story of the finished ride on screen: the one kept with it, or worked
// out now (one request to Open-Meteo) and then kept with it, so it is asked
// for once and shows without a connection afterwards.

export type StoryStatus = 'none' | 'loading' | 'ready' | 'offline' | 'error';

export interface StoryState {
  status: StoryStatus;
  story: RideStory | null;
  retry: () => void;
}

export function useRideStory(ride: RideSnapshot): StoryState {
  const [status, setStatus] = useState<StoryStatus>('none');
  const [attempt, setAttempt] = useState(0);
  const { phase, rideId, story } = ride;

  useEffect(() => {
    if (phase !== 'finished' || !rideId) return undefined;
    if (story) {
      setStatus('ready');
      return undefined;
    }
    const points = rideRecorder.getPoints();
    if (points.length < 2) {
      setStatus('none');
      return undefined;
    }
    if (!navigator.onLine) {
      setStatus('offline');
      return undefined;
    }
    const controller = new AbortController();
    const samples = storySamples(points);
    setStatus('loading');
    fetchStoryWeather(samples, controller.signal)
      .then((weather) => {
        if (controller.signal.aborted) return;
        const found = buildStory(samples, weather, highestAltitude(points));
        if (found.samples === 0) {
          setStatus('error');
          return;
        }
        // Kept with the ride; the new snapshot brings it back here as `story`.
        rideRecorder.keepStory(rideId, found);
      })
      .catch((err: unknown) => {
        if (isAbort(err) || controller.signal.aborted) return;
        console.warn('[ride] the ride’s weather could not be found', err);
        setStatus('error');
      });
    return () => controller.abort();
  }, [phase, rideId, story, attempt]);

  const retry = useCallback(() => setAttempt((a) => a + 1), []);
  return { status, story, retry };
}
