import { useSyncExternalStore } from 'react';
import { rideRecorder, type RideSnapshot } from './recorder';

/** The ride recorder's state, re-rendering on every change (about once a second while riding). */
export function useRide(): RideSnapshot {
  return useSyncExternalStore(rideRecorder.subscribe, rideRecorder.getSnapshot);
}
