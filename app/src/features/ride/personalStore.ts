import { useSyncExternalStore } from 'react';
import type { CurrentWeather } from '../../lib/types';
import { learnTolerance, personalScore, type Learned } from './logic/personal';
import { rideRecorder } from './recorder';
import { listFinished } from './store';

// What "Scorul tău" has learned, for any screen: worked out from the rides on
// this phone when first asked for, and again whenever a ride screen closes
// (a new ride, a new story). Outside React, like the recorder.

const NOTHING: Learned = { rides: 0, tolerance: null };

class PersonalStore {
  private learned: Learned = NOTHING;
  private listeners = new Set<() => void>();
  private started = false;
  private lastPhase = rideRecorder.getSnapshot().phase;

  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    this.start();
    return () => this.listeners.delete(listener);
  };

  readonly getSnapshot = (): Learned => this.learned;

  private start(): void {
    if (this.started) return;
    this.started = true;
    this.reload();
    rideRecorder.subscribe(() => {
      const phase = rideRecorder.getSnapshot().phase;
      if (phase === 'idle' && this.lastPhase !== 'idle') this.reload();
      this.lastPhase = phase;
    });
  }

  private reload(): void {
    listFinished()
      .then((items) => {
        this.learned = learnTolerance(items.map((i) => i.story));
        for (const listener of this.listeners) listener();
      })
      .catch((err: unknown) => console.warn('[ride] could not read the rides for the personal score', err));
  }
}

export const personalStore = new PersonalStore();

export function useLearned(): Learned {
  return useSyncExternalStore(personalStore.subscribe, personalStore.getSnapshot);
}

/** The score now for this rider, or null while it is not personal yet (or says the same). */
export function usePersonalScore(current: CurrentWeather | null | undefined): number | null {
  const { tolerance } = useLearned();
  if (!tolerance || !current) return null;
  const mine = personalScore(current, tolerance);
  return mine !== null && mine !== current.moto_score ? mine : null;
}
