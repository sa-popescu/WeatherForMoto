import { finishClock, NEW_CLOCK, pauseClock, resumeClock, type RideClock } from './logic/clock';
import { msToKmh } from './logic/rideFormat';
import { EMPTY_TOTALS, EMPTY_TRACK, foldPoints, liveSpeedMs, MAX_ACCURACY_M, stepTrack, type RideFix, type RidePoint, type RideTotals, type TrackState } from './logic/track';
import { findUnfinished, saveProgress, type RideMeta, type RideStatus } from './store';

// The ride recorder: one per app and outside React, so the GPS, the screen
// lock and the saving go on whatever the screens do. React reads it through
// useRide (useSyncExternalStore); the map reads the points with getPoints().
//
// A web app only gets positions while it is open with the screen on. When the
// phone hides it, the next position after coming back is far away in time:
// the track counts that as a gap (see logic/track.ts) instead of a jump.

export type RidePhase = 'idle' | 'resume' | 'recording' | 'paused' | 'finished';
export type GpsState = 'waiting' | 'ok' | 'weak' | 'denied' | 'unavailable' | 'unsupported';
export type WakeState = 'on' | 'off' | 'unsupported';

export interface RideSnapshot {
  phase: RidePhase;
  gps: GpsState;
  accuracyM: number | null;
  /** Speed of the latest fix, in km/h. */
  speedKmh: number | null;
  lastFixAt: number | null;
  position: { lat: number; lon: number } | null;
  totals: RideTotals;
  clock: RideClock;
  /** Grows with every kept point, so the map knows when to draw. */
  pointCount: number;
  wake: WakeState;
  saveFailed: boolean;
}

const IDLE: RideSnapshot = {
  phase: 'idle',
  gps: 'waiting',
  accuracyM: null,
  speedKmh: null,
  lastFixAt: null,
  position: null,
  totals: EMPTY_TOTALS,
  clock: NEW_CLOCK,
  pointCount: 0,
  wake: 'off',
  saveFailed: false,
};

/** New points go to the phone's storage this often. */
const FLUSH_MS = 5_000;
/** Picking a ride up after longer than this counts the time away as a pause. */
const AWAY_AS_PAUSE_MS = 10 * 60_000;
/** A GPS timestamp further than this from the device clock is not trusted. */
const CLOCK_SKEW_MS = 10_000;
const GEO_OPTIONS: PositionOptions = { enableHighAccuracy: true, maximumAge: 0, timeout: 20_000 };

class RideRecorder {
  private listeners = new Set<() => void>();
  private snapshot: RideSnapshot = IDLE;
  private meta: RideMeta | null = null;
  private track: TrackState = EMPTY_TRACK;
  private points: RidePoint[] = [];
  private unsaved: RidePoint[] = [];
  private nextSeq = 0;
  private lastFix: RideFix | null = null;
  private watchId: number | null = null;
  private wakeLock: WakeLockSentinel | null = null;
  private flushTimer: number | null = null;
  private writing: Promise<void> = Promise.resolve();
  private restoreStarted = false;

  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  readonly getSnapshot = (): RideSnapshot => this.snapshot;

  getPoints(): readonly RidePoint[] {
    return this.points;
  }

  /** Starts a new ride. The clock starts with the first usable GPS fix. */
  start(): void {
    if (this.snapshot.phase !== 'idle') return;
    const now = Date.now();
    this.meta = { id: `ride-${now.toString(36)}`, createdAt: now, status: 'recording', clock: NEW_CLOCK, seg: 0 };
    this.track = EMPTY_TRACK;
    this.points = [];
    this.unsaved = [];
    this.nextSeq = 0;
    this.lastFix = null;
    this.publish({ ...IDLE, phase: 'recording' });
    this.goLive();
    this.flush();
  }

  pause(): void {
    if (this.snapshot.phase !== 'recording' || !this.meta) return;
    this.setMeta('paused', pauseClock(this.meta.clock, Date.now()));
    this.publish({ phase: 'paused', clock: this.meta.clock });
    this.flush();
  }

  resume(): void {
    if (this.snapshot.phase !== 'paused' || !this.meta) return;
    // A new segment: nothing is counted between the pause and now.
    this.meta = { ...this.meta, seg: this.meta.seg + 1 };
    this.setMeta('recording', resumeClock(this.meta.clock, Date.now()));
    this.publish({ phase: 'recording', clock: this.meta.clock });
    this.flush();
  }

  stop(): void {
    const { phase } = this.snapshot;
    if ((phase !== 'recording' && phase !== 'paused') || !this.meta) return;
    this.setMeta('finished', finishClock(this.meta.clock, Date.now()));
    this.goQuiet();
    this.publish({ phase: 'finished', clock: this.meta.clock, speedKmh: null });
    this.flush();
  }

  /** Leaves the summary of a finished ride. The ride stays stored on the phone. */
  close(): void {
    if (this.snapshot.phase !== 'finished') return;
    this.meta = null;
    this.track = EMPTY_TRACK;
    this.points = [];
    this.unsaved = [];
    this.lastFix = null;
    this.publish(IDLE);
  }

  /** At start-up: offers to pick up a ride the app was closed in the middle of. */
  async restoreUnfinished(): Promise<void> {
    if (this.restoreStarted) return;
    this.restoreStarted = true;
    try {
      const found = await findUnfinished();
      if (!found || this.snapshot.phase !== 'idle') return;
      if (found.points.length === 0) {
        // It never got a GPS fix: nothing to pick up, so it is simply closed.
        await saveProgress({ ...found.meta, status: 'finished', clock: finishClock(found.meta.clock, found.meta.createdAt) }, [], found.nextSeq);
        return;
      }
      this.meta = found.meta;
      this.points = found.points;
      this.track = foldPoints(found.points);
      this.nextSeq = found.nextSeq;
      const last = found.points[found.points.length - 1];
      this.publish({
        ...IDLE,
        phase: 'resume',
        totals: this.track.totals,
        clock: found.meta.clock,
        pointCount: found.points.length,
        position: { lat: last.lat, lon: last.lon },
        lastFixAt: last.t,
      });
    } catch (err) {
      console.warn('[ride] could not look for an unfinished ride', err);
    }
  }

  /** Picks the unfinished ride up again. */
  continueUnfinished(): void {
    if (this.snapshot.phase !== 'resume' || !this.meta) return;
    const now = Date.now();
    const lastT = this.track.last?.t ?? this.meta.createdAt;
    if (this.meta.status === 'paused') {
      // Still paused: the time away keeps counting as the pause it was.
      this.publish({ phase: 'paused' });
    } else if (now - lastT > AWAY_AS_PAUSE_MS) {
      // Long away: count it as a pause from the last point, and start a new segment.
      this.meta = { ...this.meta, seg: this.meta.seg + 1 };
      this.setMeta('recording', resumeClock(pauseClock(this.meta.clock, lastT), now));
      this.publish({ phase: 'recording', clock: this.meta.clock });
    } else {
      this.publish({ phase: 'recording' });
    }
    this.goLive();
    this.flush();
  }

  /** Ends the unfinished ride where it stopped, and shows its summary. */
  finishUnfinished(): void {
    if (this.snapshot.phase !== 'resume' || !this.meta) return;
    const at = this.meta.clock.pausedAt ?? this.track.last?.t ?? Date.now();
    this.setMeta('finished', finishClock(this.meta.clock, at));
    this.publish({ phase: 'finished', clock: this.meta.clock });
    this.flush();
  }

  private setMeta(status: RideStatus, clock: RideClock): void {
    if (this.meta) this.meta = { ...this.meta, status, clock };
  }

  private publish(patch: Partial<RideSnapshot>): void {
    this.snapshot = { ...this.snapshot, ...patch };
    for (const listener of this.listeners) listener();
  }

  private isLive(): boolean {
    return this.snapshot.phase === 'recording' || this.snapshot.phase === 'paused';
  }

  private goLive(): void {
    this.startWatch();
    void this.lockScreen();
    if (this.flushTimer === null) {
      this.flushTimer = window.setInterval(() => {
        if (this.unsaved.length > 0) this.flush();
      }, FLUSH_MS);
    }
    document.addEventListener('visibilitychange', this.onVisibility);
    window.addEventListener('pagehide', this.onPageHide);
  }

  private goQuiet(): void {
    this.stopWatch();
    this.releaseScreen();
    if (this.flushTimer !== null) window.clearInterval(this.flushTimer);
    this.flushTimer = null;
    document.removeEventListener('visibilitychange', this.onVisibility);
    window.removeEventListener('pagehide', this.onPageHide);
  }

  // ---- GPS ----

  private startWatch(): void {
    if (!('geolocation' in navigator)) {
      this.publish({ gps: 'unsupported' });
      return;
    }
    if (this.watchId !== null) return;
    this.watchId = navigator.geolocation.watchPosition(this.onPosition, this.onGeoError, GEO_OPTIONS);
  }

  private stopWatch(): void {
    if (this.watchId !== null) navigator.geolocation.clearWatch(this.watchId);
    this.watchId = null;
  }

  private readonly onPosition = (pos: GeolocationPosition): void => {
    const now = Date.now();
    const t = Math.abs(pos.timestamp - now) <= CLOCK_SKEW_MS ? pos.timestamp : now;
    const { latitude: lat, longitude: lon, accuracy, speed, altitude } = pos.coords;
    const fix: RideFix = { t, lat, lon, acc: accuracy, speed, alt: altitude };
    const speedMs = liveSpeedMs(this.lastFix, fix);
    this.lastFix = fix;
    const patch: Partial<RideSnapshot> = {
      gps: accuracy > MAX_ACCURACY_M ? 'weak' : 'ok',
      accuracyM: accuracy,
      speedKmh: speedMs === null ? null : msToKmh(speedMs),
      lastFixAt: t,
      position: { lat, lon },
    };
    if (this.snapshot.phase === 'recording' && this.meta) {
      const step = stepTrack(this.track, fix, this.meta.seg);
      this.track = step.state;
      if (step.kept) {
        this.points.push(step.kept);
        this.unsaved.push(step.kept);
        if (this.meta.clock.startedAt === null) {
          this.meta = { ...this.meta, clock: { ...this.meta.clock, startedAt: step.kept.t } };
          patch.clock = this.meta.clock;
        }
        patch.totals = this.track.totals;
        patch.pointCount = this.points.length;
      }
    }
    this.publish(patch);
  };

  private readonly onGeoError = (err: GeolocationPositionError): void => {
    if (err.code === err.PERMISSION_DENIED) {
      this.stopWatch();
      this.publish({ gps: 'denied', speedKmh: null });
      return;
    }
    // A timeout or a lost signal: the watch keeps trying on its own.
    this.publish({ gps: err.code === err.TIMEOUT ? 'waiting' : 'unavailable', speedKmh: null });
  };

  // ---- Screen ----

  private async lockScreen(): Promise<void> {
    if (!('wakeLock' in navigator)) {
      this.publish({ wake: 'unsupported' });
      return;
    }
    if (this.wakeLock && !this.wakeLock.released) return;
    try {
      const lock = await navigator.wakeLock.request('screen');
      if (!this.isLive()) {
        void lock.release();
        return;
      }
      this.wakeLock = lock;
      lock.addEventListener('release', () => {
        if (this.wakeLock !== lock) return;
        this.wakeLock = null;
        this.publish({ wake: 'off' });
      });
      this.publish({ wake: 'on' });
    } catch (err) {
      console.warn('[ride] the screen may turn off: the wake lock was refused', err);
      this.publish({ wake: 'off' });
    }
  }

  private releaseScreen(): void {
    const lock = this.wakeLock;
    this.wakeLock = null;
    lock?.release().catch((err: unknown) => console.warn('[ride] could not release the wake lock', err));
    this.publish({ wake: 'off' });
  }

  private readonly onVisibility = (): void => {
    if (document.visibilityState === 'hidden') {
      this.flush();
      return;
    }
    if (!this.isLive()) return;
    // Back on screen: browsers drop the wake lock while hidden, and a phone may
    // have stopped the GPS, so both are asked for again.
    void this.lockScreen();
    if (this.snapshot.gps !== 'denied') {
      this.stopWatch();
      this.startWatch();
    }
  };

  private readonly onPageHide = (): void => this.flush();

  // ---- Saving ----

  /** Queues a write of the metadata and the points not saved yet. Writes run one at a time. */
  private flush(): void {
    const meta = this.meta;
    if (!meta) return;
    const batch = this.unsaved;
    this.unsaved = [];
    const seq = batch.length > 0 ? this.nextSeq++ : this.nextSeq;
    this.writing = this.writing.then(async () => {
      try {
        await saveProgress(meta, batch, seq);
        if (this.snapshot.saveFailed) this.publish({ saveFailed: false });
      } catch (err) {
        console.warn('[ride] could not save the ride on this phone', err);
        // Kept for the next write; loading puts the points back in time order.
        if (this.meta?.id === meta.id) this.unsaved = [...batch, ...this.unsaved];
        this.publish({ saveFailed: true });
      }
    });
  }
}

export const rideRecorder = new RideRecorder();
