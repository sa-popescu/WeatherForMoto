import { KEYS, readJson, writeJson } from '../../lib/storage';
import { noteUse } from '../../lib/usage';
import { LIVE_PATH, liveUrl, SEND_EVERY_MS } from '../live/constants';
import { averageKmh } from './logic/clock';
import { arrivalAt } from './logic/follow';
import { rideRecorder, type RidePhase, type RideSnapshot } from './recorder';

// Sharing the ride's position by link while it is recorded. The position goes
// to the Worker on our own domain (worker/live.ts), never to the app's
// database, about every SEND_EVERY_MS. The share ends with the ride, by
// hand, or by itself after 12 hours. Its id and key are kept on the phone,
// so a ride picked up after the app was closed goes on being shared.

/** A big move is sent sooner than every SEND_EVERY_MS, but never under this. */
const SEND_SOONER_MS = 26_000;
const SOONER_AFTER_KM = 0.5;

export type LiveError = 'budget' | 'off' | 'network';

export interface LiveShareState {
  status: 'off' | 'starting' | 'on';
  /** The link to send, while sharing. */
  url: string | null;
  lastSentAt: number | null;
  error: LiveError | null;
}

interface Session {
  id: string;
  key: string;
  expiresAt: number;
}

function isSession(value: unknown): value is Session {
  const v = value as Session | null;
  return typeof v === 'object' && v !== null && typeof v.id === 'string' && typeof v.key === 'string' && typeof v.expiresAt === 'number';
}

function kmBetween(a: { lat: number; lon: number }, b: { lat: number; lon: number }): number {
  const kx = 111.32 * Math.cos((a.lat * Math.PI) / 180);
  return Math.hypot((b.lat - a.lat) * 111.32, (b.lon - a.lon) * kx);
}

/** What the viewers see of the ride: the position, the speed and, on a route, where to and when. */
export function updateBody(ride: RideSnapshot, now: number): Record<string, unknown> | null {
  if (!ride.position) return null;
  const body: Record<string, unknown> = { lat: ride.position.lat, lon: ride.position.lon, speedKmh: ride.speedKmh };
  if (ride.route) {
    body.dest = ride.route.name;
    if (ride.follow?.km != null) {
      const { movingDistanceM, movingMs } = ride.totals;
      body.etaMs = arrivalAt(ride.route.distanceKm - ride.follow.km, averageKmh(movingDistanceM, movingMs), movingMs, ride.route.plannedKmh, now);
    }
  }
  return body;
}

class LiveShare {
  private state: LiveShareState = { status: 'off', url: null, lastSentAt: null, error: null };
  private session: Session | null = null;
  private lastSent: { at: number; lat: number; lon: number } | null = null;
  private sending = false;
  private prevPhase: RidePhase = 'idle';
  private detach: (() => void) | null = null;
  private readonly listeners = new Set<() => void>();

  constructor() {
    const saved = readJson<Session | null>(KEYS.liveShare, null, (v): v is Session | null => v === null || isSession(v));
    if (saved && saved.expiresAt > Date.now()) this.resume(saved);
    else if (saved) writeJson(KEYS.liveShare, null);
  }

  readonly subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  readonly getSnapshot = (): LiveShareState => this.state;

  private publish(patch: Partial<LiveShareState>): void {
    this.state = { ...this.state, ...patch };
    for (const listener of this.listeners) listener();
  }

  /** Start sharing; the link, or null when it could not start (the reason is in the state). */
  async start(): Promise<string | null> {
    if (this.session) return this.state.url;
    this.publish({ status: 'starting', error: null });
    try {
      const response = await fetch(LIVE_PATH, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: '{}' });
      if (!response.ok) {
        const error = response.status === 429 ? 'budget' : response.status === 503 || response.status === 404 ? 'off' : 'network';
        this.publish({ status: 'off', error });
        return null;
      }
      const session = (await response.json()) as unknown;
      if (!isSession(session)) throw new Error('live: unexpected answer');
      writeJson(KEYS.liveShare, session);
      this.resume(session);
      noteUse('ride:live');
      return this.state.url;
    } catch (err) {
      console.warn('[live] could not start sharing', err);
      this.publish({ status: 'off', error: 'network' });
      return null;
    }
  }

  /** Stop sharing: the Worker deletes the position and the trail. */
  async stop(): Promise<void> {
    const session = this.session;
    this.end();
    if (!session) return;
    try {
      await fetch(`${LIVE_PATH}/${session.id}`, { method: 'DELETE', headers: { Authorization: `Bearer ${session.key}` }, keepalive: true });
    } catch (err) {
      // The share still ends by itself within 12 hours.
      console.warn('[live] could not end the share, it expires by itself', err);
    }
  }

  private resume(session: Session): void {
    this.session = session;
    this.lastSent = null;
    this.publish({ status: 'on', url: liveUrl(session.id), lastSentAt: null, error: null });
    this.prevPhase = rideRecorder.getSnapshot().phase;
    this.detach ??= rideRecorder.subscribe(this.onRide);
    this.onRide();
  }

  private end(): void {
    this.session = null;
    this.lastSent = null;
    this.detach?.();
    this.detach = null;
    writeJson(KEYS.liveShare, null);
    this.publish({ status: 'off', url: null, lastSentAt: null });
  }

  private readonly onRide = (): void => {
    const session = this.session;
    if (!session) return;
    const ride = rideRecorder.getSnapshot();
    const was = this.prevPhase;
    this.prevPhase = ride.phase;
    const now = Date.now();
    if (now >= session.expiresAt) {
      this.end();
      return;
    }
    // The ride ended (finished, or closed): so does the share.
    if ((ride.phase === 'finished' && was !== 'finished') || (ride.phase === 'idle' && was !== 'idle')) {
      void this.stop();
      return;
    }
    if (ride.phase !== 'recording' || !ride.position || this.sending) return;
    const since = this.lastSent ? now - this.lastSent.at : Infinity;
    const moved = this.lastSent ? kmBetween(this.lastSent, ride.position) : Infinity;
    if (since < SEND_EVERY_MS && !(moved >= SOONER_AFTER_KM && since >= SEND_SOONER_MS)) return;
    const body = updateBody(ride, now);
    if (body) void this.send(session, body, now, ride.position);
  };

  private async send(session: Session, body: Record<string, unknown>, now: number, at: { lat: number; lon: number }): Promise<void> {
    this.sending = true;
    // Counted as sent before the answer, so a slow network does not stack requests.
    this.lastSent = { at: now, ...at };
    try {
      const response = await fetch(`${LIVE_PATH}/${session.id}`, {
        method: 'PUT',
        headers: { Authorization: `Bearer ${session.key}`, 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      if (response.status === 404 || response.status === 403) {
        // Ended elsewhere or expired: nothing left to send to.
        this.end();
        return;
      }
      if (response.ok) this.publish({ lastSentAt: now });
    } catch (err) {
      // No signal: the next fix tries again.
      console.warn('[live] position not sent', err);
    } finally {
      this.sending = false;
    }
  }
}

export const liveShare = new LiveShare();
