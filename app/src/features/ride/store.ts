import type { GapFill } from './gapFill';
import type { RideClock } from './logic/clock';
import type { RideRoute } from './logic/follow';
import type { RideStory } from './logic/story';
import { summaryFromPoints, type RideSummary } from './logic/summary';
import type { RidePoint } from './logic/track';

// Rides kept on the phone, in IndexedDB, so that a ride survives the app being
// closed or killed mid-way, and finished rides stay to be looked at again. A ride is one record of metadata plus its points,
// written in small chunks every few seconds: appending a chunk costs the same
// at the first kilometre and at the three hundredth, unlike rewriting one
// growing array. Nothing here leaves the device.

const DB_NAME = 'motometeo-rides';
/** 2 added the planned route of a ride. */
const DB_VERSION = 2;
const RIDES = 'rides';
const CHUNKS = 'chunks';
const ROUTES = 'routes';

export type RideStatus = 'recording' | 'paused' | 'finished';

export interface RideMeta {
  id: string;
  createdAt: number;
  status: RideStatus;
  clock: RideClock;
  /** The recorder's current segment (it grows after each pause). */
  seg: number;
  /** Gaps already filled along the road (absent in rides recorded before fills existed). */
  fills?: GapFill[];
  /** The totals, written when the ride ends, so the list of rides needs no points. */
  summary?: RideSummary;
  /** Where the ride was going, when it followed a route. */
  routeName?: string;
  /** The ride's id in the account's ride log, once it was saved there. */
  loggedId?: number;
  /** The weather the ride met, worked out once after it ended. */
  story?: RideStory;
}

/** A finished ride, as the list of rides shows it. */
export interface RideListItem {
  id: string;
  startedAt: number;
  summary: RideSummary;
  routeName: string | null;
  loggedId: number | null;
}

interface RideChunk {
  rideId: string;
  seq: number;
  points: RidePoint[];
}

interface StoredRoute {
  rideId: string;
  route: RideRoute;
}

export interface StoredRide {
  meta: RideMeta;
  points: RidePoint[];
  /** The route the ride followed, if it followed one. */
  route: RideRoute | null;
}

let dbPromise: Promise<IDBDatabase> | null = null;

function openDb(): Promise<IDBDatabase> {
  if (dbPromise) return dbPromise;
  dbPromise = new Promise<IDBDatabase>((resolve, reject) => {
    if (typeof indexedDB === 'undefined') {
      reject(new Error('IndexedDB is not available'));
      return;
    }
    const request = indexedDB.open(DB_NAME, DB_VERSION);
    request.onupgradeneeded = () => {
      const db = request.result;
      if (!db.objectStoreNames.contains(RIDES)) db.createObjectStore(RIDES, { keyPath: 'id' });
      if (!db.objectStoreNames.contains(CHUNKS)) db.createObjectStore(CHUNKS, { keyPath: ['rideId', 'seq'] });
      if (!db.objectStoreNames.contains(ROUTES)) db.createObjectStore(ROUTES, { keyPath: 'rideId' });
    };
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error('Could not open the ride database'));
    request.onblocked = () => reject(new Error('The ride database is blocked by another tab'));
  });
  // A failed open must not stick: the next call tries again.
  dbPromise.catch(() => {
    dbPromise = null;
  });
  return dbPromise;
}

function done(tx: IDBTransaction): Promise<void> {
  return new Promise((resolve, reject) => {
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error ?? new Error('Ride database write failed'));
    tx.onabort = () => reject(tx.error ?? new Error('Ride database write was aborted'));
  });
}

function result<T>(request: IDBRequest<T>): Promise<T> {
  return new Promise((resolve, reject) => {
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error ?? new Error('Ride database read failed'));
  });
}

/** Writes the ride's metadata and, when there are any, its newest points as chunk `seq`. */
export async function saveProgress(meta: RideMeta, points: readonly RidePoint[], seq: number): Promise<void> {
  const db = await openDb();
  const tx = db.transaction([RIDES, CHUNKS], 'readwrite');
  tx.objectStore(RIDES).put(meta);
  if (points.length > 0) {
    const chunk: RideChunk = { rideId: meta.id, seq, points: [...points] };
    tx.objectStore(CHUNKS).put(chunk);
  }
  await done(tx);
}

async function loadPoints(db: IDBDatabase, rideId: string): Promise<{ points: RidePoint[]; nextSeq: number }> {
  const tx = db.transaction(CHUNKS, 'readonly');
  const range = IDBKeyRange.bound([rideId, 0], [rideId, Number.MAX_SAFE_INTEGER]);
  const chunks = await result(tx.objectStore(CHUNKS).getAll(range) as IDBRequest<RideChunk[]>);
  const nextSeq = chunks.reduce((max, c) => Math.max(max, c.seq + 1), 0);
  // A chunk that failed once is written again later under a higher seq, so
  // the points are put back in time order rather than chunk order.
  const points = chunks.flatMap((c) => c.points).sort((a, b) => a.t - b.t);
  return { points, nextSeq };
}

/** Keeps the route a ride follows, once, at its start. */
export async function saveRoute(rideId: string, route: RideRoute): Promise<void> {
  const db = await openDb();
  const tx = db.transaction(ROUTES, 'readwrite');
  const record: StoredRoute = { rideId, route };
  tx.objectStore(ROUTES).put(record);
  await done(tx);
}

async function loadRoute(db: IDBDatabase, rideId: string): Promise<RideRoute | null> {
  const tx = db.transaction(ROUTES, 'readonly');
  const record = await result(tx.objectStore(ROUTES).get(rideId) as IDBRequest<StoredRoute | undefined>);
  return record?.route ?? null;
}

async function allRides(db: IDBDatabase): Promise<RideMeta[]> {
  const tx = db.transaction(RIDES, 'readonly');
  return result(tx.objectStore(RIDES).getAll() as IDBRequest<RideMeta[]>);
}

/** The newest ride that was never finished, with its points and route, or null. */
export async function findUnfinished(): Promise<(StoredRide & { nextSeq: number }) | null> {
  const db = await openDb();
  const open = (await allRides(db)).filter((r) => r.status !== 'finished').sort((a, b) => b.createdAt - a.createdAt)[0];
  if (!open) return null;
  return loadRideIn(db, open);
}

async function loadRideIn(db: IDBDatabase, meta: RideMeta): Promise<StoredRide & { nextSeq: number }> {
  const { points, nextSeq } = await loadPoints(db, meta.id);
  const route = await loadRoute(db, meta.id);
  return { meta, points, route, nextSeq };
}

/** One stored ride with everything in it, or null if it is not there any more. */
export async function loadRide(id: string): Promise<(StoredRide & { nextSeq: number }) | null> {
  const db = await openDb();
  const tx = db.transaction(RIDES, 'readonly');
  const meta = await result(tx.objectStore(RIDES).get(id) as IDBRequest<RideMeta | undefined>);
  return meta ? loadRideIn(db, meta) : null;
}

/** The finished rides on this phone, newest first. Rides that never got a GPS fix are left out. */
export async function listFinished(): Promise<RideListItem[]> {
  const db = await openDb();
  const finished = (await allRides(db)).filter((r) => r.status === 'finished' && r.clock.startedAt !== null);
  const items: RideListItem[] = [];
  for (const meta of finished) {
    let summary = meta.summary;
    if (!summary) {
      // A ride finished before summaries were kept: worked out once from its points.
      const { points } = await loadPoints(db, meta.id);
      summary = summaryFromPoints(points, new Map((meta.fills ?? []).map((f) => [f.t, f.roadM])), meta.clock);
    }
    items.push({ id: meta.id, startedAt: meta.clock.startedAt ?? meta.createdAt, summary, routeName: meta.routeName ?? null, loggedId: meta.loggedId ?? null });
  }
  return items.sort((a, b) => b.startedAt - a.startedAt);
}

/** Removes a ride for good: its metadata, its points and its route. */
export async function deleteRide(id: string): Promise<void> {
  const db = await openDb();
  const tx = db.transaction([RIDES, CHUNKS, ROUTES], 'readwrite');
  tx.objectStore(RIDES).delete(id);
  tx.objectStore(CHUNKS).delete(IDBKeyRange.bound([id, 0], [id, Number.MAX_SAFE_INTEGER]));
  tx.objectStore(ROUTES).delete(id);
  await done(tx);
}
