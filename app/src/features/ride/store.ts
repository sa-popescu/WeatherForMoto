import type { GapFill } from './gapFill';
import type { RideClock } from './logic/clock';
import type { RidePoint } from './logic/track';

// Rides kept on the phone, in IndexedDB, so that a ride survives the app being
// closed or killed mid-way. A ride is one record of metadata plus its points,
// written in small chunks every few seconds: appending a chunk costs the same
// at the first kilometre and at the three hundredth, unlike rewriting one
// growing array. Nothing here leaves the device.

const DB_NAME = 'motometeo-rides';
const DB_VERSION = 1;
const RIDES = 'rides';
const CHUNKS = 'chunks';

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
}

interface RideChunk {
  rideId: string;
  seq: number;
  points: RidePoint[];
}

export interface StoredRide {
  meta: RideMeta;
  points: RidePoint[];
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

/** The newest ride that was never finished, with its points, or null. */
export async function findUnfinished(): Promise<(StoredRide & { nextSeq: number }) | null> {
  const db = await openDb();
  const tx = db.transaction(RIDES, 'readonly');
  const rides = await result(tx.objectStore(RIDES).getAll() as IDBRequest<RideMeta[]>);
  const open = rides.filter((r) => r.status !== 'finished').sort((a, b) => b.createdAt - a.createdAt)[0];
  if (!open) return null;
  const { points, nextSeq } = await loadPoints(db, open.id);
  return { meta: open, points, nextSeq };
}
