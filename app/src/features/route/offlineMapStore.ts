import { useSyncExternalStore } from 'react';
import { OFFLINE_MAP_CACHE_PREFIX } from '../../lib/offlineMapKeys';
import { KEYS, readJson, writeJson } from '../../lib/storage';
import { routeMapId } from './offlineMap';
import { downloadRouteMap, OfflineMapError, offlineMapsSupported, type OfflineMap, type OfflineMapErrorKind } from './offlineMapDownload';
import type { LatLon } from './types';

// The maps on the phone (a list in localStorage, one cache each, see
// offlineMapDownload.ts) and the downloads under way. A map older than
// KEEP_DAYS is deleted when the app starts.

export const KEEP_DAYS = 30;

// ---- The list ----------------------------------------------------------

function isList(value: unknown): value is OfflineMap[] {
  return Array.isArray(value) && value.every((m) => typeof m === 'object' && m !== null && typeof (m as OfflineMap).id === 'string');
}

let snapshot: OfflineMap[] = readJson(KEYS.offlineMaps, [], isList);
const listeners = new Set<() => void>();

function writeList(next: OfflineMap[]): void {
  snapshot = next;
  writeJson(KEYS.offlineMaps, next);
  for (const listener of listeners) listener();
}

function subscribe(listener: () => void): () => void {
  listeners.add(listener);
  return () => listeners.delete(listener);
}

/** The maps on this phone, oldest first. */
export function useOfflineMaps(): OfflineMap[] {
  return useSyncExternalStore(subscribe, () => snapshot, () => snapshot);
}

export async function deleteOfflineMap(id: string): Promise<void> {
  writeList(snapshot.filter((m) => m.id !== id));
  await caches.delete(OFFLINE_MAP_CACHE_PREFIX + id);
}

export async function deleteAllOfflineMaps(): Promise<void> {
  writeList([]);
  const names = await caches.keys();
  await Promise.all(names.filter((n) => n.startsWith(OFFLINE_MAP_CACHE_PREFIX)).map((n) => caches.delete(n)));
}

/** At start-up: drop downloads past KEEP_DAYS, and caches a stopped download left behind. */
export async function pruneOfflineMaps(now: number = Date.now()): Promise<void> {
  if (!offlineMapsSupported()) return;
  try {
    const keep = snapshot.filter((m) => now - m.savedAt < KEEP_DAYS * 86_400_000);
    if (keep.length !== snapshot.length) writeList(keep);
    const kept = new Set(keep.map((m) => OFFLINE_MAP_CACHE_PREFIX + m.id));
    const names = await caches.keys();
    await Promise.all(names.filter((n) => n.startsWith(OFFLINE_MAP_CACHE_PREFIX) && !kept.has(n)).map((n) => caches.delete(n)));
  } catch (err) {
    console.warn('[offline map] could not tidy the downloaded maps', err);
  }
}

// ---- Downloads under way -------------------------------------------------
//
// Kept here, not in a component, so a download goes on when the rider leaves
// the screen it started from, and the planner and Drum show the same one.

export interface DownloadState {
  progress: { done: number; total: number } | null;
  error: OfflineMapErrorKind | null;
}

const IDLE: DownloadState = { progress: null, error: null };
let downloads: Readonly<Record<string, DownloadState>> = {};
const controllers = new Map<string, AbortController>();
const downloadListeners = new Set<() => void>();

function setDownload(id: string, state: DownloadState | null): void {
  const next = { ...downloads };
  if (state) next[id] = state;
  else delete next[id];
  downloads = next;
  for (const listener of downloadListeners) listener();
}

function subscribeDownloads(listener: () => void): () => void {
  downloadListeners.add(listener);
  return () => downloadListeners.delete(listener);
}

export function useDownloadState(id: string | null): DownloadState {
  return useSyncExternalStore(subscribeDownloads, () => (id ? (downloads[id] ?? IDLE) : IDLE), () => IDLE);
}

/** Start downloading the route's map, unless it is already under way. */
export function startDownload(line: readonly LatLon[], name: string): void {
  const id = routeMapId(line);
  if (controllers.has(id)) return;
  const controller = new AbortController();
  controllers.set(id, controller);
  setDownload(id, { progress: { done: 0, total: 0 }, error: null });
  // Re-render every few files, not on each of up to 3,000.
  let shown = 0;
  const onProgress = (done: number, total: number): void => {
    if (done - shown < 20 && done !== total && done !== 0) return;
    shown = done;
    setDownload(id, { progress: { done, total }, error: null });
  };
  downloadRouteMap(line, name, onProgress, controller.signal)
    .then((entry) => {
      writeList([...snapshot.filter((m) => m.id !== entry.id), entry]);
      setDownload(id, null);
    })
    .catch((err: unknown) => {
      if (controller.signal.aborted) {
        setDownload(id, null);
        return;
      }
      console.warn('[offline map] download failed', err);
      setDownload(id, { progress: null, error: err instanceof OfflineMapError ? err.kind : 'network' });
    })
    .finally(() => controllers.delete(id));
}

export function stopDownload(id: string): void {
  controllers.get(id)?.abort();
}
