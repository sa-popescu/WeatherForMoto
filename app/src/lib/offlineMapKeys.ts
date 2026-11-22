// Where a map downloaded for a route lives in the Cache Storage. Shared by
// the page that downloads it (features/route/offlineMapStore.ts) and the
// service worker that serves it when there is no signal (sw.ts), so no DOM
// here.

/** Each download is its own cache, so it can be deleted on its own. */
export const OFFLINE_MAP_CACHE_PREFIX = 'mm-offline-map-';

// OpenFreeMap puts each weekly release of the vector tiles in its own folder:
// /planet/20261115_080022_pt/14/9310/5800.pbf
const RELEASE_TILE = /\/planet\/[^/]+\/(\d+\/\d+\/\d+\.pbf)$/;

/**
 * The key a downloaded map file is kept under. Vector tiles lose their
 * release folder, so a map downloaded before a new release is still found
 * after the app has seen the new one; every other file keeps its address.
 */
export function offlineKey(href: string): string {
  return href.replace(RELEASE_TILE, '/planet/offline/$1');
}
