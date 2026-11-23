// Shared by the phone that sends its position (features/ride/liveShare.ts)
// and the page that shows it (LiveView.tsx); the Worker is worker/live.ts.

/** Where the Worker answers, on the app's own domain. */
export const LIVE_PATH = '/live';

/** How often the position is sent, and asked for; the Worker refuses updates under 25 s apart. */
export const SEND_EVERY_MS = 30_000;

/** The link a viewer opens: the id travels after the "#", so it stays out of every server log. */
export function liveUrl(id: string, origin: string = window.location.origin): string {
  return `${origin}/#/live/${id}`;
}

/** The id in a live link's hash, or null. */
export function liveIdFromHash(hash: string): string | null {
  const match = /^#\/live\/([A-Za-z0-9_-]{22})$/.exec(hash);
  return match ? match[1] : null;
}
