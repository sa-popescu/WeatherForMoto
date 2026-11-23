// Shared by the phone that sends its position (features/ride/liveShare.ts)
// and the page that shows it (LiveView.tsx); the Worker is worker/live.ts.

/** Where the Worker answers, on the app's own domain. */
export const LIVE_PATH = '/live';

/** How often the position is sent, and asked for; the Worker refuses updates under 25 s apart. */
export const SEND_EVERY_MS = 30_000;

/**
 * The link a rider sends. The Worker (worker/live.ts) sends people on to the
 * live page, /#/live/<id>, and gives a chat app's preview robot the ride's
 * destination and arrival, which a link to the hash could not. The share
 * adds "?lang=en" for that preview in English (LiveShareControl.tsx).
 */
export function liveUrl(id: string, origin: string = window.location.origin): string {
  return `${origin}/l/${id}`;
}

/** The id in the live page's hash, or null. */
export function liveIdFromHash(hash: string): string | null {
  const match = /^#\/live\/([A-Za-z0-9_-]{22})$/.exec(hash);
  return match ? match[1] : null;
}
