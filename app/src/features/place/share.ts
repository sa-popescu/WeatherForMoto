// Shareable link to a place: the app resolves ?q=<name> on start-up.

const SHARE_BASE_URL = 'https://weatherformoto.bluemouse.cc/';

export function shareUrl(name: string): string {
  return `${SHARE_BASE_URL}?q=${encodeURIComponent(name)}`;
}

export type ShareOutcome = 'shared' | 'copied' | 'cancelled';

/**
 * Native share sheet when available, otherwise the clipboard.
 * Throws when neither works, so the caller can show an error.
 */
export async function sharePlace(name: string, text: string): Promise<ShareOutcome> {
  const url = shareUrl(name);
  if (typeof navigator.share === 'function') {
    try {
      await navigator.share({ title: 'MotoMeteo', text, url });
      return 'shared';
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') return 'cancelled';
      console.warn('[place] native share failed, copying instead', err);
    }
  }
  await navigator.clipboard.writeText(url);
  return 'copied';
}
