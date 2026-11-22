// Sharing a link: the phone's share sheet where there is one, the clipboard
// otherwise. Used for places (features/place) and ride plans (features/route).

export type ShareOutcome = 'shared' | 'copied' | 'cancelled';

/**
 * Offers `url` with `text` through the share sheet, or copies the url.
 * A dismissed share sheet is 'cancelled', not an error. Throws when neither
 * the share sheet nor the clipboard works, so the caller can say so.
 */
export async function shareLink(url: string, text: string): Promise<ShareOutcome> {
  if (typeof navigator.share === 'function') {
    try {
      await navigator.share({ title: 'Motonor', text, url });
      return 'shared';
    } catch (err) {
      if (err instanceof DOMException && err.name === 'AbortError') return 'cancelled';
      console.warn('[share] native share failed, copying instead', err);
    }
  }
  await navigator.clipboard.writeText(url);
  return 'copied';
}
