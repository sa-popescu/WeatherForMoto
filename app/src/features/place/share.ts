import { placeLinkUrl } from '../../lib/placeLink';
import type { Place } from '../../lib/types';

// Share a place: the app opens the link on start-up (see lib/placeLink.ts).

export type ShareOutcome = 'shared' | 'copied' | 'cancelled';

/**
 * Native share sheet when available, otherwise the clipboard.
 * Throws when neither works, so the caller can show an error.
 */
export async function sharePlace(place: Place, text: string): Promise<ShareOutcome> {
  const url = placeLinkUrl(place);
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
