import { placeLinkUrl } from '../../lib/placeLink';
import { shareLink, type ShareOutcome } from '../../lib/share';
import type { Place } from '../../lib/types';

// Share a place: the app opens the link on start-up (see lib/placeLink.ts).

export type { ShareOutcome };

/** Share sheet or clipboard (lib/share.ts); throws when neither works. */
export function sharePlace(place: Place, text: string): Promise<ShareOutcome> {
  return shareLink(placeLinkUrl(place), text);
}
