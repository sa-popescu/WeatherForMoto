import { describe, expect, it } from 'vitest';
import { liveIdFromHash, liveUrl } from './constants';

describe('live links', () => {
  const id = 'AbCdEfGhIjKlMnOpQrSt_-';

  it('sends a link the Worker can preview, which then opens the live page', () => {
    expect(liveUrl(id, 'https://motonor.bluemouse.cc')).toBe(`https://motonor.bluemouse.cc/l/${id}`);
  });

  it('reads the id back, and nothing that is not one', () => {
    expect(liveIdFromHash(`#/live/${id}`)).toBe(id);
    expect(liveIdFromHash('#/live/short')).toBeNull();
    expect(liveIdFromHash(`#/live/${id}/x`)).toBeNull();
    expect(liveIdFromHash('#/drum')).toBeNull();
  });
});
