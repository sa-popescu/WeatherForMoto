import { describe, expect, it } from 'vitest';
import { liveIdFromHash, liveUrl } from './constants';

describe('live links', () => {
  const id = 'AbCdEfGhIjKlMnOpQrSt_-';

  it('puts the id after the "#", where no server sees it', () => {
    expect(liveUrl(id, 'https://motonor.bluemouse.cc')).toBe(`https://motonor.bluemouse.cc/#/live/${id}`);
  });

  it('reads the id back, and nothing that is not one', () => {
    expect(liveIdFromHash(`#/live/${id}`)).toBe(id);
    expect(liveIdFromHash('#/live/short')).toBeNull();
    expect(liveIdFromHash(`#/live/${id}/x`)).toBeNull();
    expect(liveIdFromHash('#/drum')).toBeNull();
  });
});
