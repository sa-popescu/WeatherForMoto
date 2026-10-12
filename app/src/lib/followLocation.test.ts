import { describe, expect, it } from 'vitest';
import { FIX_STALE_MS, shouldLocateOnBoot, shouldRefreshOnResume } from './followLocation';

describe('shouldLocateOnBoot', () => {
  it('locates when the permission is already granted, whatever the setting says', () => {
    expect(shouldLocateOnBoot(false, 'granted')).toBe(true);
    expect(shouldLocateOnBoot(true, 'granted')).toBe(true);
  });

  it('asks only the people who asked for it', () => {
    // Safari answers nothing for geolocation, so the state reads as "prompt".
    expect(shouldLocateOnBoot(true, 'prompt')).toBe(true);
    expect(shouldLocateOnBoot(false, 'prompt')).toBe(false);
  });

  it('never nags when the answer is already no', () => {
    expect(shouldLocateOnBoot(true, 'denied')).toBe(false);
    expect(shouldLocateOnBoot(true, 'unsupported')).toBe(false);
  });
});

describe('shouldRefreshOnResume', () => {
  const now = 1_700_000_000_000;

  it('refreshes a fix that has gone stale', () => {
    expect(shouldRefreshOnResume(true, 'granted', now - FIX_STALE_MS - 1, now)).toBe(true);
    expect(shouldRefreshOnResume(true, 'granted', now - 60_000, now)).toBe(false);
  });

  it('refreshes when there is no fix yet', () => {
    expect(shouldRefreshOnResume(true, 'prompt', null, now)).toBe(true);
  });

  it('leaves a chosen place alone when following is off', () => {
    expect(shouldRefreshOnResume(false, 'granted', 0, now)).toBe(false);
  });

  it('stays quiet when location is refused or missing', () => {
    expect(shouldRefreshOnResume(true, 'denied', 0, now)).toBe(false);
    expect(shouldRefreshOnResume(true, 'unsupported', 0, now)).toBe(false);
  });
});
