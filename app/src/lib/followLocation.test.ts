import { describe, expect, it } from 'vitest';
import { FIX_STALE_MS, locateWaitMs, PROMPT_WAIT_MS, shouldLocateOnBoot, shouldRefreshOnResume } from './followLocation';

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

  it('never brings the prompt back on return when no fix landed in this run', () => {
    // Refused, ignored or never asked: the browser would ask again, so stay quiet.
    expect(shouldRefreshOnResume(true, 'prompt', null, now)).toBe(false);
    // Granted for good: locating is silent, so the first fix can still be fetched.
    expect(shouldRefreshOnResume(true, 'granted', null, now)).toBe(true);
  });

  it('refreshes a stale fix on Safari, where the permission always reads as "prompt"', () => {
    // A fix already landed in this run, so the answer was yes and the page keeps it.
    expect(shouldRefreshOnResume(true, 'prompt', now - FIX_STALE_MS - 1, now)).toBe(true);
    expect(shouldRefreshOnResume(true, 'prompt', now - 60_000, now)).toBe(false);
  });

  it('leaves a chosen place alone when following is off', () => {
    expect(shouldRefreshOnResume(false, 'granted', 0, now)).toBe(false);
  });

  it('stays quiet when location is refused or missing', () => {
    expect(shouldRefreshOnResume(true, 'denied', 0, now)).toBe(false);
    expect(shouldRefreshOnResume(true, 'unsupported', 0, now)).toBe(false);
  });
});

describe('locateWaitMs', () => {
  it('keeps the short limit when no prompt can appear', () => {
    expect(locateWaitMs('granted', 6_000)).toBe(6_000);
  });

  it('gives the rider time to answer the prompt', () => {
    expect(locateWaitMs('prompt', 6_000)).toBe(PROMPT_WAIT_MS);
    expect(locateWaitMs('prompt', PROMPT_WAIT_MS + 5_000)).toBe(PROMPT_WAIT_MS + 5_000);
  });
});
