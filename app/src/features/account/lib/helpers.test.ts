import { describe, expect, it } from 'vitest';
import type { CheckNowResponse } from '../../../lib/types';
import { PREF_DEFAULTS } from '../../../state/auth';
import { DEFAULT_PROFILE } from '../../../state/settings';
import { describeCheckNow } from './checkNow';
import { isIosDevice, pushAvailability } from './platform';
import { mapPool } from './pool';
import { dropSent, mergePrefs } from './prefsOverlay';
import { prefsFromProfile, reconcileProfile } from './profile';

describe('prefs overlay', () => {
  it('shows local edits on top of the server copy', () => {
    expect(mergePrefs(PREF_DEFAULTS, { min_score: 60 })?.min_score).toBe(60);
    expect(mergePrefs(null, { min_score: 60 })).toBeNull();
  });

  it('drops what was sent but keeps newer edits', () => {
    const overlay = { min_score: 60, severity: 'high' as const };
    expect(dropSent(overlay, { min_score: 60 })).toEqual({ severity: 'high' });
    expect(dropSent({ min_score: 65 }, { min_score: 60 })).toEqual({ min_score: 65 });
    const same = { city: 'Sibiu' };
    expect(dropSent(same, { min_score: 1 })).toBe(same);
    expect(dropSent({ min_temp: null }, { min_temp: null })).toEqual({});
  });
});

describe('describeCheckNow', () => {
  const oneDevice = 1;

  it('explains skipped checks', () => {
    expect(describeCheckNow({ reason: 'alerts_disabled' }, oneDevice)).toEqual({ kind: 'disabled' });
    expect(describeCheckNow({ reason: 'missing_home_location' }, oneDevice)).toEqual({ kind: 'noLocation' });
    expect(describeCheckNow({ reason: 'quiet_hours', events: [{}] }, oneDevice)).toEqual({ kind: 'quiet' });
  });

  // Older backends sent `delivered` as a boolean; both shapes must work.
  const res = (body: Record<string, unknown>): CheckNowResponse => body as CheckNowResponse;

  it('counts deliveries and separates the empty cases', () => {
    expect(describeCheckNow(res({ ok: true, delivered: 2, events: [{}, {}] }), oneDevice)).toEqual({ kind: 'delivered', count: 2 });
    expect(describeCheckNow(res({ ok: true, delivered: 0, events: [] }), oneDevice)).toEqual({ kind: 'nothing' });
    expect(describeCheckNow(res({ ok: true, delivered: 0, events: [{}] }), 0)).toEqual({ kind: 'noChannel' });
    expect(describeCheckNow(res({ ok: true, delivered: 0, events: [{}] }), oneDevice)).toEqual({ kind: 'alreadySent', count: 1 });
    expect(describeCheckNow(res({ ok: true, delivered: true, events: [{}] }), oneDevice)).toEqual({ kind: 'delivered', count: 1 });
  });
});

describe('platform', () => {
  it('recognises iPhones and iPads that pose as Macs', () => {
    expect(isIosDevice('Mozilla/5.0 (iPhone; CPU iPhone OS 17_4 like Mac OS X)', 'iPhone', 5)).toBe(true);
    expect(isIosDevice('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', 'MacIntel', 5)).toBe(true);
    expect(isIosDevice('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)', 'MacIntel', 0)).toBe(false);
    expect(isIosDevice('Mozilla/5.0 (Linux; Android 14)', 'Linux armv8l', 5)).toBe(false);
  });

  it('asks iOS users to install before anything else', () => {
    const base = { supported: true, ios: false, standalone: false, permission: 'default' as const };
    expect(pushAvailability(base)).toBe('ok');
    expect(pushAvailability({ ...base, ios: true, supported: false })).toBe('ios-install');
    expect(pushAvailability({ ...base, ios: true, standalone: true })).toBe('ok');
    expect(pushAvailability({ ...base, supported: false })).toBe('unsupported');
    expect(pushAvailability({ ...base, permission: 'denied' })).toBe('denied');
  });
});

describe('mapPool', () => {
  it('never runs more than the limit at once and keeps result order', async () => {
    let running = 0;
    let peak = 0;
    const results = await mapPool([30, 10, 20, 5], 2, async (ms) => {
      running += 1;
      peak = Math.max(peak, running);
      await new Promise((resolve) => setTimeout(resolve, ms));
      running -= 1;
      return ms * 2;
    });
    expect(peak).toBe(2);
    expect(results.map((r) => (r.status === 'fulfilled' ? r.value : null))).toEqual([60, 20, 40, 10]);
  });

  it('records failures and stops picking work after abort', async () => {
    const controller = new AbortController();
    const started: number[] = [];
    const results = await mapPool([1, 2, 3, 4], 1, async (n) => {
      started.push(n);
      if (n === 2) controller.abort();
      if (n === 1) throw new Error('no data');
      return n;
    }, controller.signal);
    expect(started).toEqual([1, 2]);
    expect(results[0]?.status).toBe('rejected');
  });
});

describe('reconcileProfile', () => {
  const custom = { motoType: 'touring' as const, comfortTemp: 18, windTolerance: 'high' as const, rainTolerance: 'low' as const };

  it('adopts the account copy when it was customised', () => {
    expect(reconcileProfile(DEFAULT_PROFILE, prefsFromProfile(custom))).toEqual({ action: 'adopt', profile: custom });
  });

  it('uploads the device copy when the account still has defaults', () => {
    expect(reconcileProfile(custom, prefsFromProfile(DEFAULT_PROFILE))).toEqual({ action: 'push', profile: custom });
  });

  it('does nothing when both agree', () => {
    expect(reconcileProfile(custom, prefsFromProfile(custom))).toEqual({ action: 'none' });
  });
});
