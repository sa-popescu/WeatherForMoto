import { describe, expect, it } from 'vitest';
import { AUTO_APPLY_WINDOW_MS, MIN_GAP_MS, checkDue, mayApplyUnasked } from './updateTiming';

describe('checkDue', () => {
  it('checks the first time', () => expect(checkDue(1_000, 0)).toBe(true));
  it('folds checks fired together', () => expect(checkDue(10_000 + MIN_GAP_MS - 1, 10_000)).toBe(false));
  it('checks again after the gap', () => expect(checkDue(10_000 + MIN_GAP_MS, 10_000)).toBe(true));
});

describe('mayApplyUnasked', () => {
  it('loads a version found right as the app started', () => expect(mayApplyUnasked(5_000, 1_000, false)).toBe(true));
  it('asks once the user has tapped', () => expect(mayApplyUnasked(5_000, 1_000, true)).toBe(false));
  it('asks when the version comes later', () => expect(mayApplyUnasked(1_000 + AUTO_APPLY_WINDOW_MS + 1, 1_000, false)).toBe(false));
  it('asks when the start time is unknown', () => expect(mayApplyUnasked(5_000, 0, false)).toBe(false));
});
