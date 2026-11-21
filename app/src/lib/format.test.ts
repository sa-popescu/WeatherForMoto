import { describe, expect, it } from 'vitest';
import { fmtNumber, fmtTemp } from './format';

describe('fmtNumber', () => {
  it('writes a value that rounds to zero from below as a plain zero', () => {
    expect(fmtNumber(-0.3, 'ro')).toBe('0');
    expect(fmtNumber(-0, 'en')).toBe('0');
    expect(fmtNumber(-0.04, 'en', 1)).toBe('0.0');
    expect(fmtNumber(-0.04, 'ro', 1)).toBe('0,0');
  });

  it('keeps the minus once the value shows a digit', () => {
    expect(fmtNumber(-0.5, 'ro')).toBe('-1');
    expect(fmtNumber(-1.2, 'ro')).toBe('-1');
    expect(fmtNumber(-0.05, 'en', 1)).toBe('-0.1');
  });

  it('leaves the rest as it was', () => {
    expect(fmtNumber(12.4, 'ro')).toBe('12');
    expect(fmtNumber(1.25, 'ro', 1)).toBe('1,3');
    expect(fmtNumber(null, 'ro')).toBe('–');
  });
});

describe('fmtTemp', () => {
  it('never shows "-0°"', () => {
    expect(fmtTemp(-0.3, 'ro')).toBe('0°');
    expect(fmtTemp(-0.5, 'ro')).toBe('0°');
  });

  it('keeps real frost', () => {
    expect(fmtTemp(-0.6, 'ro')).toBe('-1°');
    expect(fmtTemp(-3.2, 'en')).toBe('-3°');
  });
});
