import { describe, expect, it } from 'vitest';
import { cityForPrefs, cleanDisplayName, deleteConfirmMatches, isCompleteCode, isValidEmail, normalizeCode, passwordProblem } from './validation';

describe('isValidEmail', () => {
  it('accepts ordinary addresses, trimmed', () => {
    expect(isValidEmail('rider@example.ro')).toBe(true);
    expect(isValidEmail('  a.b+moto@mail.co.uk ')).toBe(true);
  });

  it('rejects incomplete ones', () => {
    expect(isValidEmail('')).toBe(false);
    expect(isValidEmail('rider@')).toBe(false);
    expect(isValidEmail('rider@example')).toBe(false);
    expect(isValidEmail('two words@example.ro')).toBe(false);
  });
});

describe('passwordProblem', () => {
  it('enforces the 8 to 256 character range', () => {
    expect(passwordProblem('1234567')).toBe('short');
    expect(passwordProblem('12345678')).toBeNull();
    expect(passwordProblem('x'.repeat(256))).toBeNull();
    expect(passwordProblem('x'.repeat(257))).toBe('long');
  });
});

describe('login codes', () => {
  it('keeps six digits from pasted text', () => {
    expect(normalizeCode('Cod: 123 456')).toBe('123456');
    expect(normalizeCode('12345678')).toBe('123456');
    expect(normalizeCode('abc')).toBe('');
  });

  it('knows when the code is complete', () => {
    expect(isCompleteCode('123456')).toBe(true);
    expect(isCompleteCode('12345')).toBe(false);
  });
});

describe('deleteConfirmMatches', () => {
  it('accepts the Romanian word with or without diacritics, any case', () => {
    expect(deleteConfirmMatches('ȘTERGE', 'ro')).toBe(true);
    expect(deleteConfirmMatches(' șterge ', 'ro')).toBe(true);
    expect(deleteConfirmMatches('sterge', 'ro')).toBe(true);
    expect(deleteConfirmMatches('ŞTERGE', 'ro')).toBe(true);
    expect(deleteConfirmMatches('DELETE', 'ro')).toBe(false);
    expect(deleteConfirmMatches('STERG', 'ro')).toBe(false);
  });

  it('accepts DELETE in English', () => {
    expect(deleteConfirmMatches('delete', 'en')).toBe(true);
    expect(deleteConfirmMatches('ȘTERGE', 'en')).toBe(false);
  });
});

describe('text cleanup', () => {
  it('collapses spaces in display names and caps the length', () => {
    expect(cleanDisplayName('  Ana   Maria ')).toBe('Ana Maria');
    expect(cleanDisplayName('x'.repeat(100))).toHaveLength(80);
  });

  it('makes a place name safe for the city field', () => {
    expect(cityForPrefs('  Brașov  ')).toBe('Brașov');
    expect(cityForPrefs('Cluj<script>')).toBe('Clujscript');
    expect(cityForPrefs('Sibiu')).toBe('Sibiu');
    expect(cityForPrefs('   ')).toBeNull();
    expect(cityForPrefs('x'.repeat(200))).toHaveLength(120);
  });
});
