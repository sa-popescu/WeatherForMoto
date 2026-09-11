import type { Lang } from '../../../lib/i18n';

// Client-side checks that mirror the backend limits (backend/auth_alerts.py),
// so the user sees the problem before a round trip.

export const PASSWORD_MIN = 8;
export const PASSWORD_MAX = 256;
export const CODE_LENGTH = 6;
export const NAME_MAX = 80;
export const CITY_MAX = 120;

// Deliberately loose: the server does the real validation (EmailStr).
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;

export function isValidEmail(value: string): boolean {
  const email = value.trim();
  return email.length <= 254 && EMAIL_RE.test(email);
}

export type PasswordProblem = 'short' | 'long' | null;

export function passwordProblem(password: string): PasswordProblem {
  if (password.length < PASSWORD_MIN) return 'short';
  if (password.length > PASSWORD_MAX) return 'long';
  return null;
}

/** Keeps digits only, at most six (paste of "123 456" or "Cod: 123456" works). */
export function normalizeCode(input: string): string {
  return input.replace(/\D/g, '').slice(0, CODE_LENGTH);
}

export function isCompleteCode(code: string): boolean {
  return new RegExp(`^\\d{${CODE_LENGTH}}$`).test(code);
}

export function cleanDisplayName(value: string): string {
  return value.replace(/\s+/g, ' ').trim().slice(0, NAME_MAX);
}

/** The word the user must type to delete the account, per language. */
export const DELETE_WORD: Record<Lang, string> = { ro: 'ȘTERGE', en: 'DELETE' };

// Variants many phone keyboards produce: no diacritics, or the legacy cedilla "Ş" (U+015E).
const DELETE_WORD_RO_VARIANTS = ['STERGE', 'ŞTERGE'];

/** Accepts the word in any case; for Romanian also the keyboard variants above. */
export function deleteConfirmMatches(input: string, lang: Lang): boolean {
  const typed = input.trim().toLocaleUpperCase('ro-RO');
  if (lang === 'en') return typed === DELETE_WORD.en;
  return typed === DELETE_WORD.ro || DELETE_WORD_RO_VARIANTS.includes(typed);
}

function isControlChar(ch: string): boolean {
  const code = ch.codePointAt(0) ?? 0;
  return code < 0x20 || (code >= 0x7f && code <= 0x9f);
}

/**
 * City name as the backend accepts it: trimmed, no control characters, no
 * angle brackets, at most 120 characters. Returns null when nothing is left.
 */
export function cityForPrefs(name: string): string | null {
  const cleaned = Array.from(name)
    .filter((ch) => !isControlChar(ch) && ch !== '<' && ch !== '>')
    .join('')
    .replace(/\s+/g, ' ')
    .trim()
    .slice(0, CITY_MAX)
    .trim();
  return cleaned || null;
}
