import { KEYS, readJson, writeJson } from '../../lib/storage';

// When the install invitation shows up by itself: at the first visit in a
// browser, once more a week later if the app is still not installed, then
// never again (the guide stays in Eu, Aplicație). Only this note is kept,
// on the device.

export interface InviteRecord {
  /** How many times the invitation was shown. */
  shown: number;
  /** When it was last shown, in ms. */
  lastAt: number;
}

export const REMIND_AFTER_MS = 7 * 24 * 60 * 60 * 1000;
export const MAX_SHOWS = 2;

function isRecord(value: unknown): value is InviteRecord | null {
  if (value === null) return true;
  const v = value as InviteRecord;
  return typeof v === 'object' && Number.isFinite(v.shown) && Number.isFinite(v.lastAt);
}

export function inviteDue(record: InviteRecord | null, now: number): boolean {
  if (!record) return true;
  if (record.shown >= MAX_SHOWS) return false;
  return now - record.lastAt >= REMIND_AFTER_MS;
}

export function afterShowing(record: InviteRecord | null, now: number): InviteRecord {
  return { shown: (record?.shown ?? 0) + 1, lastAt: now };
}

export function readInvite(): InviteRecord | null {
  return readJson<InviteRecord | null>(KEYS.installInvite, null, isRecord);
}

export function writeInvite(record: InviteRecord): void {
  writeJson(KEYS.installInvite, record);
}
