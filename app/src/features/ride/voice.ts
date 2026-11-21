import type { Lang } from '../../lib/i18n';
import { KEYS, readString, writeString } from '../../lib/storage';

// Spoken announcements, through the phone's own speech synthesis: with a
// Bluetooth intercom they reach the helmet. On iPhone, speech has to start
// from a tap once, so the ride's first sentence is said from the Start tap.

const LANG_TAG: Record<Lang, string> = { ro: 'ro-RO', en: 'en-GB' };

export function voiceAvailable(): boolean {
  return typeof window !== 'undefined' && 'speechSynthesis' in window && typeof SpeechSynthesisUtterance !== 'undefined';
}

/** On unless the rider turned it off (remembered on this phone). */
export function voiceWanted(): boolean {
  return readString(KEYS.rideVoice) !== 'off';
}

export function rememberVoice(on: boolean): void {
  writeString(KEYS.rideVoice, on ? null : 'off');
}

/** Says one sentence; a newer one cuts the one still being said. */
export function speak(text: string, lang: Lang): void {
  if (!voiceAvailable()) return;
  try {
    const utterance = new SpeechSynthesisUtterance(text);
    utterance.lang = LANG_TAG[lang];
    const voice = window.speechSynthesis.getVoices().find((v) => v.lang.replace('_', '-').toLowerCase().startsWith(lang));
    if (voice) utterance.voice = voice;
    window.speechSynthesis.cancel();
    window.speechSynthesis.speak(utterance);
  } catch (err) {
    console.warn('[ride] could not speak an announcement', err);
  }
}

/** Romanian puts "de" after whole numbers ending in 00 or in 20 to 99: "20 de minute", "112 minute". */
function takesDe(n: number): boolean {
  return Number.isInteger(n) && n >= 20 && (n % 100 === 0 || n % 100 >= 20);
}

/** Minutes as they are said: "un minut", "15 minute", "20 de minute". */
export function spokenMinutes(minutes: number, lang: Lang): string {
  const n = Math.max(1, Math.round(minutes));
  if (lang === 'en') return n === 1 ? '1 minute' : `${n} minutes`;
  if (n === 1) return 'un minut';
  return takesDe(n) ? `${n} de minute` : `${n} minute`;
}

/** A gust speed as it is said: "60 de kilometri pe oră". */
export function spokenKmh(kmh: number, lang: Lang): string {
  const n = Math.round(kmh);
  if (lang === 'en') return `${n} kilometres an hour`;
  return takesDe(n) ? `${n} de kilometri pe oră` : `${n} kilometri pe oră`;
}

/** A road number rather than a place: A1, DN7, DJ703N, E81. */
const ROAD_NAME = /^(A|DN|DJ|DC|E)\s?\d/i;

/** Where, said: "pe la Pitești", "pe DN7" (and "near Pitești", "on DN7"). */
export function spokenWhere(name: string, lang: Lang): string {
  const road = ROAD_NAME.test(name.trim());
  if (lang === 'en') return road ? `on ${name}` : `near ${name}`;
  return road ? `pe ${name}` : `pe la ${name}`;
}

/** A distance as it is said: "500 de metri", "un kilometru", "4,5 kilometri", "124 de kilometri". */
export function spokenDistance(km: number, lang: Lang): string {
  if (km < 0.95) {
    const metres = Math.max(50, Math.round((km * 1000) / 50) * 50);
    return lang === 'ro' ? `${metres} de metri` : `${metres} metres`;
  }
  const value = km < 10 ? Math.round(km * 10) / 10 : Math.round(km);
  const text = Number.isInteger(value) ? String(value) : value.toLocaleString(lang === 'ro' ? 'ro-RO' : 'en-GB', { maximumFractionDigits: 1 });
  if (lang === 'en') return value === 1 ? '1 kilometre' : `${text} kilometres`;
  if (value === 1) return 'un kilometru';
  return takesDe(value) ? `${text} de kilometri` : `${text} kilometri`;
}
