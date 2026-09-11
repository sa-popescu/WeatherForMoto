import { createContext, useContext } from 'react';

// Tiny, type-safe i18n. Every feature keeps its own dictionary next to its
// components (no shared file to fight over) and declares both languages:
//
//   const S = defineStrings({ title: 'Traseu' }, { title: 'Route' });
//   const s = useStrings(S);  // s.title
//
// The EN dictionary must have exactly the RO keys, checked at compile time.

export type Lang = 'ro' | 'en';

export const LangContext = createContext<Lang>('ro');

export function useLang(): Lang {
  return useContext(LangContext);
}

export interface Strings<T extends Record<string, string>> {
  ro: T;
  en: { [K in keyof T]: string };
}

export function defineStrings<T extends Record<string, string>>(ro: T, en: { [K in keyof T]: string }): Strings<T> {
  return { ro, en };
}

export function useStrings<T extends Record<string, string>>(dict: Strings<T>): { [K in keyof T]: string } {
  return useLang() === 'en' ? dict.en : dict.ro;
}

/** Picks a language without React (e.g. inside plain helper functions). */
export function pick<T extends Record<string, string>>(dict: Strings<T>, lang: Lang): { [K in keyof T]: string } {
  return lang === 'en' ? dict.en : dict.ro;
}

/** Replaces {name} placeholders: fmt('Până la {hour}', { hour: '16:00' }). */
export function fmt(template: string, vars: Record<string, string | number>): string {
  return template.replace(/\{(\w+)\}/g, (match, key: string) => (key in vars ? String(vars[key]) : match));
}
