import { createContext, useCallback, useContext, useEffect, useMemo, useState, type ReactNode } from 'react';
import { LangContext, type Lang } from '../lib/i18n';
import { KEYS, readJson, readString, writeJson, writeString } from '../lib/storage';
import type { Level, MotoType } from '../lib/types';

export type Theme = 'dark' | 'day';

export interface MotoProfile {
  motoType: MotoType;
  comfortTemp: number;
  windTolerance: Level;
  rainTolerance: Level;
}

export const DEFAULT_PROFILE: MotoProfile = { motoType: 'naked', comfortTemp: 20, windTolerance: 'medium', rainTolerance: 'medium' };

interface SettingsState {
  lang: Lang;
  setLang: (lang: Lang) => void;
  theme: Theme;
  setTheme: (theme: Theme) => void;
  profile: MotoProfile;
  setProfile: (profile: MotoProfile) => void;
}

const SettingsContext = createContext<SettingsState | null>(null);

const THEME_COLOR: Record<Theme, string> = { dark: '#0b0c0a', day: '#f3f4ee' };

function initialLang(): Lang {
  const stored = readString(KEYS.lang);
  return stored === 'en' ? 'en' : 'ro';
}

function initialTheme(): Theme {
  // The legacy app stored "light"; the new day theme replaces it.
  const stored = readString(KEYS.theme);
  return stored === 'light' || stored === 'day' ? 'day' : 'dark';
}

function isObject(value: unknown): value is Partial<MotoProfile> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

export function SettingsProvider({ children }: { children: ReactNode }) {
  const [lang, setLangState] = useState<Lang>(initialLang);
  const [theme, setThemeState] = useState<Theme>(initialTheme);
  const [profile, setProfileState] = useState<MotoProfile>(() => ({ ...DEFAULT_PROFILE, ...readJson(KEYS.profile, {}, isObject) }));

  useEffect(() => {
    document.documentElement.lang = lang;
  }, [lang]);

  useEffect(() => {
    document.documentElement.dataset.theme = theme;
    document.querySelector('meta[name="theme-color"]')?.setAttribute('content', THEME_COLOR[theme]);
  }, [theme]);

  const setLang = useCallback((next: Lang) => {
    writeString(KEYS.lang, next);
    setLangState(next);
  }, []);

  const setTheme = useCallback((next: Theme) => {
    writeString(KEYS.theme, next);
    setThemeState(next);
  }, []);

  const setProfile = useCallback((next: MotoProfile) => {
    writeJson(KEYS.profile, next);
    setProfileState(next);
  }, []);

  const value = useMemo(() => ({ lang, setLang, theme, setTheme, profile, setProfile }), [lang, setLang, theme, setTheme, profile, setProfile]);

  return (
    <SettingsContext.Provider value={value}>
      <LangContext.Provider value={lang}>{children}</LangContext.Provider>
    </SettingsContext.Provider>
  );
}

export function useSettings(): SettingsState {
  const ctx = useContext(SettingsContext);
  if (!ctx) throw new Error('useSettings must be used inside SettingsProvider');
  return ctx;
}
