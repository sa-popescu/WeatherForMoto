import { useEffect, useRef, useState } from 'react';
import { CORE } from '../../i18n/core';
import { ageMinutes, dayMonth, dayName, hourOf, localNowIso } from '../../lib/format';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { usePlace } from '../../state/place';
import { useToast } from '../../state/toast';
import { Icon } from '../../ui/icons';
import { IconButton, Spinner } from '../../ui/primitives';
import { geoMessage } from './geoMessage';
import { PlaceSheet } from './PlaceSheet';
import { S_PLACE } from './strings';
import './place.css';

// Place header: city (opens the place sheet), local date and time at the
// location, number of sources, data age, plus GPS and search buttons.

export interface PlaceHeaderProps {
  /** The location's UTC offset once weather is loaded, for its local clock. */
  utcOffsetSeconds: number | null;
  sources: number | null;
  fetchedAt: number | null;
  /** Refreshing data that is already on screen (not the first load). */
  refreshing: boolean;
  /** First load in progress. */
  loading: boolean;
  nowMs: number;
}

type SheetMode = 'closed' | 'browse' | 'search';

const AGE_WORDS = {
  ro: { justNow: 'chiar acum', minutes: 'acum {n} min', hours: 'acum {n} h' },
  en: { justNow: 'just now', minutes: '{n} min ago', hours: '{n} h ago' },
} as const;

export function PlaceHeader({ utcOffsetSeconds, sources, fetchedAt, refreshing, loading, nowMs }: PlaceHeaderProps) {
  const s = useStrings(S_PLACE);
  const core = useStrings(CORE);
  const lang = useLang();
  const toast = useToast();
  const { place, locate, locating, locateError } = usePlace();
  const [mode, setMode] = useState<SheetMode>('closed');
  const askedHere = useRef(false);

  // Only a tap on this button reports GPS failures (the silent start-up fix stays silent).
  useEffect(() => {
    if (!askedHere.current || !locateError) return;
    askedHere.current = false;
    toast(geoMessage(locateError, s), { tone: 'error' });
  }, [locateError, s, toast]);

  const onLocate = async (): Promise<void> => {
    askedHere.current = true;
    if (await locate()) askedHere.current = false;
  };

  const local = utcOffsetSeconds == null ? null : localNowIso(utcOffsetSeconds, nowMs);
  const when = local ? `${dayName(local.slice(0, 10), lang, 'long')} ${dayMonth(local.slice(0, 10), lang)} · ${hourOf(local)}` : null;
  const sourceText = sources == null || sources === 0 ? null : sources === 1 ? s.sourcesOne : fmt(s.sourcesMany, { n: sources });
  const minutes = fetchedAt == null ? null : ageMinutes(fetchedAt, nowMs);
  const words = AGE_WORDS[lang];
  const age = minutes == null ? null : minutes < 1 ? words.justNow : minutes < 60 ? fmt(words.minutes, { n: minutes }) : fmt(words.hours, { n: Math.floor(minutes / 60) });

  return (
    <header className="place-head">
      <div className="place-head__text">
        <button type="button" className="place-head__name" onClick={() => setMode('browse')} aria-haspopup="dialog" aria-label={fmt(s.changePlace, { name: place.name })}>
          <span className="place-head__city num">{place.name}</span>
          <Icon name="chevronDown" size={20} className="place-head__chev" />
        </button>
        <p className="place-head__meta">
          {(when || loading) && <span>{when ?? core.loading}</span>}
          {(sourceText || age || refreshing) && (
            <span className="place-head__status">
              {sourceText}
              {sourceText && (age || refreshing) ? ' · ' : ''}
              {refreshing ? (
                <>
                  <Spinner size={11} />
                  {s.refreshing}
                </>
              ) : (
                age
              )}
            </span>
          )}
        </p>
      </div>
      <div className="place-head__actions">
        <button type="button" className="icon-btn" onClick={() => void onLocate()} disabled={locating} aria-label={locating ? s.locating : s.locate} title={s.locate}>
          {locating ? <Spinner size={20} /> : <Icon name="pin" size={22} />}
        </button>
        <IconButton icon="search" label={s.search} onClick={() => setMode('search')} />
      </div>
      <PlaceSheet open={mode !== 'closed'} focusSearch={mode === 'search'} onClose={() => setMode('closed')} />
    </header>
  );
}
