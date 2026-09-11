import { useEffect, useId, useRef, useState } from 'react';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import type { Place } from '../../lib/types';
import { usePlace, type PlaceSource } from '../../state/place';
import { useToast } from '../../state/toast';
import { Icon } from '../../ui/icons';
import { Sheet } from '../../ui/Sheet';
import { Spinner } from '../../ui/primitives';
import { geoMessage } from './geoMessage';
import { CurrentPlace, FavoriteList, SearchResults } from './PlaceLists';
import { sharePlace } from './share';
import { S_PLACE } from './strings';
import { MIN_QUERY_CHARS, usePlaceSearch } from './usePlaceSearch';

interface PlaceSheetProps {
  open: boolean;
  /** Focus the search box on open (the search button); the city name only browses. */
  focusSearch: boolean;
  onClose: () => void;
}

export function PlaceSheet({ open, focusSearch, onClose }: PlaceSheetProps) {
  const s = useStrings(S_PLACE);
  const lang = useLang();
  const toast = useToast();
  const { place, setPlace, favorites, isFavorite, toggleFavorite, removeFavorite, locate, locating, locateError } = usePlace();
  const [query, setQuery] = useState('');
  const [askedHere, setAskedHere] = useState(false);
  const search = usePlaceSearch(query, lang);
  const inputRef = useRef<HTMLInputElement>(null);
  const inputId = useId();

  // Runs after the sheet's own focus handling, so the input wins when asked.
  useEffect(() => {
    if (!open) {
      setQuery('');
      setAskedHere(false);
      return;
    }
    if (focusSearch) inputRef.current?.focus();
  }, [open, focusSearch]);

  const choose = (p: Place, source: PlaceSource): void => {
    setPlace(p, source);
    onClose();
  };

  const onLocate = async (): Promise<void> => {
    setAskedHere(true);
    if (await locate()) onClose();
  };

  const onShare = async (): Promise<void> => {
    try {
      const outcome = await sharePlace(place.name, fmt(s.shareText, { name: place.name }));
      if (outcome === 'copied') toast(s.linkCopied, { tone: 'success' });
    } catch (err) {
      console.warn('[place] share failed', err);
      toast(s.shareFailed, { tone: 'error' });
    }
  };

  const searching = (query.trim().split(',')[0] ?? '').trim().length >= MIN_QUERY_CHARS;

  return (
    <Sheet open={open} onClose={onClose} title={s.sheetTitle}>
      <div className="field">
        <label className="field__label" htmlFor={inputId}>
          {s.searchLabel}
        </label>
        <input
          ref={inputRef}
          id={inputId}
          className="field__input"
          type="search"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Enter' && search.results[0]) choose(search.results[0], 'search');
          }}
          autoComplete="off"
          autoCorrect="off"
          spellCheck={false}
          enterKeyHint="search"
          aria-describedby={`${inputId}-hint`}
        />
        <span id={`${inputId}-hint`} className="field__hint">
          {s.searchHint}
        </span>
      </div>

      {searching ? (
        <SearchResults state={search} query={query.trim()} onChoose={(p) => choose(p, 'search')} />
      ) : (
        <>
          <button type="button" className="place-row" onClick={() => void onLocate()} disabled={locating}>
            {locating ? <Spinner size={22} /> : <Icon name="locate" size={22} />}
            <span className="place-row__text">{locating ? s.locating : s.locate}</span>
          </button>
          {askedHere && locateError && (
            <p className="place-error" role="alert">
              {geoMessage(locateError, s)}
            </p>
          )}
          <CurrentPlace place={place} favorite={isFavorite(place)} onToggle={() => toggleFavorite(place)} onShare={() => void onShare()} />
          <FavoriteList favorites={favorites} current={place} onChoose={(p) => choose(p, 'favorite')} onRemove={removeFavorite} />
        </>
      )}
    </Sheet>
  );
}
