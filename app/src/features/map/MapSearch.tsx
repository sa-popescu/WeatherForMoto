import { useId, useState, type KeyboardEvent } from 'react';
import type { PlaceSuggestion } from '../../lib/geo';
import { useLang, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { cx, Spinner } from '../../ui/primitives';
import { usePlaceSearch } from '../place/usePlaceSearch';
import { MAP_STRINGS } from './strings';

// The search field at the top of the desktop map panel: a combobox with the
// same suggestions as the place search (ARIA 1.2 pattern: arrows move, Enter
// picks, Escape closes). Picking a place flies the map there and shows its
// weather; it does not change the app's place.

interface MapSearchProps {
  /** Results near this point rank first (the middle of the map). */
  near: { lat: number; lon: number } | null;
  /** The map is still loading: the spinner takes the place of the icon. */
  loading: boolean;
  onPick: (hit: PlaceSuggestion) => void;
}

export function MapSearch({ near, loading, onPick }: MapSearchProps) {
  const s = useStrings(MAP_STRINGS);
  const lang = useLang();
  const id = useId();
  const listId = `${id}-list`;
  const [query, setQuery] = useState('');
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(-1);
  const search = usePlaceSearch(query, lang, near ?? undefined);
  const results = search.results;
  const showList = open && search.status !== 'idle';

  const choose = (hit: PlaceSuggestion): void => {
    onPick(hit);
    setQuery('');
    setOpen(false);
    setActive(-1);
  };

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>): void => {
    if (event.key === 'ArrowDown' && results.length > 0) {
      event.preventDefault();
      setOpen(true);
      setActive((a) => (a + 1) % results.length);
    } else if (event.key === 'ArrowUp' && results.length > 0) {
      event.preventDefault();
      setActive((a) => (a <= 0 ? results.length - 1 : a - 1));
    } else if (event.key === 'Enter' && showList && results.length > 0) {
      event.preventDefault();
      choose(results[Math.max(0, active)]);
    } else if (event.key === 'Escape') {
      event.preventDefault();
      if (showList) setOpen(false);
      else setQuery('');
    }
  };

  return (
    <div className="map-search">
      <label className="map-search__field" htmlFor={id}>
        <span className="map-search__icon" aria-hidden="true">
          {loading ? <Spinner size={18} /> : <Icon name="search" size={18} />}
        </span>
        <input
          id={id}
          className="map-search__input"
          type="search"
          value={query}
          placeholder={s.searchMap}
          role="combobox"
          aria-label={s.searchMap}
          aria-expanded={showList}
          aria-controls={showList ? listId : undefined}
          aria-autocomplete="list"
          aria-activedescendant={showList && active >= 0 ? `${listId}-${active}` : undefined}
          autoComplete="off"
          spellCheck={false}
          enterKeyHint="search"
          onChange={(e) => {
            setQuery(e.target.value);
            setOpen(true);
            setActive(-1);
          }}
          onFocus={() => setOpen(true)}
          onBlur={() => setOpen(false)}
          onKeyDown={onKeyDown}
        />
      </label>
      {showList && (
        <ul id={listId} role="listbox" className="map-search__list" aria-label={s.searchResults}>
          {results.map((hit, i) => (
            <li
              key={`${hit.lat},${hit.lon},${i}`}
              id={`${listId}-${i}`}
              role="option"
              aria-selected={i === active}
              className={cx('map-search__item', i === active && 'map-search__item--active')}
              // Keep focus in the input so the list does not close before the click lands.
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => choose(hit)}
            >
              <span className="map-search__name">{hit.name}</span>
              <span className="map-search__meta">{[hit.region, hit.country].filter(Boolean).join(', ')}</span>
            </li>
          ))}
          {results.length === 0 && (
            <li className="map-search__note" role="option" aria-selected={false} aria-disabled="true">
              {search.status === 'loading' ? s.searching : search.status === 'error' ? s.searchFailed : s.searchNone}
            </li>
          )}
        </ul>
      )}
    </div>
  );
}
