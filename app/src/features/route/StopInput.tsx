import { useId, useState, type KeyboardEvent, type ReactNode } from 'react';
import type { PlaceSuggestion } from '../../lib/geo';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { cx } from '../../ui/primitives';
import { RS } from './strings';
import type { StopDraft } from './types';
import { usePlaceSearch } from './usePlaceSearch';

// One stop row: a combobox with debounced place suggestions (ARIA 1.2
// pattern: arrows move, Enter picks, Escape closes).

interface StopInputProps {
  stop: StopDraft;
  /** Accessible field name: "Plecare", "Oprirea 2", "Destinație". */
  field: string;
  /** Small role word shown above the text. */
  role: string;
  placeholder: string;
  dotClass: string;
  onText: (text: string) => void;
  onPick: (hit: PlaceSuggestion) => void;
  trailing?: ReactNode;
}

export function StopInput({ stop, field, role, placeholder, dotClass, onText, onPick, trailing }: StopInputProps) {
  const s = useStrings(RS);
  const lang = useLang();
  const id = useId();
  const listId = `${id}-list`;
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(-1);
  const search = usePlaceSearch(stop.text, lang, open && !stop.place);
  const results = search.results;
  const showList = open && !stop.place && search.status !== 'idle';

  const choose = (hit: PlaceSuggestion): void => {
    onPick(hit);
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
    } else if (event.key === 'Escape' && showList) {
      event.preventDefault();
      setOpen(false);
    }
  };

  return (
    <div className="route-stop">
      <span className={cx('route-stop__dot', dotClass)} aria-hidden="true" />
      <div className="route-stop__field">
        <span className="route-stop__role" aria-hidden="true">
          {role}
        </span>
        <input
          id={id}
          className="route-stop__input"
          value={stop.text}
          placeholder={placeholder}
          role="combobox"
          aria-label={field}
          aria-expanded={showList}
          aria-controls={showList ? listId : undefined}
          aria-autocomplete="list"
          aria-activedescendant={showList && active >= 0 ? `${listId}-${active}` : undefined}
          autoComplete="off"
          spellCheck={false}
          enterKeyHint="search"
          onChange={(e) => {
            onText(e.target.value);
            setOpen(true);
            setActive(-1);
          }}
          onFocus={() => setOpen(true)}
          onBlur={() => setOpen(false)}
          onKeyDown={onKeyDown}
        />
      </div>
      {trailing}
      {showList && (
        <ul id={listId} role="listbox" className="route-suggest" aria-label={fmt(s.suggestionsFor, { field })}>
          {results.map((hit, i) => (
            <li
              key={`${hit.lat},${hit.lon},${i}`}
              id={`${listId}-${i}`}
              role="option"
              aria-selected={i === active}
              className={cx('route-suggest__item', i === active && 'route-suggest__item--active')}
              // Keep focus in the input so the list does not close before the click lands.
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => choose(hit)}
            >
              <span className="route-suggest__name">{hit.name}</span>
              <span className="route-suggest__meta">{[hit.region, hit.country].filter(Boolean).join(', ')}</span>
            </li>
          ))}
          {results.length === 0 && (
            <li className="route-suggest__note" role="option" aria-selected={false} aria-disabled="true">
              {search.status === 'loading' ? s.searching : search.status === 'error' ? s.searchFailed : s.noSuggestions}
            </li>
          )}
        </ul>
      )}
    </div>
  );
}
