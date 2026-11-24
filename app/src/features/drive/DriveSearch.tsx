import { useEffect, useId, useRef, useState, type KeyboardEvent } from 'react';
import type { NearPoint } from '../../lib/geo';
import { useLang, useStrings } from '../../lib/i18n';
import type { Place } from '../../lib/types';
import { Icon } from '../../ui/icons';
import { cx } from '../../ui/primitives';
import { usePlaceSearch } from '../route/usePlaceSearch';
import type { DrivePlaces, Shortcut } from './places';
import { DRIVE_S } from './strings';

// "Unde mergi?": one search box with place suggestions (ARIA 1.2 combobox:
// arrows move, Enter picks, Escape closes) and, under it, the quick
// destinations: Acasă, Serviciu and the recent ones. A quick destination that
// is not set yet turns the box into a search for its address. The way to the
// planner (stops, another day, a GPX) is pinned at the right of that row: with
// a few recent places it used to sit past the edge, where nobody found it.

export type SearchPurpose = 'go' | Shortcut;

/** A row of the list: a place to go to, or a change to Acasă or Serviciu. */
interface Option {
  key: string;
  label: string;
  meta: string;
  place: Place | null;
  change?: Shortcut;
}

interface DriveSearchProps {
  destination: Place | null;
  purpose: SearchPurpose;
  places: DrivePlaces;
  near: NearPoint;
  onPick: (place: Place, purpose: SearchPurpose) => void;
  onClear: () => void;
  onPurpose: (purpose: SearchPurpose) => void;
}

export function DriveSearch({ destination, purpose, places, near, onPick, onClear, onPurpose }: DriveSearchProps) {
  const s = useStrings(DRIVE_S);
  const lang = useLang();
  const id = useId();
  const listId = `${id}-list`;
  const inputRef = useRef<HTMLInputElement>(null);
  const [text, setText] = useState(destination?.name ?? '');
  const [open, setOpen] = useState(false);
  const [active, setActive] = useState(-1);
  const typing = text.trim().length >= 2 && text !== destination?.name;
  const search = usePlaceSearch(text, lang, open && typing, near);

  useEffect(() => setText(destination?.name ?? ''), [destination]);

  // Setting Acasă or Serviciu starts in the box, ready to type.
  useEffect(() => {
    if (purpose !== 'go') inputRef.current?.focus();
  }, [purpose]);

  const shortcut = (which: Shortcut, place: Place | null): Option | null =>
    place ? { key: which, label: which === 'home' ? s.home : s.work, meta: place.name, place } : null;
  const quick: Option[] = [
    shortcut('home', places.home),
    shortcut('work', places.work),
    ...places.recent.map((p, i) => ({ key: `r${i}`, label: p.name, meta: s.recent, place: p })),
    places.home ? { key: 'change-home', label: s.changeHome, meta: '', place: null, change: 'home' as const } : null,
    places.work ? { key: 'change-work', label: s.changeWork, meta: '', place: null, change: 'work' as const } : null,
  ].filter((o): o is Option => o !== null);
  const options: Option[] = typing
    ? search.results.map((hit, i) => ({ key: `s${i}`, label: hit.name, meta: [hit.region, hit.country].filter(Boolean).join(', '), place: { name: hit.name, lat: hit.lat, lon: hit.lon } }))
    : purpose === 'go'
      ? quick
      : [];
  const showList = open && (options.length > 0 || typing);

  const choose = (option: Option): void => {
    setActive(-1);
    if (option.change) {
      // The box stays open, now asking for the new address.
      setText('');
      onPurpose(option.change);
      return;
    }
    if (!option.place) return;
    onPick(option.place, purpose);
    setOpen(false);
    inputRef.current?.blur();
  };

  const onKeyDown = (event: KeyboardEvent<HTMLInputElement>): void => {
    if (event.key === 'ArrowDown' && options.length > 0) {
      event.preventDefault();
      setOpen(true);
      setActive((a) => (a + 1) % options.length);
    } else if (event.key === 'ArrowUp' && options.length > 0) {
      event.preventDefault();
      setActive((a) => (a <= 0 ? options.length - 1 : a - 1));
    } else if (event.key === 'Enter' && showList && options.length > 0) {
      event.preventDefault();
      choose(options[Math.max(0, active)]);
    } else if (event.key === 'Escape') {
      setOpen(false);
      if (purpose !== 'go') onPurpose('go');
    }
  };

  const placeholder = purpose === 'home' ? s.searchHome : purpose === 'work' ? s.searchWork : s.searchPlaceholder;
  const chips: Array<{ key: string; label: string; set: boolean; onClick: () => void }> = [
    { key: 'home', label: places.home ? s.home : s.setHome, set: places.home !== null, onClick: () => (places.home ? onPick(places.home, 'go') : onPurpose('home')) },
    { key: 'work', label: places.work ? s.work : s.setWork, set: places.work !== null, onClick: () => (places.work ? onPick(places.work, 'go') : onPurpose('work')) },
    ...places.recent.slice(0, 4).map((p, i) => ({ key: `r${i}`, label: p.name, set: true, onClick: () => onPick(p, 'go') })),
  ];

  return (
    <div className="drive-search">
      <div className="drive-search__box">
        <Icon name={purpose === 'go' ? 'search' : purpose === 'home' ? 'pin' : 'flag'} size={20} />
        <input
          ref={inputRef}
          className="drive-search__input"
          value={text}
          placeholder={placeholder}
          role="combobox"
          aria-label={s.searchLabel}
          aria-expanded={showList}
          aria-controls={showList ? listId : undefined}
          aria-autocomplete="list"
          aria-activedescendant={showList && active >= 0 ? `${listId}-${active}` : undefined}
          autoComplete="off"
          spellCheck={false}
          enterKeyHint="search"
          onChange={(e) => {
            setText(e.target.value);
            setOpen(true);
            setActive(-1);
          }}
          onFocus={(e) => {
            e.target.select();
            setOpen(true);
          }}
          onBlur={() => setOpen(false)}
          onKeyDown={onKeyDown}
        />
        {purpose !== 'go' ? (
          <button type="button" className="drive-search__clear" onMouseDown={(e) => e.preventDefault()} onClick={() => onPurpose('go')}>
            {s.cancel}
          </button>
        ) : (
          (destination || text) && (
            <button
              type="button"
              className="drive-search__clear"
              aria-label={s.clear}
              onClick={() => {
                setText('');
                onClear();
              }}
            >
              <Icon name="close" size={18} />
            </button>
          )
        )}
      </div>

      {showList && (
        <ul id={listId} role="listbox" className="drive-suggest" aria-label={typing ? s.suggestions : s.shortcuts}>
          {options.map((o, i) => (
            <li
              key={o.key}
              id={`${listId}-${i}`}
              role="option"
              aria-selected={i === active}
              className={cx('drive-suggest__item', i === active && 'drive-suggest__item--active', o.change && 'drive-suggest__item--change')}
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => choose(o)}
            >
              <span className="drive-suggest__name">{o.label}</span>
              {o.meta && <span className="drive-suggest__meta">{o.meta}</span>}
            </li>
          ))}
          {typing && options.length === 0 && (
            <li className="drive-suggest__note" role="option" aria-selected={false} aria-disabled="true">
              {search.status === 'error' ? s.searchFailed : search.status === 'ready' ? s.noSuggestions : s.searching}
            </li>
          )}
        </ul>
      )}

      {!showList && !destination && purpose === 'go' && (
        <div className="drive-chips">
          <div className="drive-chips__scroll" role="group" aria-label={s.shortcuts}>
            {chips.map((c) => (
              <button key={c.key} type="button" className={cx('drive-chip', !c.set && 'drive-chip--unset')} onClick={c.onClick}>
                {c.key === 'home' && <Icon name="pin" size={16} />}
                {c.key === 'work' && <Icon name="flag" size={16} />}
                <span className="drive-chip__label">{c.label}</span>
              </button>
            ))}
          </div>
          {/* The planner, for a ride with stops, another day or a GPX: always in sight. */}
          <button type="button" className="drive-chip drive-chip--plan" aria-label={s.planChipAria} onClick={() => (window.location.hash = '#/traseu')}>
            <Icon name="route" size={16} />
            <span className="drive-chip__label">{s.planChip}</span>
          </button>
        </div>
      )}
    </div>
  );
}
