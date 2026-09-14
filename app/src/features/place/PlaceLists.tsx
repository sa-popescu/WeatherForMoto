import type { PlaceSuggestion } from '../../lib/geo';
import { fmt, useStrings } from '../../lib/i18n';
import type { Place } from '../../lib/types';
import { samePlace } from '../../state/place';
import { Icon } from '../../ui/icons';
import { IconButton, Spinner } from '../../ui/primitives';
import { S_PLACE } from './strings';
import type { PlaceSearchState } from './usePlaceSearch';

export function SearchResults({ state, query, onChoose }: { state: PlaceSearchState; query: string; onChoose: (p: PlaceSuggestion) => void }) {
  const s = useStrings(S_PLACE);
  return (
    <section className="place-section" aria-label={s.results} aria-busy={state.status === 'loading'}>
      <p className="place-status" role="status">
        {state.status === 'loading' && (
          <>
            <Spinner size={16} />
            {s.searching}
          </>
        )}
        {state.status === 'done' && state.results.length === 0 && fmt(s.noResults, { q: query })}
        {state.status === 'error' && <span className="place-error">{s.searchError}</span>}
      </p>
      {state.results.length > 0 && (
        <ul className="place-list">
          {state.results.map((r) => (
            <li key={`${r.name}|${r.lat},${r.lon}`}>
              <button type="button" className="place-row" onClick={() => onChoose(r)}>
                <Icon name="pin" size={22} />
                <span className="place-row__text">
                  <span className="place-row__name">{r.name}</span>
                  <span className="place-row__sub">{[r.region, r.country].filter(Boolean).join(', ')}</span>
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
      {state.status === 'done' && state.results.length > 0 && <p className="place-credit">{s.searchCredit}</p>}
    </section>
  );
}

export function CurrentPlace({ place, favorite, onToggle, onShare }: { place: Place; favorite: boolean; onToggle: () => void; onShare: () => void }) {
  const s = useStrings(S_PLACE);
  return (
    <div className="place-current">
      <div className="place-current__text">
        <span className="eyebrow">{s.current}</span>
        <span className="place-current__name num">{place.name}</span>
      </div>
      <IconButton
        icon={favorite ? 'starFilled' : 'star'}
        label={favorite ? s.unfavorite : s.favorite}
        tone={favorite ? 'accent' : 'default'}
        aria-pressed={favorite}
        onClick={onToggle}
      />
      <IconButton icon="share" label={s.share} onClick={onShare} />
    </div>
  );
}

interface FavoriteListProps {
  favorites: Place[];
  current: Place;
  onChoose: (p: Place) => void;
  onRemove: (p: Place) => void;
}

export function FavoriteList({ favorites, current, onChoose, onRemove }: FavoriteListProps) {
  const s = useStrings(S_PLACE);
  return (
    <section className="place-section" aria-labelledby="place-favs-title">
      <h3 id="place-favs-title" className="eyebrow">
        {s.favorites}
      </h3>
      {favorites.length === 0 ? (
        <p className="place-empty">{s.noFavorites}</p>
      ) : (
        <ul className="place-list">
          {favorites.map((f) => (
            <li key={`${f.name}-${f.lat}`} className="place-fav">
              <button type="button" className="place-row" onClick={() => onChoose(f)} aria-current={samePlace(f, current) ? 'true' : undefined}>
                <Icon name="starFilled" size={20} />
                <span className="place-row__text">
                  <span className="place-row__name">{f.name}</span>
                </span>
              </button>
              <IconButton icon="trash" label={fmt(s.removeFavorite, { name: f.name })} onClick={() => onRemove(f)} />
            </li>
          ))}
        </ul>
      )}
    </section>
  );
}
