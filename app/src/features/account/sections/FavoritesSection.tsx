import { useRef } from 'react';
import { fmt, useStrings } from '../../../lib/i18n';
import type { Place } from '../../../lib/types';
import { samePlace, usePlace } from '../../../state/place';
import { useToast } from '../../../state/toast';
import { Icon } from '../../../ui/icons';
import { Card, IconButton } from '../../../ui/primitives';
import { APP } from '../strings/app';
import { Group } from '../ui/controls';
import { WeekendRanking } from './WeekendRanking';

export function FavoritesSection() {
  const s = useStrings(APP);
  const { favorites, removeFavorite, toggleFavorite, isFavorite, setPlace, place } = usePlace();
  const toast = useToast();
  // The undo runs seconds later: read the latest callbacks, not the ones captured at removal.
  const latest = useRef({ toggleFavorite, isFavorite });
  latest.current = { toggleFavorite, isFavorite };

  const remove = (p: Place): void => {
    removeFavorite(p);
    toast(fmt(s.favRemoved, { name: p.name }), {
      actionLabel: s.undo,
      onAction: () => {
        if (!latest.current.isFavorite(p)) latest.current.toggleFavorite(p);
      },
    });
  };

  const show = (p: Place): void => {
    setPlace(p, 'favorite');
    toast(fmt(s.favShown, { name: p.name }), {
      actionLabel: s.favSee,
      onAction: () => {
        window.location.hash = '#/acum';
      },
    });
  };

  return (
    <Group title={s.favSection}>
      <Card className="acct-favs-card">
        {favorites.length === 0 ? (
          <p className="acct-hint acct-empty">{s.favEmpty}</p>
        ) : (
          <ul className="acct-favs">
            {favorites.map((p) => {
              const current = samePlace(p, place);
              return (
                <li key={`${p.name}|${p.lat}|${p.lon}`} className="acct-fav">
                  <button type="button" className="acct-fav__main" onClick={() => show(p)} aria-label={fmt(s.favShow, { name: p.name })}>
                    <Icon name="starFilled" size={20} />
                    <span className="acct-fav__name">{p.name}</span>
                    {current && <span className="acct-tag">{s.favCurrent}</span>}
                  </button>
                  <IconButton icon="trash" label={fmt(s.favRemove, { name: p.name })} onClick={() => remove(p)} />
                </li>
              );
            })}
          </ul>
        )}
      </Card>
      <WeekendRanking />
    </Group>
  );
}
