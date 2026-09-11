import { fmt, useStrings } from '../../lib/i18n';
import { Icon } from '../../ui/icons';
import { Card, IconButton } from '../../ui/primitives';
import { StopInput } from './StopInput';
import { addStop, MAX_STOPS, MIN_STOPS, moveItem, removeStop, roleOf, type StopRole } from './stops';
import { RS } from './strings';
import type { StopsState } from './useStopsState';

// 2 to 10 stops. Normal mode edits the places; "Ordine" mode shows move up,
// move down and remove buttons (no drag needed, works with gloves and keys).

type Strings = (typeof RS)['en'];

function fieldName(role: StopRole, index: number, s: Strings): string {
  if (role === 'origin') return s.fieldOrigin;
  if (role === 'destination') return s.fieldDestination;
  return fmt(s.fieldVia, { n: index + 1 });
}

const ROLE_WORD: Record<StopRole, 'roleOrigin' | 'roleVia' | 'roleDestination'> = {
  origin: 'roleOrigin',
  via: 'roleVia',
  destination: 'roleDestination',
};

const PLACEHOLDER: Record<StopRole, 'placeholderOrigin' | 'placeholderVia' | 'placeholderDestination'> = {
  origin: 'placeholderOrigin',
  via: 'placeholderVia',
  destination: 'placeholderDestination',
};

export function StopsEditor({ state }: { state: StopsState }) {
  const s = useStrings(RS);
  const { stops, setStops, reorder, setReorder, updateText, pickSuggestion, useMyLocation, locating } = state;
  const count = stops.length;

  return (
    <Card className="route-stops" aria-labelledby="route-stops-title">
      <div className="route-stops__head">
        <h2 className="eyebrow" id="route-stops-title">
          {s.stopsTitle}
        </h2>
        <button type="button" className="route-link-btn" aria-pressed={reorder} onClick={() => setReorder(!reorder)}>
          {reorder ? s.reorderDone : s.reorder}
        </button>
      </div>
      <ol className="route-stops__list">
        {stops.map((stop, index) => {
          const role = roleOf(index, count);
          const dotClass = `route-stop__dot--${role}`;
          const name = stop.place?.name ?? (stop.text.trim() || s.emptyStop);
          if (reorder) {
            return (
              <li key={stop.id} className="route-stop">
                <span className={`route-stop__dot ${dotClass}`} aria-hidden="true" />
                <span className="route-stop__field">
                  <span className="route-stop__role">{s[ROLE_WORD[role]]}</span>
                  <span className="route-stop__name">{name}</span>
                </span>
                <span className="route-stop__tools">
                  <IconButton
                    icon="chevronDown"
                    className="route-rot180"
                    label={fmt(s.moveUp, { name })}
                    disabled={index === 0}
                    onClick={() => setStops(moveItem(stops, index, index - 1))}
                  />
                  <IconButton
                    icon="chevronDown"
                    label={fmt(s.moveDown, { name })}
                    disabled={index === count - 1}
                    onClick={() => setStops(moveItem(stops, index, index + 1))}
                  />
                  <IconButton icon="close" label={fmt(s.removeStop, { name })} disabled={count <= MIN_STOPS} onClick={() => setStops(removeStop(stops, stop.id))} />
                </span>
              </li>
            );
          }
          return (
            <li key={stop.id}>
              <StopInput
                stop={stop}
                field={fieldName(role, index, s)}
                role={stop.gps ? `${s[ROLE_WORD[role]]} · ${s.myLocation}` : s[ROLE_WORD[role]]}
                placeholder={s[PLACEHOLDER[role]]}
                dotClass={dotClass}
                onText={(text) => updateText(stop.id, text)}
                onPick={(hit) => pickSuggestion(stop.id, hit)}
                trailing={
                  <IconButton
                    icon="locate"
                    tone={stop.gps ? 'accent' : 'default'}
                    label={fmt(s.useMyLocationFor, { field: fieldName(role, index, s) })}
                    aria-busy={locating || undefined}
                    disabled={locating}
                    onClick={() => void useMyLocation(stop.id)}
                  />
                }
              />
            </li>
          );
        })}
      </ol>
      <button type="button" className="route-add" disabled={count >= MAX_STOPS} onClick={() => setStops(addStop(stops))}>
        <Icon name="plus" size={18} strokeWidth={2} />
        {s.addStop}
        <span className="route-add__max">{fmt(s.maxStops, { n: MAX_STOPS })}</span>
      </button>
    </Card>
  );
}
