import { useEffect, useState } from 'react';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import type { SavedRoute } from '../../lib/types';
import { CORE } from '../../i18n/core';
import { Sheet } from '../../ui/Sheet';
import { Button, IconButton, Skeleton } from '../../ui/primitives';
import { AS } from './actionStrings';
import { fmtKm } from './routeFormat';
import type { Loadable } from './useAccountData';

// Saved routes: tap to load (fills the stops and recalculates), delete with
// a confirmation shown in the same sheet (no stacked dialogs).

interface SavedRoutesSheetProps {
  open: boolean;
  onClose: () => void;
  saved: Loadable<SavedRoute[]>;
  onRetry: () => void;
  onLoad: (route: SavedRoute) => void;
  onDelete: (route: SavedRoute) => Promise<boolean>;
}

export function SavedRoutesSheet({ open, onClose, saved, onRetry, onLoad, onDelete }: SavedRoutesSheetProps) {
  const as = useStrings(AS);
  const core = useStrings(CORE);
  const lang = useLang();
  const [confirming, setConfirming] = useState<SavedRoute | null>(null);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!open) setConfirming(null);
  }, [open]);

  if (confirming) {
    const remove = async (): Promise<void> => {
      setBusy(true);
      const ok = await onDelete(confirming);
      setBusy(false);
      if (ok) setConfirming(null);
    };
    return (
      <Sheet
        open={open}
        onClose={() => setConfirming(null)}
        title={as.deleteTitle}
        footer={
          <>
            <Button onClick={() => setConfirming(null)}>{core.cancel}</Button>
            <Button variant="danger" icon="trash" busy={busy} onClick={() => void remove()}>
              {core.delete}
            </Button>
          </>
        }
      >
        <p>{fmt(as.deleteBody, { name: confirming.name })}</p>
      </Sheet>
    );
  }

  const list = saved.data ?? [];
  return (
    <Sheet open={open} onClose={onClose} title={as.savedTitle}>
      {saved.status === 'loading' && list.length === 0 && <Skeleton height={56} radius={14} />}
      {saved.status === 'error' && (
        <div className="route-inline-error">
          <p className="route-note">{as.savedErr}</p>
          <Button icon="refresh" onClick={onRetry}>
            {core.retry}
          </Button>
        </div>
      )}
      {saved.status === 'ready' && list.length === 0 && <p className="route-note">{as.savedEmpty}</p>}
      {list.length > 0 && (
        <ul className="route-saved">
          {list.map((route) => (
            <li key={route.id} className="route-saved__row">
              <button
                type="button"
                className="route-saved__load"
                aria-label={fmt(as.savedLoad, { name: route.name })}
                onClick={() => {
                  onClose();
                  onLoad(route);
                }}
              >
                <span className="route-saved__name">{route.name}</span>
                <span className="route-saved__meta">
                  {route.stops.join(' → ')}
                  {route.total_distance_km != null && ` · ${fmtKm(route.total_distance_km, lang)} km`}
                </span>
              </button>
              <IconButton icon="trash" label={fmt(as.savedDelete, { name: route.name })} onClick={() => setConfirming(route)} />
            </li>
          ))}
        </ul>
      )}
    </Sheet>
  );
}
