import { useEffect, useRef } from 'react';
import { CORE } from '../../i18n/core';
import { useStrings } from '../../lib/i18n';
import { Button } from '../../ui/primitives';
import { MAP_STRINGS } from './strings';

// Shown while choosing where a hazard is: the map centre (under the
// crosshair) becomes the report position. Escape cancels.

interface Props {
  onCancel: () => void;
  onConfirm: () => void;
}

export function PickBar({ onCancel, onConfirm }: Props) {
  const s = useStrings(MAP_STRINGS);
  const core = useStrings(CORE);
  const cancel = useRef(onCancel);
  cancel.current = onCancel;

  useEffect(() => {
    const onKey = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') cancel.current();
    };
    document.addEventListener('keydown', onKey);
    return () => document.removeEventListener('keydown', onKey);
  }, []);

  return (
    <div className="map-panel map-pickbar" role="group" aria-label={s.reportTitle}>
      <p className="map-pickbar__hint">{s.pickHint}</p>
      <p className="map-pickbar__sub">{s.pickLongPress}</p>
      <div className="map-pickbar__actions">
        <Button size="lg" onClick={onCancel}>
          {core.cancel}
        </Button>
        {/* Focus lands on the confirm button so keyboard users can act at once. */}
        <Button size="lg" variant="primary" icon="check" autoFocus onClick={onConfirm}>
          {s.pickConfirm}
        </Button>
      </div>
    </div>
  );
}
