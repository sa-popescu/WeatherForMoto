import { useEffect, useRef } from 'react';
import { useStrings } from '../../lib/i18n';
import { usePlace } from '../../state/place';
import { useToast } from '../../state/toast';
import { Icon } from '../../ui/icons';
import { Spinner } from '../../ui/primitives';
import { geoMessage } from '../place/geoMessage';
import { S_PLACE } from '../place/strings';
import { DRIVE_S } from './strings';

// "Locația mea" on the Drum map, where map apps keep it: above the card on a
// phone, in the map's corner on a desktop (DriveMapButtons.tsx). A tap
// asks for the position (and the permission, the first time), then brings the
// map back to it. The first fix also makes the app open on the rider's
// position from then on (state/place.tsx). Lit while the map shows the rider.

interface Props {
  /** Brings the map back to the place on screen. */
  onRecenter: () => void;
}

export function DriveLocate({ onRecenter }: Props) {
  const s = useStrings(DRIVE_S);
  const sPlace = useStrings(S_PLACE);
  const toast = useToast();
  const { source, locate, locating, locateError } = usePlace();
  const asked = useRef(false);
  const onYou = source === 'gps';

  // Only a tap here reports a failure: the silent fix at start-up stays silent.
  useEffect(() => {
    if (!asked.current || !locateError) return;
    asked.current = false;
    toast(geoMessage(locateError, sPlace), { tone: 'error' });
  }, [locateError, sPlace, toast]);

  const onTap = async (): Promise<void> => {
    // Already on the rider: back to the last fix straight away, then freshen it.
    if (onYou) onRecenter();
    asked.current = true;
    if (await locate()) {
      asked.current = false;
      onRecenter();
    }
  };

  return (
    <button
      type="button"
      className="icon-btn drive-locate"
      aria-label={s.locate}
      title={s.locate}
      aria-pressed={onYou}
      aria-busy={locating || undefined}
      disabled={locating}
      onClick={() => void onTap()}
    >
      {locating ? <Spinner size={20} /> : <Icon name="locate" size={22} />}
    </button>
  );
}
