import { fmt, useStrings } from '../../../lib/i18n';
import { usePlace } from '../../../state/place';
import { Icon } from '../../../ui/icons';
import { Banner, Button, Card } from '../../../ui/primitives';
import { cityForPrefs } from '../lib/validation';
import { ALERTS } from '../strings/alerts';
import type { AlertCardProps } from './AlertsSection';

// About 1 km: closer than that counts as the same place.
const SAME_PLACE_DEG = 0.01;

function roundCoord(value: number): number {
  return Math.round(value * 10_000) / 10_000;
}

/**
 * The alert location changes only when the user asks for it here (the legacy
 * app silently overwrote it with whatever place was on screen).
 */
export function AlertLocation({ prefs, update }: AlertCardProps) {
  const s = useStrings(ALERTS);
  const { place } = usePlace();

  const lat = prefs.home_lat;
  const lon = prefs.home_lon;
  const hasHome = lat != null && lon != null;
  const same = hasHome && Math.abs(lat - place.lat) < SAME_PLACE_DEG && Math.abs(lon - place.lon) < SAME_PLACE_DEG;
  const label = prefs.city ?? (hasHome ? `${lat.toFixed(3)}, ${lon.toFixed(3)}` : s.locationNone);

  const useCurrent = (): void => {
    const city = cityForPrefs(place.name);
    update(
      { home_lat: roundCoord(place.lat), home_lon: roundCoord(place.lon), city },
      { successMessage: fmt(s.locationSaved, { city: city ?? place.name }) },
    );
  };

  return (
    <Card className="acct-stack">
      <div className="acct-loc">
        <span className="acct-loc__icon" aria-hidden="true">
          <Icon name="pin" size={22} />
        </span>
        <div className="acct-loc__text">
          <span className="eyebrow">{s.locationTitle}</span>
          <strong className="acct-loc__city num">{label}</strong>
        </div>
      </div>
      {!hasHome && (
        <Banner tone="warn" icon="alert">
          {s.locationMissing}
        </Banner>
      )}
      {same ? (
        <p className="acct-hint">{s.locationSame}</p>
      ) : (
        <Button full icon="locate" onClick={useCurrent}>
          {`${s.locationUse}: ${place.name}`}
        </Button>
      )}
    </Card>
  );
}
