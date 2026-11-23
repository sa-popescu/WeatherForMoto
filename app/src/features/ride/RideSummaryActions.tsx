import { useState } from 'react';
import { useLang, useStrings, type Lang } from '../../lib/i18n';
import { noteUse } from '../../lib/usage';
import { useAuth } from '../../state/auth';
import { useToast } from '../../state/toast';
import { Button, cx } from '../../ui/primitives';
import { gpxFileName } from '../route/gpx';
import { fmtClockTime } from './logic/rideFormat';
import { buildRideGpx } from './logic/rideGpx';
import { summaryOf } from './logic/summary';
import { rideRecorder } from './recorder';
import { logRideToAccount } from './rideLog';
import { RIDE_S } from './strings';
import { useRide } from './useRide';

// Under a finished ride's summary: the GPX file, saving it to the account's
// ride log (signed in), and closing the screen. The ride stays on the phone
// whatever is chosen here.

/** "Tură liberă · 22 nov 09:30" or "Spre Sibiu · 22 nov 09:30". */
function rideTitle(routeName: string | null, startedAt: number, lang: Lang): string {
  const s = RIDE_S[lang];
  const day = new Date(startedAt).toLocaleDateString(lang === 'en' ? 'en-GB' : 'ro-RO', { day: 'numeric', month: 'short' }).replace('.', '');
  const head = routeName ? `${s.towards} ${routeName}` : s.entryTitle;
  return `${head} · ${day} ${fmtClockTime(startedAt, lang)}`;
}

function download(fileName: string, text: string): void {
  const url = URL.createObjectURL(new Blob([text], { type: 'application/gpx+xml' }));
  const link = document.createElement('a');
  link.href = url;
  link.download = fileName;
  document.body.appendChild(link);
  link.click();
  link.remove();
  window.setTimeout(() => URL.revokeObjectURL(url), 1000);
}

export function RideSummaryActions() {
  const s = useStrings(RIDE_S);
  const lang = useLang();
  const toast = useToast();
  const { token, status } = useAuth();
  const ride = useRide();
  const [saving, setSaving] = useState(false);
  const startedAt = ride.clock.startedAt ?? Date.now();
  const title = rideTitle(ride.route?.name ?? null, startedAt, lang);

  const exportGpx = (): void => {
    try {
      download(gpxFileName(title), buildRideGpx(title, rideRecorder.getPoints(), new Date(startedAt).toISOString()));
      toast(s.gpxOk, { tone: 'success' });
      noteUse('ride:gpx');
    } catch (err) {
      console.warn('[ride] GPX export failed', err);
      toast(s.gpxErr, { tone: 'error' });
    }
  };

  const saveToAccount = async (): Promise<void> => {
    const rideId = ride.rideId;
    if (!token || !rideId) return;
    setSaving(true);
    try {
      const summary = summaryOf(ride.totals, ride.clock);
      const id = await logRideToAccount(token, rideRecorder.getPoints(), summary, ride.route?.name ?? null, lang);
      rideRecorder.markLogged(rideId, id);
      toast(s.logOk, { tone: 'success' });
    } catch (err) {
      console.warn('[ride] could not save the ride to the account', err);
      toast(s.logErr, { tone: 'error' });
    } finally {
      setSaving(false);
    }
  };

  const signedIn = status === 'signed-in' && token !== null;
  return (
    <div className="ride-controls">
      <div className={cx('ride-controls__row', signedIn && 'ride-controls--two')}>
        <Button size="lg" full icon="download" onClick={exportGpx}>
          {s.exportGpx}
        </Button>
        {signedIn &&
          (ride.loggedId !== null ? (
            <Button size="lg" full icon="check" disabled>
              {s.logged}
            </Button>
          ) : (
            <Button size="lg" full icon="bookmark" busy={saving} onClick={() => void saveToAccount()}>
              {s.logRide}
            </Button>
          ))}
      </div>
      <Button variant="primary" size="lg" full icon="check" onClick={() => rideRecorder.close()}>
        {s.close}
      </Button>
    </div>
  );
}
