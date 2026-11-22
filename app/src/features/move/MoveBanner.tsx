import { useState } from 'react';
import { defineStrings, useStrings } from '../../lib/i18n';
import { Button } from '../../ui/primitives';
import { moveLink } from './move';

// On the old address only: the app lives at motonor.bluemouse.cc now. One tap
// carries this phone's settings, places and rides there (move.ts).

const MOVE_S = defineStrings(
  {
    title: 'Motonor are adresă nouă: motonor.bluemouse.cc',
    lead: 'Mută-te acolo cu setările, locurile și turele de pe acest telefon. Contul îl deschizi din nou acolo.',
    iphone: 'Pe iPhone: după mutare, adaug-o pe ecranul principal din Safari (Partajează, apoi „Adaugă pe ecranul principal”) și șterge iconița veche.',
    move: 'Mută-te pe adresa nouă',
    later: 'Mai târziu',
    ridesLeft: 'Turele sunt prea multe pentru link și rămân aici; exportă-le în GPX din „Ture” înainte.',
  },
  {
    title: 'Motonor has a new address: motonor.bluemouse.cc',
    lead: 'Move there with this phone’s settings, places and rides. Sign in again there.',
    iphone: 'On iPhone: after moving, add it to the Home Screen from Safari (Share, then “Add to Home Screen”) and remove the old icon.',
    move: 'Move to the new address',
    later: 'Later',
    ridesLeft: 'There are too many rides for the link and they stay here; export them as GPX from “Rides” first.',
  },
);

export function MoveBanner() {
  const s = useStrings(MOVE_S);
  const [busy, setBusy] = useState(false);
  const [ridesLeft, setRidesLeft] = useState(false);
  // "Later" hides it until the app is opened again.
  const [hidden, setHidden] = useState(false);
  if (hidden) return null;

  const move = async (): Promise<void> => {
    setBusy(true);
    try {
      const link = await moveLink();
      if (link.ridesLeft && !ridesLeft) {
        // Said once; a second tap moves without the rides.
        setRidesLeft(true);
        return;
      }
      window.location.href = link.url;
    } catch (err) {
      console.warn('[move] the move link could not be made', err);
      window.location.href = 'https://motonor.bluemouse.cc/';
    } finally {
      setBusy(false);
    }
  };

  return (
    <div className="move-banner" role="region" aria-label={s.title}>
      <p className="move-banner__title">{s.title}</p>
      <p className="move-banner__lead">{s.lead}</p>
      <p className="move-banner__lead">{s.iphone}</p>
      {ridesLeft && <p className="move-banner__warn">{s.ridesLeft}</p>}
      <div className="move-banner__actions">
        <Button variant="primary" full icon="external" busy={busy} onClick={() => void move()}>
          {s.move}
        </Button>
        <Button variant="ghost" onClick={() => setHidden(true)}>
          {s.later}
        </Button>
      </div>
    </div>
  );
}
