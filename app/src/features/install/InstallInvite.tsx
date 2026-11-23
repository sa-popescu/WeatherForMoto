import { lazy, Suspense, useEffect, useId, useMemo, useState } from 'react';
import { useStrings } from '../../lib/i18n';
import { useToast } from '../../state/toast';
import { Button } from '../../ui/primitives';
import { getInstallState, promptInstall, useInstallState } from '../account/installPrompt';
import { afterShowing, inviteDue, readInvite, writeInvite } from './inviteState';
import { canInstallHere, currentPlatform } from './platform';
import { INSTALL_S } from './strings';
import './install.css';

// The invitation to install Motonor, shown by itself at the first visit in a
// browser and once more a week later (inviteState.ts). Where the browser has
// its own install dialog, one tap opens it; otherwise it opens the guide. On
// a computer it always opens the guide, which also has the QR code for the
// phone. App.tsx keeps it away from rides and from the old address.

const InstallGuide = lazy(() => import('./InstallGuide'));

/** Time for the map and the weather to appear before the invitation does. */
const SHOW_AFTER_MS = 2500;

export function InstallInvite() {
  const s = useStrings(INSTALL_S);
  const toast = useToast();
  const titleId = useId();
  const install = useInstallState();
  const platform = useMemo(currentPlatform, []);
  // A computer, by its browser rather than the window's width: a tablet in
  // landscape still installs like a phone.
  const desktop = platform.kind === 'desktop';
  const [visible, setVisible] = useState(false);
  const [guide, setGuide] = useState(false);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (getInstallState() === 'installed' || !inviteDue(readInvite(), Date.now())) return undefined;
    const timer = window.setTimeout(() => {
      if (getInstallState() === 'installed') return;
      writeInvite(afterShowing(readInvite(), Date.now()));
      setVisible(true);
    }, SHOW_AFTER_MS);
    return () => window.clearTimeout(timer);
  }, []);

  // Installed from the browser's own button meanwhile: nothing left to ask.
  useEffect(() => {
    if (install === 'installed') setVisible(false);
  }, [install]);

  const oneTap = install === 'available' && !desktop;

  const openGuide = (): void => {
    setVisible(false);
    setGuide(true);
  };

  const onInstall = async (): Promise<void> => {
    setBusy(true);
    try {
      const outcome = await promptInstall();
      if (outcome === 'accepted') toast(s.installing, { tone: 'success' });
      if (outcome === 'unavailable') {
        openGuide();
        return;
      }
      setVisible(false);
    } catch (err) {
      console.warn('[install] the install dialog did not open', err);
      openGuide();
    } finally {
      setBusy(false);
    }
  };

  const lead = desktop
    ? canInstallHere(platform)
      ? s.inviteLeadDesktop
      : s.inviteLeadPhoneOnly
    : platform.kind === 'ios'
      ? s.inviteLeadIos
      : s.inviteLead;

  return (
    <>
      {visible && (
        <div className="install-invite" role="region" aria-labelledby={titleId}>
          <img className="install-invite__icon" src="/icon-192.png" alt="" width={44} height={44} />
          <div className="install-invite__text">
            <p id={titleId} className="install-invite__title num">
              {s.inviteTitle}
            </p>
            <p className="install-invite__lead">{lead}</p>
          </div>
          <div className="install-invite__actions">
            {oneTap ? (
              <Button variant="primary" icon="download" busy={busy} onClick={() => void onInstall()}>
                {s.install}
              </Button>
            ) : (
              <Button variant="primary" icon="download" onClick={openGuide}>
                {s.howTo}
              </Button>
            )}
            <Button variant="ghost" onClick={() => setVisible(false)}>
              {s.notNow}
            </Button>
          </div>
        </div>
      )}
      {guide && (
        <Suspense fallback={null}>
          <InstallGuide open onClose={() => setGuide(false)} />
        </Suspense>
      )}
    </>
  );
}
