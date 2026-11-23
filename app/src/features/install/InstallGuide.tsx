import { lazy, Suspense, useMemo, useState } from 'react';
import { CORE } from '../../i18n/core';
import { fmt, useStrings } from '../../lib/i18n';
import { useToast } from '../../state/toast';
import { Icon } from '../../ui/icons';
import { Button } from '../../ui/primitives';
import { Sheet } from '../../ui/Sheet';
import { promptInstall, useInstallState } from '../account/installPrompt';
import { canInstallHere, currentPlatform } from './platform';
import { installSteps } from './steps';
import { INSTALL_S } from './strings';
import './install.css';

// How to install Motonor here, in the manner of Aapogee Opportunities: the
// browser's own install button where it offers one, otherwise the steps for
// this device and browser. On a computer, a QR code puts it on the phone too.

const QrCode = lazy(() => import('./QrCode'));

interface Props {
  open: boolean;
  onClose: () => void;
}

export default function InstallGuide({ open, onClose }: Props) {
  const s = useStrings(INSTALL_S);
  const core = useStrings(CORE);
  const toast = useToast();
  const install = useInstallState();
  const platform = useMemo(currentPlatform, []);
  const [busy, setBusy] = useState(false);
  const [promptFailed, setPromptFailed] = useState(false);
  const desktop = platform.kind === 'desktop';
  const steps = installSteps(platform);
  const appUrl = `${window.location.origin}/`;

  const onInstall = async (): Promise<void> => {
    setBusy(true);
    try {
      const outcome = await promptInstall();
      if (outcome === 'accepted') {
        toast(s.installing, { tone: 'success' });
        onClose();
      } else if (outcome === 'unavailable') {
        setPromptFailed(true);
      }
    } catch (err) {
      console.warn('[install] the install dialog did not open', err);
      setPromptFailed(true);
    } finally {
      setBusy(false);
    }
  };

  const showSteps = (install !== 'available' || promptFailed) && steps.length > 0;

  return (
    <Sheet open={open} onClose={onClose} title={s.guideTitle} footer={<Button onClick={onClose}>{core.close}</Button>}>
      <p className="install-lead">{desktop ? s.guideLeadDesktop : s.guideLead}</p>
      {install === 'available' && !promptFailed && (
        <Button variant="primary" size="lg" full icon="download" busy={busy} onClick={() => void onInstall()}>
          {s.installNow}
        </Button>
      )}
      {promptFailed && <p className="install-note">{s.installFailed}</p>}
      {showSteps && (
        <ol className="install-steps">
          {steps.map((step, i) => (
            <li key={step.text} className="install-step">
              <span className="install-step__n num" aria-hidden="true">
                {i + 1}
              </span>
              <span className="install-step__icon" aria-hidden="true">
                <Icon name={step.icon} size={20} />
              </span>
              <span className="install-step__text">{s[step.text]}</span>
            </li>
          ))}
        </ol>
      )}
      {!canInstallHere(platform) && <p className="install-note">{s.firefoxDesktop}</p>}
      {platform.kind === 'ios' && <p className="install-note">{s.iosNote}</p>}
      {desktop && (
        <section className="install-phone" aria-labelledby="install-phone-title">
          <Suspense fallback={<span className="install-qr install-qr--loading" />}>
            <QrCode value={appUrl} label={fmt(s.qrLabel, { url: appUrl })} />
          </Suspense>
          <div className="install-phone__text">
            <h3 id="install-phone-title" className="install-phone__title">
              {s.phoneTitle}
            </h3>
            <p className="install-phone__lead">{s.phoneLead}</p>
            <p className="install-phone__url num">{window.location.host}</p>
          </div>
        </section>
      )}
    </Sheet>
  );
}
