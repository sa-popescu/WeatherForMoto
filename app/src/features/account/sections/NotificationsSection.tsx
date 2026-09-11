import { useState } from 'react';
import { api, ApiError } from '../../../lib/api';
import { fmt, useStrings } from '../../../lib/i18n';
import type { MeResponse } from '../../../lib/types';
import { useAuth } from '../../../state/auth';
import { useToast } from '../../../state/toast';
import { Banner, Button, Card, Toggle } from '../../../ui/primitives';
import { usePrefsEditor } from '../hooks/prefsEditor';
import { useErrorText } from '../hooks/useErrorText';
import { usePushState } from '../hooks/usePushState';
import { describeCheckNow, type CheckOutcome } from '../lib/checkNow';
import { isSessionError } from '../lib/errors';
import { ALERTS } from '../strings/alerts';
import { Group, Note } from '../ui/controls';

type AlertStrings = typeof ALERTS.en;

function outcomeText(outcome: CheckOutcome, s: AlertStrings): string {
  switch (outcome.kind) {
    case 'disabled':
      return s.testDisabled;
    case 'noLocation':
      return s.testNoLocation;
    case 'quiet':
      return s.testQuiet;
    case 'delivered':
      return fmt(s.testDelivered, { count: outcome.count });
    case 'nothing':
      return s.testNothing;
    case 'noChannel':
      return s.testNoChannel;
    case 'alreadySent':
      return s.testAlready;
  }
}

/** Alerts reach the rider only as web push, so this is the one channel to manage. */
export function NotificationsSection({ me, active }: { me: MeResponse; active: boolean }) {
  const s = useStrings(ALERTS);
  const { token, handleAuthError } = useAuth();
  const { prefs, update } = usePrefsEditor();
  const toast = useToast();
  const errorText = useErrorText();
  const push = usePushState(active);
  const [testing, setTesting] = useState(false);

  const reportError = (err: unknown, fallback: string): void => {
    console.warn('[account] notification action failed', err);
    toast(err instanceof ApiError ? errorText(err, 'other') : fallback, { tone: 'error' });
    if (isSessionError(err, 'other')) handleAuthError(err);
  };

  const onPush = async (next: boolean): Promise<void> => {
    try {
      if (!next) {
        await push.disable();
        toast(s.pushDisabled);
        return;
      }
      const result = await push.enable();
      if (result === 'enabled') {
        toast(s.pushEnabled, { tone: 'success' });
        // Alerts switched off entirely server-side would make push silent.
        if (prefs && !prefs.enabled) update({ enabled: true }, { quiet: true });
      } else if (result === 'unsupported') {
        toast(s.pushUnsupported, { tone: 'error' });
      }
    } catch (err) {
      reportError(err, s.pushFailed);
    }
  };

  const onTest = async (): Promise<void> => {
    if (!token) return;
    setTesting(true);
    try {
      const outcome = describeCheckNow(await api.checkNow(token), me.pushSubscriptions);
      const enableAction = outcome.kind === 'disabled' ? { actionLabel: s.enableAlerts, onAction: () => update({ enabled: true }) } : {};
      toast(outcomeText(outcome, s), { tone: outcome.kind === 'delivered' ? 'success' : 'info', durationMs: 7000, ...enableAction });
    } catch (err) {
      reportError(err, s.pushFailed);
    } finally {
      setTesting(false);
    }
  };

  const pushDescription = push.on ? fmt(s.pushOn, { count: Math.max(1, me.pushSubscriptions) }) : s.pushOff;

  return (
    <Group title={s.notifSection}>
      <Card className="acct-stack">
        <Toggle
          checked={push.on}
          onChange={(next) => void onPush(next)}
          disabled={push.busy || (!push.on && push.availability !== 'ok')}
          label={s.pushLabel}
          description={pushDescription}
        />
        {push.availability === 'denied' && (
          <Banner tone="warn" icon="lock">
            {s.pushDenied}
          </Banner>
        )}
        {push.availability === 'ios-install' && (
          <Banner tone="info" icon="download">
            {s.pushIos}
          </Banner>
        )}
        {push.availability === 'unsupported' && <Note>{s.pushUnsupported}</Note>}
        <div className="acct-divider" />
        <Button full icon="bell" busy={testing} onClick={() => void onTest()}>
          {s.test}
        </Button>
        <p className="acct-hint">{s.testNote}</p>
      </Card>
    </Group>
  );
}
