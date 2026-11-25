import { useState } from 'react';
import { api, ApiError } from '../../../lib/api';
import { fmt, useStrings } from '../../../lib/i18n';
import type { MeResponse } from '../../../lib/types';
import { useAuth } from '../../../state/auth';
import { useToast } from '../../../state/toast';
import { Button, Card, Toggle } from '../../../ui/primitives';
import { usePrefsEditor } from '../hooks/prefsEditor';
import { useErrorText } from '../hooks/useErrorText';
import { isSessionError } from '../lib/errors';
import { ALERTS } from '../strings/alerts';
import { Note } from '../ui/controls';
import { HourSelect, hourLabel } from './QuietHours';

/** The device's IANA zone: the server reads the chosen hour in it. */
function deviceTimeZone(): string | null {
  try {
    return Intl.DateTimeFormat().resolvedOptions().timeZone || null;
  } catch {
    return null;
  }
}

/**
 * Opt-in daily push with the next 24 hours at the alert location, at a
 * whole hour (the server's scheduler runs at minute 5 of every hour).
 * Hidden until the server's database has the columns for it.
 */
export function DailyForecast({ me, pushOn }: { me: MeResponse; pushOn: boolean }) {
  const s = useStrings(ALERTS);
  const { token, handleAuthError } = useAuth();
  const { prefs, update } = usePrefsEditor();
  const toast = useToast();
  const errorText = useErrorText();
  const [sending, setSending] = useState(false);
  if (!prefs || !me.dailyForecastAvailable) return null;

  const enabled = prefs.daily_forecast_enabled;
  const hour = prefs.daily_forecast_hour;
  const hasLocation = prefs.home_lat != null && prefs.home_lon != null;

  const onToggle = (next: boolean): void => update({ daily_forecast_enabled: next, daily_forecast_tz: deviceTimeZone() });
  const onHour = (next: number): void => update({ daily_forecast_hour: next, daily_forecast_tz: deviceTimeZone() });

  const onSend = async (): Promise<void> => {
    if (!token) return;
    setSending(true);
    try {
      const result = await api.dailyForecastNow(token);
      if (result.status === 'delivered') toast(s.dailySent, { tone: 'success' });
      else if (result.status === 'no_channel' || result.status === 'failed')
        toast(result.body ? fmt(s.dailyNoChannel, { body: result.body }) : s.dailyNoPush, { tone: 'info', durationMs: 12000 });
      else if (result.status === 'missing_home_location') toast(s.dailyNoLocation, { tone: 'info' });
      else toast(s.dailyNoData, { tone: 'info' });
    } catch (err) {
      console.warn('[account] daily forecast send failed', err);
      toast(err instanceof ApiError ? errorText(err, 'other') : s.dailyFailed, { tone: 'error' });
      if (isSessionError(err, 'other')) handleAuthError(err);
    } finally {
      setSending(false);
    }
  };

  return (
    <Card className="acct-stack">
      <Toggle
        checked={enabled}
        onChange={onToggle}
        label={s.dailyTitle}
        description={enabled ? fmt(s.dailyDesc, { hour: hourLabel(hour) }) : s.dailyOff}
      />
      {enabled && (
        <>
          <HourSelect label={s.dailyHour} value={hour} onChange={onHour} />
          {!pushOn && <Note icon="alert">{s.dailyNoPush}</Note>}
          {!hasLocation && <Note icon="alert">{s.dailyNoLocation}</Note>}
          <Button full icon="bell" busy={sending} onClick={() => void onSend()}>
            {s.dailySend}
          </Button>
        </>
      )}
    </Card>
  );
}
