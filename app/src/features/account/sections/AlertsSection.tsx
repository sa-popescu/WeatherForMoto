import { useStrings } from '../../../lib/i18n';
import type { AlertPrefs, Level } from '../../../lib/types';
import { Card } from '../../../ui/primitives';
import { usePrefsEditor, type PrefsEditor } from '../hooks/prefsEditor';
import { ALERTS } from '../strings/alerts';
import { Choices, Group } from '../ui/controls';
import { AlertLocation } from './AlertLocation';
import { AlertThresholds } from './AlertThresholds';
import { QuietHours } from './QuietHours';

/** Props every alert sub-card gets: the merged prefs and the shared editor. */
export interface AlertCardProps {
  prefs: AlertPrefs;
  update: PrefsEditor['update'];
}

/** Sliders wait this long after the last move before saving. */
export const SLIDER_DEBOUNCE_MS = 700;

export function AlertsSection() {
  const s = useStrings(ALERTS);
  const { prefs, update } = usePrefsEditor();
  if (!prefs) return null;

  const severities: ReadonlyArray<{ value: Level; label: string; description: string }> = [
    { value: 'low', label: s.sevLow, description: s.sevLowDesc },
    { value: 'medium', label: s.sevMedium, description: s.sevMediumDesc },
    { value: 'high', label: s.sevHigh, description: s.sevHighDesc },
  ];

  return (
    <Group title={s.alertsSection}>
      <AlertLocation prefs={prefs} update={update} />
      <Card className="acct-stack">
        <h3 className="acct-sub">{s.severityTitle}</h3>
        <Choices label={s.severityTitle} options={severities} value={prefs.severity} onChange={(severity) => update({ severity })} />
      </Card>
      <AlertThresholds prefs={prefs} update={update} />
      <QuietHours prefs={prefs} update={update} />
    </Group>
  );
}
