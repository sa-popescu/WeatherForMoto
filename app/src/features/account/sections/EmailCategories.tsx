import { useStrings } from '../../../lib/i18n';
import type { AlertPrefs } from '../../../lib/types';
import { Card, Toggle } from '../../../ui/primitives';
import { ALERTS } from '../strings/alerts';
import type { AlertCardProps } from './AlertsSection';

type CategoryKey = 'email_alert_score' | 'email_alert_wind' | 'email_alert_rain' | 'email_alert_temp_low' | 'email_alert_temp_high' | 'email_alert_frost';
type LabelKey = 'catScore' | 'catWind' | 'catRain' | 'catTempLow' | 'catTempHigh' | 'catFrost';

// The backend reads these only when sending EMAIL (_is_email_event_enabled);
// push gets every event that passes the severity level. Temperature events
// exist only when the matching threshold is set; frost only when
// frost_risk_enabled is on.
const CATEGORIES: ReadonlyArray<{ key: CategoryKey; label: LabelKey; needs?: 'min_temp' | 'max_temp' | 'frost_risk_enabled' }> = [
  { key: 'email_alert_score', label: 'catScore' },
  { key: 'email_alert_wind', label: 'catWind' },
  { key: 'email_alert_rain', label: 'catRain' },
  { key: 'email_alert_temp_low', label: 'catTempLow', needs: 'min_temp' },
  { key: 'email_alert_temp_high', label: 'catTempHigh', needs: 'max_temp' },
  { key: 'email_alert_frost', label: 'catFrost', needs: 'frost_risk_enabled' },
];

function thresholdMissing(prefs: AlertPrefs, needs: 'min_temp' | 'max_temp' | 'frost_risk_enabled' | undefined): boolean {
  if (!needs) return false;
  return needs === 'frost_risk_enabled' ? !prefs.frost_risk_enabled : prefs[needs] == null;
}

export function EmailCategories({ prefs, update, verified }: AlertCardProps & { verified: boolean }) {
  const s = useStrings(ALERTS);
  const ready = verified && prefs.email_alerts_enabled;

  const set = (key: CategoryKey, on: boolean): void => {
    const change: Partial<AlertPrefs> = {};
    change[key] = on;
    update(change);
  };

  return (
    <Card className="acct-stack">
      <h3 className="acct-sub">{s.emailCatsTitle}</h3>
      <p className="acct-hint">{ready ? s.emailCatsNote : s.emailCatsOff}</p>
      {CATEGORIES.map((category) => (
        <Toggle
          key={category.key}
          checked={prefs[category.key]}
          onChange={(on) => set(category.key, on)}
          disabled={!ready}
          label={s[category.label]}
          description={ready && thresholdMissing(prefs, category.needs) ? s.catNeedsThreshold : undefined}
        />
      ))}
    </Card>
  );
}
