import { useStrings } from '../../../lib/i18n';
import { Card, Toggle } from '../../../ui/primitives';
import { ALERTS } from '../strings/alerts';
import type { AlertCardProps } from './AlertsSection';

/** The per-alert switches live in alert_states, a small JSON object on the server. */
function readStates(raw: string | null | undefined): Record<string, unknown> {
  if (!raw) return {};
  try {
    const parsed: unknown = JSON.parse(raw);
    return parsed && typeof parsed === 'object' && !Array.isArray(parsed) ? (parsed as Record<string, unknown>) : {};
  } catch {
    return {};
  }
}

/** On unless turned off, like the server reads it (severe_alerts.enabled). */
export function severeEnabled(raw: string | null | undefined): boolean {
  return readStates(raw).severe !== false;
}

export function withSevere(raw: string | null | undefined, on: boolean): string {
  return JSON.stringify({ ...readStates(raw), severe: on });
}

export function SevereAlerts({ prefs, update }: AlertCardProps) {
  const s = useStrings(ALERTS);
  return (
    <Card className="acct-stack">
      <h3 className="acct-sub">{s.severeTitle}</h3>
      <Toggle
        checked={severeEnabled(prefs.alert_states)}
        onChange={(on) => update({ alert_states: withSevere(prefs.alert_states, on) })}
        label={s.severe}
        description={s.severeDesc}
      />
    </Card>
  );
}
