import { useId } from 'react';
import { fmt, useStrings } from '../../../lib/i18n';
import { Card, Toggle } from '../../../ui/primitives';
import { ALERTS } from '../strings/alerts';
import { Note } from '../ui/controls';
import type { AlertCardProps } from './AlertsSection';

const HOURS = Array.from({ length: 24 }, (_, hour) => hour);

function hourLabel(hour: number): string {
  return `${String(hour).padStart(2, '0')}:00`;
}

function HourSelect({ label, value, onChange }: { label: string; value: number; onChange: (hour: number) => void }) {
  const id = useId();
  return (
    <div className="field">
      <label className="field__label" htmlFor={id}>
        {label}
      </label>
      <select id={id} className="field__input acct-select num" value={value} onChange={(e) => onChange(Number(e.target.value))}>
        {HOURS.map((hour) => (
          <option key={hour} value={hour}>
            {hourLabel(hour)}
          </option>
        ))}
      </select>
    </div>
  );
}

/** Quiet hours hold delivery at the location's local time; start == end means off (backend rule). */
export function QuietHours({ prefs, update }: AlertCardProps) {
  const s = useStrings(ALERTS);
  const start = prefs.quiet_start_hour;
  const end = prefs.quiet_end_hour;
  return (
    <Card className="acct-stack">
      <Toggle
        checked={prefs.quiet_hours_enabled}
        onChange={(quiet_hours_enabled) => update({ quiet_hours_enabled })}
        label={s.quietTitle}
        description={fmt(s.quietDesc, { start: hourLabel(start), end: hourLabel(end) })}
      />
      {prefs.quiet_hours_enabled && (
        <div className="acct-quiet">
          <HourSelect label={s.quietStart} value={start} onChange={(quiet_start_hour) => update({ quiet_start_hour })} />
          <HourSelect label={s.quietEnd} value={end} onChange={(quiet_end_hour) => update({ quiet_end_hour })} />
        </div>
      )}
      {prefs.quiet_hours_enabled && start === end && <Note icon="alert">{s.quietSame}</Note>}
    </Card>
  );
}
