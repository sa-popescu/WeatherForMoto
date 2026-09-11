import { fmt, useLang, useStrings } from '../../../lib/i18n';
import { fmtNumber } from '../../../lib/format';
import type { AlertPrefs } from '../../../lib/types';
import { Card, Toggle } from '../../../ui/primitives';
import { ALERTS } from '../strings/alerts';
import { Note, RangeField } from '../ui/controls';
import { SLIDER_DEBOUNCE_MS, type AlertCardProps } from './AlertsSection';

// Slider ranges stay inside the backend limits (AlertPrefsPayload).
const SCORE = { min: 0, max: 100, step: 5 };
const GUSTS = { min: 20, max: 120, step: 5 };
const COLD = { min: -15, max: 20, step: 1, initial: 5 };
const HEAT = { min: 25, max: 45, step: 1, initial: 35 };

/** Rain has no threshold on purpose: it follows the probability x intensity matrix. */
export function AlertThresholds({ prefs, update }: AlertCardProps) {
  const s = useStrings(ALERTS);
  const lang = useLang();
  const slide = (changes: Partial<AlertPrefs>): void => update(changes, { debounceMs: SLIDER_DEBOUNCE_MS });
  const deg = (value: number): string => fmtNumber(value, lang);

  return (
    <Card className="acct-stack">
      <h3 className="acct-sub">{s.thresholdsTitle}</h3>
      <RangeField
        label={s.minScore}
        {...SCORE}
        value={prefs.min_score}
        display={fmt(s.minScoreValue, { value: prefs.min_score })}
        onChange={(min_score) => slide({ min_score })}
      />
      <RangeField
        label={s.gusts}
        {...GUSTS}
        value={prefs.max_wind_gust}
        display={fmt(s.gustsValue, { value: Math.round(prefs.max_wind_gust) })}
        onChange={(max_wind_gust) => slide({ max_wind_gust })}
      />
      <div className="acct-divider" />
      <Toggle
        checked={prefs.min_temp != null}
        onChange={(on) => update({ min_temp: on ? COLD.initial : null })}
        label={s.cold}
        description={s.coldDesc}
      />
      {prefs.min_temp != null && (
        <RangeField
          label={s.cold}
          hideLabel
          min={COLD.min}
          max={COLD.max}
          step={COLD.step}
          value={prefs.min_temp}
          display={fmt(s.coldValue, { value: deg(prefs.min_temp) })}
          onChange={(min_temp) => slide({ min_temp })}
        />
      )}
      <Toggle
        checked={prefs.max_temp != null}
        onChange={(on) => update({ max_temp: on ? HEAT.initial : null })}
        label={s.heat}
        description={s.heatDesc}
      />
      {prefs.max_temp != null && (
        <RangeField
          label={s.heat}
          hideLabel
          min={HEAT.min}
          max={HEAT.max}
          step={HEAT.step}
          value={prefs.max_temp}
          display={fmt(s.heatValue, { value: deg(prefs.max_temp) })}
          onChange={(max_temp) => slide({ max_temp })}
        />
      )}
      <div className="acct-divider" />
      <Toggle
        checked={prefs.frost_risk_enabled}
        onChange={(frost_risk_enabled) => update({ frost_risk_enabled })}
        label={s.frost}
        description={s.frostDesc}
      />
      <Note icon="drop">{s.rainNote}</Note>
    </Card>
  );
}
