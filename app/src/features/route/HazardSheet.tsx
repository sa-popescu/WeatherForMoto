import { useEffect, useId, useState } from 'react';
import type { HazardInput } from '../../lib/api';
import { getCurrentPosition } from '../../lib/geo';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import type { HazardType } from '../../lib/types';
import { CORE } from '../../i18n/core';
import { useToast } from '../../state/toast';
import { Sheet } from '../../ui/Sheet';
import { Button, Chip, Segmented } from '../../ui/primitives';
import { AS } from './actionStrings';
import { DESCRIPTION_MAX, DESCRIPTION_MIN, HAZARD_TTL_HOURS, HAZARD_TYPES, typeKey } from './hazards';
import { fmtKm } from './routeFormat';
import type { SamplePoint } from './types';

// Report a hazard at a point of the route or at the rider's GPS position.

const GPS = 'gps';
const SEVERITIES = ['1', '2', '3', '4', '5'] as const;

interface HazardSheetProps {
  open: boolean;
  onClose: () => void;
  points: readonly SamplePoint[];
  onSubmit: (input: HazardInput) => Promise<boolean>;
}

export function HazardSheet({ open, onClose, points, onSubmit }: HazardSheetProps) {
  const as = useStrings(AS);
  const core = useStrings(CORE);
  const lang = useLang();
  const toast = useToast();
  const ids = useId();
  const [type, setType] = useState<HazardType>('gravel');
  const [severity, setSeverity] = useState<(typeof SEVERITIES)[number]>('3');
  const [description, setDescription] = useState('');
  const [ttl, setTtl] = useState<number>(6);
  const [where, setWhere] = useState<string>(points[0]?.id ?? GPS);
  const [touched, setTouched] = useState(false);
  const [busy, setBusy] = useState(false);

  useEffect(() => {
    if (!open) return;
    // Fresh form on every open; the first route point is the usual answer,
    // GPS is an explicit choice (it can fail or be denied).
    setDescription('');
    setTouched(false);
    setWhere(points[0]?.id ?? GPS);
  }, [open, points]);

  const text = description.trim();
  const valid = text.length >= DESCRIPTION_MIN && text.length <= DESCRIPTION_MAX;
  const sevWords = [as.sev1, as.sev2, as.sev3, as.sev4, as.sev5];

  const submit = async (): Promise<void> => {
    setTouched(true);
    if (!valid) return;
    setBusy(true);
    try {
      let lat: number;
      let lon: number;
      const point = points.find((p) => p.id === where);
      if (point) {
        ({ lat, lon } = point);
      } else {
        try {
          ({ lat, lon } = await getCurrentPosition());
        } catch (err) {
          console.warn('[route] GPS for hazard failed', err);
          toast(as.hzGpsFail, { tone: 'error' });
          return;
        }
      }
      const ok = await onSubmit({ lat, lon, hazard_type: type, severity: Number(severity), description: text, ttl_hours: ttl });
      if (ok) onClose();
    } finally {
      setBusy(false);
    }
  };

  return (
    <Sheet
      open={open}
      onClose={onClose}
      title={as.hzReportTitle}
      footer={
        <>
          <Button onClick={onClose}>{core.cancel}</Button>
          <Button variant="primary" busy={busy} onClick={() => void submit()}>
            {as.hzSend}
          </Button>
        </>
      }
    >
      <fieldset className="route-fieldset">
        <legend>{as.hzType}</legend>
        <div className="route-chips">
          {HAZARD_TYPES.map((t) => (
            <Chip key={t} selected={t === type} onClick={() => setType(t)}>
              {as[typeKey(t)]}
            </Chip>
          ))}
        </div>
      </fieldset>
      <div className="route-fieldset">
        <span className="route-fieldset__legend">{as.hzSeverity}</span>
        <Segmented label={as.hzSeverity} options={SEVERITIES.map((v) => ({ value: v, label: v }))} value={severity} onChange={setSeverity} />
        <span className="route-note route-note--small" aria-live="polite">
          {fmt(as.hzSev, { n: severity, word: sevWords[Number(severity) - 1] })}
        </span>
      </div>
      <div className="field">
        <label className="field__label" htmlFor={`${ids}-desc`}>
          {as.hzDesc}
        </label>
        <textarea
          id={`${ids}-desc`}
          className="field__input route-textarea"
          value={description}
          maxLength={DESCRIPTION_MAX}
          aria-invalid={touched && !valid ? true : undefined}
          aria-describedby={`${ids}-desc-hint`}
          onChange={(e) => setDescription(e.target.value)}
        />
        <span id={`${ids}-desc-hint`} className={touched && !valid ? 'field__hint field__hint--error' : 'field__hint'}>
          {touched && !valid ? as.hzDescError : fmt(as.hzDescHint, { n: text.length })}
        </span>
      </div>
      <fieldset className="route-fieldset">
        <legend>{as.hzTtl}</legend>
        <div className="route-chips">
          {HAZARD_TTL_HOURS.map((h) => (
            <Chip key={h} selected={h === ttl} onClick={() => setTtl(h)}>
              {fmt(as.hzTtlOption, { n: h })}
            </Chip>
          ))}
        </div>
      </fieldset>
      <div className="field">
        <label className="field__label" htmlFor={`${ids}-where`}>
          {as.hzWhere}
        </label>
        <select id={`${ids}-where`} className="field__input route-select" value={where} onChange={(e) => setWhere(e.target.value)}>
          {points.map((p) => (
            <option key={p.id} value={p.id}>
              {p.name} · km {fmtKm(p.km, lang)}
            </option>
          ))}
          <option value={GPS}>{as.hzGps}</option>
        </select>
      </div>
    </Sheet>
  );
}
