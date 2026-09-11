import { useEffect, useState } from 'react';
import { fmtMm, fmtNumber } from '../../lib/format';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { CORE } from '../../i18n/core';
import { Sheet } from '../../ui/Sheet';
import { Button, TextField } from '../../ui/primitives';
import { AS } from './actionStrings';
import type { RouteStats } from './plan';
import { fmtDuration, fmtKm } from './routeFormat';
import { RS } from './strings';

// Small sheets: sign-in explanation, save route (name), log ride (summary).

export function SignInSheet({ open, onClose }: { open: boolean; onClose: () => void }) {
  const as = useStrings(AS);
  return (
    <Sheet
      open={open}
      onClose={onClose}
      title={as.signedOutTitle}
      footer={
        <a className="btn btn--primary btn--lg" href="#/eu" onClick={onClose}>
          {as.goToAccount}
        </a>
      }
    >
      <p>{as.signedOutBody}</p>
    </Sheet>
  );
}

const NAME_MIN = 2;
const NAME_MAX = 80;

interface SaveSheetProps {
  open: boolean;
  onClose: () => void;
  defaultName: string;
  onSave: (name: string) => Promise<boolean>;
}

export function SaveRouteSheet({ open, onClose, defaultName, onSave }: SaveSheetProps) {
  const as = useStrings(AS);
  const core = useStrings(CORE);
  const [name, setName] = useState(defaultName);
  const [busy, setBusy] = useState(false);
  const [touched, setTouched] = useState(false);

  useEffect(() => {
    if (open) {
      setName(defaultName.slice(0, NAME_MAX));
      setTouched(false);
    }
  }, [open, defaultName]);

  const trimmed = name.trim();
  const valid = trimmed.length >= NAME_MIN && trimmed.length <= NAME_MAX;
  const submit = async (): Promise<void> => {
    setTouched(true);
    if (!valid) return;
    setBusy(true);
    const ok = await onSave(trimmed);
    setBusy(false);
    if (ok) onClose();
  };

  return (
    <Sheet
      open={open}
      onClose={onClose}
      title={as.saveTitle}
      footer={
        <>
          <Button onClick={onClose}>{core.cancel}</Button>
          <Button variant="primary" busy={busy} onClick={() => void submit()}>
            {core.save}
          </Button>
        </>
      }
    >
      <form
        onSubmit={(e) => {
          e.preventDefault();
          void submit();
        }}
      >
        <TextField
          label={as.saveName}
          value={name}
          onChange={setName}
          maxLength={NAME_MAX}
          hint={as.saveNameHint}
          error={touched && !valid ? as.saveNameError : null}
          autoComplete="off"
        />
      </form>
    </Sheet>
  );
}

interface LogSheetProps {
  open: boolean;
  onClose: () => void;
  distanceKm: number;
  durationMin: number;
  speed: number;
  stats: RouteStats;
  onConfirm: () => Promise<boolean>;
}

export function LogRideSheet({ open, onClose, distanceKm, durationMin, speed, stats, onConfirm }: LogSheetProps) {
  const as = useStrings(AS);
  const s = useStrings(RS);
  const core = useStrings(CORE);
  const lang = useLang();
  const [busy, setBusy] = useState(false);
  const rows: [string, string][] = [
    [as.logDistance, `${fmtKm(distanceKm, lang)} km`],
    [fmt(as.logDuration, { v: speed }), fmtDuration(durationMin, s)],
    [as.logAvg, fmtNumber(stats.avgScore, lang)],
    [as.logWorst, stats.worstScore == null ? '–' : `${stats.worstScore} (${stats.worstName ?? '–'})`],
    [as.logGust, stats.maxGustKmh == null ? '–' : `${fmtNumber(Math.round(stats.maxGustKmh), lang)} km/h`],
    [as.logPrecip, stats.maxPrecipMm == null ? '–' : `${fmtMm(stats.maxPrecipMm, lang)} mm/h`],
  ];
  const confirm = async (): Promise<void> => {
    setBusy(true);
    const ok = await onConfirm();
    setBusy(false);
    if (ok) onClose();
  };
  return (
    <Sheet
      open={open}
      onClose={onClose}
      title={as.logTitle}
      footer={
        <>
          <Button onClick={onClose}>{core.cancel}</Button>
          <Button variant="primary" busy={busy} onClick={() => void confirm()}>
            {as.logConfirm}
          </Button>
        </>
      }
    >
      <p className="muted">{as.logIntro}</p>
      <dl className="route-dl">
        {rows.map(([label, value]) => (
          <div key={label} className="route-dl__row">
            <dt>{label}</dt>
            <dd className="num">{value}</dd>
          </div>
        ))}
      </dl>
    </Sheet>
  );
}
