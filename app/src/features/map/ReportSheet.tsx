import { useId, useState, type FormEvent } from 'react';
import { CORE } from '../../i18n/core';
import { api } from '../../lib/api';
import { fmtNumber } from '../../lib/format';
import { distanceKm } from '../../lib/geo';
import { fmt, useLang, useStrings } from '../../lib/i18n';
import { useAuth } from '../../state/auth';
import { usePlace } from '../../state/place';
import { useToast } from '../../state/toast';
import { Icon } from '../../ui/icons';
import { Button } from '../../ui/primitives';
import { Sheet } from '../../ui/Sheet';
import { isDescriptionValid, reportFailure, type LatLon, type ReportFailure } from './hazards';
import { EMPTY_DRAFT, ReportFields, type ReportDraft } from './ReportFields';
import { MAP_STRINGS } from './strings';

// Report a hazard at a picked spot. Needs an account: signed out, the sheet
// explains why and links to "Eu".

interface Props {
  at: LatLon | null;
  onClose: () => void;
  onReported: () => void;
}

export function ReportSheet({ at, onClose, onReported }: Props) {
  if (!at) return null;
  // A new spot starts a fresh form.
  return <ReportForm key={`${at.lat}:${at.lon}`} at={at} onClose={onClose} onReported={onReported} />;
}

const round5 = (value: number): number => Math.round(value * 1e5) / 1e5;

function ReportForm({ at, onClose, onReported }: Props & { at: LatLon }) {
  const s = useStrings(MAP_STRINGS);
  const core = useStrings(CORE);
  const lang = useLang();
  const { token, handleAuthError } = useAuth();
  const { place } = usePlace();
  const toast = useToast();
  const formId = useId();
  const [draft, setDraft] = useState<ReportDraft>(EMPTY_DRAFT);
  const [showErrors, setShowErrors] = useState(false);
  const [busy, setBusy] = useState(false);

  if (!token) {
    return (
      <Sheet
        open
        onClose={onClose}
        title={s.reportTitle}
        footer={
          <a className="btn btn--primary btn--lg btn--full" href="#/eu" onClick={onClose}>
            {s.goToAccount}
          </a>
        }
      >
        <p className="map-signed-out">{s.reportSignedOut}</p>
      </Sheet>
    );
  }

  const failureText: Record<ReportFailure, string> = {
    auth: s.reportSession,
    rate: s.reportTooMany,
    invalid: s.reportInvalid,
    network: core.networkError,
    other: core.genericError,
  };

  const submit = async (event: FormEvent<HTMLFormElement>): Promise<void> => {
    event.preventDefault();
    setShowErrors(true);
    if (busy || draft.type === null || !isDescriptionValid(draft.description)) return;
    setBusy(true);
    try {
      await api.reportHazard(token, {
        lat: round5(at.lat),
        lon: round5(at.lon),
        hazard_type: draft.type,
        severity: draft.severity,
        description: draft.description.trim(),
        ttl_hours: draft.ttl,
      });
      toast(s.reportDone, { tone: 'success' });
      onReported();
      onClose();
    } catch (err) {
      const failure = reportFailure(err);
      if (failure === 'auth') handleAuthError(err);
      else console.warn('[map] hazard report failed', err);
      toast(failureText[failure], { tone: 'error' });
      setBusy(false);
    }
  };

  const km = distanceKm(place, at);
  const footer = (
    <>
      <Button size="lg" onClick={onClose}>
        {core.cancel}
      </Button>
      <Button type="submit" form={formId} variant="primary" size="lg" icon="alert" busy={busy}>
        {s.reportSubmit}
      </Button>
    </>
  );

  return (
    <Sheet open onClose={onClose} title={s.reportTitle} footer={footer}>
      <form id={formId} className="map-report-form" noValidate onSubmit={(event) => void submit(event)}>
        <p className="map-where">
          <Icon name="pin" size={18} />
          <span>{fmt(s.distanceFrom, { km: fmtNumber(km, lang, km < 10 ? 1 : 0), place: place.name })}</span>
          <span className="num muted">
            {at.lat.toFixed(4)}, {at.lon.toFixed(4)}
          </span>
        </p>
        <ReportFields draft={draft} onChange={(change) => setDraft((d) => ({ ...d, ...change }))} showErrors={showErrors} />
      </form>
    </Sheet>
  );
}
