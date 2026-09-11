import { useEffect, useRef } from 'react';
import { fmt, useStrings } from '../../../lib/i18n';
import type { Level, MotoType } from '../../../lib/types';
import { useAuth } from '../../../state/auth';
import { useSettings, type MotoProfile } from '../../../state/settings';
import { Card } from '../../../ui/primitives';
import { usePrefsEditor } from '../hooks/prefsEditor';
import { prefsFromProfile, reconcileProfile } from '../lib/profile';
import { APP } from '../strings/app';
import { Choices, Group, LabeledSegmented, RangeField } from '../ui/controls';
import { SLIDER_DEBOUNCE_MS } from './AlertsSection';

const COMFORT = { min: 5, max: 35, step: 1 };

/** Works signed out (device only); when signed in it also syncs to the account prefs. */
export function MotoSection() {
  const s = useStrings(APP);
  const { profile, setProfile } = useSettings();
  const { token } = useAuth();
  const { prefs, update } = usePrefsEditor();
  const reconciledFor = useRef<string | null>(null);
  const synced = prefs !== null;

  // Once per session: settle device vs account copy (see reconcileProfile).
  useEffect(() => {
    if (!prefs || !token || reconciledFor.current === token) return;
    reconciledFor.current = token;
    const result = reconcileProfile(profile, prefs);
    if (result.action === 'adopt') setProfile(result.profile);
    else if (result.action === 'push') update(prefsFromProfile(result.profile), { quiet: true });
  }, [prefs, token, profile, setProfile, update]);

  const change = (next: MotoProfile, debounceMs = 0): void => {
    setProfile(next);
    if (synced) update(prefsFromProfile(next), { quiet: true, debounceMs });
  };

  const types: ReadonlyArray<{ value: MotoType; label: string }> = [
    { value: 'naked', label: s.motoNaked },
    { value: 'touring', label: s.motoTouring },
    { value: 'enduro', label: s.motoEnduro },
    { value: 'sport', label: s.motoSport },
    { value: 'scooter', label: s.motoScooter },
  ];
  const levels: ReadonlyArray<{ value: Level; label: string }> = [
    { value: 'low', label: s.tolLow },
    { value: 'medium', label: s.tolMedium },
    { value: 'high', label: s.tolHigh },
  ];

  return (
    <Group title={s.motoSection}>
      <Card className="acct-stack">
        <div className="acct-seg-field">
          <span className="acct-seg-field__label" aria-hidden="true">
            {s.motoType}
          </span>
          <Choices variant="chips" label={s.motoType} options={types} value={profile.motoType} onChange={(motoType) => change({ ...profile, motoType })} />
        </div>
        <LabeledSegmented label={s.windTol} options={levels} value={profile.windTolerance} onChange={(windTolerance) => change({ ...profile, windTolerance })} />
        <LabeledSegmented label={s.rainTol} options={levels} value={profile.rainTolerance} onChange={(rainTolerance) => change({ ...profile, rainTolerance })} />
        <RangeField
          label={s.comfort}
          {...COMFORT}
          value={profile.comfortTemp}
          display={fmt(s.comfortValue, { value: profile.comfortTemp })}
          onChange={(comfortTemp) => change({ ...profile, comfortTemp }, SLIDER_DEBOUNCE_MS)}
        />
        <p className="acct-hint">{synced ? s.motoSynced : s.motoLocal}</p>
      </Card>
    </Group>
  );
}
