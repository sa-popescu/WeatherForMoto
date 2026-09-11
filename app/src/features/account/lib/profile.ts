import type { AlertPrefs } from '../../../lib/types';
import { DEFAULT_PROFILE, type MotoProfile } from '../../../state/settings';

// The moto profile lives on the device (useSettings) and, when signed in, in
// the account prefs under different field names.

export type ProfilePrefs = Pick<AlertPrefs, 'moto_type' | 'comfort_temp' | 'wind_tolerance' | 'rain_tolerance'>;

export function prefsFromProfile(profile: MotoProfile): ProfilePrefs {
  return {
    moto_type: profile.motoType,
    comfort_temp: Math.round(profile.comfortTemp),
    wind_tolerance: profile.windTolerance,
    rain_tolerance: profile.rainTolerance,
  };
}

export function profileFromPrefs(prefs: ProfilePrefs): MotoProfile {
  return {
    motoType: prefs.moto_type,
    comfortTemp: prefs.comfort_temp,
    windTolerance: prefs.wind_tolerance,
    rainTolerance: prefs.rain_tolerance,
  };
}

export function sameProfile(a: MotoProfile, b: MotoProfile): boolean {
  return a.motoType === b.motoType && a.comfortTemp === b.comfortTemp && a.windTolerance === b.windTolerance && a.rainTolerance === b.rainTolerance;
}

export type Reconcile = { action: 'none' } | { action: 'adopt'; profile: MotoProfile } | { action: 'push'; profile: MotoProfile };

/**
 * On sign-in: the account copy wins (it follows the rider across devices),
 * unless the account still has the factory defaults and this device was
 * customised, in which case the device copy is uploaded instead.
 */
export function reconcileProfile(local: MotoProfile, server: ProfilePrefs): Reconcile {
  const remote = profileFromPrefs(server);
  if (sameProfile(local, remote)) return { action: 'none' };
  if (sameProfile(remote, DEFAULT_PROFILE)) return { action: 'push', profile: local };
  return { action: 'adopt', profile: remote };
}
