// The parts of the app whose use is counted: lib/usage.ts notes them, the
// Worker (worker/usage.ts) adds them up per day and its /stats page shows
// them with these labels. A visit counts each part at most once. The Worker
// drops any name that is not in this list, so nothing else can be stored.
// This file is shared by the app and the Worker: no browser code here.

export const USAGE_EVENTS = [
  ['visit', 'Vizite'],
  ['visit:installed', 'Vizite din aplicația instalată'],
  ['visit:signed-in', 'Vizite cu cont'],
  ['visit:en', 'Vizite în engleză'],
  ['view:drum', 'Ecran: Drum'],
  ['view:traseu', 'Ecran: Plan cu opriri'],
  ['view:acum', 'Ecran: Vreme, prognoza'],
  ['view:harta', 'Ecran: Vreme, harta'],
  ['view:ture', 'Ecran: Ture'],
  ['view:eu', 'Ecran: Eu'],
  ['view:live', 'Ecran: locația live a cuiva'],
  ['drive:routes', 'Drum: variante calculate'],
  ['drive:departure', 'Drum: altă oră de plecare aleasă'],
  ['plan:route', 'Plan: traseu calculat'],
  ['route:gpx-import', 'Traseu: GPX importat'],
  ['route:gpx-export', 'Traseu: GPX descărcat'],
  ['route:share', 'Traseu: trimis'],
  ['route:save', 'Traseu: salvat în cont'],
  ['route:offline-map', 'Traseu: hartă offline descărcată'],
  ['ride:start', 'Tură: pornită'],
  ['ride:live', 'Tură: locația live pornită'],
  ['ride:gpx', 'Tură: GPX descărcat'],
  ['ride:story', 'Tură: card trimis'],
  ['map:play', 'Hartă: animația pornită'],
  ['map:clouds', 'Hartă: nori'],
  ['hazard:report', 'Pericol raportat'],
  ['push:on', 'Notificări pornite'],
] as const;

export type UsageEvent = (typeof USAGE_EVENTS)[number][0];

const NAMES: ReadonlySet<string> = new Set(USAGE_EVENTS.map(([name]) => name));

export function isUsageEvent(value: unknown): value is UsageEvent {
  return typeof value === 'string' && NAMES.has(value);
}
