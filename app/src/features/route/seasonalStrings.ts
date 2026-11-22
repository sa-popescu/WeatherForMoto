import { defineStrings } from '../../lib/i18n';

// Copy for the high roads closed in winter (seasonalClosures.ts), in the planner
// and on the Drum route cards.
export const SEASONAL_S = defineStrings(
  {
    title: 'Drumuri închise iarna',
    closed:
      'Între {from} și {to} drumul e de obicei închis din noiembrie până la sfârșitul lui mai, uneori și în iunie. Datele le anunță CNAIR în fiecare an, după zăpadă.',
    maybe: 'Între {from} și {to} drumul se poate închide sau poate fi încă închis în perioada asta, după cum cade zăpada.',
    check: 'Drumurile închise acum (Poliția Română)',
    shortClosed: '{name}: de obicei închis iarna',
    shortMaybe: '{name}: poate fi închis acum',
  },
  {
    title: 'Roads closed in winter',
    closed:
      'Between {from} and {to} the road is usually closed from November to the end of May, sometimes into June. CNAIR sets the dates each year, by the snow.',
    maybe: 'Between {from} and {to} the road may close, or still be closed, at this time of year, depending on the snow.',
    check: 'Roads closed right now (Romanian Police)',
    shortClosed: '{name}: usually closed in winter',
    shortMaybe: '{name}: may be closed now',
  },
);
