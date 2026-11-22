import { defineStrings } from '../../lib/i18n';

// Copy for the fuel stops (fuel.ts), in the planner, on the route map and on
// the Drum route cards.
export const FUEL_S = defineStrings(
  {
    title: 'Alimentare',
    noRange: 'Setează câți km faci cu un plin și îți arătăm unde să alimentezi pe drum.',
    setRange: 'Setează autonomia',
    loading: 'Caut benzinăriile de pe traseu…',
    error: 'Nu am putut încărca benzinăriile. Încearcă din nou mai târziu.',
    stop: 'km {km} · {name}',
    anyStation: 'benzinărie',
    nearRoad: 'lângă drum',
    offRoad: 'la {d} km de drum',
    gapTooLong:
      'Între km {from} și km {to} nu e nicio benzinărie lângă drum, iar distanța e mai mare decât autonomia ta de {range} km. Alege altă rută sau fă plinul cât mai aproape de km {from}.',
    gapTight: 'Între km {from} și km {to} nu e nicio benzinărie lângă drum: ajungi pe rezervă.',
    note: 'Cu plinul făcut la plecare și oprire înainte de {pct}% din autonomia de {range} km. Benzinăriile vin din OpenStreetMap și se actualizează lunar.',
    marker: 'Alimentare: {name}, km {km}',
    driveStop: 'Alimentezi la km {km} · {name}',
    driveMore: '{text} și încă {n}',
    driveGap: 'Fără benzinărie între km {from} și {to}',
  },
  {
    title: 'Fuel',
    noRange: 'Set how far you go on a full tank and we show where to fill up on the way.',
    setRange: 'Set the range',
    loading: 'Looking for fuel stations on the route…',
    error: 'The fuel stations could not be loaded. Try again later.',
    stop: 'km {km} · {name}',
    anyStation: 'fuel station',
    nearRoad: 'by the road',
    offRoad: '{d} km off the road',
    gapTooLong:
      'Between km {from} and km {to} there is no fuel station by the road, and that is longer than your {range} km range. Pick another route or fill up as close to km {from} as you can.',
    gapTight: 'Between km {from} and km {to} there is no fuel station by the road: you will be on reserve.',
    note: 'Starting on a full tank and filling up before {pct}% of your {range} km range. Stations come from OpenStreetMap, updated monthly.',
    marker: 'Fuel: {name}, km {km}',
    driveStop: 'Fill up at km {km} · {name}',
    driveMore: '{text} and {n} more',
    driveGap: 'No fuel between km {from} and {to}',
  },
);
