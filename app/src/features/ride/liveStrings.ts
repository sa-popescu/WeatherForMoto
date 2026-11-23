import { defineStrings } from '../../lib/i18n';

// Copy for sharing the ride's position (liveShare.ts, LiveShareControl.tsx).
export const LIVE_S = defineStrings(
  {
    start: 'Partajează locația live',
    starting: 'Pornesc partajarea…',
    on: 'Locația ta se vede prin link',
    sentAgo: 'Trimisă acum {s} s',
    waiting: 'Aștept semnal GPS',
    sendLink: 'Trimite linkul',
    stop: 'Oprește',
    shareText: 'Urmărește-mi tura în direct pe Motonor:',
    copied: 'Linkul a fost copiat.',
    shareFailed: 'Nu am putut trimite linkul.',
    budget: 'Azi nu se mai poate porni partajarea live. Merge din nou de mâine.',
    off: 'Partajarea live nu e disponibilă acum.',
    network: 'Nu am putut porni partajarea. Verifică semnalul și încearcă din nou.',
    note: 'Linkul arată unde ești cât înregistrezi tura, cel mult 12 ore. La sfârșitul turei se șterge tot.',
  },
  {
    start: 'Share live location',
    starting: 'Starting to share…',
    on: 'Your location can be seen through the link',
    sentAgo: 'Sent {s} s ago',
    waiting: 'Waiting for GPS',
    sendLink: 'Send the link',
    stop: 'Stop',
    shareText: 'Follow my ride live on Motonor:',
    copied: 'The link was copied.',
    shareFailed: 'The link could not be shared.',
    budget: 'Live sharing cannot start again today. It works again tomorrow.',
    off: 'Live sharing is not available right now.',
    network: 'Sharing could not start. Check the signal and try again.',
    note: 'The link shows where you are while the ride is recorded, at most 12 hours. Everything is deleted when the ride ends.',
  },
);
