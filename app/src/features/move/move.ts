import { KEYS, readString, writeString, type StorageKey } from '../../lib/storage';
import { dumpRides, restoreRides } from '../ride/store';

// The app moved from weatherformoto.bluemouse.cc to motonor.bluemouse.cc. What
// a phone keeps (settings, favourites, quick destinations, the rides) belongs
// to the address it was saved under, so the old address offers to carry it
// over: everything goes, compressed, in the fragment of one link to the new
// address (a fragment never reaches a server), and the new address takes it
// in at start-up. The sign-in does not travel: it is done again there.

export const NEW_ORIGIN = 'https://motonor.bluemouse.cc';
const OLD_HOSTS: ReadonlySet<string> = new Set(['weatherformoto.bluemouse.cc']);
const PARAM = 'muta';
/** Past this the link is too long for some browsers: the rides stay behind. */
const MAX_LINK_CHARS = 1_500_000;
const VERSION = 1;

/** Settings and places worth carrying over; never the sign-in. */
const MOVED: readonly StorageKey[] = [
  KEYS.theme,
  KEYS.lang,
  KEYS.profile,
  KEYS.favorites,
  KEYS.lastPlace,
  KEYS.followMe,
  KEYS.rideVoice,
  KEYS.driveHome,
  KEYS.driveWork,
  KEYS.driveRecent,
];

interface Parcel {
  v: number;
  keys: Record<string, string>;
  rides: unknown;
}

export function onOldAddress(): boolean {
  return OLD_HOSTS.has(window.location.hostname);
}

function toBase64Url(bytes: Uint8Array): string {
  let text = '';
  for (let i = 0; i < bytes.length; i += 0x8000) text += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
  return btoa(text).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

function fromBase64Url(text: string): Uint8Array<ArrayBuffer> {
  const plain = atob(text.replace(/-/g, '+').replace(/_/g, '/'));
  const bytes = new Uint8Array(new ArrayBuffer(plain.length));
  for (let i = 0; i < plain.length; i += 1) bytes[i] = plain.charCodeAt(i);
  return bytes;
}

async function pack(parcel: Parcel): Promise<string> {
  const stream = new Blob([JSON.stringify(parcel)]).stream().pipeThrough(new CompressionStream('gzip'));
  return toBase64Url(new Uint8Array(await new Response(stream).arrayBuffer()));
}

async function unpack(text: string): Promise<Parcel> {
  const stream = new Blob([fromBase64Url(text)]).stream().pipeThrough(new DecompressionStream('gzip'));
  return JSON.parse(await new Response(stream).text()) as Parcel;
}

/** The link that opens the new address with this phone's data. `ridesLeft`: the rides did not fit. */
export async function moveLink(): Promise<{ url: string; ridesLeft: boolean }> {
  const keys: Record<string, string> = {};
  for (const key of MOVED) {
    const value = readString(key);
    if (value !== null) keys[key] = value;
  }
  let rides: unknown = null;
  try {
    rides = await dumpRides();
  } catch (err) {
    console.warn('[move] the rides could not be read', err);
  }
  let data = await pack({ v: VERSION, keys, rides });
  let ridesLeft = false;
  if (data.length > MAX_LINK_CHARS) {
    data = await pack({ v: VERSION, keys, rides: null });
    ridesLeft = true;
  }
  return { url: `${NEW_ORIGIN}/#/drum?${PARAM}=${data}`, ridesLeft };
}

/**
 * At start-up on the new address: takes in what a move link carries, then
 * drops it from the address. A setting already made here is kept; rides come
 * in only on a phone with none here yet.
 */
export async function receiveMove(): Promise<void> {
  const hash = window.location.hash;
  const at = hash.indexOf(`${PARAM}=`);
  if (at < 0) return;
  const data = hash.slice(at + PARAM.length + 1).split('&')[0];
  window.history.replaceState(null, '', `${window.location.pathname}${window.location.search}#/drum`);
  try {
    const parcel = await unpack(data);
    if (parcel.v !== VERSION) return;
    for (const [key, value] of Object.entries(parcel.keys ?? {})) {
      const known = MOVED.find((k) => k === key);
      if (known && typeof value === 'string' && readString(known) === null) writeString(known, value);
    }
    if (parcel.rides) await restoreRides(parcel.rides);
  } catch (err) {
    console.warn('[move] the moved data could not be read', err);
  }
}
