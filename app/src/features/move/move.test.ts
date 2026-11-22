import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { KEYS } from '../../lib/storage';

// The move link in both directions, with the phone's storage faked: what
// travels, what does not (the sign-in), and that a setting already made on
// the new address is kept.

vi.mock('../ride/store', () => ({
  dumpRides: vi.fn(async () => ({ rides: [], chunks: [], routes: [] })),
  restoreRides: vi.fn(async () => 0),
}));

const { moveLink, receiveMove, NEW_ORIGIN } = await import('./move');

class MemoryStorage {
  private data = new Map<string, string>();
  getItem(key: string): string | null {
    return this.data.get(key) ?? null;
  }
  setItem(key: string, value: string): void {
    this.data.set(key, value);
  }
  removeItem(key: string): void {
    this.data.delete(key);
  }
  clear(): void {
    this.data.clear();
  }
}

const storage = new MemoryStorage();
let hash = '';

beforeEach(() => {
  storage.clear();
  vi.stubGlobal('localStorage', storage);
  vi.stubGlobal('window', {
    location: {
      hostname: 'weatherformoto.bluemouse.cc',
      pathname: '/',
      search: '',
      get hash() {
        return hash;
      },
    },
    history: {
      replaceState: (_s: unknown, _t: string, url: string) => {
        hash = url.slice(url.indexOf('#'));
      },
    },
  });
});

afterEach(() => {
  vi.unstubAllGlobals();
  hash = '';
});

describe('moving to the new address', () => {
  it('carries the settings and places, never the sign-in, and keeps what the new address already has', async () => {
    storage.setItem(KEYS.favorites, '[{"name":"Sinaia","lat":45.35,"lon":25.55}]');
    storage.setItem(KEYS.theme, 'day');
    storage.setItem(KEYS.token, 'secret-session');
    const { url, ridesLeft } = await moveLink();
    expect(url.startsWith(`${NEW_ORIGIN}/#/drum?muta=`)).toBe(true);
    expect(ridesLeft).toBe(false);
    expect(url).not.toContain('secret');

    // The new address: empty, but the theme was already chosen there.
    storage.clear();
    storage.setItem(KEYS.theme, 'dark');
    hash = url.slice(url.indexOf('#'));
    await receiveMove();
    expect(storage.getItem(KEYS.favorites)).toContain('Sinaia');
    expect(storage.getItem(KEYS.theme)).toBe('dark');
    expect(storage.getItem(KEYS.token)).toBeNull();
    // The data leaves the address bar once read.
    expect(hash).toBe('#/drum');
  });

  it('does nothing without a move in the address, and survives a broken one', async () => {
    hash = '#/acum';
    await receiveMove();
    expect(hash).toBe('#/acum');
    hash = '#/drum?muta=not-really-data';
    await expect(receiveMove()).resolves.toBeUndefined();
    expect(storage.getItem(KEYS.favorites)).toBeNull();
  });
});
