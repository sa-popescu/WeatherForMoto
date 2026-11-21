import { afterEach, describe, expect, it, vi } from 'vitest';
import { keepPhonesUpright } from './orientation';

function device(opts: { width: number; height: number; standalone: boolean; lock?: () => Promise<void> }) {
  vi.stubGlobal('window', {
    matchMedia: (query: string) => ({ matches: query === '(display-mode: standalone)' && opts.standalone }),
  });
  vi.stubGlobal('screen', { width: opts.width, height: opts.height, orientation: { lock: opts.lock } });
}

afterEach(() => {
  vi.unstubAllGlobals();
});

describe('keepPhonesUpright', () => {
  it('locks an installed app on a phone to portrait', () => {
    const lock = vi.fn(() => Promise.resolve());
    device({ width: 412, height: 915, standalone: true, lock });
    keepPhonesUpright();
    expect(lock).toHaveBeenCalledWith('portrait');
  });

  it('leaves a tablet free to turn, whichever way it is held', () => {
    const lock = vi.fn(() => Promise.resolve());
    device({ width: 1280, height: 800, standalone: true, lock });
    keepPhonesUpright();
    expect(lock).not.toHaveBeenCalled();
  });

  it('leaves a browser tab alone', () => {
    const lock = vi.fn(() => Promise.resolve());
    device({ width: 390, height: 844, standalone: false, lock });
    keepPhonesUpright();
    expect(lock).not.toHaveBeenCalled();
  });

  it('shrugs off a browser that refuses or lacks the lock', async () => {
    const lock = vi.fn(() => Promise.reject(new Error('NotSupportedError')));
    device({ width: 390, height: 844, standalone: true, lock });
    expect(() => keepPhonesUpright()).not.toThrow();
    await Promise.resolve();
    device({ width: 390, height: 844, standalone: true });
    expect(() => keepPhonesUpright()).not.toThrow();
  });
});
