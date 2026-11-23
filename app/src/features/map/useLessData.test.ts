import { afterEach, describe, expect, it, vi } from 'vitest';
import { prefersLessData } from './useLessData';

function withConnection(hints: object | undefined): void {
  vi.stubGlobal('navigator', { connection: hints });
}

describe('prefersLessData', () => {
  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('follows the data saver and a very slow connection', () => {
    withConnection({ saveData: true });
    expect(prefersLessData()).toBe(true);
    withConnection({ saveData: false, effectiveType: '2g' });
    expect(prefersLessData()).toBe(true);
    withConnection({ saveData: false, effectiveType: '4g' });
    expect(prefersLessData()).toBe(false);
  });

  it('reads the band as usual where the browser gives no hint', () => {
    withConnection(undefined);
    expect(prefersLessData()).toBe(false);
  });
});
