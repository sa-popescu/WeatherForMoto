import { describe, expect, it } from 'vitest';
import { buildPayload, stripUrlTails, tabFromHash } from './errorReport';

const context = { hash: '#/drum', userAgent: 'Mozilla/5.0 (iPhone)' };

describe('error reports', () => {
  it('names the tab and nothing after it', () => {
    expect(tabFromHash('#/drum')).toBe('drum');
    expect(tabFromHash('#/traseu?r=44.43,26.10;45.6,25.5')).toBe('traseu');
    expect(tabFromHash('#/ture/123')).toBe('ture');
    expect(tabFromHash('')).toBe('');
    expect(tabFromHash('#/<script>')).toBe('');
  });

  it('cuts query strings and fragments off every URL', () => {
    expect(stripUrlTails('at f (https://motonor.bluemouse.cc/assets/a.js?v=1#lat=44:1:2)')).toBe(
      'at f (https://motonor.bluemouse.cc/assets/a.js)',
    );
  });

  it('sends the message, a short stack, the version, the tab and the browser', () => {
    const error = new TypeError("Cannot read properties of undefined (reading 'lat')");
    error.stack = ['TypeError: boom', ...Array.from({ length: 20 }, (_, i) => `  at f${i} (https://x.test/a.js?t=${i}:1:1)`)].join('\n');
    const payload = buildPayload('error', error, context);
    expect(payload).not.toBeNull();
    expect(payload?.message).toBe("TypeError: Cannot read properties of undefined (reading 'lat')");
    expect(payload?.stack.split('\n')).toHaveLength(8);
    expect(payload?.stack).not.toContain('?t=');
    expect(payload?.tab).toBe('drum');
    // The build day and commit (lib/appVersion.ts): "2026.11.23+5fc4f24", "local" off CI.
    expect(payload?.version).toMatch(/^\d{4}\.\d{2}\.\d{2}\+[0-9a-z]{1,7}$/);
  });

  it('adds the component stack of a render error', () => {
    const payload = buildPayload('render', new Error('bad'), { ...context, extraStack: '\n    at RouteMap\n    at RouteScreen' });
    expect(payload?.stack).toContain('at RouteMap');
  });

  it('leaves out noise: the network, cancelled requests, extensions', () => {
    expect(buildPayload('rejection', new TypeError('Failed to fetch'), context)).toBeNull();
    expect(buildPayload('rejection', new TypeError('Load failed'), context)).toBeNull();
    expect(buildPayload('rejection', new DOMException('The operation was aborted.', 'AbortError'), context)).toBeNull();
    expect(buildPayload('error', 'Script error.', context)).toBeNull();
    const fromExtension = new Error('x');
    fromExtension.stack = 'Error: x\n at chrome-extension://abc/content.js:1:1';
    expect(buildPayload('error', fromExtension, context)).toBeNull();
  });

  it('reports a rejection that is not an Error', () => {
    expect(buildPayload('rejection', { code: 7 }, context)?.message).toBe('{"code":7}');
  });
});
