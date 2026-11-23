import { describe, expect, it } from 'vitest';
import { afterShowing, inviteDue, MAX_SHOWS, REMIND_AFTER_MS } from './inviteState';
import { canInstallHere, detectPlatform } from './platform';
import { installSteps } from './steps';

const UA = {
  iphoneSafari:
    'Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Mobile/15E148 Safari/604.1',
  iphoneChrome:
    'Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/131.0.6778.73 Mobile/15E148 Safari/604.1',
  iphoneEdge:
    'Mozilla/5.0 (iPhone; CPU iPhone OS 18_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.0 EdgiOS/131.2903.68 Mobile/15E148 Safari/605.1.15',
  ipadAsMac: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
  androidChrome: 'Mozilla/5.0 (Linux; Android 14; Pixel 8) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Mobile Safari/537.36',
  androidSamsung:
    'Mozilla/5.0 (Linux; Android 14; SM-S911B) AppleWebKit/537.36 (KHTML, like Gecko) SamsungBrowser/26.0 Chrome/122.0.0.0 Mobile Safari/537.36',
  androidFirefox: 'Mozilla/5.0 (Android 14; Mobile; rv:132.0) Gecko/132.0 Firefox/132.0',
  windowsEdge: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36 Edg/131.0.0.0',
  windowsChrome: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36',
  windowsFirefox: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0',
  macSafari: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.1 Safari/605.1.15',
};

describe('install platform', () => {
  it('tells the iPhone browsers apart', () => {
    expect(detectPlatform(UA.iphoneSafari, 'iPhone', 5)).toEqual({ kind: 'ios', browser: 'safari' });
    expect(detectPlatform(UA.iphoneChrome, 'iPhone', 5)).toEqual({ kind: 'ios', browser: 'chrome' });
    expect(detectPlatform(UA.iphoneEdge, 'iPhone', 5)).toEqual({ kind: 'ios', browser: 'edge' });
  });

  it('knows an iPad that says it is a Mac by its touch points', () => {
    expect(detectPlatform(UA.ipadAsMac, 'MacIntel', 5)).toEqual({ kind: 'ios', browser: 'safari' });
    expect(detectPlatform(UA.macSafari, 'MacIntel', 0)).toEqual({ kind: 'desktop', browser: 'safari' });
  });

  it('tells the Android browsers apart', () => {
    expect(detectPlatform(UA.androidChrome, 'Linux armv8l', 5)).toEqual({ kind: 'android', browser: 'chrome' });
    expect(detectPlatform(UA.androidSamsung, 'Linux armv8l', 5)).toEqual({ kind: 'android', browser: 'samsung' });
    expect(detectPlatform(UA.androidFirefox, 'Linux armv8l', 5)).toEqual({ kind: 'android', browser: 'firefox' });
  });

  it('tells Edge from Chrome on a computer, and knows Firefox cannot install', () => {
    const edge = detectPlatform(UA.windowsEdge, 'Win32', 0);
    const firefox = detectPlatform(UA.windowsFirefox, 'Win32', 0);
    expect(edge).toEqual({ kind: 'desktop', browser: 'edge' });
    expect(detectPlatform(UA.windowsChrome, 'Win32', 0)).toEqual({ kind: 'desktop', browser: 'chrome' });
    expect(canInstallHere(edge)).toBe(true);
    expect(canInstallHere(firefox)).toBe(false);
  });
});

describe('install steps', () => {
  it('starts from Share on an iPhone, where each browser keeps it', () => {
    expect(installSteps({ kind: 'ios', browser: 'safari' }).map((s) => s.text)).toEqual(['iosShareSafari', 'iosAdd', 'iosConfirm']);
    expect(installSteps({ kind: 'ios', browser: 'chrome' })[0]).toEqual({ text: 'iosShareChrome', icon: 'iosShare' });
    expect(installSteps({ kind: 'ios', browser: 'firefox' })[0]?.text).toBe('iosShareOther');
  });

  it('starts from the menu on Android, top or bottom by browser', () => {
    expect(installSteps({ kind: 'android', browser: 'chrome' })[0]?.text).toBe('androidMenuChrome');
    expect(installSteps({ kind: 'android', browser: 'samsung' })[0]?.text).toBe('androidMenuBottom');
  });

  it('has no steps for Firefox on a computer and the Dock for Safari', () => {
    expect(installSteps({ kind: 'desktop', browser: 'firefox' })).toEqual([]);
    expect(installSteps({ kind: 'desktop', browser: 'safari' })[0]?.text).toBe('safariDock');
  });
});

describe('install invitation', () => {
  const day = 24 * 60 * 60 * 1000;

  it('shows at the first visit, once more a week later, then never', () => {
    const start = 1_800_000_000_000;
    expect(inviteDue(null, start)).toBe(true);
    const first = afterShowing(null, start);
    expect(first).toEqual({ shown: 1, lastAt: start });
    expect(inviteDue(first, start + 6 * day)).toBe(false);
    expect(inviteDue(first, start + REMIND_AFTER_MS)).toBe(true);
    const second = afterShowing(first, start + REMIND_AFTER_MS);
    expect(second.shown).toBe(MAX_SHOWS);
    expect(inviteDue(second, start + 365 * day)).toBe(false);
  });
});
