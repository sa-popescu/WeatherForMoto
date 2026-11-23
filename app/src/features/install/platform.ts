import { isIosDevice } from '../account/lib/platform';

// Which device and browser the install guide talks to. Each one adds a web
// app to the home screen (or the computer) from a different place, so the
// steps follow from this. Read from the user agent only: nothing leaves the
// device.

export type IosBrowser = 'safari' | 'chrome' | 'edge' | 'firefox' | 'other';
export type AndroidBrowser = 'chrome' | 'samsung' | 'firefox' | 'edge' | 'other';
export type DesktopBrowser = 'chrome' | 'edge' | 'safari' | 'firefox' | 'other';

export type InstallPlatform =
  | { kind: 'ios'; browser: IosBrowser }
  | { kind: 'android'; browser: AndroidBrowser }
  | { kind: 'desktop'; browser: DesktopBrowser };

function iosBrowser(ua: string): IosBrowser {
  // Every browser on iOS is Safari underneath, with its own token added.
  if (/EdgiOS/i.test(ua)) return 'edge';
  if (/CriOS/i.test(ua)) return 'chrome';
  if (/FxiOS/i.test(ua)) return 'firefox';
  if (/OPiOS|OPT\/|Brave|DuckDuckGo|Mercury|GSA\//i.test(ua)) return 'other';
  return 'safari';
}

function androidBrowser(ua: string): AndroidBrowser {
  if (/SamsungBrowser/i.test(ua)) return 'samsung';
  if (/Firefox\//i.test(ua)) return 'firefox';
  if (/EdgA\//i.test(ua)) return 'edge';
  if (/OPR\/|Opera|YaBrowser|UCBrowser|MiuiBrowser|HuaweiBrowser/i.test(ua)) return 'other';
  if (/Chrome\//i.test(ua)) return 'chrome';
  return 'other';
}

function desktopBrowser(ua: string): DesktopBrowser {
  if (/Edg\//i.test(ua)) return 'edge';
  if (/Firefox\//i.test(ua)) return 'firefox';
  if (/OPR\/|Opera/i.test(ua)) return 'other';
  if (/Chrome\/|Chromium\//i.test(ua)) return 'chrome';
  if (/Safari\//i.test(ua)) return 'safari';
  return 'other';
}

export function detectPlatform(ua: string, platform: string, maxTouchPoints: number): InstallPlatform {
  if (isIosDevice(ua, platform, maxTouchPoints)) return { kind: 'ios', browser: iosBrowser(ua) };
  if (/Android/i.test(ua)) return { kind: 'android', browser: androidBrowser(ua) };
  return { kind: 'desktop', browser: desktopBrowser(ua) };
}

export function currentPlatform(): InstallPlatform {
  return detectPlatform(navigator.userAgent, navigator.platform, navigator.maxTouchPoints ?? 0);
}

/** Firefox on a computer has no way to install a web app. */
export function canInstallHere(p: InstallPlatform): boolean {
  return !(p.kind === 'desktop' && p.browser === 'firefox');
}
