import type { IconName } from '../../ui/icons';
import type { InstallPlatform } from './platform';
import type { INSTALL_S } from './strings';

// The steps that add Motonor to the home screen (or the computer) on each
// device and browser, for when the browser offers no install button of its
// own. Empty where it cannot be installed (Firefox on a computer).

type InstallText = keyof (typeof INSTALL_S)['ro'];

export interface InstallStep {
  text: InstallText;
  icon: IconName;
}

const IOS_END: InstallStep[] = [
  { text: 'iosAdd', icon: 'addSquare' },
  { text: 'iosConfirm', icon: 'check' },
];

const ANDROID_END: InstallStep[] = [
  { text: 'androidAdd', icon: 'download' },
  { text: 'androidConfirm', icon: 'check' },
];

export function installSteps(p: InstallPlatform): InstallStep[] {
  if (p.kind === 'ios') {
    const share: InstallText = p.browser === 'safari' ? 'iosShareSafari' : p.browser === 'chrome' ? 'iosShareChrome' : 'iosShareOther';
    return [{ text: share, icon: 'iosShare' }, ...IOS_END];
  }
  if (p.kind === 'android') {
    const menu: InstallText =
      p.browser === 'chrome'
        ? 'androidMenuChrome'
        : p.browser === 'samsung' || p.browser === 'edge'
          ? 'androidMenuBottom'
          : p.browser === 'firefox'
            ? 'androidMenuFirefox'
            : 'androidMenuOther';
    return [{ text: menu, icon: 'settings' }, ...ANDROID_END];
  }
  switch (p.browser) {
    case 'chrome':
    case 'edge':
      return [
        { text: 'desktopIcon', icon: 'download' },
        { text: 'desktopConfirm', icon: 'check' },
      ];
    case 'safari':
      return [
        { text: 'safariDock', icon: 'addSquare' },
        { text: 'safariConfirm', icon: 'check' },
      ];
    case 'firefox':
      return [];
    default:
      return [{ text: 'desktopOther', icon: 'download' }];
  }
}
