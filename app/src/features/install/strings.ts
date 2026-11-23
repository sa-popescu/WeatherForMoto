import { defineStrings } from '../../lib/i18n';

// Copy of the install invitation (InstallInvite.tsx) and guide (InstallGuide.tsx).
// The steps quote the menu items the way the browsers name them; where a
// browser names them differently by version, the step says what to look for.
export const INSTALL_S = defineStrings(
  {
    inviteTitle: 'Instalează Motonor',
    inviteLead: 'Se deschide din ecranul principal, pe tot ecranul, și merge și fără semnal.',
    inviteLeadIos: 'Se deschide din ecranul principal, pe tot ecranul, și merge și fără semnal. Pe iPhone, alertele de vreme vin doar în aplicația instalată.',
    inviteLeadDesktop: 'Pe drum o ai la îndemână pe telefon: scanezi un cod și o instalezi de acolo. Merge instalată și pe calculator.',
    inviteLeadPhoneOnly: 'Pe drum o ai la îndemână pe telefon: scanezi un cod și o instalezi de acolo.',
    install: 'Instalează',
    howTo: 'Arată-mi cum',
    notNow: 'Nu acum',
    installing: 'Gata, Motonor se instalează.',
    installFailed: 'Instalarea nu a pornit. Urmează pașii de mai jos.',

    guideTitle: 'Instalează Motonor',
    guideLead: 'O adaugi pe ecranul principal și se deschide ca o aplicație: pe tot ecranul, fără bara browserului, și merge și fără semnal.',
    guideLeadDesktop: 'O adaugi pe calculator și se deschide ca o aplicație, în fereastra ei.',
    installNow: 'Instalează acum',

    iosShareSafari: 'Apasă Partajează, în bara de jos a Safari. Pe iPad e sus; pe iOS 26 îl găsești în meniul ⋯.',
    iosShareChrome: 'Apasă Partajează, în bara de adresă, sus în dreapta.',
    iosShareOther: 'Deschide meniul browserului (⋯ sau ☰) și apasă Partajează.',
    iosAdd: 'Alege „Adaugă pe ecranul principal”. Dacă nu-l vezi, derulează lista în jos.',
    iosConfirm: 'Apasă „Adaugă”, sus în dreapta. Motonor apare pe ecran, lângă celelalte aplicații.',
    iosNote: 'Merge din Safari, Chrome, Edge și Firefox. Pe iOS mai vechi de 16.4, doar din Safari.',

    androidMenuChrome: 'Apasă meniul ⋮, sus în dreapta.',
    androidMenuBottom: 'Apasă meniul din bara de jos (☰ sau ⋯).',
    androidMenuFirefox: 'Apasă meniul ⋮, sus sau jos, după unde ai bara de adresă.',
    androidMenuOther: 'Deschide meniul browserului.',
    androidAdd: 'Alege „Instalează aplicația” sau „Adaugă pe ecranul de pornire” (la unele browsere: „Adaugă pagina la”, apoi ecranul de pornire).',
    androidConfirm: 'Confirmă. Motonor apare pe ecran, lângă celelalte aplicații.',

    desktopIcon: 'În dreapta barei de adresă, apasă iconița de instalare. Dacă nu apare, caută „Instalează” în meniul browserului (⋮ sau ⋯).',
    desktopConfirm: 'Confirmă cu „Instalează”. Motonor se deschide în fereastra lui și apare printre aplicațiile calculatorului.',
    safariDock: 'Din meniul Fișier, alege „Adaugă în Dock” (pe macOS 14 sau mai nou).',
    safariConfirm: 'Apasă „Adaugă”. Motonor apare în Dock.',
    desktopOther: 'Din meniul browserului, alege instalarea aplicației.',
    firefoxDesktop: 'Firefox pe calculator nu poate instala aplicații web. Deschide Motonor în Chrome sau Edge, sau pune-l pe telefon cu codul de mai jos.',

    phoneTitle: 'Pe telefon',
    phoneLead: 'Scanează codul cu camera telefonului, deschide linkul și instalează Motonor de acolo.',
    qrLabel: 'Cod QR cu adresa {url}',
  },
  {
    inviteTitle: 'Install Motonor',
    inviteLead: 'It opens from the home screen, full screen, and works without signal too.',
    inviteLeadIos: 'It opens from the home screen, full screen, and works without signal too. On iPhone, weather alerts only reach the installed app.',
    inviteLeadDesktop: 'On the road it is at hand on your phone: scan a code and install it from there. It also works installed on a computer.',
    inviteLeadPhoneOnly: 'On the road it is at hand on your phone: scan a code and install it from there.',
    install: 'Install',
    howTo: 'Show me how',
    notNow: 'Not now',
    installing: 'Done, Motonor is installing.',
    installFailed: 'The install did not start. Follow the steps below.',

    guideTitle: 'Install Motonor',
    guideLead: 'Add it to the home screen and it opens like an app: full screen, without the browser bar, and it works without signal too.',
    guideLeadDesktop: 'Add it to the computer and it opens like an app, in its own window.',
    installNow: 'Install now',

    iosShareSafari: 'Tap Share, in the bottom bar of Safari. On iPad it is at the top; on iOS 26 it is in the ⋯ menu.',
    iosShareChrome: 'Tap Share, in the address bar, top right.',
    iosShareOther: 'Open the browser menu (⋯ or ☰) and tap Share.',
    iosAdd: 'Choose “Add to Home Screen”. If you do not see it, scroll the list down.',
    iosConfirm: 'Tap “Add”, top right. Motonor shows up on the screen, next to the other apps.',
    iosNote: 'Works in Safari, Chrome, Edge and Firefox. On iOS older than 16.4, only in Safari.',

    androidMenuChrome: 'Tap the ⋮ menu, top right.',
    androidMenuBottom: 'Tap the menu in the bottom bar (☰ or ⋯).',
    androidMenuFirefox: 'Tap the ⋮ menu, top or bottom, wherever your address bar is.',
    androidMenuOther: 'Open the browser menu.',
    androidAdd: 'Choose “Install app” or “Add to Home screen” (in some browsers: “Add page to”, then the home screen).',
    androidConfirm: 'Confirm. Motonor shows up on the screen, next to the other apps.',

    desktopIcon: 'On the right of the address bar, click the install icon. If it is not there, look for “Install” in the browser menu (⋮ or ⋯).',
    desktopConfirm: 'Confirm with “Install”. Motonor opens in its own window and joins the computer’s apps.',
    safariDock: 'From the File menu, choose “Add to Dock” (macOS 14 or newer).',
    safariConfirm: 'Click “Add”. Motonor shows up in the Dock.',
    desktopOther: 'From the browser menu, choose to install the app.',
    firefoxDesktop: 'Firefox on a computer cannot install web apps. Open Motonor in Chrome or Edge, or put it on your phone with the code below.',

    phoneTitle: 'On your phone',
    phoneLead: 'Scan the code with the phone’s camera, open the link and install Motonor from there.',
    qrLabel: 'QR code with the address {url}',
  },
);
