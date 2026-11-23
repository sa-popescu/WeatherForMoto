import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
// Only the Latin subsets (latin-ext carries ă, â, î, ș, ț): the full sets would
// add Cyrillic, Greek and Vietnamese files to the offline precache.
import '@fontsource/chakra-petch/latin-500.css';
import '@fontsource/chakra-petch/latin-ext-500.css';
import '@fontsource/chakra-petch/latin-600.css';
import '@fontsource/chakra-petch/latin-ext-600.css';
import '@fontsource/chakra-petch/latin-700.css';
import '@fontsource/chakra-petch/latin-ext-700.css';
import '@fontsource/ibm-plex-sans/latin-400.css';
import '@fontsource/ibm-plex-sans/latin-ext-400.css';
import '@fontsource/ibm-plex-sans/latin-500.css';
import '@fontsource/ibm-plex-sans/latin-ext-500.css';
import '@fontsource/ibm-plex-sans/latin-600.css';
import '@fontsource/ibm-plex-sans/latin-ext-600.css';
import './styles/tokens.css';
import './styles/base.css';
import './ui/ui.css';
import App from './App';
// Registers the beforeinstallprompt listener at start-up, before the Eu tab is ever opened.
import './features/account/installPrompt';
import { receiveMove } from './features/move/move';
import { pruneOfflineMaps } from './features/route/offlineMapStore';
import { api } from './lib/api';
import { showCrashScreen } from './lib/crashScreen';
import { installErrorReporting, reportError } from './lib/errorReport';
import { keepPhonesUpright } from './lib/orientation';
import { initServiceWorker } from './lib/pwa';
import { setScoringMeta } from './lib/scoring';
import { claimStaleReload, isReloadingToNewBuild, isStaleFileError, reloadToNewBuild, watchStaleFiles } from './lib/staleBuild';
import { Providers } from './state/providers';

installErrorReporting();
watchStaleFiles();

const rootElement = document.getElementById('root');
if (!rootElement) throw new Error('Missing #root element');

// An error while drawing that nothing caught takes the whole app down: it is
// reported, and a note with a reload button takes the empty screen's place.
// A screen's file missing after a deploy is not a bug: the page loads the new
// build instead, once a minute at most (lib/staleBuild.ts).
const onUncaughtError = (error: unknown, info: { componentStack?: string }): void => {
  if (isReloadingToNewBuild()) return;
  if (isStaleFileError(error) && claimStaleReload()) {
    console.warn('[app] a file of an older build is gone, loading the new build', error);
    void reloadToNewBuild();
    return;
  }
  console.error('[app] uncaught render error', error);
  reportError('render', error, info.componentStack);
  showCrashScreen();
};

// What a move from the old address carries is taken in before anything reads
// the settings (features/move); without one this returns at once.
void receiveMove().finally(() => {
  createRoot(rootElement, { onUncaughtError }).render(
    <StrictMode>
      <Providers>
        <App />
      </Providers>
    </StrictMode>,
  );
});

initServiceWorker();
keepPhonesUpright();
// Route maps kept for offline use go after 30 days (features/route/offlineMapStore.ts).
void pruneOfflineMaps();

// Live scoring constants replace the bundled copy when the API answers.
api
  .metaScoring()
  .then(setScoringMeta)
  .catch((err: unknown) => console.warn('[scoring] using the bundled constants', err));
