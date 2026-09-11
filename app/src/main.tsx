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
import { api } from './lib/api';
import { initServiceWorker } from './lib/pwa';
import { setScoringMeta } from './lib/scoring';
import { Providers } from './state/providers';

const rootElement = document.getElementById('root');
if (!rootElement) throw new Error('Missing #root element');

createRoot(rootElement).render(
  <StrictMode>
    <Providers>
      <App />
    </Providers>
  </StrictMode>,
);

initServiceWorker();

// Live scoring constants replace the bundled copy when the API answers.
api
  .metaScoring()
  .then(setScoringMeta)
  .catch((err: unknown) => console.warn('[scoring] using the bundled constants', err));
