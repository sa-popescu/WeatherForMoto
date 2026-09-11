/// <reference types="vite/client" />
/// <reference types="vite-plugin-pwa/client" />

interface ImportMetaEnv {
  /** Base URL of the MotoMeteo API. "/api" in dev (proxied), absolute in production. */
  readonly VITE_API_BASE?: string;
}
