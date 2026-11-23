/// <reference types="vite/client" />
/// <reference types="vite-plugin-pwa/client" />

interface ImportMetaEnv {
  /** Base URL of the Motonor API. "/api" in dev (proxied), absolute in production. */
  readonly VITE_API_BASE?: string;
}

/** Short commit of the build (Workers Builds or Vercel), "local" elsewhere (vite.config.ts). */
declare const __APP_BUILD__: string;
/** The day of the build on Romanian time, "2026.11.23" (vite.config.ts). */
declare const __APP_DATE__: string;
