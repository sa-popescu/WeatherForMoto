// Which version of the app this is: the day it was built (Romanian time) and
// the short commit, both set by vite.config.ts at build time. Every deploy
// changes it on its own, and the commit matches the one in error reports.

/** Shown in Eu, Aplicație: "2026.11.23 · 5fc4f24". */
export const APP_VERSION = `${__APP_DATE__} · ${__APP_BUILD__}`;

/** Sent with error reports, without spaces: "2026.11.23+5fc4f24". */
export const APP_BUILD = `${__APP_DATE__}+${__APP_BUILD__}`;
