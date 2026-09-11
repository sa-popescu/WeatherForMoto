// Retires the old MotoMeteo service worker wherever this root file is served
// (GitHub Pages branch deploy): clears its caches, unregisters, and sends open
// windows to the app's new home. The app's real worker is built from app/src/sw.ts.
const APP_URL = 'https://weatherformoto.bluemouse.cc/';

self.addEventListener('install', () => self.skipWaiting());

self.addEventListener('activate', (event) => {
  event.waitUntil((async () => {
    const names = await caches.keys();
    await Promise.all(names.map((name) => caches.delete(name)));
    await self.registration.unregister();
    const windows = await self.clients.matchAll({ type: 'window' });
    windows.forEach((client) => client.navigate(APP_URL));
  })());
});
