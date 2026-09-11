import { api } from './api';

// Web Push (VAPID) helpers. Native Capacitor push (FCM/APNs) is a separate
// integration and not handled here.

export function pushSupported(): boolean {
  return 'serviceWorker' in navigator && 'PushManager' in window && 'Notification' in window;
}

function urlBase64ToUint8Array(base64: string): Uint8Array<ArrayBuffer> {
  const padding = '='.repeat((4 - (base64.length % 4)) % 4);
  const raw = atob((base64 + padding).replace(/-/g, '+').replace(/_/g, '/'));
  const out = new Uint8Array(new ArrayBuffer(raw.length));
  for (let i = 0; i < raw.length; i += 1) out[i] = raw.charCodeAt(i);
  return out;
}

export async function currentPushSubscription(): Promise<PushSubscription | null> {
  if (!pushSupported()) return null;
  const registration = await navigator.serviceWorker.getRegistration();
  return registration ? registration.pushManager.getSubscription() : null;
}

export type EnablePushResult = 'enabled' | 'denied' | 'unsupported';

export async function enablePush(token: string): Promise<EnablePushResult> {
  if (!pushSupported()) return 'unsupported';
  const permission = await Notification.requestPermission();
  if (permission !== 'granted') return 'denied';
  const registration = await navigator.serviceWorker.ready;
  const keys = await api.pushPublicKey();
  const publicKey = keys.publicKey ?? keys.public_key;
  if (!publicKey) throw new Error('Server returned no VAPID public key');
  const subscription =
    (await registration.pushManager.getSubscription()) ??
    (await registration.pushManager.subscribe({ userVisibleOnly: true, applicationServerKey: urlBase64ToUint8Array(publicKey) }));
  const json = subscription.toJSON();
  await api.subscribePush(token, {
    endpoint: subscription.endpoint,
    keys: { p256dh: json.keys?.p256dh ?? '', auth: json.keys?.auth ?? '' },
  });
  return 'enabled';
}

/** Removes this device's subscription on the server (when signed in) and in the browser. */
export async function disablePush(token: string | null): Promise<void> {
  const subscription = await currentPushSubscription();
  if (!subscription) return;
  if (token) {
    try {
      await api.unsubscribePush(token, subscription.endpoint);
    } catch (err) {
      console.warn('[push] server unsubscribe failed; removing the local subscription anyway', err);
    }
  }
  await subscription.unsubscribe();
}
