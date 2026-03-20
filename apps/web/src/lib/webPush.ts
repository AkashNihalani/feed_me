export type PushSubscriptionPayload = {
  endpoint: string;
  expirationTime?: number | null;
  keys: {
    auth: string;
    p256dh: string;
  };
};

function urlBase64ToUint8Array(base64String: string) {
  const padding = '='.repeat((4 - (base64String.length % 4)) % 4);
  const normalized = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
  const rawData = window.atob(normalized);
  return Uint8Array.from(rawData, (char) => char.charCodeAt(0));
}

async function readJsonError(response: Response, fallback: string) {
  try {
    const data = await response.json() as { error?: string };
    return data.error || fallback;
  } catch {
    return fallback;
  }
}

export async function ensureServiceWorkerRegistration() {
  if (!('serviceWorker' in navigator)) {
    throw new Error('Service workers are not supported in this browser');
  }

  await navigator.serviceWorker.register('/sw.js');
  return navigator.serviceWorker.ready;
}

export async function getCurrentPushSubscription() {
  const registration = await ensureServiceWorkerRegistration();
  return registration.pushManager.getSubscription();
}

async function fetchPublicKey() {
  const response = await fetch('/api/push/public-key', { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(await readJsonError(response, 'Web Push is not configured'));
  }

  const data = await response.json() as { publicKey?: string };
  if (!data.publicKey) {
    throw new Error('Web Push public key is missing');
  }

  return data.publicKey;
}

function parseSubscriptionPayload(subscription: PushSubscription): PushSubscriptionPayload {
  const json = subscription.toJSON();
  const endpoint = typeof json.endpoint === 'string' ? json.endpoint.trim() : '';
  const p256dh = typeof json.keys?.p256dh === 'string' ? json.keys.p256dh.trim() : '';
  const auth = typeof json.keys?.auth === 'string' ? json.keys.auth.trim() : '';

  if (!endpoint || !p256dh || !auth) {
    throw new Error('Push subscription is missing required browser keys');
  }

  return {
    endpoint,
    expirationTime: json.expirationTime ?? null,
    keys: { auth, p256dh },
  };
}

export async function subscribeToWebPush() {
  const registration = await ensureServiceWorkerRegistration();
  let subscription = await registration.pushManager.getSubscription();

  if (!subscription) {
    const publicKey = await fetchPublicKey();
    subscription = await registration.pushManager.subscribe({
      applicationServerKey: urlBase64ToUint8Array(publicKey),
      userVisibleOnly: true,
    });
  }

  const payload = parseSubscriptionPayload(subscription);
  const response = await fetch('/api/push/subscription', {
    body: JSON.stringify(payload),
    headers: { 'Content-Type': 'application/json' },
    method: 'POST',
  });

  if (!response.ok) {
    throw new Error(await readJsonError(response, 'Failed to save push subscription'));
  }

  return subscription;
}

export async function unsubscribeFromWebPush() {
  if (!('serviceWorker' in navigator)) {
    return;
  }

  const registration = await ensureServiceWorkerRegistration();
  const subscription = await registration.pushManager.getSubscription();
  const endpoint = subscription?.endpoint;

  if (endpoint) {
    const response = await fetch('/api/push/subscription', {
      body: JSON.stringify({ endpoint }),
      headers: { 'Content-Type': 'application/json' },
      method: 'DELETE',
    });

    if (!response.ok) {
      throw new Error(await readJsonError(response, 'Failed to remove push subscription'));
    }
  }

  if (subscription) {
    await subscription.unsubscribe();
  }
}
