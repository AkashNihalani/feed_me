self.addEventListener('install', (event) => {
  event.waitUntil(self.skipWaiting());
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

self.addEventListener('push', (event) => {
  const fallback = {
    body: 'Fresh fire alerts crossed your threshold.',
    tag: 'feedme-fire-alert',
    title: 'FeedMe alert',
    url: '/fire',
  };

  let payload = fallback;
  if (event.data) {
    try {
      const parsed = event.data.json();
      payload = {
        ...fallback,
        ...parsed,
      };
    } catch {
      payload = {
        ...fallback,
        body: event.data.text() || fallback.body,
      };
    }
  }

  event.waitUntil(
    self.registration.showNotification(payload.title, {
      badge: '/icons/icon-192.png',
      body: payload.body,
      data: { url: payload.url || '/fire' },
      icon: '/icons/icon-192.png',
      tag: payload.tag || fallback.tag,
    })
  );
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();

  const targetUrl = event.notification.data && event.notification.data.url
    ? event.notification.data.url
    : '/fire';

  event.waitUntil((async () => {
    const clients = await self.clients.matchAll({ includeUncontrolled: true, type: 'window' });

    for (const client of clients) {
      if ('focus' in client) {
        if ('navigate' in client) {
          await client.navigate(targetUrl);
        }
        await client.focus();
        return;
      }
    }

    if (self.clients.openWindow) {
      await self.clients.openWindow(targetUrl);
    }
  })());
});
