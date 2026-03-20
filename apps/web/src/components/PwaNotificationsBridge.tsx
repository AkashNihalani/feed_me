'use client';

import { useEffect } from 'react';
import { ensureServiceWorkerRegistration } from '@/lib/webPush';

export default function PwaNotificationsBridge() {
  useEffect(() => {
    ensureServiceWorkerRegistration().catch((error) => {
      console.error('[pwa-notifications] Service worker registration failed', error);
    });
  }, []);

  return null;
}
