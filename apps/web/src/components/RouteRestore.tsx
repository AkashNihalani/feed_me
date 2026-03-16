'use client';

import { useEffect } from 'react';
import { usePathname, useRouter } from 'next/navigation';

const STORAGE_KEY = 'feedme:last-path';
const STORAGE_TIME = 'feedme:last-path-ts';

function isReload(): boolean {
  if (typeof window === 'undefined') return false;
  const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined;
  return nav?.type === 'reload';
}

export default function RouteRestore() {
  const pathname = usePathname();
  const router = useRouter();

  useEffect(() => {
    if (!pathname) return;
    if (pathname === '/login') return;
    const existing = sessionStorage.getItem(STORAGE_KEY);
    if (pathname === '/' && existing && existing !== '/') return;
    const next = `${pathname}${window.location.search || ''}`;
    sessionStorage.setItem(STORAGE_KEY, next);
    sessionStorage.setItem(STORAGE_TIME, String(Date.now()));
  }, [pathname]);

  useEffect(() => {
    if (pathname !== '/') return;
    const last = sessionStorage.getItem(STORAGE_KEY);
    if (!last || last === '/' || last === '/login') return;
    const ts = Number(sessionStorage.getItem(STORAGE_TIME) || 0);
    if (Date.now() - ts > 15000 && !isReload()) return;
    router.replace(last);
  }, [pathname, router]);

  return null;
}
