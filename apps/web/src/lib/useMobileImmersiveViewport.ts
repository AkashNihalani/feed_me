'use client';

import { CSSProperties, useEffect, useMemo, useState } from 'react';
import { getVisualViewportEventTarget } from './visualViewport';
import { acquireDocumentClass, acquireRootPageScroll } from './rootScrollMode';

function isStandaloneDisplayMode(): boolean {
  if (typeof window === 'undefined') return false;
  return window.matchMedia('(display-mode: standalone)').matches
    || Boolean((window.navigator as Navigator & { standalone?: boolean }).standalone);
}

export function useMobileImmersiveViewport() {
  const [isStandaloneMode, setIsStandaloneMode] = useState(false);
  const [isDesktopViewport, setIsDesktopViewport] = useState(false);
  const useBrowserPageScroll = !isDesktopViewport;
  const useTranslucentBrowserChrome = useBrowserPageScroll && !isStandaloneMode;

  useEffect(() => {
    const standaloneMql = window.matchMedia('(display-mode: standalone)');
    const desktopMql = window.matchMedia('(min-width: 1024px)');
    const syncMode = () => {
      setIsStandaloneMode(isStandaloneDisplayMode());
      setIsDesktopViewport(desktopMql.matches);
    };

    syncMode();
    standaloneMql.addEventListener?.('change', syncMode);
    desktopMql.addEventListener?.('change', syncMode);
    window.addEventListener('resize', syncMode);

    return () => {
      standaloneMql.removeEventListener?.('change', syncMode);
      desktopMql.removeEventListener?.('change', syncMode);
      window.removeEventListener('resize', syncMode);
    };
  }, []);

  useEffect(() => {
    if (!useBrowserPageScroll) return undefined;
    return acquireRootPageScroll();
  }, [useBrowserPageScroll]);

  useEffect(() => {
    if (!useTranslucentBrowserChrome) return undefined;
    return acquireDocumentClass('fm-mobile-browser-immersive');
  }, [useTranslucentBrowserChrome]);

  useEffect(() => {
    const syncViewportMetrics = () => {
      const standalone = isStandaloneDisplayMode();
      const vv = window.visualViewport;
      const visualHeight = Math.round(vv?.height ?? 0);
      const innerHeight = Math.round(window.innerHeight);
      const clientHeight = Math.round(document.documentElement.clientHeight);
      const nextHeight = standalone
        ? Math.max(innerHeight, clientHeight, visualHeight)
        : visualHeight || innerHeight || clientHeight;
      const offsetTop = Math.max(0, Math.round(vv?.offsetTop ?? 0));

      document.documentElement.style.setProperty('--fm-app-height', `${nextHeight}px`);
      document.documentElement.style.setProperty('--pwa-top-pad', standalone ? `${offsetTop + 8}px` : '0px');
      document.documentElement.style.setProperty('--pwa-top-fix', standalone ? `${-(offsetTop + 6)}px` : '0px');
    };

    const standaloneMql = window.matchMedia('(display-mode: standalone)');
    const viewport = getVisualViewportEventTarget();
    syncViewportMetrics();
    standaloneMql.addEventListener?.('change', syncViewportMetrics);
    viewport?.addEventListener('resize', syncViewportMetrics);
    viewport?.addEventListener('scroll', syncViewportMetrics);
    window.addEventListener('resize', syncViewportMetrics);

    return () => {
      standaloneMql.removeEventListener?.('change', syncViewportMetrics);
      viewport?.removeEventListener('resize', syncViewportMetrics);
      viewport?.removeEventListener('scroll', syncViewportMetrics);
      window.removeEventListener('resize', syncViewportMetrics);
    };
  }, []);

  const appShellStyle = useMemo(() => {
    const shellHeight = useTranslucentBrowserChrome ? '100lvh' : 'var(--fm-app-height, 100dvh)';
    return {
      minHeight: shellHeight,
      height: useBrowserPageScroll ? undefined : shellHeight,
    } satisfies CSSProperties;
  }, [useBrowserPageScroll, useTranslucentBrowserChrome]);

  return {
    appShellStyle,
    isDesktopViewport,
    isStandaloneMode,
    useBrowserPageScroll,
    useTranslucentBrowserChrome,
  };
}
