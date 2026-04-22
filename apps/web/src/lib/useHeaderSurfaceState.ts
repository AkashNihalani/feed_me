'use client';

import { useEffect } from 'react';

const HEADER_SURFACE_SCROLL_THRESHOLD_PX = 2;
let activeHeaderSurfaceHooks = 0;

export function useHeaderSurfaceState(useBrowserPageScroll: boolean) {
  useEffect(() => {
    const html = document.documentElement;
    const forceOnSurface = !useBrowserPageScroll;

    let rafId: number | null = null;
    let lastValue: boolean | null = null;

    activeHeaderSurfaceHooks += 1;

    const apply = () => {
      rafId = null;
      const scrolled = window.scrollY > HEADER_SURFACE_SCROLL_THRESHOLD_PX
        || window.pageYOffset > HEADER_SURFACE_SCROLL_THRESHOLD_PX;
      const onSurface = forceOnSurface || scrolled;
      if (onSurface === lastValue) return;
      lastValue = onSurface;
      html.dataset.headerOnSurface = onSurface ? 'true' : 'false';
    };

    const schedule = () => {
      if (rafId != null) return;
      rafId = window.requestAnimationFrame(apply);
    };

    apply();
    window.addEventListener('scroll', schedule, { passive: true });
    window.visualViewport?.addEventListener('scroll', schedule);

    return () => {
      if (rafId != null) window.cancelAnimationFrame(rafId);
      window.removeEventListener('scroll', schedule);
      window.visualViewport?.removeEventListener('scroll', schedule);
      activeHeaderSurfaceHooks = Math.max(0, activeHeaderSurfaceHooks - 1);
      if (activeHeaderSurfaceHooks === 0) {
        delete html.dataset.headerOnSurface;
      }
    };
  }, [useBrowserPageScroll]);
}
