'use client';

import { RefObject, useEffect, useState } from 'react';

// Small noise floor so a stray sub-pixel scroll event never flips direction.
const DIRECTION_NOISE = 4;

type CollapseOptions = {
  // How far you must scroll DOWN (px) before the header collapses.
  collapseDistance?: number;
  // How far you must scroll UP (px) before it expands again.
  expandDistance?: number;
  // Always-expanded zone near the very top.
  topGuard?: number;
};

/**
 * Direction-aware, distance-based header collapse.
 *
 * Returns a boolean that flips to `true` after the user has scrolled DOWN a
 * meaningful distance and back to `false` after scrolling UP a shorter distance.
 * The asymmetry + distance gate is what makes it feel deliberate instead of
 * twitchy.
 *
 * The state is a coarse boolean on purpose: the page animates the morph on a
 * timed curve when this flips, and freezes its content offset so the resize
 * doesn't stall momentum scroll.
 */
export function useCompressedOnScroll(
  scrollRef: RefObject<HTMLElement | null>,
  useBrowserPageScroll: boolean,
  options: CollapseOptions = {},
) {
  const [compressed, setCompressed] = useState(false);
  const { collapseDistance, expandDistance, topGuard } = options;

  useEffect(() => {
    if (typeof window === 'undefined') return;

    const source: HTMLElement | Window | null = useBrowserPageScroll
      ? window
      : scrollRef.current;
    if (!source) {
      const reset = window.requestAnimationFrame(() => setCompressed(false));
      return () => window.cancelAnimationFrame(reset);
    }

    const read = () => (
      source === window
        ? window.scrollY
        : (source as HTMLElement).scrollTop
    );

    const viewport = () => window.innerHeight || 800;

    let raf = 0;
    let state = false;
    let lastY = read();
    // Anchor = the scroll position of the last direction reversal. Distance is
    // measured from here, so a flick-and-hold doesn't accumulate forever.
    let anchor = lastY;
    let dir = 0; // -1 up, 1 down, 0 idle

    const sync = () => {
      const collapseAt = collapseDistance ?? viewport() * 1.15;
      const expandAt = expandDistance ?? viewport() * 0.6;
      const guard = topGuard ?? 56;

      const y = read();
      const delta = y - lastY;

      if (y <= guard) {
        // Always fully expanded at the top.
        state = false;
        anchor = y;
        dir = 0;
      } else if (delta > DIRECTION_NOISE) {
        // Scrolling down.
        if (dir <= 0) { dir = 1; anchor = lastY; }
        if (!state && y - anchor > collapseAt) state = true;
      } else if (delta < -DIRECTION_NOISE) {
        // Scrolling up.
        if (dir >= 0) { dir = -1; anchor = lastY; }
        if (state && anchor - y > expandAt) state = false;
      }

      if (Math.abs(delta) > DIRECTION_NOISE) lastY = y;
      setCompressed((current) => (current === state ? current : state));
    };

    const onScroll = () => {
      if (raf) return;
      raf = window.requestAnimationFrame(() => {
        raf = 0;
        sync();
      });
    };

    sync();
    source.addEventListener('scroll', onScroll, { passive: true });

    return () => {
      source.removeEventListener('scroll', onScroll);
      if (raf) window.cancelAnimationFrame(raf);
    };
  }, [scrollRef, useBrowserPageScroll, collapseDistance, expandDistance, topGuard]);

  return compressed;
}
