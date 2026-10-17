'use client';

import { RefObject, useEffect, useState } from 'react';

// Small noise floor so a stray sub-pixel scroll event never flips direction.
const DIRECTION_NOISE = 4;

/* A jump the page makes itself (Feed's zoom or new pick landing somewhere else on the page) is not a direction: the
   header never folds or unfolds because of which way it went. 'keep': the header stays as it is; 'place': it shows
   where the page landed (folded in the posts, open near the top). Open at the very top either way. The scroll
   events within a moment of the mark are the jump's.
   `at`: where the page is landing, for a jump made later than it is seen (Feed holds the scroll through a move on
   WebKit and hands it over when it lands): the header answers now, as if the page were already there */
export type ScrollJump = 'keep' | 'place';
let jump: { mode: ScrollJump; until: number; at: number | null } | null = null;
const jumpListeners = new Set<() => void>();

export function markScrollJump(mode: ScrollJump = 'keep', ms = 180, at: number | null = null) {
  jump = { mode, until: performance.now() + ms, at };
  if (at != null) jumpListeners.forEach((listener) => listener());
}

type CollapseOptions = {
  // How far you must scroll DOWN (px) before the header collapses.
  collapseDistance?: number;
  // How far you must scroll UP (px) before it expands again.
  expandDistance?: number;
  // Always-expanded zone near the very top.
  topGuard?: number;
};

/* The fold itself, outside React: follows `source`'s scroll (once a frame while it moves) and calls `onChange` only
   when it flips (and once at the start, with where it begins). For what folds without re-rendering anything (the
   phone's bottom bar writes it straight onto its element). `reset` puts it open or folded where the page is now,
   measuring from there; `stop` lets go of the scroll */
export type ScrollFold = { reset: (folded: boolean) => void; stop: () => void };

export function watchScrollFold(source: HTMLElement | Window, options: CollapseOptions, onChange: (folded: boolean) => void): ScrollFold {
  const { collapseDistance, expandDistance, topGuard } = options;
  const read = () => (source === window ? window.scrollY : (source as HTMLElement).scrollTop);
  const viewport = () => window.innerHeight || 800;

  let raf = 0;
  let state = false;
  let told: boolean | null = null;
  let lastY = read();
  // Anchor = the scroll position of the last direction reversal. Distance is
  // measured from here, so a flick-and-hold doesn't accumulate forever.
  let anchor = lastY;
  let dir = 0; // -1 up, 1 down, 0 idle

  const tell = () => {
    if (told === state) return;
    told = state;
    onChange(state);
  };

  const sync = () => {
    const collapseAt = collapseDistance ?? viewport() * 1.15;
    const expandAt = expandDistance ?? viewport() * 0.6;
    const guard = topGuard ?? 56;

    const live = jump && performance.now() < jump.until ? jump : null;
    const y = live?.at ?? read();
    const delta = y - lastY;

    if (live) {
      // the page's own jump: no direction taken from it, the next scroll measures from here
      if (y <= guard) state = false;
      else if (live.mode === 'place') state = y > guard + collapseAt;
      anchor = y;
      lastY = y;
      dir = 0;
    } else if (y <= guard) {
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
    tell();
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
  jumpListeners.add(sync);

  return {
    reset: (folded) => {
      state = folded;
      lastY = read();
      anchor = lastY;
      dir = 0;
      tell();
    },
    stop: () => {
      source.removeEventListener('scroll', onScroll);
      jumpListeners.delete(sync);
      if (raf) window.cancelAnimationFrame(raf);
    },
  };
}

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

    const fold = watchScrollFold(source, { collapseDistance, expandDistance, topGuard }, setCompressed);
    return () => fold.stop();
  }, [scrollRef, useBrowserPageScroll, collapseDistance, expandDistance, topGuard]);

  return compressed;
}
