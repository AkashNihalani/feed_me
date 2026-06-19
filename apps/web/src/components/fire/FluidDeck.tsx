'use client';

import { memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AnimatePresence, motion, type PanInfo } from 'framer-motion';
import { Loader2 } from 'lucide-react';
import { FireItem } from './types';
import { FireCard3D } from './FireCard3D';
import { GRID_LAYOUT_SPRING, GRID_ITEM_EASE } from '@/lib/motion';

interface FluidDeckProps {
  cards: FireItem[];
  hasMore?: boolean;
  loadingMore?: boolean;
  onLoadMore?: () => void;
  onOpenCard?: (item: FireItem) => void;
  onStandaloneIndexChange?: (index: number, meta: StandaloneIndexMeta) => void;
  usePageScroll?: boolean;
  resetKey?: string;
  total?: number;
}

type StandaloneIndexMeta = {
  previousIndex: number;
  direction: -1 | 0 | 1;
};

type StandaloneDeckState = {
  activeCardId: string | null;
  currentIndex: number;
  scrollTop: number;
  ts: number;
};

const STANDALONE_DECK_STATE_PREFIX = 'fire:pwa-deck:v1';
const STANDALONE_RESTORE_STEPS_MS = [0, 120, 280, 520, 860] as const;
// Cards in the PWA strip overlap slightly: stride = cardHeight − overlap, so
// each card's edge tucks under its neighbour (active card sits on top via
// z-index). No background is ever visible between cards — the strip reads as
// one continuous deck, not individual cards scrolling past.
const PWA_CARD_OVERLAP_PX = 12;
const PWA_CARD_MAX_WIDTH_PX = 560;
const PWA_RENDER_RADIUS = 2;
// Flick velocity (px/s) that advances the deck even on a short drag.
const PWA_FLICK_VELOCITY = 420;
const PWA_STRIP_SPRING = { type: 'spring', stiffness: 300, damping: 30, mass: 0.9 } as const;
const FIRE_TAB_RESELECT_EVENT = 'feedme:fire-tab-reselect';
const MOBILE_STACK_LAYOUT_SPRING = { type: 'spring', stiffness: 250, damping: 30, mass: 0.92 } as const;
const MOBILE_DECK_SWAP_SPRING = { type: 'spring', stiffness: 250, damping: 28, mass: 0.94 } as const;
const FIRE_DIALOG_SHARED_SPRING = { type: 'spring', stiffness: 420, damping: 42, mass: 0.9 } as const;
// Do not add `contain: layout paint style` or `content-visibility: auto` to
// the desktop card slots — both create a paint-containment boundary that
// breaks the sticky header's `backdrop-filter` (flat chrome, no frosted
// dispersion) on Chrome desktop. The cost of skipping containment is a tiny
// amount of off-screen layout work and is worth it.

function isStandaloneDisplayMode(): boolean {
  if (typeof window === 'undefined') return false;
  return window.matchMedia('(display-mode: standalone)').matches
    || Boolean((window.navigator as Navigator & { standalone?: boolean }).standalone);
}

function getScrollContainer(root: HTMLDivElement | null, usePageScroll: boolean): HTMLElement | null {
  if (typeof window === 'undefined') return null;
  if (!usePageScroll) return root;
  return document.scrollingElement instanceof HTMLElement ? document.scrollingElement : document.documentElement;
}

function getCurrentScrollTop(root: HTMLDivElement | null, usePageScroll: boolean): number {
  const scrollContainer = getScrollContainer(root, usePageScroll);
  if (!scrollContainer) return 0;
  return usePageScroll ? window.scrollY || scrollContainer.scrollTop : scrollContainer.scrollTop;
}

function setScrollTop(root: HTMLDivElement | null, usePageScroll: boolean, next: number): void {
  const scrollContainer = getScrollContainer(root, usePageScroll);
  if (!scrollContainer) return;
  if (usePageScroll) {
    window.scrollTo(0, next);
    return;
  }
  scrollContainer.scrollTop = next;
}

function buildStandaloneDeckStateKey(resetKey?: string): string {
  return `${STANDALONE_DECK_STATE_PREFIX}:${resetKey || 'default'}`;
}

function readStandaloneDeckState(key: string): StandaloneDeckState | null {
  if (typeof window === 'undefined') return null;
  try {
    const raw = window.sessionStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as Partial<StandaloneDeckState>;
    if (!parsed || typeof parsed !== 'object') return null;
    return {
      activeCardId: typeof parsed.activeCardId === 'string' && parsed.activeCardId.trim() ? parsed.activeCardId : null,
      currentIndex: Number.isFinite(parsed.currentIndex) ? Math.max(0, Math.floor(parsed.currentIndex as number)) : 0,
      scrollTop: Number.isFinite(parsed.scrollTop) ? Math.max(0, parsed.scrollTop as number) : 0,
      ts: Number.isFinite(parsed.ts) ? parsed.ts as number : Date.now(),
    };
  } catch {
    return null;
  }
}

function writeStandaloneDeckState(
  key: string,
  state: Omit<StandaloneDeckState, 'ts'>,
): void {
  if (typeof window === 'undefined') return;
  try {
    window.sessionStorage.setItem(key, JSON.stringify({
      ...state,
      ts: Date.now(),
    }));
  } catch {
    // ignore storage failures
  }
}

// ─── VirtualSlot: used by non-PWA mobile only (desktop renders inline) ───────
function VirtualSlot({
  item,
  index,
  isActive,
  mobileAutoplayEnabled,
  onOpenDetails,
  onBeforeOpenPost,
}: {
  item: FireItem;
  index: number;
  isActive: boolean;
  mobileAutoplayEnabled: boolean;
  onOpenDetails: () => void;
  onBeforeOpenPost?: (itemId: string) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [isObservedVisible, setIsObservedVisible] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      ([entry]) => setIsObservedVisible(entry.isIntersecting),
      { rootMargin: '200% 0px' },
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  return (
    <div
      ref={ref}
      data-card-id={item.id}
      className="flex w-full items-center justify-center"
    >
      {isObservedVisible ? (
        <motion.div
          initial={{ opacity: 0, y: 12 }}
          animate={{
            opacity: isActive ? 1 : 0.72,
            y: isActive ? -6 : 14,
            scale: isActive ? 1.02 : 0.965,
          }}
          transition={{ duration: 0.22, delay: Math.min(index * 0.016, 0.1), ease: [0.22, 1, 0.36, 1] }}
          className="relative w-full"
          style={{ zIndex: isActive ? 30 : 10 }}
        >
          <div
            className={[
              'rounded-[22px] transition-shadow duration-250',
              isActive
                ? 'shadow-[0_20px_38px_rgba(0,0,0,0.24)] dark:shadow-[0_24px_44px_rgba(0,0,0,0.6)]'
                : 'shadow-[0_12px_24px_rgba(0,0,0,0.18)] dark:shadow-[0_14px_28px_rgba(0,0,0,0.46)]',
            ].join(' ')}
          >
            <FireCard3D
              item={item}
              highlighted={isActive}
              layoutMode="mobile"
              mobileAutoplayEnabled={mobileAutoplayEnabled}
              onOpenDetails={onOpenDetails}
              onBeforeOpenPost={onBeforeOpenPost}
            />
          </div>
        </motion.div>
      ) : (
        <div className="w-full aspect-[4/5] lg:aspect-[11/14]" />
      )}
    </div>
  );
}

// ─── Main component ───────────────────────────────────────────────────────────
function FluidDeck({
  cards,
  hasMore,
  loadingMore,
  onLoadMore,
  onOpenCard,
  onStandaloneIndexChange,
  usePageScroll = false,
  resetKey,
  total,
}: FluidDeckProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const [activeCardId, setActiveCardId] = useState<string | null>(cards[0]?.id ?? null);
  const activeCardIdRef = useRef<string | null>(cards[0]?.id ?? null);
  const [isDesktop, setIsDesktop] = useState(() => typeof window !== 'undefined' && window.matchMedia('(min-width: 1024px)').matches);
  const [isStandalone, setIsStandalone] = useState(isStandaloneDisplayMode);
  const [isIOS] = useState(() => typeof navigator !== 'undefined' && /iPad|iPhone|iPod/.test(navigator.userAgent));
  const [mobileAutoplayEnabled, setMobileAutoplayEnabled] = useState(false);
  const currentIndexRef = useRef(0);
  const usePwaSnap = isStandalone;
  const standaloneDeckStateKey = useMemo(() => buildStandaloneDeckStateKey(resetKey), [resetKey]);
  const restoredStandaloneKeyRef = useRef<string | null>(null);
  const restoreTimeoutIdsRef = useRef<number[]>([]);
  const restoreRafIdsRef = useRef<number[]>([]);
  const scrollToTopRafRef = useRef<number | null>(null);
  const pwaTopReturnTimeoutIdsRef = useRef<number[]>([]);
  const reportedPwaIndexRef = useRef(0);

  // ── PWA-specific state ──────────────────────────────────────────────────────
  // Independent index tracked outside scroll — drives the transform-based stack
  const [pwaIndex, setPwaIndex] = useState(0);
  const pwaLastNavRef = useRef(0);
  // First commit renders ONLY the active card; neighbours mount a frame later.
  // Five FireCard3D mounts in one commit was the main-thread spike that made
  // switching to the fire tab hitch in PWA mode.
  const [pwaNeighboursReady, setPwaNeighboursReady] = useState(false);
  useEffect(() => {
    if (!usePwaSnap || isDesktop || pwaNeighboursReady) return;
    const frame = window.requestAnimationFrame(() => setPwaNeighboursReady(true));
    return () => window.cancelAnimationFrame(frame);
  }, [isDesktop, pwaNeighboursReady, usePwaSnap]);

  // The band the deck lives in: from just under the header glass to just above
  // the nav glass. Paddings are minimal on purpose — PWA has no browser chrome,
  // so the card claims as much of the canvas as the viewport allows.
  const pwaDeckTopExpr = 'calc(env(safe-area-inset-top) + var(--fm-tab-mobile-content-offset))';
  const pwaDeckBottomExpr = 'calc(var(--fire-bottom-clearance, 86px) + env(safe-area-inset-bottom) + 10px)';
  // Card cap mirrors the band EXACTLY so the active card always fits between
  // the chrome bars; neighbours overflow the band and peek under the glass.
  const pwaCardMaxHExpr = `calc(var(--fire-app-height, 100dvh) - ${pwaDeckTopExpr} - ${pwaDeckBottomExpr})`;

  // Measured band rect → real card height → strip stride (cardHeight − overlap),
  // so consecutive cards always touch/tuck with zero background between them.
  const pwaBandRef = useRef<HTMLDivElement | null>(null);
  const [pwaBandSize, setPwaBandSize] = useState<{ w: number; h: number } | null>(null);
  useEffect(() => {
    if (!usePwaSnap || isDesktop) return;
    const node = pwaBandRef.current;
    if (!node || typeof ResizeObserver === 'undefined') return;
    const observer = new ResizeObserver((entries) => {
      const rect = entries[0]?.contentRect;
      if (!rect || rect.height < 1) return;
      setPwaBandSize((current) => (
        current && Math.abs(current.w - rect.width) < 1 && Math.abs(current.h - rect.height) < 1
          ? current
          : { w: rect.width, h: rect.height }
      ));
    });
    observer.observe(node);
    return () => observer.disconnect();
  }, [isDesktop, usePwaSnap]);

  const pwaCardW = pwaBandSize
    ? Math.min(pwaBandSize.w, PWA_CARD_MAX_WIDTH_PX)
    : 0;
  const pwaCardH = pwaBandSize ? pwaBandSize.h : 0;
  const pwaStride = pwaCardH > 0
    ? pwaCardH - PWA_CARD_OVERLAP_PX
    : (typeof window === 'undefined' ? 600 : Math.max(420, window.innerHeight - 300));

  useEffect(() => {
    activeCardIdRef.current = activeCardId;
  }, [activeCardId]);

  useEffect(() => {
    restoredStandaloneKeyRef.current = null;
  }, [standaloneDeckStateKey]);

  useEffect(() => {
    const mql = window.matchMedia('(min-width: 1024px)');
    const handler = (event: MediaQueryListEvent) => setIsDesktop(event.matches);
    mql.addEventListener('change', handler as (event: MediaQueryListEvent) => void);
    return () => mql.removeEventListener('change', handler as (event: MediaQueryListEvent) => void);
  }, []);

  useEffect(() => {
    const mql = window.matchMedia('(display-mode: standalone)');
    const handler = () => setIsStandalone(isStandaloneDisplayMode());
    mql.addEventListener?.('change', handler as (event: MediaQueryListEvent) => void);
    return () => mql.removeEventListener?.('change', handler as (event: MediaQueryListEvent) => void);
  }, []);

  // ── PWA: pwaIndex → activeCardId + currentIndexRef ────────────────────────
  useEffect(() => {
    if (!usePwaSnap || isDesktop) return;
    const card = cards[pwaIndex];
    if (!card) return;
    currentIndexRef.current = pwaIndex;
    activeCardIdRef.current = card.id;
  }, [cards, isDesktop, pwaIndex, usePwaSnap]);

  useEffect(() => {
    if (!usePwaSnap || isDesktop) return;
    const previousIndex = reportedPwaIndexRef.current;
    const delta = pwaIndex - previousIndex;
    const direction = delta === 0 ? 0 : delta > 0 ? 1 : -1;
    reportedPwaIndexRef.current = pwaIndex;
    onStandaloneIndexChange?.(pwaIndex, { previousIndex, direction });
  }, [isDesktop, onStandaloneIndexChange, pwaIndex, usePwaSnap]);

  const clearScheduledStandaloneRestore = useCallback(() => {
    restoreTimeoutIdsRef.current.forEach((id) => window.clearTimeout(id));
    restoreTimeoutIdsRef.current = [];
    restoreRafIdsRef.current.forEach((id) => window.cancelAnimationFrame(id));
    restoreRafIdsRef.current = [];
  }, []);

  const clearScrollToTopAnimation = useCallback(() => {
    if (scrollToTopRafRef.current != null) {
      window.cancelAnimationFrame(scrollToTopRafRef.current);
      scrollToTopRafRef.current = null;
    }
  }, []);

  const animateScrollToTop = useCallback(() => {
    if (typeof window === 'undefined') return;
    const root = containerRef.current;
    const start = getCurrentScrollTop(root, usePageScroll);
    if (start <= 1) {
      setScrollTop(root, usePageScroll, 0);
      return;
    }
    clearScrollToTopAnimation();
    const duration = Math.max(360, Math.min(760, 420 + Math.min(start, 1200) * 0.14));
    const startedAt = window.performance.now();

    const tick = (now: number) => {
      const elapsed = now - startedAt;
      const progress = Math.min(1, elapsed / duration);
      const eased = 1 - Math.pow(1 - progress, 4);
      setScrollTop(root, usePageScroll, Math.max(0, start * (1 - eased)));
      if (progress < 1) {
        scrollToTopRafRef.current = window.requestAnimationFrame(tick);
      } else {
        scrollToTopRafRef.current = null;
        setScrollTop(root, usePageScroll, 0);
      }
    };

    scrollToTopRafRef.current = window.requestAnimationFrame(tick);
  }, [clearScrollToTopAnimation, usePageScroll]);

  const clearPwaTopReturn = useCallback(() => {
    pwaTopReturnTimeoutIdsRef.current.forEach((id) => window.clearTimeout(id));
    pwaTopReturnTimeoutIdsRef.current = [];
  }, []);

  // ── PWA: restore index from session storage (no scroll needed) ─────────────
  const restorePwaIndex = useCallback(() => {
    if (!usePwaSnap || isDesktop) return;
    const saved = readStandaloneDeckState(standaloneDeckStateKey);
    if (!saved) return;
    const idx = saved.activeCardId
      ? cards.findIndex(c => c.id === saved.activeCardId)
      : -1;
    const target = idx >= 0 ? idx : Math.min(saved.currentIndex, cards.length - 1);
    if (target >= 0 && target < cards.length) {
      setPwaIndex(target);
    }
  }, [cards, isDesktop, standaloneDeckStateKey, usePwaSnap]);

  const schedulePwaRestore = useCallback(() => {
    if (!usePwaSnap || isDesktop) return;
    clearScheduledStandaloneRestore();
    STANDALONE_RESTORE_STEPS_MS.forEach((delayMs) => {
      const timeoutId = window.setTimeout(() => {
        const rafId = window.requestAnimationFrame(restorePwaIndex);
        restoreRafIdsRef.current.push(rafId);
      }, delayMs);
      restoreTimeoutIdsRef.current.push(timeoutId);
    });
  }, [clearScheduledStandaloneRestore, isDesktop, restorePwaIndex, usePwaSnap]);

  const syncDeckFocusState = useCallback(() => {
    if (isDesktop) return { currentIndex: 0, nodes: [] as HTMLElement[] };
    // PWA uses pwaIndex, not scroll-based detection
    if (usePwaSnap) return { currentIndex: currentIndexRef.current, nodes: [] as HTMLElement[] };

    const root = containerRef.current;
    if (!root) return { currentIndex: 0, nodes: [] as HTMLElement[] };

    const centerY = usePageScroll
      ? window.innerHeight * 0.5
      : (() => {
          const rootRect = root.getBoundingClientRect();
          return rootRect.top + rootRect.height * 0.5;
        })();

    const nodes = Array.from(root.querySelectorAll<HTMLElement>('[data-card-id]'));
    let nextId: string | null = null;
    let bestIndex = 0;
    let bestDistance = Number.POSITIVE_INFINITY;

    nodes.forEach((node, index) => {
      const rect = node.getBoundingClientRect();
      const cardCenter = rect.top + rect.height * 0.5;
      const absDistance = Math.abs(cardCenter - centerY);
      if (absDistance < bestDistance) {
        bestDistance = absDistance;
        bestIndex = index;
        nextId = node.dataset.cardId ?? null;
      }
    });

    currentIndexRef.current = bestIndex;
    if (nextId) setActiveCardId((current) => (current === nextId ? current : nextId));
    return { currentIndex: bestIndex, nodes };
  }, [isDesktop, usePageScroll, usePwaSnap]);

  const persistStandaloneDeckState = useCallback((preferredCardId?: string) => {
    if (isDesktop || !usePwaSnap) return;
    const preferredIndex = preferredCardId ? cards.findIndex((card) => card.id === preferredCardId) : -1;
    const fallbackCardId = cards[currentIndexRef.current]?.id ?? activeCardIdRef.current;
    writeStandaloneDeckState(standaloneDeckStateKey, {
      activeCardId: preferredCardId ?? fallbackCardId,
      currentIndex: preferredIndex >= 0 ? preferredIndex : currentIndexRef.current,
      scrollTop: 0,
    });
  }, [cards, isDesktop, standaloneDeckStateKey, usePwaSnap]);

  const triggerPwaReturnToTop = useCallback(() => {
    if (!usePwaSnap || isDesktop) return;
    clearPwaTopReturn();
    const firstCardId = cards[0]?.id ?? null;
    if (pwaIndex === 0) {
      persistStandaloneDeckState(firstCardId ?? undefined);
      return;
    }

    pwaLastNavRef.current = Date.now();
    const distance = pwaIndex;
    const stepMs = Math.max(28, Math.min(60, 520 / Math.max(distance, 1)));

    for (let nextIndex = distance - 1; nextIndex >= 0; nextIndex -= 1) {
      const delay = Math.round((distance - 1 - nextIndex) * stepMs);
      pwaTopReturnTimeoutIdsRef.current.push(
        window.setTimeout(() => {
          currentIndexRef.current = nextIndex;
          setPwaIndex(nextIndex);
          if (nextIndex === 0) {
            setActiveCardId(firstCardId);
            persistStandaloneDeckState(firstCardId ?? undefined);
          }
        }, delay),
      );
    }
  }, [cards, clearPwaTopReturn, isDesktop, persistStandaloneDeckState, pwaIndex, usePwaSnap]);

  // ── PWA: navigation ────────────────────────────────────────────────────────
  const navigatePwa = useCallback((dir: number) => {
    const now = Date.now();
    if (now - pwaLastNavRef.current < 320) return;
    clearPwaTopReturn();
    pwaLastNavRef.current = now;
    setPwaIndex(prev => Math.max(0, Math.min(cards.length - 1, prev + dir)));
  }, [cards.length, clearPwaTopReturn]);

  const handlePwaDragEnd = useCallback((_: MouseEvent | TouchEvent | PointerEvent, info: PanInfo) => {
    // Distance commits a deliberate drag; velocity commits a quick flick.
    if (info.offset.y < -50 || info.velocity.y < -PWA_FLICK_VELOCITY) navigatePwa(1);
    else if (info.offset.y > 50 || info.velocity.y > PWA_FLICK_VELOCITY) navigatePwa(-1);
  }, [navigatePwa]);

  // Reset pwaIndex when filter/day changes
  const prevResetKeyRef = useRef(resetKey);
  useEffect(() => {
    if (resetKey === prevResetKeyRef.current) return;
    prevResetKeyRef.current = resetKey;

    if (usePwaSnap) {
      clearPwaTopReturn();
      currentIndexRef.current = 0;
      const frame = window.requestAnimationFrame(() => {
        activeCardIdRef.current = cards[0]?.id ?? null;
        setPwaIndex(0);
      });
      return () => window.cancelAnimationFrame(frame);
    }

    const nextId = cards[0]?.id ?? null;
    const frame = window.requestAnimationFrame(() => {
      currentIndexRef.current = 0;
      activeCardIdRef.current = nextId;
      setActiveCardId(nextId);
    });
    if (isDesktop) return () => window.cancelAnimationFrame(frame);

    const root = containerRef.current;
    if (!usePageScroll && !root) return () => window.cancelAnimationFrame(frame);
    animateScrollToTop();
    return () => window.cancelAnimationFrame(frame);
  }, [animateScrollToTop, cards, clearPwaTopReturn, isDesktop, resetKey, usePageScroll, usePwaSnap]);

  // Scroll-based active card sync — non-PWA mobile only
  useEffect(() => {
    if (isDesktop || usePwaSnap) return;
    const root = containerRef.current;
    if (!root) return;

    let frame = 0;
    const requestSync = () => {
      if (frame) cancelAnimationFrame(frame);
      frame = window.requestAnimationFrame(() => {
        frame = 0;
        syncDeckFocusState();
      });
    };

    requestSync();
    const scrollTarget: Window | HTMLDivElement = usePageScroll ? window : root;
    scrollTarget.addEventListener('scroll', requestSync, { passive: true });
    window.addEventListener('resize', requestSync);
    return () => {
      if (frame) cancelAnimationFrame(frame);
      scrollTarget.removeEventListener('scroll', requestSync);
      window.removeEventListener('resize', requestSync);
    };
  }, [cards, isDesktop, syncDeckFocusState, usePageScroll, usePwaSnap]);

  // PWA: restore index on mount/cards-change
  useEffect(() => {
    if (!usePwaSnap || isDesktop) return;
    const alreadyRestored = restoredStandaloneKeyRef.current === standaloneDeckStateKey;
    if (!alreadyRestored) {
      restoredStandaloneKeyRef.current = standaloneDeckStateKey;
      const frame = window.requestAnimationFrame(() => restorePwaIndex());
      return () => window.cancelAnimationFrame(frame);
    }
  }, [cards, isDesktop, restorePwaIndex, standaloneDeckStateKey, usePwaSnap]);

  // PWA: persist + restore on visibility/lifecycle events
  useEffect(() => {
    if (!usePwaSnap || isDesktop) return;

    const onHide = () => persistStandaloneDeckState();
    const onShow = () => schedulePwaRestore();
    const onVisibility = () => {
      if (document.visibilityState === 'hidden') persistStandaloneDeckState();
      else if (document.visibilityState === 'visible') schedulePwaRestore();
    };

    window.addEventListener('pagehide', onHide);
    window.addEventListener('pageshow', onShow);
    window.addEventListener('focus', onShow);
    document.addEventListener('visibilitychange', onVisibility);
    return () => {
      window.removeEventListener('pagehide', onHide);
      window.removeEventListener('pageshow', onShow);
      window.removeEventListener('focus', onShow);
      document.removeEventListener('visibilitychange', onVisibility);
    };
  }, [isDesktop, persistStandaloneDeckState, schedulePwaRestore, usePwaSnap]);

  // PWA: persist when index changes
  useEffect(() => {
    if (!usePwaSnap || isDesktop) return;
    persistStandaloneDeckState();
  }, [pwaIndex, cards.length, isDesktop, persistStandaloneDeckState, usePwaSnap]);

  useEffect(() => {
    if (isDesktop) return;

    const handleFireTabReselect = () => {
      if (usePwaSnap) {
        triggerPwaReturnToTop();
        return;
      }
      animateScrollToTop();
    };

    window.addEventListener(FIRE_TAB_RESELECT_EVENT, handleFireTabReselect);
    return () => window.removeEventListener(FIRE_TAB_RESELECT_EVENT, handleFireTabReselect);
  }, [animateScrollToTop, isDesktop, triggerPwaReturnToTop, usePwaSnap]);

  useEffect(() => () => {
    clearScheduledStandaloneRestore();
    clearPwaTopReturn();
    clearScrollToTopAnimation();
  }, [clearPwaTopReturn, clearScheduledStandaloneRestore, clearScrollToTopAnimation]);

  useEffect(() => {
    if (isDesktop || !usePwaSnap || typeof window === 'undefined' || !('scrollRestoration' in window.history)) return;
    const previous = window.history.scrollRestoration;
    window.history.scrollRestoration = 'manual';
    return () => { window.history.scrollRestoration = previous; };
  }, [isDesktop, usePwaSnap]);

  const handleLoadMore = useCallback(() => {
    if (hasMore && !loadingMore && onLoadMore) onLoadMore();
  }, [hasMore, loadingMore, onLoadMore]);

  if (!cards || cards.length === 0) {
    return (
      <div className="mt-16 flex h-64 items-center justify-center font-mono text-sm tracking-widest text-neutral-500">
        NO ANOMALIES DETECTED
      </div>
    );
  }

  const resolvedActive = isDesktop ? null : activeCardId;
  const mobileStackClass = usePageScroll
    ? isIOS
      ? '-mt-[18vh] flex w-full min-h-[58svh] items-center justify-center first:mt-0 md:-mt-[20vh] md:min-h-[60dvh]'
      : '-mt-[22vh] flex w-full min-h-[58svh] items-center justify-center first:mt-0 md:-mt-[24vh] md:min-h-[60dvh]'
    : isIOS
      ? '-mt-[10vh] flex w-full min-h-[74svh] items-center justify-center first:mt-0 md:-mt-[12vh] md:min-h-[76dvh]'
      : '-mt-[14vh] flex w-full min-h-[74svh] items-center justify-center first:mt-0 md:-mt-[16vh] md:min-h-[76dvh]';
  const enableContainerSnap = usePwaSnap && !usePageScroll;
  const remainingCount = Math.max(0, (total || 0) - cards.length);
  // For load dock, use pwaIndex directly in PWA mode
  const showStandaloneLoadDock = usePwaSnap
    && Boolean(hasMore)
    && pwaIndex >= Math.max(0, cards.length - 2);
  // ── PWA render: one continuous card strip (TikTok/Reels style) ──────────────
  // The band is the rect between the chrome bars; the strip inside holds every
  // card at index × stride, and a single transform moves them all together —
  // the finger drags the whole strip 1:1 and one spring slots it per swipe.
  if (usePwaSnap) {
    const restY = -pwaIndex * pwaStride;
    const canPrev = pwaIndex > 0;
    const canNext = pwaIndex < cards.length - 1;

    return (
      <>
        <AnimatePresence mode="sync">
          <motion.div
            key="pwa-fire-deck"
            ref={pwaBandRef}
            initial={{ opacity: 0, y: 24, scale: 0.985 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -16, scale: 0.992 }}
            transition={MOBILE_DECK_SWAP_SPRING}
            className="fixed inset-x-0 z-10"
            style={{ top: pwaDeckTopExpr, bottom: pwaDeckBottomExpr }}
          >
            <motion.div
              className="absolute inset-0"
              drag="y"
              dragDirectionLock
              dragMomentum={false}
              dragElastic={0.14}
              dragConstraints={{
                top: restY - (canNext ? pwaStride : 0),
                bottom: restY + (canPrev ? pwaStride : 0),
              }}
              onDragEnd={handlePwaDragEnd}
              animate={{ y: restY }}
              transition={PWA_STRIP_SPRING}
              style={{ willChange: 'transform', backfaceVisibility: 'hidden' }}
            >
              {cards.map((card, index) => {
                const diff = index - pwaIndex;
                // Keep one offscreen buffer so neighbours glide out instead of popping away.
                if (Math.abs(diff) > PWA_RENDER_RADIUS) return null;
                if (!pwaNeighboursReady && diff !== 0) return null;
                const isCurrent = diff === 0;

                return (
                  <div
                    key={card.id}
                    className="absolute inset-x-0 flex h-full items-start justify-center px-0 sm:px-2"
                    style={{
                      top: index * pwaStride,
                      zIndex: isCurrent ? 10 : 8,
                      pointerEvents: isCurrent ? 'auto' : 'none',
                    }}
                  >
                    <div
                      className="w-full"
                      style={{
                        width: pwaCardW > 0 ? `${pwaCardW}px` : '100%',
                        maxWidth: `${PWA_CARD_MAX_WIDTH_PX}px`,
                        ['--fire-card-max-height' as string]: pwaCardH > 0 ? `${pwaCardH}px` : pwaCardMaxHExpr,
                        ['--fire-card-aspect' as string]: pwaCardW > 0 && pwaCardH > 0 ? `${pwaCardW} / ${pwaCardH}` : '9 / 14',
                      }}
                    >
                      <FireCard3D
                        item={card}
                        highlighted={isCurrent}
                        layoutMode="mobile"
                        mobileAutoplayEnabled={mobileAutoplayEnabled}
                        showMobileAutoplayToggle={isCurrent}
                        onOpenDetails={() => undefined}
                        onToggleMobileAutoplay={setMobileAutoplayEnabled}
                        onBeforeOpenPost={persistStandaloneDeckState}
                      />
                    </div>
                  </div>
                );
              })}
            </motion.div>
          </motion.div>
        </AnimatePresence>

        {/* Spacer so the page has height (needed for fixed positioning context) */}
        <div style={{ height: '100dvh' }} />

        {/* Load more dock */}
        {showStandaloneLoadDock && (
          <div
            className="pointer-events-none fixed inset-x-0 z-[120] flex justify-center px-4"
            style={{ bottom: 'calc(var(--fire-bottom-clearance,86px) + env(safe-area-inset-bottom) + 12px)' }}
          >
            <div className="pointer-events-auto w-full max-w-[320px]">
              <div className="rounded-[22px] border border-white/70 bg-white/72 p-2 backdrop-blur-[24px] shadow-[0_16px_34px_rgba(0,0,0,0.14),inset_0_1px_0_rgba(255,255,255,0.86)] dark:border-white/12 dark:bg-black/55 dark:shadow-[0_18px_36px_rgba(0,0,0,0.46),inset_0_1px_0_rgba(255,255,255,0.08)]">
                <button
                  type="button"
                  onClick={handleLoadMore}
                  disabled={loadingMore}
                  className="inline-flex w-full items-center justify-center rounded-[16px] border border-white/78 bg-white/88 px-5 py-3 text-[11px] font-black uppercase tracking-[0.2em] text-[#E11D48] shadow-[0_10px_24px_rgba(0,0,0,0.18),inset_0_1px_0_rgba(255,255,255,0.86)] transition active:scale-[0.98] disabled:cursor-wait disabled:opacity-70 dark:border-transparent dark:bg-[#E11D48] dark:text-white dark:shadow-[0_12px_26px_rgba(225,29,72,0.18)]"
                >
                  {loadingMore ? (
                    <span className="inline-flex items-center gap-2">
                      <Loader2 className="h-4 w-4 animate-spin" />
                      Loading next batch
                    </span>
                  ) : (
                    'Continue feeding'
                  )}
                </button>
                <div className="pt-2 text-center text-[10px] font-black uppercase tracking-[0.16em] text-black/42 dark:text-white/34">
                  {remainingCount > 0 ? `${remainingCount} more signals waiting` : 'Loads the next batch of signals'}
                </div>
              </div>
            </div>
          </div>
        )}
      </>
    );
  }

  // ── Desktop + non-PWA mobile render (unchanged) ───────────────────────────
  const containerClasses = [
    usePageScroll
      ? 'relative w-full overflow-visible'
      : enableContainerSnap
        ? 'relative h-full w-full overflow-y-auto overflow-x-hidden overscroll-y-contain hide-scrollbar'
        : 'relative h-full w-full overflow-y-auto overflow-x-hidden overscroll-y-contain scroll-smooth hide-scrollbar',
    'px-0 sm:px-3 lg:px-4',
    usePageScroll && !isDesktop ? 'pt-[calc(var(--fm-tab-mobile-content-offset)+env(safe-area-inset-top))]' : 'pt-[var(--fm-tab-desktop-content-offset)]',
    isDesktop ? 'pb-[148px]' : 'pb-[88px]',
    'lg:snap-none',
  ].join(' ');

  const containerStyle = usePageScroll
    ? undefined
    : { WebkitOverflowScrolling: 'touch' as const };

  return (
    <div ref={containerRef} className={containerClasses} style={containerStyle}>
      <div className="mx-auto w-full lg:max-w-none lg:px-1 xl:px-2">
        {isDesktop ? (
          <motion.div
            layout
            transition={{ layout: GRID_LAYOUT_SPRING }}
            className="grid grid-cols-5 gap-3 xl:gap-[14px] 2xl:grid-cols-6 2xl:gap-5"
          >
            <AnimatePresence mode="popLayout">
              {cards.map((card, index) => {
                const isActive = resolvedActive === card.id;
                const enterDelay = Math.min(index * 0.026, 0.2);
                return (
                  <motion.div
                    key={card.id}
                    layout
                    data-card-id={card.id}
                    initial={{ opacity: 0, y: 18, scale: 0.975 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={{ opacity: 0, y: 10, scale: 0.97 }}
                    transition={{
                      layout: GRID_LAYOUT_SPRING,
                      opacity: { duration: 0.18, delay: enterDelay, ease: GRID_ITEM_EASE },
                      y: { duration: 0.24, delay: enterDelay, ease: GRID_ITEM_EASE },
                      scale: { duration: 0.24, delay: enterDelay, ease: GRID_ITEM_EASE },
                    }}
                    className="flex w-full items-center justify-center"
                    style={{ zIndex: isActive ? 30 : 10 }}
                  >
                    <motion.div
                      layoutId={`fire-card-dialog-${card.id}`}
                      transition={{ layout: FIRE_DIALOG_SHARED_SPRING }}
                      className={[
                        'relative w-full rounded-[22px] transition-shadow duration-250',
                        isActive
                          ? 'shadow-[0_22px_42px_rgba(0,0,0,0.34)] dark:shadow-[0_24px_48px_rgba(0,0,0,0.62)]'
                          : 'shadow-[0_14px_26px_rgba(0,0,0,0.22)] dark:shadow-[0_16px_30px_rgba(0,0,0,0.48)]',
                      ].join(' ')}
                    >
                      <FireCard3D
                        item={card}
                        highlighted={isActive}
                        layoutMode="desktop"
                        mobileAutoplayEnabled={mobileAutoplayEnabled}
                        onOpenDetails={() => onOpenCard?.(card)}
                        onBeforeOpenPost={persistStandaloneDeckState}
                      />
                    </motion.div>
                  </motion.div>
                );
              })}
            </AnimatePresence>
          </motion.div>
        ) : (
          <AnimatePresence mode="sync">
            <motion.div
              key="mobile-fire-deck"
              initial={{ opacity: 0, y: 22, scale: 0.985 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: -14, scale: 0.992 }}
              transition={MOBILE_DECK_SWAP_SPRING}
              className="flex flex-col"
              style={{ willChange: 'transform,opacity', backfaceVisibility: 'hidden' }}
            >
              <div className="flex flex-col">
                <AnimatePresence mode="popLayout">
                  {cards.map((card, index) => {
                    const isActive = resolvedActive === card.id;
                    return (
                      <motion.div
                        key={card.id}
                        layout="position"
                        initial={{ opacity: 0, y: 10, scale: 0.985 }}
                        animate={{ opacity: 1, scale: 1 }}
                        exit={{ opacity: 0, y: -8, scale: 0.97 }}
                        transition={{
                          layout: MOBILE_STACK_LAYOUT_SPRING,
                          opacity: { duration: 0.18, ease: GRID_ITEM_EASE },
                          scale: { duration: 0.22, ease: GRID_ITEM_EASE },
                          y: { duration: 0.22, ease: GRID_ITEM_EASE },
                        }}
                        className={mobileStackClass}
                        style={{ zIndex: isActive ? 40 : 10 }}
                      >
                      <div className="w-full max-w-[472px]">
                        <VirtualSlot
                          item={card}
                          index={index}
                          isActive={isActive}
                          mobileAutoplayEnabled={mobileAutoplayEnabled}
                          onOpenDetails={() => undefined}
                          onBeforeOpenPost={persistStandaloneDeckState}
                        />
                      </div>
                      </motion.div>
                    );
                  })}
                </AnimatePresence>
              </div>
            </motion.div>
          </AnimatePresence>
        )}

        {hasMore ? (
          <div className="py-8">
            <div className="flex flex-col items-center justify-center gap-2">
              <button
                type="button"
                onClick={handleLoadMore}
                disabled={loadingMore}
                className="inline-flex min-w-[220px] items-center justify-center rounded-[18px] border border-white/35 bg-white/55 px-5 py-3 text-[11px] font-black uppercase tracking-[0.2em] text-[#E11D48] shadow-[0_18px_40px_rgba(0,0,0,0.14),inset_0_1px_0_rgba(255,255,255,0.86)] transition hover:-translate-y-0.5 hover:bg-white/72 disabled:translate-y-0 disabled:cursor-wait disabled:opacity-70 dark:border-white/16 dark:bg-black/38 dark:text-[#E11D48] dark:shadow-[0_18px_36px_rgba(0,0,0,0.52),inset_0_1px_0_rgba(255,255,255,0.08)] dark:hover:bg-black/52"
              >
                {loadingMore ? (
                  <span className="inline-flex items-center gap-2">
                    <Loader2 className="h-4 w-4 animate-spin" />
                    Loading next batch
                  </span>
                ) : (
                  'Continue feeding'
                )}
              </button>
              <div className="text-[10px] font-black uppercase tracking-[0.16em] text-black/40 dark:text-white/34">
                {remainingCount > 0 ? `${remainingCount} more signals waiting` : 'Loads the next batch of signals'}
              </div>
            </div>
          </div>
        ) : (
          <div className="h-1 w-full" />
        )}
      </div>
    </div>
  );
}

export default memo(FluidDeck);
