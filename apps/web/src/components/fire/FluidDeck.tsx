'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { AnimatePresence, motion, type PanInfo } from 'framer-motion';
import { Loader2 } from 'lucide-react';
import { FireItem } from './types';
import { FireCard3D } from './FireCard3D';

interface FluidDeckProps {
  cards: FireItem[];
  hasMore?: boolean;
  loadingMore?: boolean;
  onLoadMore?: () => void;
  onOpenCard?: (item: FireItem) => void;
  usePageScroll?: boolean;
  resetKey?: string;
  total?: number;
}

type StandaloneDeckState = {
  activeCardId: string | null;
  currentIndex: number;
  scrollTop: number;
  ts: number;
};

const STANDALONE_DECK_STATE_PREFIX = 'fire:pwa-deck:v1';
const STANDALONE_RESTORE_STEPS_MS = [0, 120, 280, 520, 860] as const;
const PWA_NEIGHBOUR_OFFSET_RATIO = 0.56;
const PWA_OFFSCREEN_OFFSET_RATIO = 1.16;
const PWA_RENDER_RADIUS = 2;
const FIRE_TAB_RESELECT_EVENT = 'feedme:fire-tab-reselect';

function isStandaloneDisplayMode(): boolean {
  if (typeof window === 'undefined') return false;
  return window.matchMedia('(display-mode: standalone)').matches
    || Boolean((window.navigator as Navigator & { standalone?: boolean }).standalone);
}

function readRootCssPx(name: string, fallback: number): number {
  if (typeof window === 'undefined') return fallback;
  const raw = window.getComputedStyle(document.documentElement).getPropertyValue(name);
  const value = Number.parseFloat(raw);
  return Number.isFinite(value) ? value : fallback;
}

function getPageFocusCenterY(): number {
  if (typeof window === 'undefined') return 0;
  const headerHeight = readRootCssPx('--fire-header-height', 168);
  const bottomClearance = readRootCssPx('--fire-bottom-clearance', 86);
  const topBoundary = headerHeight + 24;
  const bottomBoundary = Math.max(topBoundary + 1, window.innerHeight - (bottomClearance + 12));
  return topBoundary + (bottomBoundary - topBoundary) * 0.5;
}

function getPwaViewportSlotHeight(): number {
  if (typeof window === 'undefined') return 600;
  return Math.max(420, readRootCssPx('--fire-app-height', window.innerHeight));
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

function getSnapViewportCenter(root: HTMLDivElement | null, usePageScroll: boolean): number {
  if (usePageScroll) return getPageFocusCenterY();
  if (!root) return typeof window === 'undefined' ? 0 : window.innerHeight * 0.5;
  const rootRect = root.getBoundingClientRect();
  return rootRect.top + rootRect.height * 0.5;
}

function getSnapScrollTop(node: HTMLElement, root: HTMLDivElement | null, usePageScroll: boolean): number {
  const currentScrollTop = getCurrentScrollTop(root, usePageScroll);
  const focusCenter = getSnapViewportCenter(root, usePageScroll);
  const rect = node.getBoundingClientRect();
  const cardCenter = rect.top + rect.height * 0.5;
  return Math.max(0, currentScrollTop + (cardCenter - focusCenter));
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

// ─── VirtualSlot: used by desktop + non-PWA mobile only ──────────────────────
function VirtualSlot({
  item,
  index,
  isActive,
  isDesktop,
  mobileAutoplayEnabled,
  onOpenDetails,
  onBeforeOpenPost,
}: {
  item: FireItem;
  index: number;
  isActive: boolean;
  isDesktop: boolean;
  mobileAutoplayEnabled: boolean;
  onOpenDetails: () => void;
  onBeforeOpenPost?: (itemId: string) => void;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [isVisible, setIsVisible] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;
    const observer = new IntersectionObserver(
      ([entry]) => setIsVisible(entry.isIntersecting),
      { rootMargin: '200% 0px' },
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  return (
    <div ref={ref} data-card-id={item.id} className="flex w-full items-center justify-center">
      {isVisible ? (
        <motion.div
          initial={{ opacity: 0, y: 12, scale: 0.99 }}
          animate={isDesktop
            ? { opacity: 1, y: 0, scale: 1, filter: 'none' }
            : {
                opacity: isActive ? 1 : 0.72,
                y: isActive ? -8 : 18,
                scale: isActive ? 1.035 : 0.94,
                filter: isActive ? 'blur(0px)' : 'blur(0.4px)',
              }}
          transition={{ duration: 0.22, delay: Math.min(index * 0.016, 0.1), ease: [0.22, 1, 0.36, 1] }}
          className="relative w-full"
          style={{ zIndex: isActive ? 30 : 10 }}
        >
          <div
            className={[
              'rounded-[22px] transition-shadow duration-250',
              isDesktop
                ? isActive
                  ? 'shadow-[0_22px_42px_rgba(0,0,0,0.34)] dark:shadow-[0_24px_48px_rgba(0,0,0,0.62)]'
                  : 'shadow-[0_14px_26px_rgba(0,0,0,0.22)] dark:shadow-[0_16px_30px_rgba(0,0,0,0.48)]'
                : isActive
                  ? 'shadow-[0_20px_38px_rgba(0,0,0,0.24)] dark:shadow-[0_24px_44px_rgba(0,0,0,0.6)]'
                  : 'shadow-[0_12px_24px_rgba(0,0,0,0.18)] dark:shadow-[0_14px_28px_rgba(0,0,0,0.46)]',
            ].join(' ')}
          >
            <FireCard3D
              item={item}
              highlighted={isActive}
              layoutMode={isDesktop ? 'desktop' : 'mobile'}
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
export default function FluidDeck({ cards, hasMore, loadingMore, onLoadMore, onOpenCard, usePageScroll = false, resetKey, total }: FluidDeckProps) {
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

  // ── PWA-specific state ──────────────────────────────────────────────────────
  // Independent index tracked outside scroll — drives the transform-based stack
  const [pwaIndex, setPwaIndex] = useState(0);
  const pwaLastNavRef = useRef(0);
  // Height of one full-canvas card slot (used for PWA y offsets).
  const [pwaSlotH, setPwaSlotH] = useState(() => {
    if (typeof window === 'undefined') return 600;
    return getPwaViewportSlotHeight();
  });

  // Shift the center point of the card stack so the active card sits between
  // header and bottom nav instead of the raw viewport center.
  // Uses CSS calc() so values are always live — no stale measurements on tab switch.
  const pwaCenterOffset = 'calc((var(--fire-header-height, 168px) - var(--fire-bottom-clearance, 86px)) / 2)';
  const pwaCardMaxHExpr = 'calc(var(--fire-app-height, 100dvh) - var(--fire-header-height, 168px) - var(--fire-bottom-clearance, 86px) - 32px)';

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

  // ── PWA: measure container slot height from CSS variables ──────────────────
  useEffect(() => {
    if (!usePwaSnap || isDesktop) return;
    const update = () => {
      setPwaSlotH(getPwaViewportSlotHeight());
    };
    update();
    window.addEventListener('resize', update);
    window.visualViewport?.addEventListener('resize', update);
    window.visualViewport?.addEventListener('scroll', update);
    return () => {
      window.removeEventListener('resize', update);
      window.visualViewport?.removeEventListener('resize', update);
      window.visualViewport?.removeEventListener('scroll', update);
    };
  }, [isDesktop, usePwaSnap]);

  // ── PWA: pwaIndex → activeCardId + currentIndexRef ────────────────────────
  useEffect(() => {
    if (!usePwaSnap || isDesktop) return;
    const card = cards[pwaIndex];
    if (!card) return;
    currentIndexRef.current = pwaIndex;
    setActiveCardId(id => (id === card.id ? id : card.id));
  }, [cards, isDesktop, pwaIndex, usePwaSnap]);

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
    writeStandaloneDeckState(standaloneDeckStateKey, {
      activeCardId: preferredCardId ?? activeCardIdRef.current,
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
    if (info.offset.y < -50) navigatePwa(1);       // swipe up → next card
    else if (info.offset.y > 50) navigatePwa(-1);  // swipe down → prev card
  }, [navigatePwa]);

  // Returns the animate target for a card at cards[index] relative to pwaIndex
  const getPwaCardStyle = useCallback((index: number) => {
    const diff = index - pwaIndex;
    if (diff === 0) return { y: 0,           scale: 1,    opacity: 1, zIndex: 10 };
    if (diff === 1) return { y: pwaSlotH * PWA_NEIGHBOUR_OFFSET_RATIO,    scale: 1,    opacity: 1, zIndex: 8  };
    if (diff === -1) return { y: -pwaSlotH * PWA_NEIGHBOUR_OFFSET_RATIO,  scale: 1,    opacity: 1, zIndex: 8  };
    return { y: diff > 0 ? pwaSlotH * PWA_OFFSCREEN_OFFSET_RATIO : -pwaSlotH * PWA_OFFSCREEN_OFFSET_RATIO, scale: 1, opacity: 1, zIndex: 2 };
  }, [pwaIndex, pwaSlotH]);

  // Reset pwaIndex when filter/day changes
  const prevResetKeyRef = useRef(resetKey);
  useEffect(() => {
    if (resetKey === prevResetKeyRef.current) return;
    prevResetKeyRef.current = resetKey;

    if (usePwaSnap) {
      clearPwaTopReturn();
      currentIndexRef.current = 0;
      setActiveCardId(cards[0]?.id ?? null);
      setPwaIndex(0);
      return;
    }

    const nextId = cards[0]?.id ?? null;
    const frame = window.requestAnimationFrame(() => setActiveCardId(nextId));
    const root = containerRef.current;
    if (!usePageScroll && !root) return () => window.cancelAnimationFrame(frame);
    animateScrollToTop();
    return () => window.cancelAnimationFrame(frame);
  }, [animateScrollToTop, cards, clearPwaTopReturn, resetKey, usePageScroll, usePwaSnap]);

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
      restorePwaIndex();
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
  const mobileStackClass = isIOS
    ? '-mt-[10vh] flex w-full min-h-[74svh] items-center justify-center first:mt-0 md:-mt-[12vh] md:min-h-[76dvh]'
    : '-mt-[14vh] flex w-full min-h-[74svh] items-center justify-center first:mt-0 md:-mt-[16vh] md:min-h-[76dvh]';
  const enableContainerSnap = usePwaSnap && !usePageScroll;
  const remainingCount = Math.max(0, (total || 0) - cards.length);
  // For load dock, use pwaIndex directly in PWA mode
  const showStandaloneLoadDock = usePwaSnap
    && Boolean(hasMore)
    && pwaIndex >= Math.max(0, cards.length - 2);

  // ── PWA render: fixed-position transform stack (TikTok/Reels style) ─────────
  if (usePwaSnap) {
    return (
      <>
        {/* Full-canvas PWA deck so cards pass under floating chrome. */}
        <div className="fixed inset-0 z-10 overflow-hidden">
          {cards.map((card, index) => {
            const diff = index - pwaIndex;
            // Keep one offscreen buffer so neighbours glide out instead of popping away.
            if (Math.abs(diff) > PWA_RENDER_RADIUS) return null;
            const style = getPwaCardStyle(index);
            const isCurrent = index === pwaIndex;
            const isOffscreenBuffer = Math.abs(diff) === PWA_RENDER_RADIUS;

            return (
              <motion.div
                key={card.id}
                className="absolute inset-0 flex items-center justify-center px-2"
                animate={{ y: style.y, scale: style.scale, opacity: style.opacity }}
                transition={{
                  type: 'spring',
                  stiffness: isOffscreenBuffer ? 250 : 300,
                  damping: isOffscreenBuffer ? 34 : 30,
                  mass: isOffscreenBuffer ? 1 : 0.9,
                }}
                drag={isCurrent ? 'y' : false}
                dragConstraints={{ top: 0, bottom: 0 }}
                dragElastic={0.08}
                onDragEnd={handlePwaDragEnd}
                style={{
                  zIndex: style.zIndex,
                  pointerEvents: isCurrent ? 'auto' : 'none',
                  willChange: 'transform',
                  backfaceVisibility: 'hidden',
                }}
              >
                <div className="flex h-full w-full items-center justify-center" style={{ marginTop: pwaCenterOffset }}>
                  <div
                    className="w-full max-w-[472px]"
                    style={{
                      ['--fire-card-max-height' as string]: pwaCardMaxHExpr,
                      ['--fire-card-aspect' as string]: '9 / 14',
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
              </motion.div>
            );
          })}
        </div>

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
    'px-2 sm:px-3 lg:px-4',
    'pt-[calc(var(--fire-header-height,168px)+40px)]',
    isDesktop ? 'pb-8' : 'pb-[88px]',
    'lg:snap-none',
  ].join(' ');

  const containerStyle = usePageScroll
    ? undefined
    : { WebkitOverflowScrolling: 'touch' as const };

  return (
    <div ref={containerRef} className={containerClasses} style={containerStyle}>
      <div className="mx-auto w-full lg:max-w-none lg:px-1 xl:px-2">
        {isDesktop ? (
          <div className="grid grid-cols-5 gap-[14px] xl:gap-4 2xl:grid-cols-6">
            <AnimatePresence mode="popLayout">
              {cards.map((card, index) => {
                const isActive = resolvedActive === card.id;
                return (
                  <motion.div
                    key={card.id}
                    layout
                    initial={{ opacity: 0, scale: 0.88 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={{ opacity: 0, scale: 0.88 }}
                    transition={{ type: 'spring', damping: 24, stiffness: 300, mass: 0.75 }}
                    whileHover={{ y: -4, scale: 1.008 }}
                  >
                    <VirtualSlot
                      item={card}
                      index={index}
                      isActive={isActive}
                      isDesktop
                      mobileAutoplayEnabled={mobileAutoplayEnabled}
                      onOpenDetails={() => onOpenCard?.(card)}
                      onBeforeOpenPost={persistStandaloneDeckState}
                    />
                  </motion.div>
                );
              })}
            </AnimatePresence>
          </div>
        ) : (
          <div className="flex flex-col">
            <AnimatePresence mode="sync">
              {cards.map((card, index) => {
                const isActive = resolvedActive === card.id;
                return (
                  <motion.div
                    key={card.id}
                    initial={{ opacity: 0, y: 10, scale: 0.985 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0, y: -8, scale: 0.97 }}
                    transition={{ duration: 0.2, ease: [0.22, 1, 0.36, 1] }}
                    className={mobileStackClass}
                    style={{ zIndex: isActive ? 40 : 10 }}
                  >
                    <div className="w-full max-w-[472px]">
                    <VirtualSlot
                      item={card}
                      index={index}
                      isActive={isActive}
                      isDesktop={false}
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
