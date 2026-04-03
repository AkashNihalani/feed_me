'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
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
}

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

function getScrollContainer(root: HTMLDivElement | null, usePageScroll: boolean): HTMLElement | null {
  if (typeof window === 'undefined') return null;
  if (!usePageScroll) return root;
  return document.scrollingElement instanceof HTMLElement ? document.scrollingElement : document.documentElement;
}

/** Fast, sharp scroll animation for TikTok-like snap feel. */
function animateScrollTo(
  target: HTMLElement | Window,
  to: number,
  duration = 260,
): void {
  const isWindow = target === window;
  const getScroll = () => (isWindow ? window.scrollY : (target as HTMLElement).scrollTop);
  const from = getScroll();
  const delta = to - from;
  if (Math.abs(delta) < 2) return;

  const start = performance.now();
  // Smooth ease-out curve — fast start, gentle deceleration
  const ease = (t: number) => 1 - Math.pow(1 - t, 2.4);

  const step = (now: number) => {
    const elapsed = now - start;
    const progress = Math.min(elapsed / duration, 1);
    const value = from + delta * ease(progress);

    if (isWindow) {
      window.scrollTo(0, value);
    } else {
      (target as HTMLElement).scrollTop = value;
    }

    if (progress < 1) {
      requestAnimationFrame(step);
    }
  };
  requestAnimationFrame(step);
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

function VirtualSlot({
  item,
  index,
  isActive,
  isDesktop,
  onOpenDetails,
}: {
  item: FireItem;
  index: number;
  isActive: boolean;
  isDesktop: boolean;
  onOpenDetails: () => void;
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
            ? {
                opacity: 1,
                y: 0,
                scale: 1,
                filter: 'none',
              }
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
              onOpenDetails={onOpenDetails}
            />
          </div>
        </motion.div>
      ) : (
        <div className="w-full aspect-[4/5] lg:aspect-[11/14]" />
      )}
    </div>
  );
}

export default function FluidDeck({ cards, hasMore, loadingMore, onLoadMore, onOpenCard, usePageScroll = false, resetKey }: FluidDeckProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const sentinelRef = useRef<HTMLDivElement>(null);
  const [activeCardId, setActiveCardId] = useState<string | null>(cards[0]?.id ?? null);
  const activeCardIdRef = useRef<string | null>(cards[0]?.id ?? null);
  const [isDesktop, setIsDesktop] = useState(() => typeof window !== 'undefined' && window.matchMedia('(min-width: 1024px)').matches);
  const [isStandalone, setIsStandalone] = useState(isStandaloneDisplayMode);
  const [isIOS] = useState(() => typeof navigator !== 'undefined' && /iPad|iPhone|iPod/.test(navigator.userAgent));
  const currentIndexRef = useRef(0);
  const usePwaSnap = isStandalone;

  useEffect(() => {
    activeCardIdRef.current = activeCardId;
  }, [activeCardId]);

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

  // Only scroll to top when the page changes (day/filter), NOT on auto-refresh or load-more
  const prevResetKeyRef = useRef(resetKey);
  useEffect(() => {
    if (resetKey === prevResetKeyRef.current) return;
    prevResetKeyRef.current = resetKey;

    const nextId = cards[0]?.id ?? null;
    const frame = window.requestAnimationFrame(() => {
      setActiveCardId(nextId);
    });

    const root = containerRef.current;
    if (!root) {
      return () => window.cancelAnimationFrame(frame);
    }
    if (usePageScroll) {
      window.scrollTo({ top: 0, behavior: 'smooth' });
    } else {
      root.scrollTo({ top: 0, behavior: 'smooth' });
    }
    return () => window.cancelAnimationFrame(frame);
  }, [resetKey, cards, usePageScroll]);

  useEffect(() => {
    if (isDesktop) return;
    const root = containerRef.current;
    if (!root) return;

    let frame = 0;
    const syncActiveToCenter = () => {
      frame = 0;
      const centerY = usePageScroll
        ? usePwaSnap
          ? getPageFocusCenterY()
          : window.innerHeight * 0.5
        : (() => {
            const rootRect = root.getBoundingClientRect();
            return rootRect.top + rootRect.height * 0.5;
          })();
      const nodes = root.querySelectorAll<HTMLElement>('[data-card-id]');

      let nextId: string | null = null;
      let bestDistance = Number.POSITIVE_INFINITY;
      nodes.forEach((node) => {
        const rect = node.getBoundingClientRect();
        const cardCenter = rect.top + rect.height * 0.5;
        const distance = Math.abs(cardCenter - centerY);
        if (distance < bestDistance) {
          bestDistance = distance;
          nextId = node.dataset.cardId ?? null;
        }
      });

      if (nextId) setActiveCardId((current) => (current === nextId ? current : nextId));
    };

    const requestSync = () => {
      if (frame) cancelAnimationFrame(frame);
      frame = requestAnimationFrame(syncActiveToCenter);
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
  }, [cards, isDesktop, usePageScroll, usePwaSnap]);

  // PWA: fully take over touch scrolling — one swipe = one card, always
  useEffect(() => {
    if (isDesktop || !usePwaSnap) return;
    const root = containerRef.current;
    if (!root) return;

    const getCardNodes = () => Array.from(root.querySelectorAll<HTMLElement>('[data-card-id]'));

    const scrollToIndex = (index: number, animated = true) => {
      const nodes = getCardNodes();
      const target = nodes[index];
      if (!target) return;
      currentIndexRef.current = index;
      const targetId = target.dataset.cardId ?? null;
      setActiveCardId((current) => (current === targetId ? current : targetId));

      const nextTop = getSnapScrollTop(target, root, usePageScroll);
      if (animated) {
        animateScrollTo(usePageScroll ? window : root, nextTop, 420);
      } else {
        if (usePageScroll) {
          window.scrollTo(0, nextTop);
        } else {
          root.scrollTop = nextTop;
        }
      }
    };

    // Sync currentIndexRef to whatever card is closest on mount
    const nodes = getCardNodes();
    if (nodes.length > 0) {
      const focusCenter = getSnapViewportCenter(root, usePageScroll);
      let bestIdx = 0;
      let bestDist = Infinity;
      nodes.forEach((node, i) => {
        const rect = node.getBoundingClientRect();
        const d = Math.abs(rect.top + rect.height / 2 - focusCenter);
        if (d < bestDist) { bestDist = d; bestIdx = i; }
      });
      currentIndexRef.current = bestIdx;
    }

    // Touch state (non-reactive, only used inside handlers)
    let startY = 0;
    let startX = 0;
    let swiped = false;
    let directionLocked: 'vertical' | 'horizontal' | null = null;

    const onTouchStart = (e: TouchEvent) => {
      const t = e.touches[0];
      if (!t) return;
      startY = t.clientY;
      startX = t.clientX;
      swiped = false;
      directionLocked = null;
    };

    const onTouchMove = (e: TouchEvent) => {
      const t = e.touches[0];
      if (!t) return;
      const dy = t.clientY - startY;
      const dx = t.clientX - startX;

      // Lock direction on first significant movement
      if (!directionLocked) {
        if (Math.abs(dy) > 8 || Math.abs(dx) > 8) {
          directionLocked = Math.abs(dy) >= Math.abs(dx) ? 'vertical' : 'horizontal';
        }
      }

      // Only intercept vertical swipes
      if (directionLocked === 'vertical') {
        e.preventDefault(); // Kill native scroll momentum entirely

        if (!swiped && Math.abs(dy) > 30) {
          swiped = true;
          const nodes = getCardNodes();
          const maxIdx = nodes.length - 1;
          const nextIdx = dy < 0
            ? Math.min(currentIndexRef.current + 1, maxIdx) // swipe up → next card
            : Math.max(currentIndexRef.current - 1, 0);      // swipe down → prev card
          scrollToIndex(nextIdx);
        }
      }
    };

    const onTouchEnd = () => {
      directionLocked = null;
      // If no swipe was triggered (tiny tap/drag), snap to current card
      if (!swiped) {
        scrollToIndex(currentIndexRef.current);
      }
    };

    // Non-passive so we can preventDefault on touchmove
    root.addEventListener('touchstart', onTouchStart, { passive: true });
    root.addEventListener('touchmove', onTouchMove, { passive: false });
    root.addEventListener('touchend', onTouchEnd, { passive: true });
    root.addEventListener('touchcancel', onTouchEnd, { passive: true });

    // Initial snap
    scrollToIndex(currentIndexRef.current, false);

    return () => {
      root.removeEventListener('touchstart', onTouchStart);
      root.removeEventListener('touchmove', onTouchMove);
      root.removeEventListener('touchend', onTouchEnd);
      root.removeEventListener('touchcancel', onTouchEnd);
    };
  }, [cards, isDesktop, usePageScroll, usePwaSnap]);

  const handleLoadMore = useCallback(() => {
    if (hasMore && !loadingMore && onLoadMore) onLoadMore();
  }, [hasMore, loadingMore, onLoadMore]);

  useEffect(() => {
    const sentinel = sentinelRef.current;
    if (!sentinel) return;

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) handleLoadMore();
      },
      { rootMargin: '400px 0px' },
    );
    observer.observe(sentinel);
    return () => observer.disconnect();
  }, [handleLoadMore]);

  if (!cards || cards.length === 0) {
    return (
      <div className="mt-16 flex h-64 items-center justify-center font-mono text-sm tracking-widest text-neutral-500">
        NO ANOMALIES DETECTED
      </div>
    );
  }

  const resolvedActive = isDesktop ? null : activeCardId;
  const mobileStackClass = isIOS && isStandalone
    ? '-mt-[124px] flex w-full min-h-[56dvh] items-center justify-center first:mt-0 md:-mt-[136px] md:min-h-[58dvh]'
    : isIOS
      ? '-mt-[10vh] flex w-full min-h-[74svh] items-center justify-center first:mt-0 md:-mt-[12vh] md:min-h-[76dvh]'
      : '-mt-[14vh] flex w-full min-h-[74svh] items-center justify-center first:mt-0 md:-mt-[16vh] md:min-h-[76dvh]';
  const enableContainerSnap = usePwaSnap && !usePageScroll;

  const containerClasses = [
    usePageScroll
      ? 'relative w-full overflow-visible'
      : enableContainerSnap
        // PWA: no scroll-smooth, no CSS snap — JS handles snapping for TikTok-like feel
        ? 'relative h-full w-full overflow-y-auto overflow-x-hidden overscroll-y-contain hide-scrollbar'
        : 'relative h-full w-full overflow-y-auto overflow-x-hidden overscroll-y-contain scroll-smooth hide-scrollbar',
    'px-2 sm:px-3 lg:px-4',
    'pt-[calc(var(--fire-header-height,168px)+40px)]',
    usePwaSnap
      ? 'pb-0'
      : isDesktop
        ? 'pb-8'
        : 'pb-[88px]',
    'lg:snap-none',
  ].join(' ');

  const containerStyle = usePageScroll
    ? undefined
    : enableContainerSnap
      ? { touchAction: 'none' as const } // PWA: JS fully controls scroll, no native momentum
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
                    initial={{ opacity: 0, y: 14, scale: 0.986 }}
                    animate={{ opacity: 1, y: 0, scale: 1 }}
                    exit={{ opacity: 0, y: -8, scale: 0.98 }}
                    transition={{ type: 'spring', damping: 22, stiffness: 300, mass: 0.75 }}
                    whileHover={{ y: -4, scale: 1.008 }}
                  >
                    <VirtualSlot
                      item={card}
                      index={index}
                      isActive={isActive}
                      isDesktop
                      onOpenDetails={() => onOpenCard?.(card)}
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
                        onOpenDetails={() => undefined}
                      />
                    </div>
                  </motion.div>
                );
              })}
            </AnimatePresence>
          </div>
        )}

        <div
          ref={sentinelRef}
          className={usePwaSnap ? 'h-[calc(var(--fire-bottom-clearance,86px)+18px)] w-full shrink-0' : 'h-1 w-full'}
        />

        {loadingMore && (
          <div className="flex justify-center py-8">
            <Loader2 className="h-8 w-8 animate-spin text-lime" />
          </div>
        )}
      </div>
    </div>
  );
}
