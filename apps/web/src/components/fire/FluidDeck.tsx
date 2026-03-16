'use client';

import { useCallback, useEffect, useRef, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Loader2 } from 'lucide-react';
import { FireItem } from './types';
import { FireCard3D } from './FireCard3D';

interface FluidDeckProps {
  cards: FireItem[];
  hasMore?: boolean;
  loadingMore?: boolean;
  onLoadMore?: () => void;
}

/* ─── Virtualized card slot ──────────────────────────────────
   Uses IntersectionObserver to only mount the heavy FireCard3D
   when the slot is near the viewport. Off-screen slots render a
   lightweight placeholder that preserves scroll height. */
function VirtualSlot({
  item,
  isActive,
  index,
  activeLift,
  activeScale,
}: {
  item: FireItem;
  isActive: boolean;
  index: number;
  activeLift: number;
  activeScale: number;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const [isVisible, setIsVisible] = useState(false);

  useEffect(() => {
    const el = ref.current;
    if (!el) return;

    const observer = new IntersectionObserver(
      ([entry]) => setIsVisible(entry.isIntersecting),
      { rootMargin: '200% 0px' } // generous buffer: ~2 screens above/below
    );
    observer.observe(el);
    return () => observer.disconnect();
  }, []);

  return (
    <div ref={ref} data-card-id={item.id} className="w-full flex items-center justify-center">
      {isVisible ? (
        <motion.div
          initial={{ opacity: 0, y: 12, scale: 0.99 }}
          animate={{
            opacity: isActive ? 1 : 0.72,
            y: isActive ? activeLift : 18,
            scale: isActive ? activeScale : 0.94,
            filter: isActive ? 'blur(0px)' : 'blur(0.4px)',
          }}
          transition={{ duration: 0.22, delay: Math.min(index * 0.016, 0.1), ease: [0.22, 1, 0.36, 1] }}
          className="relative w-full max-w-[472px] will-change-transform"
          style={{ zIndex: isActive ? 40 : 10 }}
        >
          <div
            className={[
              'rounded-[22px] transition-shadow duration-250',
              isActive
                ? 'shadow-[0_20px_38px_rgba(0,0,0,0.24)] dark:shadow-[0_24px_44px_rgba(0,0,0,0.6)]'
                : 'shadow-[0_12px_24px_rgba(0,0,0,0.18)] dark:shadow-[0_14px_28px_rgba(0,0,0,0.46)]',
            ].join(' ')}
          >
            <FireCard3D item={item} highlighted={isActive} />
          </div>
        </motion.div>
      ) : (
        /* Lightweight placeholder — 80vh on mobile matches snap-scroll card height */
        <div className="w-full max-w-[472px] aspect-[4/5] lg:aspect-auto lg:h-[400px]" />
      )}
    </div>
  );
}

export default function FluidDeck({ cards, hasMore, loadingMore, onLoadMore }: FluidDeckProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const sentinelRef = useRef<HTMLDivElement>(null);
  const [activeCardId, setActiveCardId] = useState<string | null>(cards[0]?.id ?? null);
  const [hoverCardId, setHoverCardId] = useState<string | null>(null);
  const [isDesktop, setIsDesktop] = useState(false);
  const [isStandalone, setIsStandalone] = useState(false);
  const [isIOS, setIsIOS] = useState(false);

  // Track desktop vs mobile
  useEffect(() => {
    const mql = window.matchMedia('(min-width: 1024px)');
    const handler = (e: MediaQueryListEvent | MediaQueryList) => setIsDesktop(e.matches);
    handler(mql);
    mql.addEventListener('change', handler as (e: MediaQueryListEvent) => void);
    return () => mql.removeEventListener('change', handler as (e: MediaQueryListEvent) => void);
  }, []);

  useEffect(() => {
    setIsIOS(/iPad|iPhone|iPod/.test(navigator.userAgent));
    const mql = window.matchMedia('(display-mode: standalone)');
    const handler = (e: MediaQueryListEvent | MediaQueryList) => setIsStandalone('matches' in e ? e.matches : (e as MediaQueryList).matches);
    handler(mql);
    mql.addEventListener?.('change', handler as (e: MediaQueryListEvent) => void);
    return () => mql.removeEventListener?.('change', handler as (e: MediaQueryListEvent) => void);
  }, []);

  // Reset active card when cards change (day/filter switch)
  useEffect(() => {
    setActiveCardId(cards[0]?.id ?? null);
    setHoverCardId(null);
    const root = containerRef.current;
    if (root) root.scrollTo({ top: 0, behavior: 'smooth' });
  }, [cards]);

  // Mobile: sync highlight to centered card via scroll
  useEffect(() => {
    if (isDesktop) return;
    const root = containerRef.current;
    if (!root) return;

    let frame = 0;
    const syncActiveToCenter = () => {
      frame = 0;
      const rootRect = root.getBoundingClientRect();
      const centerY = rootRect.top + rootRect.height * 0.5;
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

      if (nextId) setActiveCardId((c) => (c === nextId ? c : nextId));
    };

    const requestSync = () => {
      if (frame) cancelAnimationFrame(frame);
      frame = requestAnimationFrame(syncActiveToCenter);
    };

    requestSync();
    root.addEventListener('scroll', requestSync, { passive: true });
    window.addEventListener('resize', requestSync);
    return () => {
      if (frame) cancelAnimationFrame(frame);
      root.removeEventListener('scroll', requestSync);
      window.removeEventListener('resize', requestSync);
    };
  }, [cards, isDesktop]);

  // Infinite scroll: watch sentinel at bottom of list
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
      { rootMargin: '400px 0px' }
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

  const resolvedActive = isDesktop ? (hoverCardId ?? null) : activeCardId;
  const mobileActiveLift = isIOS && !isStandalone ? 0 : -8;
  const mobileActiveScale = isIOS && !isStandalone ? 1.02 : 1.035;
  const stackClass = isIOS && !isStandalone
    ? '-mt-[10vh] flex w-full min-h-[74svh] snap-center items-center justify-center first:mt-0 md:-mt-[12vh] md:min-h-[76dvh] lg:mt-0 lg:min-h-0 lg:snap-none lg:py-4'
    : '-mt-[14vh] flex w-full min-h-[74svh] snap-center items-center justify-center first:mt-0 md:-mt-[16vh] md:min-h-[76dvh] lg:mt-0 lg:min-h-0 lg:snap-none lg:py-4';

  return (
    <div
      ref={containerRef}
      className={[
        'relative h-full w-full overflow-y-auto overflow-x-hidden overscroll-y-contain scroll-smooth snap-y snap-mandatory px-2 transform-gpu hide-scrollbar sm:px-3 lg:snap-none lg:px-4',
        '[@media(hover:hover)_and_(pointer:fine)]:snap-none',
        'pb-[calc(170px+env(safe-area-inset-bottom))] sm:pb-[calc(150px+env(safe-area-inset-bottom))] lg:pb-[160px]',
        'pt-[calc(210px+env(safe-area-inset-top))] sm:pt-[calc(196px+env(safe-area-inset-top))] lg:pt-[calc(260px+env(safe-area-inset-top))]',
      ].join(' ')}
      style={{
        WebkitOverflowScrolling: 'touch',
        scrollPaddingTop: 'calc(200px + env(safe-area-inset-top))',
        scrollPaddingBottom: 'calc(170px + env(safe-area-inset-bottom))',
      }}
    >
      <div className="mx-auto w-full lg:max-w-none lg:px-8 xl:px-12 2xl:px-16">
        <div className="flex flex-col lg:grid lg:grid-cols-3 lg:gap-6 xl:gap-8 2xl:gap-10 lg:py-8 justify-items-center">
          <AnimatePresence mode={isDesktop ? 'popLayout' : 'sync'}>
            {cards.map((card, idx) => {
              const isActive = resolvedActive === card.id;
              return (
                <motion.div
                  layout={isDesktop ? true : false}
                  initial={{ opacity: 0, y: 10, scale: 0.985 }}
                  animate={{ opacity: 1, scale: 1 }}
                  exit={{ opacity: 0, y: -8, scale: 0.97 }}
                  transition={isDesktop
                    ? { type: 'spring', damping: 22, stiffness: 300, mass: 0.75 }
                    : { duration: 0.2, ease: [0.22, 1, 0.36, 1] }
                  }
                  key={card.id}
                  onPointerEnter={() => isDesktop && setHoverCardId(card.id)}
                  onPointerLeave={() => isDesktop && setHoverCardId(null)}
                  className={[
                    stackClass,
                    '[@media(hover:hover)_and_(pointer:fine)]:snap-none',
                  ].join(' ')}
                >
                  <VirtualSlot item={card} index={idx} isActive={isActive} activeLift={mobileActiveLift} activeScale={mobileActiveScale} />
                </motion.div>
              );
            })}
          </AnimatePresence>
        </div>

        {/* Infinite scroll sentinel */}
        <div ref={sentinelRef} className="h-1 w-full" />

        {/* Loading indicator for next page */}
        {loadingMore && (
          <div className="flex justify-center py-8">
            <Loader2 className="h-8 w-8 animate-spin text-lime" />
          </div>
        )}
      </div>
    </div>
  );
}
