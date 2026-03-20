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
                scale: isActive ? 1.012 : 1,
                filter: 'blur(0px)',
              }
            : {
                opacity: isActive ? 1 : 0.72,
                y: isActive ? -8 : 18,
                scale: isActive ? 1.035 : 0.94,
                filter: isActive ? 'blur(0px)' : 'blur(0.4px)',
              }}
          transition={{ duration: 0.22, delay: Math.min(index * 0.016, 0.1), ease: [0.22, 1, 0.36, 1] }}
          className="relative w-full will-change-transform"
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

export default function FluidDeck({ cards, hasMore, loadingMore, onLoadMore, onOpenCard }: FluidDeckProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const sentinelRef = useRef<HTMLDivElement>(null);
  const [activeCardId, setActiveCardId] = useState<string | null>(cards[0]?.id ?? null);
  const [hoverCardId, setHoverCardId] = useState<string | null>(null);
  const [isDesktop, setIsDesktop] = useState(() => typeof window !== 'undefined' && window.matchMedia('(min-width: 1024px)').matches);
  const [isStandalone, setIsStandalone] = useState(() => typeof window !== 'undefined' && window.matchMedia('(display-mode: standalone)').matches);
  const [isIOS] = useState(() => typeof navigator !== 'undefined' && /iPad|iPhone|iPod/.test(navigator.userAgent));

  useEffect(() => {
    const mql = window.matchMedia('(min-width: 1024px)');
    const handler = (event: MediaQueryListEvent) => setIsDesktop(event.matches);
    mql.addEventListener('change', handler as (event: MediaQueryListEvent) => void);
    return () => mql.removeEventListener('change', handler as (event: MediaQueryListEvent) => void);
  }, []);

  useEffect(() => {
    const mql = window.matchMedia('(display-mode: standalone)');
    const handler = (event: MediaQueryListEvent) => setIsStandalone(event.matches);
    mql.addEventListener?.('change', handler as (event: MediaQueryListEvent) => void);
    return () => mql.removeEventListener?.('change', handler as (event: MediaQueryListEvent) => void);
  }, []);

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

      if (nextId) setActiveCardId((current) => (current === nextId ? current : nextId));
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

  const resolvedActive = isDesktop ? hoverCardId : activeCardId;
  const mobileStackClass = isIOS && !isStandalone
    ? '-mt-[10vh] flex w-full min-h-[74svh] snap-center items-center justify-center first:mt-0 md:-mt-[12vh] md:min-h-[76dvh]'
    : '-mt-[14vh] flex w-full min-h-[74svh] snap-center items-center justify-center first:mt-0 md:-mt-[16vh] md:min-h-[76dvh]';

  return (
    <>
      <div
        ref={containerRef}
        className={[
          'relative h-full w-full overflow-y-auto overflow-x-hidden overscroll-y-contain scroll-smooth transform-gpu hide-scrollbar',
          'px-2 sm:px-3 lg:px-4',
          'pb-[calc(var(--fire-bottom-clearance,188px)+env(safe-area-inset-bottom))]',
          'pt-[calc(var(--fire-header-height,168px)+24px)]',
          'snap-y snap-mandatory lg:snap-none',
          '[@media(hover:hover)_and_(pointer:fine)]:snap-none',
        ].join(' ')}
        style={{
          WebkitOverflowScrolling: 'touch',
          scrollPaddingTop: 'calc(var(--fire-header-height, 168px) + 24px)',
          scrollPaddingBottom: 'calc(var(--fire-bottom-clearance, 188px) + env(safe-area-inset-bottom))',
        }}
      >
        <div className="mx-auto w-full lg:max-w-none lg:px-1 xl:px-2">
          {isDesktop ? (
            <div className="grid grid-cols-5 gap-[14px] xl:gap-4 2xl:grid-cols-6">
              <AnimatePresence mode="popLayout">
                {cards.map((card, index) => {
                  const isActive = resolvedActive === card.id;
                  return (
                    <motion.div
                      layout
                      key={card.id}
                      initial={{ opacity: 0, y: 14, scale: 0.986 }}
                      animate={{ opacity: 1, y: 0, scale: 1 }}
                      exit={{ opacity: 0, y: -8, scale: 0.98 }}
                      transition={{ type: 'spring', damping: 22, stiffness: 300, mass: 0.75 }}
                      onPointerEnter={() => setHoverCardId(card.id)}
                      onPointerLeave={() => setHoverCardId((current) => (current === card.id ? null : current))}
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

          <div ref={sentinelRef} className="h-1 w-full" />

          {loadingMore && (
            <div className="flex justify-center py-8">
              <Loader2 className="h-8 w-8 animate-spin text-lime" />
            </div>
          )}
        </div>
      </div>

    </>
  );
}
