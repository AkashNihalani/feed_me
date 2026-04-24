'use client';

import { memo, startTransition, useDeferredValue, useEffect, useMemo, useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { ScatterPoint } from './dashboardTypes';

type Blip = {
  id: string;
  x: number;
  y: number;
  isCompetitor: boolean;
  percentile: number | null;
  handle: string;
  date: string;
  views: number;
  daysAgo: number;
  postIndex: number; // 0 = most recent
};

function formatPointDate(value: string | null): string {
  if (!value) return 'N/A';
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return 'N/A';
  return d.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
}

const POST_COUNTS = [5, 15, 25] as const;
type PostCount = typeof POST_COUNTS[number];
const TOP_ZONE_PERCENT = 35;

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
}

function FeedScatterField({ points, windowDays }: { points: ScatterPoint[]; windowDays: number }) {
  const [activeCount, setActiveCount] = useState<PostCount>(15);
  const [hoveredPoint, setHoveredPoint] = useState<Blip | null>(null);
  const [isCompactViewport, setIsCompactViewport] = useState(false);
  const deferredActiveCount = useDeferredValue(activeCount);

  useEffect(() => {
    if (typeof window === 'undefined') return undefined;

    const media = window.matchMedia('(max-width: 768px)');
    const handleChange = () => setIsCompactViewport(media.matches);
    handleChange();

    if (typeof media.addEventListener === 'function') {
      media.addEventListener('change', handleChange);
      return () => media.removeEventListener('change', handleChange);
    }

    media.addListener(handleChange);
    return () => media.removeListener(handleChange);
  }, []);

  const allPoints = useMemo<Blip[]>(() => {
    const src = Array.isArray(points) ? points : [];
    const sorted = [...src].sort((a, b) => {
      const dayDiff = (Number(a.days_ago) || 0) - (Number(b.days_ago) || 0);
      if (dayDiff !== 0) return dayDiff;
      return (b.posted_at_ist || '').localeCompare(a.posted_at_ist || '');
    });

    const days = sorted.map((p) => Math.max(0, Number(p.days_ago) || 0));
    const groupSizes = new Map<number, number>();
    for (const day of days) {
      groupSizes.set(day, (groupSizes.get(day) ?? 0) + 1);
    }
    const groupOffsets = new Map<number, number>();
    const windowMaxDays = Math.max(1, windowDays - 1);
    const edgePadding = isCompactViewport ? 6.5 : 5.25;
    const usableWidth = 100 - edgePadding * 2;

    return sorted.map((p, index) => {
      const percentile = typeof p.percentile_performance === 'number' ? p.percentile_performance : null;
      const daysAgo = Math.max(0, Number(p.days_ago) || 0);
      const groupSize = groupSizes.get(daysAgo) ?? 1;
      const orderWithinDay = groupOffsets.get(daysAgo) ?? 0;
      groupOffsets.set(daysAgo, orderWithinDay + 1);

      const latestRatio = 1 - clamp(daysAgo / windowMaxDays, 0, 1);
      const baseX = edgePadding + latestRatio * usableWidth;
      const spread = groupSize > 1
        ? Math.min(isCompactViewport ? 7.25 : 6.25, 2.2 + groupSize * (isCompactViewport ? 0.42 : 0.34))
        : 0;
      const centeredOffset = groupSize > 1
        ? ((orderWithinDay / Math.max(1, groupSize - 1)) - 0.5) * spread
        : 0;

      return {
        id: p.post_key || `${p.handle}-${p.days_ago}`,
        x: clamp(baseX + centeredOffset, edgePadding, 100 - edgePadding),
        y: percentile === null ? 48 : clamp(100 - percentile, 6, 96),
        isCompetitor: false,
        percentile,
        handle: p.handle || 'unknown',
        date: formatPointDate(p.posted_at_ist),
        views: Math.max(0, Number(p.views) || 0),
        daysAgo,
        postIndex: index,
      };
    });
  }, [isCompactViewport, points, windowDays]);

  const visiblePoints = useMemo(
    () => allPoints.slice(0, Math.min(deferredActiveCount, allPoints.length)),
    [allPoints, deferredActiveCount],
  );

  const activeHoveredPoint = useMemo(() => {
    if (!hoveredPoint) return null;
    return visiblePoints.some((point) => point.id === hoveredPoint.id) ? hoveredPoint : null;
  }, [hoveredPoint, visiblePoints]);

  return (
    <motion.div
      className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4"
    >
      <div className="relative z-10 mb-2 flex items-start justify-between gap-2">
        <div>
          <span className="fm-label fm-depth-title">Percentage Map</span>
          <div className="mt-0.5 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/34">
            Latest {visiblePoints.length} of {allPoints.length}
          </div>
        </div>
        <div className="hide-scrollbar flex shrink-0 items-center gap-0.5 rounded-full border border-black/6 bg-black/[0.025] p-[3px] dark:border-white/8 dark:bg-white/[0.04]">
          {POST_COUNTS.map(count => (
            <motion.button
              key={count}
              onClick={() => {
                startTransition(() => setActiveCount(count));
              }}
              whileTap={{ scale: 0.95 }}
              className={`relative rounded-full px-3 py-1.25 text-[10px] font-black uppercase tracking-[0.12em] sm:px-3.5 sm:py-1.5 ${activeCount === count ? 'z-10 text-white' : 'z-0 text-foreground/42 dark:text-white/40'}`}
            >
              {activeCount === count && (
                <motion.span
                  layoutId="scatter-pill-bg"
                  className="absolute inset-0 rounded-full bg-[#E11D48] shadow-[0_10px_20px_-10px_rgba(225,29,72,0.55)] dark:shadow-[0_10px_20px_-10px_rgba(225,29,72,0.45)]"
                  transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }}
                />
              )}
              <span className="relative z-10">{count}</span>
            </motion.button>
          ))}
        </div>
      </div>

      {/* Chart area */}
      <div className="relative min-h-[208px] w-full flex-1 rounded-[14px] fm-depth-inner sm:min-h-[228px]">
        <div
          className="absolute inset-x-0 border-t border-black/12 dark:border-white/12"
          style={{ top: `${TOP_ZONE_PERCENT}%` }}
        >
          <span className="absolute -top-3.5 right-1.5 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/32 dark:text-white/30">
            Top 35%
          </span>
        </div>

        <div className="absolute inset-x-1.5 inset-y-4 sm:inset-x-3 sm:inset-y-4 lg:inset-x-4">
          {visiblePoints.map((blip, selectionIndex) => {
            const isTop = (blip.percentile ?? 100) <= TOP_ZONE_PERCENT;
            const dotSize = isCompactViewport
              ? (isTop ? 15 : 13)
              : (isTop ? 16.5 : 14);
            const targetLeft = `${blip.x}%`;
            const targetBottom = `${blip.y}%`;
            const shouldPulse = selectionIndex < Math.min(3, visiblePoints.length);

            return (
              <motion.button
                key={blip.id}
                initial={{ opacity: 0, scale: 0.72, left: targetLeft, bottom: targetBottom }}
                animate={{
                  left: targetLeft,
                  bottom: targetBottom,
                  opacity: isTop ? 1 : 0.92,
                  scale: isTop ? 1.08 : 1,
                  filter: 'brightness(1)',
                }}
                transition={{
                  duration: 0.62,
                  ease: [0.22, 1, 0.36, 1],
                  delay: selectionIndex * 0.018,
                }}
                onClick={() => setHoveredPoint(blip)}
                onMouseEnter={() => setHoveredPoint(blip)}
                onMouseLeave={() => setHoveredPoint(null)}
                className="absolute -translate-x-1/2 -translate-y-1/2 transition-transform duration-500 hover:scale-125 hover:z-50 focus:outline-none"
                style={{
                  zIndex: isTop ? 30 : 20,
                }}
              >
                <motion.div
                  animate={{
                    height: dotSize,
                    width: dotSize,
                    opacity: isTop ? 1 : 0.92,
                  }}
                  transition={{ duration: 0.62, ease: [0.22, 1, 0.36, 1] }}
                  className={[
                    'rounded-full',
                    isTop
                      ? 'bg-[#FB7185] shadow-[0_0_0_1px_rgba(255,255,255,0.2),0_0_18px_rgba(225,29,72,0.22)] dark:bg-[#E11D48] dark:shadow-[0_0_0_1px_rgba(255,255,255,0.22),0_0_22px_rgba(225,29,72,0.45)]'
                      : 'bg-[#FB7185]/88 shadow-[0_0_0_1px_rgba(255,255,255,0.16),0_0_14px_rgba(225,29,72,0.14)] dark:bg-[#E11D48]/88 dark:shadow-[0_0_0_1px_rgba(255,255,255,0.16),0_0_16px_rgba(225,29,72,0.3)]'
                  ].join(' ')}
                />
                {shouldPulse && (
                  <motion.div
                    className="absolute inset-[-7px] rounded-full border border-[#FB7185]/34 dark:border-[#E11D48]/42"
                    animate={{ scale: [1, 1.4, 1], opacity: [0, 0.28, 0] }}
                    transition={{ duration: 3.6, repeat: Infinity, ease: 'easeInOut', delay: selectionIndex * 0.08 }}
                  />
                )}
              </motion.button>
            );
          })}
        </div>

        <div className="pointer-events-none absolute bottom-1.5 left-3 right-3 flex items-center justify-between text-[8px] font-black uppercase tracking-[0.14em] text-foreground/30 sm:bottom-2 sm:left-4 sm:right-4">
          <span>Older</span>
          <span>Latest</span>
        </div>
        
        {/* Tooltip */}
        <AnimatePresence>
          {activeHoveredPoint && (
            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="pointer-events-none absolute left-1/2 top-3 z-50 min-w-[168px] -translate-x-1/2 rounded-[11px] border border-black/10 bg-white/90 px-3.5 py-2.5 shadow-[0_10px_26px_rgba(0,0,0,0.12)] backdrop-blur-xl dark:border-white/10 dark:bg-[#0A0A0A]/93 dark:shadow-[0_12px_30px_rgba(0,0,0,0.72)]"
            >
              <div className="mb-2 flex items-center justify-between">
                <span className="text-[10px] font-black uppercase tracking-[0.1em] text-black/50 dark:text-white/40">{activeHoveredPoint.date}</span>
                <span className="text-[12px] font-black text-black/62 dark:text-white/72">{activeHoveredPoint.percentile === null ? '--' : `${Math.round(activeHoveredPoint.percentile)}%`}</span>
              </div>
              <div className="text-[16px] font-black tracking-[-0.02em] text-black dark:text-white">@{activeHoveredPoint.handle}</div>
              <div className="mt-1 text-[11px] font-bold text-black/60 dark:text-white/60">{(activeHoveredPoint.views / 1000).toFixed(1)}k Views</div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </motion.div>
  );
}

const MemoizedFeedScatterField = memo(FeedScatterField);
MemoizedFeedScatterField.displayName = 'FeedScatterField';

export default MemoizedFeedScatterField;
