'use client';

import { useMemo, useState } from 'react';
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

export default function FeedScatterField({ points }: { points: ScatterPoint[] }) {
  const [activeCount, setActiveCount] = useState<PostCount>(15);
  const [hoveredPoint, setHoveredPoint] = useState<Blip | null>(null);

  const allPoints = useMemo<Blip[]>(() => {
    const src = Array.isArray(points) ? points : [];
    // Sort by days_ago ascending (most recent first)
    const sorted = [...src].sort((a, b) => (Number(a.days_ago) || 0) - (Number(b.days_ago) || 0));
    const maxDays = Math.max(1, ...sorted.map((p) => Number(p.days_ago) || 0));
    return sorted.map((p, index) => {
      const percentile = typeof p.percentile_performance === 'number' ? p.percentile_performance : null;
      return {
        id: p.post_key || `${p.handle}-${p.days_ago}`,
        x: Math.min(100, ((Number(p.days_ago) || 0) / maxDays) * 100),
        y: percentile === null ? 50 : Math.max(2, Math.min(98, 100 - percentile)),
        isCompetitor: false,
        percentile,
        handle: p.handle || 'unknown',
        date: formatPointDate(p.posted_at_ist),
        views: Math.max(0, Number(p.views) || 0),
        daysAgo: Number(p.days_ago) || 0,
        postIndex: index,
      };
    });
  }, [points]);

  // Show all points but only twinkle the most recent N
  const twinkleSet = useMemo(() => {
    const ids = new Set<string>();
    for (let i = 0; i < Math.min(activeCount, allPoints.length); i++) {
      ids.add(allPoints[i].id);
    }
    return ids;
  }, [activeCount, allPoints]);

  return (
    <motion.div
      className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3.5 sm:p-4 lg:p-5"
    >
      <div className="relative z-10 mb-4 flex items-start justify-between gap-2">
        <span className="fm-label fm-depth-title">The Field</span>
        {/* Post count pills */}
        <div className="hide-scrollbar flex shrink-0 items-center gap-1 rounded-full border border-black/5 bg-black/[0.03] p-0.5 dark:border-white/5 dark:bg-white/[0.03]">
          {POST_COUNTS.map(count => (
            <motion.button
              key={count}
              onClick={() => setActiveCount(count)}
              whileTap={{ scale: 0.95 }}
              className={`relative rounded-full px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.1em] ${activeCount === count ? 'text-black z-10' : 'text-foreground/42 z-0'}`}
            >
              {activeCount === count && (
                <motion.span
                  layoutId="scatter-pill-bg"
                  className="absolute inset-0 rounded-full bg-[#CCFF00] shadow-[0_2px_8px_rgba(204,255,0,0.2)]"
                  transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }}
                />
              )}
              <span className="relative z-10">Last {count}</span>
            </motion.button>
          ))}
        </div>
      </div>

      {/* Chart area */}
      <div className="relative min-h-[200px] flex-1 w-full rounded-[14px] fm-depth-inner">
        {[10, 25, 50].map((t) => (
          <div key={t} className="absolute left-0 right-0 border-t border-dashed border-black/10 dark:border-white/10" style={{ top: `${t}%` }}>
            <span className="absolute -top-3.5 right-1 text-[8px] font-black tracking-[0.1em] text-foreground/30">{t}%</span>
          </div>
        ))}

        <div className="absolute inset-4">
          <AnimatePresence>
            {allPoints.map((blip) => {
              const isInSelection = twinkleSet.has(blip.id);
              const isTop = (blip.percentile ?? 100) <= 25;

              return (
                <motion.button
                  key={blip.id}
                  initial={{ opacity: 0, scale: 0 }}
                  animate={{ opacity: 1, scale: 1 }}
                  transition={{ duration: 0.4, delay: blip.postIndex * 0.008 }}
                  onClick={() => setHoveredPoint(blip)}
                  onMouseEnter={() => setHoveredPoint(blip)}
                  onMouseLeave={() => setHoveredPoint(null)}
                  className="absolute -translate-x-1/2 -translate-y-1/2 transition-transform hover:scale-150 hover:z-50 focus:outline-none"
                  style={{
                    left: `${100 - blip.x}%`,
                    bottom: `${blip.y}%`,
                    zIndex: isTop ? 40 : 10,
                  }}
                >
                  <div
                    className={`rounded-full ${
                      isTop
                        ? 'bg-[#CCFF00] h-[10px] w-[10px] md:h-[12px] md:w-[12px]'
                        : 'bg-black/30 dark:bg-white/45 h-[6px] w-[6px] md:h-[8px] md:w-[8px]'
                    }`}
                    style={{ boxShadow: isTop ? '0 0 12px rgba(204,255,0,0.6)' : 'none' }}
                  />
                  {/* Twinkle animation on selected recent posts */}
                  {isInSelection && isTop && (
                    <motion.div
                      className="absolute inset-[-3px] rounded-full border border-[#CCFF00]/40"
                      animate={{ scale: [1, 1.8, 1], opacity: [0.6, 0, 0.6] }}
                      transition={{ duration: 2.5, repeat: Infinity, ease: 'easeInOut', delay: blip.postIndex * 0.15 }}
                    />
                  )}
                </motion.button>
              );
            })}
          </AnimatePresence>
        </div>
        
        {/* Tooltip */}
        <AnimatePresence>
          {hoveredPoint && (
            <motion.div
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="pointer-events-none absolute left-1/2 top-4 z-50 -translate-x-1/2 rounded-[12px] border border-black/10 bg-white/90 px-4 py-3 shadow-[0_12px_32px_rgba(0,0,0,0.15)] backdrop-blur-xl dark:border-white/10 dark:bg-[#0A0A0A]/95 dark:shadow-[0_12px_32px_rgba(0,0,0,0.8)] min-w-[180px]"
            >
              <div className="flex items-center justify-between mb-2">
                <span className="text-[10px] font-black uppercase tracking-[0.1em] text-black/50 dark:text-white/40">{hoveredPoint.date}</span>
                <span className="text-[12px] font-black text-[#CCFF00]">{hoveredPoint.percentile === null ? '--' : `${Math.round(hoveredPoint.percentile)}%`}</span>
              </div>
              <div className="text-[16px] font-black tracking-[-0.02em] text-black dark:text-white">@{hoveredPoint.handle}</div>
              <div className="mt-1 text-[12px] font-bold text-black/60 dark:text-white/60">{(hoveredPoint.views / 1000).toFixed(1)}k Views</div>
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </motion.div>
  );
}
