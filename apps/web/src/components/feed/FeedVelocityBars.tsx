'use client';

import { useEffect, useState } from 'react';
import { motion, useMotionValue, useSpring } from 'framer-motion';
import { DashboardSummary } from './dashboardTypes';

type MetricRow = { label: string; value: number | null };
const SCALE_MAX = 3.5;

function percentileToRatio(value: number | null): number | null {
  if (value === null) return null;
  return Math.max(0.4, Math.min(SCALE_MAX, (101 - value) / 35));
}

function AnimatedMultiplier({ value }: { value: number | null }) {
  const mv = useMotionValue(0);
  const spring = useSpring(mv, { stiffness: 320, damping: 28, mass: 0.9 });
  const [renderValue, setRenderValue] = useState(value ?? 0);

  useEffect(() => {
    const unsub = spring.on('change', (v) => setRenderValue(v));
    return unsub;
  }, [spring]);

  useEffect(() => {
    mv.set(value ?? 0);
  }, [mv, value]);

  if (value === null) return <>--</>;
  return <>{renderValue.toFixed(1)}x</>;
}

export default function FeedVelocityBars({ summary }: { summary: DashboardSummary | null }) {
  const metrics: MetricRow[] = [
    { label: 'Views', value: percentileToRatio(summary?.avg_views_percentile ?? null) },
    { label: 'Likes', value: percentileToRatio(summary?.avg_likes_percentile ?? null) },
    { label: 'Comments', value: percentileToRatio(summary?.avg_comments_percentile ?? null) },
  ];
  const hasData = metrics.some((metric) => metric.value !== null);
  const postsWithMetrics = Math.max(0, Number(summary?.posts_with_metrics) || 0);

  return (
    <div className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">
      <div className="relative z-10 flex flex-1 flex-col">
        <div className="mb-2 flex items-center justify-between">
          <span className="fm-label fm-depth-title">Performance</span>
          <span className="fm-depth-chip rounded-full px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/40 dark:text-white/40">
            {hasData ? `${postsWithMetrics} posts` : 'Awaiting data'}
          </span>
        </div>

        <div className="grid flex-1 grid-cols-3 gap-2 sm:gap-2.5">
          {metrics.map((metric) => {
            const fillPct =
              metric.value == null ? 0 : Math.max(16, Math.min(100, (metric.value / SCALE_MAX) * 100));
            const isAbove = metric.value != null && metric.value >= 1;

            return (
              <div key={metric.label} className="flex min-h-[150px] sm:min-h-[175px] flex-col">
                {/* Bar track — relative container with explicit flex-1 */}
                <div className="relative flex-1 overflow-hidden rounded-[18px] border border-black/6 bg-[linear-gradient(180deg,rgba(255,255,255,0.82),rgba(235,235,235,0.94))] shadow-[inset_0_1px_0_rgba(255,255,255,0.96),0_5px_14px_rgba(0,0,0,0.05)] dark:border-white/6 dark:bg-[linear-gradient(180deg,rgba(28,28,28,0.98),rgba(10,10,10,0.98))] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),0_4px_12px_rgba(0,0,0,0.2)]">
                  {/* Baseline mark */}
                  <div className="absolute inset-x-0 z-20 border-t border-dashed border-black/12 dark:border-white/12"
                    style={{ bottom: `${(1 / SCALE_MAX) * 100}%` }} />

                  {/* Fill bar — anchored to bottom with absolute positioning */}
                  <motion.div
                    className={`absolute inset-x-0 bottom-0 rounded-t-[12px] ${
                      isAbove
                        ? 'bg-[#E11D48] shadow-[0_-6px_24px_rgba(225,29,72,0.3)] dark:bg-[#E11D48] dark:shadow-[0_-4px_24px_rgba(225,29,72,0.25)]'
                        : 'bg-black/10 dark:bg-white/10'
                    }`}
                    initial={{ height: 0 }}
                    animate={{ height: `${fillPct}%` }}
                    transition={{ duration: 0.8, ease: [0.25, 0.1, 0.25, 1] as const, delay: 0.2 }}
                  >
                    {/* Top shine */}
                    <div className={`h-[3px] w-full rounded-t-[12px] ${isAbove ? 'bg-white/58 dark:bg-white/20' : 'bg-white/20 dark:bg-white/5'}`} />
                  </motion.div>
                </div>

                {/* Label */}
                <div className="mt-2 text-center">
                  <div className={`text-[clamp(18px,3.5vw,24px)] font-black leading-none tracking-[-0.03em] fm-depth-title ${
                    isAbove ? 'text-foreground dark:text-white' : 'text-foreground/50 dark:text-white/40'
                  }`}>
                    <AnimatedMultiplier value={metric.value} />
                  </div>
                  <div className="mt-0.5 text-[9px] font-black uppercase tracking-[0.16em] text-foreground/40 dark:text-white/36 fm-depth-title">
                    {metric.label}
                  </div>
                </div>
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
