'use client';

import { useState, useMemo } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { ApexMixPoint } from './dashboardTypes';

interface Segment {
  id: string;
  label: string;
  percentage: number;
  count: number;
}

export default function FeedApexArch({ mix }: { mix: ApexMixPoint[] }) {
  const [activeSegment, setActiveSegment] = useState<string | null>(null);

  const segments: Segment[] = useMemo(() => {
    const normalized = Array.isArray(mix) ? mix.slice(0, 4) : [];
    const fallback: Segment[] = [
      { id: 'reels', label: 'Reels', percentage: 58, count: 58 },
      { id: 'carousel', label: 'Carousel', percentage: 27, count: 27 },
      { id: 'image', label: 'Image', percentage: 15, count: 15 },
    ];
    if (normalized.length === 0) return fallback;
    return normalized.map((item, idx) => ({
      id: `${item.media_type}-${idx}`,
      label: item.media_type || 'Unknown',
      percentage: Math.max(1, Math.round((item.share > 0 ? item.share : 0) * 100)),
      count: Math.max(0, Number(item.post_count) || 0),
    }));
  }, [mix]);

  const total = segments.reduce((sum, s) => sum + s.percentage, 0) || 1;
  const totalPostCount = segments.reduce((sum, s) => sum + s.count, 0);
  const normalizedSegments = segments
    .map((s) => ({ ...s, percentage: Math.round((s.percentage / total) * 100) }))
    .sort((a, b) => b.percentage - a.percentage);

  // Donut ring geometry
  const size = 160;
  const center = size / 2;
  const radius = 56;
  const strokeWidth = 14;
  const gapDeg = 3;

  const arcs = useMemo(() => {
    return normalizedSegments.reduce<Array<Segment & { idx: number; d: string; isTop: boolean }>>((acc, seg, idx) => {
      const cursor = acc.reduce((sum, item) => sum + item.percentage, 0) * 3.6;
      const sweep = (seg.percentage / 100) * 360;
      const startAngle = cursor + gapDeg / 2 - 90;
      const endAngle = cursor + sweep - gapDeg / 2 - 90;

      const startRad = (startAngle * Math.PI) / 180;
      const endRad = (endAngle * Math.PI) / 180;
      const x1 = center + radius * Math.cos(startRad);
      const y1 = center + radius * Math.sin(startRad);
      const x2 = center + radius * Math.cos(endRad);
      const y2 = center + radius * Math.sin(endRad);
      const largeArc = sweep - gapDeg > 180 ? 1 : 0;

      acc.push({
        ...seg,
        idx,
        d: `M ${x1} ${y1} A ${radius} ${radius} 0 ${largeArc} 1 ${x2} ${y2}`,
        isTop: idx === 0,
      });
      return acc;
    }, []);
  }, [center, gapDeg, normalizedSegments, radius]);

  return (
    <div className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-4">
      <div className="relative z-10 flex h-full flex-col">
        {/* Header */}
        <div className="flex items-center justify-between mb-3">
          <span className="text-[10px] sm:text-[11px] font-black uppercase tracking-[0.14em] text-black/60 dark:text-white/45 fm-depth-title">
            Posting Pattern
          </span>
        </div>

        {/* Ring + legend side by side */}
        <div className="flex flex-1 items-center gap-4 sm:gap-6">
          {/* Donut ring */}
          <div className="relative shrink-0" style={{ width: size, height: size }}>
            <svg
              width={size}
              height={size}
              viewBox={`0 0 ${size} ${size}`}
              className="overflow-visible"
            >
              {/* Background track */}
              <circle
                cx={center}
                cy={center}
                r={radius}
                fill="none"
                stroke="currentColor"
                strokeWidth={strokeWidth}
                className="text-black/[0.04] dark:text-white/[0.06]"
              />

              {/* Segments */}
              {arcs.map((arc) => {
                const isActive = activeSegment === arc.id;
                const isHoveringAny = activeSegment !== null;
                const opacity = isHoveringAny && !isActive ? 0.3 : 1;

                return (
                  <motion.path
                    key={arc.id}
                    d={arc.d}
                    fill="none"
                    stroke={arc.isTop ? '#CCFF00' : 'currentColor'}
                    strokeWidth={isActive ? strokeWidth + 4 : strokeWidth}
                    strokeLinecap="round"
                    initial={{ pathLength: 0, opacity: 0 }}
                    animate={{ pathLength: 1, opacity }}
                    transition={{ duration: 0.8, ease: 'easeOut', opacity: { duration: 0.2 } }}
                    className={
                      arc.isTop
                        ? 'cursor-pointer drop-shadow-[0_0_6px_rgba(204,255,0,0.2)]'
                        : 'cursor-pointer text-black/20 dark:text-white/20'
                    }
                    onMouseEnter={() => setActiveSegment(arc.id)}
                    onMouseLeave={() => setActiveSegment(null)}
                    onClick={() => setActiveSegment(activeSegment === arc.id ? null : arc.id)}
                  />
                );
              })}
            </svg>

            {/* Center content */}
            <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
              <AnimatePresence mode="wait">
                {activeSegment ? (
                  <motion.div
                    key="active"
                    initial={{ opacity: 0, scale: 0.9 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0, scale: 0.9 }}
                    transition={{ duration: 0.15 }}
                    className="flex flex-col items-center"
                  >
                    <span className="text-[8px] font-black uppercase tracking-[0.16em] text-black/40 dark:text-white/35">
                      {normalizedSegments.find((s) => s.id === activeSegment)?.label}
                    </span>
                    <span className="text-[28px] font-black leading-none tracking-tighter text-black dark:text-white mt-0.5">
                      {normalizedSegments.find((s) => s.id === activeSegment)?.count ?? 0}
                    </span>
                    <span className="mt-1 text-[8px] font-black uppercase tracking-[0.14em] text-black/35 dark:text-white/30">
                      {normalizedSegments.find((s) => s.id === activeSegment)?.percentage}% share
                    </span>
                  </motion.div>
                ) : (
                  <motion.div
                    key="default"
                    initial={{ opacity: 0, scale: 0.9 }}
                    animate={{ opacity: 1, scale: 1 }}
                    exit={{ opacity: 0, scale: 0.9 }}
                    transition={{ duration: 0.15 }}
                    className="flex flex-col items-center"
                  >
                    <span className="text-[8px] font-black uppercase tracking-[0.16em] text-black/40 dark:text-white/35">
                      Posts Tracked
                    </span>
                    <span className="text-[32px] font-black leading-none tracking-tighter text-black dark:text-white mt-0.5">
                      {totalPostCount}
                    </span>
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </div>

          {/* Legend */}
          <div className="flex flex-1 flex-col gap-2.5">
            {normalizedSegments.map((seg, idx) => {
              const isTop = idx === 0;
              const isActive = activeSegment === seg.id;

              return (
                <motion.div
                  key={seg.id}
                  className="flex items-center justify-between cursor-pointer group"
                  onMouseEnter={() => setActiveSegment(seg.id)}
                  onMouseLeave={() => setActiveSegment(null)}
                  animate={{ opacity: activeSegment && !isActive ? 0.4 : 1 }}
                  transition={{ duration: 0.15 }}
                >
                  <div className="flex items-center gap-2">
                    <div
                      className={`h-2.5 w-2.5 rounded-full shrink-0 ${
                        isTop
                          ? 'bg-[#CCFF00] shadow-[0_0_6px_rgba(204,255,0,0.3)]'
                          : 'bg-black/15 dark:bg-white/15'
                      }`}
                    />
                    <span className="text-[10px] sm:text-[11px] font-black uppercase tracking-[0.08em] text-black/60 dark:text-white/50">
                      {seg.label}
                    </span>
                  </div>
                  <span
                    className={`text-[13px] sm:text-[14px] font-black tabular-nums tracking-tight ${
                      isTop
                        ? 'text-black dark:text-[#CCFF00]'
                        : 'text-black/40 dark:text-white/35'
                    }`}
                  >
                    {seg.percentage}%
                  </span>
                </motion.div>
              );
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
