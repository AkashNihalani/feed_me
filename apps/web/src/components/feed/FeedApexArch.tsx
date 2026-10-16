'use client';

import { useState, useMemo } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { beatArrive } from '@/lib/motion';
import { ApexMixPoint } from './dashboardTypes';

// Each arc settles in on the page beat, one step after the arc before it: it fades up while turning the last few
// degrees along the ring (the old draw direction). Opacity + a transform string on an HTML layer run on the
// compositor; the old pathLength draw re-painted the SVG on the main thread every frame.
const ARC_HIDDEN = { opacity: 0, transform: 'rotate(-12deg)' } as const;
const ARC_SHOWN = { opacity: 1, transform: 'rotate(0deg)' } as const;
const ARC_DIM = { duration: 0.2, ease: 'easeOut' } as const;
const CENTER_HIDDEN = { opacity: 0, transform: 'scale(0.9)' } as const;
const CENTER_SHOWN = { opacity: 1, transform: 'scale(1)' } as const;
const CENTER_SWAP = { duration: 0.15 } as const;

interface Segment {
  id: string;
  label: string;
  percentage: number;
  count: number;
}

function formatMediaLabel(value: string | null | undefined): string {
  const normalized = (value || '').trim().toLowerCase();
  if (normalized === 'sidecar' || normalized === 'sidcar' || normalized === 'sidecar/carousel') return 'Carousel';
  if (normalized === 'reel' || normalized === 'video') return 'Reel';
  if (normalized === 'photo') return 'Image';
  if (!normalized) return 'Unknown';
  return normalized.replace(/\b\w/g, (letter) => letter.toUpperCase());
}

export default function FeedApexArch({ mix }: { mix: ApexMixPoint[] }) {
  const [activeSegment, setActiveSegment] = useState<string | null>(null);

  const segments: Segment[] = useMemo(() => {
    const normalized = Array.isArray(mix) ? mix.slice(0, 4) : [];
    if (normalized.length === 0) return [];
    return normalized.map((item, idx) => ({
      id: `${item.media_type}-${idx}`,
      label: formatMediaLabel(item.media_type),
      percentage: Math.max(0, Math.round((item.share > 0 ? item.share : 0) * 100)),
      count: Math.max(0, Number(item.post_count) || 0),
    }));
  }, [mix]);

  const total = segments.reduce((sum, s) => sum + s.percentage, 0);
  const totalPostCount = segments.reduce((sum, s) => sum + s.count, 0);
  const normalizedSegments = useMemo(
    () => (total > 0 && totalPostCount > 0
      ? segments
        .map((s) => ({ ...s, percentage: Math.round((s.percentage / total) * 100) }))
        .sort((a, b) => b.percentage - a.percentage)
      : []),
    [segments, total, totalPostCount]
  );
  const hasData = normalizedSegments.length > 0;
  const activeSeg = activeSegment ? normalizedSegments.find((s) => s.id === activeSegment) : undefined;

  // Donut ring geometry
  const size = 180;
  const center = size / 2;
  const radius = 66;
  const strokeWidth = 16;
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
    <div className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">
      <div className="relative z-10 flex h-full flex-col">
        {/* Header */}
        <div className="flex items-center justify-between mb-2">
          <span className="fm-label fm-depth-title">
            Media Mix
          </span>
        </div>

        {/* Ring + legend */}
        <div className="flex flex-1 flex-col justify-between gap-3 sm:flex-row sm:items-center sm:gap-5">
          {/* Donut ring */}
          <div className="relative mx-auto h-[170px] w-[170px] shrink-0 sm:mx-0 sm:h-[164px] sm:w-[164px]" style={{ maxWidth: size + 30, maxHeight: size + 30 }}>
            <svg
              viewBox={`0 0 ${size} ${size}`}
              className="h-full w-full overflow-visible"
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
            </svg>

            {/* Segments: one HTML layer per arc, same box + viewBox as the track and stacked in the old paint order
                (later arcs over earlier caps). The outer layer carries the entrance, the inner one the hover dim, so
                both animate opacity/transform on the compositor. Only the painted stroke takes the pointer. */}
            {arcs.map((arc) => {
              const isActive = activeSegment === arc.id;
              const isHoveringAny = activeSegment !== null;
              const opacity = isHoveringAny && !isActive ? 0.3 : 1;

              return (
                <motion.div
                  key={arc.id}
                  className="pointer-events-none absolute inset-0"
                  initial={ARC_HIDDEN}
                  animate={ARC_SHOWN}
                  transition={beatArrive(arc.idx)}
                >
                  <motion.div
                    className="absolute inset-0"
                    initial={false}
                    animate={{ opacity }}
                    transition={ARC_DIM}
                  >
                    <svg
                      viewBox={`0 0 ${size} ${size}`}
                      className="h-full w-full overflow-visible"
                    >
                      <path
                        d={arc.d}
                        fill="none"
                        stroke={arc.isTop ? '#E11D48' : 'currentColor'}
                        strokeWidth={isActive ? strokeWidth + 4 : strokeWidth}
                        strokeLinecap="round"
                        className={
                          arc.isTop
                            ? 'pointer-events-auto cursor-pointer'
                            : 'pointer-events-auto cursor-pointer text-black/20 dark:text-white/20'
                        }
                        onMouseEnter={() => setActiveSegment(arc.id)}
                        onMouseLeave={() => setActiveSegment(null)}
                        onClick={() => setActiveSegment(activeSegment === arc.id ? null : arc.id)}
                      />
                    </svg>
                  </motion.div>
                </motion.div>
              );
            })}

            {/* Center content */}
            <div className="absolute inset-0 flex flex-col items-center justify-center pointer-events-none">
              <AnimatePresence mode="wait">
                {activeSegment ? (
                  <motion.div
                    key="active"
                    initial={CENTER_HIDDEN}
                    animate={CENTER_SHOWN}
                    exit={CENTER_HIDDEN}
                    transition={CENTER_SWAP}
                    className="flex flex-col items-center"
                  >
                    <span className="max-w-[96px] text-center text-[8px] font-black uppercase leading-none tracking-[0.14em] text-black/40 dark:text-white/35">
                      {activeSeg?.label}
                    </span>
                    <span className="mt-0.5 text-[28px] font-black leading-none tracking-tighter text-black dark:text-white">
                      {activeSeg?.count ?? 0}
                    </span>
                    <span className="mt-1 max-w-[96px] text-center text-[8px] font-black uppercase leading-none tracking-[0.14em] text-black/35 dark:text-white/30">
                      {activeSeg?.percentage}% share
                    </span>
                  </motion.div>
                ) : (
                  <motion.div
                    key="default"
                    initial={CENTER_HIDDEN}
                    animate={CENTER_SHOWN}
                    exit={CENTER_HIDDEN}
                    transition={CENTER_SWAP}
                    className="flex flex-col items-center"
                  >
                    <span className="max-w-[96px] text-center text-[8px] font-black uppercase leading-none tracking-[0.14em] text-black/40 dark:text-white/35">
                      {hasData ? 'Posts' : 'Awaiting Posts'}
                    </span>
                    <span className="text-[34px] font-black leading-none tracking-tighter text-black dark:text-white mt-0.5">
                      {totalPostCount}
                    </span>
                    {!hasData ? (
                      <span className="mt-1 text-[8px] font-black uppercase tracking-[0.14em] text-black/35 dark:text-white/30">
                        mix appears after posts land
                      </span>
                    ) : null}
                  </motion.div>
                )}
              </AnimatePresence>
            </div>
          </div>

          {/* Legend */}
          <div className="grid flex-1 grid-cols-3 gap-3 sm:flex sm:flex-col sm:gap-2.5">
            {hasData ? normalizedSegments.map((seg, idx) => {
              const isTop = idx === 0;
              const isActive = activeSegment === seg.id;

              return (
                <motion.div
                  key={seg.id}
                  className="group flex cursor-pointer flex-col items-center gap-2 rounded-[14px] border border-black/6 bg-black/[0.025] px-2.5 py-3 text-center sm:flex-row sm:items-center sm:justify-between sm:gap-0 sm:border-0 sm:bg-transparent sm:px-0 sm:py-0 sm:text-left"
                  onMouseEnter={() => setActiveSegment(seg.id)}
                  onMouseLeave={() => setActiveSegment(null)}
                  animate={{ opacity: activeSegment && !isActive ? 0.4 : 1 }}
                  transition={{ duration: 0.15 }}
                >
                  <div className="flex items-center gap-2">
                    <div
                      className={`h-2.5 w-2.5 shrink-0 rounded-full ${
                        isTop
                          ? 'bg-[var(--fm-accent)] shadow-[0_0_6px_rgb(var(--fm-accent-rgb)/0.32)]'
                          : 'bg-black/15 dark:bg-white/15'
                      }`}
                    />
                    <span className="text-[10px] sm:text-[12px] font-black uppercase tracking-[0.14em] text-black/60 dark:text-white/50">
                      {seg.label}
                    </span>
                  </div>
                  <span
                    className={`text-[14px] sm:text-[14px] font-black tabular-nums tracking-tight ${
                      isTop
                        ? 'text-foreground dark:text-white'
                        : 'text-black/40 dark:text-white/35'
                    }`}
                  >
                    {seg.percentage}%
                  </span>
                </motion.div>
              );
            }) : (
              <div className="rounded-[18px] border border-black/6 bg-black/[0.025] px-3 py-3 text-[10px] font-black uppercase tracking-[0.14em] text-black/42 dark:border-white/8 dark:bg-white/[0.03] dark:text-white/38">
                No media mix yet. Posts in this window will populate this panel.
              </div>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
