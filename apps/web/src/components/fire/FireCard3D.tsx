'use client';

import { useEffect, useMemo, useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { FireItem } from './types';
import { compact } from '@/components/fire/fireLogicHelpers';

export type FireCard3DProps = {
  item: FireItem;
  forcedOpen?: boolean;
  highlighted?: boolean;
  layoutMode?: 'mobile' | 'desktop';
  onOpenDetails?: () => void;
};

function asRec(v: unknown): Record<string, unknown> {
  return v && typeof v === 'object' && !Array.isArray(v) ? (v as Record<string, unknown>) : {};
}

function num(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') {
    const n = Number(v.replace('%', '').trim());
    if (Number.isFinite(n)) return n;
  }
  return null;
}

function text(v: unknown): string {
  return typeof v === 'string' ? v : '';
}

function GlassTile({ label, value, className = '' }: { label: string; value: string; className?: string }) {
  return (
    <div
      className={[
        'rounded-[11px] border border-white/55 bg-white/45 p-2 sm:p-2.5',
        'shadow-[0_12px_28px_rgba(0,0,0,0.22),inset_0_1px_0_rgba(255,255,255,0.7),inset_0_-8px_16px_rgba(255,255,255,0.12)]',
        'dark:border-white/22 dark:bg-black/45 dark:shadow-[0_14px_30px_rgba(0,0,0,0.6),inset_0_1px_0_rgba(255,255,255,0.16)]',
        className,
      ].join(' ')}
    >
      <div className="text-[7px] sm:text-[8px] font-black uppercase tracking-[0.14em] text-foreground/70">{label}</div>
      <div className="mt-1 text-[18px] sm:text-[24px] font-black leading-[0.9] tracking-[-0.02em] text-foreground/95">{value}</div>
    </div>
  );
}

function MetricChip({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[10px] border border-white/48 bg-white/34 px-2 py-1.5 sm:px-2.5 sm:py-2 shadow-[0_12px_24px_rgba(0,0,0,0.2),inset_0_1px_0_rgba(255,255,255,0.66),inset_0_-6px_14px_rgba(255,255,255,0.08)] dark:border-white/16 dark:bg-black/38 dark:shadow-[0_14px_28px_rgba(0,0,0,0.58),inset_0_1px_0_rgba(255,255,255,0.1)]">
      <div className="text-[6px] sm:text-[7px] font-black uppercase tracking-[0.16em] text-foreground/62">{label}</div>
      <div className="mt-1 text-[18px] sm:text-[24px] font-black leading-none tracking-[-0.03em] text-foreground/94">{value}</div>
    </div>
  );
}

export function FireCard3D({
  item,
  forcedOpen = false,
  highlighted = false,
  layoutMode = 'mobile',
  onOpenDetails,
}: FireCard3DProps) {
  const [openLocal, setOpenLocal] = useState(false);
  const [isPrimed, setIsPrimed] = useState(false);
  const isDesktopCard = layoutMode === 'desktop';
  const isOpen = forcedOpen || openLocal;

  useEffect(() => {
    if (!highlighted && !forcedOpen) {
      setOpenLocal(false);
      setIsPrimed(false);
    }
  }, [forcedOpen, highlighted]);

  const payload = asRec(item.payload);
  const metrics = asRec(payload.metrics);
  const position = asRec(payload.position);
  const timing = asRec(payload.timing);
  const trajectory = asRec(payload.trajectory);

  const bestMetric = (text(payload.best_metric) || item.metricKey || 'views').toUpperCase();
  const bestMetricObj = asRec(metrics[bestMetric.toLowerCase()]);

  const value = num(bestMetricObj.value) ?? item.metricValue;
  const baseline = num(bestMetricObj.baseline);
  const multiple = num(bestMetricObj.multiple);
  const supportMetrics = ['views', 'likes', 'comments']
    .filter((metric) => metric !== bestMetric.toLowerCase())
    .map((metric) => {
      const metricObj = asRec(metrics[metric]);
      const metricMultiple = num(metricObj.multiple);
      return {
        key: metric,
        label: metric === 'views' ? 'View' : metric === 'likes' ? 'Like' : 'Comment',
        value: metricMultiple == null ? 'x--' : `${metricMultiple.toFixed(2)}x`,
      };
    });

  const feedRank = num(bestMetricObj.rank_feed) ?? num(position.feed_rank);
  const feederRank = num(position.feeder_rank) ?? num(position.rank_overall) ?? num(position.rank_all_time);
  const bestInLastN = num(bestMetricObj.best_in_last_n);

  const hour = num(timing.hour);
  const hourPct = num(timing.hour_percentile);
  const hourMult = num(timing.hour_multiple);

  const d1 = num(trajectory.d1);
  const d3 = num(trajectory.d3);
  const d7 = num(trajectory.d7);
  const rawDelta = num(trajectory.delta) ?? item.trajectoryDeltaPercentile;
  const trajectorySeries = [d1, d3, d7];
  const firstTraj = trajectorySeries.find((v) => v != null) ?? null;
  const lastTraj = [...trajectorySeries].reverse().find((v) => v != null) ?? null;
  const computedDelta =
    firstTraj != null && lastTraj != null && trajectorySeries.filter((v) => v != null).length > 1
      ? lastTraj - firstTraj
      : null;
  const delta = computedDelta ?? rawDelta;

  // Lower percentile is better. Negative delta means improvement.
  const isPositiveShift = delta != null && delta < 0;
  const displayDeltaStr = delta == null
    ? '--'
    : delta === 0
      ? '0'
      : delta < 0
        ? `+${Math.abs(Math.round(delta))}`
        : `-${Math.abs(Math.round(delta))}`;

  const deltaBgClass = isPositiveShift 
    ? 'bg-[#CCFF00] dark:bg-[#CCFF00] shadow-[0_4px_12px_rgba(204,255,0,0.35),inset_0_2px_4px_rgba(255,255,255,0.9),inset_0_-2px_4px_rgba(130,156,0,0.4)] border border-[#CCFF00]/10' 
    : 'bg-white/55 dark:bg-white/14 shadow-[inset_0_1px_0_rgba(255,255,255,0.6)]';
  const deltaLabelClass = isPositiveShift ? 'text-black/60' : 'text-foreground/70';
  const deltaTextClass = isPositiveShift ? 'text-black drop-shadow-sm' : 'text-foreground/95';

  const cp = item.checkpoint.toUpperCase();
  const isD1 = cp === 'D1';

  const stamp = useMemo(() => {
    const handle = `@${(item.surfaceHandle || 'FEEDER').replace(/^@+/, '').toUpperCase()}`;
    const media = (item.surfaceMediaType || 'POST').toUpperCase();
    return `${handle} · ${media} · ${bestMetric} ${compact(value)} · ${cp}`;
  }, [item.surfaceHandle, item.surfaceMediaType, bestMetric, value, cp]);

  const handleCardActivate = () => {
    if (isDesktopCard) {
      onOpenDetails?.();
      return;
    }
    setOpenLocal((v) => !v);
  };

  return (
    <motion.div
      role="button"
      tabIndex={0}
      onClick={handleCardActivate}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          handleCardActivate();
        }
      }}
      className={isDesktopCard
        ? 'relative block w-full aspect-[11/14] overflow-hidden rounded-[24px] text-left fm-depth-glass'
        : 'relative block w-full aspect-[4/5] max-h-[78svh] overflow-hidden rounded-[26px] text-left fm-depth-glass sm:max-h-none sm:rounded-[32px]'}
      style={{
        WebkitTapHighlightColor: 'transparent',
        boxShadow: isDesktopCard
          ? highlighted
            ? '0 24px 46px rgba(0,0,0,0.34)'
            : '0 14px 28px rgba(0,0,0,0.24)'
          : highlighted
            ? '0 18px 38px rgba(0,0,0,0.24)'
            : '0 10px 22px rgba(0,0,0,0.16)',
        willChange: 'transform',
      }}
      whileTap={{ scale: 0.994 }}
      transition={{ duration: 0.08, ease: [0.22, 1, 0.36, 1] }}
    >
      {item.thumbnailUrl ? (
        <motion.img
          src={item.thumbnailUrl}
          alt="cover"
          className="absolute inset-0 h-full w-full object-cover"
          loading="lazy"
          animate={{
            scale: isOpen ? 1.022 : highlighted ? 1.01 : 1,
          }}
          transition={{ duration: 0.16, ease: [0.22, 1, 0.36, 1] }}
          style={{ willChange: 'transform' }}
        />
      ) : (
        <div className="absolute inset-0 bg-black" />
      )}

      <div
        className="absolute inset-0"
        style={{
          background:
            'linear-gradient(180deg, rgba(0,0,0,0.18) 0%, rgba(0,0,0,0.1) 40%, rgba(0,0,0,0.68) 100%), radial-gradient(120% 90% at 18% 8%, rgba(0,0,0,0.3) 0%, rgba(0,0,0,0) 58%)',
        }}
      />

      <motion.div
        className={isDesktopCard ? 'absolute left-4 top-4 z-10' : 'absolute left-4 top-8 z-10 md:top-4'}
        style={{ marginTop: 'var(--pwa-top-pad)' }}
        animate={{ opacity: !isDesktopCard && isOpen ? 0.08 : 1, y: !isDesktopCard && isOpen ? -10 : 0, scale: !isDesktopCard && isOpen ? 0.95 : 1 }}
        transition={{ duration: 0.22, ease: [0.22, 1, 0.36, 1] }}
      >
        <div className={isDesktopCard
          ? 'text-[clamp(50px,4.6vw,82px)] font-black leading-[0.82] tracking-[-0.055em] text-white drop-shadow-[0_10px_18px_rgba(0,0,0,0.58)]'
          : 'text-[clamp(108px,30vw,210px)] font-black leading-[0.76] tracking-[-0.055em] text-white drop-shadow-[0_10px_18px_rgba(0,0,0,0.58)]'}>
          {item.surfacePercentile == null ? '--' : Math.round(item.surfacePercentile)}
          <span className="ml-1 align-top text-[0.42em]">%</span>
        </div>
      </motion.div>

      <motion.div
        className={isDesktopCard
          ? 'absolute bottom-3.5 left-1/2 z-10 -translate-x-1/2 whitespace-nowrap rounded-[12px] border border-white/24 bg-black/42 px-3 py-1.5 text-center text-[8px] font-black uppercase tracking-[0.08em] text-white/90 shadow-[0_12px_24px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.12)] backdrop-blur-[18px]'
          : 'absolute bottom-8 left-1/2 z-10 md:bottom-6 -translate-x-1/2 whitespace-nowrap rounded-[12px] border border-white/38 bg-white/14 px-3 py-1.5 text-center text-[10px] font-black uppercase tracking-[0.1em] text-white/92 shadow-[0_10px_24px_rgba(0,0,0,0.24),inset_0_1px_0_rgba(255,255,255,0.48)] backdrop-blur-[16px]'}
        animate={{ opacity: !isDesktopCard && isOpen ? 0.1 : 1, y: !isDesktopCard && isOpen ? 10 : 0 }}
        transition={{ duration: 0.18, ease: [0.22, 1, 0.36, 1] }}
      >
        {stamp}
      </motion.div>

      <AnimatePresence initial={false}>
        {!isDesktopCard && isOpen && (
          <motion.div
            className="absolute inset-x-2 top-2 z-20"
            initial={{ opacity: 0, y: 10, scale: 0.986 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -6, scale: 0.992 }}
            transition={{ type: 'spring', stiffness: 420, damping: 34, mass: 0.58 }}
            style={{ willChange: 'transform, opacity' }}
          >
            <div className="relative overflow-hidden rounded-[24px] border border-white/80 bg-white/70 p-2 sm:p-3 shadow-[0_32px_80px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.95),inset_0_-16px_32px_rgba(255,255,255,0.1)] backdrop-blur-[48px] backdrop-saturate-[220%] dark:border-white/[0.08] dark:bg-[rgba(10,10,10,0.75)] dark:shadow-[0_40px_100px_rgba(0,0,0,0.8),inset_0_1px_0_rgba(255,255,255,0.1),inset_0_-1px_0_rgba(0,0,0,0.5)]">
              <div className="pointer-events-none absolute inset-0 rounded-[24px] bg-gradient-to-br from-white/90 via-white/40 to-transparent dark:from-white/10 dark:via-white/[0.02] dark:to-transparent" />
              <div className="pointer-events-none absolute inset-[1px] rounded-[23px] z-0 dark:hidden" style={{ boxShadow: 'inset 0 2px 4px rgba(255,255,255,0.7), inset 0 -2px 6px rgba(0,0,0,0.04)' }} />
              
              <div className="relative z-10">
              <div className="mb-2 sm:mb-3 rounded-[16px] bg-[#CCFF00] p-2.5 sm:p-3 shadow-[0_8px_24px_rgba(204,255,0,0.35),inset_0_2px_4px_rgba(255,255,255,0.8),inset_0_-2px_4px_rgba(130,156,0,0.4)] dark:shadow-[0_12px_32px_rgba(204,255,0,0.25),inset_0_2px_4px_rgba(255,255,255,0.8),inset_0_-2px_4px_rgba(130,156,0,0.4)] border border-[#CCFF00]/10">
                <div className="text-[9px] sm:text-[10px] font-black uppercase tracking-[0.16em] text-black/60">Performance</div>
                <div className="mt-0.5 text-[clamp(28px,8.2vw,46px)] font-black leading-[0.88] tracking-[-0.04em] text-black drop-shadow-sm">
                  {compact(value)} {bestMetric}
                </div>
                <div className="mt-1 flex items-end gap-2 text-[clamp(11px,3.3vw,19px)] font-black leading-none">
                  <span className="text-black/60">{compact(baseline)} USUAL</span>
                  <span className="text-black">{multiple == null ? '--' : multiple.toFixed(2)}× MULTIPLE</span>
                </div>
              </div>

              <motion.div
                className="grid grid-cols-12 gap-1 sm:gap-1.5"
                initial={{ opacity: 0.98, y: 4 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.12, ease: [0.22, 1, 0.36, 1] }}
              >
                <div className="col-span-12 sm:col-span-6">
                  <GlassTile
                    label="Best in Last N Post"
                    value={bestInLastN == null ? 'BEST IN -- POSTS' : `BEST IN ${Math.max(1, Math.round(bestInLastN))} POSTS`}
                    className="[&>div:last-child]:text-[20px]"
                  />
                </div>

                <div className="col-span-6 sm:col-span-3">
                  <GlassTile label="Feed Rank" value={feedRank == null ? '--' : `#${Math.round(feedRank)}`} />
                </div>

                <div className="col-span-6 sm:col-span-3">
                  <GlassTile label="Feeder Rank" value={feederRank == null ? '--' : `#${Math.round(feederRank)}`} />
                </div>

                {isD1 ? (
                  <>
                    <div className="col-span-12 sm:col-span-4">
                      <GlassTile label="Posting Hour" value={hour == null ? '--:--' : `${String(Math.round(hour)).padStart(2, '0')}:00`} />
                    </div>
                    <div className="col-span-6 sm:col-span-4">
                      <GlassTile label="Hour Performance" value={hourPct == null ? 'P--' : `P${Math.round(hourPct)}`} />
                    </div>
                    <div className="col-span-6 sm:col-span-4">
                      <GlassTile label="Hour Engagement" value={hourMult == null ? 'x--' : `x${hourMult.toFixed(2)}`} />
                    </div>
                  </>
                ) : (
                  <>
                    <div className="col-span-12">
                      <div className="rounded-[11px] border border-white/55 bg-white/45 p-2 shadow-[0_12px_28px_rgba(0,0,0,0.22),inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-white/22 dark:bg-black/45 dark:shadow-[0_14px_30px_rgba(0,0,0,0.6),inset_0_1px_0_rgba(255,255,255,0.16)]">
                        <div className="text-[8px] font-black uppercase tracking-[0.14em] text-foreground/70">Trajectory</div>
                        <div className="mt-1 grid grid-cols-4 gap-1.5">
                          <div className="rounded-[8px] bg-white/55 p-1.5 text-center shadow-[inset_0_1px_0_rgba(255,255,255,0.6)] dark:bg-white/14"><div className="text-[8px] font-black uppercase text-foreground/70">D1</div><div className="text-[16px] sm:text-[18px] font-black leading-none text-foreground/95">{d1 == null ? '--' : Math.round(d1)}</div></div>
                          <div className="rounded-[8px] bg-white/55 p-1.5 text-center shadow-[inset_0_1px_0_rgba(255,255,255,0.6)] dark:bg-white/14"><div className="text-[8px] font-black uppercase text-foreground/70">D3</div><div className="text-[16px] sm:text-[18px] font-black leading-none text-foreground/95">{d3 == null ? '--' : Math.round(d3)}</div></div>
                          <div className="rounded-[8px] bg-white/55 p-1.5 text-center shadow-[inset_0_1px_0_rgba(255,255,255,0.6)] dark:bg-white/14"><div className="text-[8px] font-black uppercase text-foreground/70">D7</div><div className="text-[16px] sm:text-[18px] font-black leading-none text-foreground/95">{d7 == null ? '--' : Math.round(d7)}</div></div>
                          <div className={`rounded-[8px] p-1.5 text-center transition-colors duration-300 ${deltaBgClass}`}>
                            <div className={`text-[8px] font-black uppercase ${deltaLabelClass}`}>Δ</div>
                            <div className={`text-[16px] sm:text-[18px] font-black leading-none ${deltaTextClass}`}>
                              {displayDeltaStr}
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>

                    <div className="col-span-12 grid grid-cols-2 gap-1">
                      {supportMetrics.map((metric) => (
                        <MetricChip key={metric.key} label={metric.label} value={metric.value} />
                      ))}
                    </div>
                  </>
                )}

                <div className="col-span-12 mt-1 sm:mt-2">
                  <div
                    onClick={(event) => {
                      event.stopPropagation();
                      if (isPrimed && item.postUrl) {
                        window.open(item.postUrl, '_blank', 'noreferrer');
                        setIsPrimed(false);
                      } else {
                        event.preventDefault();
                        setIsPrimed(true);
                        setTimeout(() => setIsPrimed(false), 3000);
                      }
                    }}
                    className="group relative cursor-pointer pointer-events-auto flex h-10 sm:h-13 w-full items-center justify-center rounded-[16px] overflow-hidden bg-black dark:bg-[#111] shadow-[0_16px_32px_rgba(0,0,0,0.4),inset_0_2px_4px_rgba(255,255,255,0.2)] dark:shadow-[0_16px_40px_rgba(0,0,0,0.6),inset_0_1px_2px_rgba(255,255,255,0.08)] transition-transform active:scale-[0.96]"
                  >
                    <motion.div 
                      initial={{ y: '100%' }}
                      animate={{ y: isPrimed ? '0%' : '100%' }}
                      transition={{ duration: 0.3, ease: 'easeOut' }}
                      className="absolute inset-0 bg-[#CCFF00] shadow-[inset_0_2px_4px_rgba(255,255,255,0.8)] z-0"
                    />
                    <span 
                      className={`relative z-10 text-[11px] font-black uppercase tracking-[0.2em] transition-colors duration-300 ${isPrimed ? 'text-black drop-shadow-sm' : 'text-[#CCFF00] drop-shadow-[0_0_8px_rgba(204,255,0,0.3)] dark:text-white dark:drop-shadow-none'}`}
                    >
                      {isPrimed ? 'Tap To Open' : 'Open Post'}
                    </span>
                  </div>
                </div>
              </motion.div>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
