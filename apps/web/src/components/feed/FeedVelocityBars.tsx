'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  AnimatePresence,
  motion,
  useMotionValue,
  useReducedMotion,
  useSpring,
} from 'framer-motion';
import { DashboardSummary } from './dashboardTypes';
import { cn } from '@/lib/utils';

type MetricKey = 'views' | 'likes' | 'comments';

type MetricRow = {
  key: MetricKey;
  label: 'VIEWS' | 'LIKES' | 'COMMENTS';
  current: number | null;
  baseline: number | null;
};

type Tag =
  | { kind: 'none' }
  | { kind: 'base' }
  | { kind: 'above'; delta: number }
  | { kind: 'below'; delta: number };

const SCALE_MAX = 3.5;
const EQ_TOL = 0.05;
const EASE = [0.25, 0.1, 0.25, 1] as const;
const LAYOUT_SPRING = { type: 'spring', stiffness: 420, damping: 36, mass: 0.9 } as const;
const BAR_SPRING = { type: 'spring', stiffness: 160, damping: 24, mass: 0.9 } as const;

function percentileToRatio(value: number | null | undefined): number | null {
  if (value == null) return null;
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.max(0.4, Math.min(SCALE_MAX, (101 - n) / 35));
}

function tagFor(current: number | null, baseline: number | null): Tag {
  if (current == null || baseline == null) return { kind: 'none' };
  const delta = current - baseline;
  if (Math.abs(delta) <= EQ_TOL) return { kind: 'base' };
  return delta > 0 ? { kind: 'above', delta } : { kind: 'below', delta: -delta };
}

function toPct(value: number | null, min = 0, max = 100) {
  if (value == null) return 0;
  const pct = (value / SCALE_MAX) * 100;
  return Math.max(min, Math.min(max, pct));
}

function formatTopPercentile(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return '--';
  return `Top ${Math.round(value)}%`;
}

function ratioToTopPercentile(value: number | null | undefined) {
  if (value == null || !Number.isFinite(value)) return null;
  return Math.max(1, Math.min(99, Math.round(101 - value * 35)));
}

function AnimatedMultiplier({ value, suffix = 'x' }: { value: number | null; suffix?: string }) {
  const mv = useMotionValue(0);
  const spring = useSpring(mv, { stiffness: 320, damping: 28, mass: 0.9 });
  const [rendered, setRendered] = useState(value ?? 0);

  useEffect(() => {
    const unsub = spring.on('change', (v) => setRendered(v));
    return unsub;
  }, [spring]);

  useEffect(() => {
    mv.set(value ?? 0);
  }, [mv, value]);

  if (value === null) return <>--</>;
  return <>{rendered.toFixed(1)}{suffix}</>;
}

function AnimatedDelta({ delta }: { delta: number }) {
  const mv = useMotionValue(delta);
  const spring = useSpring(mv, { stiffness: 320, damping: 28, mass: 0.9 });
  const [rendered, setRendered] = useState(delta);

  useEffect(() => {
    const unsub = spring.on('change', (v) => setRendered(v));
    return unsub;
  }, [spring]);

  useEffect(() => {
    mv.set(delta);
  }, [mv, delta]);

  return <>{rendered.toFixed(1)}</>;
}

function TagChip({ tag }: { tag: Tag }) {
  if (tag.kind === 'none') return null;

  const tagKey = tag.kind === 'base' ? 'base' : tag.kind;
  const isPositive = tag.kind === 'above';
  const isNegative = tag.kind === 'below';

  return (
    <AnimatePresence mode="popLayout" initial={false}>
      <motion.span
        key={tagKey}
        initial={{ opacity: 0, y: 4, scale: 0.92 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        exit={{ opacity: 0, y: -4, scale: 0.92 }}
        transition={{ duration: 0.22, ease: EASE }}
        className={[
          'fm-depth-chip inline-flex min-h-[32px] min-w-[70px] shrink-0 items-center justify-center whitespace-nowrap rounded-full px-4 py-2 leading-none sm:min-h-[36px] sm:min-w-[78px] sm:px-[18px] sm:py-[9px] lg:min-h-[40px] lg:min-w-[86px] lg:px-5 lg:py-2.5',
          'text-[14px] font-black uppercase tracking-[0.14em] sm:text-[14px] sm:tracking-[0.14em] lg:text-[16px]',
          isPositive
            ? 'text-[var(--fm-accent)]'
            : 'text-foreground/55 dark:text-white/55',
        ].join(' ')}
        style={
          isPositive
            ? {
                background: 'rgb(var(--fm-accent-rgb)/0.1)',
                boxShadow:
                  '0 1px 0 rgba(255,255,255,0.66) inset, 0 -1px 0 rgb(var(--fm-accent-rgb)/0.08) inset, 0 0 0 1px rgb(var(--fm-accent-rgb)/0.18) inset, 0 5px 14px -6px rgb(var(--fm-accent-rgb)/0.24)',
              }
            : isNegative
              ? {
                  background: 'rgba(17,24,39,0.04)',
                  boxShadow:
                    '0 1px 0 rgba(255,255,255,0.66) inset, 0 0 0 1px rgba(17,24,39,0.06) inset, 0 4px 12px -8px rgba(17,24,39,0.12)',
                }
              : undefined
        }
      >
        {tag.kind === 'base' && <span>BASE</span>}
        {tag.kind === 'above' && (
          <span>
            +<AnimatedDelta delta={tag.delta} />
          </span>
        )}
        {tag.kind === 'below' && (
          <span>
            -<AnimatedDelta delta={tag.delta} />
          </span>
        )}
      </motion.span>
    </AnimatePresence>
  );
}

function FooterStat({
  label,
  value,
  meta,
  accent = false,
}: {
  label: string;
  value: string;
  meta: string;
  accent?: boolean;
}) {
  const longValue = value.length >= 7 && !value.includes(' ');

  return (
    <div className="fm-depth-inner flex min-h-[78px] min-w-0 flex-col justify-center gap-2 overflow-hidden rounded-[14px] px-2.5 py-2.5 sm:min-h-[82px] sm:px-3 lg:min-h-[86px]">
      <span className="fm-depth-title block max-w-full truncate text-[8px] font-black uppercase tracking-[0.14em] text-foreground/40 dark:text-white/36 sm:text-[10px]">
        {label}
      </span>
      <div className="flex min-w-0 flex-col gap-1">
        <span
          className={[
            'fm-depth-title block max-w-full font-black leading-[0.92]',
            longValue
              ? 'text-[18px] tracking-[-0.04em] min-[390px]:text-[18px] sm:text-[22px] sm:tracking-[-0.04em] lg:text-[28px] lg:tracking-[-0.04em]'
              : 'text-[22px] tracking-[-0.04em] sm:text-[22px] lg:text-[28px] lg:tracking-[-0.04em]',
            accent ? 'text-[var(--fm-accent)]' : 'text-foreground dark:text-white',
          ].join(' ')}
        >
          {value}
        </span>
        <span className="block max-w-full text-[8px] font-black uppercase leading-tight tracking-[0.14em] text-foreground/36 dark:text-white/32 sm:text-[10px] lg:text-[10px]">
          {meta}
        </span>
      </div>
    </div>
  );
}

function PerformanceRow({
  row,
  reduceMotion,
}: {
  row: MetricRow;
  reduceMotion: boolean;
}) {
  const tag = tagFor(row.current, row.baseline);
  const fillPct = toPct(row.current, 4, 100);
  const isAbove = tag.kind === 'above';
  const isBelow = tag.kind === 'below';

  return (
    <motion.li
      layout={reduceMotion ? false : 'position'}
      transition={reduceMotion ? { duration: 0 } : { layout: LAYOUT_SPRING }}
      className="relative isolate flex min-h-[88px] w-full min-w-0 flex-1 overflow-hidden rounded-[18px] border border-black/[0.04] bg-foreground/[0.035] px-3 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.62)] dark:border-white/[0.055] dark:bg-white/[0.035] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.045)] sm:min-h-[102px] sm:px-3.5 sm:py-3.5 lg:min-h-[108px]"
    >
      <motion.span
        aria-hidden="true"
        className={cn(
          'absolute inset-y-0 left-0 -z-10 w-full origin-left',
          isBelow
            ? 'bg-foreground/[0.055] dark:bg-white/[0.055]'
            : 'bg-[rgb(var(--fm-accent-rgb)/0.10)] dark:bg-[rgb(var(--fm-accent-rgb)/0.14)]',
        )}
        initial={reduceMotion ? false : { scaleX: 0 }}
        animate={{ scaleX: fillPct / 100 }}
        transition={reduceMotion ? { duration: 0 } : BAR_SPRING}
      />
      <span
        aria-hidden="true"
        className={cn(
          'pointer-events-none absolute inset-y-0 left-1/2 -z-10 w-px border-l border-dashed',
          isAbove ? 'border-[rgb(var(--fm-accent-rgb)/0.34)]' : 'border-foreground/16 dark:border-white/14',
        )}
      />

      <div className="relative z-[1] grid min-w-0 flex-1 grid-cols-[minmax(0,1fr)_auto] items-center gap-3">
        <div className="flex min-w-0 flex-col gap-2">
          <span className="fm-depth-title shrink-0 text-[10px] font-black uppercase leading-none tracking-[0.14em] text-foreground/45 dark:text-white/40 sm:text-[12px] lg:text-[12px]">
            {row.label}
          </span>
          <span className="min-w-0 truncate text-[9px] font-black uppercase tracking-[0.14em] text-foreground/34 dark:text-white/30">
            {row.current == null ? 'awaiting read' : `${formatTopPercentile(ratioToTopPercentile(row.current))} window`}
          </span>
        </div>
        <div className="flex shrink-0 items-center gap-2.5">
          <TagChip tag={tag} />
          <span
            className={cn(
              'fm-depth-title min-w-[74px] text-right text-[34px] font-black leading-none tracking-[-0.04em] tabular-nums sm:text-[42px] lg:text-[48px]',
              isBelow ? 'text-foreground/58 dark:text-white/58' : 'text-foreground dark:text-white',
            )}
          >
            <AnimatedMultiplier value={row.current} />
          </span>
        </div>
      </div>
    </motion.li>
  );
}

export default function FeedVelocityBars({
  summary,
  baselineSummary,
}: {
  summary: DashboardSummary | null;
  baselineSummary?: DashboardSummary | null;
}) {
  const prefersReducedMotion = useReducedMotion();
  const reduceMotion = Boolean(prefersReducedMotion);
  const baselineSource = baselineSummary ?? summary;

  const rows: MetricRow[] = useMemo(() => {
    const make = (key: MetricKey, label: MetricRow['label'], field: keyof DashboardSummary) => ({
      key,
      label,
      current: percentileToRatio((summary?.[field] as number | null | undefined) ?? null),
      baseline: percentileToRatio((baselineSource?.[field] as number | null | undefined) ?? null),
    });

    return [
      make('views', 'VIEWS', 'avg_views_percentile'),
      make('likes', 'LIKES', 'avg_likes_percentile'),
      make('comments', 'COMMENTS', 'avg_comments_percentile'),
    ];
  }, [summary, baselineSource]);

  const orderedRows = useMemo(() => {
    return [...rows].sort((a, b) => {
      const av = a.current ?? -Infinity;
      const bv = b.current ?? -Infinity;
      return bv - av;
    });
  }, [rows]);

  const hasData = rows.some((row) => row.current !== null);
  const postsWithMetrics = Math.max(0, Number(summary?.posts_with_metrics) || 0);
  const totalPosts = Math.max(0, Number(summary?.post_count) || 0);
  const leadRow = orderedRows.find((row) => row.current !== null) ?? null;
  const windowDays = Math.max(0, Number(summary?.window_days) || 0);
  const overallPace = formatTopPercentile(summary?.avg_percentile_performance ?? null);
  const windowLabel = windowDays > 0 ? `${windowDays}D` : '--';
  const coverageMeta =
    totalPosts > 0 ? `${postsWithMetrics}/${totalPosts} measured` : `${postsWithMetrics} measured`;
  const leadValue = leadRow?.label ?? '--';
  const leadMeta = leadRow?.current == null ? 'Awaiting data' : `${leadRow.current.toFixed(1)}x strongest`;

  return (
    <div className="fm-depth-glass relative flex h-full max-h-full w-full min-h-0 flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">
      <div className="relative z-10 flex min-h-0 flex-1 flex-col">
        <div className="mb-2 flex shrink-0 items-center justify-between sm:mb-2.5">
          <span className="fm-label fm-depth-title">Performance</span>
          <span className="fm-depth-chip rounded-full px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/40 dark:text-white/40">
            {hasData ? `${postsWithMetrics} posts` : 'Awaiting data'}
          </span>
        </div>

        <motion.ul
          layout={reduceMotion ? false : true}
          transition={reduceMotion ? { duration: 0 } : { layout: LAYOUT_SPRING }}
          className="relative flex min-h-0 flex-1 list-none flex-col gap-2 p-0 sm:gap-2.5 lg:gap-3"
        >
          {orderedRows.map((row) => (
            <PerformanceRow key={row.key} row={row} reduceMotion={reduceMotion} />
          ))}
        </motion.ul>

        <div className="mt-2.5 grid shrink-0 grid-cols-[0.95fr_1.1fr_0.95fr] gap-1.5 sm:mt-3 sm:gap-2.5">
          <FooterStat
            label="Avg Pace"
            value={overallPace}
            meta="Feed average"
            accent={overallPace !== '--'}
          />
          <FooterStat
            label="Lead"
            value={leadValue}
            meta={leadMeta}
          />
          <FooterStat
            label="Window"
            value={windowLabel}
            meta={coverageMeta}
          />
        </div>
      </div>
    </div>
  );
}
