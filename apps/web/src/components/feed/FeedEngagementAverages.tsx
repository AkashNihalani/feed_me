'use client';

import { memo, useEffect, useMemo, useState } from 'react';
import { animate, motion, useMotionValue, useReducedMotion } from 'framer-motion';
import { Eye, Film, Heart, Image as ImageIcon, Layers, MessageCircle } from 'lucide-react';
import { EngagementAverageMediaType, EngagementAverageRow } from './dashboardTypes';

type MetricKey = 'avg_likes' | 'avg_comments' | 'avg_views';
type DisplayMediaType = Exclude<EngagementAverageMediaType, 'all'>;
type DisplayEngagementAverageRow = EngagementAverageRow & { media_type: DisplayMediaType };

const MEDIA_ORDER: DisplayMediaType[] = ['reel', 'carousel', 'image'];

const MEDIA_META = {
  reel: { label: 'Reels', Icon: Film },
  image: { label: 'Images', Icon: ImageIcon },
  carousel: { label: 'Carousels', Icon: Layers },
} satisfies Record<DisplayMediaType, { label: string; Icon: typeof Film }>;

const METRIC_META = {
  avg_likes: { label: 'Likes', Icon: Heart },
  avg_comments: { label: 'Comms', Icon: MessageCircle },
  avg_views: { label: 'Views', Icon: Eye },
} satisfies Record<MetricKey, { label: string; Icon: typeof Heart }>;

function emptyRow(mediaType: DisplayMediaType): DisplayEngagementAverageRow {
  return {
    media_type: mediaType,
    post_count: 0,
    metric_count: 0,
    avg_likes: null,
    avg_comments: null,
    avg_views: null,
  };
}

function formatCompact(value: number): string {
  if (!Number.isFinite(value)) return '--';
  const rounded = Math.max(0, Math.round(value));
  if (rounded >= 1_000_000) {
    const next = rounded / 1_000_000;
    return `${next >= 10 ? next.toFixed(1) : next.toFixed(2)}`.replace(/\.0+$/, '').replace(/(\.\d)0$/, '$1') + 'M';
  }
  if (rounded >= 1_000) {
    const next = rounded / 1_000;
    return `${next >= 10 ? next.toFixed(1) : next.toFixed(2)}`.replace(/\.0+$/, '').replace(/(\.\d)0$/, '$1') + 'K';
  }
  return new Intl.NumberFormat('en-US').format(rounded);
}

function AnimatedMetricValue({ value, muted }: { value: number | null; muted?: boolean }) {
  const reduceMotion = useReducedMotion();
  const motionValue = useMotionValue(value ?? 0);
  const [displayValue, setDisplayValue] = useState(value ?? 0);

  useEffect(() => {
    if (value == null) return undefined;

    const unsubscribe = motionValue.on('change', (latest) => setDisplayValue(latest));
    const controls = animate(motionValue, value, {
      duration: reduceMotion ? 0.01 : 0.58,
      ease: [0.22, 1, 0.36, 1],
    });

    return () => {
      unsubscribe();
      controls.stop();
    };
  }, [motionValue, reduceMotion, value]);

  if (value == null) {
    return <span className="text-foreground/28 dark:text-white/24">--</span>;
  }

  return (
    <motion.span
      key={value == null ? 'empty' : 'value'}
      initial={false}
      animate={{ opacity: muted ? 0.58 : 1 }}
      className="tabular-nums"
    >
      {formatCompact(displayValue)}
    </motion.span>
  );
}

function MetricCell({ row, metricKey }: { row: DisplayEngagementAverageRow; metricKey: MetricKey }) {
  const value = row[metricKey];
  const muted = row.metric_count === 0 || value == null;

  return (
    <div className="flex min-w-0 items-center justify-end text-right text-[18px] font-black leading-none text-foreground dark:text-white sm:text-[20px] lg:text-[24px]">
      <AnimatedMetricValue value={value} muted={muted} />
    </div>
  );
}

function FeedEngagementAverages({ rows }: { rows: EngagementAverageRow[] }) {
  const normalizedRows = useMemo(() => {
    const byType = new Map<DisplayMediaType, DisplayEngagementAverageRow>();
    for (const row of Array.isArray(rows) ? rows : []) {
      if (MEDIA_ORDER.includes(row.media_type as DisplayMediaType)) {
        byType.set(row.media_type as DisplayMediaType, row as DisplayEngagementAverageRow);
      }
    }
    return MEDIA_ORDER.map((type) => byType.get(type) ?? emptyRow(type));
  }, [rows]);

  const totalPosts = normalizedRows.reduce((sum, row) => sum + Math.max(0, Number(row.post_count) || 0), 0);

  return (
    <div className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">
      <div className="relative z-10 flex items-start justify-between gap-3">
        <div>
          <span className="fm-label fm-depth-title">Average Engagement</span>
          <div className="mt-0.5 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/34">
            Latest checkpoint averages
          </div>
        </div>
        <motion.div
          key={totalPosts}
          initial={{ opacity: 0, y: -4 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.24, ease: [0.22, 1, 0.36, 1] }}
          className="shrink-0 rounded-full bg-[#E11D48] px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.12em] text-white shadow-[0_9px_20px_-10px_rgba(225,29,72,0.72)] sm:px-3.5 sm:py-1.5 sm:text-[10px] lg:px-4 lg:py-2 lg:text-[11px]"
        >
          {totalPosts} posts
        </motion.div>
      </div>

      <div className="relative z-10 mt-3 flex min-h-0 flex-1 flex-col overflow-hidden border-y border-black/7 dark:border-white/8">
        <div className="grid shrink-0 grid-cols-[minmax(96px,1.1fr)_repeat(3,minmax(54px,1fr))] items-center gap-2 border-b border-black/6 px-1 py-2.5 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/36 dark:border-white/8 dark:text-white/30 sm:text-[9px]">
          <span>Type</span>
          {(Object.keys(METRIC_META) as MetricKey[]).map((metricKey) => {
            const { label, Icon } = METRIC_META[metricKey];
            return (
              <span key={metricKey} className="flex items-center justify-end gap-1">
                <Icon className="h-3.5 w-3.5" strokeWidth={2.5} />
                {label}
              </span>
            );
          })}
        </div>

        <div className="relative flex min-h-0 flex-1 flex-col divide-y divide-black/6 dark:divide-white/7">
          {normalizedRows.map((row, index) => {
            const { label, Icon } = MEDIA_META[row.media_type];

            return (
              <motion.div
                key={row.media_type}
                layout
                initial={{ opacity: 0, y: 8 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ duration: 0.28, delay: index * 0.035, ease: [0.22, 1, 0.36, 1] }}
                className="grid min-h-[58px] flex-1 grid-cols-[minmax(96px,1.1fr)_repeat(3,minmax(54px,1fr))] items-center gap-2 px-1 py-2.5 sm:min-h-[64px]"
              >
                <div className="flex min-w-0 items-center gap-2.5">
                  <div
                    className="flex h-9 w-9 shrink-0 items-center justify-center rounded-full bg-black/[0.04] text-foreground/62 ring-1 ring-black/6 dark:bg-white/[0.06] dark:text-white/52 dark:ring-white/8 sm:h-10 sm:w-10"
                  >
                    <Icon className="h-[18px] w-[18px] sm:h-5 sm:w-5" strokeWidth={2.55} />
                  </div>
                  <div className="min-w-0">
                    <div className="truncate text-[12px] font-black leading-none text-foreground dark:text-white sm:text-[13px]">
                      {label}
                    </div>
                    <div className="mt-1.5 text-[8px] font-black uppercase tracking-[0.1em] text-foreground/34 dark:text-white/28 sm:text-[9px]">
                      {row.post_count} posts
                    </div>
                  </div>
                </div>

                <MetricCell row={row} metricKey="avg_likes" />
                <MetricCell row={row} metricKey="avg_comments" />
                <MetricCell row={row} metricKey="avg_views" />
              </motion.div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

const MemoizedFeedEngagementAverages = memo(FeedEngagementAverages);
MemoizedFeedEngagementAverages.displayName = 'FeedEngagementAverages';

export default MemoizedFeedEngagementAverages;
