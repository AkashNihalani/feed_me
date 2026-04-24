'use client';

import { PatternBoardItem } from './dashboardTypes';

type FeedPatternBoardProps = {
  patterns: PatternBoardItem[];
};

function toFiniteNumber(value: unknown): number | null {
  const numeric = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function familyLabel(context: PatternBoardItem['context']): string {
  if (context === 'cross') return 'Cross';
  if (context === 'anchor') return 'Anchor';
  return 'Own';
}

export default function FeedPatternBoard({ patterns }: FeedPatternBoardProps) {
  if (patterns.length === 0) {
    return (
      <div className="fm-feed-mobile-panel min-w-0">
        <div className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/38 dark:text-white/32">
          Pattern Board
        </div>
        <div className="mt-3 rounded-[20px] border border-white/70 bg-white/58 px-4 py-5 text-[12px] font-semibold text-foreground/50 shadow-[inset_0_1px_0_rgba(255,255,255,0.84),0_12px_26px_-18px_rgba(15,23,42,0.16)] dark:border-white/8 dark:bg-white/[0.03] dark:text-white/42 dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.05),0_16px_32px_-18px_rgba(0,0,0,0.45)]">
          Patterns will start showing here as new D7 alerts accumulate.
        </div>
      </div>
    );
  }

  return (
    <div className="fm-feed-mobile-panel min-w-0">
      <div className="flex items-center justify-between gap-3">
        <div className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/38 dark:text-white/32">
          Pattern Board
        </div>
        <div className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/30 dark:text-white/24">
          Overview
        </div>
      </div>

      <div className="mt-3 grid grid-cols-1 gap-2.5 lg:grid-cols-2">
        {patterns.slice(0, 6).map((pattern) => (
          <div
            key={`${pattern.signal_code}:${pattern.context}:${pattern.pattern_name}:${pattern.latest_business_day}`}
            className="rounded-[20px] border border-white/70 bg-white/58 px-4 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.84),0_12px_26px_-18px_rgba(15,23,42,0.16)] dark:border-white/8 dark:bg-white/[0.03] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.05),0_16px_32px_-18px_rgba(0,0,0,0.45)]"
          >
            <div className="flex items-center justify-between gap-3">
              <div className="text-[13px] font-black tracking-[-0.02em] text-foreground dark:text-white">
                {pattern.pattern_label}
              </div>
              <div className="rounded-full bg-black/6 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/60 dark:bg-white/8 dark:text-white/58">
                {familyLabel(pattern.context)}
              </div>
            </div>

            <div className="mt-2 flex flex-wrap gap-1.5">
              <span className="rounded-full bg-black/6 px-2 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/66 dark:bg-white/8 dark:text-white/60">
                {pattern.trigger_count} triggers
              </span>
              {pattern.avg_hot_percentile != null && (
              <span className="rounded-full bg-black/6 px-2 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/66 dark:bg-white/8 dark:text-white/60">
                  Avg top {Math.round(toFiniteNumber(pattern.avg_hot_percentile) ?? 0)}%
                </span>
              )}
              {pattern.feeders_count != null && pattern.feeders_count > 1 && (
                <span className="rounded-full bg-black/6 px-2 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/66 dark:bg-white/8 dark:text-white/60">
                  {Math.round(pattern.feeders_count)} feeders
                </span>
              )}
              {pattern.anchor_gap != null && (
                <span className="rounded-full bg-black/6 px-2 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/66 dark:bg-white/8 dark:text-white/60">
                  Gap +{Math.round(toFiniteNumber(pattern.anchor_gap) ?? 0)} pts
                </span>
              )}
              {(toFiniteNumber(pattern.recent_lift) ?? 0) > 0 && (
                <span className="rounded-full bg-black/6 px-2 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/66 dark:bg-white/8 dark:text-white/60">
                  14d lift {(toFiniteNumber(pattern.recent_lift) ?? 0).toFixed(1)}x
                </span>
              )}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
