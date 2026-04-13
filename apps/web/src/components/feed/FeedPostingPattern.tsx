'use client';

import { useMemo } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { cn } from '@/lib/utils';
import { PostingPatternPayload, Timeframe } from './dashboardTypes';

/* ═══════════════════════════════════════════════════════════════════
   POSTING PULSE  v3
   Headline → Activity strip → Stat chips → Feeder bars
   ═══════════════════════════════════════════════════════════════════ */

type Props = {
  pattern: PostingPatternPayload | null | undefined;
  timeframe?: Timeframe;
};

const EASE = [0.22, 1, 0.36, 1] as const;
const SPRING = { type: 'spring' as const, stiffness: 300, damping: 26, mass: 0.8 };

/* ── Helpers ── */

function mediaIcon(type: string | null | undefined): string {
  if (!type || type === 'unknown') return '◻';
  const l = type.toLowerCase();
  if (l === 'video' || l === 'reel') return '🎬';
  if (l === 'image' || l === 'photo') return '📸';
  if (l.includes('carousel') || l.includes('sidecar')) return '📐';
  return '◻';
}

function mediaLabel(type: string | null | undefined): string {
  if (!type || type === 'unknown') return 'Mix';
  if (type === 'sidecar/carousel') return 'Carousel';
  const l = type.toLowerCase();
  if (l === 'video') return 'Reel';
  return type.replace(/\b\w/g, (c) => c.toUpperCase());
}

function windowLabel(tf: Timeframe): string {
  if (tf === '7D') return 'this week';
  if (tf === '30D') return 'this month';
  if (tf === '60D') return 'in 60 days';
  return 'this quarter';
}

function formatGapShort(hours: number | null | undefined): string {
  if (hours == null || !Number.isFinite(hours)) return '--';
  if (hours < 1) return '<1h';
  if (hours < 24) return `${Math.round(hours)}h`;
  const days = hours / 24;
  return days >= 10 ? `${Math.round(days)}d` : `${days.toFixed(1)}d`;
}

function buildHeadline(p: PostingPatternPayload, tf: Timeframe): string {
  const totalPosts = p.rhythm_days.reduce((s, d) => s + Math.max(0, d.post_count), 0);
  const window = windowLabel(tf);
  const daysSince = p.days_since_last_post;
  const delta = p.delta_percent;

  let gapSuffix = '';
  if (daysSince != null && daysSince > 0) {
    gapSuffix = daysSince === 1 ? ' · last post yesterday' : ` · last post ${daysSince}d ago`;
  } else if (daysSince === 0) {
    gapSuffix = ' · posted today';
  }

  if (p.status === 'insufficient_data') return 'Just getting started — pattern forming';
  if (p.status === 'dormant') {
    return daysSince != null
      ? `Silent for ${daysSince} day${daysSince === 1 ? '' : 's'} — longest gap yet`
      : 'No recent posts detected';
  }

  let deltaClause = '';
  if (delta != null && Math.abs(delta) >= 5) {
    deltaClause = delta > 0
      ? ` — ${Math.round(delta)}% hotter than usual`
      : ` — ${Math.abs(Math.round(delta))}% cooler than usual`;
  } else {
    deltaClause = ' — same energy as usual';
  }

  return `${totalPosts} post${totalPosts === 1 ? '' : 's'} ${window}${deltaClause}${gapSuffix}`;
}

/* ── Dot grouping ── */

type DotGroup = { label: string; days: Array<{ day: string; count: number }> };

function groupDots(rhythmDays: PostingPatternPayload['rhythm_days'], tf: Timeframe): DotGroup[] {
  const days = rhythmDays.map((d) => ({ day: d.day_ist, count: Math.max(0, d.post_count) }));

  if (tf === '7D') {
    return days.map((d) => {
      const date = new Date(d.day + 'T00:00:00+05:30');
      const label = date.toLocaleDateString('en-US', { weekday: 'narrow', timeZone: 'Asia/Kolkata' });
      return { label, days: [d] };
    });
  }

  const groups: DotGroup[] = [];
  for (let i = 0; i < days.length; i += 7) {
    const chunk = days.slice(i, i + 7);
    groups.push({ label: `W${Math.floor(i / 7) + 1}`, days: chunk });
  }
  return groups;
}

/* ── Stat chip glass styles ── */
const CHIP_CLASS = cn(
  'rounded-[14px] px-3 py-2 text-center',
  'bg-gradient-to-b from-white/70 to-white/40 border border-white/70',
  'shadow-[inset_0_1px_0_rgba(255,255,255,0.8),0_2px_8px_rgba(15,23,42,0.04)]',
  'dark:from-white/[0.05] dark:to-white/[0.02] dark:border-white/[0.06]',
  'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),0_4px_12px_rgba(0,0,0,0.2)]',
);

/* ── Component ── */

export default function FeedPostingPattern({ pattern, timeframe = '7D' }: Props) {
  const maxCount = useMemo(() => {
    const counts = pattern?.rhythm_days?.map((d) => Math.max(0, d.post_count)) ?? [];
    return Math.max(1, ...counts);
  }, [pattern]);

  const dotGroups = useMemo(
    () => groupDots(pattern?.rhythm_days ?? [], timeframe),
    [pattern?.rhythm_days, timeframe],
  );

  const totalDays = pattern?.rhythm_days?.length ?? 0;
  const useCompactDots = totalDays > 30;

  /* Full media type breakdown from backend */
  const mediaMix = useMemo(() => {
    if (!pattern?.media_mix?.length) {
      // Fallback: derive from feeder dominant types if media_mix not yet available
      const seen = new Map<string, number>();
      for (const row of (pattern?.feeder_rows ?? [])) {
        const t = row.dominant_media_type;
        if (t && t !== 'unknown') seen.set(t, (seen.get(t) || 0) + 1);
      }
      const total = Array.from(seen.values()).reduce((s, v) => s + v, 0) || 1;
      return Array.from(seen.entries())
        .map(([type, count]) => ({ type, count, pct: Math.round((count / total) * 100) }))
        .sort((a, b) => b.count - a.count);
    }
    return pattern.media_mix;
  }, [pattern]);

  /* Max feeder rate for proportional bars */
  const maxFeederRate = useMemo(() => {
    if (!pattern) return 1;
    return Math.max(1, ...pattern.feeder_rows.map((r) => r.posts_per_week_current));
  }, [pattern]);

  /* ── Empty state ── */
  if (!pattern) {
    return (
      <div className="fm-depth-glass relative flex h-full w-full flex-col justify-between overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">
        <div className="relative z-10">
          <span className="fm-label fm-depth-title">Posting Pulse</span>
          <div className="mt-5 text-[24px] font-black leading-tight text-foreground dark:text-white sm:text-[28px]">
            Just getting started
          </div>
          <p className="mt-3 max-w-[26rem] text-[12px] font-semibold leading-5 text-foreground/52 dark:text-white/48">
            Activity dots appear after posts land in the current window. Check back soon.
          </p>
        </div>
      </div>
    );
  }

  const headline = buildHeadline(pattern, timeframe);
  const rows = pattern.feeder_rows.slice(0, 4);
  const fewFeeders = rows.length <= 2;

  return (
    <div className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">
      <div className="relative z-10 flex h-full min-h-0 flex-col">

        {/* ── Label ── */}
        <span className="fm-label fm-depth-title">Posting Pulse</span>

        {/* ── Conversational Headline ── */}
        <AnimatePresence mode="wait">
          <motion.p
            key={headline}
            initial={{ opacity: 0, y: 8, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -4, scale: 0.98 }}
            transition={SPRING}
            className="mt-2 text-[clamp(15px,3vw,20px)] font-black leading-[1.25] tracking-[-0.02em] text-foreground dark:text-white"
          >
            {headline}
          </motion.p>
        </AnimatePresence>

        {/* ── Activity Strip (recessed channel with bars) ── */}
        <div className={cn(
          'mt-3.5 rounded-[14px] px-2.5 py-2.5 sm:px-3',
          'bg-black/[0.03] border border-black/[0.04]',
          'shadow-[inset_0_2px_4px_rgba(0,0,0,0.04)]',
          'dark:bg-white/[0.03] dark:border-white/[0.04]',
          'dark:shadow-[inset_0_2px_6px_rgba(0,0,0,0.3)]',
        )}>
          <div className="flex items-end gap-[3px] sm:gap-1" style={{ height: useCompactDots ? 28 : 36 }}>
            {dotGroups.map((group, gi) => (
              <div key={gi} className="flex flex-1 items-end gap-[2px]">
                {group.days.map((d, di) => {
                  const hasPosts = d.count > 0;
                  const sizePct = hasPosts ? 30 + (d.count / maxCount) * 70 : 0;
                  return (
                    <motion.div
                      key={d.day}
                      initial={{ height: 0, opacity: 0 }}
                      animate={{
                        height: hasPosts ? `${sizePct}%` : useCompactDots ? 2 : 4,
                        opacity: hasPosts ? 1 : 0.2,
                      }}
                      transition={{ ...SPRING, delay: Math.min((gi * group.days.length + di) * 0.015, 0.3) }}
                      className={cn(
                        'flex-1 rounded-[4px]',
                        hasPosts
                          ? 'bg-[#E11D48] shadow-[0_0_8px_rgba(225,29,72,0.2)] dark:shadow-[0_0_10px_rgba(225,29,72,0.3)]'
                          : 'bg-black/8 dark:bg-white/10 rounded-full',
                      )}
                      style={{ minHeight: hasPosts ? 4 : undefined }}
                      title={`${d.count} post${d.count === 1 ? '' : 's'} · ${d.day}`}
                    />
                  );
                })}
                {/* Week separator for 30D+ */}
                {!useCompactDots && gi < dotGroups.length - 1 && timeframe !== '7D' && (
                  <div className="mx-[1px] h-full w-px shrink-0 bg-foreground/[0.05] dark:bg-white/[0.05]" />
                )}
              </div>
            ))}
          </div>
          {/* Labels row */}
          <div className="mt-1.5 flex">
            {dotGroups.map((group, i) => (
              <div
                key={i}
                className="flex-1 text-center text-[7px] font-black uppercase tracking-wider text-foreground/28 dark:text-white/20"
              >
                {group.label}
              </div>
            ))}
          </div>
        </div>

        {/* ── Stat Chips: Cadence · Avg Gap · Dominant Media ── */}
        <div className={cn(
          'mt-3 grid gap-1.5 sm:gap-2',
          mediaMix.length <= 1 ? 'grid-cols-3' : mediaMix.length === 2 ? 'grid-cols-4' : 'grid-cols-3 sm:grid-cols-5',
        )}>
          <motion.div
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ ...SPRING, delay: 0.1 }}
            className={CHIP_CLASS}
          >
            <div className="text-[7px] font-black uppercase tracking-[0.14em] text-foreground/38 dark:text-white/30">Cadence</div>
            <div className="mt-0.5 text-[15px] font-black leading-none tracking-[-0.02em] text-foreground dark:text-white sm:text-[17px]">
              {pattern.posts_per_week_current >= 10
                ? `${Math.round(pattern.posts_per_week_current)}`
                : pattern.posts_per_week_current.toFixed(1)}/wk
            </div>
          </motion.div>

          <motion.div
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ ...SPRING, delay: 0.15 }}
            className={CHIP_CLASS}
          >
            <div className="text-[7px] font-black uppercase tracking-[0.14em] text-foreground/38 dark:text-white/30">Avg gap</div>
            <div className="mt-0.5 text-[15px] font-black leading-none tracking-[-0.02em] text-foreground dark:text-white sm:text-[17px]">
              {formatGapShort(pattern.usual_gap_hours)}
            </div>
          </motion.div>

          {mediaMix.map((item, i) => (
            <motion.div
              key={item.type}
              initial={{ opacity: 0, y: 6 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ ...SPRING, delay: 0.2 + i * 0.05 }}
              className={cn(CHIP_CLASS, 'relative overflow-hidden')}
            >
              {/* Fill bar showing share */}
              <motion.div
                initial={{ width: 0 }}
                animate={{ width: `${item.pct}%` }}
                transition={{ duration: 0.6, ease: EASE, delay: 0.35 + i * 0.05 }}
                className="pointer-events-none absolute inset-y-0 left-0 bg-[#E11D48]/6 dark:bg-[#E11D48]/10 rounded-[14px]"
              />
              <div className="relative z-10 text-[7px] font-black uppercase tracking-[0.14em] text-foreground/38 dark:text-white/30">
                {mediaIcon(item.type)} {mediaLabel(item.type)}
              </div>
              <div className="relative z-10 mt-0.5 text-[15px] font-black leading-none tracking-[-0.02em] text-foreground dark:text-white sm:text-[17px]">
                {item.pct}%
              </div>
            </motion.div>
          ))}
        </div>

        {/* ── Feeder Section ── */}
        {rows.length > 0 && (
          <div className={cn(
            'mt-3 min-h-0 flex-1 overflow-hidden border-t border-black/6 dark:border-white/[0.06]',
            fewFeeders ? 'pt-3' : 'pt-2',
          )}>
            <div className={cn(fewFeeders ? 'space-y-3' : 'space-y-1.5')}>
              {rows.map((row, ri) => {
                const barPct = maxFeederRate > 0
                  ? Math.max(4, (row.posts_per_week_current / maxFeederRate) * 100)
                  : 4;
                const isDormant = row.status === 'dormant' || row.status === 'insufficient_data';
                const feederMedia = row.dominant_media_type;

                return (
                  <motion.div
                    key={row.feeder_id}
                    initial={{ opacity: 0, x: -10 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ ...SPRING, delay: 0.2 + ri * 0.07 }}
                    className={cn(
                      'rounded-[12px] px-2.5 py-2',
                      fewFeeders && 'bg-black/[0.02] border border-black/[0.03] dark:bg-white/[0.02] dark:border-white/[0.03]',
                    )}
                  >
                    {/* Top row: handle + rate + media badge */}
                    <div className="flex items-center justify-between gap-2">
                      <div className="flex items-center gap-2 min-w-0">
                        {/* Status dot */}
                        <div className={cn(
                          'h-1.5 w-1.5 shrink-0 rounded-full',
                          isDormant
                            ? 'bg-foreground/20 dark:bg-white/20'
                            : row.status === 'accelerating'
                              ? 'bg-[#E11D48] shadow-[0_0_6px_rgba(225,29,72,0.4)]'
                              : 'bg-foreground/40 dark:bg-white/40',
                        )} />
                        <span className="truncate text-[11px] font-black text-foreground dark:text-white sm:text-[12px]">
                          @{row.handle}
                        </span>
                      </div>
                      <div className="flex items-center gap-2 shrink-0">
                        {feederMedia && feederMedia !== 'unknown' && (
                          <span className="text-[9px] font-bold uppercase tracking-[0.06em] text-foreground/35 dark:text-white/28">
                            {mediaIcon(feederMedia)}
                          </span>
                        )}
                        <span className={cn(
                          'text-[10px] font-black tabular-nums sm:text-[11px]',
                          isDormant ? 'text-foreground/40 dark:text-white/32' : 'text-foreground/65 dark:text-white/58',
                        )}>
                          {isDormant
                            ? row.days_since_last_post != null ? `${row.days_since_last_post}d quiet` : 'quiet'
                            : row.posts_per_week_current >= 10
                              ? `${Math.round(row.posts_per_week_current)}/wk`
                              : `${row.posts_per_week_current.toFixed(1)}/wk`}
                        </span>
                        {/* Delta arrow */}
                        {row.delta_percent != null && Math.abs(row.delta_percent) >= 5 && !isDormant && (
                          <span className={cn(
                            'text-[9px] font-black',
                            row.delta_percent > 0 ? 'text-[#E11D48]' : 'text-foreground/40 dark:text-white/30',
                          )}>
                            {row.delta_percent > 0 ? '↑' : '↓'}
                          </span>
                        )}
                      </div>
                    </div>

                    {/* Activity bar — proportional to max feeder rate */}
                    <div className={cn(
                      'relative mt-1.5 h-[6px] w-full overflow-hidden rounded-full',
                      'bg-black/[0.04] dark:bg-white/[0.06]',
                      fewFeeders && 'h-[8px]',
                    )}>
                      <motion.div
                        initial={{ width: 0 }}
                        animate={{ width: isDormant ? '2%' : `${barPct}%` }}
                        transition={{ duration: 0.6, ease: EASE, delay: 0.28 + ri * 0.07 }}
                        className={cn(
                          'absolute inset-y-0 left-0 rounded-full',
                          isDormant
                            ? 'bg-foreground/10 dark:bg-white/8'
                            : 'bg-[#E11D48] shadow-[0_0_10px_rgba(225,29,72,0.18)] dark:shadow-[0_0_12px_rgba(225,29,72,0.28)]',
                        )}
                      />
                    </div>
                  </motion.div>
                );
              })}
            </div>
          </div>
        )}

        {rows.length === 0 && (
          <div className="mt-3 flex-1 border-t border-black/6 pt-3 dark:border-white/[0.06]">
            <p className="text-[11px] font-semibold leading-4 text-foreground/45 dark:text-white/40">
              Feeder breakdown appears after more posts land.
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
