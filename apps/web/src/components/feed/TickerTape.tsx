'use client';

import { useMemo, useRef, useEffect, useState } from 'react';
import { cn } from '@/lib/utils';

export type TickerItem = {
  id: string;
  handle: string;
  likesDelta: number;
  commentsDelta: number;
  postsDelta: number;
};

interface TickerTapeProps {
  items: TickerItem[];
  className?: string;
}

type MetricMode = 'likes' | 'comments' | 'posts';

const MODES: { key: MetricMode; label: string }[] = [
  { key: 'likes', label: 'LIKES' },
  { key: 'comments', label: 'COMMENTS' },
  { key: 'posts', label: 'POSTS' },
];

function compactSigned(value: number) {
  const abs = Math.abs(value);
  const sign = value > 0 ? '+' : value < 0 ? '-' : '';
  if (abs >= 1_000_000) return `${sign}${(abs / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
  if (abs >= 1_000) return `${sign}${(abs / 1_000).toFixed(1).replace(/\.0$/, '')}K`;
  return `${sign}${abs}`;
}

/* ── Single entry chip ── */
function EntryChip({ handle, delta }: { handle: string; delta: number }) {
  const pos = delta > 0;
  const neg = delta < 0;
  return (
    <div className="mr-5 flex shrink-0 items-center gap-2.5 border-r border-black/6 pr-5 last:border-0 dark:border-white/6">
      <span className="text-[10px] font-black uppercase tracking-[0.12em] text-black/60 dark:text-white/50 sm:text-[11px]">
        {handle}
      </span>
      <span
        className={cn(
          'flex items-center gap-1 rounded-[8px] px-2 py-0.5 text-[10px] font-black tracking-[0.1em] sm:text-[11px]',
          pos && 'border border-[#FB7185]/60 bg-[#E11D48]/78 text-white dark:bg-[#E11D48] dark:text-white dark:border-[#E11D48]/30',
          neg && 'bg-black/8 text-black/50 border border-black/8 dark:bg-white/6 dark:text-white/40 dark:border-white/8',
          !pos && !neg && 'bg-black/4 text-black/35 border border-black/4 dark:bg-white/4 dark:text-white/30 dark:border-white/4',
        )}
      >
        {compactSigned(delta)}
        {pos && <span className="ml-0.5 text-[8px]">▲</span>}
        {neg && <span className="ml-0.5 text-[8px] opacity-60">▼</span>}
        {!pos && !neg && <span className="ml-0.5 text-[8px] opacity-40">~</span>}
      </span>
    </div>
  );
}

/* ── Neon mode tag (inline in the tape) ── */
function ModeTag({ label }: { label: string }) {
  return (
    <div className="mr-5 flex shrink-0 items-center pr-5">
      <span
        className={cn(
          'rounded-[10px] px-3 py-1.5',
          'border border-[#FB7185] bg-[#E11D48]',
          'shadow-[inset_0_1px_0_rgba(255,255,255,0.74),inset_0_-2px_4px_rgba(136,19,55,0.18),0_4px_10px_rgba(225,29,72,0.25)]',
          'dark:border-[#FDA4AF]/30',
          'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.18),0_4px_20px_rgba(225,29,72,0.2),0_12px_28px_rgba(0,0,0,0.5)]',
          'text-[9px] font-black tracking-[0.16em] text-white sm:text-[10px]',
        )}
      >
        {label}
      </span>
    </div>
  );
}

export default function TickerTape({ items, className }: TickerTapeProps) {
  const trackRef = useRef<HTMLDivElement>(null);
  const [duration, setDuration] = useState(24);

  /* Build a flat tape: [TAG entries...] [TAG entries...] [TAG entries...] */
  const tapeSegments = useMemo(() => {
    if (items.length === 0) return [];
    const result: Array<{ type: 'tag'; label: string; key: string } | { type: 'entry'; handle: string; delta: number; key: string }> = [];

    for (const mode of MODES) {
      result.push({ type: 'tag', label: mode.label, key: `tag-${mode.key}` });
      for (const item of items) {
        const delta =
          mode.key === 'likes' ? item.likesDelta
          : mode.key === 'comments' ? item.commentsDelta
          : item.postsDelta;
        result.push({ type: 'entry', handle: item.handle, delta, key: `${mode.key}-${item.id}` });
      }
    }
    return result;
  }, [items]);

  /* Measure actual track width and compute duration for consistent speed */
  useEffect(() => {
    if (!trackRef.current) return;
    const measure = () => {
      const el = trackRef.current;
      if (!el) return;
      const halfWidth = el.scrollWidth / 2;
      // Speed: ~60px per second — premium, readable pace
      const speed = 60;
      setDuration(Math.max(12, halfWidth / speed));
    };
    measure();
    const ro = new ResizeObserver(measure);
    ro.observe(trackRef.current);
    return () => ro.disconnect();
  }, [tapeSegments]);

  if (items.length === 0) return null;

  /* Duplicate once for seamless loop */
  const renderTape = () => (
    <div className="flex shrink-0 items-center">
      {tapeSegments.map((seg, idx) =>
        seg.type === 'tag' ? (
          <ModeTag key={`${seg.key}-${idx}`} label={seg.label} />
        ) : (
          <EntryChip key={`${seg.key}-${idx}`} handle={seg.handle} delta={seg.delta} />
        ),
      )}
    </div>
  );

  return (
    <div
      className={cn(
        /* Recessed neumorphic track — matching ChronoTabs & dashboard timeframe selector */
        'relative flex h-[42px] w-full items-center overflow-hidden rounded-[14px]',
        'bg-black/[0.03] border border-black/[0.04]',
        'shadow-[inset_0_2px_4px_rgba(0,0,0,0.06),inset_0_-1px_0_rgba(255,255,255,0.5)]',
        'dark:bg-white/[0.03] dark:border-white/[0.05]',
        'dark:shadow-[inset_0_2px_4px_rgba(0,0,0,0.3),inset_0_-1px_0_rgba(255,255,255,0.03)]',
        className,
      )}
    >
      {/* Edge fades */}
      <div className="pointer-events-none absolute inset-y-0 left-0 z-10 w-10 bg-gradient-to-r from-white/90 via-white/40 to-transparent dark:from-[#0a0a0a]/90 dark:via-[#0a0a0a]/40" />
      <div className="pointer-events-none absolute inset-y-0 right-0 z-10 w-10 bg-gradient-to-l from-white/90 via-white/40 to-transparent dark:from-[#0a0a0a]/90 dark:via-[#0a0a0a]/40" />

      {/* CSS-animated infinite scrolling track */}
      <div
        ref={trackRef}
        className="flex items-center whitespace-nowrap will-change-transform"
        style={{
          animation: `tickerScrollHalf ${duration}s linear infinite`,
          transform: 'translate3d(0,0,0)',
        }}
      >
        {renderTape()}
        {renderTape()}
      </div>
    </div>
  );
}
