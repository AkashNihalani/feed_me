'use client';

/* ─────────────────────────────────────────────────────────────
   TOP BADGE — the number: where a post ranks among its feeder's
   own posts at its latest checkpoint. Lower is stronger; Top 1%
   is the best.

   A winner (top 10%) wears the vibrant punch: a filled rose plate,
   white type, tight black numerals, and under the big sizes a
   still rose glow. Everything else sits on a neutral dark plate
   that reads over any picture without a blur. Not measured yet:
   a muted "Measuring".

   sm, md — compact pills (TOP 4%) for small places
   lg — the sheet's number · xl — the reel's lead (a 48px numeral)
   lg and xl are tickets: TOP and AT D3 across the top, the
   numeral under them.
   ───────────────────────────────────────────────────────────── */

import { memo } from 'react';
import { formatTopPercent, isWinner, type TopBadgeProps } from '@/lib/feedStream/contract';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

// the vibrant punch: filled rose, white type, an inner highlight
const WINNER = 'bg-[var(--fm-accent)] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.24)]';
// the tickets add a still rose glow under the plate
const WINNER_TICKET = 'bg-[var(--fm-accent)] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.24),0_14px_30px_-14px_rgb(var(--fm-accent-rgb)/0.9)]';
// everyone else: a dark plate, white type, a hairline edge
const NEUTRAL = 'bg-black/55 text-white shadow-[inset_0_0_0_1px_rgba(255,255,255,0.07)]';

function Measuring({ size, className }: { size: TopBadgeProps['size']; className?: string }) {
  return (
    <span
      className={cn(
        'inline-flex shrink-0 items-center whitespace-nowrap leading-none',
        NEUTRAL,
        'text-white/65',
        size === 'sm' && 'rounded-[6px] px-1.5 py-[5px] text-[10px] font-bold',
        size === 'md' && 'rounded-[10px] px-2 py-[7px] text-[12px] font-bold',
        size === 'lg' && 'rounded-[14px] px-3 py-3 text-[16px] font-black tracking-[-0.04em]',
        size === 'xl' && 'rounded-[18px] px-3.5 py-3.5 text-[18px] font-black tracking-[-0.04em]',
        className,
      )}
    >
      Measuring
    </span>
  );
}

function TopBadge({ topPercent, checkpoint, size, className }: TopBadgeProps) {
  if (topPercent == null || !Number.isFinite(topPercent)) return <Measuring size={size} className={className} />;
  const winner = isWinner(topPercent);
  // "4%" → "4": the sign is drawn smaller beside it
  const value = formatTopPercent(topPercent).slice(0, -1);

  if (size === 'sm' || size === 'md') {
    const sm = size === 'sm';
    return (
      <span
        className={cn(
          'inline-flex shrink-0 items-baseline whitespace-nowrap font-black leading-none',
          sm ? 'gap-[3px] rounded-[6px] px-1.5 py-1' : 'gap-1 rounded-[10px] px-2 py-[5px]',
          winner ? WINNER : NEUTRAL,
          className,
        )}
      >
        <span className={cn('uppercase tracking-[0.14em] opacity-80', sm ? 'text-[8px]' : 'text-[10px]')}>Top</span>
        <span className={cn('tabular-nums tracking-[-0.04em]', sm ? 'text-[12px]' : 'text-[16px]')}>
          {value}
          <span className={cn('ml-px tracking-normal opacity-80', sm ? 'text-[10px]' : 'text-[12px]')}>%</span>
        </span>
      </span>
    );
  }

  const xl = size === 'xl';
  return (
    <span
      className={cn(
        'inline-flex shrink-0 flex-col whitespace-nowrap',
        xl ? 'gap-2 rounded-[18px] px-3.5 pb-3 pt-2.5' : 'gap-1.5 rounded-[14px] px-3 pb-2.5 pt-2',
        winner ? WINNER_TICKET : NEUTRAL,
        className,
      )}
    >
      <span className="flex items-center justify-between gap-4 text-[10px] font-black uppercase leading-none tracking-[0.14em] opacity-80">
        <span>Top</span>
        {checkpoint ? <span>at {checkpoint}</span> : null}
      </span>
      <span className={cn('flex items-baseline font-black leading-[0.8] tracking-[-0.04em] tabular-nums', xl ? 'text-[48px]' : 'text-[28px]')}>
        {value}
        <span className={cn('ml-0.5 tracking-normal opacity-80', xl ? 'text-[22px]' : 'text-[16px]')}>%</span>
      </span>
    </span>
  );
}

export default memo(TopBadge);
