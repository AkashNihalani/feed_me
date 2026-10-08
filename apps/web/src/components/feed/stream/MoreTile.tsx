'use client';

/* ─────────────────────────────────────────────────────────────
   MORE TILE — the last place in a MONTH group, past its
   highlights: how many of the month's posts aren't shown ("+230"),
   and the way to all of them (a tap opens WEEK there). Quiet: the
   raised surface, the count set big, a rose plus.
   ───────────────────────────────────────────────────────────── */

import { memo } from 'react';
import { ArrowUpRight } from 'lucide-react';
import { press } from '@/components/feed/stream/press';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

function MoreTile({ count, span, width, height, onOpen }: { count: number; span: string; width: number; height: number; onOpen: (element: HTMLElement) => void }) {
  const big = width >= 150;
  return (
    <button
      type="button"
      onClick={(event) => onOpen(event.currentTarget)}
      {...press}
      aria-label={`${count} more ${count === 1 ? 'post' : 'posts'} ${span}. See them all`}
      className="group relative block shrink-0 rounded-[6px] p-0 text-left outline-none [-webkit-tap-highlight-color:transparent] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
      style={{ width, height }}
    >
      <span data-clip="" className="absolute inset-0 flex flex-col justify-between overflow-hidden rounded-[6px] bg-[var(--st-surface-2)] p-3 shadow-[inset_0_0_0_1px_var(--st-line)] transition-transform duration-200 ease-out group-data-[pressed]:scale-[0.97]">
        <ArrowUpRight
          className="h-4 w-4 self-end text-[var(--st-text-3)] transition-transform duration-200 ease-out group-hover:-translate-y-px group-hover:translate-x-px group-hover:text-[var(--st-text-2)]"
          strokeWidth={2.4}
          aria-hidden="true"
        />
        <span className="block">
          <span className={cn('flex items-baseline font-black leading-[0.85] tracking-[-0.04em] tabular-nums text-[var(--st-text)]', big ? 'text-[40px]' : 'text-[28px]')}>
            <span className="mr-0.5 text-[var(--fm-accent-bright)]">+</span>
            {count}
          </span>
          <span className={cn('mt-2 block font-medium leading-tight text-[var(--st-text-2)]', big ? 'text-[13px]' : 'text-[11px]')}>
            more {span}
          </span>
        </span>
      </span>
    </button>
  );
}

export default memo(MoreTile);
