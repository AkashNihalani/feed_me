'use client';

/* ─────────────────────────────────────────────────────────────
   SECTION HEADER — the break between groups in the grids (a day
   at WEEK, a week at MONTH): a hairline rule (not over the first
   group), a rose kicker ("FRIDAY", "THIS WEEK") over the group in
   big bold type ("2 October", "29 Sep – 5 Oct"), and on the right
   how many it holds and, once one is measured, its best number in
   rose. It sits low in its box, close to the posts it heads.
   ───────────────────────────────────────────────────────────── */

import { memo } from 'react';
import { formatTopPercent, type SectionHeaderProps } from '@/lib/feedStream/contract';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

function SectionHeader({ label, kicker, title, first, count, best, order, width, height }: SectionHeaderProps) {
  const noun = order === 'results' ? 'result' : 'post';
  const counted = `${count} ${noun}${count === 1 ? '' : 's'}`;
  // the taller header is a wide screen's (layout: sectionHeaderHeight)
  const wide = height >= 90;
  const ranked = best != null && Number.isFinite(best);
  return (
    <div data-clip="" className="relative flex items-end justify-between gap-4 overflow-hidden px-[var(--st-inset)] pb-3.5" style={{ width, height }}>
      {first ? null : <span aria-hidden="true" className="absolute inset-x-[var(--st-inset)] top-2 h-px bg-[var(--st-line)]" />}
      <h2 aria-label={`${label}, ${counted}`} className="min-w-0">
        <span className="block text-[12px] font-black uppercase leading-none tracking-[0.22em] text-[var(--fm-accent-text)]">{kicker}</span>
        <span className={cn('mt-2 block truncate font-black leading-[0.9] tracking-[-0.04em] text-[var(--st-text)]', wide ? 'text-[40px]' : 'text-[28px]')}>
          {title}
        </span>
      </h2>
      <div aria-hidden="true" className="flex shrink-0 flex-col items-end gap-1.5 pb-0.5">
        {ranked ? (
          <span className="inline-flex items-baseline gap-1 rounded-full bg-[rgb(var(--fm-accent-rgb)/0.14)] px-2.5 py-1 font-black leading-none text-[var(--fm-accent-text)]">
            <span className="text-[10px] uppercase tracking-[0.14em]">Best</span>
            <span className={cn('tabular-nums tracking-[-0.03em]', wide ? 'text-[16px]' : 'text-[14px]')}>{formatTopPercent(best)}</span>
          </span>
        ) : null}
        <span className={cn('font-medium leading-none tabular-nums text-[var(--st-text-2)]', wide ? 'text-[14px]' : 'text-[12px]')}>{counted}</span>
      </div>
    </div>
  );
}

export default memo(SectionHeader);
