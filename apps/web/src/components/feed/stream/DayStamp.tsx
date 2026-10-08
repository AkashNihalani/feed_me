'use client';

/* ─────────────────────────────────────────────────────────────
   DAY STAMP — the Day view's date on a wide screen: big, bold and
   still, in the margin beside the posts, saying which day the post
   in view went up (or got its result, in results order).

     FRIDAY          the rose kicker (TODAY / YESTERDAY / weekday)
     2               the date, set as a hero numeral
     October         the month
     ──              a short rose rule
     Post 3 of 12    where this post sits in its day

   It follows the scroll, not the rest: crossing into another day,
   each line that changes swaps (the old one rises away as the new
   one rises in, overlapping, never masked, so a big numeral is
   never cut mid-change), the date a beat after the weekday. Over
   the profile card and while a zoom flies it slides away past the
   screen's left edge, and back in after.

   It fills the box it is given; FeedStream pins that box beside
   the card band with position: sticky inside the page's layer (a
   fixed box would be caught by the tab host's will-change).
   ───────────────────────────────────────────────────────────── */

import { memo, useState, type CSSProperties } from 'react';
import { groupParts, istDay } from '@/lib/feedStream/contract';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

export type DayStampProps = {
  day: string | null; // the IST day of the post in view; null: nothing to stamp (the profile card)
  slot: number; // its place in the day
  count: number; // how many the day holds
  hidden: boolean; // a zoom in flight
  width: number; // the margin's width: the numeral is sized to it
};

/* One line that swaps when its value changes: the old copy rises away while the new one rises in, both in one grid
   cell (no clip, no mask). Transform and opacity only (stream.css, .st-swap-in / .st-swap-out) */
function Swap({ value, step = 0, className, style }: { value: string; step?: number; className?: string; style?: CSSProperties }) {
  const [shown, setShown] = useState<{ current: string; previous: string | null; turn: number }>({ current: value, previous: null, turn: 0 });
  if (shown.current !== value) setShown({ current: value, previous: shown.current, turn: shown.turn + 1 });
  const delay = { '--st-swap-delay': `${step * 50}ms` } as CSSProperties;
  return (
    <span className={cn('relative inline-grid justify-items-end', className)} style={style}>
      {shown.previous !== null ? (
        <span
          key={`out:${shown.turn}`}
          aria-hidden="true"
          className="st-swap-out col-start-1 row-start-1 whitespace-pre"
          style={delay}
          onAnimationEnd={() => setShown((now) => (now.turn === shown.turn ? { ...now, previous: null } : now))}
        >
          {shown.previous}
        </span>
      ) : null}
      <span key={`in:${shown.turn}`} className={cn('col-start-1 row-start-1 whitespace-pre', shown.turn ? 'st-swap-in' : null)} style={delay}>
        {shown.current}
      </span>
    </span>
  );
}

function DayStamp({ day, slot, count, hidden, width }: DayStampProps) {
  const parts = day ? groupParts(day, 'day', istDay()) : null;
  const [date = '', month = ''] = parts ? parts.title.split(' ') : [];
  // the numeral fills the margin, held to a hero range
  const numeral = Math.round(Math.min(152, Math.max(80, width * 0.62)));
  return (
    <div
      aria-hidden="true"
      data-shown={parts && !hidden ? '' : undefined}
      className="st-stamp pointer-events-none flex h-full w-full flex-col items-end justify-center text-right"
    >
      <Swap value={(parts?.kicker ?? '').toUpperCase()} className="text-[13px] font-black leading-none tracking-[0.22em] text-[var(--fm-accent-text)]" />
      {/* the numeral: a full line box (leading 1) so no glyph ever reaches past it */}
      <Swap value={date} step={1} className="mt-2 font-black leading-none tracking-[-0.06em] tabular-nums text-[var(--st-text)]" style={{ fontSize: numeral }} />
      <Swap value={month} step={2} className="mt-1 text-[22px] font-bold leading-none tracking-[-0.02em] text-[var(--st-text-2)]" />
      <span className="mt-6 block h-[3px] w-9 rounded-full bg-[var(--fm-accent)]" />
      <span className={cn('mt-3 block text-[13px] font-medium leading-none tabular-nums text-[var(--st-text-3)]', count > 1 ? null : 'invisible')}>
        <Swap value={`Post ${slot + 1} of ${count}`} step={3} />
      </span>
    </div>
  );
}

export default memo(DayStamp);
