'use client';

/* ─────────────────────────────────────────────────────────────
   CHECKPOINT TRAIL — a post's four reads, D1 · D3 · D7 · D21.

   A landed read is filled with its shade (readShade: the colours
   of Read's run boxes, crimson at the top, darkening below the
   middle) and carries its number; the latest one wears a light
   inner ring, the way Read marks the run its move is read from.
   A read still to come is a hollow outline with its label and when
   it lands ("in 9d", "soon"); one that was missed (a later read
   landed, or its day is past) says so with a dash.

   Sizes: sm a strip of numbers, md a row of labelled boxes, lg a
   small tile per read, its label over its number (a phone card's
   open reading, where the trail is the thing you came for).

   Still: nothing here moves. A parent that wants the trail to
   arrive animates its segments ([data-trail-seg]) itself, once
   (ReelCard does, on the page beat).
   ───────────────────────────────────────────────────────────── */

import { memo, type CSSProperties } from 'react';
import { UNRANKED_SHADE, shade } from '@/components/read/readShade';
import { STREAM_CHECKPOINTS, formatTopPercent, istDay, type StreamCheckpoint, type StreamResult } from '@/lib/feedStream/contract';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

export type CheckpointTrailProps = {
  results: StreamResult[];
  latest: StreamCheckpoint | null;
  size: 'sm' | 'md' | 'lg';
  postedDay?: string | null; // the IST day the post went up (YYYY-MM-DD): says when a read still to come lands
};

// how many days after the post each read is taken
const CHECKPOINT_DAY: Record<StreamCheckpoint, number> = { D1: 1, D3: 3, D7: 7, D21: 21 };

// whole days from one IST day (YYYY-MM-DD) to another
function daysBetween(from: string, to: string) {
  return Math.round((Date.parse(`${to}T00:00:00Z`) - Date.parse(`${from}T00:00:00Z`)) / 86_400_000);
}

function CheckpointTrail({ results, latest, size, postedDay = null }: CheckpointTrailProps) {
  const lg = size === 'lg';
  // labelled (md and lg): each box says which read it is, and when one still to come lands
  const md = size === 'md' || lg;
  const age = postedDay ? daysBetween(postedDay, istDay()) : null;
  const box = cn(
    'flex min-w-0 flex-1 overflow-hidden whitespace-nowrap leading-none',
    lg
      ? 'h-[60px] flex-col items-start rounded-[14px] px-2.5 py-2.5'
      : md ? 'h-[30px] items-center gap-1 rounded-[10px] px-2' : 'h-[18px] items-center rounded-[6px] px-1',
  );
  return (
    <span role="list" aria-label="Checkpoints" className={cn('flex w-full min-w-0', lg ? 'gap-2' : md ? 'gap-1.5' : 'gap-1')}>
      {STREAM_CHECKPOINTS.map((checkpoint) => {
        const result = results.find((entry) => entry.checkpoint === checkpoint);

        if (!result) {
          // missed: a later read landed, or its day is past (a day's grace for the read to land); else still to come
          const later = results.some((entry) => STREAM_CHECKPOINTS.indexOf(entry.checkpoint) > STREAM_CHECKPOINTS.indexOf(checkpoint));
          const due = age == null || !Number.isFinite(age) ? null : CHECKPOINT_DAY[checkpoint] - age;
          const missed = later || (due != null && due < -1);
          const note = missed ? '—' : due == null ? '' : due <= 1 ? 'soon' : `in ${due}d`;
          const label = missed ? `${checkpoint}, no read` : due == null ? `${checkpoint}, not landed yet` : `${checkpoint}, lands ${due <= 1 ? 'soon' : `in ${due} days`}`;
          return (
            <span
              key={checkpoint}
              role="listitem"
              data-trail-seg=""
              aria-label={label}
              className={cn(
                box,
                'text-[var(--st-text-3)] shadow-[inset_0_0_0_1px_var(--st-line)]',
                lg || (md && note) ? 'justify-between' : 'justify-center',
              )}
            >
              <span className={cn('font-black uppercase tracking-[0.14em]', lg ? 'text-[11px]' : 'text-[10px]')}>{checkpoint}</span>
              {md && note ? <span className={cn('tabular-nums', lg ? 'text-[15px] font-bold' : 'text-[12px] font-semibold')}>{note}</span> : null}
            </span>
          );
        }

        const top = result.topPercent;
        const ranked = top != null && Number.isFinite(top);
        const tone = ranked ? shade(top) : UNRANKED_SHADE;
        const now = checkpoint === latest;
        const style: CSSProperties = {
          background: tone.c,
          color: tone.tx,
          boxShadow: [
            now ? 'inset 0 0 0 1.5px rgb(var(--fm-fg-rgb) / 0.62)' : null,
            `inset 0 1px 0 rgba(255, 255, 255, ${tone.gi})`,
            md && tone.gc !== 'transparent' ? `0 0 ${lg ? 22 : 18}px ${tone.gc}` : null,
          ].filter(Boolean).join(', '),
        };
        const value = ranked ? formatTopPercent(top).slice(0, -1) : '—';

        return (
          <span
            key={checkpoint}
            role="listitem"
            data-trail-seg=""
            aria-label={ranked ? `${checkpoint}, top ${value}%` : `${checkpoint}, unranked`}
            className={cn(box, md ? 'justify-between' : 'justify-center')}
            style={style}
          >
            {md ? (
              <span className={cn('font-black uppercase tracking-[0.14em]', lg ? 'text-[11px]' : 'text-[10px]', now ? 'opacity-90' : 'opacity-60')}>{checkpoint}</span>
            ) : null}
            <span className={cn('font-black tabular-nums tracking-[-0.04em]', lg ? 'text-[clamp(17px,5.6vw,22px)]' : md ? 'text-[14px]' : 'text-[10px]')}>
              {value}
              {ranked ? <span className={cn('ml-px tracking-normal opacity-75', lg ? 'text-[0.6em]' : md ? 'text-[10px]' : 'text-[8px]')}>%</span> : null}
            </span>
          </span>
        );
      })}
    </span>
  );
}

export default memo(CheckpointTrail);
