'use client';

/* ─────────────────────────────────────────────────────────────
   CHECKPOINT TRAIL — a post's four reads, D1 · D3 · D7 · D21.

   A landed read is filled with its shade (readShade: the colours
   of Read's run boxes, crimson at the top, darkening below the
   middle) and carries its number; the latest one wears a light
   inner ring, the way Read marks the run its move is read from.
   A read still to come is a hollow outline with its label.

   Still: nothing here moves. A parent that wants the trail to
   arrive animates its segments ([data-trail-seg]) itself, once
   (ReelCard does, on the page beat).
   ───────────────────────────────────────────────────────────── */

import { memo, type CSSProperties } from 'react';
import { shade } from '@/components/read/readShade';
import { STREAM_CHECKPOINTS, formatTopPercent, type StreamCheckpoint, type StreamResult } from '@/lib/feedStream/contract';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

export type CheckpointTrailProps = {
  results: StreamResult[];
  latest: StreamCheckpoint | null;
  size: 'sm' | 'md';
};

// a read that landed without a rank (rare): the run boxes' empty fill
const UNRANKED = { c: 'rgb(29, 29, 35)', tx: 'rgba(255, 255, 255, 0.7)', gc: 'transparent', gi: '0.08' };

function CheckpointTrail({ results, latest, size }: CheckpointTrailProps) {
  const md = size === 'md';
  const box = cn(
    'flex min-w-0 flex-1 items-center overflow-hidden whitespace-nowrap leading-none',
    md ? 'h-[30px] gap-1 rounded-[10px] px-2' : 'h-[18px] rounded-[6px] px-1',
  );
  return (
    <span role="list" aria-label="Checkpoints" className={cn('flex w-full min-w-0', md ? 'gap-1.5' : 'gap-1')}>
      {STREAM_CHECKPOINTS.map((checkpoint) => {
        const result = results.find((entry) => entry.checkpoint === checkpoint);

        if (!result) {
          return (
            <span
              key={checkpoint}
              role="listitem"
              data-trail-seg=""
              aria-label={`${checkpoint}, not landed yet`}
              className={cn(
                box,
                'justify-center font-black uppercase tracking-[0.14em] text-[var(--st-text-3)] shadow-[inset_0_0_0_1px_var(--st-line)]',
                md ? 'text-[10px]' : 'text-[8px]',
              )}
            >
              {checkpoint}
            </span>
          );
        }

        const top = result.topPercent;
        const ranked = top != null && Number.isFinite(top);
        const tone = ranked ? shade(top) : UNRANKED;
        const now = checkpoint === latest;
        const style: CSSProperties = {
          background: tone.c,
          color: tone.tx,
          boxShadow: [
            now ? 'inset 0 0 0 1.5px rgba(255, 255, 255, 0.62)' : null,
            `inset 0 1px 0 rgba(255, 255, 255, ${tone.gi})`,
            md && tone.gc !== 'transparent' ? `0 0 18px ${tone.gc}` : null,
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
              <span className={cn('text-[10px] font-black uppercase tracking-[0.14em]', now ? 'opacity-90' : 'opacity-60')}>{checkpoint}</span>
            ) : null}
            <span className={cn('font-black tabular-nums tracking-[-0.04em]', md ? 'text-[14px]' : 'text-[10px]')}>
              {value}
              {ranked ? <span className={cn('ml-px tracking-normal opacity-75', md ? 'text-[10px]' : 'text-[8px]')}>%</span> : null}
            </span>
          </span>
        );
      })}
    </span>
  );
}

export default memo(CheckpointTrail);
