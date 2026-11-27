'use client';

/* ─────────────────────────────────────────────────────────────
   CHECKPOINT STRIP — a post's four reads as one small control,
   beside the number they belong to (a closed card): D1 · D3 · D7
   · D21. The one the card shows wears the glide, in the accent
   when the post was a winner there (the top 10%); tap another, or
   slide along the strip, and the glide travels to it as the
   number rolls. A read still to come, or missed, is there but
   faint: a tap on it, or on the one that is on, opens the card's
   details, where all four are told. With none landed yet
   (progress), D1's glide fills as the day to it goes, breathing.

   LineSwap is the line beside it: it rolls over when it changes.
   ───────────────────────────────────────────────────────────── */

import { memo, useState, type ReactNode } from 'react';
import { useScrub } from '@/components/feed/stream/checkpointPick';
import { STREAM_CHECKPOINTS, isWinner, type StreamCheckpoint, type StreamPost } from '@/lib/feedStream/contract';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

// a segment's width (the strip is four of them and its rim)
const SEGMENT = 38;

function CheckpointStrip({ post, at, topPercent, onPick, onOpen, progress }: {
  post: StreamPost;
  // the read the card shows, and its rank
  at: StreamCheckpoint | null;
  topPercent: number | null;
  onPick: (checkpoint: StreamCheckpoint) => void;
  onOpen: () => void;
  // no read landed yet: how far the day to D1 has gone (0–1)
  progress?: number;
}) {
  const scrub = useScrub(post.results, at, onPick);
  const index = Math.max(0, at ? STREAM_CHECKPOINTS.indexOf(at) : 0);
  return (
    <span
      role="radiogroup"
      aria-label="Checkpoints"
      {...scrub.box}
      className="st-strip pointer-events-auto relative inline-flex shrink-0 touch-pan-y select-none rounded-full bg-white/[0.09] p-[3px]"
    >
      <span aria-hidden="true" className="st-glide absolute left-[3px] top-[3px] h-8 rounded-full" style={{ width: SEGMENT, transform: `translate3d(${index * SEGMENT}px, 0px, 0px)` }}>
        {progress == null ? (
          <span className="absolute inset-0 rounded-full bg-white/[0.2]" />
        ) : (
          <>
            <span className="absolute inset-0 rounded-full bg-white/[0.1]" />
            <span className="st-wait-fill absolute inset-0 rounded-full bg-white/[0.32]" style={{ clipPath: `inset(0px ${((1 - progress) * 100).toFixed(1)}% 0px 0px round 999px)` }} />
          </>
        )}
        <span className={cn('st-glide-win absolute inset-0 rounded-full bg-[var(--fm-accent)]', isWinner(topPercent) ? 'opacity-100' : 'opacity-0')} />
      </span>
      {STREAM_CHECKPOINTS.map((checkpoint) => {
        const landed = scrub.landed(checkpoint);
        const on = checkpoint === at;
        return (
          <button
            key={checkpoint}
            type="button"
            role="radio"
            aria-checked={on}
            aria-label={landed ? `Show ${checkpoint}` : `${checkpoint}, no read`}
            aria-disabled={!landed}
            onClick={() => (landed && !on ? scrub.pick(checkpoint) : onOpen())}
            // (taller to the finger than it looks)
            className={cn(
              'st-seg relative grid h-8 place-items-center text-[13px] font-bold leading-none tracking-[-0.01em] outline-none before:absolute before:inset-x-0 before:-inset-y-2 before:content-[""] [-webkit-tap-highlight-color:transparent] focus-visible:rounded-full focus-visible:ring-2 focus-visible:ring-white/80',
              on ? 'text-white' : landed ? 'text-white/65' : 'text-white/25',
            )}
            style={{ width: SEGMENT }}
          >
            {checkpoint}
          </button>
        );
      })}
    </span>
  );
}

export default memo(CheckpointStrip);

/* One line that rolls over when it changes (id): the old one up and away as the new one comes up under it, in one place */
export function LineSwap({ id, children, className }: { id: string; children: ReactNode; className?: string }) {
  const [shown, setShown] = useState<{ id: string; node: ReactNode; previous: ReactNode; turn: number }>({ id, node: children, previous: null, turn: 0 });
  if (shown.id !== id) setShown({ id, node: children, previous: shown.node, turn: shown.turn + 1 });
  return (
    <span className={cn('relative grid min-w-0', className)}>
      {shown.previous !== null ? (
        <span
          key={`out:${shown.turn}`}
          aria-hidden="true"
          className="st-swap-out col-start-1 row-start-1 block min-w-0 truncate"
          onAnimationEnd={() => setShown((now) => (now.turn === shown.turn ? { ...now, previous: null } : now))}
        >
          {shown.previous}
        </span>
      ) : null}
      <span key={`in:${shown.turn}`} className={cn('col-start-1 row-start-1 block min-w-0 truncate', shown.turn ? 'st-swap-in' : null)}>{children}</span>
    </span>
  );
}
