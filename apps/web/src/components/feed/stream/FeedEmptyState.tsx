'use client';

/* ─────────────────────────────────────────────────────────────
   FEED EMPTY STATE — what Feed says when there is nothing to
   scroll: no feeds yet (start one: the button opens the manage
   sheet straight into the Brief), or a scope with nothing posted
   in the 90-day window (add feeders from Manage).

   Calm and centred in whatever box it is given. Its pieces rise
   once on the page beat as they arrive (a CSS animation, played
   again whenever the tab comes back on screen); colours are the
   stream's tokens, with dark fallbacks.
   ───────────────────────────────────────────────────────────── */

import type { CSSProperties } from 'react';
import { CalendarClock, Plus, Users } from 'lucide-react';
import { useAppHaptics } from '@/lib/haptics';
import { BEAT_ARRIVE_S, BEAT_EASE_CSS, beatDelay } from '@/lib/motion';
import { STREAM_WINDOW_DAYS } from '@/lib/feedStream/contract';
import { cn } from '@/lib/utils';

export type FeedEmptyStateProps = {
  kind: 'no-feeds' | 'no-posts';
  onCreateFeed: () => void;
};

const TOKENS = {
  '--es-text': 'var(--st-text, #ffffff)',
  '--es-text-2': 'var(--st-text-2, rgba(255, 255, 255, 0.65))',
  '--es-line': 'var(--st-line, rgba(255, 255, 255, 0.08))',
  '--es-surface-2': 'var(--st-surface-2, #0e0e10)',
} as CSSProperties;

// the theme's `enter` rise (opacity + transform), retimed to the page beat and held hidden until its step
const ARRIVE = 'animate-enter motion-reduce:animate-none';
function arrive(step: number): CSSProperties {
  return {
    animationDuration: `${BEAT_ARRIVE_S}s`,
    animationTimingFunction: BEAT_EASE_CSS,
    animationDelay: `${beatDelay(step)}s`,
    animationFillMode: 'both',
  };
}

export function FeedEmptyState({ kind, onCreateFeed }: FeedEmptyStateProps) {
  const { play } = useAppHaptics();
  const noFeeds = kind === 'no-feeds';
  const Icon = noFeeds ? Users : CalendarClock;

  return (
    <div
      className="flex h-full min-h-[320px] w-full flex-col items-center justify-center px-6 py-16 text-center"
      style={TOKENS}
    >
      <span
        aria-hidden="true"
        className={cn(ARRIVE, 'grid h-14 w-14 place-items-center rounded-[18px] border border-(--es-line) bg-(--es-surface-2) text-(--es-text-2)')}
        style={arrive(0)}
      >
        <Icon size={24} strokeWidth={2.2} />
      </span>
      <h2
        className={cn(ARRIVE, 'mt-5 max-w-[320px] text-[22px] font-black leading-[1.1] tracking-[-0.04em] text-(--es-text) lg:text-[28px]')}
        style={arrive(1)}
      >
        {noFeeds ? 'Start your first feed' : `Nothing posted in the last ${STREAM_WINDOW_DAYS} days`}
      </h2>
      <p
        className={cn(ARRIVE, 'mt-2 max-w-[300px] text-[14px] font-medium leading-5 text-(--es-text-2)')}
        style={arrive(2)}
      >
        {noFeeds
          ? 'Group the accounts you watch. Every post they make lands here, ranked against their own usual.'
          : 'Add feeders from Manage, and their new posts land here as they go up.'}
      </p>
      {noFeeds ? (
        <button
          type="button"
          onClick={() => {
            play('snapLock');
            onCreateFeed();
          }}
          className={cn(
            ARRIVE,
            'mt-6 inline-flex h-12 items-center gap-2 rounded-[14px] bg-(--fm-accent) px-5 text-[14px] font-semibold text-white outline-none',
            'shadow-[0_14px_32px_-16px_rgb(var(--fm-accent-rgb)/0.7)] transition-[scale] duration-150 ease-out active:scale-[0.97]',
            'focus-visible:ring-2 focus-visible:ring-white/70 [-webkit-tap-highlight-color:transparent]',
          )}
          style={arrive(3)}
        >
          <Plus size={16} strokeWidth={2.8} aria-hidden="true" />
          New feed
        </button>
      ) : null}
    </div>
  );
}
