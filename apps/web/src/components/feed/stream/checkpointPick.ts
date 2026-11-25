/* ─────────────────────────────────────────────────────────────
   CHECKPOINT PICK — a post as it was at one of its four reads.

   A card (and a row's reading) shows its latest read; picking an
   earlier one shows the post as it stood then: its rank, and what
   it had. Here is what each pick reads, and how it moved since
   the read before it; and the scrub: a finger slid along a set of
   checkpoints picks each it comes to, the ones that landed only.
   ───────────────────────────────────────────────────────────── */

import { useRef, type MouseEvent as ReactMouseEvent, type PointerEvent as ReactPointerEvent } from 'react';
import { STREAM_CHECKPOINTS, dayLabel, formatCount, formatTopPercent, type StreamCheckpoint, type StreamPost, type StreamResult } from '@/lib/feedStream/contract';
import { playHaptic } from '@/lib/haptics';

export type PostRead = {
  checkpoint: StreamCheckpoint | null;
  // its place among the four (D1: 0)
  order: number;
  topPercent: number | null;
  likes: number | null;
  comments: number | null;
  engagementRate: number | null;
  multiple: number | null;
  // "24 Nov": when it landed
  day: string | null;
  // the ranked read before it (null: the first)
  before: StreamResult | null;
  // the one a card shows unless another is picked
  latest: boolean;
};

const ranked = (value: number | null | undefined): value is number => value != null && Number.isFinite(value);

// the post at a pick (null: its latest read)
export function readAt(post: StreamPost, pick: StreamCheckpoint | null): PostRead {
  const checkpoint = pick && post.results.some((result) => result.checkpoint === pick) ? pick : post.latestCheckpoint;
  const index = post.results.findIndex((result) => result.checkpoint === checkpoint);
  const result = index >= 0 ? post.results[index] : null;
  const latest = checkpoint === post.latestCheckpoint;
  const label = result?.day ? dayLabel(result.day) : null;
  return {
    checkpoint,
    order: checkpoint ? STREAM_CHECKPOINTS.indexOf(checkpoint) : 0,
    topPercent: latest ? post.topPercent : result?.topPercent ?? null,
    likes: latest ? post.likes : result?.likes ?? null,
    comments: latest ? post.comments : result?.comments ?? null,
    engagementRate: latest ? post.engagementRate : result?.engagementRate ?? null,
    multiple: latest ? post.multiple : result?.multiple ?? null,
    day: label ? `${label.date} ${label.month}` : null,
    before: index > 0 ? post.results.slice(0, index).reverse().find((earlier) => ranked(earlier.topPercent)) ?? null : null,
    latest,
  };
}

// how its rank moved since the read before it (a lower Top % is a climb)
export type ReadMove = { way: 'up' | 'down' | 'same' | 'first'; text: string };
export function moveOf(read: PostRead): ReadMove | null {
  if (!ranked(read.topPercent)) return null;
  const { before } = read;
  if (!before || !ranked(before.topPercent)) return { way: 'first', text: 'First read' };
  const from = formatTopPercent(before.topPercent);
  if (from === formatTopPercent(read.topPercent)) return { way: 'same', text: `Same as ${before.checkpoint}` };
  return { way: read.topPercent < before.topPercent ? 'up' : 'down', text: `from ${from} at ${before.checkpoint}` };
}

// what a count gained since the read before it: "+3K" ("−12" in the rare case it lost some), null when either is missing
export function gainOf(now: number | null, then: number | null | undefined): string | null {
  if (now == null || then == null) return null;
  const by = now - then;
  if (by === 0) return null;
  return `${by > 0 ? '+' : '−'}${formatCount(Math.abs(by))}`;
}

// one line of a read, for a small place: "7.9K likes · 27 comments"
export function readLine(read: PostRead): string | null {
  const parts = [
    read.likes != null ? `${formatCount(read.likes)} ${read.likes === 1 ? 'like' : 'likes'}` : null,
    read.comments != null ? `${formatCount(read.comments)} ${read.comments === 1 ? 'comment' : 'comments'}` : null,
  ].filter(Boolean);
  return parts.length ? parts.join(' · ') : null;
}

/* ── the scrub ── */

// a finger has slid this far along the set, more along it than across it, before it is a scrub and not a tap or a swipe
const SCRUB_PX = 8;

/* A set of checkpoints side by side, each as wide as the next: the handlers its box takes. A slide along it picks each
   landed one it comes to (a tick of haptics on each); a tap is the segment's own. The box is `touch-action: pan-y`: the
   page's swipe passes through it. Nothing on it ever reaches what it sits on (a card's own tap) */
export function useScrub(results: readonly StreamResult[], at: StreamCheckpoint | null, onPick: (checkpoint: StreamCheckpoint) => void) {
  const drag = useRef<{ id: number; x: number; y: number; on: boolean; at: StreamCheckpoint | null } | null>(null);
  const spent = useRef(false);
  const landed = (checkpoint: StreamCheckpoint) => results.some((result) => result.checkpoint === checkpoint && ranked(result.topPercent));
  const pick = (checkpoint: StreamCheckpoint, from: StreamCheckpoint | null = at) => {
    if (checkpoint === from || !landed(checkpoint)) return;
    playHaptic('pick');
    onPick(checkpoint);
  };
  const end = (event: ReactPointerEvent<HTMLElement>) => {
    const held = drag.current;
    if (!held || held.id !== event.pointerId) return;
    spent.current = held.on;
    drag.current = null;
  };
  return {
    landed,
    pick,
    box: {
      onPointerDown: (event: ReactPointerEvent<HTMLElement>) => {
        if (event.button !== 0) return;
        spent.current = false;
        drag.current = { id: event.pointerId, x: event.clientX, y: event.clientY, on: false, at };
      },
      onPointerMove: (event: ReactPointerEvent<HTMLElement>) => {
        const held = drag.current;
        if (!held || held.id !== event.pointerId) return;
        if (!held.on) {
          const along = Math.abs(event.clientX - held.x);
          if (along < SCRUB_PX || along < Math.abs(event.clientY - held.y)) return;
          held.on = true;
          event.currentTarget.setPointerCapture(event.pointerId);
        }
        const rect = event.currentTarget.getBoundingClientRect();
        const under = ((event.clientX - rect.left) / Math.max(1, rect.width)) * STREAM_CHECKPOINTS.length;
        // the landed one nearest the finger
        let best: StreamCheckpoint | null = null;
        let gap = Number.POSITIVE_INFINITY;
        STREAM_CHECKPOINTS.forEach((checkpoint, index) => {
          const away = Math.abs(index + 0.5 - under);
          if (landed(checkpoint) && away < gap) {
            best = checkpoint;
            gap = away;
          }
        });
        if (!best || best === held.at) return;
        pick(best, held.at);
        held.at = best;
      },
      onPointerUp: end,
      onPointerCancel: end,
      // a scrub's own click is spent; any other stops here, the card under the set never hearing it
      onClickCapture: (event: ReactMouseEvent<HTMLElement>) => {
        if (!spent.current) return;
        spent.current = false;
        event.preventDefault();
        event.stopPropagation();
      },
      onClick: (event: ReactMouseEvent<HTMLElement>) => event.stopPropagation(),
    },
  };
}
