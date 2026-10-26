'use client';

/* ─────────────────────────────────────────────────────────────
   PROFILE DOORS — what the Feed's profile shows of the other two
   tabs, so each is a reason to open it rather than a button:

   - Read: the trajectory Read's board draws (components/read/
     readBoard): runs of 10, each shaded by its typical post, and
     which way the latest run moved. A feeder shows their own runs;
     a feed or all feeds says how the group splits, in the words of
     Read's index, over each feeder's latest run, strongest first.
   - Lead: the first win of Lead's Hall of Feed for the same pick
     and window (components/lead/leadHall), with its post.

   Both come from what the other tabs load anyway and keep for the
   visit (Read's board per feed, Lead's shared load), fetched once
   the feed is quiet, never while it draws its first screen.
   undefined: still coming; null: nothing to show.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useMemo, useState } from 'react';
import { keptLead, leadOnHand, loadLead, type LeadLoad } from '@/components/lead/leadData';
import { hallWins, type HallFact } from '@/components/lead/leadHall';
import { RUN_SLOTS, cachedBoard, loadBoard, summaryOf, type BoardFeeder, type BoardRun } from '@/components/read/readBoard';
import { SAMPLE_FEED_ID, cachedFeeds, feedOf, warmReadFeeds, type RailFeed } from '@/components/read/readFeeds';
import type { StreamScope } from '@/lib/feedStream/contract';

export type ReadDoor =
  | { kind: 'feeder'; row: BoardFeeder }
  | { kind: 'group'; head: string; up: number; down: number; latest: Array<{ handle: string; run: BoardRun }> };

export type Doors = { read: ReadDoor | null | undefined; lead: HallFact | null | undefined };

// after the feed's first screen, in a quiet moment
function whenQuiet(run: () => void) {
  if (typeof window === 'undefined') return () => {};
  if (typeof window.requestIdleCallback === 'function') {
    const id = window.requestIdleCallback(run, { timeout: 2000 });
    return () => window.cancelIdleCallback(id);
  }
  const id = window.setTimeout(run, 600);
  return () => window.clearTimeout(id);
}

// the feeds a pick reads from: the feeder's or the feed's own, or every real one
function feedsFor(feeds: RailFeed[], scope: StreamScope) {
  const real = feeds.filter((feed) => feed.id !== SAMPLE_FEED_ID);
  const id = scope.feedId ?? feedOf(real, scope.handle);
  if (id) return real.filter((feed) => feed.id === id);
  return scope.handle ? [] : real;
}

function readDoorOf(feeds: RailFeed[], scope: StreamScope): ReadDoor | null | undefined {
  const scoped = feedsFor(feeds, scope);
  const boards = scoped.map((feed) => cachedBoard(feed.id));
  if (boards.some((rows) => rows == null)) return undefined;
  const rows = boards.flatMap((list) => list || []);
  if (scope.handle) {
    const row = rows.find((item) => item.handle === scope.handle);
    return row && row.total ? { kind: 'feeder', row } : null;
  }
  if (!rows.length) return null;
  const latest = rows
    .filter((row): row is BoardFeeder & { now: BoardRun & { tp: number } } => row.now?.tp != null)
    .sort((a, b) => a.now.tp - b.now.tp)
    .slice(0, RUN_SLOTS)
    .map((row) => ({ handle: row.handle, run: row.now }));
  const count = (momentum: BoardFeeder['momentum']) => rows.filter((row) => row.momentum === momentum).length;
  return { kind: 'group', head: summaryOf(rows, '').head, up: count('up'), down: count('down'), latest };
}

export function useProfileDoors(scope: StreamScope): Doors {
  const [feeds, setFeeds] = useState<RailFeed[] | null>(() => cachedFeeds());
  const [lead, setLead] = useState<LeadLoad | null>(() => leadOnHand());
  // bumps when a board this pick reads lands
  const [boardsAt, setBoardsAt] = useState(0);

  useEffect(() => {
    let alive = true;
    const stop = whenQuiet(() => {
      if (!feeds) void warmReadFeeds().then((list) => alive && setFeeds(list)).catch(() => {});
      if (!lead) {
        void keptLead()
          .then((kept) => kept ?? loadLead())
          .then((load) => alive && setLead(load))
          .catch(() => alive && setLead(null));
      }
    });
    return () => {
      alive = false;
      stop();
    };
  }, [feeds, lead]);

  const scoped = useMemo(() => (feeds ? feedsFor(feeds, scope) : []), [feeds, scope]);
  useEffect(() => {
    let alive = true;
    const missing = scoped.filter((feed) => !cachedBoard(feed.id));
    if (!missing.length) return undefined;
    const stop = whenQuiet(() => {
      missing.forEach((feed) => {
        void loadBoard(feed).then(() => alive && setBoardsAt((n) => n + 1)).catch(() => {});
      });
    });
    return () => {
      alive = false;
      stop();
    };
  }, [scoped]);

  const read = useMemo(() => (feeds ? readDoorOf(feeds, scope) : undefined), [feeds, scope, boardsAt]); // eslint-disable-line react-hooks/exhaustive-deps
  const win = useMemo(() => {
    if (!lead) return undefined;
    return hallWins(lead, { feedId: scope.feedId, handle: scope.handle })[0] ?? null;
  }, [lead, scope.feedId, scope.handle]);
  return { read, lead: win };
}
