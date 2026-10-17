/* ─────────────────────────────────────────────────────────────
   FRESH — the feeders with a post in the last 24 hours, across
   every feed (the server's rolling day: the every-feed index's
   freshHandles). What the rails mark with the red story ring, on
   every tab, so red on a rail always means something new and
   never a pick (a pick is ink).

   Read from what is in memory while a rail is drawn (never a
   fetch mid-render), and followed: a rail drawn before the index
   is in picks its rings up the moment it lands. The rail re-renders
   only when the set changes (one identity per list), never for the
   stream's other news. Asked for once, after the rail is on screen.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useSyncExternalStore } from 'react';
import { normalizeHandle } from '@/lib/feedLabels';
import type { StreamScope } from './contract';
import { loadedStreamIndex, peekStreamIndex, subscribeStream } from './store';

const EVERY_FEED: StreamScope = { feedId: null, handle: null };
const NONE: ReadonlySet<string> = new Set();

let seen: readonly string[] | null = null;
let set: ReadonlySet<string> = NONE;

export function freshHandlesNow(): ReadonlySet<string> {
  const handles = loadedStreamIndex(EVERY_FEED)?.freshHandles ?? null;
  if (handles !== seen) {
    seen = handles;
    set = handles ? new Set(handles.map((handle) => normalizeHandle(handle))) : NONE;
  }
  return set;
}

const noneOnServer = () => NONE;

// the set, followed (the rails: TabHeader, SideRail)
export function useFreshHandles(): ReadonlySet<string> {
  const fresh = useSyncExternalStore(subscribeStream, freshHandlesNow, noneOnServer);
  useEffect(() => {
    peekStreamIndex(EVERY_FEED);
  }, []);
  return fresh;
}
