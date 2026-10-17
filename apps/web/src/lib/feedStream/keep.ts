/* ─────────────────────────────────────────────────────────────
   KEEP — what Feed has loaded, kept on the device between
   launches, so a scope you have seen opens at once next time
   (even after the app is closed): its index and the posts of its
   newest days, at most KEEP_POSTS a scope, the KEEP_SCOPES used
   last. The store shows what is kept the moment a scope is asked
   for and brings it up to date in the background (store.ts).

   It is your data on your device, so it goes when you sign out,
   whenever the sign-in page is shown (another account may sign
   in next), and when the server turns the session away.
   ───────────────────────────────────────────────────────────── */

import type { StreamIndex, StreamPost } from './contract';
import { forgetKept } from '@/lib/keepStore';
import { forgetLaunched } from '@/lib/launch';

const KEEP_PREFIX = 'feed:stream:keep:v1:';
const KEEP_SCOPES = 8;
export const KEEP_POSTS = 120;

export type Kept = { ts: number; index: StreamIndex; days: Array<[string, StreamPost[]]> };

function storage(): Storage | null {
  try {
    return typeof window === 'undefined' ? null : window.localStorage;
  } catch {
    return null;
  }
}

export function readKept(key: string): Kept | null {
  const store = storage();
  if (!store) return null;
  try {
    const raw = store.getItem(KEEP_PREFIX + key);
    if (!raw) return null;
    const kept = JSON.parse(raw) as Kept;
    return kept && typeof kept.ts === 'number' && kept.index && Array.isArray(kept.days) ? kept : null;
  } catch {
    return null;
  }
}

// the scopes kept, oldest first (when each was kept is read off the head of its entry: writeKept puts ts first, and
// a whole entry is never parsed just to sort)
function keptKeys(store: Storage): Array<{ key: string; ts: number }> {
  const found: Array<{ key: string; ts: number }> = [];
  for (let at = 0; at < store.length; at += 1) {
    const key = store.key(at);
    if (!key?.startsWith(KEEP_PREFIX)) continue;
    const head = /^\{"ts":(\d+)/.exec((store.getItem(key) ?? '').slice(0, 32));
    found.push({ key, ts: head ? Number(head[1]) : 0 });
  }
  return found.sort((a, b) => a.ts - b.ts);
}

export function writeKept(key: string, kept: Kept) {
  const store = storage();
  if (!store) return;
  const raw = JSON.stringify(kept);
  const put = () => store.setItem(KEEP_PREFIX + key, raw);
  try {
    put();
  } catch {
    // full: the oldest scopes make room, then try once more (or keep nothing for this one)
    try {
      keptKeys(store).slice(0, Math.max(1, Math.floor(KEEP_SCOPES / 2))).forEach(({ key: old }) => store.removeItem(old));
      put();
    } catch {
      return;
    }
  }
  // only the scopes used last
  try {
    const all = keptKeys(store);
    all.slice(0, Math.max(0, all.length - KEEP_SCOPES)).forEach(({ key: old }) => store.removeItem(old));
  } catch {
    // storage unavailable
  }
}

// forget what is kept: every scope, or those whose key starts with `scopePrefix` (a feed's, the all-feeds ones). All of
// it takes the other tabs' kept data too (Lead's board: keepStore)
export function clearKept(scopePrefix = '') {
  if (!scopePrefix) {
    forgetKept();
    forgetLaunched();
  }
  const store = storage();
  if (!store) return;
  try {
    for (let at = store.length - 1; at >= 0; at -= 1) {
      const key = store.key(at);
      if (key?.startsWith(KEEP_PREFIX + scopePrefix)) store.removeItem(key);
    }
  } catch {
    // storage unavailable
  }
}
