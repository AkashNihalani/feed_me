/* ─────────────────────────────────────────────────────────────
   LEAD DATA — what Lead's board is drawn from: the feeds, every
   feed's posts over twice the longest range, and the activity of
   that range. Loaded once and shared, so it can be warmed before
   Lead is ever opened (the launch, a quiet moment after the first
   screen: lib/launch) and Lead opens on it at once.

   The last load is kept on the device (keepStore) and is what a
   later launch opens Lead on, marked kept: Lead shows it at once
   and loads afresh behind it.
   ───────────────────────────────────────────────────────────── */

import type { LeadActivity } from '@/app/api/lead/activity/route';
import { onForget, readKeptValue, writeKeptValue } from '@/lib/keepStore';

// the longest range Lead offers: its activity (and the range before it) covers every shorter one
export const LEAD_SPAN_DAYS = 90;
const KEEP_KEY = 'lead:v1';

// a feed as /api/feed has it, with its posts (Lead types them)
export type LeadFeedLoad = { id: string; feeders?: unknown; posts: unknown[] } & Record<string, unknown>;
// at: when it was loaded; kept: read back from the device (a launch's: shown, then loaded afresh)
export type LeadLoad = { feeds: LeadFeedLoad[]; activity: LeadActivity | null; at: number; kept?: boolean };

let latest: LeadLoad | null = null;
let loading: Promise<LeadLoad> | null = null;
let keptRead: Promise<LeadLoad | null> | null = null;
// the account's generation: a load begun before a sign-out never lands after it (on hand, or kept)
let generation = 0;
// what the device kept is waited for this long at most (an IndexedDB that never answers is a load from the network)
const KEPT_WAIT_MS = 600;

onForget(() => {
  generation += 1;
  latest = null;
  loading = null;
  keptRead = null;
});

// a kept load in the shape this build draws (an older one's is let go)
function sound(kept: unknown): kept is LeadLoad {
  if (!kept || typeof kept !== 'object') return false;
  const load = kept as Partial<LeadLoad>;
  if (typeof load.at !== 'number' || !Array.isArray(load.feeds)) return false;
  if (load.activity != null && !Array.isArray((load.activity as { posts?: unknown }).posts)) return false;
  return load.feeds.every((feed) => Boolean(feed) && typeof feed.id === 'string' && Array.isArray(feed.posts));
}

// what is on hand now (this visit's load, or what the device kept), or null
export function leadOnHand(): LeadLoad | null {
  return latest;
}

export class LeadLoadError extends Error {
  constructor(message: string, readonly status: number) {
    super(message);
  }
}

// the board's data, loaded afresh (one load at a time, shared by whoever asks)
export function loadLead(): Promise<LeadLoad> {
  if (loading) return loading;
  const born = generation;
  const load = (async () => {
    const feedResponse = await fetch('/api/feed', { cache: 'no-store', credentials: 'include' });
    if (!feedResponse.ok) throw new LeadLoadError(feedResponse.status === 401 ? 'Sign in to load this board.' : 'Feed data could not be loaded.', feedResponse.status);
    const feedPayload = await feedResponse.json() as { feeds?: Array<Record<string, unknown> & { id?: string }> };
    const apiFeeds = (feedPayload.feeds || []).filter((feed): feed is Record<string, unknown> & { id: string } => Boolean(feed.id));
    const activityRequest = fetch(`/api/lead/activity?days=${LEAD_SPAN_DAYS}`, { cache: 'no-store', credentials: 'include' })
      .then(async (response) => (response.ok ? await response.json() as LeadActivity : null))
      .catch(() => null);
    const feeds = await Promise.all(apiFeeds.map(async (feed) => {
      const params = new URLSearchParams({ feedId: feed.id, handle: 'all', timeframe: `${LEAD_SPAN_DAYS * 2}D` });
      const postsResponse = await fetch(`/api/feed/feeder-posts?${params.toString()}`, { cache: 'no-store', credentials: 'include' });
      const postsPayload = postsResponse.ok ? await postsResponse.json() as { posts?: unknown[] } : {};
      return { ...feed, posts: Array.isArray(postsPayload.posts) ? postsPayload.posts : [] } as LeadFeedLoad;
    }));
    const activity = await activityRequest;
    const fresh: LeadLoad = { feeds, activity, at: Date.now() };
    // (signed out meanwhile: another account's board is never kept or shown)
    if (born !== generation) throw new LeadLoadError('Signed out.', 401);
    latest = fresh;
    // kept for the next launch, once the page is quiet
    const keep = () => {
      if (born === generation) void writeKeptValue(KEEP_KEY, fresh);
    };
    if (typeof window !== 'undefined' && typeof window.requestIdleCallback === 'function') window.requestIdleCallback(keep, { timeout: 4000 });
    else window.setTimeout(keep, 1200);
    return fresh;
  })();
  loading = load;
  const done = () => {
    if (loading === load) loading = null;
  };
  load.then(done, done);
  return load;
}

// what the device kept from the last load (read once), on hand from then on unless a fresher load is
export function keptLead(): Promise<LeadLoad | null> {
  if (latest) return Promise.resolve(latest);
  if (!keptRead) {
    const born = generation;
    const read = readKeptValue<unknown>(KEEP_KEY).then((kept) => {
      if (born !== generation) return null;
      if (!sound(kept)) {
        if (kept != null) void writeKeptValue(KEEP_KEY, null);
        return null;
      }
      const shown: LeadLoad = { ...kept, kept: true };
      if (!latest) latest = shown;
      return latest;
    });
    keptRead = Promise.race([read, new Promise<null>((resolve) => window.setTimeout(() => resolve(null), KEPT_WAIT_MS))]);
  }
  return keptRead;
}
