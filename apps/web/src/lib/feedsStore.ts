/* ─────────────────────────────────────────────────────────────
   FEEDS STORE — your feeds and their feeders: the /api/feed
   bundle, shared by everything on the new Feed tab (the story
   rail, the scope bar, the manage sheet, a post's feeder).

   It shows the cached bundle at once (the same sessionStorage
   entry and shape the old Feed used, so whoever else reads that
   cache keeps working), fetches once per app load, and again on
   reload(). A small external store: a change re-renders only
   those who read it.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useSyncExternalStore } from 'react';
import { clearCacheByPrefix, readCache, removeCache, setCache } from '@/lib/pageCache';
import type { StreamFeeder } from '@/lib/feedStream/contract';

/* ── types (the bundle /api/feed returns) ──────────────────── */

export type FeedMetrics = { likes: string; comments: string; views: string; postsTracked: string };

export type AppFeeder = {
  handle: string;
  isAnchor: boolean;
  profilePicUrl?: string | null; // the /api/media proxy
  thumbnailUrl?: string | null;
  followerCount?: number | null;
  metrics: FeedMetrics;
};

export type AppFeed = {
  id: string;
  title: string; // as the bundle sends it (upper case)
  feeders: AppFeeder[];
  metrics: FeedMetrics & { percentile?: number | string | null };
  compositePercentile?: number | string | null;
  feedBrief?: Record<string, unknown>;
  feedBriefText?: string;
  contextBrief?: Record<string, unknown>;
  contextBible?: string;
};

export type SlotUsage = {
  used: number;
  handles?: string[];
  limit?: number | null;
  plan?: { price: number; postsCap: number; packPrice: number; packSize: number };
};

type TickerItem = {
  id: string;
  handle: string;
  likesDelta: number;
  commentsDelta: number;
  viewsDelta: number;
  postsDelta: number;
};

type FeedBundleCache = { feeds: AppFeed[]; ticker: TickerItem[]; slots?: SlotUsage };

export type FeedsStatus = 'idle' | 'loading' | 'ready' | 'error';

export type FeedsState = {
  feeds: AppFeed[] | null; // null until the cache or the first fetch has them
  slots: SlotUsage | null;
  status: FeedsStatus;
  reload: () => Promise<void>;
};

/* ── the cache the old Feed tab shares ─────────────────────── */

const FEED_CACHE_KEY = 'feed:bundle:v7';
const DASHBOARD_CACHE_PREFIX = 'feed:dashboard:v12';

/* ── signed out ────────────────────────────────────────────── */

let redirectedToLogin = false;

// off to /login (coming back here after), once per app load however many requests are turned away
export function redirectToLoginOnce() {
  if (redirectedToLogin || typeof window === 'undefined') return;
  if (window.location.pathname.startsWith('/login')) return;
  redirectedToLogin = true;
  window.location.replace(`/login?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`);
}

/* ── the store ─────────────────────────────────────────────── */

let feeds: AppFeed[] | null = null;
let slots: SlotUsage | null = null;
let status: FeedsStatus = 'idle';
let hydrated = false;
let fetchedThisLoad = false;
let inflight: Promise<void> | null = null;
let queuedReload: Promise<void> | null = null;
let snapshot: FeedsState | null = null;
const listeners = new Set<() => void>();

const SERVER_SNAPSHOT: FeedsState = { feeds: null, slots: null, status: 'idle', reload: reloadFeeds };

function emit() {
  snapshot = null;
  listeners.forEach((listener) => listener());
}

function subscribe(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

function hydrate() {
  if (hydrated || typeof window === 'undefined') return;
  hydrated = true;
  const cached = readCache<FeedBundleCache>(FEED_CACHE_KEY)?.data;
  if (!cached || !Array.isArray(cached.feeds)) return;
  feeds = cached.feeds;
  slots = cached.slots ?? null;
  status = 'ready';
}

function getSnapshot(): FeedsState {
  hydrate();
  if (!snapshot) snapshot = { feeds, slots, status, reload: reloadFeeds };
  return snapshot;
}

function getServerSnapshot(): FeedsState {
  return SERVER_SNAPSHOT;
}

function sameJson(left: unknown, right: unknown) {
  try {
    return JSON.stringify(left) === JSON.stringify(right);
  } catch {
    return false;
  }
}

// fresh: skip the browser's HTTP cache (the bundle is sent with max-age=30), so a reload after a change is current
function load(fresh: boolean): Promise<void> {
  if (typeof window === 'undefined') return Promise.resolve();
  fetchedThisLoad = true;
  if (inflight) return inflight;
  hydrate();

  const request = Promise.resolve().then(async () => {
    try {
      const response = await fetch('/api/feed', fresh ? { cache: 'no-store' } : undefined);
      const json = (await response.json().catch(() => ({}))) as {
        feeds?: unknown;
        ticker?: unknown;
        slots?: unknown;
        error?: string;
      };
      if (!response.ok) {
        if (response.status === 401) {
          removeCache(FEED_CACHE_KEY);
          clearCacheByPrefix(DASHBOARD_CACHE_PREFIX);
          feeds = [];
          slots = null;
          status = 'error';
          redirectToLoginOnce();
          return;
        }
        throw new Error(json.error || 'Failed to load feeds');
      }
      const nextFeeds = (Array.isArray(json.feeds) ? json.feeds : []) as AppFeed[];
      const nextTicker = (Array.isArray(json.ticker) ? json.ticker : []) as TickerItem[];
      const nextSlots = (json.slots && typeof json.slots === 'object' ? json.slots : null) as SlotUsage | null;
      // the same feeds keep the same array, so whatever is derived from them stays put
      if (!feeds || !sameJson(feeds, nextFeeds)) feeds = nextFeeds;
      if (nextSlots) slots = nextSlots;
      setCache<FeedBundleCache>(FEED_CACHE_KEY, { feeds: nextFeeds, ticker: nextTicker, slots: nextSlots || undefined });
      status = 'ready';
    } catch {
      // a failed refresh keeps what is on screen
      status = feeds ? 'ready' : 'error';
    } finally {
      inflight = null;
      emit();
    }
  });

  inflight = request;
  if (!feeds) {
    status = 'loading';
    emit();
  }
  return request;
}

/* ── the API ───────────────────────────────────────────────── */

// fetch the bundle again, past the browser cache; a reload asked for while one is running follows it
export function reloadFeeds(): Promise<void> {
  if (!inflight) return load(true);
  if (!queuedReload) {
    queuedReload = inflight.then(() => {
      queuedReload = null;
      return load(true);
    });
  }
  return queuedReload;
}

export function getFeedsSnapshot(): AppFeed[] | null {
  hydrate();
  return feeds;
}

export function useFeeds(): FeedsState {
  const state = useSyncExternalStore(subscribe, getSnapshot, getServerSnapshot);
  useEffect(() => {
    if (!fetchedThisLoad) void load(false);
  }, []);
  return state;
}

/* ── feeders by handle ─────────────────────────────────────── */

const lookups = new WeakMap<AppFeed[], Map<string, StreamFeeder>>();
const EMPTY_LOOKUP = new Map<string, StreamFeeder>();

function normalizeHandle(value: unknown) {
  return String(value ?? '').trim().replace(/^@+/, '').toLowerCase();
}

// every feeder by handle (lowercase, no @), the first feed it sits in when it sits in several. The same map for
// the same feeds array, so it is safe to use in memos and as a prop
export function feederLookup(list: AppFeed[] | null): Map<string, StreamFeeder> {
  if (!list) return EMPTY_LOOKUP;
  const cached = lookups.get(list);
  if (cached) return cached;
  const map = new Map<string, StreamFeeder>();
  for (const feed of list) {
    for (const feeder of feed.feeders || []) {
      const handle = normalizeHandle(feeder.handle);
      if (!handle || map.has(handle)) continue;
      const followers = Number(feeder.followerCount);
      map.set(handle, {
        handle,
        profilePicUrl: feeder.profilePicUrl || null,
        feedId: String(feed.id),
        feedTitle: String(feed.title || ''),
        isAnchor: feeder.isAnchor === true,
        followerCount: feeder.followerCount != null && Number.isFinite(followers) ? followers : null,
      });
    }
  }
  lookups.set(list, map);
  return map;
}
