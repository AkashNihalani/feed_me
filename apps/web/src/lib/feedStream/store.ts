/* ─────────────────────────────────────────────────────────────
   FEED STREAM STORE — the client half of the stream: per scope
   (feed · feeder · order) the day index, then the posts of the
   days on screen, loaded on demand.

   Index: shown at once from the session cache, refreshed in the
   background on use once it is 5 minutes old, one request at a
   time per scope.
   Days: kept in memory only. Missing days go out as contiguous
   ranges sized from the index counts (about 90 posts, at most 31
   days a request, growing from the day asked for towards older
   days first), at most two requests at once across the app, the
   days on screen first. Memory stays near 3000 posts by letting
   go of the least recently used scopes, then days.

   Snapshots are stable: a day's array (and each post in it) keeps
   its identity until that day's posts really change, and a hook's
   map keeps its identity until one of its days does, so memoized
   pieces re-render only for what moved.
   ───────────────────────────────────────────────────────────── */

import { useCallback, useEffect, useMemo, useRef, useSyncExternalStore } from 'react';
import { clearCacheByPrefix, readCache, setCache } from '@/lib/pageCache';
import { redirectToLoginOnce } from '@/lib/feedsStore';
import {
  STREAM_WINDOW_DAYS,
  istDay,
  orderDayPosts,
  scopeKey,
  scopeQuery,
  sectionDayOf,
  shiftDay,
  type FeedOrder,
  type StreamIndex,
  type StreamPage,
  type StreamPost,
  type StreamScope,
} from './contract';

/* ── types ─────────────────────────────────────────────────── */

export type StreamIndexStatus = 'idle' | 'loading' | 'ready' | 'error' | 'unauthorized';

export type StreamIndexState = {
  index: StreamIndex | null;
  status: StreamIndexStatus;
  error: string | null;
  refresh: () => void;
};

type NormalizedScope = { feedId: string | null; handle: string | null; order: FeedOrder };

type DayPage = {
  posts: StreamPost[]; // ordered with orderDayPosts
  fetchedAt: number;
  stale: boolean; // the index says the day changed: refetch when next asked for
  usedAt: number;
};

type ScopeState = {
  key: string;
  scope: NormalizedScope;
  generation: number; // bumps on invalidate: answers to older requests are dropped
  usedAt: number;

  index: StreamIndex | null;
  indexDays: Map<string, number>; // day → its position in index.days
  indexAt: number;
  indexError: string | null;
  indexErrorAt: number;
  indexInflight: Promise<void> | null;
  refreshQueued: boolean;
  unauthorized: boolean;
  indexWatchers: number;
  snapshot: StreamIndexState | null;
  refresh: () => void;

  pages: Map<string, DayPage>;
  postCount: number;
  version: number; // bumps whenever a day's posts change
  inflight: Set<string>;
  interest: Map<string, number>; // days on screen → how many mounted hooks show them
  hints: Map<string, number>; // prefetched days → until when the hint counts
  wanted: Map<string, number>; // day → when it was last asked for (a sequence; the latest goes first)
  failures: number;
  retryAt: number;
  retryTimer: ReturnType<typeof setTimeout> | null;
  mismatchAt: number;
};

type Batch = { state: ScopeState; days: string[]; from: string; to: string };

/* ── constants ─────────────────────────────────────────────── */

// v2: days carry tops and the index carries the profile (v1 entries are cleared on first use)
const INDEX_CACHE_PREFIX = 'feed:stream:v2:index:';
const RETIRED_CACHE_PREFIX = 'feed:stream:v1:';
const INDEX_FRESH_MS = 5 * 60_000;
const INDEX_RETRY_MS = 5_000;
const PAGE_FRESH_MS = 10 * 60_000;
const BATCH_POSTS = 90;
const BATCH_DAYS = 31;
const MAX_PAGE_REQUESTS = 2;
const MEMORY_CAP_POSTS = 3000;
const HINT_MS = 10_000;
const MISMATCH_REFRESH_MS = 30_000;
const MAX_RETRY_MS = 30_000;
const DAY_MS = 24 * 60 * 60 * 1000;

const EMPTY_POSTS: StreamPost[] = [];
const EMPTY_DAYS: ReadonlyMap<string, StreamPost[]> = new Map();
const SERVER_INDEX_STATE: StreamIndexState = { index: null, status: 'idle', error: null, refresh: () => {} };

/* ── state ─────────────────────────────────────────────────── */

const scopes = new Map<string, ScopeState>();
const scopesByKey = new Map<string, NormalizedScope>();
const listeners = new Set<() => void>();
let emitQueued = false;
let pumpQueued = false;
let activePageRequests = 0;
let wantSequence = 0;
let versionSequence = 0;
let visibilityWatched = false;

/* ── scopes ────────────────────────────────────────────────── */

function normalizeScope(scope: StreamScope): NormalizedScope {
  const feedId = scope.feedId == null ? '' : String(scope.feedId).trim();
  const handle = String(scope.handle ?? '').trim().replace(/^@+/, '').toLowerCase();
  return {
    feedId: feedId || null,
    handle: handle && handle !== 'all' ? handle : null,
    order: scope.order === 'results' ? 'results' : 'posted',
  };
}

// the scope's key, remembering which scope it names
function keyFor(scope: StreamScope): string {
  const normalized = normalizeScope(scope);
  const key = scopeKey(normalized);
  if (!scopesByKey.has(key)) scopesByKey.set(key, normalized);
  return key;
}

function ensureScope(key: string): ScopeState {
  let state = scopes.get(key);
  if (state) return state;
  const scope = scopesByKey.get(key) ?? { feedId: null, handle: null, order: 'posted' };
  const created: ScopeState = {
    key,
    scope,
    generation: 0,
    usedAt: Date.now(),
    index: null,
    indexDays: new Map(),
    indexAt: 0,
    indexError: null,
    indexErrorAt: 0,
    indexInflight: null,
    refreshQueued: false,
    unauthorized: false,
    indexWatchers: 0,
    snapshot: null,
    refresh: () => refreshScope(created),
    pages: new Map(),
    postCount: 0,
    version: ++versionSequence,
    inflight: new Set(),
    interest: new Map(),
    hints: new Map(),
    wanted: new Map(),
    failures: 0,
    retryAt: 0,
    retryTimer: null,
    mismatchAt: 0,
  };
  state = created;
  scopes.set(key, state);
  hydrateIndex(state);
  return state;
}

/* ── notifying ─────────────────────────────────────────────── */

// listeners hear once per tick however many things changed in it
function emit() {
  if (emitQueued) return;
  emitQueued = true;
  queueMicrotask(() => {
    emitQueued = false;
    listeners.forEach((listener) => listener());
  });
}

export function subscribeStream(listener: () => void): () => void {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

/* ── comparing (so unchanged data keeps its identity) ──────── */

function sameValue(left: unknown, right: unknown): boolean {
  if (Object.is(left, right)) return true;
  if (Array.isArray(left) && Array.isArray(right)) {
    if (left.length !== right.length) return false;
    for (let index = 0; index < left.length; index += 1) if (!sameValue(left[index], right[index])) return false;
    return true;
  }
  if (left && right && typeof left === 'object' && typeof right === 'object' && !Array.isArray(left) && !Array.isArray(right)) {
    const leftKeys = Object.keys(left);
    if (leftKeys.length !== Object.keys(right).length) return false;
    for (const key of leftKeys) {
      if (!sameValue((left as Record<string, unknown>)[key], (right as Record<string, unknown>)[key])) return false;
    }
    return true;
  }
  return false;
}

// the new day's posts, reusing every unchanged post object, and the old array itself when nothing changed
function shareDay(previous: StreamPost[] | undefined, next: StreamPost[]): StreamPost[] {
  if (next.length === 0) return previous && previous.length === 0 ? previous : EMPTY_POSTS;
  if (!previous || previous.length === 0) return next;
  const byKey = new Map(previous.map((post) => [post.key, post]));
  let same = previous.length === next.length;
  const shared = next.map((post, index) => {
    const old = byKey.get(post.key);
    const kept = old && sameValue(old, post) ? old : post;
    if (kept !== previous[index]) same = false;
    return kept;
  });
  return same ? previous : shared;
}

// the new index, reusing unchanged days, and the old index itself when nothing changed
function shareIndex(previous: StreamIndex | null, next: StreamIndex): StreamIndex {
  if (!previous) return next;
  const byDay = new Map(previous.days.map((day) => [day.day, day]));
  let same = previous.days.length === next.days.length && previous.total === next.total && previous.scopeKey === next.scopeKey;
  const days = next.days.map((day, index) => {
    const old = byDay.get(day.day);
    const kept = old && sameValue(old, day) ? old : day;
    if (kept !== previous.days[index]) same = false;
    return kept;
  });
  const freshHandles = sameValue(previous.freshHandles, next.freshHandles) ? previous.freshHandles : next.freshHandles;
  const profile = sameValue(previous.profile, next.profile) ? previous.profile : next.profile;
  if (same && freshHandles === previous.freshHandles && profile === previous.profile) return previous;
  return { ...next, days, freshHandles, profile };
}

/* ── the index ─────────────────────────────────────────────── */

function windowStart() {
  return shiftDay(istDay(), -(STREAM_WINDOW_DAYS - 1));
}

// a cached index from an earlier day: its days that have since left the window go
function withinWindow(index: StreamIndex): StreamIndex {
  const start = windowStart();
  if (!index.days.some((day) => day.day < start)) return index;
  const days = index.days.filter((day) => day.day >= start);
  return { ...index, days, total: days.reduce((sum, day) => sum + day.count, 0) };
}

function isIndex(value: unknown): value is StreamIndex {
  if (!value || typeof value !== 'object') return false;
  const index = value as StreamIndex;
  return Array.isArray(index.days)
    && Boolean(index.profile) && typeof index.profile === 'object'
    && (index.days.length === 0 || Array.isArray(index.days[0].tops));
}

let retiredCacheCleared = false;

function setIndex(state: ScopeState, index: StreamIndex | null, at: number) {
  state.index = index;
  state.indexDays = new Map((index?.days ?? []).map((day, position) => [day.day, position]));
  state.indexAt = at;
  state.snapshot = null;
}

function hydrateIndex(state: ScopeState) {
  if (typeof window === 'undefined') return;
  if (!retiredCacheCleared) {
    retiredCacheCleared = true;
    clearCacheByPrefix(RETIRED_CACHE_PREFIX);
  }
  const cached = readCache<StreamIndex>(INDEX_CACHE_PREFIX + state.key);
  if (!cached || !isIndex(cached.data)) return;
  setIndex(state, withinWindow(cached.data), cached.ts);
}

function statusOf(state: ScopeState): StreamIndexStatus {
  if (state.unauthorized) return 'unauthorized';
  if (state.index) return 'ready';
  if (state.indexInflight) return 'loading';
  return state.indexError ? 'error' : 'idle';
}

function indexStateOf(state: ScopeState): StreamIndexState {
  if (!state.snapshot) {
    state.snapshot = { index: state.index, status: statusOf(state), error: state.indexError, refresh: state.refresh };
  }
  return state.snapshot;
}

function loadIndex(state: ScopeState): Promise<void> {
  if (state.indexInflight) return state.indexInflight;
  const generation = state.generation;
  const request = Promise.resolve().then(async () => {
    try {
      const response = await fetch(`/api/feed/stream/index?${scopeQuery(state.scope).toString()}`, {
        cache: 'no-store',
        credentials: 'include',
      });
      if (generation !== state.generation) return;
      if (response.status === 401) {
        markUnauthorized(state);
        return;
      }
      const payload = (await response.json().catch(() => null)) as { index?: unknown; error?: string } | null;
      if (generation !== state.generation) return;
      const index = payload?.index;
      if (!response.ok || !isIndex(index)) {
        throw new Error(payload?.error || `The feed could not be loaded (${response.status})`);
      }
      applyIndex(state, index);
    } catch (error) {
      if (generation !== state.generation) return;
      state.indexError = error instanceof Error ? error.message : 'The feed could not be loaded';
      state.indexErrorAt = Date.now();
    } finally {
      if (generation === state.generation) {
        state.indexInflight = null;
        state.snapshot = null;
        emit();
        pump();
        if (state.refreshQueued && !state.unauthorized) {
          state.refreshQueued = false;
          void loadIndex(state);
        }
      }
    }
  });
  state.indexInflight = request;
  state.snapshot = null;
  emit();
  return request;
}

function applyIndex(state: ScopeState, incoming: StreamIndex) {
  const previous = state.index;
  const next = shareIndex(previous, withinWindow(incoming));
  if (previous !== next) {
    // a day whose summary changed refetches when next asked for; a day that left the index is let go
    const before = new Map((previous?.days ?? []).map((day) => [day.day, day]));
    const after = new Map(next.days.map((day) => [day.day, day]));
    let dropped = false;
    state.pages.forEach((page, day) => {
      const now = after.get(day);
      if (!now) {
        state.postCount -= page.posts.length;
        state.pages.delete(day);
        dropped = true;
      } else if (now !== before.get(day)) {
        page.stale = true;
      }
    });
    if (dropped) state.version = ++versionSequence;
    setIndex(state, next, Date.now());
  } else {
    state.indexAt = Date.now();
  }
  state.indexError = null;
  state.indexErrorAt = 0;
  state.snapshot = null;
  setCache(INDEX_CACHE_PREFIX + state.key, next);
}

// load the index when there is none, or refresh it in the background once it is old
function ensureIndex(state: ScopeState) {
  if (typeof window === 'undefined' || state.unauthorized || state.indexInflight) return;
  const now = Date.now();
  if (state.index && now - state.indexAt < INDEX_FRESH_MS) return;
  if (state.indexErrorAt && now - state.indexErrorAt < INDEX_RETRY_MS) return;
  void loadIndex(state);
}

function refreshScope(state: ScopeState) {
  if (typeof window === 'undefined') return;
  state.unauthorized = false;
  state.indexError = null;
  state.indexErrorAt = 0;
  state.failures = 0;
  state.retryAt = 0;
  state.pages.forEach((page) => {
    page.stale = true;
  });
  state.snapshot = null;
  if (state.indexInflight) state.refreshQueued = true;
  else void loadIndex(state);
  emit();
  pump();
}

function markUnauthorized(state: ScopeState) {
  state.unauthorized = true;
  setIndex(state, null, 0);
  state.pages.clear();
  state.postCount = 0;
  state.version = ++versionSequence;
  clearCacheByPrefix(INDEX_CACHE_PREFIX);
  emit();
  redirectToLoginOnce();
}

function watchVisibility() {
  if (visibilityWatched || typeof window === 'undefined') return;
  visibilityWatched = true;
  const onVisible = () => {
    if (document.visibilityState !== 'visible') return;
    scopes.forEach((state) => {
      if (state.indexWatchers > 0) ensureIndex(state);
    });
  };
  document.addEventListener('visibilitychange', onVisible);
  window.addEventListener('focus', onVisible);
}

/* ── days ──────────────────────────────────────────────────── */

function dayNumber(day: string): number {
  const [year, month, date] = day.split('-').map(Number);
  return Date.UTC(year, month - 1, date) / DAY_MS;
}

function daySpan(from: string, to: string): number {
  return dayNumber(to) - dayNumber(from) + 1;
}

function needsFetch(state: ScopeState, day: string, now: number): boolean {
  if (state.inflight.has(day) || !state.indexDays.has(day)) return false;
  const page = state.pages.get(day);
  return !page || page.stale || now - page.fetchedAt > PAGE_FRESH_MS;
}

// the day to fetch next in this scope: on screen before prefetched, the latest asked for first, then the newest
function pickSeed(state: ScopeState, now: number): number | null {
  let best: number | null = null;
  let bestRank = -Infinity;
  const consider = (day: string, onScreen: boolean) => {
    const position = state.indexDays.get(day);
    if (position == null || !needsFetch(state, day, now)) return;
    const rank = (onScreen ? 1e15 : 0) + (state.wanted.get(day) ?? 0) * 1e3 - position / 1e3;
    if (rank > bestRank) {
      bestRank = rank;
      best = position;
    }
  };
  state.interest.forEach((_count, day) => consider(day, true));
  state.hints.forEach((until, day) => {
    if (until <= now) {
      state.hints.delete(day);
      if (!state.interest.has(day)) state.wanted.delete(day);
      return;
    }
    if (!state.interest.has(day)) consider(day, false);
  });
  return best;
}

// from the seed, take in neighbouring days not loaded yet — older first — while it stays within one request
function growBatch(state: ScopeState, seed: number): Batch {
  const days = (state.index as StreamIndex).days;
  let newest = seed;
  let oldest = seed;
  let posts = days[seed].count;
  const open = (position: number) => !state.inflight.has(days[position].day) && !state.pages.has(days[position].day);
  while (
    oldest + 1 < days.length
    && open(oldest + 1)
    && posts + days[oldest + 1].count <= BATCH_POSTS
    && daySpan(days[oldest + 1].day, days[newest].day) <= BATCH_DAYS
  ) {
    oldest += 1;
    posts += days[oldest].count;
  }
  while (
    newest - 1 >= 0
    && open(newest - 1)
    && posts + days[newest - 1].count <= BATCH_POSTS
    && daySpan(days[oldest].day, days[newest - 1].day) <= BATCH_DAYS
  ) {
    newest -= 1;
    posts += days[newest].count;
  }
  return {
    state,
    days: days.slice(newest, oldest + 1).map((day) => day.day),
    from: days[oldest].day,
    to: days[newest].day,
  };
}

function nextBatch(): Batch | null {
  const now = Date.now();
  const byUse = Array.from(scopes.values()).sort((left, right) => right.usedAt - left.usedAt);
  for (const state of byUse) {
    if (!state.index || state.unauthorized || state.retryAt > now) continue;
    if (state.interest.size === 0 && state.hints.size === 0) continue;
    const seed = pickSeed(state, now);
    if (seed != null) return growBatch(state, seed);
  }
  return null;
}

function pump() {
  if (pumpQueued || typeof window === 'undefined') return;
  pumpQueued = true;
  queueMicrotask(() => {
    pumpQueued = false;
    while (activePageRequests < MAX_PAGE_REQUESTS) {
      const batch = nextBatch();
      if (!batch) return;
      startBatch(batch);
    }
  });
}

function startBatch({ state, days, from, to }: Batch) {
  activePageRequests += 1;
  days.forEach((day) => state.inflight.add(day));
  const generation = state.generation;
  const query = scopeQuery(state.scope);
  query.set('from', from);
  query.set('to', to);

  void fetch(`/api/feed/stream?${query.toString()}`, { cache: 'no-store', credentials: 'include' })
    .then(async (response) => {
      if (generation !== state.generation) return;
      if (response.status === 401) {
        markUnauthorized(state);
        return;
      }
      const payload = (await response.json().catch(() => null)) as { page?: StreamPage; error?: string } | null;
      if (generation !== state.generation) return;
      if (!response.ok || !payload?.page || !Array.isArray(payload.page.posts)) {
        throw new Error(payload?.error || `Posts could not be loaded (${response.status})`);
      }
      state.failures = 0;
      applyPage(state, payload.page, days);
    })
    .catch(() => {
      if (generation !== state.generation) return;
      // back off, then try again (1s, 2s, 4s … 30s)
      state.failures += 1;
      const delay = Math.min(MAX_RETRY_MS, 1000 * 2 ** Math.min(5, state.failures - 1));
      state.retryAt = Date.now() + delay;
      if (state.retryTimer) clearTimeout(state.retryTimer);
      state.retryTimer = setTimeout(() => {
        state.retryTimer = null;
        pump();
      }, delay);
    })
    .finally(() => {
      activePageRequests -= 1;
      if (generation === state.generation) days.forEach((day) => state.inflight.delete(day));
      pump();
    });
}

function applyPage(state: ScopeState, page: StreamPage, requested: string[]) {
  const now = Date.now();
  const order = state.scope.order;
  const grouped = new Map<string, StreamPost[]>();
  for (const post of page.posts) {
    const day = sectionDayOf(post, order);
    if (!day) continue;
    const bucket = grouped.get(day);
    if (bucket) bucket.push(post);
    else grouped.set(day, [post]);
  }
  // every day asked for is answered: a day the range had nothing for is an empty day
  requested.forEach((day) => {
    if (!grouped.has(day)) grouped.set(day, EMPTY_POSTS);
  });

  let changed = false;
  let mismatch = false;
  grouped.forEach((posts, day) => {
    const previous = state.pages.get(day);
    const ordered = posts.length ? orderDayPosts(posts, order) : EMPTY_POSTS;
    const shared = shareDay(previous?.posts, ordered);
    if (!previous || previous.posts !== shared) changed = true;
    state.postCount += shared.length - (previous?.posts.length ?? 0);
    state.pages.set(day, { posts: shared, fetchedAt: now, stale: false, usedAt: previous?.usedAt ?? now });
    const position = state.indexDays.get(day);
    const expected = position == null || !state.index ? 0 : state.index.days[position].count;
    if (expected !== shared.length) mismatch = true;
  });
  requested.forEach((day) => {
    if (!state.interest.has(day) && !state.hints.has(day)) state.wanted.delete(day);
  });

  if (changed) {
    state.version = ++versionSequence;
    emit();
  }
  // the posts moved on since the index was made: bring the index (and so the layout) up to date
  if (mismatch && now - state.mismatchAt > MISMATCH_REFRESH_MS) {
    state.mismatchAt = now;
    if (state.indexInflight) state.refreshQueued = true;
    else void loadIndex(state);
  }
  enforceMemoryCap();
}

// a day let go for memory stops being prefetched too (or it would come straight back)
function deletePage(state: ScopeState, day: string) {
  const page = state.pages.get(day);
  if (!page) return;
  state.postCount -= page.posts.length;
  state.pages.delete(day);
  state.hints.delete(day);
  if (!state.interest.has(day)) state.wanted.delete(day);
  state.version = ++versionSequence;
}

// near MEMORY_CAP_POSTS: first whole scopes nobody looks at (least recently used first), then single days
function enforceMemoryCap() {
  let total = 0;
  scopes.forEach((state) => {
    total += state.postCount;
  });
  if (total <= MEMORY_CAP_POSTS) return;

  const byUse = Array.from(scopes.values()).sort((left, right) => left.usedAt - right.usedAt);
  const current = byUse[byUse.length - 1];
  for (const state of byUse) {
    if (total <= MEMORY_CAP_POSTS) break;
    if (state === current || state.interest.size > 0 || state.postCount === 0) continue;
    total -= state.postCount;
    state.pages.clear();
    state.postCount = 0;
    state.hints.clear();
    state.wanted.clear();
    state.version = ++versionSequence;
  }

  if (total > MEMORY_CAP_POSTS) {
    const days: Array<{ state: ScopeState; day: string; page: DayPage }> = [];
    scopes.forEach((state) => state.pages.forEach((page, day) => {
      if (page.posts.length && !state.interest.has(day)) days.push({ state, day, page });
    }));
    days.sort((left, right) => left.page.usedAt - right.page.usedAt);
    for (const entry of days) {
      if (total <= MEMORY_CAP_POSTS) break;
      total -= entry.page.posts.length;
      deletePage(entry.state, entry.day);
    }
  }
  emit();
}

function addInterest(state: ScopeState, days: readonly string[]) {
  const now = Date.now();
  const sequence = ++wantSequence;
  state.usedAt = now;
  days.forEach((day) => {
    state.interest.set(day, (state.interest.get(day) ?? 0) + 1);
    state.wanted.set(day, sequence);
    const page = state.pages.get(day);
    if (page) page.usedAt = now;
  });
}

function removeInterest(state: ScopeState, days: readonly string[]) {
  days.forEach((day) => {
    const count = (state.interest.get(day) ?? 0) - 1;
    if (count > 0) {
      state.interest.set(day, count);
      return;
    }
    state.interest.delete(day);
    if (!state.hints.has(day)) state.wanted.delete(day);
  });
}

function postsOf(state: ScopeState, day: string): StreamPost[] | undefined {
  const page = state.pages.get(day);
  if (page) return page.posts;
  // the index has no such day: it has no posts
  return state.index && !state.indexDays.has(day) ? EMPTY_POSTS : undefined;
}

/* ── invalidating ──────────────────────────────────────────── */

function resetScope(state: ScopeState) {
  state.generation += 1;
  setIndex(state, null, 0);
  state.indexError = null;
  state.indexErrorAt = 0;
  state.indexInflight = null;
  state.refreshQueued = false;
  state.unauthorized = false;
  state.pages.clear();
  state.postCount = 0;
  state.version = ++versionSequence;
  state.inflight.clear();
  state.failures = 0;
  state.retryAt = 0;
  state.mismatchAt = 0;
  if (state.retryTimer) {
    clearTimeout(state.retryTimer);
    state.retryTimer = null;
  }
  if (typeof window !== 'undefined' && (state.indexWatchers > 0 || state.interest.size > 0)) void loadIndex(state);
}

/* ── the API ───────────────────────────────────────────────── */

export function useStreamIndex(scope: StreamScope): StreamIndexState {
  const key = keyFor(scope);
  const getSnapshot = useCallback(() => indexStateOf(ensureScope(key)), [key]);
  const state = useSyncExternalStore(subscribeStream, getSnapshot, getServerIndexState);

  useEffect(() => {
    const entry = ensureScope(key);
    entry.indexWatchers += 1;
    entry.usedAt = Date.now();
    watchVisibility();
    ensureIndex(entry);
    return () => {
      entry.indexWatchers -= 1;
    };
  }, [key]);

  return state;
}

type DaysMemo = {
  key: string;
  list: readonly string[];
  version: number;
  index: StreamIndex | null;
  map: ReadonlyMap<string, StreamPost[]>;
};

export function useDayPosts(scope: StreamScope, days: readonly string[]): ReadonlyMap<string, StreamPost[]> {
  const key = keyFor(scope);
  const daysKey = days.join(',');
  // a new array with the same days is the same request
  // eslint-disable-next-line react-hooks/exhaustive-deps
  const list = useMemo(() => days.slice(), [daysKey]);
  const memo = useRef<DaysMemo | null>(null);

  const getSnapshot = useCallback(() => {
    const state = ensureScope(key);
    const cached = memo.current;
    if (cached && cached.key === key && cached.list === list && cached.version === state.version && cached.index === state.index) {
      return cached.map;
    }
    const map = new Map<string, StreamPost[]>();
    list.forEach((day) => {
      const posts = postsOf(state, day);
      if (posts) map.set(day, posts);
    });
    let result: ReadonlyMap<string, StreamPost[]> = map;
    if (cached && cached.map.size === map.size) {
      let same = true;
      map.forEach((posts, day) => {
        if (cached.map.get(day) !== posts) same = false;
      });
      if (same) result = cached.map;
    }
    memo.current = { key, list, version: state.version, index: state.index, map: result };
    return result;
  }, [key, list]);

  const map = useSyncExternalStore(subscribeStream, getSnapshot, getServerDays);

  useEffect(() => {
    const entry = ensureScope(key);
    addInterest(entry, list);
    ensureIndex(entry);
    pump();
    return () => removeInterest(entry, list);
  }, [key, list]);

  return map;
}

export function getDayPosts(scope: StreamScope, day: string): StreamPost[] | undefined {
  const state = ensureScope(keyFor(scope));
  const page = state.pages.get(day);
  if (page) page.usedAt = Date.now();
  return postsOf(state, day);
}

// a scope's index as the store has it (null: not yet), its load started if it is missing or stale
export function peekStreamIndex(scope: StreamScope): StreamIndex | null {
  if (typeof window === 'undefined') return null;
  const state = ensureScope(keyFor(scope));
  ensureIndex(state);
  return state.index;
}

// warm the days about to come on screen (the hint lapses after a few seconds if they never do)
export function prefetchDays(scope: StreamScope, days: readonly string[]): void {
  if (typeof window === 'undefined' || days.length === 0) return;
  const state = ensureScope(keyFor(scope));
  const now = Date.now();
  const sequence = ++wantSequence;
  state.usedAt = now;
  days.forEach((day) => {
    state.hints.set(day, now + HINT_MS);
    if (!state.interest.has(day)) state.wanted.set(day, sequence);
  });
  ensureIndex(state);
  pump();
}

// drop what is cached, for every scope or for one feed's scopes plus the all-feeds ones (both orders); scopes on
// screen load again at once
export function invalidateStream(feedId?: string | null): void {
  const target = feedId == null ? null : String(feedId).trim() || null;
  scopes.forEach((state) => {
    if (target === null || state.scope.feedId === null || state.scope.feedId === target) resetScope(state);
  });
  if (target === null) {
    clearCacheByPrefix(INDEX_CACHE_PREFIX);
  } else {
    clearCacheByPrefix(`${INDEX_CACHE_PREFIX}${target}:`);
    clearCacheByPrefix(`${INDEX_CACHE_PREFIX}all:`);
  }
  emit();
  pump();
}

function getServerIndexState(): StreamIndexState {
  return SERVER_INDEX_STATE;
}

function getServerDays(): ReadonlyMap<string, StreamPost[]> {
  return EMPTY_DAYS;
}
