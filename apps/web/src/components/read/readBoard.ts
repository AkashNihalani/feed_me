/* ─────────────────────────────────────────────────────────────
   READ BOARD — every feeder's trajectory, from the posts the app
   tracks: the same runs of 10 a read is built on, each run's
   typical post (median top %), which way the latest run moved
   against the one before, the latest post and the best one in
   memory. It is what Read shows before you pick someone, so a
   feeder without a full read still has something true to say.

   One /api/feed/feeder-posts call per feed (every feeder's posts,
   newest first), kept for the visit. The samples gathered into
   Read's own feed are worked out from the reader data instead.
   ───────────────────────────────────────────────────────────── */

import readerData from '@/data/readerPreview.json';
import { SAMPLE_FEED_ID, normalizeHandle, type RailFeed } from '@/components/read/readFeeds';

export const RUN_SIZE = 10;
export const RUN_SLOTS = 10;
const MEMORY = 100;
const QUIET_DAYS = 14;
// how far (in top-% points) a run's typical post has to move against the run before it to count as a move
const SHIFT = 8;

export type BoardPost = {
  key: string;
  url: string | null;
  thumb: string | null;
  // the colours a sample post's drawn cover uses, when there is no real thumbnail
  scene: string | null;
  postedAt: number;
  pct: number | null;
  checkpoint: string;
};

export type BoardRun = {
  r: number;
  // the typical post: median top % of the run's ranked posts
  tp: number | null;
  count: number;
  live: boolean;
  items: BoardPost[];
};

export type Momentum = 'up' | 'down' | 'level' | 'early';

export type BoardFeeder = {
  handle: string;
  feedId: string;
  // when this was worked out (ages are measured from here, so drawing it stays pure)
  at: number;
  total: number;
  // the last runs, oldest first (at most RUN_SLOTS)
  runs: BoardRun[];
  // the run the move is read from (the live one once it has 5 posts, else the last full one) and the one before it
  now: BoardRun | null;
  before: BoardRun | null;
  momentum: Momentum;
  latest: BoardPost | null;
  best: BoardPost | null;
  quietDays: number | null;
};

type ApiPost = {
  postKey?: string | null;
  postUrl?: string | null;
  thumbnailUrl?: string | null;
  postedAt?: string | null;
  handle?: string | null;
  latestCheckpoint?: string | null;
  latestPercentile?: number | null;
};

function median(values: number[]) {
  const sorted = [...values].sort((a, b) => a - b);
  const mid = sorted.length >> 1;
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

export function summarize(handle: string, feedId: string, posts: BoardPost[], now: number): BoardFeeder {
  const chron = [...posts].sort((a, b) => a.postedAt - b.postedAt);
  const runCount = Math.ceil(chron.length / RUN_SIZE);
  const all: BoardRun[] = Array.from({ length: runCount }, (_, k) => {
    const items = chron.slice(k * RUN_SIZE, k * RUN_SIZE + RUN_SIZE);
    const ranked = items.map((post) => post.pct).filter((v): v is number => v != null && Number.isFinite(v));
    return {
      r: k + 1,
      tp: ranked.length ? Math.max(1, Math.round(median(ranked))) : null,
      count: items.length,
      live: k === runCount - 1 && items.length < RUN_SIZE,
      items,
    };
  });
  const last = all[all.length - 1] || null;
  const nowRun = last && (!last.live || last.count >= 5) ? last : all[all.length - 2] || null;
  const at = nowRun ? all.indexOf(nowRun) : -1;
  const before = at > 0 ? all[at - 1] : null;
  let momentum: Momentum = 'early';
  if (nowRun?.tp != null && before?.tp != null) {
    const moved = before.tp - nowRun.tp;
    momentum = moved >= SHIFT ? 'up' : moved <= -SHIFT ? 'down' : 'level';
  }
  const memory = chron.slice(-MEMORY);
  const best = memory.reduce<BoardPost | null>((top, post) => (post.pct != null && (top?.pct == null || post.pct < top.pct) ? post : top), null);
  const latest = chron[chron.length - 1] || null;
  const quiet = latest ? Math.floor((now - latest.postedAt) / 86_400_000) : null;
  return {
    handle,
    feedId,
    at: now,
    total: chron.length,
    runs: all.slice(-RUN_SLOTS),
    now: nowRun,
    before,
    momentum,
    latest,
    best,
    quietDays: quiet != null && quiet >= QUIET_DAYS ? quiet : null,
  };
}

/* the samples: the reader data's own posts, ranked the way the engine ranks them (within the feeder) */
type SamplePost = { id: string; date: string; rank_final?: number | null; pct_fixed?: number | null; scene?: string | null };
const SAMPLES = (readerData as { feeders: { handle: string; posts: SamplePost[] }[] }).feeders;

function sampleBoard(feed: RailFeed, now: number): BoardFeeder[] {
  return feed.feeders.map((feeder) => {
    const source = SAMPLES.find((item) => normalizeHandle(item.handle) === feeder.handle);
    const raw = source?.posts || [];
    const score = (post: SamplePost) => post.pct_fixed ?? post.rank_final ?? 100;
    const order = [...raw].sort((a, b) => score(a) - score(b));
    const posts: BoardPost[] = raw.map((post) => ({
      key: post.id,
      url: null,
      thumb: null,
      scene: post.scene || null,
      postedAt: Date.parse(`${post.date}T12:00:00Z`),
      pct: post.pct_fixed ?? ((order.indexOf(post) + 1) / raw.length) * 100,
      checkpoint: 'D7',
    }));
    return summarize(feeder.handle, feed.id, posts, now);
  });
}

const boards = new Map<string, BoardFeeder[]>();
const pending = new Map<string, Promise<BoardFeeder[]>>();

export function cachedBoard(feedId: string) {
  return boards.get(feedId) || null;
}

export function loadBoard(feed: RailFeed): Promise<BoardFeeder[]> {
  if (feed.id === SAMPLE_FEED_ID) {
    const rows = sampleBoard(feed, Date.now());
    boards.set(feed.id, rows);
    return Promise.resolve(rows);
  }
  const inFlight = pending.get(feed.id);
  if (inFlight) return inFlight;
  const params = new URLSearchParams({ feedId: feed.id, handle: 'all', timeframe: '90D' });
  const request = fetch(`/api/feed/feeder-posts?${params.toString()}`, { cache: 'no-store', credentials: 'include' })
    .then((response) => {
      // a feed with no active feeders answers 404: it simply has no rows
      if (response.status === 404) return { posts: [] as ApiPost[] };
      if (!response.ok) throw new Error(`feeder-posts ${response.status}`);
      return response.json() as Promise<{ posts?: ApiPost[] }>;
    })
    .then((payload) => {
      const byHandle = new Map<string, BoardPost[]>();
      (payload.posts || []).forEach((post) => {
        const handle = normalizeHandle(post.handle);
        const postedAt = Date.parse(post.postedAt || '');
        if (!handle || !Number.isFinite(postedAt)) return;
        const list = byHandle.get(handle) || [];
        list.push({
          key: String(post.postKey || `${handle}:${postedAt}`),
          url: post.postUrl || null,
          thumb: post.thumbnailUrl || null,
          scene: null,
          postedAt,
          pct: post.latestPercentile ?? null,
          checkpoint: String(post.latestCheckpoint || '').toUpperCase(),
        });
        byHandle.set(handle, list);
      });
      const now = Date.now();
      const rows = feed.feeders.map((feeder) => summarize(feeder.handle, feed.id, byHandle.get(feeder.handle) || [], now));
      boards.set(feed.id, rows);
      return rows;
    })
    .finally(() => pending.delete(feed.id));
  pending.set(feed.id, request);
  return request;
}
