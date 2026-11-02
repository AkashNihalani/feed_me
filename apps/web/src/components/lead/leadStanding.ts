/* ─────────────────────────────────────────────────────────────
   LEAD STANDING — where the pick stands right now, read from
   Lead's own load (leadData) and ranked the way Lead's board
   ranks: on the typical post (the median Top % of the last 30
   days), then the best post, then the median ER multiple, then
   comments. Each place's move is its place against the 30 days
   before, ranked the same way (app/lead/page.tsx: boardOrder,
   rankRows).

   The standings are always feeders: a feeder's feed (as the tabs
   share it), a feed's own, or every feeder across your feeds for
   all of them (an account in two feeds counts once). Feed's
   profile shows them (ProfileHeader's Standing).
   ───────────────────────────────────────────────────────────── */

import { normalizeHandle, titleCase } from '@/lib/feedLabels';
import { postMoment } from '@/lib/postMoment';
import type { LeadLoad } from '@/components/lead/leadData';
import { leadScope } from '@/components/lead/leadHall';

export const STANDING_DAYS = 30;
const DAY_MS = 86_400_000;

type StandingPost = {
  postKey?: string | null;
  postedAt: string | null;
  postUrl: string | null;
  handle: string | null;
  latestPercentile: number | null;
  engagementRateMultiple?: number | null;
  comments?: number | null;
};

export type StandingRow = {
  handle: string;
  profilePicUrl: string | null;
  place: number;
  // places climbed since the 30 days before (down: below zero); null: no posts then
  move: number | null;
  typical: number | null;
  posts: number;
};

export type Standing = {
  // where the places are: a feed's title, or null for every feed
  where: string | null;
  // every feeder, first place first
  rows: StandingRow[];
  // the picked feeder's row
  you: StandingRow | null;
};

type Feeder = { handle: string; profilePicUrl: string | null };
type Tally = Feeder & { typical: number | null; best: number | null; er: number | null; comments: number; posts: number };

// when a post went out (its shortcode's moment), worked out once per post
const moments = new WeakMap<StandingPost, number>();
function momentOf(post: StandingPost) {
  let at = moments.get(post);
  if (at === undefined) {
    at = postMoment(post.postedAt, post.postUrl) ?? 0;
    moments.set(post, at);
  }
  return at;
}

function median(values: Array<number | null | undefined>): number | null {
  const sorted = values.filter((value): value is number => value != null && Number.isFinite(value)).sort((a, b) => a - b);
  if (!sorted.length) return null;
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function tally(feeder: Feeder, posts: StandingPost[]): Tally {
  let best: number | null = null;
  let comments = 0;
  for (const post of posts) {
    if (post.latestPercentile != null && (best == null || post.latestPercentile < best)) best = post.latestPercentile;
    comments += Number(post.comments) || 0;
  }
  return {
    ...feeder,
    typical: median(posts.map((post) => post.latestPercentile)),
    best,
    er: median(posts.map((post) => post.engagementRateMultiple)),
    comments,
    posts: posts.length,
  };
}

// Lead's board order: the typical post first, the best post breaks a tie, then the ER multiple, then comments
function boardOrder(a: Tally, b: Tally) {
  return ((a.typical ?? 101) - (b.typical ?? 101))
    || ((a.best ?? 101) - (b.best ?? 101))
    || ((b.er ?? 0) - (a.er ?? 0))
    || (b.comments - a.comments);
}

function standingOf(feeders: Feeder[], posts: StandingPost[], where: string | null, handle: string): Standing {
  const now = Date.now();
  const from = now - STANDING_DAYS * DAY_MS;
  const before = from - STANDING_DAYS * DAY_MS;
  const nowBy = new Map<string, StandingPost[]>();
  const thenBy = new Map<string, StandingPost[]>();
  for (const post of posts) {
    const owner = normalizeHandle(post.handle);
    const at = momentOf(post);
    // (as Lead's window: a post with no moment counts as this window's)
    const bucket = at === 0 || at >= from ? nowBy : at >= before ? thenBy : null;
    if (!bucket) continue;
    const list = bucket.get(owner);
    if (list) list.push(post);
    else bucket.set(owner, [post]);
  }
  const placesThen = new Map(
    feeders
      .map((feeder) => tally(feeder, thenBy.get(feeder.handle) ?? []))
      .filter((row) => row.posts > 0)
      .sort(boardOrder)
      .map((row, index) => [row.handle, index + 1]),
  );
  const rows = feeders
    .map((feeder) => tally(feeder, nowBy.get(feeder.handle) ?? []))
    .sort(boardOrder)
    .map((row, index): StandingRow => {
      const then = placesThen.get(row.handle);
      return { handle: row.handle, profilePicUrl: row.profilePicUrl, place: index + 1, move: then == null ? null : then - (index + 1), typical: row.typical, posts: row.posts };
    });
  return { where, rows, you: handle ? rows.find((row) => row.handle === handle) ?? null : null };
}

type LoadFeed = { id: string; title: string; feeders: Feeder[]; posts: StandingPost[] };

function feedsOf(load: LeadLoad): LoadFeed[] {
  return load.feeds.map((feed) => ({
    id: feed.id,
    title: String(feed.title ?? ''),
    feeders: (Array.isArray(feed.feeders) ? feed.feeders : [])
      .map((feeder: { handle?: unknown; profilePicUrl?: unknown }) => ({
        handle: normalizeHandle(String(feeder?.handle ?? '')),
        profilePicUrl: typeof feeder?.profilePicUrl === 'string' && feeder.profilePicUrl ? feeder.profilePicUrl : null,
      }))
      .filter((feeder) => feeder.handle),
    posts: feed.posts as StandingPost[],
  }));
}

/* the standings for the pick every tab shares (a feed Lead doesn't have, or none yet, is every feed), kept per load */
const made = new WeakMap<LeadLoad, Map<string, Standing>>();
export function standingFor(load: LeadLoad, scope: { feedId: string | null | undefined; handle: string | null }): Standing {
  const feeds = feedsOf(load);
  const { activeFeedId, activeFeeder } = leadScope(feeds, scope);
  const key = `${activeFeedId}|${activeFeeder}`;
  let known = made.get(load);
  if (!known) made.set(load, (known = new Map()));
  const kept = known.get(key);
  if (kept) return kept;

  let standing: Standing;
  const feed = feeds.find((candidate) => candidate.id === activeFeedId);
  if (feed) {
    standing = standingOf(feed.feeders, feed.posts, titleCase(feed.title), activeFeeder);
  } else {
    // every feed: each account once (its first feed's face), each post once
    const feeders = new Map<string, Feeder>();
    const posts = new Map<string, StandingPost>();
    for (const one of feeds) {
      for (const feeder of one.feeders) if (!feeders.has(feeder.handle)) feeders.set(feeder.handle, feeder);
      for (const post of one.posts) {
        const id = post.postKey || `${post.handle}:${post.postUrl}:${post.postedAt}`;
        if (!posts.has(id)) posts.set(id, post);
      }
    }
    standing = standingOf([...feeders.values()], [...posts.values()], null, '');
  }
  known.set(key, standing);
  return standing;
}
