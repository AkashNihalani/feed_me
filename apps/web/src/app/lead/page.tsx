'use client';

// Lead — who's winning, in numbers.
// Spine: Top of the feed (the leaders strip) → the Feederboard (media-first rows
// ranked on the TYPICAL post percentile, throne row for rank 1) → Hall of Feed
// (the standout posts: wins, likes, comments, engagement) → Follow-through (the
// follower curve and its spikes) → Feeding habits (cadence, prime time, the menu).
// Each section: a short handle (the cheeky name) and one plain line of what it
// measures. Every number traces to feeder-posts percentiles/multiples or the
// ascent series. No generated copy — verdict words are threshold math.

import Link from 'next/link';
import { CSSProperties, Ref, memo, startTransition, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, useTransition } from 'react';
import { ChevronDown } from 'lucide-react';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { usePageReady } from '@/components/shell/AppShell';
import { MENU_APPLY_AFTER_CLOSE_MS, TimeframeControl, useTabHeader } from '@/components/shell/TabHeader';
import SlotText from '@/components/SlotText';
import { useCompressedOnScroll } from '@/lib/useCompressedOnScroll';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import { normalizeHandle, titleCase } from '@/lib/feedLabels';
import { BEAT_ARRIVE_S, BEAT_CONTAINER, BEAT_HIDDEN, BEAT_ITEM, BEAT_LEAVE_EASE, BEAT_SHOWN, beatArrive, beatLeave } from '@/lib/motion';
import { getTabScope, setTabScope, useTabScope, useTabScopeTransition, type TabScope } from '@/lib/tabScope';
import { cn } from '@/lib/utils';
import { postMoment } from '@/lib/postMoment';
import { ms, sec } from '@/lib/tempo';
import { afterDrawn, holdsFlights } from '@/lib/webkitMotion';
import { buildFeederboardFacts } from '@/lib/feederboardFacts';
import type { LeadActivity } from '@/app/api/lead/activity/route';
import LeadGrowth from '@/components/lead/LeadGrowth';
import LeadRhythm from '@/components/lead/LeadRhythm';
import LeadTiming from '@/components/lead/LeadTiming';
import LeadFormats from '@/components/lead/LeadFormats';
import { CrossfadeImage, DIM, MARK, NOTE, SOFT, Swap, holdArrival, useHeldNear, useWide, whenIdle } from '@/components/lead/leadKit';
import { LEDGER_DEAL_MS, LEDGER_MOVE_MS, Ledger, keepBelow, ledgerDelay } from '@/components/lead/ledgerMotion';
import { ascentFromActivity, formatsFor, growthFor, remember, rhythmFor, timingFor } from '@/components/lead/leadModels';
import { announceFirstScreen } from '@/lib/launch';
import { keptLead, leadOnHand, loadLead, type LeadLoad } from '@/components/lead/leadData';
import './lead.css';

const TIMEFRAMES = [7, 30, 60, 90] as const;
type Timeframe = (typeof TIMEFRAMES)[number];
const SOFT_EASE = [0.16, 0.9, 0.2, 1] as const;
// an open row unfolds its three parts one after another (its rows below make room on the ledger's beat)
const OPEN_CONTAINER = { hidden: {}, visible: {} } as const;
const OPEN_GRID = { hidden: {}, visible: { transition: { staggerChildren: sec(0.09), delayChildren: sec(0.05) } } } as const;
const OPEN_ITEM = {
  hidden: { opacity: 0, transform: 'translateY(16px)' },
  visible: { opacity: 1, transform: 'translateY(0px)', transition: { duration: sec(0.56), ease: [0.2, 0.7, 0.2, 1] } },
} as const;
// how long a loaded board counts as fresh when you come back to the tab (checkpoints land hourly at most)
const LEAD_FRESH_MS = 5 * 60 * 1000;

type MetricTotals = {
  likes?: string | number | null;
  comments?: string | number | null;
  postsTracked?: string | number | null;
};

type ApiFeeder = {
  handle: string;
  profilePicUrl?: string | null;
  thumbnailUrl?: string | null;
  followerCount?: number | null;
  metrics?: MetricTotals;
};

type ApiFeed = {
  id: string;
  title: string;
  feeders: ApiFeeder[];
  metrics?: MetricTotals;
};

type TrackedPost = {
  postKey: string;
  postUrl: string | null;
  thumbnailUrl: string | null;
  mediaType: 'image' | 'carousel' | 'reel' | 'unknown';
  postedAt: string | null;
  handle: string | null;
  firstCheckpoint?: string | null;
  firstPercentile?: number | null;
  latestCheckpoint: string;
  latestBusinessDayIst: string | null;
  latestPercentile: number | null;
  views: number | null;
  likes: number | null;
  comments: number | null;
  engagementRate: number | null;
  rankingMetric: string | null;
  rankingMultiple: number | null;
  likesMultiple: number | null;
  commentsMultiple: number | null;
  engagementRateMultiple: number | null;
};

type FeederboardFact = {
  kind: string;
  headline: string;
  detail: string;
  thumbnailUrl: string | null;
  postUrl: string | null;
  evidencePostKeys: string[];
};

type AscentPoint = {
  snapshot_date_ist: string;
  follower_count: number;
};

type RuntimeFeed = ApiFeed & {
  posts: TrackedPost[];
  ascent: AscentPoint[];
};

type BoardRow = {
  id: string;
  rank: number;
  rankMove: number | null;
  typicalShift: number | null;
  prevPostCount: number | null;
  name: string;
  caption: string;
  feedName: string;
  profilePicUrl: string | null;
  anchorThumb: string | null;
  typical: number | null;
  best: number | null;
  bestPost: TrackedPost | null;
  floor: number | null;
  lastPostedAt: number | null;
  peak: number | null;
  verdict: string;
  hot: boolean;
  likes: number;
  comments: number;
  engagementRate: number | null;
  likesMultiple: number | null;
  commentsMultiple: number | null;
  engagementRateMultiple: number | null;
  followers: number;
  followerDelta: number | null;
  postCount: number;
  proofPosts: TrackedPost[];
};

function numeric(value: unknown) {
  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function median(values: Array<number | null | undefined>): number | null {
  const sorted = values
    .filter((value): value is number => value != null && Number.isFinite(value))
    .sort((a, b) => a - b);
  if (sorted.length === 0) return null;
  const middle = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[middle] : (sorted[middle - 1] + sorted[middle]) / 2;
}

function compactNumber(value: number) {
  const absolute = Math.abs(value);
  const sign = value < 0 ? '-' : '';
  if (absolute >= 1_000_000_000) return `${sign}${(absolute / 1_000_000_000).toFixed(1).replace(/\.0$/, '')}B`;
  if (absolute >= 1_000_000) return `${sign}${(absolute / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
  if (absolute >= 1_000) return `${sign}${(absolute / 1_000).toFixed(1).replace(/\.0$/, '')}K`;
  return `${Math.round(value)}`;
}

function signedNumber(value: number | null) {
  if (value == null) return '—';
  if (value === 0) return '0';
  return `${value > 0 ? '+' : ''}${compactNumber(value)}`;
}

function formatMultiple(value: number | null) {
  if (value == null) return '—';
  return `${value.toFixed(value >= 10 ? 0 : 1).replace(/\.0$/, '')}×`;
}

function formatEngagementRate(value: number | null) {
  if (value == null) return '—';
  const percent = Math.abs(value) <= 1 ? value * 100 : value;
  return `${percent.toFixed(Math.abs(percent) >= 10 ? 1 : 2).replace(/\.0+$/, '').replace(/(\.\d*[1-9])0+$/, '$1')}%`;
}

function percentLabel(value: number | null) {
  return value == null ? '—' : String(Math.max(1, Math.round(value)));
}

function lastPostedLabel(time: number | null) {
  if (!time) return '—';
  const days = Math.max(0, Math.floor((Date.now() - time) / 86_400_000));
  return days === 0 ? 'TODAY' : `${days}D`;
}

/* the pick every tab shares, as Lead reads it: a feed Lead doesn't have (or none yet) is all feeds */
function resolveScope(feeds: RuntimeFeed[], scope: TabScope) {
  const feedId = scope.feedId ?? (scope.handle
    ? feeds.find((feed) => feed.feeders.some((feeder) => normalizeHandle(feeder.handle) === scope.handle))?.id ?? null
    : null);
  const activeFeedId = feedId && feeds.some((feed) => feed.id === feedId) ? feedId : 'all';
  return { activeFeedId, activeFeeder: activeFeedId !== 'all' && scope.handle ? scope.handle : '' };
}

/* what the board is drawn from, as one string: a background refresh that comes back the same leaves the page alone */
function feedsSignature(feeds: RuntimeFeed[]) {
  return feeds.map((feed) => [
    feed.id,
    feed.title,
    feed.feeders.map((feeder) => `${feeder.handle}:${feeder.profilePicUrl || ''}`).join(','),
    feed.posts.map((post) => `${post.postKey}:${post.latestCheckpoint}:${post.latestPercentile ?? ''}:${post.likes ?? ''}:${post.comments ?? ''}`).join(','),
    feed.ascent.map((point) => `${point.snapshot_date_ist}:${point.follower_count}`).join(','),
  ].join('|')).join('||');
}

/* the board's feeds from a load (components/lead/leadData), and its signature: worked out once per load */
const runtimes = new WeakMap<LeadLoad, { feeds: RuntimeFeed[]; signature: string }>();
function runtimeOf(load: LeadLoad): { feeds: RuntimeFeed[]; signature: string } {
  let made = runtimes.get(load);
  if (!made) {
    const feeds = load.feeds.map((loaded) => {
      const feed = loaded as unknown as ApiFeed & { posts: TrackedPost[] };
      return {
        ...feed,
        feeders: Array.isArray(feed.feeders) ? feed.feeders : [],
        posts: Array.isArray(feed.posts) ? feed.posts : [],
        ascent: ascentFromActivity(load.activity, feed.id),
      } satisfies RuntimeFeed;
    });
    made = { feeds, signature: `${feedsSignature(feeds)}#${load.activity?.posts.length ?? 0}:${load.activity?.to ?? ''}` };
    runtimes.set(load, made);
  }
  return made;
}

/* when a post went out, worked out once per post (the shortcode read is not free, and every window asks) */
const postTimes = new WeakMap<TrackedPost, number>();

function postTime(post: TrackedPost) {
  let time = postTimes.get(post);
  if (time === undefined) {
    time = postMoment(post.postedAt, post.postUrl) ?? 0;
    postTimes.set(post, time);
  }
  return time;
}

/* a feed's posts in the selected window, kept per range (see remember): every builder below asks for the same few */
function postsInWindow(posts: TrackedPost[], days: number) {
  return remember(posts, null, `window|${days}`, () => {
    const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
    return posts.filter((post) => {
      const time = postTime(post);
      return time === 0 || time >= cutoff;
    });
  });
}

/* The window immediately before the selected one, same length. Every level on
   the page is read against this — that's where the direction comes from. */
function postsInPriorWindow(posts: TrackedPost[], days: number) {
  return remember(posts, null, `prior|${days}`, () => {
    const spanMs = days * 24 * 60 * 60 * 1000;
    const end = Date.now() - spanMs;
    const start = end - spanMs;
    return posts.filter((post) => {
      const time = postTime(post);
      return time > 0 && time >= start && time < end;
    });
  });
}

function windowDelta(points: AscentPoint[], startMs: number, endMs: number) {
  const first = valueAt(points, startMs);
  const last = valueAt(points, endMs);
  if (first == null || last == null) return null;
  return last - first;
}

/* Checkpoint trajectory: positive = the post climbed the pool between its
   first and latest read (percentile fell). */
function climbOf(post: TrackedPost) {
  if (post.firstPercentile == null || post.latestPercentile == null) return null;
  const delta = post.firstPercentile - post.latestPercentile;
  return Math.abs(delta) < 0.5 ? 0 : delta;
}

function bestFirst(a: TrackedPost, b: TrackedPost) {
  const percentileDifference = (a.latestPercentile ?? 101) - (b.latestPercentile ?? 101);
  if (percentileDifference !== 0) return percentileDifference;
  const multipleDifference = (b.rankingMultiple ?? 0) - (a.rankingMultiple ?? 0);
  if (multipleDifference !== 0) return multipleDifference;
  return (numeric(b.comments) * 5 + numeric(b.likes)) - (numeric(a.comments) * 5 + numeric(a.likes));
}

/* a feed's follower readings in time order, read once per curve */
function ascentSeries(points: AscentPoint[]) {
  return remember(points, null, 'series', () => points
    .map((point) => ({ time: Date.parse(point.snapshot_date_ist), value: numeric(point.follower_count) }))
    .filter((point) => Number.isFinite(point.time))
    .sort((a, b) => a.time - b.time));
}

function ascentDelta(points: AscentPoint[]) {
  const sorted = ascentSeries(points);
  if (sorted.length < 2) return null;
  return sorted[sorted.length - 1].value - sorted[0].value;
}

function valueAt(points: AscentPoint[], target: number) {
  const sorted = ascentSeries(points);
  if (!sorted.length) return null;
  const prior = sorted.filter((point) => point.time <= target).at(-1);
  return prior?.value ?? sorted.find((point) => point.time >= target)?.value ?? null;
}

/* Verdict words are threshold math on the typical percentile — never prose.
   carrying ≤10 · working ≤25 · steady ≤45 · quiet above that. */
function verdictFor(typical: number | null, postCount: number) {
  if (postCount === 0 || typical == null) return 'no reads';
  if (typical <= 10) return 'carrying';
  if (typical <= 25) return 'working';
  if (typical <= 45) return 'steady';
  return 'quiet';
}

function proofMetric(post: TrackedPost) {
  if (post.rankingMetric === 'comments') return `${compactNumber(numeric(post.comments))} comments`;
  if (post.rankingMetric === 'engagement_rate') return `${formatEngagementRate(post.engagementRate)} eng. rate`;
  return `${compactNumber(numeric(post.likes))} likes`;
}

function createBoardRow({
  id,
  name,
  caption,
  feedName,
  profilePicUrl,
  followers,
  followerDelta,
  posts,
  prevPosts,
}: {
  id: string;
  name: string;
  caption: string;
  feedName: string;
  profilePicUrl: string | null;
  followers: number;
  followerDelta: number | null;
  posts: TrackedPost[];
  prevPosts: TrackedPost[];
}): BoardRow {
  const sortedPosts = [...posts].sort(bestFirst);
  const proofPosts = sortedPosts.filter((post) => post.thumbnailUrl).slice(0, 4);
  const bestPost = sortedPosts[0] || null;
  const typical = median(posts.map((post) => post.latestPercentile));
  const best = bestPost?.latestPercentile ?? null;
  const floor = posts.reduce<number | null>((max, post) => {
    if (post.latestPercentile == null) return max;
    return max == null || post.latestPercentile > max ? post.latestPercentile : max;
  }, null);
  const lastPostedAt = posts.reduce<number | null>((max, post) => {
    const time = postTime(post);
    return time > 0 && (max == null || time > max) ? time : max;
  }, null);
  const peak = bestPost?.rankingMultiple
    ?? posts.reduce<number | null>((max, post) => {
      const value = post.rankingMultiple;
      if (value == null || !Number.isFinite(value)) return max;
      return max == null || value > max ? value : max;
    }, null);
  const verdict = verdictFor(typical, posts.length);
  const prevTypical = median(prevPosts.map((post) => post.latestPercentile));
  const typicalShift = typical != null && prevTypical != null
    ? Math.round(prevTypical - typical)
    : null;

  return {
    id,
    rank: 0,
    rankMove: null,
    typicalShift: typicalShift === 0 ? 0 : typicalShift,
    prevPostCount: prevPosts.length,
    name,
    caption,
    feedName,
    profilePicUrl,
    anchorThumb: proofPosts[0]?.thumbnailUrl || null,
    typical,
    best,
    bestPost,
    floor,
    lastPostedAt,
    peak,
    verdict,
    hot: verdict === 'carrying' || verdict === 'working',
    likes: posts.reduce((sum, post) => sum + numeric(post.likes), 0),
    comments: posts.reduce((sum, post) => sum + numeric(post.comments), 0),
    engagementRate: median(posts.map((post) => post.engagementRate)),
    likesMultiple: median(posts.map((post) => post.likesMultiple)),
    commentsMultiple: median(posts.map((post) => post.commentsMultiple)),
    engagementRateMultiple: median(posts.map((post) => post.engagementRateMultiple)),
    followers,
    followerDelta,
    postCount: posts.length,
    proofPosts,
  };
}

/* Rank on the typical post, not the outlier: median percentile first, best
   post breaks ties, then median ER multiple, then response volume. */
function boardOrder(a: BoardRow, b: BoardRow) {
  const typical = (a.typical ?? 101) - (b.typical ?? 101);
  if (typical !== 0) return typical;
  const best = (a.best ?? 101) - (b.best ?? 101);
  if (best !== 0) return best;
  const engagement = (b.engagementRateMultiple ?? 0) - (a.engagementRateMultiple ?? 0);
  if (engagement !== 0) return engagement;
  return b.comments - a.comments;
}

/* Rank both windows with the same comparator; the diff is the real move chip.
   Entities with no prior-window posts are entrants (rankMove null). */
function rankRows(rows: BoardRow[], priorRows: BoardRow[]) {
  const priorRanks = new Map(
    [...priorRows]
      .filter((row) => row.postCount > 0)
      .sort(boardOrder)
      .map((row, index) => [row.id, index + 1]),
  );
  return [...rows]
    .sort(boardOrder)
    .slice(0, 10)
    .map((row, index) => {
      const rank = index + 1;
      const priorRank = priorRanks.get(row.id) ?? null;
      return { ...row, rank, rankMove: priorRank == null ? null : priorRank - rank };
    });
}

function buildRows(feeds: RuntimeFeed[], activeFeedId: string, days: number) {
  const now = Date.now();
  const spanMs = days * 24 * 60 * 60 * 1000;

  if (activeFeedId === 'all') {
    const build = (windowed: (posts: TrackedPost[]) => TrackedPost[], prevWindowed: (posts: TrackedPost[]) => TrackedPost[], followerWindow: [number, number]) =>
      feeds.map((feed) => {
        const followers = feed.feeders.reduce((sum, feeder) => sum + numeric(feeder.followerCount), 0);
        return createBoardRow({
          id: `feed:${feed.id}`,
          name: titleCase(feed.title),
          caption: `${feed.feeders.length} ${feed.feeders.length === 1 ? 'feeder' : 'feeders'}`,
          feedName: titleCase(feed.title),
          profilePicUrl: feed.feeders[0]?.profilePicUrl || null,
          followers,
          followerDelta: windowDelta(feed.ascent, followerWindow[0], followerWindow[1]) ?? ascentDelta(feed.ascent),
          posts: windowed(feed.posts),
          prevPosts: prevWindowed(feed.posts),
        });
      });
    const current = build((posts) => postsInWindow(posts, days), (posts) => postsInPriorWindow(posts, days), [now - spanMs, now]);
    const prior = build((posts) => postsInPriorWindow(posts, days), () => [], [now - spanMs * 2, now - spanMs]);
    return rankRows(current, prior);
  }

  const feed = feeds.find((candidate) => candidate.id === activeFeedId);
  if (!feed) return [];

  const buildFeeders = (windowed: (posts: TrackedPost[]) => TrackedPost[], prevWindowed: (posts: TrackedPost[]) => TrackedPost[]) =>
    feed.feeders.map((feeder) => {
      const handle = normalizeHandle(feeder.handle);
      const forHandle = (posts: TrackedPost[]) => posts.filter((post) => normalizeHandle(post.handle) === handle);
      return createBoardRow({
        id: `feeder:${feed.id}:${handle}`,
        name: `@${handle}`,
        caption: titleCase(feed.title),
        feedName: titleCase(feed.title),
        profilePicUrl: feeder.profilePicUrl || null,
        followers: numeric(feeder.followerCount),
        followerDelta: null,
        posts: forHandle(windowed(feed.posts)),
        prevPosts: forHandle(prevWindowed(feed.posts)),
      });
    });
  const current = buildFeeders((posts) => postsInWindow(posts, days), (posts) => postsInPriorWindow(posts, days));
  const prior = buildFeeders((posts) => postsInPriorWindow(posts, days), () => []);
  return rankRows(current, prior);
}

/* What each part of the page shows for one scope and window, built once (see remember) and shared: the leaders and
   the board both read the rows, the board, the cards, the facts and the post mortem the scope's posts. */
function rowsFor(feeds: RuntimeFeed[], activeFeedId: string, days: number) {
  return remember(feeds, null, `rows|${activeFeedId}|${days}`, () => buildRows(feeds, activeFeedId, days));
}

function tapeFor(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number) {
  return remember(feeds, null, `tape|${activeFeedId}|${feederHandle}|${days}`, () => buildTapeEvents(feeds, activeFeedId, feederHandle, days));
}

function leadersFor(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number) {
  return remember(feeds, null, `leaders|${activeFeedId}|${feederHandle}|${days}`, () => buildLeaderCards(rowsFor(feeds, activeFeedId, days), feeds, activeFeedId, days, feederHandle));
}

function postsFor(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number): ScopedPost[] {
  return remember(feeds, null, `posts|${activeFeedId}|${feederHandle}|${days}`, () => {
    const scoped = scopedPostsFor(feeds, activeFeedId, days);
    return feederHandle ? scoped.filter((post) => normalizeHandle(post.handle) === feederHandle) : scoped;
  });
}

/* What the facts and the post mortem read: the scope's posts dated in the window by their stored date, 500 at most
   (as their API routes read them) */
function factBasis(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number) {
  return remember(feeds, null, `basis|${activeFeedId}|${feederHandle}|${days}`, () => {
    const cutoff = Date.now() - days * 86_400_000;
    return postsFor(feeds, activeFeedId, feederHandle, days).slice(0, 500).filter((post) => {
      const time = Date.parse(post.postedAt || '');
      return Number.isFinite(time) && time >= cutoff;
    });
  });
}

/* the facts for one scope and window: the deterministic builder (lib/feederboardFacts) run on what the page already
   holds, so a switch waits on no request */
function factsFor(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number): FeederboardFact[] {
  return remember(feeds, null, `facts|${activeFeedId}|${feederHandle}|${days}`, () => buildFeederboardFacts(factBasis(feeds, activeFeedId, feederHandle, days), days) as FeederboardFact[]);
}

/* Everything a switch could ask for next, built ahead while the page is idle: the scope in view in every range
   first, then the other scopes in the range in view, then the rest. A switch then only draws. */
function warmJobs(feeds: RuntimeFeed[], activity: LeadActivity | null, feedId: string, feeder: string, days: number) {
  const scopes: Array<[string, string]> = [[feedId, feeder], ['all', ''], ...feeds.map((feed): [string, string] => [feed.id, ''])];
  const open = feeds.find((feed) => feed.id === feedId);
  for (const one of open?.feeders || []) scopes.push([feedId, normalizeHandle(one.handle)]);
  const ranges = [days, ...TIMEFRAMES.filter((range) => range !== days)];
  const order: Array<[string, string, number]> = [
    ...ranges.map((range): [string, string, number] => [feedId, feeder, range]),
    ...scopes.map(([id, handle]): [string, string, number] => [id, handle, days]),
    ...ranges.flatMap((range) => scopes.map(([id, handle]): [string, string, number] => [id, handle, range])),
  ];
  const seen = new Set<string>();
  const jobs: Array<() => void> = [];
  for (const [id, handle, range] of order) {
    const key = `${id}|${handle}|${range}`;
    if (seen.has(key)) continue;
    seen.add(key);
    const scope = { feeds, activity, feedId: id, feeder: handle, days: range };
    jobs.push(
      () => rowsFor(feeds, id, range),
      () => leadersFor(feeds, id, handle, range),
      () => tapeFor(feeds, id, handle, range),
      () => postsFor(feeds, id, handle, range),
      () => factsFor(feeds, id, handle, range),
      () => growthFor(scope),
      () => rhythmFor(scope),
      () => timingFor(scope),
      () => formatsFor(scope),
    );
  }
  return jobs;
}

/* ── Shared motion primitives ────────────────────────────────────────────── */

/* ── Chrome ──────────────────────────────────────────────────────────────── */

/* What the app's one tab header (TabHeader, the same one Read feeds) shows while Lead is on screen: LEAD · where
   you are, the time range on the right, and the story rail of feeds and their feeders. A feeder tapped again goes
   back to its feed. */
function LeadHeader({
  feeds,
  days,
  compressed,
  pending,
  onFeedChange,
  onFeederChange,
  onDaysChange,
}: {
  feeds: RuntimeFeed[];
  days: Timeframe;
  compressed: boolean;
  pending: boolean;
  onFeedChange: (id: string) => void;
  onFeederChange: (handle: string) => void;
  onDaysChange: (days: Timeframe) => void;
}) {
  // the header reads the pick itself, so the rail answers a tap at once while the page follows in the background
  const { activeFeedId, activeFeeder } = resolveScope(feeds, useTabScope());
  const activeFeed = feeds.find((feed) => feed.id === activeFeedId) || null;
  const railFeeds = useMemo(() => feeds.map((feed) => ({
    id: feed.id,
    title: feed.title,
    feeders: feed.feeders.map((feeder) => ({ handle: normalizeHandle(feeder.handle), profilePicUrl: feeder.profilePicUrl })),
  })), [feeds]);
  const scopeLabel = activeFeeder
    ? `@${activeFeeder}`
    : activeFeed ? titleCase(activeFeed.title) : 'All feeds';

  useTabHeader('lead', {
    title: 'LEAD',
    eyebrow: 'Feederboard',
    label: scopeLabel,
    control: <TimeframeControl id="lead" value={days} options={TIMEFRAMES} pending={pending} applyMs={MENU_APPLY_AFTER_CLOSE_MS} onChange={onDaysChange} />,
    controlLabel: 'Time range',
    compressed,
    // none yet means still loading (the header keeps the rail it has)
    feeds: railFeeds.length ? railFeeds : null,
    feedId: activeFeed?.id ?? null,
    handle: activeFeeder || null,
    feederLabel: (handle) => `Spotlight @${handle}`,
    onAll: () => onFeedChange('all'),
    onFeed: (id) => (id === activeFeed?.id ? onFeederChange('') : onFeedChange(id)),
    onFeeder: (handle) => onFeederChange(activeFeeder === handle ? '' : handle),
  });
  return null;
}

/* ── The tape — the event stream that opens the page ────────────────────── */

type TapeEvent = { id: string; text: string; value: string; dir: 'up' | 'down' | 'flat' };

/* Deterministic market events from data already on hand: checkpoint climbs
   and cools, fresh reads, and each feed's latest follower day-move. Band
   crossings come later from the rank ledger. */
function buildTapeEvents(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number): TapeEvent[] {
  const scoped = activeFeedId === 'all' ? feeds : feeds.filter((feed) => feed.id === activeFeedId);
  const inScope = (post: TrackedPost) => !feederHandle || normalizeHandle(post.handle) === feederHandle;
  const climbs: TapeEvent[] = [];
  const reads: TapeEvent[] = [];
  const seen = new Set<string>();
  const freshCutoff = Date.now() - 2 * 24 * 60 * 60 * 1000;

  for (const feed of scoped) {
    for (const post of postsInWindow(feed.posts, days).filter(inScope)) {
      const handle = normalizeHandle(post.handle) || titleCase(feed.title).toLowerCase();
      const media = post.mediaType !== 'unknown' ? post.mediaType : 'post';
      const trajectory = climbOf(post);
      if (trajectory != null && Math.abs(trajectory) >= 5 && post.firstPercentile != null && post.latestPercentile != null) {
        const intoTopTen = trajectory > 0 && post.latestPercentile <= 10 && post.firstPercentile > 10;
        climbs.push({
          id: `climb:${post.postKey}`,
          text: `@${handle} ${media} ${trajectory > 0 ? (intoTopTen ? 'into the top 10%' : 'climbing') : 'cooling'}`,
          value: `${percentLabel(post.firstPercentile)}% → ${percentLabel(post.latestPercentile)}%`,
          dir: trajectory > 0 ? 'up' : 'down',
        });
        seen.add(post.postKey);
        continue;
      }
      const readTime = Date.parse(post.latestBusinessDayIst || '');
      if (Number.isFinite(readTime) && readTime >= freshCutoff && post.latestPercentile != null && !seen.has(post.postKey)) {
        reads.push({
          id: `read:${post.postKey}`,
          text: `@${handle} ${(post.latestCheckpoint || 'd3').toUpperCase()} read`,
          value: `TOP ${percentLabel(post.latestPercentile)}%`,
          dir: post.latestPercentile <= 20 ? 'up' : 'flat',
        });
      }
    }
  }

  const followerMoves: TapeEvent[] = scoped.flatMap((feed) => {
    const sorted = [...feed.ascent]
      .map((point) => ({ time: Date.parse(point.snapshot_date_ist), value: numeric(point.follower_count) }))
      .filter((point) => Number.isFinite(point.time))
      .sort((a, b) => a.time - b.time);
    if (sorted.length < 2) return [];
    const delta = sorted[sorted.length - 1].value - sorted[sorted.length - 2].value;
    if (delta === 0) return [];
    return [{
      id: `followers:${feed.id}`,
      text: `${titleCase(feed.title)} audience`,
      value: `${signedNumber(delta)} today`,
      dir: delta > 0 ? 'up' as const : 'down' as const,
    }];
  });

  return [...climbs, ...followerMoves, ...reads].slice(0, 14);
}

const TAPE_CSS = `
@keyframes fm-lead-tape-scroll{0%{transform:translate3d(0,0,0)}100%{transform:translate3d(-50%,0,0)}}
`;

function TapeChip({ event }: { event: TapeEvent }) {
  return (
    <span className="mr-6 flex shrink-0 items-center gap-2 border-r border-fg/[0.07] pr-6 last:border-0">
      <span className={cn('text-[8px]', event.dir === 'up' ? 'text-[var(--fm-accent-text)]' : DIM)}>
        {event.dir === 'up' ? '▲' : event.dir === 'down' ? '▼' : '●'}
      </span>
      <span className={cn('whitespace-nowrap', NOTE, SOFT)}>{event.text}</span>
      <span className={cn(
        'whitespace-nowrap rounded-[8px] px-1.5 py-0.5 text-[12px] font-black tabular-nums',
        event.dir === 'up'
          ? 'bg-[var(--fm-accent)] text-white'
          : event.dir === 'down'
            ? cn('border border-fg/8 bg-fg/[0.05]', DIM)
            : cn('border border-fg/8 bg-fg/[0.03]', SOFT),
      )}>{event.value}</span>
    </span>
  );
}

function LeadTape({ events, dealId }: { events: TapeEvent[]; dealId: string }) {
  const reduce = Boolean(useReducedMotion());
  // the tape only runs while it is on screen: a loop nobody can see still costs every frame
  const tapeRef = useRef<HTMLDivElement | null>(null);
  const [onScreen, setOnScreen] = useState(true);
  useEffect(() => {
    const el = tapeRef.current;
    if (!el) return undefined;
    const observer = new IntersectionObserver(([entry]) => setOnScreen(entry.isIntersecting));
    observer.observe(el);
    return () => observer.disconnect();
  }, [events.length]);
  if (!events.length) return null;
  const duration = Math.max(22, events.length * 4.2);

  return (
    <div ref={tapeRef} data-testid="lead-v3-tape" className="relative -mx-3 mb-3 overflow-hidden border-y border-fg/[0.08] bg-(--ld-panel) py-2.5 sm:mx-0 sm:rounded-[16px] sm:border">
      <style>{TAPE_CSS}</style>
      {reduce ? (
        <div className="flex overflow-x-auto px-4 [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
          {events.map((event) => <TapeChip key={event.id} event={event} />)}
        </div>
      ) : (
        <AnimatePresence mode="popLayout" initial={false}>
          <motion.div
            key={dealId}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1, transition: beatArrive(0) }}
            exit={{ opacity: 0, transition: beatLeave(0) }}
            className="flex w-max hover:[animation-play-state:paused]"
            style={{ animation: `fm-lead-tape-scroll ${duration}s linear infinite`, animationPlayState: onScreen ? undefined : 'paused' }}
          >
            <div className="flex pl-4">
              {events.map((event) => <TapeChip key={event.id} event={event} />)}
            </div>
            <div className="flex pl-4" aria-hidden="true">
              {events.map((event) => <TapeChip key={`dup:${event.id}`} event={event} />)}
            </div>
          </motion.div>
        </AnimatePresence>
      )}
      <span aria-hidden="true" className="pointer-events-none absolute inset-y-0 left-0 w-8 bg-[linear-gradient(90deg,var(--ld-panel),transparent)]" />
      <span aria-hidden="true" className="pointer-events-none absolute inset-y-0 right-0 w-8 bg-[linear-gradient(270deg,var(--ld-panel),transparent)]" />
    </div>
  );
}

/* ── The wire — one server-read line that captions the run ──────────────── */
/* Deterministic composer for the preview; run-signals `run_bites` plugs into
   this same slot. Direction and shape only — no numbers repeated, no "why". */

function composeWireLine(rows: BoardRow[]): string | null {
  if (!rows.length) return null;
  const carrier = rows[0];
  const movers = rows.filter((row) => (row.typicalShift ?? 0) > 0).sort((a, b) => (b.typicalShift ?? 0) - (a.typicalShift ?? 0));
  const faders = rows.filter((row) => (row.typicalShift ?? 0) < 0).sort((a, b) => (a.typicalShift ?? 0) - (b.typicalShift ?? 0));
  const parts: string[] = [];
  if (movers[0]?.id === carrier.id) {
    parts.push(`${carrier.name} pulling away at the top`);
  } else {
    parts.push(`${carrier.name} holds the board`);
    if (movers[0]) parts.push(`${movers[0].name} closing in`);
  }
  const fader = faders.find((row) => row.id !== carrier.id);
  if (fader) parts.push(`${fader.name} cooling off`);
  const entrant = rows.find((row) => row.rankMove == null && row.postCount > 0 && row.id !== carrier.id);
  if (parts.length < 3 && entrant) parts.push(`${entrant.name} enters the window`);
  return `${parts.slice(0, 3).join(' · ')}.`;
}

function composeFeederWireLine(posts: TrackedPost[]): string | null {
  if (!posts.length) return null;
  const climbing = posts.filter((post) => (climbOf(post) ?? 0) >= 5).length;
  const cooling = posts.filter((post) => (climbOf(post) ?? 0) <= -5).length;
  const latest = [...posts].sort((a, b) => postTime(b) - postTime(a))[0];
  const parts: string[] = [];
  if (climbing) parts.push(`${climbing} ${climbing === 1 ? 'post' : 'posts'} still climbing`);
  if (cooling) parts.push(`${cooling} cooling`);
  if (latest && latest.latestPercentile != null) parts.push(`latest drop sits top ${percentLabel(latest.latestPercentile)}%`);
  return parts.length ? `${parts.slice(0, 3).join(' · ')}.` : null;
}

function WireLine({ line, dealId }: { line: string | null; dealId: string }) {
  if (!line) return null;
  return (
    <div data-testid="lead-v3-wire" className="flex items-center gap-3 border-b border-fg/[0.08] bg-(--ld-deep) px-4 py-3 sm:px-5">
      <span className={cn('shrink-0 rounded-full bg-fg px-2 py-0.5 text-(--fm-page)', MARK)}>Wire</span>
      {/* the old line and the new share one cell (no stylesheet popped in and out mid-switch); two lines' room
          whatever the line says, so a shorter one doesn't pull the board up */}
      <div className="grid min-w-0 flex-1">
        <AnimatePresence initial={false}>
          <motion.p key={`${dealId}:${line}`} initial={{ opacity: 0, transform: 'translateY(6px)' }} animate={{ opacity: 1, transform: 'translateY(0px)', transition: beatArrive(0) }} exit={{ opacity: 0, transform: 'translateY(-6px)', transition: beatLeave(0) }} className="[grid-area:1/1] min-h-[2lh] min-w-0 text-[12px] font-bold leading-snug text-fg/72 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden">{line}</motion.p>
        </AnimatePresence>
      </div>
    </div>
  );
}

/* ── Leaders strip — three cards, three different questions ─────────────── */

type LeaderCard = {
  key: string;
  eyebrow: string;
  value: string;
  valuePrefix?: string;
  valueSuffix?: string;
  hint: string;
  name: string;
  detail: string;
  thumb: string | null;
  collage?: Array<string>;
  shift?: { up: boolean; text: string } | null;
};

function buildLeaderCards(rows: BoardRow[], feeds: RuntimeFeed[], activeFeedId: string, days: number, feederHandle: string): LeaderCard[] {
  const scoped = activeFeedId === 'all' ? feeds : feeds.filter((feed) => feed.id === activeFeedId);
  const inScope = (post: TrackedPost) => !feederHandle || normalizeHandle(post.handle) === feederHandle;
  const scopedPosts = scoped
    .flatMap((feed) => postsInWindow(feed.posts, days).filter(inScope).map((post) => ({ post, feedName: titleCase(feed.title) })))
    .sort((a, b) => bestFirst(a.post, b.post));
  const priorPosts = scoped
    .flatMap((feed) => postsInPriorWindow(feed.posts, days).filter(inScope))
    .sort(bestFirst);

  const cards: LeaderCard[] = [];

  const position = scopedPosts[0] || null;
  if (position) {
    const priorBest = priorPosts[0]?.latestPercentile ?? null;
    const currentBest = position.post.latestPercentile;
    cards.push({
      key: 'position',
      eyebrow: 'Best post',
      value: percentLabel(currentBest),
      valuePrefix: 'TOP',
      valueSuffix: '%',
      hint: 'in the stream',
      name: position.post.handle ? `@${normalizeHandle(position.post.handle)}` : position.feedName,
      detail: `${(position.post.latestCheckpoint || 'd3').toUpperCase()} · ${proofMetric(position.post)}`,
      thumb: position.post.thumbnailUrl,
      shift: priorBest == null || currentBest == null
        ? null
        : { up: currentBest < priorBest, text: `prior best ${percentLabel(priorBest)}%` },
    });
  }

  const typical = median(scopedPosts.map(({ post }) => post.latestPercentile));
  const priorTypical = median(priorPosts.map((post) => post.latestPercentile));
  const typicalDelta = typical != null && priorTypical != null ? Math.round(priorTypical - typical) : null;
  cards.push({
    key: 'typical',
    eyebrow: 'Typical post',
    value: percentLabel(typical),
    valuePrefix: 'TOP',
    valueSuffix: '%',
    hint: 'the median',
    name: feederHandle ? 'its running level' : rows[0] ? `${rows[0].name} leads` : '—',
    detail: `${scopedPosts.length} posts · ${days}D`,
    thumb: null,
    collage: scopedPosts.map(({ post }) => post.thumbnailUrl).filter((url): url is string => Boolean(url)).slice(0, 4),
    shift: typicalDelta == null || typicalDelta === 0
      ? null
      : { up: typicalDelta > 0, text: `from ${percentLabel(priorTypical)}%` },
  });

  const now = Date.now();
  const spanMs = days * 24 * 60 * 60 * 1000;
  const currentDeltas = scoped.map((feed) => windowDelta(feed.ascent, now - spanMs, now)).filter((value): value is number => value != null);
  const priorDeltas = scoped.map((feed) => windowDelta(feed.ascent, now - spanMs * 2, now - spanMs)).filter((value): value is number => value != null);
  const netDelta = currentDeltas.length ? currentDeltas.reduce((sum, value) => sum + value, 0) : null;
  const priorNet = priorDeltas.length ? priorDeltas.reduce((sum, value) => sum + value, 0) : null;
  const strongestWindowPost = scopedPosts.find(({ post }) => post.thumbnailUrl) || null;
  cards.push({
    key: 'payoff',
    eyebrow: 'Gains',
    value: netDelta == null
      ? compactNumber(rows.reduce((sum, row) => sum + row.followers, 0))
      : signedNumber(netDelta),
    hint: netDelta == null ? 'followers' : 'net followers',
    name: netDelta == null ? 'across the board' : (rows.find((row) => (row.followerDelta ?? 0) > 0)?.name || 'across the board'),
    detail: `${days}D window`,
    thumb: strongestWindowPost?.post.thumbnailUrl || null,
    shift: netDelta == null || priorNet == null
      ? null
      : { up: netDelta >= priorNet, text: `from ${signedNumber(priorNet)}` },
  });

  // the post that climbed furthest between its first read and its latest
  const climber = scopedPosts
    .map(({ post }) => ({ post, climb: climbOf(post) ?? 0 }))
    .filter((entry) => entry.climb > 0)
    .sort((a, b) => b.climb - a.climb)[0] || null;
  cards.push({
    key: 'climber',
    eyebrow: 'Climber',
    value: climber ? String(Math.round(climber.climb)) : '—',
    valuePrefix: climber ? '▲' : undefined,
    hint: 'places since D1',
    name: climber?.post.handle ? `@${normalizeHandle(climber.post.handle)}` : 'nothing climbed',
    detail: climber ? `${percentLabel(climber.post.firstPercentile ?? null)}% → ${percentLabel(climber.post.latestPercentile)}%` : `${days}D window`,
    thumb: climber?.post.thumbnailUrl || null,
    shift: null,
  });

  // the post people talked about most, against its own usual
  const talked = scopedPosts
    .filter(({ post }) => post.commentsMultiple != null && post.commentsMultiple > 0)
    .sort((a, b) => (b.post.commentsMultiple ?? 0) - (a.post.commentsMultiple ?? 0))[0] || null;
  cards.push({
    key: 'talked',
    eyebrow: 'Talked about',
    value: talked ? formatMultiple(talked.post.commentsMultiple) : '—',
    hint: 'comments vs usual',
    name: talked?.post.handle ? `@${normalizeHandle(talked.post.handle)}` : 'no comments read',
    detail: talked ? `${compactNumber(numeric(talked.post.comments))} comments` : `${days}D window`,
    thumb: talked?.post.thumbnailUrl || null,
    shift: null,
  });

  return cards;
}

function LeadersStrip({ cards }: { cards: LeaderCard[] }) {
  const reduce = Boolean(useReducedMotion());
  if (!cards.length) return null;

  return (
    <motion.div
      variants={BEAT_CONTAINER}
      initial={reduce ? false : 'hidden'}
      animate="visible"
      data-reveal="dealt"
      className="-mx-3 flex snap-x snap-mandatory scroll-px-4 gap-3 overflow-x-auto px-4 pb-2 [scrollbar-width:none] sm:mx-0 sm:scroll-px-0 sm:px-0 lg:grid lg:grid-cols-5 lg:overflow-visible lg:pb-0 [&::-webkit-scrollbar]:hidden"
    >
      {cards.map((card) => (
        <motion.article
          key={card.key}
          variants={BEAT_ITEM}
          data-testid={`lead-v3-leader-${card.key}`}
          className="relative aspect-[4/5] w-[64vw] min-w-[64vw] max-w-[300px] snap-center overflow-hidden rounded-[22px] border border-white/10 bg-[#0a0a0a] sm:w-[260px] sm:min-w-[260px] lg:w-auto lg:min-w-0 lg:max-w-none"
        >
          {card.thumb ? (
            <CrossfadeImage src={card.thumb} eager settle />
          ) : card.collage?.length ? (
            // a 4:5 card holds four 4:5 pictures exactly
            <div className="absolute inset-0 grid grid-cols-2 grid-rows-2 gap-px">
              {[0, 1, 2, 3].map((index) => (
                <span key={`${card.key}:${index}`} className="relative block h-full w-full overflow-hidden bg-white/[0.04] opacity-55">
                  <CrossfadeImage src={card.collage?.[index] || null} />
                </span>
              ))}
            </div>
          ) : null}
          <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(4,4,4,.7)_0%,rgba(4,4,4,.28)_38%,rgba(4,4,4,.9)_82%)]" />

          <div className="relative z-10 flex h-full flex-col justify-between p-3.5 sm:p-4">
            <div className="min-w-0">
              <div className={cn('truncate text-white/80', MARK, 'tracking-[0.18em]')}>{card.eyebrow}</div>
              <div className={cn('mt-0.5 truncate text-white/78', NOTE)}>{card.hint}</div>
            </div>
            <div className="min-w-0">
              <div className="fm-depth-title ld-on-media flex items-end font-black leading-[0.78] tracking-[-0.05em] text-white">
                {card.valuePrefix ? <span className="mr-1.5 pb-1 text-[12px] tracking-normal text-white/78">{card.valuePrefix}</span> : null}
                <SlotText value={card.value} className="text-[48px] tabular-nums lg:text-[46px] xl:text-[54px] laptop:text-[40px]" />
                {card.valueSuffix ? <span className="ml-0.5 pb-1 text-[22px] text-[var(--fm-accent-bright)]">{card.valueSuffix}</span> : null}
              </div>
              <div className={cn('mt-2 h-4 truncate', NOTE, card.shift?.up ? 'text-white/90' : 'text-white/72')}>
                {card.shift ? <><span className={cn('mr-1 text-[8px] leading-none', card.shift.up && 'text-[var(--fm-accent)]')}>{card.shift.up ? '▲' : '▼'}</span><Swap value={card.shift.text} className="align-top" /></> : null}
              </div>
              <div className="mt-2.5 border-t border-white/[0.1] pt-2.5">
                <div className="truncate text-[12px] font-black text-white"><Swap value={card.name} /></div>
                <div className={cn('mt-0.5 truncate text-white/72', NOTE)}><Swap value={card.detail} /></div>
              </div>
            </div>
          </div>
        </motion.article>
      ))}
    </motion.div>
  );
}

/* ── Feederboard rows ────────────────────────────────────────────────────── */

/* bring a piece into view clear of the header (which floats over the page) and the nav below */
function revealBelowHeader(el: HTMLElement) {
  const header = document.querySelector('.fm-tab-header-shell')?.getBoundingClientRect();
  const top = (header?.bottom ?? 0) + 12;
  const bottom = window.innerHeight - 110;
  const rect = el.getBoundingClientRect();
  let delta = 0;
  if (rect.top < top) delta = rect.top - top;
  else if (rect.bottom > bottom) delta = Math.min(rect.bottom - bottom, rect.top - top);
  if (Math.abs(delta) < 2) return;
  const page = el.closest<HTMLElement>('[data-testid="lead-v3-page"]');
  if (page && page.scrollHeight > page.clientHeight + 10) page.scrollBy({ top: delta, behavior: 'smooth' });
  else window.scrollBy({ top: delta, behavior: 'smooth' });
}

function RowThumb({ row }: { row: BoardRow }) {
  return (
    <span className={cn(
      'relative block shrink-0 overflow-hidden rounded-[14px] border border-fg/12 bg-(--ld-thumb) shadow-(--ld-shadow-thumb)',
      'h-[56px] w-[45px]',
    )}>
      {!row.anchorThumb && (
        <span className="absolute inset-0 grid place-items-center p-1.5">
          <FeederStoryAvatar feeder={{ handle: row.name, profilePicUrl: row.profilePicUrl }} className="text-[11px]" />
        </span>
      )}
      <CrossfadeImage src={row.anchorThumb} />
    </span>
  );
}

/* the verdict word, in a box as wide as the longest one: the caption beside it keeps its width as the word rolls.
   On a phone it heads the numbers' column instead (`end`), over the typical post it judges */
function RowVerdict({ row, delay = 0, end = false }: { row: BoardRow; delay?: number; end?: boolean }) {
  return (
    <span className={cn('block w-[60px] shrink-0 overflow-hidden whitespace-nowrap', MARK, 'leading-none', end && 'ml-auto text-right', row.hot ? 'text-fg' : DIM)}>
      <SlotText value={row.verdict} align={end ? 'right' : 'left'} delay={delay} />
    </span>
  );
}

/* Rank movement vs the prior equal window. Accent up, dim down, NEW for
   entrants — the market grammar, never green/red. It sits in a box of its own width whatever it says, so the name
   beside it never re-truncates as it changes. */
function MoveChip({ move, className }: { move: number | null; className?: string }) {
  if (move == null) {
    return <span className={cn('inline-flex w-[40px] shrink-0 items-center', className)}><span className={cn('rounded-[7px] bg-fg/[0.07] px-1.5 py-0.5 tracking-[0.08em]', MARK, DIM)}>new</span></span>;
  }
  const up = move > 0;
  const down = move < 0;
  return (
    <span
      className={cn(
        'inline-flex w-[40px] shrink-0 items-center gap-0.5 text-[12px] font-black tabular-nums leading-none',
        up ? 'text-fg' : DIM,
        className,
      )}
    >
      {/* a rise: the ▲ carries the accent, the number stays ink (small type is ink) */}
      <span className={cn('text-[8px]', up && 'text-[var(--fm-accent)]')}>{up ? '▲' : down ? '▼' : '–'}</span>
      {move === 0 ? '' : Math.abs(move)}
    </span>
  );
}

/* a rolling number in a box at least `min` wide, its digits against the box's right edge: what follows it (a %, a
   unit) stays put while the number rolls, whatever it rolls to (SlotText's own width is its glyphs', and its display
   is fixed, so the box is a wrapper) */
function NumberBox({ value, delay = 0, min = '2ch', className }: { value: string; delay?: number; min?: string; className?: string }) {
  return (
    <span className={cn('inline-flex justify-end', className)} style={{ minWidth: min }}>
      <SlotText value={value} delay={delay} />
    </span>
  );
}

function MetricLedger({ row }: { row: BoardRow }) {
  const metrics = [
    { label: 'Comments', value: compactNumber(row.comments), multiple: row.commentsMultiple },
    { label: 'Likes', value: compactNumber(row.likes), multiple: row.likesMultiple },
    { label: 'Eng. rate', value: formatEngagementRate(row.engagementRate), multiple: row.engagementRateMultiple },
    { label: row.followerDelta == null ? 'Followers' : 'Follower move', value: row.followerDelta == null ? compactNumber(row.followers) : signedNumber(row.followerDelta), multiple: null },
  ];
  return (
    <motion.dl variants={OPEN_ITEM} className="order-3 -mx-4 flex min-w-0 overflow-x-auto border-y border-fg/[0.08] px-4 [scrollbar-width:none] sm:-mx-6 sm:px-6 lg:order-2 lg:mx-0 lg:grid lg:grid-cols-2 lg:gap-x-6 lg:gap-y-7 lg:overflow-visible lg:border-y-0 lg:border-l lg:border-fg/[0.08] lg:px-0 lg:pl-7 [&::-webkit-scrollbar]:hidden">
      {metrics.map((metric) => (
        <div key={metric.label} className="min-w-[118px] border-r border-fg/[0.08] py-4 pr-4 last:border-r-0 lg:min-w-0 lg:border-r-0 lg:py-0 lg:pr-0">
          <dt className={cn('truncate', MARK, DIM)}>{metric.label}</dt>
          <dd className="fm-depth-title mt-2 truncate text-[22px] font-black tabular-nums leading-none text-fg sm:text-[26px]"><SlotText value={metric.value} /></dd>
          <div className={cn('mt-2 tabular-nums', NOTE, SOFT)}>
            {metric.multiple == null ? 'raw movement' : `${formatMultiple(metric.multiple)} usual`}
          </div>
        </div>
      ))}
    </motion.dl>
  );
}

/* An open row's receipts. The ref (AnimatePresence's, in popLayout) is its element's: closing, it leaves the flow at
   once and fades where it was while the rows under it travel up over it (components/lead/ledgerMotion). */
function RowExpansion({ row, days, onCollapse, ref }: { row: BoardRow; days: number; onCollapse: () => void; ref?: Ref<HTMLDivElement> }) {
  const reduce = Boolean(useReducedMotion());

  return (
    <motion.div
      ref={ref}
      data-testid="lead-v3-expansion"
      initial={reduce ? false : { opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0, transition: { duration: sec(0.16), ease: BEAT_LEAVE_EASE } }}
      transition={{ duration: reduce ? 0 : sec(0.2), ease: SOFT_EASE }}
      className="overflow-hidden"
    >
      <motion.div
        variants={OPEN_CONTAINER}
        initial={reduce ? false : 'hidden'}
        animate="visible"
        onClick={onCollapse}
        className="relative isolate cursor-pointer overflow-hidden border-t border-fg/[0.1] bg-(--ld-raised) p-4 sm:p-6 lg:min-h-[360px]"
      >
        <div className="pointer-events-none absolute inset-y-0 left-0 w-1 bg-fg" />
        <div className="pointer-events-none absolute -bottom-14 left-3 select-none text-[164px] font-black leading-none tracking-[-0.1em] text-fg/[0.025] sm:text-[210px]">{rankLabel(row.rank)}</div>

        <motion.div variants={OPEN_GRID} className="relative grid gap-6 lg:grid-cols-[210px_minmax(240px,.78fr)_minmax(400px,1.3fr)] lg:items-center lg:gap-8">
          <motion.div variants={OPEN_ITEM} className="order-1 min-w-0">
            <div className={cn('flex flex-wrap items-baseline justify-between gap-x-3 gap-y-1', NOTE, DIM)}>
              <span>
                {row.postCount} posts · {days}D
                {row.prevPostCount != null ? (
                  <i className={cn('ml-1.5 not-italic', row.postCount > row.prevPostCount ? 'text-fg' : DIM)}>
                    {row.postCount === row.prevPostCount ? 'same pace' : row.postCount > row.prevPostCount ? `up from ${row.prevPostCount}` : `down from ${row.prevPostCount}`}
                  </i>
                ) : null}
              </span>
              <i className="not-italic">tap outside media to close</i>
            </div>

            <div className="mt-5 flex items-end gap-4 lg:mt-7 lg:block">
              <div className="min-w-0">
                <div className={cn(MARK, DIM)}>Range · best post</div>
                <div className="fm-depth-title mt-2 flex items-end font-black leading-[0.68] tracking-[-0.08em] text-fg">
                  <span className={cn('mr-2 pb-2 text-[12px] tracking-normal', DIM)}>TOP</span>
                  <SlotText value={percentLabel(row.best)} className="text-[74px] tabular-nums sm:text-[96px] lg:text-[108px]" />
                  {row.best != null ? <span className="ml-1 pb-1 text-[30px] tracking-normal text-[var(--fm-accent-text)] lg:text-[40px]">%</span> : null}
                </div>
              </div>
              <div className="shrink-0 border-l border-fg/10 pl-4 lg:mt-4 lg:border-l-0 lg:border-t lg:border-fg/10 lg:pl-0 lg:pt-4">
                <div className="fm-depth-title flex items-baseline text-[30px] font-black leading-none tracking-[-0.05em] text-fg/56">
                  <SlotText value={percentLabel(row.floor)} className="tabular-nums" />
                  {row.floor != null ? <i className={cn('ml-0.5 not-italic text-[15px]', DIM)}>%</i> : null}
                </div>
                <div className={cn('mt-1.5', MARK, DIM)}>floor · weakest post</div>
              </div>
            </div>
          </motion.div>

          <MetricLedger row={row} />

          <motion.div variants={OPEN_ITEM} className="order-2 min-w-0 lg:order-3">
            <div className="mb-3 flex items-center justify-between gap-3">
              <span className={cn(MARK, DIM)}>Posts behind the number</span>
              <span className={cn(MARK, DIM)}>Best first</span>
            </div>
            {row.proofPosts.length ? (
              <>
                <div className="flex snap-x snap-mandatory gap-3 overflow-x-auto pb-2 [scrollbar-width:none] sm:hidden [&::-webkit-scrollbar]:hidden">
                  {row.proofPosts.slice(0, 3).map((post, index) => (
                    <a
                      key={post.postKey}
                      href={post.postUrl || '#'}
                      target={post.postUrl ? '_blank' : undefined}
                      rel={post.postUrl ? 'noreferrer' : undefined}
                      onClick={(event) => {
                        event.stopPropagation();
                        if (!post.postUrl) event.preventDefault();
                      }}
                      data-testid={'lead-v3-proof-mobile-' + index}
                      aria-label={post.postUrl ? 'Open tracked post' : 'Tracked post'}
                      className="relative aspect-[4/5] w-[58%] min-w-[58%] snap-center overflow-hidden rounded-[22px] border border-white/12 bg-(--fm-media) shadow-(--ld-shadow-proof) focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
                    >
                      {post.thumbnailUrl ? (
                        // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
                        <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" loading="lazy" decoding="async" />
                      ) : null}
                      <div className="absolute inset-0 bg-[linear-gradient(180deg,transparent_30%,rgba(0,0,0,.9))]" />
                      <div className="absolute inset-x-0 bottom-0 flex items-end justify-between gap-3 p-4">
                        <span className="text-[13px] font-black text-white">{post.latestPercentile == null ? '—' : `TOP ${percentLabel(post.latestPercentile)}%`}</span>
                        <span className="text-[12px] font-semibold leading-4 text-white/80">{proofMetric(post)}</span>
                      </div>
                    </a>
                  ))}
                </div>

                <div className="hidden items-start gap-4 pb-3 sm:flex">
                  {row.proofPosts.slice(0, 3).map((post, index) => (
                    <motion.a
                      key={post.postKey}
                      href={post.postUrl || '#'}
                      target={post.postUrl ? '_blank' : undefined}
                      rel={post.postUrl ? 'noreferrer' : undefined}
                      onClick={(event) => {
                        event.stopPropagation();
                        if (!post.postUrl) event.preventDefault();
                      }}
                      data-testid={'lead-v3-proof-desktop-' + index}
                      aria-label={post.postUrl ? 'Open tracked post' : 'Tracked post'}
                      initial={reduce ? false : { opacity: 0, transform: `translateY(${index * 10 + 14}px)` }}
                      animate={{ opacity: 1, transform: `translateY(${index * 10}px)` }}
                      transition={reduce ? { duration: 0 } : beatArrive(index + 1)}
                      className="group relative aspect-[4/5] w-full max-w-[200px] flex-1 overflow-hidden rounded-[22px] border border-white/12 bg-(--fm-media) shadow-(--ld-shadow-proof-lg) focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
                    >
                      {post.thumbnailUrl ? (
                        // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
                        <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover transition-transform duration-700 group-hover:scale-[1.035]" loading="lazy" decoding="async" />
                      ) : null}
                      <div className="absolute inset-0 bg-[linear-gradient(180deg,transparent_28%,rgba(0,0,0,.9))]" />
                      <div className="absolute inset-x-0 bottom-0 flex items-end justify-between gap-3 p-4">
                        <span className="text-[13px] font-black text-white">{post.latestPercentile == null ? '—' : `TOP ${percentLabel(post.latestPercentile)}%`}</span>
                        <span className="text-[12px] font-semibold leading-4 text-white/80">{proofMetric(post)}</span>
                      </div>
                    </motion.a>
                  ))}
                </div>
              </>
            ) : (
              <div className={cn('flex h-[180px] items-center justify-center border-y border-fg/[0.07]', MARK, DIM)}>No tracked media in this window</div>
            )}
          </motion.div>
        </motion.div>
      </motion.div>
    </motion.div>
  );
}

/* The Feederboard: feeds (on All) or a feed's feeders ranked on their typical post, or, for one feeder, its own
   posts competing. One ledger whatever it ranks (components/lead/ledgerMotion): a new scope or window re-deals it
   as one cascade, first place first (rows that stay travel to their new places, their numbers roll on their place's
   beat, rows that go hand their places to the ones that come); a row opening makes room. A feeder's ladder always
   has its ten places (the ones its posts don't fill stay empty), so a window with fewer posts doesn't fold the
   page under it. */
const LADDER_PLACES = 10;
const NO_ROWS: BoardRow[] = [];

function rankLabel(rank: number) {
  return String(rank).padStart(2, '0');
}

/* the place's numeral, rolling to the row's new rank on its beat; on first place a rose "01" over it fades in on the same
   beat (opacity on the compositor: the ink itself never changes colour) */
function RankNumeral({ rank, delay, size }: { rank: number; delay: number; size: string }) {
  const at = 'pointer-events-none absolute left-0 top-1/2 -translate-y-1/2 select-none font-black tabular-nums leading-none tracking-[-0.08em]';
  return (
    <span className="relative h-full">
      <span className={cn(at, size, 'text-fg/[0.055]')}>
        <SlotText value={rankLabel(rank)} delay={delay} />
      </span>
      <span aria-hidden="true" className={cn('ld-throne', at, size, 'text-[var(--fm-accent)]/[0.2]')} style={{ opacity: rank === 1 ? 1 : 0 }}>
        {rankLabel(1)}
      </span>
    </span>
  );
}

/* The first place's light and the bar of a row's strength sit behind the row. The light is always there and lit on
   the row holding first place, so when first place changes hands it passes on the rows' beat instead of jumping. The
   bar is nearly invisible at rest; it glows as it sweeps to a new length (the ledger plays that). */
function RowGround({ throne, strength }: { throne: boolean; strength: number }) {
  return (
    <>
      <span aria-hidden="true" className="ld-throne pointer-events-none absolute inset-0 z-0 bg-[radial-gradient(circle_at_0_0,rgb(var(--fm-accent-rgb)/var(--ld-throne-glow)),transparent_44%),linear-gradient(90deg,rgb(var(--fm-fg-rgb)/.035),rgb(var(--fm-fg-rgb)/.01))]" style={{ opacity: throne ? 1 : 0 }} />
      <span
        aria-hidden="true"
        data-ledger-sweep=""
        className={cn('pointer-events-none absolute inset-y-0 left-0 z-0 w-full origin-left', throne ? 'bg-[var(--fm-accent)]/[0.06]' : 'bg-fg/[0.03]')}
        style={{ transform: `scaleX(${strength.toFixed(3)})`, opacity: throne ? 0.07 : 0.04 }}
      />
      <span aria-hidden="true" className="ld-throne pointer-events-none absolute inset-y-0 left-0 z-20 w-1 bg-[var(--fm-accent)] shadow-(--ld-glow-throne)" style={{ opacity: throne ? 0.6 : 0 }} />
    </>
  );
}

function BoardRowItem({ row, order, selected, wide, days, onSelect }: { row: BoardRow; order: number; selected: boolean; wide: boolean; days: number; onSelect: (id: string) => void }) {
  const beat = ledgerDelay(order);
  const strength = row.typical == null ? 0.06 : Math.max(0.06, Math.min(1, (101 - row.typical) / 100));
  return (
    <div data-ledger={row.id} data-row-id={row.id} className="relative border-b border-fg/[0.07] last:border-b-0">
      <button
        type="button"
        onClick={() => onSelect(selected ? '' : row.id)}
        aria-expanded={selected}
        data-testid={`lead-v3-row-${row.rank}`}
        className={cn(
          'relative isolate block w-full overflow-hidden text-left transition-colors duration-300 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]',
          selected ? 'bg-fg/[0.03]' : 'hover:bg-fg/[0.022]',
        )}
        style={{ '--ld-beat': `${beat}ms` } as CSSProperties}
      >
        <RowGround throne={row.rank === 1} strength={strength} />

        {/* phone / tablet: the numbers' column has one width on every row and in every window; the verdict heads it,
            over the typical post it judges (the subtitle says what the number is) */}
        {!wide ? <div className="relative z-10 grid min-h-[76px] grid-cols-[34px_45px_minmax(0,1fr)_124px] items-center gap-2 px-3 py-3 sm:grid-cols-[42px_45px_minmax(0,1fr)_124px] sm:gap-3 sm:px-4">
          <RankNumeral rank={row.rank} delay={beat} size="text-[44px]" />
          <RowThumb row={row} />
          <span className="min-w-0">
            <span className="flex min-w-0 items-center gap-1.5">
              <span className="truncate text-[15px] font-black leading-none tracking-[-0.02em] text-fg">{row.name}</span>
              <MoveChip move={row.rankMove} />
            </span>
            <span className={cn('mt-1.5 block truncate', NOTE, SOFT)}>{row.caption}</span>
          </span>
          <span className="flex min-w-0 items-center justify-end gap-2 text-right">
            <span className="min-w-0">
              <RowVerdict row={row} delay={beat} end />
              <span className="fm-depth-title mt-1 flex items-baseline justify-end text-[24px] font-black tabular-nums leading-none text-fg">
                <NumberBox value={percentLabel(row.typical)} delay={beat} />
                {row.typical == null ? null : <i className="ml-0.5 not-italic text-[0.5em]">%</i>}
              </span>
              <span className="mt-1 flex items-baseline justify-end gap-1.5 text-[12px] font-black tabular-nums leading-none">
                {row.typicalShift != null && row.typicalShift !== 0 ? (
                  <span className={cn('flex items-baseline gap-0.5', row.typicalShift > 0 ? 'text-fg' : DIM)}>
                    <i className={cn('not-italic text-[8px]', row.typicalShift > 0 && 'text-[var(--fm-accent)]')}>{row.typicalShift > 0 ? '▲' : '▼'}</i>
                    <SlotText value={String(Math.abs(row.typicalShift))} delay={beat} />
                  </span>
                ) : null}
                <span className={SOFT}><NumberBox value={formatMultiple(row.peak)} delay={beat} min="3ch" /> <i className={cn('not-italic', MARK, 'tracking-[0.08em]', DIM)}>peak</i></span>
              </span>
            </span>
            <ChevronDown className={cn('h-4 w-4 shrink-0 text-fg/48 light:text-fg/58 transition-transform duration-[calc(300ms*var(--fm-tempo))]', selected && 'rotate-180 text-fg light:text-fg')} />
          </span>
        </div> : null}

        {/* desktop */}
        {wide ? <div className="relative z-10 grid min-h-[80px] grid-cols-[52px_minmax(150px,1fr)_136px_120px_104px_104px_28px] items-center gap-3 px-4 py-3 laptop:min-h-[70px] xl:grid-cols-[64px_minmax(230px,1.2fr)_150px_130px_120px_110px_30px] xl:gap-4 xl:px-5">
          <RankNumeral rank={row.rank} delay={beat} size="text-[50px] laptop:text-[42px]" />
          <span className="flex min-w-0 items-center gap-3.5">
            <RowThumb row={row} />
            <span className="min-w-0">
              <span className="flex min-w-0 items-center gap-2">
                <span className="truncate text-[16px] font-black leading-none tracking-[-0.02em] text-fg laptop:text-[15px]">{row.name}</span>
                <MoveChip move={row.rankMove} />
              </span>
              <span className="mt-1.5 flex min-w-0 items-center gap-1.5">
                <span className={cn('truncate', NOTE, SOFT)}>{row.caption}</span>
                <span className="text-fg/16">·</span>
                <RowVerdict row={row} delay={beat} />
              </span>
            </span>
          </span>
          <span className="min-w-0">
            <span className="fm-depth-title flex items-baseline text-[28px] font-black tabular-nums leading-none text-fg laptop:text-[24px]">
              <NumberBox value={percentLabel(row.typical)} delay={beat} />
              {row.typical == null ? null : <i className="ml-0.5 not-italic text-[0.5em]">%</i>}
            </span>
            <span className={cn('mt-1.5 block truncate', NOTE, DIM)}>{row.postCount} posts{row.prevPostCount != null && row.prevPostCount !== row.postCount ? ` · prev ${row.prevPostCount}` : ''}</span>
          </span>
          <span className="min-w-0">
            <span className={cn(
              'fm-depth-title flex items-baseline gap-1 text-[24px] font-black tabular-nums leading-none laptop:text-[21px]',
              row.typicalShift == null ? 'text-fg/30' : row.typicalShift > 0 ? 'text-[var(--fm-accent-text)]' : row.typicalShift < 0 ? 'text-fg/44' : 'text-fg/60',
            )}>
              {row.typicalShift != null && row.typicalShift !== 0 ? <i className="not-italic text-[0.45em]">{row.typicalShift > 0 ? '▲' : '▼'}</i> : null}
              <SlotText value={row.typicalShift == null ? '—' : String(Math.abs(row.typicalShift))} delay={beat} />
            </span>
            <span className={cn('mt-1.5 block truncate', NOTE, DIM)}>
              {row.typicalShift == null ? 'no prior window' : row.typicalShift === 0 ? 'level' : `places ${row.typicalShift > 0 ? 'stronger' : 'softer'}`}
            </span>
          </span>
          <span className="min-w-0">
            <span className="block text-[21px] font-black tabular-nums leading-none text-[var(--fm-accent-text)] laptop:text-[18px]"><SlotText value={formatMultiple(row.peak)} delay={beat} /></span>
            <span className={cn('mt-1.5 block truncate', NOTE, DIM)}>vs its usual</span>
          </span>
          <span className="min-w-0">
            <span className="block text-[15px] font-black tabular-nums leading-none text-fg/60"><SlotText value={lastPostedLabel(row.lastPostedAt)} delay={beat} /></span>
            <span className={cn('mt-1.5 block truncate', NOTE, DIM)}>{row.lastPostedAt == null ? 'no posts' : lastPostedLabel(row.lastPostedAt) === 'TODAY' ? 'posted' : 'ago · last post'}</span>
          </span>
          <span className="grid h-7 w-7 place-items-center rounded-full border border-fg/10 bg-(--ld-chip-well)">
            <ChevronDown className={cn('h-4 w-4 text-fg/48 light:text-fg/58 transition-transform duration-[calc(300ms*var(--fm-tempo))]', selected && 'rotate-180 text-fg light:text-fg')} />
          </span>
        </div> : null}
      </button>
      <AnimatePresence initial={false} mode="popLayout">
        {selected && <RowExpansion key={row.id} row={row} days={days} onCollapse={() => onSelect('')} />}
      </AnimatePresence>
    </div>
  );
}

function PostRowItem({ post, rank, order, selected, onSelect }: { post: TrackedPost; rank: number; order: number; selected: boolean; onSelect: (key: string) => void }) {
  const beat = ledgerDelay(order);
  const strength = post.latestPercentile == null ? 0.06 : Math.max(0.06, Math.min(1, (101 - post.latestPercentile) / 100));
  return (
    <div data-ledger={`post:${post.postKey}`} className="relative border-b border-fg/[0.07] last:border-b-0">
      <button
        type="button"
        onClick={() => onSelect(selected ? '' : post.postKey)}
        aria-expanded={selected}
        data-testid={`lead-v3-post-row-${rank}`}
        className={cn(
          'relative isolate block w-full overflow-hidden text-left transition-colors duration-300 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]',
          selected ? 'bg-fg/[0.03]' : 'hover:bg-fg/[0.022]',
        )}
        style={{ '--ld-beat': `${beat}ms` } as CSSProperties}
      >
        <RowGround throne={rank === 1} strength={strength} />
        {/* one height for every place on the ladder, filled or empty (EmptyPlace) */}
        <div className="relative z-10 grid h-[84px] grid-cols-[34px_45px_minmax(0,1fr)_112px] items-center gap-2.5 px-3 sm:grid-cols-[42px_45px_minmax(0,1fr)_112px] sm:gap-3 sm:px-4">
          <RankNumeral rank={rank} delay={beat} size="text-[44px]" />
          <span className="relative block h-[56px] w-[45px] shrink-0 overflow-hidden rounded-[14px] border border-fg/12 bg-(--ld-thumb) shadow-(--ld-shadow-thumb)">
            <CrossfadeImage src={post.thumbnailUrl} />
          </span>
          <span className="min-w-0">
            <span className="block truncate text-[15px] font-black capitalize leading-none tracking-[-0.02em] text-fg">{post.mediaType} · {postDateLabel(post)}</span>
            <span className="mt-1.5 flex min-w-0 items-center gap-1.5">
              <span className={cn('truncate', NOTE, SOFT)}>{(post.latestCheckpoint || '').toUpperCase()} read</span>
              <span className="text-fg/16">·</span>
              <ClimbChip post={post} />
            </span>
          </span>
          <span className="flex min-w-0 items-center justify-end gap-2 text-right">
            <span className="min-w-0">
              <span className={cn('block', MARK, 'leading-none', DIM)}>Placed</span>
              <span className="fm-depth-title mt-1 flex items-baseline justify-end text-[24px] font-black tabular-nums leading-none text-fg">
                <NumberBox value={percentLabel(post.latestPercentile)} delay={beat} />
                {post.latestPercentile == null ? null : <i className="ml-0.5 not-italic text-[0.5em]">%</i>}
              </span>
              <span className="mt-1 block whitespace-nowrap text-[12px] font-black tabular-nums leading-none text-fg/80"><NumberBox value={formatMultiple(post.rankingMultiple)} delay={beat} min="3ch" /> <i className={cn('not-italic', MARK, 'tracking-[0.08em]', DIM)}>usual</i></span>
            </span>
            <ChevronDown className={cn('h-4 w-4 shrink-0 text-fg/48 light:text-fg/58 transition-transform duration-[calc(300ms*var(--fm-tempo))]', selected && 'rotate-180 text-fg light:text-fg')} />
          </span>
        </div>
      </button>
      <AnimatePresence initial={false} mode="popLayout">
        {selected && <PostRowExpansion key={post.postKey} post={post} onCollapse={() => onSelect('')} />}
      </AnimatePresence>
    </div>
  );
}

/* a place the feeder's posts don't fill in this window: its numeral, and (the first one) what the emptiness means */
function EmptyPlace({ id, rank, note }: { id: string; rank: number; note?: string }) {
  return (
    <div data-ledger={id} aria-hidden="true" className="relative border-b border-fg/[0.07] last:border-b-0">
      {/* as tall as a post's row (PostRowItem), so places filling and emptying never change the ladder's height */}
      <div className="grid h-[84px] grid-cols-[34px_minmax(0,1fr)] items-center gap-2.5 px-3 sm:grid-cols-[42px_minmax(0,1fr)] sm:gap-3 sm:px-4">
        <span className="relative h-full">
          <span className="pointer-events-none absolute left-0 top-1/2 -translate-y-1/2 select-none text-[44px] font-black tabular-nums leading-none tracking-[-0.08em] text-fg/[0.055]">{rankLabel(rank)}</span>
        </span>
        <span className="ml-[57px] flex items-center gap-3">
          {note ? <span className={cn('shrink-0', MARK, DIM)}>{note}</span> : null}
          <span className="h-px flex-1 border-t border-dashed border-fg/[0.1]" />
        </span>
      </div>
    </div>
  );
}

function Feederboard({ rows, posts, feeder, deal, days, selectedId, onSelect, selectedPostKey, onSelectPost }: {
  rows: BoardRow[];
  posts: TrackedPost[];
  feeder: string;
  deal: string;
  days: number;
  selectedId: string;
  onSelect: (id: string) => void;
  selectedPostKey: string;
  onSelectPost: (key: string) => void;
}) {
  // one form of a row on screen (the phone's or the desktop's)
  const wide = useWide();
  const ladder = useMemo(() => (feeder ? [...posts].sort(bestFirst).slice(0, LADDER_PLACES) : []), [feeder, posts]);
  const shown = feeder ? NO_ROWS : rows;
  const ids = useMemo(
    () => (feeder
      ? [...ladder.map((post) => `post:${post.postKey}`), ...Array.from({ length: LADDER_PLACES - ladder.length }, (_, index) => `empty:${ladder.length + index}`)]
      : shown.map((row) => row.id)),
    [feeder, ladder, shown],
  );
  const openId = feeder ? (selectedPostKey ? `post:${selectedPostKey}` : '') : selectedId;

  /* When a switch re-ranks the ladder, follow the row that is open once it has landed in its new place, so it never
     sits out of view; a row just opened is brought into view once it has opened. */
  const boardRef = useRef<HTMLElement | null>(null);
  const last = useRef({ deal, openId });
  useEffect(() => {
    const was = last.current;
    last.current = { deal, openId };
    if (!openId) return undefined;
    const order = ids.indexOf(openId);
    if (order < 0) return undefined;
    const wait = was.deal !== deal ? ledgerDelay(order) + LEDGER_MOVE_MS : was.openId !== openId ? ms(420) : -1;
    if (wait < 0) return undefined;
    const timeout = window.setTimeout(() => {
      // the board's own row (the header's rail uses the same kind of ids for its circles)
      const row = boardRef.current?.querySelector<HTMLElement>(`[data-ledger="${CSS.escape(openId)}"]`);
      if (row) revealBelowHeader(row);
    }, wait);
    return () => window.clearTimeout(timeout);
  }, [deal, ids, openId]);

  // one line on a phone beside the scale's note (a tap on a row opens its receipts)
  const subtitle = feeder
    ? `${ladder.length} ${ladder.length === 1 ? 'post competes' : 'posts compete'} in ${days}D`
    : 'Ranked on the typical post';

  return (
    <section ref={boardRef} aria-labelledby="lead-v3-board-title" className="min-w-0 max-w-full overflow-hidden bg-(--ld-panel)">
      <div className="flex items-center justify-between gap-4 border-b border-fg/[0.08] px-4 py-4 sm:px-5">
        <div className="min-w-0">
          <h2 id="lead-v3-board-title" className="text-[10px] font-black uppercase tracking-[0.2em] text-fg/60">Feederboard</h2>
          <p className={cn('mt-1 overflow-hidden whitespace-nowrap', NOTE, SOFT)}><Swap value={subtitle} className="truncate align-top" /><span className="hidden sm:inline"> · tap a row for the receipts</span></p>
        </div>
        <div className={cn('max-w-[9.5rem] shrink-0 text-right sm:max-w-none', MARK, 'tracking-[0.12em] leading-[1.3]', DIM)}>
          TOP % · lower is stronger
        </div>
      </div>

      {wide ? (
        feeder ? (
          <div key="posts" ref={holdArrival} className={cn('lead-swap-in grid grid-cols-[42px_45px_minmax(0,1fr)_112px] items-end gap-3 border-b border-fg/[0.07] px-4 py-3', MARK, DIM)}>
            <span>Rank</span><span>Post</span><span /><span className="pr-6 text-right">Placed</span>
          </div>
        ) : (
          <div key="rows" ref={holdArrival} className={cn('lead-swap-in grid grid-cols-[52px_minmax(150px,1fr)_136px_120px_104px_104px_28px] items-end gap-3 border-b border-fg/[0.07] px-4 py-3 xl:grid-cols-[64px_minmax(230px,1.2fr)_150px_130px_120px_110px_30px] xl:gap-4 xl:px-5', MARK, 'tracking-[0.1em]', DIM)}>
            <span>Rank</span><span>Feed / feeder</span><span>Typical post</span><span>Shift · prev {days}D</span><span>Peak vs usual</span><span>Last posted</span><span />
          </div>
        )
      ) : null}

      <Ledger ids={ids} deal={deal} version={`${openId}|${wide}`} anchor={openId || undefined}>
        {feeder
          ? [
              ...ladder.map((post, order) => (
                <PostRowItem key={post.postKey} post={post} rank={order + 1} order={order} selected={post.postKey === selectedPostKey} onSelect={onSelectPost} />
              )),
              ...ids.slice(ladder.length).map((id, index) => <EmptyPlace key={id} id={id} rank={ladder.length + index + 1} note={index === 0 ? `No other posts in ${days}D` : undefined} />),
            ]
          : shown.map((row, order) => (
              <BoardRowItem key={row.id} row={row} order={order} selected={row.id === selectedId} wide={wide} days={days} onSelect={onSelect} />
            ))}
      </Ledger>
      {!feeder && !shown.length ? (
        <div className={cn('flex h-[140px] items-center justify-center', MARK, DIM)}>No feeds to rank yet</div>
      ) : null}
    </section>
  );
}

/* ── Feederboard views — facts and ranked posts across the current scope ─── */

type ScopedPost = TrackedPost & { feedName: string };
const NO_POSTS: ScopedPost[] = [];

function scopedPostsFor(feeds: RuntimeFeed[], activeFeedId: string, days: number): ScopedPost[] {
  return remember(feeds, null, `scoped|${activeFeedId}|${days}`, () => {
    const scoped = activeFeedId === 'all' ? feeds : feeds.filter((feed) => feed.id === activeFeedId);
    return scoped.flatMap((feed) => postsInWindow(feed.posts, days).map((post) => ({ ...post, feedName: titleCase(feed.title) })));
  });
}

const FEEDERBOARD_VIEWS = [
  { id: 'feats', label: 'Wins', ariaLabel: 'Wins' },
  { id: 'likes', label: 'Likes', ariaLabel: 'Most liked' },
  { id: 'comments', label: 'Comments', ariaLabel: 'Most talked about' },
  { id: 'er', label: 'Eng.', ariaLabel: 'Most engaged' },
] as const;

/* a win's handle: what kind of thing happened, in a word or two (the facts themselves are lib/feederboardFacts) */
const WIN_LABEL: Record<string, string> = {
  'above-streak': 'Hot streak',
  'checkpoint-climb': 'Climber',
  'busiest-day': 'Busiest day',
  'publishing-run': 'On a roll',
  'format-run': 'Format win',
  'format-time': 'Sweet spot',
  'post-concentration': 'Comment magnet',
  'feeder-participation': 'Team effort',
  'feeder-concentration': 'Carried the chat',
  'response-imbalance': 'Talking point',
  'quiet-breakout': 'Comeback',
};
// a run of misses is worth knowing (the Feederboard's verdicts show it) but isn't a win
const NOT_A_WIN = new Set(['below-streak']);

type FeederboardViewId = (typeof FEEDERBOARD_VIEWS)[number]['id'];
type FeederboardRankedViewId = Exclude<FeederboardViewId, 'feats'>;

function recordSortValue(post: ScopedPost, metric: FeederboardRankedViewId) {
  if (metric === 'comments') return numeric(post.comments);
  if (metric === 'likes') return numeric(post.likes);
  return post.engagementRate ?? -1;
}

function recordDisplay(post: ScopedPost, metric: FeederboardRankedViewId) {
  if (metric === 'comments') return { value: compactNumber(numeric(post.comments)), label: 'comments' };
  if (metric === 'likes') return { value: compactNumber(numeric(post.likes)), label: 'likes' };
  return { value: formatEngagementRate(post.engagementRate), label: 'engagement rate' };
}

const FeederboardCards = memo(function FeederboardCards(props: {
  feeds: RuntimeFeed[];
  activeFeedId: string;
  activeFeeder: string;
  days: number;
}) {
  // on screen it takes a switch with the rest of the page; off screen, once the switch has played (see useHeldNear)
  const live = useMemo(
    () => ({ feeds: props.feeds, activeFeedId: props.activeFeedId, activeFeeder: props.activeFeeder, days: props.days }),
    [props.feeds, props.activeFeedId, props.activeFeeder, props.days],
  );
  const [nearRef, { feeds, activeFeedId, activeFeeder, days }] = useHeldNear(live);
  const facts = useMemo(() => factsFor(feeds, activeFeedId, activeFeeder, days).filter((fact) => !NOT_A_WIN.has(fact.kind)), [feeds, activeFeedId, activeFeeder, days]);
  const [view, setView] = useState<FeederboardViewId>('feats');
  const posts = postsFor(feeds, activeFeedId, activeFeeder, days);
  // the posts a win stands on, by key: the card shows its pictured post's Top % and how far over its usual it went
  const postByKey = useMemo(() => new Map(posts.map((post) => [post.postKey, post])), [posts]);
  const records = useMemo(() => {
    if (view === 'feats') return [];
    return [...posts]
      .filter((post) => post.thumbnailUrl)
      .sort((a, b) => recordSortValue(b, view) - recordSortValue(a, view) || a.postKey.localeCompare(b.postKey))
      .slice(0, 10);
  }, [posts, view]);

  const cards = view === 'feats' ? facts : records;
  // a card is the post it shows: the same post read the same way in the next deal stays (and travels), anything else
  // hands its place over
  const ids = useMemo(
    () => (view === 'feats'
      ? facts.map((fact) => `fact:${fact.kind}:${fact.evidencePostKeys[0] ?? fact.headline}`)
      : records.map((post) => `${view}:${post.postKey}`)),
    [facts, records, view],
  );
  const viewIndex = FEEDERBOARD_VIEWS.findIndex((option) => option.id === view);
  // on WebKit the pill's glide waits for the frame that draws the new cards, so it plays whole with their deal
  const pillRef = useRef<HTMLSpanElement | null>(null);
  useLayoutEffect(() => {
    const pill = pillRef.current;
    if (!pill || !holdsFlights()) return undefined;
    const glides = pill.getAnimations();
    glides.forEach((glide) => {
      glide.pause();
      glide.currentTime = 0;
    });
    return afterDrawn(() => glides.forEach((glide) => glide.play()));
  }, [viewIndex]);

  // the band keeps its place on the page when it grows or shrinks by a row of cards: what is under it travels
  const bandRef = useRef<HTMLElement | null>(null);
  useEffect(() => (bandRef.current ? keepBelow(bandRef.current) : undefined), []);
  const ref = useCallback((el: HTMLElement | null) => {
    bandRef.current = el;
    nearRef(el);
  }, [nearRef]);

  return (
    <section ref={ref} aria-labelledby="lead-v3-hall-title" className="lead-band relative isolate overflow-hidden border-t border-fg/[0.08] bg-(--ld-panel) px-4 pb-5 pt-4 sm:px-5 sm:pb-6 lg:px-7 lg:pb-7">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
        <div className="min-w-0">
          <h2 id="lead-v3-hall-title" className="text-[10px] font-black uppercase tracking-[0.2em] text-fg/60">Hall of Feed</h2>
          <p className={cn('mt-1', NOTE, SOFT)}>The posts that carried the feed · {days}D</p>
        </div>
        <div className="relative grid h-11 min-w-[224px] grid-cols-4 rounded-[14px] border border-fg/[0.07] bg-(--fm-well) shadow-(--fm-well-shade) sm:min-w-[272px]">
          {/* one pill, carried to the picked view (a transform on the compositor) */}
          <span
            ref={pillRef}
            aria-hidden="true"
            className="ld-pill pointer-events-none absolute bottom-[3px] left-[3px] top-[3px] rounded-[8px] bg-fg shadow-[inset_0_1px_0_rgb(255_255_255_/_0.16)]"
            style={{ width: 'calc(25% - 6px)', transform: `translateX(calc(${viewIndex} * (100% + 6px)))` }}
          />
          {FEEDERBOARD_VIEWS.map((option) => (
            <button
              key={option.id}
              type="button"
              onClick={() => setView(option.id)}
              aria-label={option.ariaLabel}
              aria-pressed={view === option.id}
              data-testid={`lead-v3-feederboard-view-${option.id}`}
              className={cn(
                'relative grid h-full place-items-center rounded-[10px] text-[10px] font-black uppercase tracking-[0.08em] transition-colors duration-[calc(300ms*var(--fm-tempo))] sm:text-[11px]',
                view === option.id ? 'text-(--fm-page)' : 'text-fg/60 hover:text-fg/80',
              )}
            >
              <span className="relative z-10">{option.label}</span>
            </button>
          ))}
        </div>
      </div>

      <div className="relative mt-5 min-h-[300px] xl:min-h-[360px]">
        {/* the cards turn over left to right, a beat a card (components/lead/ledgerMotion) */}
        <Ledger
          ids={ids}
          deal={`${view}|${activeFeedId}|${activeFeeder}|${days}`}
          axis="x"
          startOnDeal
          className="-mx-4 flex snap-x snap-mandatory scroll-px-4 gap-3 overflow-x-auto px-4 pb-2 [scrollbar-width:none] sm:mx-0 sm:scroll-px-0 sm:px-0 xl:grid xl:grid-cols-5 xl:overflow-visible xl:pb-0 [&::-webkit-scrollbar]:hidden"
        >
          {view === 'feats' ? facts.map((fact, order) => {
            // the post the card shows (its picture is the run's strongest post), else the strongest of the evidence
            const evidence = fact.evidencePostKeys.map((key) => postByKey.get(key)).filter((post): post is ScopedPost => Boolean(post));
            const hero = evidence.find((post) => post.thumbnailUrl && post.thumbnailUrl === fact.thumbnailUrl)
              ?? [...evidence].sort(bestFirst)[0]
              ?? null;
            return (
            <article
              key={ids[order]}
              data-ledger={ids[order]}
              data-testid={`lead-v3-feederboard-fact-${fact.kind}`}
              className="group relative aspect-[4/5] w-[220px] min-w-[220px] snap-start overflow-hidden rounded-[18px] border border-white/12 bg-(--fm-media) shadow-(--ld-shadow-tile) sm:w-[236px] sm:min-w-[236px] xl:w-auto xl:min-w-0"
            >
              <CrossfadeImage src={fact.thumbnailUrl} className="transition-transform duration-500 group-hover:scale-[1.02]" />
              <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,.06)_0%,transparent_36%,rgba(0,0,0,.7)_58%,rgba(0,0,0,.94)_100%)]" />
              <span className={cn('absolute left-2.5 top-2.5 rounded-md bg-black/62 px-2 py-1 text-white', MARK, 'tracking-[0.1em]')}>{WIN_LABEL[fact.kind] ?? fact.kind.replace(/-/g, ' ')}</span>
              {fact.postUrl ? <a href={fact.postUrl} target="_blank" rel="noreferrer" aria-label="Open the post on Instagram" className={cn('absolute right-2.5 top-2.5 rounded-md bg-black/62 px-2.5 py-1.5 text-white/85', MARK, 'tracking-[0.1em]')}>Instagram</a> : null}
              <div className="absolute inset-x-0 bottom-0 flex min-h-[112px] flex-col justify-end p-3.5">
                {/* the post it stands on, in numbers (what the old post mortem showed) */}
                {hero && hero.latestPercentile != null ? (
                  <div className="mb-1.5 flex items-baseline gap-2 text-white">
                    <span className="fm-depth-title ld-on-media text-[13px] font-black tabular-nums">TOP {percentLabel(hero.latestPercentile)}<i className="not-italic">%</i></span>
                    {hero.rankingMultiple != null ? <span className="text-[12px] font-semibold text-white/78">{formatMultiple(hero.rankingMultiple)} usual</span> : null}
                  </div>
                ) : null}
                <div className="pb-[2px] text-[20px] font-black leading-[1.05] tracking-[-0.035em] text-white [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:3] overflow-hidden sm:text-[22px]">{fact.headline}</div>
                <div className="mt-2 text-[12px] font-semibold leading-4 text-white/78 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden">{fact.detail}</div>
              </div>
            </article>
            );
          }) : records.map((post, index) => {
            const display = recordDisplay(post, view);
            return (
              <a
                key={ids[index]}
                data-ledger={ids[index]}
                href={post.postUrl || '#'}
                target={post.postUrl ? '_blank' : undefined}
                rel={post.postUrl ? 'noreferrer' : undefined}
                onClick={(event) => { if (!post.postUrl) event.preventDefault(); }}
                data-testid={`lead-v3-feederboard-record-${index + 1}`}
                aria-label={post.postUrl ? `Open tracked post ranked ${index + 1} by ${display.label}` : `Tracked post ranked ${index + 1} by ${display.label}`}
                className="group relative block aspect-[4/5] w-[220px] min-w-[220px] snap-start overflow-hidden rounded-[18px] border border-white/12 bg-(--fm-media) shadow-(--ld-shadow-tile) focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)] sm:w-[236px] sm:min-w-[236px] xl:w-auto xl:min-w-0"
              >
                <CrossfadeImage src={post.thumbnailUrl} className="transition-transform duration-500 group-hover:scale-[1.02]" />
                <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,.06)_0%,transparent_54%,rgba(0,0,0,.65)_72%,rgba(0,0,0,.94)_100%)]" />
                <span className="absolute left-2.5 top-2.5 grid h-7 min-w-7 place-items-center rounded-md bg-black/62 px-1.5 text-[11px] font-black tabular-nums text-white/88">{index + 1}</span>
                <span className={cn('absolute right-2.5 top-2.5 rounded-md bg-black/62 px-2 py-1 text-white/85', MARK, 'tracking-[0.1em]')}>{activeFeedId === 'all' ? post.feedName : (post.latestCheckpoint || 'latest').toUpperCase()}</span>
                <div className="absolute inset-x-0 bottom-0 flex min-h-[112px] flex-col justify-end p-3.5">
                  <div className="fm-depth-title ld-on-media text-[28px] font-black tabular-nums leading-[0.9] tracking-[-0.04em] text-white sm:text-[30px]">{display.value}</div>
                  <div className="mt-2 truncate text-[12px] font-black text-white/90">@{normalizeHandle(post.handle) || post.feedName.toLowerCase()}</div>
                  <div className="mt-1 text-[12px] font-semibold leading-4 text-white/72 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden">{display.label} · {(post.latestCheckpoint || 'latest').toUpperCase()} read</div>
                </div>
              </a>
            );
          })}
        </Ledger>
        {!cards.length ? <div ref={holdArrival} className={cn('ld-late-in absolute inset-0 flex items-center justify-center text-center', MARK, DIM)}>{view === 'feats' ? 'No wins in this window yet' : 'No tracked posts in this window'}</div> : null}
      </div>
    </section>
  );
});

/* ── Feeder altitude: the posts become the competitors ───────────────────── */

function postDateLabel(post: TrackedPost) {
  const time = postTime(post);
  if (!time) return 'undated';
  return new Intl.DateTimeFormat('en-US', { day: '2-digit', month: 'short' }).format(new Date(time));
}

function ClimbChip({ post }: { post: TrackedPost }) {
  const trajectory = climbOf(post);
  if (trajectory == null || Math.abs(trajectory) < 3 || post.firstPercentile == null) return null;
  const up = trajectory > 0;
  return (
    <span className={cn('inline-flex shrink-0 items-center gap-1 text-[12px] font-black tabular-nums leading-4', up ? 'text-fg' : DIM)}>
      <span className={cn('text-[8px]', up && 'text-[var(--fm-accent)]')}>{up ? '▲' : '▼'}</span>
      {percentLabel(post.firstPercentile)}%→{percentLabel(post.latestPercentile)}%
    </span>
  );
}

function PostRowExpansion({ post, onCollapse, ref }: { post: TrackedPost; onCollapse: () => void; ref?: Ref<HTMLDivElement> }) {
  const reduce = Boolean(useReducedMotion());
  const metrics = [
    { label: 'Likes', value: compactNumber(numeric(post.likes)), multiple: post.likesMultiple },
    { label: 'Comments', value: compactNumber(numeric(post.comments)), multiple: post.commentsMultiple },
    { label: 'Eng. rate', value: formatEngagementRate(post.engagementRate), multiple: post.engagementRateMultiple },
  ];
  return (
    <motion.div
      ref={ref}
      data-testid="lead-v3-post-expansion"
      initial={reduce ? false : BEAT_HIDDEN}
      animate={BEAT_SHOWN}
      exit={{ opacity: 0, transition: { duration: sec(0.16), ease: BEAT_LEAVE_EASE } }}
      transition={reduce ? { duration: 0 } : { duration: BEAT_ARRIVE_S, ease: SOFT_EASE }}
      className="overflow-hidden"
    >
      <div onClick={onCollapse} className="relative cursor-pointer border-t border-fg/[0.1] bg-(--ld-raised) p-4 sm:p-6">
        <div className="pointer-events-none absolute inset-y-0 left-0 w-1 bg-fg" />
        <div className="grid gap-5 sm:grid-cols-[180px_minmax(0,1fr)] sm:items-center sm:gap-7">
          <a
            href={post.postUrl || '#'}
            target={post.postUrl ? '_blank' : undefined}
            rel={post.postUrl ? 'noreferrer' : undefined}
            onClick={(event) => {
              event.stopPropagation();
              if (!post.postUrl) event.preventDefault();
            }}
            aria-label={post.postUrl ? 'Open tracked post' : 'Tracked post'}
            className="relative mx-auto block aspect-[4/5] w-[62%] max-w-[200px] overflow-hidden rounded-[20px] border border-white/12 bg-(--fm-media) shadow-(--ld-shadow-poster) sm:mx-0 sm:w-full"
          >
            {post.thumbnailUrl ? (
              // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
              <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" loading="lazy" decoding="async" />
            ) : null}
            <div className="absolute inset-0 bg-[linear-gradient(180deg,transparent_40%,rgba(0,0,0,.85))]" />
            <span className="absolute bottom-3 left-3 text-[12px] font-black text-white">{post.latestPercentile == null ? '—' : `TOP ${percentLabel(post.latestPercentile)}%`}</span>
          </a>
          <div className="min-w-0">
            <div className={cn('flex flex-wrap items-center gap-x-3 gap-y-1', NOTE, DIM)}>
              <span>{post.mediaType} · {postDateLabel(post)}</span>
              {post.firstCheckpoint && post.firstPercentile != null ? (
                <span className={SOFT}>
                  {post.firstCheckpoint} top {percentLabel(post.firstPercentile)}% → {(post.latestCheckpoint || '').toUpperCase()} top {percentLabel(post.latestPercentile)}%
                </span>
              ) : (
                <span>{(post.latestCheckpoint || '').toUpperCase()} read</span>
              )}
            </div>
            <dl className="mt-4 grid grid-cols-3 gap-4">
              {metrics.map((metric) => (
                <div key={metric.label} className="min-w-0">
                  <dt className={cn('truncate', MARK, DIM)}>{metric.label}</dt>
                  <dd className="fm-depth-title mt-2 truncate text-[22px] font-black tabular-nums leading-none text-fg sm:text-[26px]"><SlotText value={metric.value} /></dd>
                  <div className={cn('mt-2 tabular-nums', NOTE, SOFT)}>{metric.multiple == null ? '—' : `${formatMultiple(metric.multiple)} usual`}</div>
                </div>
              ))}
            </dl>
          </div>
        </div>
      </div>
    </motion.div>
  );
}

/* ── Sections ────────────────────────────────────────────────────────────── */
/* Each section reads the pick and works out what it shows itself. A switch is then many small renders React can
   spread across frames, instead of the page computing everything in one long one; and a section whose inputs did
   not change does not render at all. */

type ScopeProps = { feeds: RuntimeFeed[]; feedId: string; feeder: string; days: number };

const TapeSection = memo(function TapeSection({ feeds, feedId, feeder, days }: ScopeProps) {
  const events = tapeFor(feeds, feedId, feeder, days);
  return <LeadTape events={events} dealId={`${feedId}|${feeder}|${days}`} />;
});

const LeadersSection = memo(function LeadersSection({ feeds, feedId, feeder, days }: ScopeProps) {
  const cards = leadersFor(feeds, feedId, feeder, days);
  return (
    <section aria-labelledby="lead-v3-leaders-title" className="min-w-0 max-w-full">
      <div className="mb-3 px-1">
        <h2 id="lead-v3-leaders-title" className="flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.18em] text-fg/76"><span className="h-1.5 w-1.5 rounded-full bg-[var(--fm-accent)] shadow-(--ld-glow-dot)" />Top of the feed</h2>
        <p className={cn('mt-1', NOTE, SOFT)}>Best post, typical post and gains · {days}D</p>
      </div>
      <LeadersStrip cards={cards} />
    </section>
  );
});

const BoardSection = memo(function BoardSection({ feeds, feedId, feeder, days, selectedId, onSelect, selectedPostKey, onSelectPost }: ScopeProps & {
  selectedId: string;
  onSelect: (id: string) => void;
  selectedPostKey: string;
  onSelectPost: (key: string) => void;
}) {
  const rows = rowsFor(feeds, feedId, days);
  const feederPosts = feeder ? postsFor(feeds, feedId, feeder, days) : NO_POSTS;
  const wireLine = useMemo(() => (feeder ? composeFeederWireLine(feederPosts) : composeWireLine(rows)), [feeder, feederPosts, rows]);
  // a row that left the board can't stay open
  const open = rows.some((row) => row.id === selectedId) ? selectedId : '';
  // the page under the board doesn't jump when the board grows or shrinks (a new scope, a row opening): it travels
  const boardRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => (boardRef.current ? keepBelow(boardRef.current) : undefined), []);
  return (
    <div ref={boardRef} className="min-w-0 max-w-full">
      <WireLine line={wireLine ?? 'Nothing posted in this window yet.'} dealId={`${feedId}|${feeder}|${days}`} />
      <Feederboard
        rows={rows}
        posts={feederPosts}
        feeder={feeder}
        deal={`${feedId}|${feeder}|${days}`}
        days={days}
        selectedId={open}
        onSelect={onSelect}
        selectedPostKey={selectedPostKey}
        onSelectPost={onSelectPost}
      />
    </div>
  );
});

/* ── States + page ───────────────────────────────────────────────────────── */

function LoadingState() {
  return (
    <div className="flex min-h-[55vh] items-center justify-center">
      <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="text-center">
        <motion.span className="mx-auto block h-2 w-2 rounded-full bg-fg" animate={{ opacity: [0.25, 1, 0.25], scale: [0.8, 1, 0.8] }} transition={{ duration: 1.4, repeat: Infinity }} />
        <div className={cn('mt-4', MARK, 'tracking-[0.18em]', DIM)}>Building the board</div>
      </motion.div>
    </div>
  );
}

export default function LeadBoardPage() {
  const reduce = Boolean(useReducedMotion());
  const pageScrollRef = useRef<HTMLDivElement | null>(null);
  const { appShellStyle, useBrowserPageScroll, useTranslucentBrowserChrome } = useMobileImmersiveViewport();
  const [days, setDays] = useState<(typeof TIMEFRAMES)[number]>(30);
  // what is on hand when the page is built (the launch warmed it, or a visit before): the board from the first frame
  const [onHand] = useState(() => leadOnHand());
  const [feeds, setFeeds] = useState<RuntimeFeed[]>(() => (onHand ? runtimeOf(onHand).feeds : []));
  const [activity, setActivity] = useState<LeadActivity | null>(() => onHand?.activity ?? null);
  // the pick every tab shares (tabScope), followed in a transition: the page's re-render yields to the rail's frames
  const scope = useTabScopeTransition();
  const { activeFeedId, activeFeeder } = resolveScope(feeds, scope);
  const [selectedId, setSelectedId] = useState('');
  const [selectedPostKey, setSelectedPostKey] = useState('');
  const [loading, setLoading] = useState(() => !onHand);
  // the board is in (or can't be): a launch veil goes
  useEffect(() => {
    if (!loading) announceFirstScreen();
  }, [loading]);
  // a range switch re-reads what is on hand in a transition: the header answers the tap first
  const [timeframePending, startTimeframe] = useTransition();
  const [error, setError] = useState('');
  const headerCompressed = useCompressedOnScroll(pageScrollRef, useBrowserPageScroll, {
    collapseDistance: 150,
    expandDistance: 64,
    topGuard: 30,
  });

  usePageReady(true);

  useEffect(() => {
    document.title = 'Lead | FeedMe';
  }, []);

  /* Everything every time range needs is loaded once: each feed's posts (all of them) and the activity of the
     longest range plus the one before it (components/lead/leadData, shared: the launch warms it before Lead is ever
     opened). A range switch is then only a re-read of what is on hand, with no request and nothing to parse, so it
     answers at once. Lead stays mounted while hidden but its effects run again each time the tab comes back: what it
     has shows at once (a load on hand, or what the device kept from last time), and once it is a few minutes old (or
     only kept) it refreshes in the background, re-drawing only if something actually changed. */
  const loaded = useRef<{ at: number; signature: string } | null>(onHand ? { at: onHand.kept ? 0 : onHand.at, signature: runtimeOf(onHand).signature } : null);

  useEffect(() => {
    const previous = loaded.current;
    if (previous && Date.now() - previous.at < LEAD_FRESH_MS) return undefined;
    let cancelled = false;
    const show = (load: LeadLoad) => {
      const { feeds: nextFeeds, signature } = runtimeOf(load);
      const shown = loaded.current;
      // (one only kept counts as old: it is shown, and loaded afresh behind it)
      loaded.current = { at: load.kept ? 0 : load.at, signature };
      if (shown && shown.signature === signature) return;
      // loading ends in the same (deferred) commit that brings the feeds: ending it on its own, at once, showed
      // "No feeds to compare yet" for the frames before the feeds landed
      startTransition(() => {
        setFeeds(nextFeeds);
        setActivity(load.activity);
        setLoading(false);
      });
    };

    async function load() {
      try {
        const onHandNow = leadOnHand() ?? await keptLead();
        if (cancelled) return;
        if (onHandNow) show(onHandNow);
        if (onHandNow && !onHandNow.kept && Date.now() - onHandNow.at < LEAD_FRESH_MS) return;
        const fresh = await loadLead();
        if (cancelled) return;
        show(fresh);
      } catch (caught) {
        if (cancelled) return;
        // a board already shown stays (a refresh that failed changes nothing)
        if (loaded.current) return;
        setError(caught instanceof Error ? caught.message : 'Feed data could not be loaded.');
        setLoading(false);
      }
    }

    void load();
    return () => {
      cancelled = true;
    };
  }, []);

  // build ahead what a switch could ask for next (see warmJobs) while the page is idle
  useEffect(() => {
    if (!feeds.length) return undefined;
    // once a switch's deal has played: nothing is built while the board moves
    return whenIdle(warmJobs(feeds, activity, activeFeedId, activeFeeder, days), LEDGER_DEAL_MS + 300);
  }, [activeFeedId, activeFeeder, activity, days, feeds]);

  // the tape and the leaders change in the same commit as the board (see useHeldNear): one commit, then the motion
  const scopeNow = useMemo(() => ({ feeds, feedId: activeFeedId, feeder: activeFeeder, days }), [activeFeedId, activeFeeder, days, feeds]);

  // picks compare against the live pick (the page's copy may still be catching up)
  const handleFeedChange = (id: string) => {
    if (id === resolveScope(feeds, getTabScope()).activeFeedId) return;
    setTabScope({ feedId: id === 'all' ? null : id, handle: null });
    startTransition(() => {
      setSelectedId('');
      setSelectedPostKey('');
    });
  };

  return (
    <div
      ref={pageScrollRef}
      data-testid="lead-v3-page"
      className={cn(
        '[overflow-anchor:none] fm-lead fm-dashboard-mesh w-full min-w-0 max-w-[100vw] scroll-pb-[calc(126px+env(safe-area-inset-bottom))] scroll-pt-[calc(84px+env(safe-area-inset-top))] lg:scroll-pt-6 text-fg',
        useTranslucentBrowserChrome ? 'bg-transparent' : 'bg-(--fm-page)',
        useBrowserPageScroll
          ? 'min-h-[100lvh] overflow-x-hidden overflow-y-visible'
          : 'h-[var(--fm-app-height,100dvh)] overflow-x-hidden overflow-y-auto overscroll-y-contain [-webkit-overflow-scrolling:touch]',
      )}
      style={appShellStyle}
    >
      <LeadHeader
        feeds={feeds}
        days={days}
        compressed={headerCompressed}
        pending={timeframePending}
        onFeedChange={handleFeedChange}
        onFeederChange={(handle) => {
          const live = resolveScope(feeds, getTabScope()).activeFeedId;
          setTabScope({ feedId: live === 'all' ? null : live, handle: handle || null });
          startTransition(() => {
            setSelectedId('');
            setSelectedPostKey('');
          });
        }}
        onDaysChange={(nextDays) => {
          if (nextDays === days) return;
          startTimeframe(() => setDays(nextDays));
        }}
      />
      <main className="relative z-10 mx-auto w-full min-w-0 max-w-[1500px] px-3 pb-[calc(148px+env(safe-area-inset-bottom))] pt-[calc(172px+env(safe-area-inset-top))] sm:px-5 sm:pt-[calc(172px+env(safe-area-inset-top))] md:pt-[calc(178px+env(safe-area-inset-top))] lg:px-7 lg:pt-(--fm-desk-top)">
        {loading && !feeds.length ? <LoadingState /> : error ? (
          <div className="flex min-h-[55vh] items-center justify-center px-6 text-center">
            <div>
              <div className="text-[11px] font-black uppercase tracking-[0.2em] text-fg/58">Board unavailable</div>
              <p className={cn('mt-3 text-[12px] font-bold', SOFT)}>{error}</p>
              <Link href="/" className={cn('mt-6 inline-flex rounded-full border border-fg/12 px-4 py-2.5 hover:text-fg', MARK, SOFT)}>Return to Feed</Link>
            </div>
          </div>
        ) : feeds.length ? (
          <motion.div initial={reduce ? false : { opacity: 0, transform: 'translateY(12px)' }} animate={{ opacity: 1, transform: 'translateY(0px)' }} transition={{ duration: sec(0.42), ease: SOFT_EASE }} className="min-w-0 max-w-full">
            <TapeSection {...scopeNow} />
            <LeadersSection {...scopeNow} />

            {/* the board: edge to edge on a phone (its bands keep a 16px gutter for text), a framed card from sm up */}
            <div className="relative -mx-3 mt-4 min-w-0 overflow-hidden border-y border-fg/[0.09] bg-(--ld-ground) sm:mx-0 sm:max-w-full sm:rounded-[28px] sm:border sm:shadow-(--ld-shadow-board)">
              <BoardSection
                feeds={feeds}
                feedId={activeFeedId}
                feeder={activeFeeder}
                days={days}
                selectedId={selectedId}
                onSelect={setSelectedId}
                selectedPostKey={selectedPostKey}
                onSelectPost={setSelectedPostKey}
              />
              <FeederboardCards feeds={feeds} activeFeedId={activeFeedId} activeFeeder={activeFeeder} days={days} />
              <LeadGrowth feeds={feeds} activity={activity} feedId={activeFeedId} feeder={activeFeeder} days={days} />
              <section aria-label="Feeding habits" className="bg-(--ld-panel)">
                <div className="border-t border-fg/[0.09] px-4 pb-0 pt-5 sm:px-6 lg:px-7">
                  <h2 className="text-[10px] font-black uppercase tracking-[0.2em] text-fg/60">Feeding habits</h2>
                  <p className={cn('mt-1', NOTE, SOFT)}>How often, when and what {activeFeeder ? `@${activeFeeder} posts` : activeFeedId === 'all' ? 'your feeds post' : 'this feed posts'} · vs the {days}D before</p>
                </div>
                <LeadRhythm feeds={feeds} activity={activity} feedId={activeFeedId} feeder={activeFeeder} days={days} />
                <LeadTiming feeds={feeds} activity={activity} feedId={activeFeedId} feeder={activeFeeder} days={days} />
                <LeadFormats feeds={feeds} activity={activity} feedId={activeFeedId} feeder={activeFeeder} days={days} />
              </section>
            </div>
          </motion.div>
        ) : (
          <div className="flex min-h-[55vh] items-center justify-center text-center">
            <div>
              <div className="text-[11px] font-black uppercase tracking-[0.2em] text-fg/58">No feeds to compare yet</div>
              <p className={cn('mt-3 text-[12px] font-bold', SOFT)}>Add feeders in Feed, then this board will fill itself.</p>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
