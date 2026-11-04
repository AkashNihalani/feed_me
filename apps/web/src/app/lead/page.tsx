'use client';

// Lead — who's winning, in numbers.
// Spine: the tape (the event stream) → Top of the feed (the leaders strip) → the Feederboard (media-first rows
// ranked on the TYPICAL post percentile, throne row for rank 1) → Hall of Feed
// (the standout posts: wins, likes, comments, engagement) → Follow-through (the
// follower curve and its spikes) → Feeding habits (cadence, prime time, the menu).
// Each section: a short handle (the cheeky name) and one plain line of what it
// measures. Every number traces to feeder-posts percentiles/multiples or the
// ascent series. No generated copy — verdict words are threshold math.

import Link from 'next/link';
import { CSSProperties, Ref, memo, startTransition, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState, useTransition } from 'react';
import { ChevronDown, Eye, Gauge, Heart, MessageCircle, Target, TrendingUp, Trophy, Users, type LucideIcon } from 'lucide-react';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { usePageReady } from '@/components/shell/AppShell';
import { MENU_APPLY_AFTER_CLOSE_MS, TimeframeControl, headerChoice, useTabHeader } from '@/components/shell/TabHeader';
import SlotText from '@/components/SlotText';
import { useCompressedOnScroll } from '@/lib/useCompressedOnScroll';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import { feedInitials, normalizeHandle, titleCase } from '@/lib/feedLabels';
import { BEAT_CONTAINER, BEAT_ITEM, BEAT_LEAVE_EASE, beatArrive, beatLeave } from '@/lib/motion';
import { getTabScope, setTabScope, useTabScope, useTabScopeTransition, type TabScope } from '@/lib/tabScope';
import { cn } from '@/lib/utils';
import { postMoment } from '@/lib/postMoment';
import { ms, sec } from '@/lib/tempo';
import { afterDrawn, holdsFlights } from '@/lib/webkitMotion';
import { CARRY_EASE_CSS, reducedMotion } from '@/lib/feedStream/motion';
import { LEAD_AT_KEY, leadScope } from '@/components/lead/leadHall';
import type { LeadActivity } from '@/app/api/lead/activity/route';
import LeadFollowers from '@/components/lead/LeadFollowers';
import LeadCadence from '@/components/lead/LeadCadence';
import LeadTiming from '@/components/lead/LeadTiming';
import LeadFormats from '@/components/lead/LeadFormats';
import { CrossfadeImage, DIM, MARK, NOTE, SOFT, Swap, holdArrival, useHeldNear, useWide, whenIdle } from '@/components/lead/leadKit';
import { aModalIsOpen, typingIn } from '@/lib/keys';
import { LEDGER_DEAL_MS, LEDGER_MOVE_MS, Ledger, keepBelow, ledgerDelay } from '@/components/lead/ledgerMotion';
import { ascentFromActivity, followersFor, formatsFor, readsBefore, remember, skylineFor, timingFor } from '@/components/lead/leadModels';
import { viewsLabel, viewsNote, viewsOf } from '@/components/lead/leadPlays';
import { RAMP, shade } from '@/components/read/readShade';
import { announceFirstScreen } from '@/lib/launch';
import { keptLead, leadOnHand, loadLead, type LeadLoad } from '@/components/lead/leadData';
import './lead.css';

const TIMEFRAMES = [7, 30, 90] as const;
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
  typical: number | null;
  best: number | null;
  bestPost: TrackedPost | null;
  lastPostedAt: number | null;
  // its posts in the window that landed in the top 10%
  hits: number;
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

function lastPostedWords(time: number | null) {
  if (!time) return '—';
  const days = Math.max(0, Math.floor((Date.now() - time) / 86_400_000));
  return days === 0 ? 'Today' : days === 1 ? 'Yesterday' : `${days} days ago`;
}

/* the pick every tab shares, as Lead reads it: a feed Lead doesn't have (or none yet) is all feeds (components/lead/leadHall,
   which Feed's profile reads too) */
function resolveScope(feeds: RuntimeFeed[], scope: TabScope) {
  return leadScope(feeds, scope);
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
   the page is read against this — that's where the direction comes from. 90D has
   none (only 90 days are kept, see readsBefore), so nothing there shows a move. */
const NO_PRIOR_POSTS: TrackedPost[] = [];
function postsInPriorWindow(posts: TrackedPost[], days: number) {
  if (!readsBefore(days)) return NO_PRIOR_POSTS;
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
  // the row's three best posts with a picture: its receipts, best first (RowExpansion)
  const proofPosts = sortedPosts.filter((post) => post.thumbnailUrl).slice(0, 3);
  const bestPost = sortedPosts[0] || null;
  const typical = median(posts.map((post) => post.latestPercentile));
  const best = bestPost?.latestPercentile ?? null;
  const lastPostedAt = posts.reduce<number | null>((max, post) => {
    const time = postTime(post);
    return time > 0 && (max == null || time > max) ? time : max;
  }, null);
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
    typical,
    best,
    bestPost,
    lastPostedAt,
    hits: posts.filter((post) => post.latestPercentile != null && post.latestPercentile <= 10).length,
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
    const prior = readsBefore(days) ? build((posts) => postsInPriorWindow(posts, days), () => [], [now - spanMs * 2, now - spanMs]) : [];
    return withoutBefore(rankRows(current, prior), days);
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
  const prior = readsBefore(days) ? buildFeeders((posts) => postsInPriorWindow(posts, days), () => []) : [];
  return withoutBefore(rankRows(current, prior), days);
}

/* at 90D there is no window before (see readsBefore): no "prev N" posts either */
function withoutBefore(rows: BoardRow[], days: number) {
  return readsBefore(days) ? rows : rows.map((row) => ({ ...row, rankMove: null, typicalShift: null, prevPostCount: null }));
}

/* What each part of the page shows for one scope and window, built once (see remember) and shared: the leaders and
   the board both read the rows, the board, the cards, the facts and the post mortem the scope's posts. */
function rowsFor(feeds: RuntimeFeed[], activeFeedId: string, days: number) {
  return remember(feeds, null, `rows|${activeFeedId}|${days}`, () => buildRows(feeds, activeFeedId, days));
}

function tapeFor(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number) {
  return remember(feeds, null, `tape|${activeFeedId}|${feederHandle}|${days}`, () => buildTapeEvents(feeds, activeFeedId, feederHandle, days));
}

/* the scope's own typical post in the window: the par line between the board's rows */
function parFor(feeds: RuntimeFeed[], activeFeedId: string, days: number) {
  return remember(feeds, null, `par|${activeFeedId}|${days}`, () => median(scopedPostsFor(feeds, activeFeedId, days).map((post) => post.latestPercentile)));
}

function postsFor(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number): ScopedPost[] {
  return remember(feeds, null, `posts|${activeFeedId}|${feederHandle}|${days}`, () => {
    const scoped = scopedPostsFor(feeds, activeFeedId, days);
    return feederHandle ? scoped.filter((post) => normalizeHandle(post.handle) === feederHandle) : scoped;
  });
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
      () => pointersFor(feeds, id, handle, range),
      () => tapeFor(feeds, id, handle, range),
      () => parFor(feeds, id, range),
      () => postsFor(feeds, id, handle, range),
      () => TOP_SORTS.forEach((sort) => topPostsFor(feeds, id, handle, range, sort.id)),
      () => followersFor(scope),
      () => skylineFor(scope),
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
    choice: headerChoice({ name: 'Time range', value: days, options: TIMEFRAMES, label: (range) => `${range}D`, onChange: onDaysChange }),
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

/* Deterministic market events from data already on hand: checkpoint climbs and cools, fresh reads, each feeder's
   current run of posts above or under its usual (3 or more in a row, newest first), and each feed's latest follower
   day-move. The kinds take turns (the biggest of each first), so the tape never runs one kind fourteen times over. */
function buildTapeEvents(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number): TapeEvent[] {
  const scoped = activeFeedId === 'all' ? feeds : feeds.filter((feed) => feed.id === activeFeedId);
  const inScope = (post: TrackedPost) => !feederHandle || normalizeHandle(post.handle) === feederHandle;
  type Sized = TapeEvent & { size: number };
  const climbs: Sized[] = [];
  const cools: Sized[] = [];
  const reads: Sized[] = [];
  const runs: Sized[] = [];
  const freshCutoff = Date.now() - 2 * 24 * 60 * 60 * 1000;

  for (const feed of scoped) {
    const windowed = postsInWindow(feed.posts, days).filter(inScope);
    for (const post of windowed) {
      const handle = normalizeHandle(post.handle) || titleCase(feed.title).toLowerCase();
      const media = post.mediaType !== 'unknown' ? post.mediaType : 'post';
      const trajectory = climbOf(post);
      if (trajectory != null && Math.abs(trajectory) >= 5 && post.firstPercentile != null && post.latestPercentile != null) {
        const intoTopTen = trajectory > 0 && post.latestPercentile <= 10 && post.firstPercentile > 10;
        (trajectory > 0 ? climbs : cools).push({
          id: `climb:${post.postKey}`,
          text: `@${handle} ${media} ${trajectory > 0 ? (intoTopTen ? 'into the top 10%' : 'climbing') : 'cooling'}`,
          value: `${percentLabel(post.firstPercentile)}% → ${percentLabel(post.latestPercentile)}%`,
          dir: trajectory > 0 ? 'up' : 'down',
          size: Math.abs(trajectory) + (intoTopTen ? 100 : 0),
        });
        continue;
      }
      const readTime = Date.parse(post.latestBusinessDayIst || '');
      if (Number.isFinite(readTime) && readTime >= freshCutoff && post.latestPercentile != null) {
        reads.push({
          id: `read:${post.postKey}`,
          text: `@${handle} ${(post.latestCheckpoint || 'd3').toUpperCase()} read`,
          value: `TOP ${percentLabel(post.latestPercentile)}%`,
          dir: post.latestPercentile <= 20 ? 'up' : 'flat',
          size: 100 - post.latestPercentile,
        });
      }
    }
    // each feeder's run at its newest end: above its usual (1.1× or more) or under it (0.9× or less)
    const byHandle = new Map<string, TrackedPost[]>();
    for (const post of windowed) {
      const handle = normalizeHandle(post.handle);
      if (!handle || post.rankingMultiple == null || !postTime(post)) continue;
      const list = byHandle.get(handle);
      if (list) list.push(post);
      else byHandle.set(handle, [post]);
    }
    for (const [handle, list] of byHandle) {
      list.sort((a, b) => postTime(b) - postTime(a));
      let run = 0;
      let dir = 0;
      for (const post of list) {
        const multiple = post.rankingMultiple as number;
        const step = multiple >= 1.1 ? 1 : multiple > 0 && multiple <= 0.9 ? -1 : 0;
        if (!step || (dir && step !== dir)) break;
        dir = step;
        run += 1;
      }
      if (run >= 3) runs.push({ id: `run:${handle}`, text: `@${handle} ${run} posts ${dir > 0 ? 'above' : 'under'} its usual`, value: `${run} in a row`, dir: dir > 0 ? 'up' : 'down', size: run });
    }
  }

  const followerMoves: Sized[] = scoped.flatMap((feed) => {
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
      size: 0,
    }];
  });

  const biggest = (a: Sized, b: Sized) => b.size - a.size;
  const lists = [climbs.sort(biggest).slice(0, 5), runs.sort(biggest).slice(0, 4), followerMoves, reads.sort(biggest).slice(0, 3), cools.sort(biggest).slice(0, 3)];
  const events: TapeEvent[] = [];
  for (let turn = 0; events.length < 14 && lists.some((list) => list.length > turn); turn += 1) {
    for (const list of lists) {
      const event = list[turn];
      if (event && events.length < 14) events.push({ id: event.id, text: event.text, value: event.value, dir: event.dir });
    }
  }
  return events;
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

/* ── Leaders strip — three cards, three different questions ─────────────── */

/* ── The pointers: the list the board opens on ──────────────────────────────
   Six glanceable reads of the scope's window, each a number first and then who or what it is about: the best post,
   the typical post (on the Top % ramp, against the window before at 7D and 30D), the follower gains, the post that
   climbed furthest since its first read, the post talked about most and the post liked most, each against its own
   account's usual. A pointer is a door: it opens the ranking it heads (Top posts), the par line or Followers. */

type PointerGo = 'rank' | 'likes' | 'comments' | 'climb' | 'views' | 'par' | 'gains';
type PointerFoot =
  | { kind: 'who'; handle: string; pic: string | null }
  | { kind: 'ruler'; now: number | null; was: number | null }
  | { kind: 'spark'; values: number[] }
  | { kind: 'share'; share: number };
type Pointer = {
  key: string;
  label: string;
  icon: LucideIcon;
  go: PointerGo;
  value: string;
  win: boolean;
  // what the number counts, in one short line (two on a narrow tile)
  unit: string;
  shift: { up: boolean } | null;
  foot: PointerFoot | null;
};

/* every feeder's face by handle, for a pointer or a card that names one */
function facesOf(feeds: RuntimeFeed[]) {
  return remember(feeds, null, 'faces', () => new Map(feeds.flatMap((feed) => feed.feeders.map((feeder) => [normalizeHandle(feeder.handle), feeder.profilePicUrl || null] as const))));
}

/* the scope's followers day by day across the window: each feed's own readings, summed on the days every feed was read */
function followerSpark(scoped: RuntimeFeed[], days: number) {
  const from = Date.now() - days * 86_400_000;
  const byDay = new Map<number, { sum: number; feeds: number }>();
  for (const feed of scoped) {
    for (const point of ascentSeries(feed.ascent)) {
      if (point.time < from) continue;
      const day = byDay.get(point.time) ?? { sum: 0, feeds: 0 };
      day.sum += point.value;
      day.feeds += 1;
      byDay.set(point.time, day);
    }
  }
  const values = [...byDay.entries()].filter(([, day]) => day.feeds === scoped.length).sort((a, b) => a[0] - b[0]).map(([, day]) => day.sum);
  return values.length >= 2 ? values : null;
}

const mediaWord = (post: TrackedPost) => (post.mediaType === 'unknown' ? 'post' : post.mediaType);

function buildPointers(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number): Pointer[] {
  const scoped = activeFeedId === 'all' ? feeds : feeds.filter((feed) => feed.id === activeFeedId);
  const inScope = (post: TrackedPost) => !feederHandle || normalizeHandle(post.handle) === feederHandle;
  const posts = postsFor(feeds, activeFeedId, feederHandle, days);
  const prior = scoped.flatMap((feed) => postsInPriorWindow(feed.posts, days).filter(inScope));
  const faces = facesOf(feeds);
  const who = (post: TrackedPost | null | undefined): PointerFoot | null => {
    const handle = normalizeHandle(post?.handle);
    return handle ? { kind: 'who', handle, pic: faces.get(handle) ?? null } : null;
  };
  const compared = readsBefore(days);
  const top = (value: number | null) => (value == null ? '—' : `Top ${percentLabel(value)}%`);

  const best = [...posts].filter((post) => post.latestPercentile != null).sort(bestFirst)[0] ?? null;

  const ranked = posts.filter((post) => post.latestPercentile != null);
  const typical = median(ranked.map((post) => post.latestPercentile));
  const priorTypical = compared ? median(prior.map((post) => post.latestPercentile)) : null;
  const typicalShift = typical != null && priorTypical != null ? Math.round(priorTypical - typical) : null;
  // every account's posts are ranked against its own: one in ten lands in its top 10% on an ordinary run
  const hits = ranked.filter((post) => (post.latestPercentile ?? 101) <= 10).length;
  const hitShare = ranked.length ? hits / ranked.length : 0;

  const now = Date.now();
  const spanMs = days * 86_400_000;
  const nets = scoped.map((feed) => windowDelta(feed.ascent, now - spanMs, now)).filter((value): value is number => value != null);
  const priors = compared ? scoped.map((feed) => windowDelta(feed.ascent, now - spanMs * 2, now - spanMs)).filter((value): value is number => value != null) : [];
  const net = nets.length ? nets.reduce((sum, value) => sum + value, 0) : null;
  const priorNet = priors.length ? priors.reduce((sum, value) => sum + value, 0) : null;
  const spark = followerSpark(scoped, days);

  const climber = posts.map((post) => ({ post, climb: climbOf(post) ?? 0 })).filter((entry) => entry.climb > 0).sort((a, b) => b.climb - a.climb)[0] ?? null;
  const talked = posts.filter((post) => (post.commentsMultiple ?? 0) > 0).sort((a, b) => (b.commentsMultiple ?? 0) - (a.commentsMultiple ?? 0))[0] ?? null;
  const liked = posts.filter((post) => (post.likesMultiple ?? 0) > 0).sort((a, b) => (b.likesMultiple ?? 0) - (a.likesMultiple ?? 0))[0] ?? null;
  // views only where they were counted (data/apifyPlays.json until the worker stores them): the tile says when there are none
  const viewed = posts.filter((post) => viewsOf(post) != null).sort((a, b) => (viewsOf(b) ?? 0) - (viewsOf(a) ?? 0));
  const mostViewed = viewed[0] ?? null;

  // the first four say how the scope is doing; the last four are the posts that stood out
  return [
    {
      key: 'best', label: 'Best post', icon: Trophy, go: 'rank', value: top(best?.latestPercentile ?? null), win: best?.latestPercentile != null && best.latestPercentile <= 10,
      unit: best ? `${formatMultiple(best.rankingMultiple)} its usual · ${mediaWord(best)}` : 'No reads yet', shift: null, foot: who(best),
    },
    {
      key: 'typical', label: 'Typical post', icon: Gauge, go: 'par', value: top(typical), win: false,
      unit: !compared || typicalShift == null
        ? `${ranked.length.toLocaleString('en-US')} ranked ${ranked.length === 1 ? 'post' : 'posts'} in ${days}D`
        : typicalShift === 0 ? `level with the ${days}D before` : `${Math.abs(typicalShift)} ${Math.abs(typicalShift) === 1 ? 'pt' : 'pts'} on the ${days}D before`,
      shift: typicalShift ? { up: typicalShift > 0 } : null,
      foot: { kind: 'ruler', now: typical, was: priorTypical },
    },
    {
      key: 'hits', label: 'Top 10%', icon: Target, go: 'rank', value: String(hits), win: ranked.length > 0 && hitShare > 0.1,
      unit: ranked.length ? `of ${ranked.length.toLocaleString('en-US')} landed in their top 10%` : 'No reads yet', shift: null,
      foot: ranked.length ? { kind: 'share', share: hitShare } : null,
    },
    {
      key: 'gains', label: 'Gains', icon: Users, go: 'gains', value: net == null ? '—' : signedNumber(net), win: false,
      unit: net == null ? 'No follower readings yet' : priorNet == null ? `net followers in ${days}D` : `from ${signedNumber(priorNet)} the ${days}D before`,
      shift: net != null && priorNet != null && Math.round(net) !== Math.round(priorNet) ? { up: net > priorNet } : null,
      foot: spark ? { kind: 'spark', values: spark } : null,
    },
    {
      key: 'climber', label: 'Climber', icon: TrendingUp, go: 'climb', value: climber ? `+${Math.round(climber.climb)}` : '—', win: false,
      unit: climber ? `places · ${percentLabel(climber.post.firstPercentile ?? null)}% → ${percentLabel(climber.post.latestPercentile)}% since ${climber.post.firstCheckpoint || 'D1'}` : 'Nothing climbed yet',
      shift: null, foot: who(climber?.post),
    },
    {
      key: 'talked', label: 'Talked about', icon: MessageCircle, go: 'comments', value: talked ? formatMultiple(talked.commentsMultiple) : '—', win: false,
      unit: talked ? `its usual comments · ${compactNumber(numeric(talked.comments))}` : 'No comments read yet', shift: null, foot: who(talked),
    },
    {
      key: 'liked', label: 'Most liked', icon: Heart, go: 'likes', value: liked ? formatMultiple(liked.likesMultiple) : '—', win: false,
      unit: liked ? `its usual likes · ${compactNumber(numeric(liked.likes))}` : 'No likes read yet', shift: null, foot: who(liked),
    },
    {
      key: 'viewed', label: 'Most viewed', icon: Eye, go: 'views', value: mostViewed ? compactNumber(viewsOf(mostViewed) ?? 0) : '—', win: false,
      unit: mostViewed ? `views · the most of ${viewed.length} counted` : 'No views counted in this window', shift: null, foot: who(mostViewed),
    },
  ];
}

function pointersFor(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number) {
  return remember(feeds, null, `pointers|${activeFeedId}|${feederHandle}|${days}`, () => buildPointers(feeds, activeFeedId, feederHandle, days));
}

/* a feeder's face as a small circle, its initials when the picture won't come (isolated: the avatar's own layers stay in it) */
function FaceDot({ handle, pic, size }: { handle: string; pic: string | null; size: string }) {
  return (
    <span className={cn('relative isolate block shrink-0', size)}>
      <FeederStoryAvatar feeder={{ handle, profilePicUrl: pic }} className="text-[8px]" />
    </span>
  );
}

/* where the typical post sits on the Top % ramp (1% at the left), and where it sat the window before (a hollow ring).
   Each marker is a full-width layer moved along by a transform, so a switch glides it on the compositor */
function PointerRuler({ now, was }: { now: number | null; was: number | null }) {
  const at = (value: number) => `${(((Math.min(100, Math.max(1, value)) - 1) / 99) * 100).toFixed(1)}%`;
  return (
    <span className="relative block h-[22px] w-full">
      <span className={cn('absolute inset-x-0 -top-0.5 flex justify-between text-[9px] font-black tracking-[0.06em]', DIM)}><span>1%</span><span>100%</span></span>
      <i aria-hidden="true" className="absolute inset-x-0 top-[11px] block h-1 rounded-full" style={{ background: RAMP }} />
      {was != null ? (
        <span aria-hidden="true" className="lead-glide absolute inset-x-0 top-[7px] block h-3" style={{ transform: `translateX(${at(was)})` }}>
          <i className="absolute -ml-1.5 block h-3 w-3 rounded-full bg-(--ld-panel) shadow-[inset_0_0_0_2px_rgb(var(--fm-fg-rgb)/0.35)]" />
        </span>
      ) : null}
      {now != null ? (
        <span aria-hidden="true" className="lead-glide absolute inset-x-0 top-[7px] block h-3" style={{ transform: `translateX(${at(now)})` }}>
          <i className="absolute -ml-1.5 block h-3 w-3 rounded-full bg-fg shadow-[0_0_0_2px_var(--ld-panel)]" />
        </span>
      ) : null}
    </span>
  );
}

/* the scope's followers across the window as a small line over its start (dashed), its last reading in rose; a new
   window's line rises in like a changed line */
function PointerSpark({ values }: { values: number[] }) {
  const width = 100;
  const height = 22;
  const lo = Math.min(...values);
  const hi = Math.max(...values);
  const x = (index: number) => (index / (values.length - 1)) * width;
  const y = (value: number) => 2 + ((hi - value) / (hi - lo || 1)) * (height - 4);
  const line = values.map((value, index) => `${index ? 'L' : 'M'}${x(index).toFixed(1)} ${y(value).toFixed(1)}`).join('');
  const key = `${values.length}|${values[0]}|${values[values.length - 1]}`;
  return (
    <span key={key} ref={holdArrival} className="lead-swap-in block w-full">
      <svg viewBox={`0 0 ${width} ${height}`} preserveAspectRatio="none" aria-hidden="true" className="block h-[22px] w-full overflow-visible">
        <path d={`M0 ${y(values[0]).toFixed(1)}H${width}`} stroke="rgb(var(--fm-fg-rgb) / 0.2)" strokeDasharray="2 2" vectorEffect="non-scaling-stroke" fill="none" />
        <path d={`${line}L${width} ${height}L0 ${height}Z`} fill="rgb(var(--fm-fg-rgb) / 0.07)" />
        <path d={line} fill="none" stroke="var(--fm-fg)" strokeWidth="1.8" vectorEffect="non-scaling-stroke" strokeLinejoin="round" />
        <circle cx={x(values.length - 1)} cy={y(values[values.length - 1])} r="2.4" fill="var(--fm-accent)" />
      </svg>
    </span>
  );
}

/* the share of the window's ranked posts that landed in their account's top 10%, against the one in ten an ordinary
   run lands (the tick): the bar glides to a new length on the compositor */
function PointerShare({ share }: { share: number }) {
  const scale = 0.25;
  const fill = Math.min(1, share / scale);
  return (
    <span className="flex w-full items-center gap-2.5">
      <span className="relative block h-1.5 min-w-0 flex-1 overflow-hidden rounded-full bg-fg/[0.08]">
        <span aria-hidden="true" className={cn('lead-glide absolute inset-0 origin-left rounded-full', share > 0.1 ? 'bg-[var(--fm-accent)]' : 'bg-fg/70')} style={{ transform: `scaleX(${fill.toFixed(3)})` }} />
        <i aria-hidden="true" className="absolute inset-y-[-2px] block w-[2px] rounded-full bg-(--ld-panel)" style={{ left: `${(0.1 / scale) * 100}%` }} />
      </span>
      <span className={cn('shrink-0 text-[12px] font-black tabular-nums', share > 0.1 ? 'text-[var(--fm-accent-text)]' : 'text-fg/70')}>
        <SlotText value={`${Math.round(share * 100)}%`} />
      </span>
    </span>
  );
}

/* The pointers: eight tiles in two rows of four (two across on a phone). Each is the same shape: what it is, its
   number (rolling on its beat when the window or scope changes), one line on what the number counts, and a foot
   under a hairline: who it is about, or the picture of it (the ramp, the follower line, the share of hits). The
   tiles arrive with the page on its beat; a tap opens what the tile points at. */
function PointersBand({ pointers, onGo }: { pointers: Pointer[]; onGo: (go: PointerGo) => void }) {
  const reduce = Boolean(useReducedMotion());
  return (
    <motion.div
      variants={BEAT_CONTAINER}
      initial={reduce ? false : 'hidden'}
      animate="visible"
      className="grid grid-cols-2 gap-2 border-b border-fg/[0.08] px-3 py-3 sm:grid-cols-4 sm:gap-2.5 sm:px-4 sm:py-4 lg:gap-3 xl:px-5"
    >
      {pointers.map((pointer, index) => {
        const Icon = pointer.icon;
        const foot = pointer.foot;
        return (
          <motion.button
            key={pointer.key}
            variants={BEAT_ITEM}
            type="button"
            onClick={() => onGo(pointer.go)}
            data-testid={`lead-v3-pointer-${pointer.key}`}
            aria-label={`${pointer.label}: ${pointer.value}${foot?.kind === 'who' ? `, @${foot.handle}` : ''}. ${pointer.unit}`}
            className="group relative flex min-w-0 flex-col rounded-[18px] border border-fg/[0.07] bg-(--ld-raised) px-3.5 pb-2.5 pt-2.5 text-left shadow-[inset_0_1px_0_rgb(255_255_255/0.6)] transition-[border-color,transform,box-shadow] duration-[calc(260ms*var(--fm-tempo))] ease-[cubic-bezier(.3,.3,.2,1)] hover:-translate-y-px hover:border-fg/[0.14] hover:shadow-(--ld-shadow-thumb) focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)] active:translate-y-0 dark:shadow-[inset_0_1px_0_rgb(255_255_255/0.04)] sm:px-4 sm:pb-3 sm:pt-3.5"
          >
            <span className="flex items-center gap-1.5">
              <Icon aria-hidden="true" strokeWidth={2.2} className={cn('h-3.5 w-3.5 shrink-0', pointer.win ? 'text-[var(--fm-accent)]' : 'text-fg/45')} />
              <span className={cn('min-w-0 flex-1 truncate', MARK, DIM)}>{pointer.label}</span>
              <svg aria-hidden="true" viewBox="0 0 12 12" className="h-3 w-3 shrink-0 text-fg/30 transition-[color,transform] duration-[calc(240ms*var(--fm-tempo))] group-hover:translate-x-0.5 group-hover:text-fg">
                <path d="M3 6h6M6.5 3.5 9 6 6.5 8.5" fill="none" stroke="currentColor" strokeWidth="1.6" strokeLinecap="round" strokeLinejoin="round" />
              </svg>
            </span>
            <span className={cn('fm-depth-title mt-2 block truncate text-[26px] font-black tabular-nums leading-none tracking-[-0.045em] sm:mt-3 sm:text-[28px] lg:text-[32px] laptop:text-[28px]', pointer.win ? 'text-[var(--fm-accent-text)]' : 'text-fg')}>
              <SlotText value={pointer.value} delay={index * ms(40)} />
            </span>
            <span className={cn('mt-1 flex h-8 min-w-0 items-start gap-1 overflow-hidden sm:mt-1.5 lg:h-4', NOTE, SOFT)}>
              {pointer.shift ? <i className={cn('shrink-0 pt-[3px] not-italic text-[8px] leading-none', pointer.shift.up ? 'text-[var(--fm-accent)]' : 'text-fg/45')}>{pointer.shift.up ? '▲' : '▼'}</i> : null}
              <Swap value={pointer.unit} className="line-clamp-2! lg:line-clamp-1!" />
            </span>
            <span className="mt-2 flex h-[30px] min-w-0 items-center border-t border-fg/[0.07] pt-2 sm:mt-3 sm:h-[34px] sm:pt-2.5">
              {foot?.kind === 'who' ? (
                <span className="flex min-w-0 items-center gap-2">
                  <FaceDot handle={foot.handle} pic={foot.pic} size="h-[22px] w-[22px]" />
                  <b className="min-w-0 truncate text-[12px] font-black text-fg"><Swap value={`@${foot.handle}`} /></b>
                </span>
              ) : foot?.kind === 'ruler' ? <PointerRuler now={foot.now} was={foot.was} />
                : foot?.kind === 'spark' ? <PointerSpark values={foot.values} />
                  : foot?.kind === 'share' ? <PointerShare share={foot.share} />
                    : <span className={cn(NOTE, DIM)}>—</span>}
            </span>
          </motion.button>
        );
      })}
    </motion.div>
  );
}

/* bring a part of the page to the top of the reader's view (under the header) and ring it once, so a pointer's tap
   lands somewhere you can see */
function jumpTo(el: HTMLElement | null, ring?: HTMLElement | null) {
  if (!el) return;
  const page = el.closest<HTMLElement>('[data-testid="lead-v3-page"]');
  const top = (page ? parseFloat(getComputedStyle(page).scrollPaddingTop) : NaN) || 12;
  const delta = el.getBoundingClientRect().top - top - 8;
  const behavior: ScrollBehavior = reducedMotion() ? 'auto' : 'smooth';
  if (page && page.scrollHeight > page.clientHeight + 10) page.scrollBy({ top: delta, behavior });
  else window.scrollBy({ top: delta, behavior });
  const lit = ring ?? el;
  if (reducedMotion()) return;
  lit.animate(
    [{ boxShadow: '0 0 0 2px rgb(var(--fm-fg-rgb) / 0.8)' }, { boxShadow: '0 0 0 2px rgb(var(--fm-fg-rgb) / 0)' }],
    { duration: ms(1600), delay: ms(420), easing: CARRY_EASE_CSS, fill: 'backwards' },
  );
}

/* ── Feederboard rows ────────────────────────────────────────────────────── */

/* bring a piece into view clear of the page's top (the status bar on a phone) and the dock below: the page's own scroll
   padding says where */
function revealBelowHeader(el: HTMLElement) {
  const page = el.closest<HTMLElement>('[data-testid="lead-v3-page"]');
  const top = (page ? parseFloat(getComputedStyle(page).scrollPaddingTop) : NaN) || 12;
  const bottom = window.innerHeight - 110;
  const rect = el.getBoundingClientRect();
  let delta = 0;
  if (rect.top < top) delta = rect.top - top;
  else if (rect.bottom > bottom) delta = Math.min(rect.bottom - bottom, rect.top - top);
  if (Math.abs(delta) < 2) return;
  if (page && page.scrollHeight > page.clientHeight + 10) page.scrollBy({ top: delta, behavior: 'smooth' });
  else window.scrollBy({ top: delta, behavior: 'smooth' });
}

/* who the row is, drawn as the rail draws it: a feed as its initials on the rail's grey, a feeder as its story circle
   (its face, or its initials when the picture won't come). A face is a circle: its posts are the 4:5 pictures */
function RowFace({ row }: { row: BoardRow }) {
  if (row.id.startsWith('feed:')) {
    return (
      <span
        className="grid h-11 w-11 shrink-0 place-items-center rounded-full border-[2px] border-white text-[12px] font-black leading-none shadow-(--ld-shadow-thumb) dark:border-[var(--fm-ink)]"
        style={{ color: 'rgb(11 11 15 / 0.78)', background: 'linear-gradient(135deg,#f7f7f8,#cfd3dc)' }}
      >
        {feedInitials(row.name)}
      </span>
    );
  }
  return (
    <span className="relative isolate block h-11 w-11 shrink-0 rounded-full shadow-(--ld-shadow-thumb)">
      <FeederStoryAvatar feeder={{ handle: row.name.replace(/^@/, ''), profilePicUrl: row.profilePicUrl }} className="text-[13px]" />
    </span>
  );
}

/* what a row says under its name: a feed's feeders and its posts on All feeds, a feeder's posts in a feed */
function rowCaption(row: BoardRow) {
  const posts = `${row.postCount} ${row.postCount === 1 ? 'post' : 'posts'}`;
  return row.id.startsWith('feed:') ? `${row.caption} · ${posts}` : posts;
}

/* the typical post, the number the board ranks on: "TOP 53%", rolling on the row's beat */
function TypicalFigure({ row, delay, className }: { row: BoardRow; delay: number; className: string }) {
  return (
    <span className={cn('fm-depth-title flex items-baseline font-black tabular-nums leading-none tracking-[-0.04em] text-fg', className)}>
      <span className={cn('mr-1 self-start pt-[0.12em] text-[10px] tracking-[0.12em]', DIM)}>TOP</span>
      <NumberBox value={percentLabel(row.typical)} delay={delay} />
      {row.typical == null ? null : <i className="ml-0.5 not-italic text-[0.5em]">%</i>}
    </span>
  );
}

/* the typical post against the same window before it, in points ("▲ 7 pts"): a rise carries the accent on its ▲, the
   number stays ink; "level" when it held, "new" when the row had no posts before. 90D has no window before (nothing) */
function shiftWords(row: BoardRow) {
  if (row.typicalShift == null) return 'new';
  if (row.typicalShift === 0) return 'level';
  return Math.abs(row.typicalShift) === 1 ? 'pt' : 'pts';
}

function ShiftLine({ row, delay, className }: { row: BoardRow; delay: number; className?: string }) {
  const shift = row.typicalShift;
  const up = shift != null && shift > 0;
  return (
    <span className={cn('inline-flex items-baseline gap-1 font-black tabular-nums leading-none', up ? 'text-fg' : DIM, className)}>
      {shift != null && shift !== 0 ? <i className={cn('not-italic text-[0.62em]', up && 'text-[var(--fm-accent)]')}>{up ? '▲' : '▼'}</i> : null}
      {shift != null && shift !== 0 ? <SlotText value={String(Math.abs(shift))} delay={delay} /> : null}
      <Swap value={shiftWords(row)} />
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

/* One of a row's three best posts. The first leads (a large card, its number big); the second and third follow smaller
   beside it, their feet on one line. The picture is the post's own, its place over it and its Top % in the ramp's
   shade; under it the post's multiple of its account's usual and, when the app has them, its views. */
function ReceiptPost({ post, place, showHandle }: { post: TrackedPost; place: number; showHandle: boolean }) {
  const lead = place === 1;
  const tone = post.latestPercentile != null ? shade(post.latestPercentile) : null;
  const views = viewsLabel(post);
  const who = showHandle && post.handle ? `@${normalizeHandle(post.handle)}` : `${post.mediaType === 'unknown' ? 'post' : post.mediaType} · ${postDateLabel(post)}`;
  return (
    <motion.a
      variants={OPEN_ITEM}
      href={post.postUrl || '#'}
      target={post.postUrl ? '_blank' : undefined}
      rel={post.postUrl ? 'noreferrer' : undefined}
      onClick={(event) => {
        event.stopPropagation();
        if (!post.postUrl) event.preventDefault();
      }}
      data-testid={`lead-v3-receipt-${place}`}
      aria-label={`${rankLabel(place)}: ${post.latestPercentile == null ? 'not ranked yet' : `top ${percentLabel(post.latestPercentile)}%`}${views ? `, ${views}` : ''}${post.postUrl ? '. Opens on Instagram' : ''}`}
      className="group block min-w-0 rounded-[20px] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
    >
      <span className={cn(
        'relative block aspect-[4/5] overflow-hidden border border-white/12 bg-(--fm-media)',
        lead ? 'rounded-[20px] shadow-(--ld-shadow-proof-lg)' : 'rounded-[14px] shadow-(--ld-shadow-proof)',
      )}>
        <CrossfadeImage src={post.thumbnailUrl} className="transition-transform duration-[calc(700ms*var(--fm-tempo))] group-hover:scale-[1.035]" />
        <span aria-hidden="true" className="absolute inset-x-0 top-0 h-[44%] bg-[linear-gradient(180deg,rgba(0,0,0,.44),transparent)]" />
        {lead ? null : <span aria-hidden="true" className="absolute inset-x-0 bottom-0 h-[36%] bg-[linear-gradient(0deg,rgba(0,0,0,.4),transparent)]" />}
        <span aria-hidden="true" className={cn('absolute left-2.5 top-2 font-black tabular-nums leading-none tracking-[-0.06em] text-white [text-shadow:0_1px_8px_rgba(0,0,0,.35)]', lead ? 'text-[24px]' : 'text-[19px]')}>{rankLabel(place)}</span>
        {/* the lead has room for its Top % beside its place; a smaller card carries it at its foot */}
        <span
          className={cn('absolute rounded-[7px] px-1.5 py-0.5 font-black tabular-nums shadow-[0_2px_8px_rgba(0,0,0,.2)]', lead ? 'right-2 top-2 text-[11px]' : 'bottom-2 left-2 text-[10px]')}
          style={{ background: tone ? tone.c : 'rgba(0,0,0,.55)', color: tone ? tone.tx : '#ffffff' }}
        >
          {post.latestPercentile == null ? 'new' : `Top ${percentLabel(post.latestPercentile)}%`}
        </span>
      </span>
      <span className="mt-2.5 block min-w-0 px-0.5">
        <span className={cn('fm-depth-title block whitespace-nowrap font-black tabular-nums leading-none tracking-[-0.045em] text-fg', lead ? 'text-[26px] sm:text-[30px]' : 'text-[19px] sm:text-[21px]')}>
          {formatMultiple(post.rankingMultiple)}
        </span>
        <span className={cn('mt-1.5 block truncate', NOTE, SOFT)}>{lead ? `its usual${views ? ` · ${views}` : ''}` : views ?? 'its usual'}</span>
        <span className={cn('mt-0.5 block truncate', NOTE, DIM)}>{who}</span>
      </span>
    </motion.a>
  );
}

/* An open row's receipts: its three best posts in the window, best first and leading, then how much it posted, how
   many of those landed in the top 10% and when it last posted. The ref (AnimatePresence's, in popLayout) is its
   element's: closing, it leaves the flow at once and fades where it was while the rows under it travel up over it
   (components/lead/ledgerMotion); opening, its parts come in one after another (OPEN_GRID) as the rows make room. */
function RowExpansion({ row, days, onCollapse, ref }: { row: BoardRow; days: number; onCollapse: () => void; ref?: Ref<HTMLDivElement> }) {
  const reduce = Boolean(useReducedMotion());
  const showHandle = row.id.startsWith('feed:');
  const facts = [
    {
      label: 'Posts',
      value: String(row.postCount),
      note: row.prevPostCount == null ? `in ${days}D` : row.prevPostCount === row.postCount ? `as many as the ${days}D before` : `${row.prevPostCount} the ${days}D before`,
    },
    { label: 'In the top 10%', value: String(row.hits), note: `of ${row.postCount} ${row.postCount === 1 ? 'post' : 'posts'}` },
    {
      label: 'Last posted',
      value: lastPostedWords(row.lastPostedAt),
      note: row.lastPostedAt ? new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', timeZone: 'Asia/Kolkata' }).format(row.lastPostedAt) : 'nothing in this window',
    },
    // how its typical post did against its own usual, and the audience it posts to
    { label: 'Likes', value: row.likesMultiple == null ? '—' : formatMultiple(row.likesMultiple), note: `typical post · ${compactNumber(row.likes)} in all` },
    { label: 'Comments', value: row.commentsMultiple == null ? '—' : formatMultiple(row.commentsMultiple), note: `typical post · ${compactNumber(row.comments)} in all` },
    {
      label: 'Followers',
      value: compactNumber(row.followers),
      note: row.followerDelta == null ? 'latest reading' : `${signedNumber(row.followerDelta)} in ${days}D`,
    },
  ];

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
        className="relative isolate cursor-pointer border-t border-fg/[0.08] bg-(--ld-raised) px-4 pb-5 pt-4 sm:px-6 sm:pb-6 lg:pl-[80px] lg:pr-6 xl:pl-[100px]"
      >
        <motion.div variants={OPEN_GRID} className="grid gap-5 lg:grid-cols-[auto_minmax(0,1fr)] lg:items-end lg:gap-10">
          {row.proofPosts.length ? (
            <div className="grid w-full min-w-0 grid-cols-[1.3fr_1fr_1fr] items-end gap-2.5 sm:max-w-[540px] sm:gap-4 lg:w-[540px]">
              {row.proofPosts.map((post, index) => <ReceiptPost key={post.postKey} post={post} place={index + 1} showHandle={showHandle} />)}
            </div>
          ) : (
            <motion.div variants={OPEN_ITEM} className={cn('flex h-[120px] items-center justify-center border-y border-fg/[0.07]', MARK, DIM)}>No ranked posts with a picture in {days}D</motion.div>
          )}
          {/* six facts, three across, filling the room beside the posts (under them on a phone) */}
          <motion.dl variants={OPEN_ITEM} className="grid min-w-0 grid-cols-3 gap-px self-stretch overflow-hidden rounded-[18px] border border-fg/[0.07] bg-fg/[0.07] lg:self-end">
            {facts.map((fact) => (
              <div key={fact.label} className="min-w-0 bg-(--ld-panel) px-3 py-3 sm:px-4 lg:px-5 lg:py-4">
                <dt className={cn('truncate', MARK, DIM)}>{fact.label}</dt>
                <dd className="fm-depth-title mt-2 truncate text-[18px] font-black tabular-nums leading-none tracking-[-0.03em] text-fg sm:text-[22px] lg:text-[26px]">{fact.value}</dd>
                <div className={cn('mt-1 line-clamp-2 lg:truncate', NOTE, DIM)}>{fact.note}</div>
              </div>
            ))}
          </motion.dl>
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

/* The first three places are a podium: the rose steps down from first to third (its numeral, its ground's wash, the
   first-place light), quieter each place, so the top three read as winners without three equal loud rows */
const PODIUM = [1, 0.6, 0.36];
const podiumOf = (place: number) => (place >= 1 && place <= PODIUM.length ? PODIUM[place - 1] : 0);

/* the place's numeral, rolling to the row's new rank on its beat; on places 1–3 a rose numeral over it fades in on the
   same beat, as strong as its place (opacity on the compositor: the ink itself never changes colour) */
function RankNumeral({ rank, delay, size }: { rank: number; delay: number; size: string }) {
  const at = 'pointer-events-none absolute left-0 top-1/2 -translate-y-1/2 select-none font-black tabular-nums leading-none tracking-[-0.08em]';
  return (
    <span className="relative h-full">
      {/* on the podium the rose numeral stands alone: the ink one under it fades out (an ink edge peeking out around
          the rose reads as an outline, light on the dark theme) */}
      <span className={cn('ld-throne', at, size, 'text-fg/[0.14]')} style={{ opacity: podiumOf(rank) ? 0 : 1 }}>
        <SlotText value={rankLabel(rank)} delay={delay} />
      </span>
      {PODIUM.map((strength, index) => (
        <span key={index} aria-hidden="true" className={cn('ld-throne', at, size, 'text-[var(--fm-accent)]')} style={{ opacity: rank === index + 1 ? strength : 0 }}>
          {rankLabel(index + 1)}
        </span>
      ))}
    </span>
  );
}

/* The first place's light and the row's ground sit behind the row. The light is always there and lit on the row
   holding first place, so when first place changes hands it passes on the rows' beat instead of jumping. The ground
   is a soft wash that runs as far as the row's typical post is strong (rose on first place), so it changes with the
   window; it brightens as it sweeps to a new length and settles back (the ledger plays that). */
function RowGround({ place, strength }: { place: number; strength: number }) {
  const podium = podiumOf(place);
  return (
    <>
      <span aria-hidden="true" className="ld-throne pointer-events-none absolute inset-0 z-0 bg-[radial-gradient(circle_at_0_0,rgb(var(--fm-accent-rgb)/var(--ld-throne-glow)),transparent_44%)]" style={{ opacity: podium * podium }} />
      {/* the wash: ink for everyone, rose laid over it on the podium (both inside the one layer the ledger sweeps) */}
      <span aria-hidden="true" data-ledger-sweep="" className="pointer-events-none absolute inset-y-0 left-0 z-0 w-full origin-left" style={{ transform: `scaleX(${strength.toFixed(3)})`, opacity: 0.5 }}>
        <span className="ld-throne absolute inset-0 bg-[linear-gradient(90deg,rgb(var(--fm-fg-rgb)/.1),rgb(var(--fm-fg-rgb)/.016))]" style={{ opacity: 1 - podium * 0.85 }} />
        <span className="ld-throne absolute inset-0 bg-[linear-gradient(90deg,rgb(var(--fm-accent-rgb)/.22),rgb(var(--fm-accent-rgb)/.03))]" style={{ opacity: podium }} />
      </span>
    </>
  );
}

/* the board's laptop columns, shared by the header and every row: rank, who, the typical post, its change on the
   window before, the best post. 90D has no window before it (see readsBefore), so its change column goes and the name
   takes the room */
const BOARD_COLS = 'grid-cols-[52px_minmax(200px,1.4fr)_136px_170px_180px_28px] xl:grid-cols-[64px_minmax(240px,1.4fr)_150px_200px_200px_30px]';
const BOARD_COLS_NO_SHIFT = 'grid-cols-[52px_minmax(200px,1.4fr)_136px_180px_28px] xl:grid-cols-[64px_minmax(240px,1.4fr)_150px_200px_30px]';

/* A row is its rank, its face, its name and its typical post, with that post's change on the window before beside it;
   everything else opens under it (RowExpansion). It moves as the ledger deals it: it travels to its new place on its
   beat, its numbers roll on the same beat, and its ground sweeps to its new strength just after. */
function BoardRowItem({ row, order, selected, wide, days, onSelect }: { row: BoardRow; order: number; selected: boolean; wide: boolean; days: number; onSelect: (id: string) => void }) {
  const beat = ledgerDelay(order);
  const compared = readsBefore(days);
  const strength = row.typical == null ? 0.04 : Math.max(0.04, Math.min(1, (101 - row.typical) / 100));
  const best = row.bestPost;
  const bestViews = best ? viewsLabel(best) : null;
  const bestLine = best?.rankingMultiple == null ? '' : `${formatMultiple(best.rankingMultiple)} its usual${bestViews ? ` · ${bestViews}` : best.mediaType !== 'unknown' ? ` · ${best.mediaType}` : ''}`;
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
        <RowGround place={row.rank} strength={strength} />

        {/* phone / tablet: rank, face, name, and the typical post with its change under it */}
        {!wide ? <div className="relative z-10 grid min-h-[76px] grid-cols-[30px_44px_minmax(0,1fr)_auto_14px] items-center gap-3 px-4 py-3 sm:grid-cols-[40px_44px_minmax(0,1fr)_auto_14px] sm:px-5">
          <RankNumeral rank={row.rank} delay={beat} size="text-[30px] sm:text-[38px]" />
          <RowFace row={row} />
          <span className="min-w-0">
            <span className="block truncate text-[15px] font-black leading-[1.15] tracking-[-0.02em] text-fg">{row.name}</span>
            <span className={cn('mt-1.5 block truncate', NOTE, DIM)}><Swap value={rowCaption(row)} /></span>
          </span>
          <span className="flex flex-col items-end text-right">
            <TypicalFigure row={row} delay={beat} className="text-[26px]" />
            <span className="mt-1.5 flex h-[15px] items-center">{compared ? <ShiftLine row={row} delay={beat} className="text-[12px]" /> : null}</span>
          </span>
          <ChevronDown className={cn('h-4 w-4 shrink-0 text-fg/48 light:text-fg/58 transition-transform duration-[calc(300ms*var(--fm-tempo))]', selected && 'rotate-180 text-fg light:text-fg')} />
        </div> : null}

        {/* desktop */}
        {wide ? <div className={cn('relative z-10 grid min-h-[84px] items-center gap-3 px-4 py-3 laptop:min-h-[74px] xl:gap-4 xl:px-5', compared ? BOARD_COLS : BOARD_COLS_NO_SHIFT)}>
          <RankNumeral rank={row.rank} delay={beat} size="text-[42px] xl:text-[48px] laptop:text-[40px]" />
          <span className="flex min-w-0 items-center gap-3.5">
            <RowFace row={row} />
            <span className="min-w-0">
              <span className="block truncate text-[16px] font-black leading-[1.15] tracking-[-0.02em] text-fg laptop:text-[15px]">{row.name}</span>
              <span className={cn('mt-1.5 block truncate', NOTE, DIM)}><Swap value={rowCaption(row)} /></span>
            </span>
          </span>
          <TypicalFigure row={row} delay={beat} className="text-[28px] laptop:text-[24px]" />
          {compared ? <span className="min-w-0">
            <ShiftLine row={row} delay={beat} className="text-[20px] laptop:text-[18px]" />
            <span className={cn('mt-1.5 block truncate', NOTE, DIM)}><Swap value={row.typicalShift == null ? `no posts the ${days}D before` : `on the ${days}D before`} /></span>
          </span> : null}
          <span className="min-w-0">
            <span className={cn('block text-[19px] font-black tabular-nums leading-none tracking-[-0.03em] laptop:text-[17px]', row.best != null && row.best <= 10 ? 'text-[var(--fm-accent-text)]' : 'text-fg')}>
              <SlotText value={row.best == null ? '—' : `Top ${percentLabel(row.best)}%`} delay={beat} />
            </span>
            <span className={cn('mt-1.5 block truncate', NOTE, DIM)}><Swap value={bestLine} /></span>
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

/* the scope's own typical post, as a line between the rows that beat it and the rows that don't. It is a place on the
   ledger like a row: a switch carries it to its new place on its beat, its number rolling */
function ParLine({ par, who, order }: { par: number | null; who: string; order: number }) {
  const beat = ledgerDelay(order);
  const whose = `${who}${who.endsWith('s') ? '’' : '’s'}`;
  return (
    <div data-ledger="par" aria-label={`${whose} typical post: top ${percentLabel(par)}%`} className="relative flex h-[30px] items-center gap-2.5 border-b border-fg/[0.07] bg-(--ld-raised) pl-[58px] pr-4 sm:pl-[72px] sm:pr-5 lg:pl-[80px] xl:pl-[100px]">
      <span className={cn('flex items-baseline gap-1 whitespace-nowrap', NOTE, DIM)}>
        <Swap value={`${whose} typical post ·`} />
        <b className="flex items-baseline font-black text-fg">Top&nbsp;<SlotText value={percentLabel(par)} delay={beat} />%</b>
      </span>
      <span aria-hidden="true" className="h-px flex-1 border-t border-dashed border-fg/[0.16]" />
    </div>
  );
}

function PostRowItem({ post, rank, order, selected, onSelect }: { post: TrackedPost; rank: number; order: number; selected: boolean; onSelect: (key: string) => void }) {
  const beat = ledgerDelay(order);
  const views = viewsLabel(post);
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
        <RowGround place={rank} strength={strength} />
        {/* one height for every place on the ladder, filled or empty (EmptyPlace) */}
        <div className="relative z-10 grid h-[84px] grid-cols-[34px_45px_minmax(0,1fr)_112px] items-center gap-2.5 px-3 sm:grid-cols-[42px_45px_minmax(0,1fr)_112px] sm:gap-3 sm:px-4">
          <RankNumeral rank={rank} delay={beat} size="text-[44px]" />
          <span className="relative block h-[56px] w-[45px] shrink-0 overflow-hidden rounded-[14px] border border-fg/12 bg-(--ld-thumb) shadow-(--ld-shadow-thumb)">
            <CrossfadeImage src={post.thumbnailUrl} />
          </span>
          <span className="min-w-0">
            <span className="block truncate text-[15px] font-black capitalize leading-none tracking-[-0.02em] text-fg">{post.mediaType} · {postDateLabel(post)}</span>
            <span className="mt-1.5 flex min-w-0 items-center gap-1.5">
              <span className={cn('truncate', NOTE, SOFT)}>{(post.latestCheckpoint || '').toUpperCase()} read{views ? ` · ${views}` : ''}</span>
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

function Feederboard({ rows, posts, feeder, deal, days, par, parWho, pointers, onPointer, selectedId, onSelect, selectedPostKey, onSelectPost }: {
  rows: BoardRow[];
  posts: TrackedPost[];
  par: number | null;
  parWho: string;
  pointers: Pointer[];
  onPointer: (go: PointerGo) => void;
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
  // the par line sits after the last row whose typical post beats the scope's own
  const parAt = useMemo(() => {
    if (feeder || par == null || !shown.length) return -1;
    const at = shown.findIndex((row) => row.typical == null || row.typical > par);
    return at < 0 ? shown.length : at;
  }, [feeder, par, shown]);
  const ids = useMemo(() => {
    if (feeder) return [...ladder.map((post) => `post:${post.postKey}`), ...Array.from({ length: LADDER_PLACES - ladder.length }, (_, index) => `empty:${ladder.length + index}`)];
    const list = shown.map((row) => row.id);
    if (parAt >= 0) list.splice(parAt, 0, 'par');
    return list;
  }, [feeder, ladder, parAt, shown]);
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

      <PointersBand pointers={pointers} onGo={onPointer} />

      {wide ? (
        feeder ? (
          <div key="posts" ref={holdArrival} className={cn('lead-swap-in grid grid-cols-[42px_45px_minmax(0,1fr)_112px] items-end gap-3 border-b border-fg/[0.07] px-4 py-3', MARK, DIM)}>
            <span>Rank</span><span>Post</span><span /><span className="pr-6 text-right">Placed</span>
          </div>
        ) : (
          <div key="rows" ref={holdArrival} className={cn('lead-swap-in grid items-end gap-3 border-b border-fg/[0.07] px-4 py-3 xl:gap-4 xl:px-5', readsBefore(days) ? BOARD_COLS : BOARD_COLS_NO_SHIFT, MARK, 'tracking-[0.1em]', DIM)}>
            <span>Rank</span><span>{shown[0]?.id.startsWith('feed:') ? 'Feed' : 'Feeder'}</span><span>Typical post</span>{readsBefore(days) ? <span>On the {days}D before</span> : null}<span>Best post</span><span />
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
          : ids.map((id, order) => {
              if (id === 'par') return <ParLine key="par" par={par} who={parWho} order={order} />;
              const row = shown.find((one) => one.id === id) as BoardRow;
              return <BoardRowItem key={row.id} row={row} order={order} selected={row.id === selectedId} wide={wide} days={days} onSelect={onSelect} />;
            })}
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

/* ── Top posts: four rankings of the scope's posts, each led by its own number ──
   The best-placed posts (Top %), the most liked and the most talked about against each account's own usual, and the
   ones that climbed furthest between their first read and their latest. The first leads, large; six follow smaller
   beside it, their feet on one line (as in a row's receipts). A switch of ranking, scope or window re-deals the cards
   on the ledger's beat, a card at a time; the pill carries itself to the picked ranking. */

const TOP_SORTS = [
  { id: 'rank', label: 'Rank', sub: 'The best-placed posts, by Top %' },
  { id: 'likes', label: 'Likes', sub: 'Most likes against each account’s usual' },
  { id: 'comments', label: 'Comments', sub: 'Most comments against each account’s usual' },
  { id: 'climb', label: 'Climb', sub: 'Climbed furthest since their first read' },
  { id: 'views', label: 'Views', sub: 'Most views, where they were counted' },
] as const;
type TopSort = (typeof TOP_SORTS)[number]['id'];
const NO_TOP: ScopedPost[] = [];

function topPostsFor(feeds: RuntimeFeed[], activeFeedId: string, feederHandle: string, days: number, sort: TopSort): ScopedPost[] {
  return remember(feeds, null, `top|${sort}|${activeFeedId}|${feederHandle}|${days}`, () => {
    const shown = postsFor(feeds, activeFeedId, feederHandle, days).filter((post) => post.thumbnailUrl && post.latestPercentile != null);
    if (sort === 'likes') return shown.filter((post) => post.likesMultiple != null).sort((a, b) => (b.likesMultiple ?? 0) - (a.likesMultiple ?? 0)).slice(0, 7);
    if (sort === 'comments') return shown.filter((post) => post.commentsMultiple != null).sort((a, b) => (b.commentsMultiple ?? 0) - (a.commentsMultiple ?? 0)).slice(0, 7);
    if (sort === 'climb') return shown.filter((post) => (climbOf(post) ?? 0) >= 1).sort((a, b) => (climbOf(b) ?? 0) - (climbOf(a) ?? 0)).slice(0, 7);
    if (sort === 'views') return shown.filter((post) => viewsOf(post) != null).sort((a, b) => (viewsOf(b) ?? 0) - (viewsOf(a) ?? 0)).slice(0, 7);
    return [...shown].sort(bestFirst).slice(0, 7);
  });
}

/* the number a card leads with in its ranking, and what it counts */
function topLead(post: TrackedPost, sort: TopSort, hero: boolean) {
  if (sort === 'views') return { fig: compactNumber(viewsOf(post) ?? 0), unit: hero ? `views · ${viewsNote(post) ?? ''} · ${formatMultiple(post.likesMultiple)} its usual likes` : 'views', win: false };
  if (sort === 'likes') return { fig: formatMultiple(post.likesMultiple), unit: hero ? `its usual likes · ${compactNumber(numeric(post.likes))} likes` : 'its usual likes', win: (post.likesMultiple ?? 0) >= 1.1 };
  if (sort === 'comments') return { fig: formatMultiple(post.commentsMultiple), unit: hero ? `its usual comments · ${compactNumber(numeric(post.comments))} comments` : 'its usual comments', win: (post.commentsMultiple ?? 0) >= 1.1 };
  if (sort === 'climb') {
    const climb = Math.round(climbOf(post) ?? 0);
    return {
      fig: `+${climb}`,
      unit: hero ? `places · Top ${percentLabel(post.firstPercentile ?? null)}% at ${post.firstCheckpoint || 'D1'} → Top ${percentLabel(post.latestPercentile)}% at ${(post.latestCheckpoint || '').toUpperCase()}` : `places since ${post.firstCheckpoint || 'D1'}`,
      win: true,
    };
  }
  return { fig: `Top ${percentLabel(post.latestPercentile)}%`, unit: `${formatMultiple(post.rankingMultiple)} its usual`, win: (post.latestPercentile ?? 101) <= 10 };
}

function TopPostCard({ id, post, place, sort, showFeed, face }: { id: string; post: ScopedPost; place: number; sort: TopSort; showFeed: boolean; face: string | null }) {
  const hero = place === 1;
  const lead = topLead(post, sort, hero);
  const tone = post.latestPercentile != null ? shade(post.latestPercentile) : null;
  const views = sort === 'views' ? null : viewsLabel(post);
  const handle = normalizeHandle(post.handle) || post.feedName.toLowerCase();
  return (
    <a
      data-ledger={id}
      data-top-hero={hero ? '' : undefined}
      href={post.postUrl || '#'}
      target={post.postUrl ? '_blank' : undefined}
      rel={post.postUrl ? 'noreferrer' : undefined}
      onClick={(event) => { if (!post.postUrl) event.preventDefault(); }}
      data-testid={`lead-v3-top-${place}`}
      aria-label={`${rankLabel(place)}: @${handle}, ${lead.fig} ${lead.unit}${views ? `, ${views}` : ''}${post.postUrl ? '. Opens on Instagram' : ''}`}
      className={cn(
        'group min-w-0 rounded-[18px] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]',
        hero ? 'col-span-3 grid grid-cols-[42%_minmax(0,1fr)] items-end gap-4 sm:gap-6 lg:col-span-1 lg:block' : 'block',
      )}
    >
      <span className={cn(
        'relative block aspect-[4/5] overflow-hidden border border-white/12 bg-(--fm-media)',
        hero ? 'rounded-[20px] shadow-(--ld-shadow-proof-lg)' : 'rounded-[14px] shadow-(--ld-shadow-proof)',
      )}>
        <CrossfadeImage src={post.thumbnailUrl} className="transition-transform duration-[calc(700ms*var(--fm-tempo))] group-hover:scale-[1.035]" />
        <span aria-hidden="true" className="absolute inset-x-0 top-0 h-[44%] bg-[linear-gradient(180deg,rgba(0,0,0,.44),transparent)]" />
        {!hero && sort !== 'rank' ? <span aria-hidden="true" className="absolute inset-x-0 bottom-0 h-[36%] bg-[linear-gradient(0deg,rgba(0,0,0,.4),transparent)]" /> : null}
        <span aria-hidden="true" className={cn('absolute left-2.5 top-2 font-black tabular-nums leading-none tracking-[-0.06em] text-white [text-shadow:0_1px_8px_rgba(0,0,0,.35)]', hero ? 'text-[26px]' : 'text-[19px]')}>{rankLabel(place)}</span>
        {sort !== 'rank' ? (
          <span
            className={cn('absolute rounded-[7px] px-1.5 py-0.5 font-black tabular-nums shadow-[0_2px_8px_rgba(0,0,0,.2)]', hero ? 'right-2 top-2 text-[11px]' : 'bottom-2 left-2 text-[10px]')}
            style={{ background: tone ? tone.c : 'rgba(0,0,0,.55)', color: tone ? tone.tx : '#ffffff' }}
          >
            Top {percentLabel(post.latestPercentile)}%
          </span>
        ) : null}
      </span>
      <span className={cn('block min-w-0', hero ? 'pb-1 lg:mt-3 lg:pb-0' : 'mt-2.5 px-0.5')}>
        <span className={cn(
          'fm-depth-title block whitespace-nowrap font-black tabular-nums leading-none tracking-[-0.045em]',
          hero ? 'text-[30px] sm:text-[38px] lg:text-[30px] xl:text-[34px]' : 'text-[18px] sm:text-[21px] lg:text-[19px]',
          hero && lead.win ? 'text-[var(--fm-accent-text)]' : 'text-fg',
        )}>
          {lead.fig}
        </span>
        <span className={cn('mt-1.5 block truncate', NOTE, SOFT)}>{lead.unit}</span>
        {/* a small card keeps the views line whether or not it has views, so the covers in a row stay level */}
        {views || !hero ? <span className={cn('mt-0.5 block truncate', NOTE, SOFT)}>{views ?? '\u00a0'}</span> : null}
        <span className={cn('flex min-w-0 items-center gap-1.5', hero ? 'mt-2.5' : 'mt-1', NOTE, DIM)}>
          {hero ? <FaceDot handle={handle} pic={face} size="h-[18px] w-[18px]" /> : null}
          <b className="min-w-0 truncate font-black text-fg">@{handle}</b>
        </span>
        {hero ? <span className={cn('mt-0.5 block truncate', NOTE, DIM)}>{mediaWord(post)} · {postDateLabel(post)}{showFeed ? ` · ${post.feedName}` : ''}</span> : null}
      </span>
    </a>
  );
}

const TopPosts = memo(function TopPosts(props: {
  feeds: RuntimeFeed[];
  activeFeedId: string;
  activeFeeder: string;
  days: number;
  sort: TopSort;
  onSort: (sort: TopSort) => void;
}) {
  // on screen it takes a switch with the rest of the page; off screen, once the switch has played (see useHeldNear)
  const live = useMemo(
    () => ({ feeds: props.feeds, activeFeedId: props.activeFeedId, activeFeeder: props.activeFeeder, days: props.days, sort: props.sort }),
    [props.feeds, props.activeFeedId, props.activeFeeder, props.days, props.sort],
  );
  const [nearRef, { feeds, activeFeedId, activeFeeder, days, sort }] = useHeldNear(live);
  const posts = feeds.length ? topPostsFor(feeds, activeFeedId, activeFeeder, days, sort) : NO_TOP;
  const faces = facesOf(feeds);
  // a card is the post read in its ranking: the same post read the same way in the next deal stays (and travels),
  // anything else hands its place over
  const ids = useMemo(() => posts.map((post) => `${sort}:${post.postKey}`), [posts, sort]);
  const sortIndex = TOP_SORTS.findIndex((option) => option.id === sort);
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
  }, [sortIndex]);

  // the band keeps its place on the page when it grows or shrinks: what is under it travels
  const bandRef = useRef<HTMLElement | null>(null);
  useEffect(() => (bandRef.current ? keepBelow(bandRef.current) : undefined), []);
  const ref = useCallback((el: HTMLElement | null) => {
    bandRef.current = el;
    nearRef(el);
  }, [nearRef]);

  return (
    <section ref={ref} id="lead-top-posts" aria-labelledby="lead-v3-top-title" className="lead-band relative isolate overflow-hidden border-t border-fg/[0.08] bg-(--ld-panel) px-4 pb-5 pt-4 sm:px-5 sm:pb-6 lg:px-7 lg:pb-7">
      <div className="flex flex-col gap-3 sm:flex-row sm:items-end sm:justify-between">
        <div className="min-w-0">
          <h2 id="lead-v3-top-title" className="text-[10px] font-black uppercase tracking-[0.2em] text-fg/60">Top posts</h2>
          <p className={cn('mt-1 overflow-hidden whitespace-nowrap', NOTE, SOFT)}><Swap value={`${TOP_SORTS[sortIndex].sub} · ${days}D`} className="truncate align-top" /></p>
        </div>
        <div className="relative grid h-11 min-w-[300px] grid-cols-5 rounded-[14px] border border-fg/[0.07] bg-(--fm-well) shadow-(--fm-well-shade) sm:min-w-[360px]">
          {/* one pill, carried to the picked ranking (a transform on the compositor) */}
          <span
            ref={pillRef}
            aria-hidden="true"
            className="ld-pill pointer-events-none absolute bottom-[3px] left-[3px] top-[3px] rounded-[8px] bg-fg shadow-[inset_0_1px_0_rgb(255_255_255_/_0.16)]"
            style={{ width: 'calc(20% - 6px)', transform: `translateX(calc(${sortIndex} * (100% + 6px)))` }}
          />
          {TOP_SORTS.map((option) => (
            <button
              key={option.id}
              type="button"
              onClick={() => props.onSort(option.id)}
              aria-pressed={sort === option.id}
              data-testid={`lead-v3-top-sort-${option.id}`}
              className={cn(
                'relative grid h-full place-items-center rounded-[10px] text-[10px] font-black uppercase tracking-[0.08em] transition-colors duration-[calc(300ms*var(--fm-tempo))] sm:text-[11px]',
                sort === option.id ? 'text-(--fm-page)' : 'text-fg/60 hover:text-fg/80',
              )}
            >
              <span className="relative z-10">{option.label}</span>
            </button>
          ))}
        </div>
      </div>

      <div className="relative mt-5 min-h-[240px]">
        {/* the cards turn over a beat a card, first place first (components/lead/ledgerMotion) */}
        <Ledger
          ids={ids}
          deal={`${sort}|${activeFeedId}|${activeFeeder}|${days}`}
          className="grid grid-cols-3 gap-x-2.5 gap-y-5 sm:gap-x-4 lg:grid-cols-[minmax(0,1.4fr)_repeat(6,minmax(0,1fr))] lg:items-end lg:gap-x-4"
        >
          {posts.map((post, index) => (
            <TopPostCard key={ids[index]} id={ids[index]} post={post} place={index + 1} sort={sort} showFeed={activeFeedId === 'all'} face={faces.get(normalizeHandle(post.handle)) ?? null} />
          ))}
        </Ledger>
        {!posts.length ? <div ref={holdArrival} className={cn('ld-late-in absolute inset-0 flex items-center justify-center text-center', MARK, DIM)}>{sort === 'climb' ? 'Nothing climbed in this window yet' : sort === 'views' ? 'No views counted for these posts yet' : 'No ranked posts with a picture in this window'}</div> : null}
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

/* An open post on a feeder's ladder, drawn as an open row is: its picture leads on the left; beside it the place it
   holds (big), how far over its account's usual it went, the road it took between its reads (a chip a read) and when
   it went out; under them, six facts on one grid (likes, comments and engagement against its usual, its views, the
   number it was ranked on, its last read). It unfolds on the open row's beat (OPEN_GRID) as the ladder makes room. */
const RANKED_ON: Record<string, string> = { likes: 'likes', comments: 'comments', engagement_rate: 'eng. rate', views: 'views' };

function PostRowExpansion({ post, onCollapse, ref }: { post: TrackedPost; onCollapse: () => void; ref?: Ref<HTMLDivElement> }) {
  const reduce = Boolean(useReducedMotion());
  const tone = post.latestPercentile != null ? shade(post.latestPercentile) : null;
  const views = viewsLabel(post);
  const time = postTime(post);
  const when = time ? new Intl.DateTimeFormat('en-GB', { weekday: 'short', day: 'numeric', month: 'short', hour: 'numeric', minute: '2-digit', hour12: true, timeZone: 'Asia/Kolkata' }).format(time) : 'undated';
  const climb = climbOf(post);
  const travelled = post.firstCheckpoint && post.firstPercentile != null && post.firstCheckpoint.toUpperCase() !== (post.latestCheckpoint || '').toUpperCase();
  const win = (post.latestPercentile ?? 101) <= 10;
  const facts = [
    { label: 'Likes', value: compactNumber(numeric(post.likes)), note: post.likesMultiple == null ? 'no usual yet' : `${formatMultiple(post.likesMultiple)} its usual` },
    { label: 'Comments', value: compactNumber(numeric(post.comments)), note: post.commentsMultiple == null ? 'no usual yet' : `${formatMultiple(post.commentsMultiple)} its usual` },
    { label: 'Eng. rate', value: formatEngagementRate(post.engagementRate), note: post.engagementRateMultiple == null ? 'no usual yet' : `${formatMultiple(post.engagementRateMultiple)} its usual` },
    { label: 'Views', value: views ? views.replace(' views', '') : '—', note: views ? viewsNote(post) ?? '' : 'not counted for this post' },
    { label: 'Ranked on', value: RANKED_ON[post.rankingMetric ?? ''] ?? 'likes', note: 'its strongest number' },
    { label: 'Last read', value: (post.latestCheckpoint || '—').toUpperCase(), note: post.latestBusinessDayIst ? new Intl.DateTimeFormat('en-GB', { day: 'numeric', month: 'short', timeZone: 'Asia/Kolkata' }).format(Date.parse(post.latestBusinessDayIst)) : '' },
  ];

  return (
    <motion.div
      ref={ref}
      data-testid="lead-v3-post-expansion"
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
        className="relative isolate cursor-pointer border-t border-fg/[0.08] bg-(--ld-raised) px-4 pb-5 pt-4 sm:px-6 sm:pb-6 lg:pl-[100px] lg:pr-6"
      >
        <motion.div variants={OPEN_GRID} className="grid gap-4 lg:grid-cols-[auto_minmax(0,1fr)] lg:items-stretch lg:gap-8">
          <div className="grid grid-cols-[42%_minmax(0,1fr)] items-end gap-4 sm:grid-cols-[200px_minmax(0,1fr)] lg:grid-cols-[200px_240px]">
            <motion.a
              variants={OPEN_ITEM}
              href={post.postUrl || '#'}
              target={post.postUrl ? '_blank' : undefined}
              rel={post.postUrl ? 'noreferrer' : undefined}
              onClick={(event) => {
                event.stopPropagation();
                if (!post.postUrl) event.preventDefault();
              }}
              aria-label={post.postUrl ? 'Open the post on Instagram' : 'The post'}
              className="group relative block aspect-[4/5] overflow-hidden rounded-[20px] border border-white/12 bg-(--fm-media) shadow-(--ld-shadow-proof-lg) focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
            >
              <CrossfadeImage src={post.thumbnailUrl} className="transition-transform duration-[calc(700ms*var(--fm-tempo))] group-hover:scale-[1.035]" />
              <span aria-hidden="true" className="absolute inset-x-0 top-0 h-[40%] bg-[linear-gradient(180deg,rgba(0,0,0,.4),transparent)]" />
              <span className="absolute right-2 top-2 rounded-[7px] px-1.5 py-0.5 text-[11px] font-black tabular-nums shadow-[0_2px_8px_rgba(0,0,0,.2)]" style={{ background: tone ? tone.c : 'rgba(0,0,0,.55)', color: tone ? tone.tx : '#ffffff' }}>
                {post.latestPercentile == null ? 'new' : `Top ${percentLabel(post.latestPercentile)}%`}
              </span>
            </motion.a>

            <motion.div variants={OPEN_ITEM} className="flex min-w-0 flex-col pb-1">
              <span className={cn(MARK, DIM)}>Placed</span>
              <span className={cn('fm-depth-title mt-1.5 block whitespace-nowrap text-[38px] font-black tabular-nums leading-none tracking-[-0.05em] sm:text-[46px]', win ? 'text-[var(--fm-accent-text)]' : 'text-fg')}>
                {post.latestPercentile == null ? '—' : `Top ${percentLabel(post.latestPercentile)}%`}
              </span>
              <span className={cn('mt-1.5 block', NOTE, SOFT)}>{post.rankingMultiple == null ? 'no usual yet' : `${formatMultiple(post.rankingMultiple)} its account’s usual`}</span>
              {/* the road between its reads: a chip a read, the arrow in rose when it climbed */}
              {travelled ? (
                <span className="mt-3 flex flex-wrap items-center gap-1.5">
                  <span className={cn('rounded-[7px] border border-fg/[0.1] px-1.5 py-0.5 tabular-nums', NOTE, SOFT)}>{post.firstCheckpoint?.toUpperCase()} Top {percentLabel(post.firstPercentile ?? null)}%</span>
                  <span className={cn('text-[12px] font-black', (climb ?? 0) > 0 ? 'text-[var(--fm-accent)]' : DIM)}>{(climb ?? 0) > 0 ? '↗' : (climb ?? 0) < 0 ? '↘' : '→'}</span>
                  <span className={cn('rounded-[7px] px-1.5 py-0.5 tabular-nums', NOTE, 'font-black text-fg', 'bg-fg/[0.06]')}>{(post.latestCheckpoint || '').toUpperCase()} Top {percentLabel(post.latestPercentile)}%</span>
                </span>
              ) : (
                <span className={cn('mt-3 block', NOTE, DIM)}>{(post.latestCheckpoint || '').toUpperCase()} read so far</span>
              )}
              <span className={cn('mt-3 block truncate', NOTE, DIM)}>{post.mediaType === 'unknown' ? 'Post' : `${post.mediaType[0].toUpperCase()}${post.mediaType.slice(1)}`} · {when}</span>
              {post.postUrl ? (
                <a
                  href={post.postUrl}
                  target="_blank"
                  rel="noreferrer"
                  onClick={(event) => event.stopPropagation()}
                  className={cn('mt-3 inline-flex w-fit items-center gap-1.5 rounded-full border border-fg/[0.12] bg-(--ld-panel) px-3 py-1.5 text-fg transition-colors hover:border-fg/[0.24]', MARK, 'tracking-[0.1em]')}
                >
                  Open on Instagram <span aria-hidden="true">↗</span>
                </a>
              ) : null}
            </motion.div>
          </div>

          <motion.dl variants={OPEN_ITEM} className="grid min-w-0 grid-cols-3 gap-px self-stretch overflow-hidden rounded-[18px] border border-fg/[0.07] bg-fg/[0.07] lg:self-end">
            {facts.map((fact) => (
              <div key={fact.label} className="min-w-0 bg-(--ld-panel) px-3 py-3 sm:px-4 lg:px-5 lg:py-4">
                <dt className={cn('truncate', MARK, DIM)}>{fact.label}</dt>
                <dd className="fm-depth-title mt-2 truncate text-[18px] font-black tabular-nums leading-none tracking-[-0.03em] text-fg sm:text-[22px] lg:text-[24px]">{fact.value}</dd>
                <div className={cn('mt-1 line-clamp-2 lg:truncate', NOTE, DIM)}>{fact.note}</div>
              </div>
            ))}
          </motion.dl>
        </motion.div>
      </motion.div>
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

const BoardSection = memo(function BoardSection({ feeds, feedId, feeder, days, onPointer, selectedId, onSelect, selectedPostKey, onSelectPost }: ScopeProps & {
  onPointer: (go: PointerGo) => void;
  selectedId: string;
  onSelect: (id: string) => void;
  selectedPostKey: string;
  onSelectPost: (key: string) => void;
}) {
  const rows = rowsFor(feeds, feedId, days);
  const feederPosts = feeder ? postsFor(feeds, feedId, feeder, days) : NO_POSTS;
  const par = parFor(feeds, feedId, days);
  const pointers = pointersFor(feeds, feedId, feeder, days);
  const parWho = feedId === 'all' ? 'All feeds' : titleCase(feeds.find((feed) => feed.id === feedId)?.title || 'This feed');
  // a row that left the board can't stay open
  const open = rows.some((row) => row.id === selectedId) ? selectedId : '';
  // the page under the board doesn't jump when the board grows or shrinks (a new scope, a row opening): it travels
  const boardRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => (boardRef.current ? keepBelow(boardRef.current) : undefined), []);
  return (
    <div ref={boardRef} className="min-w-0 max-w-full">
      <Feederboard
        rows={rows}
        posts={feederPosts}
        feeder={feeder}
        deal={`${feedId}|${feeder}|${days}`}
        days={days}
        par={par}
        parWho={parWho}
        pointers={pointers}
        onPointer={onPointer}
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
  // ← and → (or < and >) step the time range (7D · 30D · 90D), as they step Feed's zoom: ← back, a longer window,
  // → a shorter one, stopping at the ends. Only while Lead is on screen (a hidden tab's effects are off), never while
  // typing or under a dialog
  const daysRef = useRef(days);
  useLayoutEffect(() => {
    daysRef.current = days;
  }, [days]);
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      const longer = event.key === 'ArrowLeft' || event.key === '<';
      if (!longer && event.key !== 'ArrowRight' && event.key !== '>') return;
      if (event.defaultPrevented || event.metaKey || event.ctrlKey || event.altKey || event.repeat) return;
      if (typingIn(event.target) || aModalIsOpen()) return;
      const next = TIMEFRAMES[TIMEFRAMES.indexOf(daysRef.current) + (longer ? 1 : -1)];
      if (!next) return;
      event.preventDefault();
      startTimeframe(() => setDays(next));
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);
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

  /* arriving from the Feed's Hall of Feed row (ProfileHeader's Lead door): Lead opens at its Hall of Feed, on the pick
     the tabs share. The ask is taken when the tab comes on screen (its effects run again then) and kept until the Hall
     is drawn; the scroll waits two frames, so the tab's own arrival has placed the page first */
  const hallAsked = useRef(false);
  useEffect(() => {
    try {
      if (sessionStorage.getItem(LEAD_AT_KEY) !== 'hall') return;
      sessionStorage.removeItem(LEAD_AT_KEY);
      hallAsked.current = true;
    } catch {}
  }, []);
  useEffect(() => {
    if (!hallAsked.current || !feeds.length) return undefined;
    let frame = requestAnimationFrame(() => {
      frame = requestAnimationFrame(() => {
        const hall = document.getElementById('lead-v3-hall-title')?.closest<HTMLElement>('section');
        if (!hall) return;
        hallAsked.current = false;
        // a phone scrolls the window, clear of what is over the page's top (Lead's own scroll padding is its desktop
        // panel's)
        hall.style.scrollMarginTop = window.matchMedia('(min-width: 1024px)').matches ? '' : 'calc(16px + env(safe-area-inset-top) + var(--fm-mobile-header-chrome-compressed-height, 68px))';
        hall.scrollIntoView({ block: 'start' });
      });
    });
    return () => cancelAnimationFrame(frame);
  }, [feeds]);

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

  /* a pointer is a door: the typical post goes to the par line, gains to Followers, the rest to the ranking it heads
     in Top posts (picked first, then brought into view once its cards are dealt) */
  const [topSort, setTopSort] = useState<TopSort>('rank');
  const openPointer = useCallback((go: PointerGo) => {
    if (go === 'par') {
      jumpTo(document.querySelector<HTMLElement>('[data-ledger="par"]'));
      return;
    }
    if (go === 'gains') {
      jumpTo(document.querySelector<HTMLElement>('[aria-labelledby="lead-growth-title"]'));
      return;
    }
    setTopSort(go);
    requestAnimationFrame(() => {
      const band = document.getElementById('lead-top-posts');
      jumpTo(band, band?.querySelector<HTMLElement>('[data-top-hero] > span') ?? null);
    });
  }, []);

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
        '[overflow-anchor:none] fm-lead fm-dashboard-mesh w-full min-w-0 max-w-[100vw] scroll-pb-[calc(126px+env(safe-area-inset-bottom))] scroll-pt-[calc(16px+env(safe-area-inset-top)+var(--fm-mobile-header-chrome-compressed-height,68px))] lg:scroll-pt-6 text-fg',
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
      <main className="relative z-10 mx-auto w-full min-w-0 max-w-[1500px] px-3 pb-[calc(148px+env(safe-area-inset-bottom))] pt-[calc(20px+env(safe-area-inset-top)+var(--fm-mobile-header-chrome-height,152px))] sm:px-5 lg:px-7 lg:pt-(--fm-desk-top)">
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

            {/* the board: edge to edge on a phone (its bands keep a 16px gutter for text), a framed card from sm up */}
            <div className="relative -mx-3 mt-4 min-w-0 overflow-hidden border-y border-fg/[0.09] bg-(--ld-ground) sm:mx-0 sm:max-w-full sm:rounded-[28px] sm:border sm:shadow-(--ld-shadow-board)">
              <BoardSection
                feeds={feeds}
                feedId={activeFeedId}
                feeder={activeFeeder}
                days={days}
                onPointer={openPointer}
                selectedId={selectedId}
                onSelect={setSelectedId}
                selectedPostKey={selectedPostKey}
                onSelectPost={setSelectedPostKey}
              />
              <TopPosts feeds={feeds} activeFeedId={activeFeedId} activeFeeder={activeFeeder} days={days} sort={topSort} onSort={setTopSort} />
              <LeadFollowers feeds={feeds} activity={activity} feedId={activeFeedId} feeder={activeFeeder} days={days} />
              <section aria-label="Feeding habits" className="bg-(--ld-panel)">
                <div className="border-t border-fg/[0.09] px-4 pb-0 pt-5 sm:px-6 lg:px-7">
                  <h2 className="text-[10px] font-black uppercase tracking-[0.2em] text-fg/60">Feeding habits</h2>
                  <p className={cn('mt-1', NOTE, SOFT)}>How often, when and what {activeFeeder ? `@${activeFeeder} posts` : activeFeedId === 'all' ? 'your feeds post' : 'this feed posts'}{readsBefore(days) ? ` · vs the ${days}D before` : ''}</p>
                </div>
                <LeadCadence feeds={feeds} activity={activity} feedId={activeFeedId} feeder={activeFeeder} days={days} />
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
