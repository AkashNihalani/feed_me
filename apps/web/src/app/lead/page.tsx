'use client';

// Lead — the merged feederboard.
// Spine: leaders strip (three different questions) → the Board (media-first
// rows ranked on TYPICAL post percentile, throne row for rank 1) → follower
// windows → Post Mortem, which opens the full Fire wall.
// Every number traces to feeder-posts percentiles/multiples or the ascent
// series. No generated copy — verdict words are threshold math.

import Link from 'next/link';
import { CSSProperties, forwardRef, useEffect, useId, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import {
  ChevronDown,
  ChevronRight,
  Flame,
  Users,
} from 'lucide-react';
import { AnimatePresence, LayoutGroup, motion, useReducedMotion } from 'framer-motion';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { AppHeader, usePageReady } from '@/components/shell/AppShell';
import SlotText from '@/components/SlotText';
import { SLOT_CONTAINER, SLOT_ITEM } from '@/lib/motion';
import { useCompressedOnScroll } from '@/lib/useCompressedOnScroll';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import { cn } from '@/lib/utils';

const TIMEFRAMES = [7, 30, 60, 90] as const;
type Timeframe = (typeof TIMEFRAMES)[number];
const SOFT_EASE = [0.16, 0.9, 0.2, 1] as const;
const LADDER_SPRING = { type: 'spring', stiffness: 400, damping: 36, mass: 0.9 } as const;
const LEAD_RAIL_SPRING = { type: 'spring', stiffness: 300, damping: 32, mass: 0.9 } as const;
const LEAD_WEDGE_SPRING = { type: 'spring', stiffness: 420, damping: 38, mass: 0.8 } as const;
const TIMEFRAME_SLOT_SPRING = { type: 'spring', stiffness: 540, damping: 42, mass: 0.7 } as const;
const LEAD_FEEDER_SLOT = 94;
const REDUCED_RAIL_TRANSITION = { duration: 0.14, ease: SOFT_EASE } as const;
const HEADER_CIRCLE_POP = { type: 'spring', stiffness: 420, damping: 34, mass: 0.82 } as const;
const HOF_PILL_SPRING = { type: 'spring', stiffness: 360, damping: 34, mass: 0.8 } as const;
const STORY_RING_RADIUS = 45;
const STORY_RING_CIRCUMFERENCE = 2 * Math.PI * STORY_RING_RADIUS;
const CARD_STACK_EASE = [0.18, 0.88, 0.28, 1] as const;
const CARD_MASK_EASE = [0.32, 0.72, 0, 1] as const;
const CARD_MASK_DURATION = 0.92;
const CARD_MASK_CLEANUP_MS = 1040;

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

type PostMortemItem = {
  id: string;
  postKey: string;
  postUrl: string | null;
  thumbnailUrl: string | null;
  feedId: string;
  feedName: string;
  handle: string;
  trigger: string;
  tag: string;
  statement: string;
  proof: string | null;
  supportingPostKeys: string[];
  priority: number;
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

type GrowthWindow = {
  id: string;
  index: number;
  range: string;
  delta: number | null;
  postCount: number;
  posts: Array<TrackedPost & { feedName: string }>;
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

function normalizeHandle(value: string | null | undefined) {
  return String(value || '').trim().replace(/^@+/, '').toLowerCase();
}

function titleCase(value: string) {
  return value
    .toLowerCase()
    .replace(/(^|\s|[-_/&])\w/g, (character) => character.toUpperCase());
}

function feedInitials(value: string) {
  const words = value.replace(/[^a-zA-Z0-9 ]/g, ' ').trim().split(/\s+/).filter(Boolean);
  if (words.length > 1) return words.slice(0, 3).map((word) => word[0]).join('').toUpperCase();
  return (words[0] || 'ALL').slice(0, 3).toUpperCase();
}

function postTime(post: TrackedPost) {
  const parsed = Date.parse(post.postedAt || '');
  return Number.isFinite(parsed) ? parsed : 0;
}

function postsInWindow(posts: TrackedPost[], days: number) {
  const cutoff = Date.now() - days * 24 * 60 * 60 * 1000;
  return posts.filter((post) => {
    const time = postTime(post);
    return time === 0 || time >= cutoff;
  });
}

/* The window immediately before the selected one, same length. Every level on
   the page is read against this — that's where the direction comes from. */
function postsInPriorWindow(posts: TrackedPost[], days: number) {
  const spanMs = days * 24 * 60 * 60 * 1000;
  const end = Date.now() - spanMs;
  const start = end - spanMs;
  return posts.filter((post) => {
    const time = postTime(post);
    return time > 0 && time >= start && time < end;
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

function ascentDelta(points: AscentPoint[]) {
  const sorted = [...points]
    .filter((point) => Number.isFinite(Date.parse(point.snapshot_date_ist)) && Number.isFinite(Number(point.follower_count)))
    .sort((a, b) => Date.parse(a.snapshot_date_ist) - Date.parse(b.snapshot_date_ist));
  if (sorted.length < 2) return null;
  return numeric(sorted[sorted.length - 1].follower_count) - numeric(sorted[0].follower_count);
}

function valueAt(points: AscentPoint[], target: number) {
  const sorted = [...points]
    .map((point) => ({ time: Date.parse(point.snapshot_date_ist), value: numeric(point.follower_count) }))
    .filter((point) => Number.isFinite(point.time))
    .sort((a, b) => a.time - b.time);
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

function growthWindows(feeds: RuntimeFeed[], activeFeedId: string, days: number, feederHandle = ''): GrowthWindow[] {
  const scopedFeeds = activeFeedId === 'all' ? feeds : feeds.filter((feed) => feed.id === activeFeedId);
  const inScope = (post: TrackedPost) => !feederHandle || normalizeHandle(post.handle) === feederHandle;
  const count = days <= 7 ? 1 : 4;
  const span = days / count;
  const now = Date.now();
  const formatter = new Intl.DateTimeFormat('en-US', { day: '2-digit', month: 'short' });

  return Array.from({ length: count }, (_, index) => {
    const start = now - (days - index * span) * 24 * 60 * 60 * 1000;
    const end = now - (days - (index + 1) * span) * 24 * 60 * 60 * 1000;
    let delta = 0;
    let hasDelta = false;
    for (const feed of scopedFeeds) {
      const first = valueAt(feed.ascent, start);
      const last = valueAt(feed.ascent, end);
      if (first == null || last == null) continue;
      delta += last - first;
      hasDelta = true;
    }

    const posts = scopedFeeds
      .flatMap((feed) => feed.posts
        .filter((post) => {
          const time = postTime(post);
          return inScope(post) && time >= start && time < end;
        })
        .map((post) => ({ ...post, feedName: titleCase(feed.title) })))
      .sort(bestFirst);

    return {
      id: `${start}:${end}`,
      index: index + 1,
      range: `${formatter.format(new Date(start))} – ${formatter.format(new Date(end))}`.toUpperCase(),
      delta: hasDelta ? delta : null,
      postCount: posts.length,
      posts: posts.filter((post) => post.thumbnailUrl).slice(0, 2),
    };
  });
}

/* ── Shared motion primitives ────────────────────────────────────────────── */

/* Media never swaps in a single frame: the incoming image crossfades over the
   outgoing one inside its fixed frame. */
function CrossfadeImage({ src, className, eager = false }: { src: string | null; className?: string; eager?: boolean }) {
  const reduce = Boolean(useReducedMotion());
  return (
    <span aria-hidden="true" className={cn('pointer-events-none absolute inset-0 block overflow-hidden', className)}>
      <AnimatePresence initial={false}>
        {src ? (
          <motion.img
            key={src}
            src={src}
            alt=""
            initial={reduce ? { opacity: 0 } : { opacity: 0, scale: 1.06 }}
            animate={{ opacity: 1, scale: 1 }}
            exit={{ opacity: 0, transition: { duration: 0.5, ease: SOFT_EASE } }}
            transition={reduce
              ? { duration: 0.12 }
              : { opacity: { duration: 0.68, ease: SOFT_EASE }, scale: { duration: 1.05, ease: SOFT_EASE } }}
            className="absolute inset-0 h-full w-full object-cover"
            loading={eager ? 'eager' : 'lazy'}
            decoding="async"
          />
        ) : null}
      </AnimatePresence>
    </span>
  );
}

/* Short text lines swap with the header scope-label grammar: rise in, lift
   out, faint blur. For values, SlotText does the per-glyph roll instead. */
function FadeSwap({ value, className }: { value: string; className?: string }) {
  const reduce = Boolean(useReducedMotion());
  return (
    <span className="relative inline-grid min-w-0 max-w-full overflow-hidden align-baseline">
      <AnimatePresence initial={false} mode="popLayout">
        <motion.span
          key={value}
          initial={reduce ? { opacity: 0 } : { y: 8, opacity: 0, filter: 'blur(3px)' }}
          animate={{ y: 0, opacity: 1, filter: 'blur(0px)' }}
          exit={reduce ? { opacity: 0 } : { y: -7, opacity: 0, filter: 'blur(2px)' }}
          transition={{ duration: reduce ? 0.12 : 0.42, ease: SOFT_EASE }}
          className={cn('col-start-1 row-start-1 block max-w-full truncate', className)}
        >
          {value}
        </motion.span>
      </AnimatePresence>
    </span>
  );
}

/* ── Chrome ──────────────────────────────────────────────────────────────── */

type LeadFeedBadgeProps = {
  id: string;
  label: string;
  initials: string;
  selected: boolean;
  reduce: boolean;
  ariaLabel: string;
  onClick: () => void;
};

function ActiveStoryRingStroke({ reduce = false, glow = true }: { reduce?: boolean; glow?: boolean }) {
  return (
    <>
      {!reduce && glow && (
        <motion.span
          className="pointer-events-none absolute -inset-1 rounded-full bg-[var(--fm-accent)]/24 blur-md"
          initial={{ opacity: 0, scale: 0.86 }}
          animate={{ opacity: [0, 0.64, 0], scale: [0.86, 1.16, 1.02] }}
          transition={{ duration: 0.9, ease: SOFT_EASE, times: [0, 0.42, 1] }}
        />
      )}
      <svg className="pointer-events-none absolute inset-0 z-10 h-full w-full overflow-visible" viewBox="0 0 100 100" aria-hidden="true">
        <g transform="rotate(-90 50 50)">
          <circle cx="50" cy="50" r={STORY_RING_RADIUS} fill="none" stroke="rgb(var(--fm-accent-rgb)/0.22)" strokeWidth="7.5" />
          {reduce ? (
            <circle cx="50" cy="50" r={STORY_RING_RADIUS} fill="none" stroke="var(--fm-accent)" strokeWidth="7.5" strokeLinecap="round" style={glow ? { filter: 'drop-shadow(0 3px 7px rgb(var(--fm-accent-rgb)/0.34))' } : undefined} />
          ) : (
            <motion.circle
              cx="50"
              cy="50"
              r={STORY_RING_RADIUS}
              fill="none"
              stroke="var(--fm-accent)"
              strokeWidth="7.5"
              strokeLinecap="round"
              strokeDasharray={STORY_RING_CIRCUMFERENCE}
              initial={{ opacity: 0.9, strokeDashoffset: STORY_RING_CIRCUMFERENCE }}
              animate={{ opacity: 1, strokeDashoffset: 0 }}
              transition={{ duration: 1.08, ease: SOFT_EASE }}
              style={glow ? { filter: 'drop-shadow(0 3px 7px rgb(var(--fm-accent-rgb)/0.34))' } : undefined}
            />
          )}
        </g>
      </svg>
    </>
  );
}

function LeadWedgeButton({ active, current, reduce, onBack }: { active: boolean; current: boolean; reduce: boolean; onBack: () => void }) {
  return (
    <motion.button
      type="button"
      disabled={!active}
      tabIndex={active ? 0 : -1}
      onClick={onBack}
      whileTap={active ? { scale: 0.96 } : undefined}
      className="flex w-[70px] shrink-0 flex-col items-center gap-1 border-0 bg-transparent p-0 text-inherit outline-none [user-select:none] [-webkit-tap-highlight-color:transparent] disabled:cursor-default focus-visible:rounded-[18px] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)] focus-visible:ring-offset-2 focus-visible:ring-offset-[#090909] lg:w-[82px] lg:gap-[5px]"
      aria-label={active ? 'Back to All Feedboards' : 'All Feedboards'}
    >
      <span
        className={cn('relative grid place-items-center rounded-full p-1', !reduce && 'transition-[background,width,height] duration-500 ease-out', current ? 'h-[64px] w-[64px] bg-[#FFE4EA] dark:bg-[#3F0F1B] lg:h-[76px] lg:w-[76px]' : 'h-[60px] w-[60px] bg-black/[0.07] dark:bg-white/[0.11] lg:h-[70px] lg:w-[70px]')}
      >
        {current && <ActiveStoryRingStroke reduce={reduce} glow={false} />}
        <span className="relative z-20 grid h-full w-full place-items-center overflow-hidden rounded-full border-[2px] border-white" style={{ background: 'var(--lead-wedge-surface)' }}>
          <motion.span
            className="grid h-[38px] w-[38px] place-items-center leading-none lg:h-[42px] lg:w-[42px]"
            animate={{ rotate: active ? 180 : 0 }}
            transition={reduce ? REDUCED_RAIL_TRANSITION : LEAD_WEDGE_SPRING}
            style={{ transformOrigin: '50% 50%' }}
          >
            <svg aria-hidden viewBox="0 0 42 42" className="block h-[38px] w-[38px] lg:h-[42px] lg:w-[42px]">
              <path
                d="M 21 21 L 35.87 12.76 A 17 17 0 1 0 35.87 29.24 Z"
                fill="none"
                stroke="var(--lead-wedge-mark)"
                strokeWidth="3"
                strokeLinecap="round"
                strokeLinejoin="round"
              />
            </svg>
          </motion.span>
        </span>
      </span>
      <span className={cn('block w-full text-center text-[9px] font-black uppercase leading-none lg:text-[10px]', current ? 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]' : 'text-black/42 dark:text-white/44')}>{active ? 'BACK' : 'FEEDS'}</span>
    </motion.button>
  );
}

const LeadFeedBadge = forwardRef<HTMLButtonElement, LeadFeedBadgeProps>(function LeadFeedBadge({ id, label, initials, selected, reduce, ariaLabel, onClick }, ref) {
  return (
    <motion.button
      ref={ref}
      type="button"
      layout="position"
      layoutId={`lead-feed-badge-${id}`}
      initial={false}
      animate={{ opacity: 1, scale: 1 }}
      exit={reduce ? { opacity: 0, transition: REDUCED_RAIL_TRANSITION } : { opacity: 0, scale: 0.96, transition: LEAD_RAIL_SPRING }}
      transition={reduce ? REDUCED_RAIL_TRANSITION : LEAD_RAIL_SPRING}
      whileTap={{ scale: 0.95 }}
      onClick={onClick}
      className="flex w-[70px] shrink-0 flex-col items-center gap-1 border-0 bg-transparent p-0 text-inherit outline-none [user-select:none] [-webkit-tap-highlight-color:transparent] lg:w-[82px] lg:gap-[5px]"
      aria-pressed={selected}
      aria-label={ariaLabel}
    >
      <span className={cn('relative grid place-items-center rounded-full p-1', !reduce && 'transition-[background,box-shadow,width,height] duration-500 ease-out', selected ? 'h-[64px] w-[64px] bg-[#FFE4EA] shadow-[0_10px_26px_-20px_rgb(var(--fm-accent-rgb)/0.85)] dark:bg-[#3F0F1B] lg:h-[76px] lg:w-[76px]' : 'h-[60px] w-[60px] bg-black/[0.07] shadow-none dark:bg-white/[0.11] lg:h-[70px] lg:w-[70px]')}>
        {selected && <ActiveStoryRingStroke reduce={reduce} />}
        <span
          className="relative z-20 grid h-full w-full place-items-center overflow-hidden rounded-full border-[2px] border-white text-[12px] font-black leading-none dark:border-[var(--fm-ink)]"
          style={{
            color: 'var(--fm-accent)',
            background: 'radial-gradient(circle at 30% 18%, rgb(var(--fm-accent-rgb) / 0.12), transparent 58%), linear-gradient(135deg,#f7f7f8,#cfd3dc)',
          }}
        >
          {initials}
        </span>
      </span>
      <span className={cn('block w-full overflow-hidden text-ellipsis whitespace-nowrap text-center text-[9px] font-black uppercase leading-none lg:text-[10px]', selected ? 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]' : 'text-black/42 dark:text-white/44')}>{label}</span>
    </motion.button>
  );
});

type LeadFeederCircleProps = {
  feeder: ApiFeeder;
  selected: boolean;
  index?: number;
  reduce?: boolean;
  onClick: () => void;
};

const LeadFeederCircle = forwardRef<HTMLButtonElement, LeadFeederCircleProps>(function LeadFeederCircle({
  feeder,
  selected,
  index = 0,
  reduce = false,
  onClick,
}, ref) {
  const delay = Math.min(index * 0.024, 0.12);
  const originX = -LEAD_FEEDER_SLOT * (index + 1);
  const handle = normalizeHandle(feeder.handle);

  return (
    <motion.button
      ref={ref}
      type="button"
      layout="position"
      initial={reduce ? { opacity: 0 } : { opacity: 0, x: originX, scale: 0.7 }}
      animate={reduce ? { opacity: 1 } : { opacity: 1, x: 0, scale: 1 }}
      exit={
        reduce
          ? { opacity: 0, transition: REDUCED_RAIL_TRANSITION }
          : {
              opacity: 0,
              x: originX,
              scale: 0.7,
              transition: LEAD_RAIL_SPRING,
            }
      }
      transition={reduce ? REDUCED_RAIL_TRANSITION : { ...LEAD_RAIL_SPRING, delay }}
      whileTap={{ scale: 0.95 }}
      onClick={onClick}
      className="flex w-[70px] shrink-0 flex-col items-center gap-1 border-0 bg-transparent p-0 text-inherit outline-none [user-select:none] [-webkit-tap-highlight-color:transparent] lg:w-[82px] lg:gap-[5px]"
      aria-pressed={selected}
      aria-label={`Spotlight @${handle}`}
    >
      <span className={cn('relative grid place-items-center rounded-full p-1', !reduce && 'transition-[background,box-shadow,width,height] duration-500 ease-out', selected ? 'h-[64px] w-[64px] bg-[#FFE4EA] shadow-[0_10px_26px_-20px_rgb(var(--fm-accent-rgb)/0.85)] dark:bg-[#3F0F1B] lg:h-[76px] lg:w-[76px]' : 'h-[60px] w-[60px] bg-black/[0.07] shadow-none dark:bg-white/[0.11] lg:h-[70px] lg:w-[70px]')}>
        {selected && <ActiveStoryRingStroke reduce={reduce} />}
        <span className="relative z-20 flex h-full w-full items-center justify-center overflow-hidden rounded-full border-[2px] border-white bg-[linear-gradient(135deg,#fce7f3,#fff1f2)] text-[var(--fm-accent-deeper)] dark:border-[var(--fm-ink)] dark:bg-[linear-gradient(135deg,#1c1917,#18181b)] dark:text-[var(--fm-accent-soft)]">
          <FeederStoryAvatar feeder={{ handle, profilePicUrl: feeder.profilePicUrl }} />
        </span>
      </span>
      <span className={cn('block w-full overflow-hidden text-ellipsis whitespace-nowrap text-center text-[9px] font-black lowercase leading-none lg:text-[10px]', selected ? 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]' : 'text-black/42 dark:text-white/44')}>
        {handle}
      </span>
    </motion.button>
  );
});

function TimeframePicker({
  days,
  pending,
  onChange,
}: {
  days: Timeframe;
  pending: boolean;
  onChange: (days: Timeframe) => void;
}) {
  const reduce = Boolean(useReducedMotion());
  const panelId = useId();
  const triggerRef = useRef<HTMLButtonElement | null>(null);
  const panelRef = useRef<HTMLDivElement | null>(null);
  const optionRefs = useRef<Array<HTMLButtonElement | null>>([]);
  const [open, setOpen] = useState(false);
  const [placement, setPlacement] = useState<{ top: number; right: number } | null>(null);
  const availableTimeframes = TIMEFRAMES.filter((value) => value !== days);

  useEffect(() => {
    if (!open) return undefined;
    const frame = window.requestAnimationFrame(() => {
      optionRefs.current[0]?.focus();
    });
    return () => window.cancelAnimationFrame(frame);
  }, [days, open]);

  useEffect(() => {
    if (!open) return undefined;

    const closeFromOutside = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (target && !triggerRef.current?.contains(target) && !panelRef.current?.contains(target)) setOpen(false);
    };
    const closeFromKeyboard = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      setOpen(false);
      triggerRef.current?.focus();
    };
    const closeFromViewportChange = () => setOpen(false);

    document.addEventListener('pointerdown', closeFromOutside);
    document.addEventListener('keydown', closeFromKeyboard);
    window.addEventListener('orientationchange', closeFromViewportChange);
    return () => {
      document.removeEventListener('pointerdown', closeFromOutside);
      document.removeEventListener('keydown', closeFromKeyboard);
      window.removeEventListener('orientationchange', closeFromViewportChange);
    };
  }, [open]);

  const toggle = () => {
    if (open) {
      setOpen(false);
      return;
    }
    const rect = triggerRef.current?.getBoundingClientRect();
    if (!rect) return;
    setPlacement({
      top: Math.min(rect.bottom - 12, window.innerHeight - 160),
      right: Math.max(12, window.innerWidth - rect.right),
    });
    setOpen(true);
  };

  const moveFocus = (index: number, direction: number) => {
    optionRefs.current[(index + direction + availableTimeframes.length) % availableTimeframes.length]?.focus();
  };

  const picker = typeof document !== 'undefined' ? createPortal(
    <AnimatePresence>
      {open && placement ? (
        <motion.div
          ref={panelRef}
          id={panelId}
          role="menu"
          aria-label="Time range"
          initial={{ height: reduce ? 148 : 12 }}
          animate={{ height: 148 }}
          exit={{ height: reduce ? 148 : 12 }}
          transition={reduce
            ? { duration: 0 }
            : { type: 'spring', stiffness: 360, damping: 36, mass: 0.9 }}
          className="fixed z-[160] w-[76px] overflow-hidden rounded-[14px] border border-white/[0.11] bg-[linear-gradient(180deg,rgba(20,16,18,.98),rgba(5,5,5,.96))] text-white shadow-[0_24px_60px_-22px_rgba(0,0,0,.98),0_0_38px_-24px_rgb(var(--fm-accent-rgb)/.76)] backdrop-blur-[18px]"
          style={{ top: placement.top, right: placement.right, willChange: reduce ? undefined : 'height' }}
        >
          <div className="grid gap-1 px-px pb-1 pt-[14px]">
            {availableTimeframes.map((value, index) => (
                <button
                  key={value}
                  ref={(node) => { optionRefs.current[index] = node; }}
                  type="button"
                  role="menuitem"
                  onClick={() => {
                    setOpen(false);
                    triggerRef.current?.focus();
                    onChange(value);
                  }}
                  onKeyDown={(event) => {
                    if (event.key === 'ArrowDown' || event.key === 'ArrowRight') {
                      event.preventDefault();
                      moveFocus(index, 1);
                    } else if (event.key === 'ArrowUp' || event.key === 'ArrowLeft') {
                      event.preventDefault();
                      moveFocus(index, -1);
                    } else if (event.key === 'Home' || event.key === 'End') {
                      event.preventDefault();
                      optionRefs.current[event.key === 'Home' ? 0 : availableTimeframes.length - 1]?.focus();
                    }
                  }}
                  className="relative grid h-10 w-full touch-manipulation place-items-center overflow-hidden rounded-[11px] text-[16px] font-black tabular-nums tracking-[-0.03em] text-white/60 outline-none transition-[background-color,color,transform] duration-150 active:scale-[0.94] active:bg-[var(--fm-accent)]/70 active:text-white focus-visible:bg-[var(--fm-accent)]/70 focus-visible:text-white focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]"
                >
                  {value}D
                </button>
            ))}
          </div>
        </motion.div>
      ) : null}
    </AnimatePresence>,
    document.body,
  ) : null;

  return (
    <div className="shrink-0 sm:hidden">
      <button
        ref={triggerRef}
        type="button"
        aria-haspopup="menu"
        aria-expanded={open}
        aria-controls={open ? panelId : undefined}
        aria-label={`Time range: ${days} days`}
        aria-busy={pending}
        onClick={toggle}
        className="relative z-10 flex h-11 w-[76px] touch-manipulation items-center justify-center gap-1 overflow-hidden rounded-[14px] border border-white/[0.09] bg-black/38 text-[16px] font-black uppercase tracking-[-0.02em] text-white shadow-[inset_0_2px_10px_rgba(0,0,0,.38)] outline-none transition-transform duration-150 active:scale-[0.97] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
      >
        <span
          data-testid="lead-timeframe-value"
          className="relative grid h-[1.08em] w-8 shrink-0 place-items-center overflow-hidden leading-none"
        >
          <AnimatePresence initial={false}>
            <motion.span
              key={days}
              initial={reduce ? false : { transform: 'translate3d(0, 112%, 0)' }}
              animate={{ transform: 'translate3d(0, 0%, 0)' }}
              exit={reduce ? { transform: 'translate3d(0, 0%, 0)' } : { transform: 'translate3d(0, -112%, 0)' }}
              transition={reduce ? { duration: 0 } : TIMEFRAME_SLOT_SPRING}
              className="absolute inset-0 grid place-items-center tabular-nums text-white"
            >
              {days}D
            </motion.span>
          </AnimatePresence>
        </span>
        <motion.span
          animate={{ transform: open ? 'rotate(180deg)' : 'rotate(0deg)' }}
          transition={reduce ? { duration: 0.1 } : { type: 'spring', stiffness: 520, damping: 40, mass: 0.7 }}
          className="grid place-items-center"
        >
          <ChevronDown className="h-3.5 w-3.5 text-white/54" aria-hidden="true" />
        </motion.span>
      </button>
      {picker}
    </div>
  );
}

function LeadHeader({
  feeds,
  activeFeedId,
  activeFeeder,
  days,
  compressed,
  pending,
  onFeedChange,
  onFeederChange,
  onDaysChange,
}: {
  feeds: RuntimeFeed[];
  activeFeedId: string;
  activeFeeder: string;
  days: Timeframe;
  compressed: boolean;
  pending: boolean;
  onFeedChange: (id: string) => void;
  onFeederChange: (handle: string) => void;
  onDaysChange: (days: Timeframe) => void;
}) {
  const reduce = Boolean(useReducedMotion());
  const activeFeed = feeds.find((feed) => feed.id === activeFeedId) || null;
  const railScrollRef = useRef<HTMLDivElement | null>(null);
  const scopeLabel = activeFeeder
    ? `@${activeFeeder}`
    : activeFeed ? titleCase(activeFeed.title) : 'All Feedboards';

  useEffect(() => {
    if (railScrollRef.current) railScrollRef.current.scrollLeft = 0;
  }, [activeFeedId]);

  return (
    <AppHeader id="lead" compressed={compressed}>
      <header data-testid="lead-v3-header" className="h-full px-3 py-2 sm:px-4 lg:px-5 lg:py-2.5">
        <LayoutGroup id="lead-header-rail">
          <div className="relative z-10 flex h-full flex-col gap-1.5 pt-1 lg:gap-2 lg:pt-[14px]">
            <div className="flex h-11 min-h-11 items-center justify-between gap-2 lg:h-12 lg:min-h-12">
              <div className="flex min-w-0 items-center gap-2.5">
                <h1 className="fm-depth-title shrink-0 text-[22px] font-black leading-[0.88] tracking-[0.12em] text-white lg:text-[26px] lg:tracking-[0.14em]">LEAD</h1>
                <div className="flex h-7 min-w-0 flex-col justify-center border-l border-white/10 pl-2.5">
                  <div className="hidden text-[8px] font-black uppercase leading-none tracking-[0.18em] text-[var(--fm-accent-bright)] lg:block">Feederboard</div>
                  <AnimatePresence mode="popLayout" initial={false}>
                    <motion.div
                      key={scopeLabel}
                      initial={reduce ? false : { y: 8, opacity: 0, filter: 'blur(4px)' }}
                      animate={{ y: 0, opacity: 1, filter: 'blur(0px)' }}
                      exit={reduce ? { opacity: 0 } : { y: -6, opacity: 0, filter: 'blur(3px)' }}
                      transition={reduce ? { duration: 0.12 } : { duration: 0.34, delay: 0.08, ease: SOFT_EASE }}
                      className="max-w-[150px] truncate text-[14px] font-black leading-none tracking-[-0.025em] text-white/88 sm:max-w-[210px] sm:text-[16px] lg:mt-0.5"
                    >
                      {scopeLabel}
                    </motion.div>
                  </AnimatePresence>
                </div>
              </div>

              <TimeframePicker days={days} pending={pending} onChange={onDaysChange} />

              <div className="hidden h-11 w-[184px] shrink-0 grid-cols-4 rounded-[14px] border border-white/[0.07] bg-black/38 shadow-[inset_0_2px_10px_rgba(0,0,0,.38)] sm:grid">
                {TIMEFRAMES.map((value) => (
                  <button
                    key={value}
                    type="button"
                    onClick={() => onDaysChange(value)}
                    aria-busy={pending}
                    aria-pressed={days === value}
                    data-testid={`lead-v3-timeframe-${value}`}
                    className={cn('relative grid h-full place-items-center rounded-[10px] text-[13px] font-black uppercase leading-none tracking-[0.06em] transition-colors', days === value ? 'text-white' : 'text-white/52 hover:text-white/72')}
                  >
                    {days === value ? <motion.span layoutId="lead-v3-timeframe-pill" className="pointer-events-none absolute inset-[3px] rounded-[8px] bg-[var(--fm-accent)] shadow-[inset_0_1px_0_rgb(255_255_255_/_0.32)]" transition={HEADER_CIRCLE_POP} /> : null}
                    <span className="relative z-10">{value}D</span>
                  </button>
                ))}
              </div>
            </div>

            <motion.div
              data-testid="lead-header-rail"
              initial={false}
              className="min-w-0 overflow-hidden"
              animate={{
                opacity: compressed ? 0 : 1,
                y: compressed ? -12 : 0,
                clipPath: compressed ? 'inset(0 0 100% 0 round 22px)' : 'inset(0 0 0% 0 round 22px)',
              }}
              transition={{
                opacity: { duration: 0.16, ease: SOFT_EASE },
                y: { duration: 0.38, ease: SOFT_EASE },
                clipPath: { duration: 0.24, ease: SOFT_EASE },
              }}
              style={{
                pointerEvents: compressed ? 'none' : 'auto',
              }}
            >
              <div className="flex min-w-0 items-start gap-3">
                <div className="hidden sm:block">
                  <LeadWedgeButton active={Boolean(activeFeed)} current={!activeFeed} reduce={reduce} onBack={() => onFeedChange('all')} />
                </div>
                <div ref={railScrollRef} className="hide-scrollbar min-w-0 flex-1 overflow-x-auto pb-0.5 lg:pb-1">
                  <div className="flex min-w-max items-start gap-2 pr-3 lg:gap-3">
                    <div className="sm:hidden">
                      <LeadWedgeButton active={Boolean(activeFeed)} current={!activeFeed} reduce={reduce} onBack={() => onFeedChange('all')} />
                    </div>
                    <AnimatePresence initial={false} mode="popLayout">
                      {activeFeed ? (
                        <LeadFeedBadge
                          key={activeFeed.id}
                          id={activeFeed.id}
                          label={titleCase(activeFeed.title)}
                          initials={feedInitials(activeFeed.title)}
                          selected={!activeFeeder}
                          reduce={reduce}
                          ariaLabel={activeFeeder ? `Show all feeders in ${titleCase(activeFeed.title)}` : `${titleCase(activeFeed.title)} Feederboard`}
                          onClick={() => onFeederChange('')}
                        />
                      ) : (
                        feeds.map((feed) => (
                          <LeadFeedBadge
                            key={feed.id}
                            id={feed.id}
                            label={titleCase(feed.title)}
                            initials={feedInitials(feed.title)}
                            selected={false}
                            reduce={reduce}
                            ariaLabel={`Show ${titleCase(feed.title)}`}
                            onClick={() => onFeedChange(feed.id)}
                          />
                        ))
                      )}

                      {activeFeed?.feeders.map((feeder, index) => {
                        const handle = normalizeHandle(feeder.handle);
                        return (
                          <LeadFeederCircle
                            key={handle}
                            feeder={feeder}
                            selected={activeFeeder === handle}
                            index={index}
                            reduce={reduce}
                            onClick={() => onFeederChange(activeFeeder === handle ? '' : handle)}
                          />
                        );
                      })}
                    </AnimatePresence>
                  </div>
                </div>
              </div>
            </motion.div>
          </div>
        </LayoutGroup>
      </header>
    </AppHeader>
  );
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
    <span className="mr-6 flex shrink-0 items-center gap-2 border-r border-white/[0.07] pr-6 last:border-0">
      <span className={cn('text-[8px]', event.dir === 'up' ? 'text-[var(--fm-accent-bright)]' : event.dir === 'down' ? 'text-white/30' : 'text-white/40')}>
        {event.dir === 'up' ? '▲' : event.dir === 'down' ? '▼' : '●'}
      </span>
      <span className="whitespace-nowrap text-[9px] font-black uppercase tracking-[0.12em] text-white/52">{event.text}</span>
      <span className={cn(
        'whitespace-nowrap rounded-[8px] px-1.5 py-0.5 text-[9px] font-black tabular-nums tracking-[0.06em]',
        event.dir === 'up'
          ? 'bg-[var(--fm-accent)]/90 text-white'
          : event.dir === 'down'
            ? 'border border-white/8 bg-white/[0.05] text-white/44'
            : 'border border-white/8 bg-white/[0.03] text-white/56',
      )}>{event.value}</span>
    </span>
  );
}

function LeadTape({ events, dealId }: { events: TapeEvent[]; dealId: number }) {
  const reduce = Boolean(useReducedMotion());
  if (!events.length) return null;
  const duration = Math.max(22, events.length * 4.2);

  return (
    <div data-testid="lead-v3-tape" className="relative mb-3 overflow-hidden rounded-[16px] border border-white/[0.08] bg-[#070707] py-2.5">
      <style>{TAPE_CSS}</style>
      {reduce ? (
        <div className="flex overflow-x-auto px-4 [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
          {events.map((event) => <TapeChip key={event.id} event={event} />)}
        </div>
      ) : (
        <AnimatePresence mode="popLayout" initial={false}>
          <motion.div
            key={dealId}
            initial={{ y: 8, opacity: 0 }}
            animate={{ y: 0, opacity: 1 }}
            exit={{ y: -8, opacity: 0 }}
            transition={{ duration: 0.3, ease: SOFT_EASE }}
            className="flex w-max [animation-play-state:running] hover:[animation-play-state:paused]"
            style={{ animation: `fm-lead-tape-scroll ${duration}s linear infinite` }}
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
      <span aria-hidden="true" className="pointer-events-none absolute inset-y-0 left-0 w-8 bg-[linear-gradient(90deg,#070707,transparent)]" />
      <span aria-hidden="true" className="pointer-events-none absolute inset-y-0 right-0 w-8 bg-[linear-gradient(270deg,#070707,transparent)]" />
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

function WireLine({ line, dealId }: { line: string | null; dealId: number }) {
  if (!line) return null;
  return (
    <div data-testid="lead-v3-wire" className="flex items-center gap-3 border-b border-white/[0.08] bg-[#060606] px-4 py-3 sm:px-5">
      <span className="shrink-0 rounded-full bg-[var(--fm-accent)] px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.16em] text-white">Wire</span>
      <AnimatePresence mode="popLayout" initial={false}>
        <motion.p key={`${dealId}:${line}`} initial={{ y: 6, opacity: 0 }} animate={{ y: 0, opacity: 1 }} exit={{ y: -6, opacity: 0 }} transition={{ ...HEADER_CIRCLE_POP, delay: 0.16 }} className="min-w-0 text-[11px] font-bold leading-snug text-white/72 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden sm:text-[12px]">{line}</motion.p>
      </AnimatePresence>
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
      hint: 'position in the stream',
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
    hint: 'median of the window',
    name: feederHandle ? 'its running level' : rows[0] ? `${rows[0].name} leads` : '—',
    detail: `${scopedPosts.length} posts · ${days}D`,
    thumb: null,
    collage: scopedPosts.slice(0, 3).map(({ post }) => post.thumbnailUrl).filter((url): url is string => Boolean(url)),
    shift: typicalDelta == null || typicalDelta === 0
      ? null
      : { up: typicalDelta > 0, text: `${Math.abs(typicalDelta)} ${typicalDelta > 0 ? 'stronger' : 'softer'} than prior ${days}D` },
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
    eyebrow: 'Payoff',
    value: netDelta == null
      ? compactNumber(rows.reduce((sum, row) => sum + row.followers, 0))
      : signedNumber(netDelta),
    hint: netDelta == null ? 'followers watching' : 'net followers this window',
    name: netDelta == null ? 'across the board' : (rows.find((row) => (row.followerDelta ?? 0) > 0)?.name || 'across the board'),
    detail: `${days}D window`,
    thumb: strongestWindowPost?.post.thumbnailUrl || null,
    shift: netDelta == null || priorNet == null
      ? null
      : { up: netDelta >= priorNet, text: `prior window ${signedNumber(priorNet)}` },
  });

  return cards;
}

function LeadersStrip({ cards }: { cards: LeaderCard[] }) {
  const reduce = Boolean(useReducedMotion());
  if (!cards.length) return null;

  return (
    <motion.div
      variants={SLOT_CONTAINER}
      initial={reduce ? false : 'hidden'}
      animate="visible"
      className="flex snap-x snap-mandatory gap-3 overflow-x-auto pb-2 [scrollbar-width:none] lg:grid lg:grid-cols-3 lg:overflow-visible lg:pb-0 [&::-webkit-scrollbar]:hidden"
    >
      {cards.map((card) => (
        <motion.article
          key={card.key}
          variants={SLOT_ITEM}
          data-testid={`lead-v3-leader-${card.key}`}
          className="relative min-h-[176px] min-w-[74vw] snap-center overflow-hidden rounded-[24px] border border-white/10 bg-[#0a0a0a] sm:min-h-[228px] sm:min-w-[340px] sm:rounded-[26px] lg:min-w-0"
        >
          {card.thumb ? (
            <CrossfadeImage src={card.thumb} eager />
          ) : card.collage?.length ? (
            <div className="absolute inset-0 grid grid-cols-3">
              {card.collage.map((url, index) => (
                <span key={`${card.key}:${index}`} className="relative block h-full w-full opacity-40">
                  <CrossfadeImage src={url} />
                </span>
              ))}
            </div>
          ) : null}
          <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(4,4,4,.62)_0%,rgba(4,4,4,.28)_38%,rgba(4,4,4,.9)_82%)]" />

          <div className="relative z-10 flex h-full min-h-[176px] flex-col justify-between p-4 sm:min-h-[228px] sm:p-5">
            <div className="flex items-center justify-between gap-3">
              <span className="text-[9px] font-black uppercase tracking-[0.2em] text-[var(--fm-accent-bright)]">{card.eyebrow}</span>
              <span className="rounded-full border border-white/12 bg-black/40 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.12em] text-white/64">{card.hint}</span>
            </div>
            <div>
              <div className="fm-depth-title flex items-end font-black leading-[0.78] tracking-[-0.05em] text-white">
                {card.valuePrefix ? <span className="mr-2 pb-1 text-[12px] tracking-normal text-white/50">{card.valuePrefix}</span> : null}
                <SlotText value={card.value} className="text-[48px] tabular-nums sm:text-[64px]" />
                {card.valueSuffix ? <span className="ml-1 pb-1 text-[26px] text-[var(--fm-accent-bright)]">{card.valueSuffix}</span> : null}
              </div>
              {card.shift ? (
                <div className={cn('mt-2 flex items-center gap-1 text-[9px] font-black uppercase tracking-[0.13em]', card.shift.up ? 'text-[var(--fm-accent-bright)]' : 'text-white/40')}>
                  <span className="text-[8px]">{card.shift.up ? '▲' : '▼'}</span>
                  <FadeSwap value={card.shift.text} />
                </div>
              ) : null}
              <div className="mt-3 flex items-center justify-between gap-3 border-t border-white/[0.09] pt-3">
                <FadeSwap value={card.name} className="text-[12px] font-black text-white" />
                <span className="shrink-0">
                  <FadeSwap value={card.detail} className="text-right text-[9px] font-black uppercase tracking-[0.12em] text-white/56" />
                </span>
              </div>
            </div>
          </div>
        </motion.article>
      ))}
    </motion.div>
  );
}

/* ── Feederboard rows ────────────────────────────────────────────────────── */

function RowThumb({ row, throne }: { row: BoardRow; throne: boolean }) {
  return (
    <span className={cn(
      'relative block shrink-0 overflow-hidden rounded-[14px] border border-white/12 bg-[#101010] shadow-[0_14px_28px_-16px_rgba(0,0,0,.9)]',
      throne ? 'h-[72px] w-[58px] rounded-[16px] sm:h-[84px] sm:w-[67px]' : 'h-[56px] w-[45px]',
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

function RowVerdict({ row }: { row: BoardRow }) {
  return (
    <span className={cn('text-[8px] font-black uppercase tracking-[0.13em]', row.hot ? 'text-[var(--fm-accent-bright)]' : 'text-white/34')}>
      <SlotText value={row.verdict} align="left" />
    </span>
  );
}

/* Rank movement vs the prior equal window. Accent up, dim down, NEW for
   entrants — the market grammar, never green/red. */
function MoveChip({ move, className }: { move: number | null; className?: string }) {
  if (move == null) {
    return <span className={cn('inline-flex shrink-0 items-center rounded-[7px] bg-white/[0.07] px-1.5 py-0.5 text-[7px] font-black uppercase tracking-[0.1em] text-white/44', className)}>new</span>;
  }
  const up = move > 0;
  const down = move < 0;
  return (
    <span
      className={cn(
        'inline-flex shrink-0 items-center gap-0.5 text-[9px] font-black tabular-nums leading-none',
        up ? 'text-[var(--fm-accent-bright)]' : down ? 'text-white/36' : 'text-white/28',
        className,
      )}
    >
      <span className="text-[7px]">{up ? '▲' : down ? '▼' : '–'}</span>
      {move === 0 ? '' : Math.abs(move)}
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
    <motion.dl variants={SLOT_ITEM} className="order-3 -mx-4 flex min-w-0 overflow-x-auto border-y border-white/[0.08] px-4 [scrollbar-width:none] sm:-mx-6 sm:px-6 lg:order-2 lg:mx-0 lg:grid lg:grid-cols-2 lg:gap-x-6 lg:gap-y-7 lg:overflow-visible lg:border-y-0 lg:border-l lg:border-white/[0.08] lg:px-0 lg:pl-7 [&::-webkit-scrollbar]:hidden">
      {metrics.map((metric) => (
        <div key={metric.label} className="min-w-[118px] border-r border-white/[0.08] py-4 pr-4 last:border-r-0 lg:min-w-0 lg:border-r-0 lg:py-0 lg:pr-0">
          <dt className="truncate text-[8px] font-black uppercase tracking-[0.2em] text-white/30">{metric.label}</dt>
          <dd className="fm-depth-title mt-2 truncate text-[22px] font-black tabular-nums leading-none text-white sm:text-[26px]"><SlotText value={metric.value} /></dd>
          <div className="mt-2 text-[8px] font-black tabular-nums text-white/34">
            {metric.multiple == null ? 'raw movement' : `${formatMultiple(metric.multiple)} usual`}
          </div>
        </div>
      ))}
    </motion.dl>
  );
}

function RowExpansion({ row, days, onCollapse }: { row: BoardRow; days: number; onCollapse: () => void }) {
  const reduce = Boolean(useReducedMotion());

  return (
    <motion.div
      data-testid="lead-v3-expansion"
      initial={reduce ? false : { height: 0, opacity: 0 }}
      animate={{ height: 'auto', opacity: 1 }}
      exit={{ height: 0, opacity: 0 }}
      transition={reduce ? { duration: 0 } : { height: { duration: 0.44, ease: SOFT_EASE }, opacity: { duration: 0.26, delay: 0.05 } }}
      className="overflow-hidden"
    >
      <motion.div
        variants={SLOT_CONTAINER}
        initial={reduce ? false : 'hidden'}
        animate="visible"
        exit="exit"
        onClick={onCollapse}
        className="relative isolate cursor-pointer overflow-hidden border-t border-white/[0.1] bg-[#0a0a0a] p-4 sm:p-6 lg:min-h-[360px]"
      >
        <div className="pointer-events-none absolute inset-y-0 left-0 w-1 bg-[var(--fm-accent)] shadow-[0_0_34px_rgb(var(--fm-accent-rgb)/.48)]" />
        <div className="pointer-events-none absolute -bottom-14 left-3 select-none text-[164px] font-black leading-none tracking-[-0.1em] text-white/[0.025] sm:text-[210px]">0{row.rank}</div>

        <motion.div variants={SLOT_ITEM} className="relative grid gap-6 lg:grid-cols-[210px_minmax(240px,.78fr)_minmax(400px,1.3fr)] lg:items-center lg:gap-8">
          <div className="order-1 min-w-0">
            <div className="flex items-center justify-between gap-3 text-[8px] font-black uppercase tracking-[0.18em] text-white/24">
              <span>
                {row.postCount} posts · {days}D
                {row.prevPostCount != null ? (
                  <i className={cn('ml-1.5 not-italic', row.postCount > row.prevPostCount ? 'text-[var(--fm-accent-bright)]/70' : 'text-white/20')}>
                    {row.postCount === row.prevPostCount ? 'same pace' : row.postCount > row.prevPostCount ? `up from ${row.prevPostCount}` : `down from ${row.prevPostCount}`}
                  </i>
                ) : null}
              </span>
              <i className="not-italic text-white/16">tap outside media to close</i>
            </div>

            <div className="mt-5 flex items-end gap-4 lg:mt-7 lg:block">
              <div className="min-w-0">
                <div className="text-[8px] font-black uppercase tracking-[0.2em] text-white/30">Range · best post</div>
                <div className="fm-depth-title mt-2 flex items-end font-black leading-[0.68] tracking-[-0.08em] text-white">
                  <span className="mr-2 pb-2 text-[11px] tracking-normal text-white/40">TOP</span>
                  <SlotText value={percentLabel(row.best)} className="text-[74px] tabular-nums sm:text-[96px] lg:text-[108px]" />
                  {row.best != null ? <span className="ml-1 pb-1 text-[30px] tracking-normal text-[var(--fm-accent-bright)] lg:text-[40px]">%</span> : null}
                </div>
              </div>
              <div className="shrink-0 border-l border-white/10 pl-4 lg:mt-4 lg:border-l-0 lg:border-t lg:border-white/10 lg:pl-0 lg:pt-4">
                <div className="fm-depth-title flex items-baseline text-[30px] font-black leading-none tracking-[-0.05em] text-white/56">
                  <SlotText value={percentLabel(row.floor)} className="tabular-nums" />
                  {row.floor != null ? <i className="ml-0.5 not-italic text-[15px] text-white/30">%</i> : null}
                </div>
                <div className="mt-1.5 text-[8px] font-black uppercase tracking-[0.16em] text-white/30">floor · weakest post</div>
              </div>
            </div>
          </div>

          <MetricLedger row={row} />

          <motion.div variants={SLOT_ITEM} className="order-2 min-w-0 lg:order-3">
            <div className="mb-3 flex items-center justify-between gap-3">
              <span className="text-[8px] font-black uppercase tracking-[0.2em] text-white/32">Posts behind the number</span>
              <span className="text-[8px] font-black uppercase tracking-[0.16em] text-white/20">Best first</span>
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
                      className="relative aspect-[4/5] w-[58%] min-w-[58%] snap-center overflow-hidden rounded-[22px] border border-white/12 bg-black shadow-[0_24px_48px_-28px_rgba(0,0,0,.95)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
                    >
                      {post.thumbnailUrl ? (
                        // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
                        <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" loading="lazy" decoding="async" />
                      ) : null}
                      <div className="absolute inset-0 bg-[linear-gradient(180deg,transparent_30%,rgba(0,0,0,.9))]" />
                      <div className="absolute inset-x-0 bottom-0 flex items-end justify-between gap-3 p-4">
                        <span className="text-[13px] font-black text-white">{post.latestPercentile == null ? '—' : `TOP ${percentLabel(post.latestPercentile)}%`}</span>
                        <span className="text-[8px] font-black uppercase tracking-[0.12em] text-white/54">{proofMetric(post)}</span>
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
                      initial={reduce ? false : { y: 24, opacity: 0 }}
                      animate={{ y: index * 10, opacity: 1 }}
                      transition={reduce ? { duration: 0 } : { delay: 0.12 + index * 0.065, duration: 0.56, ease: SOFT_EASE }}
                      className="group relative aspect-[4/5] w-full max-w-[200px] flex-1 overflow-hidden rounded-[22px] border border-white/12 bg-black shadow-[0_30px_54px_-30px_rgba(0,0,0,.98)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
                    >
                      {post.thumbnailUrl ? (
                        // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
                        <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover transition-transform duration-700 group-hover:scale-[1.035]" loading="lazy" decoding="async" />
                      ) : null}
                      <div className="absolute inset-0 bg-[linear-gradient(180deg,transparent_28%,rgba(0,0,0,.9))]" />
                      <div className="absolute inset-x-0 bottom-0 flex items-end justify-between gap-3 p-4">
                        <span className="text-[13px] font-black text-white">{post.latestPercentile == null ? '—' : `TOP ${percentLabel(post.latestPercentile)}%`}</span>
                        <span className="text-[8px] font-black uppercase tracking-[0.12em] text-white/54">{proofMetric(post)}</span>
                      </div>
                    </motion.a>
                  ))}
                </div>
              </>
            ) : (
              <div className="flex h-[180px] items-center justify-center border-y border-white/[0.07] text-[9px] font-black uppercase tracking-[0.18em] text-white/26">No tracked media in this window</div>
            )}
          </motion.div>
        </motion.div>
      </motion.div>
    </motion.div>
  );
}

function FeederboardRows({ rows, selectedId, onSelect, days }: { rows: BoardRow[]; selectedId: string; onSelect: (id: string) => void; days: number }) {
  const reduce = Boolean(useReducedMotion());
  const listRef = useRef<HTMLDivElement | null>(null);

  /* When a window/scope switch re-ranks the ladder, follow the row the user
     had open so it never glides out of the viewport. */
  useEffect(() => {
    if (!selectedId) return undefined;
    const timeout = window.setTimeout(() => {
      listRef.current
        ?.querySelector(`[data-row-id="${CSS.escape(selectedId)}"]`)
        ?.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
    }, 460);
    return () => window.clearTimeout(timeout);
  }, [rows, selectedId]);

  return (
    <section aria-labelledby="lead-v3-board-title" className="min-w-0 max-w-full overflow-hidden bg-[#070707]">
      <div className="flex items-center justify-between gap-4 border-b border-white/[0.08] px-4 py-4 sm:px-5">
        <div>
          <h2 id="lead-v3-board-title" className="text-[10px] font-black uppercase tracking-[0.2em] text-[var(--fm-accent-bright)]">Feederboard</h2>
          <p className="mt-1 text-[10px] font-bold leading-snug text-white/50">Ranked on the typical post · tap a row for the receipts</p>
        </div>
        <div className="shrink-0 text-right text-[9px] font-black uppercase tracking-[0.12em] text-white/46">
          TOP % · lower is stronger
        </div>
      </div>

      <div className="hidden grid-cols-[64px_minmax(230px,1.2fr)_150px_130px_120px_110px_30px] items-end gap-4 border-b border-white/[0.07] px-5 py-3 text-[7px] font-black uppercase tracking-[0.17em] text-white/24 lg:grid">
        <span>Rank</span><span>Feed / feeder</span><span>Typical post</span><span>Shift</span><span>Peak vs usual</span><span>Last posted</span><span />
      </div>

      <LayoutGroup id="lead-v3-board">
        <div ref={listRef}>
          {rows.map((row) => {
            const selected = row.id === selectedId;
            const throne = row.rank === 1;
            const strength = row.typical == null ? 0.06 : Math.max(0.06, Math.min(1, (101 - row.typical) / 100));
            return (
              <motion.div
                key={row.id}
                data-row-id={row.id}
                layout="position"
                initial={reduce ? false : { y: 22, opacity: 0 }}
                animate={{ y: 0, opacity: 1 }}
                exit={reduce
                  ? { opacity: 0, transition: { duration: 0.12 } }
                  : { y: -16, opacity: 0, transition: { duration: 0.38, ease: SOFT_EASE, delay: (row.rank - 1) * 0.04 } }}
                transition={reduce
                  ? { layout: LADDER_SPRING, duration: 0.12 }
                  : { layout: LADDER_SPRING, duration: 0.68, ease: SOFT_EASE, delay: 0.08 + (row.rank - 1) * 0.08 }}
                className="border-b border-white/[0.07] last:border-b-0"
              >
                <button
                  type="button"
                  onClick={() => onSelect(selected ? '' : row.id)}
                  aria-expanded={selected}
                  data-testid={`lead-v3-row-${row.rank}`}
                  className={cn(
                    'relative isolate block w-full overflow-hidden text-left transition-colors duration-300 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]',
                    throne
                      ? 'bg-[radial-gradient(circle_at_0_0,rgb(var(--fm-accent-rgb)/.14),transparent_44%),linear-gradient(90deg,rgba(255,255,255,.035),rgba(255,255,255,.01))]'
                      : selected
                        ? 'bg-white/[0.03]'
                        : 'hover:bg-white/[0.022]',
                  )}
                >
                  <motion.span
                    aria-hidden="true"
                    className={cn('absolute inset-y-0 left-0 z-0 w-full origin-left', throne ? 'bg-[var(--fm-accent)]/[0.06]' : 'bg-[var(--fm-accent)]/[0.03]')}
                    initial={reduce ? false : { scaleX: 0 }}
                    animate={{ scaleX: strength, opacity: [0, 1, throne ? 0.07 : 0.04] }}
                    transition={reduce ? { duration: 0 } : { scaleX: { delay: 0.18 + (row.rank - 1) * 0.08, duration: 1, ease: SOFT_EASE }, opacity: { delay: 0.18 + (row.rank - 1) * 0.08, duration: 1, ease: SOFT_EASE, times: [0, 0.55, 1] } }}
                  />
                  {throne ? <motion.span initial={reduce ? false : { opacity: 0 }} animate={{ opacity: [0, 1, 0.6] }} transition={reduce ? { duration: 0 } : { duration: 1, ease: SOFT_EASE }} className="absolute inset-y-0 left-0 z-20 w-1 bg-[var(--fm-accent)] shadow-[0_0_26px_rgb(var(--fm-accent-rgb)/.46)]" /> : null}

                  {/* mobile / tablet row */}
                  <div className={cn(
                    'relative z-10 grid items-center gap-2.5 px-3 py-3 sm:gap-3 sm:px-4 lg:hidden',
                    throne
                      ? 'min-h-[100px] grid-cols-[34px_58px_minmax(0,1fr)_auto] sm:grid-cols-[42px_67px_minmax(0,1fr)_auto]'
                      : 'min-h-[76px] grid-cols-[34px_45px_minmax(0,1fr)_auto] sm:grid-cols-[42px_45px_minmax(0,1fr)_auto]',
                  )}>
                    <span className="relative h-full">
                      <span className={cn(
                        'pointer-events-none absolute left-0 top-1/2 -translate-y-1/2 select-none font-black tabular-nums leading-none tracking-[-0.08em]',
                        throne ? 'text-[54px] text-[var(--fm-accent)]/[0.16]' : 'text-[44px] text-white/[0.055]',
                      )}>{String(row.rank).padStart(2, '0')}</span>
                    </span>
                    <RowThumb row={row} throne={throne} />
                    <span className="min-w-0">
                      <span className="flex min-w-0 items-center gap-1.5">
                        <span className={cn('truncate font-black leading-none tracking-[-0.02em] text-white', throne ? 'text-[17px]' : 'text-[15px]')}>{row.name}</span>
                        <MoveChip move={row.rankMove} />
                      </span>
                      <span className="mt-1.5 flex min-w-0 items-center gap-1.5">
                        <span className="truncate text-[9px] font-black uppercase tracking-[0.11em] text-white/52">{row.caption}</span>
                        <span className="text-white/16">·</span>
                        <RowVerdict row={row} />
                      </span>
                    </span>
                    <span className="flex items-center gap-2 text-right">
                      <span className="min-w-0">
                        <span className="block text-[8px] font-black uppercase tracking-[0.13em] text-white/52">Typical</span>
                        <span className={cn('fm-depth-title mt-1 block font-black tabular-nums leading-none text-white', throne ? 'text-[32px]' : 'text-[24px]')}>
                          <SlotText value={percentLabel(row.typical)} />
                          {row.typical == null ? null : <i className="ml-0.5 not-italic text-[0.5em] text-[var(--fm-accent-bright)]">%</i>}
                        </span>
                        <span className="mt-1 flex items-baseline justify-end gap-1.5 text-[11px] font-black tabular-nums leading-none">
                          {row.typicalShift != null && row.typicalShift !== 0 ? (
                            <span className={cn('flex items-baseline gap-0.5', row.typicalShift > 0 ? 'text-[var(--fm-accent-bright)]' : 'text-white/40')}>
                              <i className="not-italic text-[8px]">{row.typicalShift > 0 ? '▲' : '▼'}</i>
                              <SlotText value={String(Math.abs(row.typicalShift))} />
                            </span>
                          ) : null}
                          <span className="text-[10px] text-white/48"><SlotText value={formatMultiple(row.peak)} /> <i className="not-italic text-[8px] uppercase tracking-[0.08em] text-white/44">peak</i></span>
                        </span>
                      </span>
                      <ChevronDown className={cn('h-4 w-4 shrink-0 text-white/36 transition-transform duration-300', selected && 'rotate-180 text-white')} />
                    </span>
                  </div>

                  {/* desktop row */}
                  <div className={cn(
                    'relative z-10 hidden grid-cols-[64px_minmax(230px,1.2fr)_150px_130px_120px_110px_30px] items-center gap-4 px-5 py-3 lg:grid',
                    throne ? 'min-h-[104px]' : 'min-h-[80px]',
                  )}>
                    <span className="relative h-full">
                      <span className={cn(
                        'pointer-events-none absolute left-0 top-1/2 -translate-y-1/2 select-none font-black tabular-nums leading-none tracking-[-0.08em]',
                        throne ? 'text-[64px] text-[var(--fm-accent)]/[0.16]' : 'text-[50px] text-white/[0.055]',
                      )}>{String(row.rank).padStart(2, '0')}</span>
                    </span>
                    <span className="flex min-w-0 items-center gap-3.5">
                      <RowThumb row={row} throne={throne} />
                      <span className="min-w-0">
                        <span className="flex min-w-0 items-center gap-2">
                          <span className={cn('truncate font-black leading-none tracking-[-0.02em] text-white', throne ? 'text-[18px]' : 'text-[16px]')}>{row.name}</span>
                          <MoveChip move={row.rankMove} />
                        </span>
                        <span className="mt-1.5 flex min-w-0 items-center gap-1.5">
                          <span className="truncate text-[8px] font-black uppercase tracking-[0.13em] text-white/34">{row.caption}</span>
                          <span className="text-white/16">·</span>
                          <RowVerdict row={row} />
                        </span>
                      </span>
                    </span>
                    <span className="min-w-0">
                      <span className={cn('fm-depth-title block font-black tabular-nums leading-none text-white', throne ? 'text-[36px]' : 'text-[28px]')}>
                        <SlotText value={percentLabel(row.typical)} />
                        {row.typical == null ? null : <i className="ml-0.5 not-italic text-[0.5em] text-[var(--fm-accent-bright)]">%</i>}
                      </span>
                      <span className="mt-1.5 block text-[7px] font-black uppercase tracking-[0.15em] text-white/24">median · {row.postCount} posts{row.prevPostCount != null && row.prevPostCount !== row.postCount ? ` · prev ${row.prevPostCount}` : ''}</span>
                    </span>
                    <span className="min-w-0">
                      <span className={cn(
                        'fm-depth-title flex items-baseline gap-1 font-black tabular-nums leading-none',
                        throne ? 'text-[28px]' : 'text-[24px]',
                        row.typicalShift == null ? 'text-white/30' : row.typicalShift > 0 ? 'text-[var(--fm-accent-bright)]' : row.typicalShift < 0 ? 'text-white/44' : 'text-white/60',
                      )}>
                        {row.typicalShift != null && row.typicalShift !== 0 ? <i className="not-italic text-[0.45em]">{row.typicalShift > 0 ? '▲' : '▼'}</i> : null}
                        <SlotText value={row.typicalShift == null ? '—' : String(Math.abs(row.typicalShift))} />
                      </span>
                      <span className="mt-1.5 block text-[7px] font-black uppercase tracking-[0.15em] text-white/24">
                        {row.typicalShift == null ? 'no prior window' : row.typicalShift === 0 ? `level · prev ${days}D` : `places ${row.typicalShift > 0 ? 'stronger' : 'softer'} · prev ${days}D`}
                      </span>
                    </span>
                    <span className="min-w-0">
                      <SlotText value={formatMultiple(row.peak)} className="block text-[21px] font-black tabular-nums leading-none text-[var(--fm-accent-bright)]" />
                      <span className="mt-1.5 block text-[7px] font-black uppercase tracking-[0.15em] text-white/24">vs its usual</span>
                    </span>
                    <span className="min-w-0">
                      <SlotText value={lastPostedLabel(row.lastPostedAt)} className="block text-[15px] font-black tabular-nums leading-none text-white/60" />
                      <span className="mt-1.5 block text-[7px] font-black uppercase tracking-[0.15em] text-white/24">{row.lastPostedAt == null ? 'no posts' : lastPostedLabel(row.lastPostedAt) === 'TODAY' ? 'posted' : 'ago · last post'}</span>
                    </span>
                    <span className="grid h-7 w-7 place-items-center rounded-full border border-white/10 bg-black/20">
                      <ChevronDown className={cn('h-4 w-4 text-white/36 transition-transform duration-300', selected && 'rotate-180 text-white')} />
                    </span>
                  </div>
                </button>
                <AnimatePresence initial={false} mode="popLayout">
                  {selected && <RowExpansion key={row.id} row={row} days={days} onCollapse={() => onSelect('')} />}
                </AnimatePresence>
              </motion.div>
            );
          })}
        </div>
      </LayoutGroup>
    </section>
  );
}

/* ── Follower windows ────────────────────────────────────────────────────── */

function GrowthWindows({ windows, days, dealId }: { windows: GrowthWindow[]; days: number; dealId: number }) {
  const reduce = Boolean(useReducedMotion());
  const total = windows.reduce((sum, window) => sum + (window.delta ?? 0), 0);
  const hasDelta = windows.some((window) => window.delta != null);
  const highlightIndex = windows.reduce((bestIndex, window, index) => (
    Math.abs(window.delta ?? 0) >= Math.abs(windows[bestIndex]?.delta ?? 0) ? index : bestIndex
  ), 0);
  return (
    <section aria-labelledby="lead-v3-windows-title" className="relative isolate overflow-hidden border-t border-white/[0.09] bg-[#070707] p-5 sm:p-6 lg:p-7">
      <div className="flex items-end justify-between gap-6">
        <div>
          <div className="flex items-center gap-2 text-white/44"><Users className="h-4 w-4" /><span className="text-[8px] font-black uppercase tracking-[0.22em]">Follower windows</span></div>
          <h2 id="lead-v3-windows-title" className="mt-2 text-[22px] font-black leading-none tracking-[-0.035em] text-white sm:text-[26px]">What landed, when.</h2>
        </div>
        <div className="text-right">
          <SlotText value={hasDelta ? signedNumber(total) : '—'} className="fm-depth-title text-[38px] font-black tabular-nums leading-[0.78] tracking-[-0.045em] text-white sm:text-[50px]" />
          <div className="mt-2 text-[8px] font-black uppercase tracking-[0.18em] text-white/28">{days}D net new</div>
        </div>
      </div>

      <div className="relative mt-7 overflow-x-auto pb-2 [scrollbar-width:none] [&::-webkit-scrollbar]:hidden">
        <motion.div
          aria-hidden="true"
          initial={reduce ? false : { scaleX: 0, opacity: 0 }}
          animate={{ scaleX: 1, opacity: 1 }}
          transition={reduce ? { duration: 0 } : { duration: 0.8, ease: SOFT_EASE }}
          className="pointer-events-none absolute left-5 right-5 top-[51px] h-px origin-left bg-white/[0.13]"
        />
        <motion.div key={dealId} variants={SLOT_CONTAINER} initial={reduce ? false : 'hidden'} animate="visible" className="grid min-w-max auto-cols-[210px] grid-flow-col lg:min-w-0 lg:auto-cols-fr">
          {windows.map((window, index) => {
            const highlighted = index === highlightIndex;
            return (
              <motion.article key={window.id} variants={SLOT_ITEM} className="relative min-h-[168px] snap-start px-5 first:pl-1 last:pr-1">
                <div className="text-[8px] font-black uppercase tracking-[0.17em] text-white/34">{window.range}</div>
                <span className={cn('absolute top-[47px] h-[9px] w-[9px] rounded-full border-2 border-[#070707]', highlighted ? 'bg-[var(--fm-accent)] shadow-[0_0_18px_rgb(var(--fm-accent-rgb)/.78)]' : 'bg-white/36')} />
                <SlotText value={signedNumber(window.delta)} className={cn('fm-depth-title mt-10 block text-[28px] font-black tabular-nums leading-none tracking-[-0.035em]', highlighted ? 'text-[var(--fm-accent-bright)]' : 'text-white')} />
                <div className="mt-2 text-[7px] font-black uppercase tracking-[0.15em] text-white/24">Window {window.index} · {window.postCount} posts</div>
                <div className="mt-4 flex -space-x-3">
                  {window.posts.map((post) => (
                    <a
                      key={post.postKey}
                      href={post.postUrl || '#'}
                      target={post.postUrl ? '_blank' : undefined}
                      rel={post.postUrl ? 'noreferrer' : undefined}
                      onClick={(event) => {
                        if (!post.postUrl) event.preventDefault();
                      }}
                      className="h-14 shrink-0 aspect-[4/5] overflow-hidden rounded-[14px] border-2 border-[#070707] bg-white/[0.04] shadow-[0_12px_26px_-12px_rgba(0,0,0,.9)] transition-transform hover:-translate-y-1 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
                      aria-label={post.postUrl ? 'Open tracked post' : 'Tracked post'}
                    >
                      {post.thumbnailUrl ? (
                        // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
                        <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" loading="lazy" decoding="async" />
                      ) : null}
                    </a>
                  ))}
                  {!window.posts.length ? <span className="pt-2 text-[8px] font-black uppercase tracking-[0.14em] text-white/18">No landings</span> : null}
                </div>
              </motion.article>
            );
          })}
        </motion.div>
      </div>
    </section>
  );
}

/* ── Feederboard views — facts and ranked posts across the current scope ─── */

type ScopedPost = TrackedPost & { feedName: string };

function scopedPostsFor(feeds: RuntimeFeed[], activeFeedId: string, days: number): ScopedPost[] {
  const scoped = activeFeedId === 'all' ? feeds : feeds.filter((feed) => feed.id === activeFeedId);
  return scoped.flatMap((feed) => postsInWindow(feed.posts, days).map((post) => ({ ...post, feedName: titleCase(feed.title) })));
}

const FEEDERBOARD_VIEWS = [
  { id: 'feats', label: 'Feats' },
  { id: 'er', label: 'Eng.', ariaLabel: 'Engagement rate' },
  { id: 'comments', label: 'Comments' },
  { id: 'likes', label: 'Likes' },
] as const;

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

function FeederboardCards({
  feeds,
  activeFeedId,
  activeFeeder,
  days,
  facts,
}: {
  feeds: RuntimeFeed[];
  activeFeedId: string;
  activeFeeder: string;
  days: number;
  facts: FeederboardFact[];
}) {
  const reduce = Boolean(useReducedMotion());
  const [view, setView] = useState<FeederboardViewId>('feats');
  const posts = useMemo(
    () => scopedPostsFor(feeds, activeFeedId, days).filter((post) => !activeFeeder || normalizeHandle(post.handle) === activeFeeder),
    [activeFeedId, activeFeeder, days, feeds],
  );
  const records = useMemo(() => {
    if (view === 'feats') return [];
    return [...posts]
      .filter((post) => post.thumbnailUrl)
      .sort((a, b) => recordSortValue(b, view) - recordSortValue(a, view) || a.postKey.localeCompare(b.postKey))
      .slice(0, 10);
  }, [posts, view]);

  const cards = view === 'feats' ? facts : records;

  if (!posts.length) return null;

  return (
    <section aria-label="Feederboard views" className="relative isolate overflow-hidden bg-[#070707] px-4 pb-5 pt-3 sm:px-5 sm:pb-6 lg:px-7 lg:pb-7">
      <div className="flex justify-end">
        <div className="grid h-11 min-w-[224px] grid-cols-4 rounded-[14px] border border-white/[0.07] bg-black/38 shadow-[inset_0_2px_10px_rgba(0,0,0,.38)] sm:min-w-[272px]">
          {FEEDERBOARD_VIEWS.map((option) => (
            <button
              key={option.id}
              type="button"
              onClick={() => setView(option.id)}
              aria-label={'ariaLabel' in option ? option.ariaLabel : option.label}
              aria-pressed={view === option.id}
              data-testid={`lead-v3-feederboard-view-${option.id}`}
              className={cn(
                'relative grid h-full place-items-center rounded-[10px] text-[10px] font-black uppercase tracking-[0.08em] transition-colors sm:text-[11px]',
                view === option.id
                  ? 'text-white'
                  : 'text-white/36 hover:text-white/72',
              )}
            >
              {view === option.id ? <motion.span layoutId="lead-v3-feederboard-pill" className="pointer-events-none absolute inset-[3px] rounded-[8px] bg-[var(--fm-accent)] shadow-[inset_0_1px_0_rgb(255_255_255_/_0.28)]" transition={reduce ? REDUCED_RAIL_TRANSITION : HOF_PILL_SPRING} /> : null}
              <span className="relative z-10">{option.label}</span>
            </button>
          ))}
        </div>
      </div>

      <div className="mt-6 min-h-[300px] lg:min-h-[360px]">
        <AnimatePresence mode="popLayout" initial={false}>
          <motion.div
            key={`${view}:${activeFeedId}:${activeFeeder}:${days}:${facts.map((fact) => fact.kind).join(':')}`}
            initial={reduce ? false : { opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={reduce ? { opacity: 0 } : { opacity: 0, y: -6 }}
            transition={reduce ? REDUCED_RAIL_TRANSITION : { duration: 0.24, ease: SOFT_EASE }}
            className="flex snap-x snap-mandatory gap-3 overflow-x-auto pb-2 [scrollbar-width:none] lg:grid lg:grid-cols-5 lg:overflow-visible lg:pb-0 [&::-webkit-scrollbar]:hidden"
          >
            {view === 'feats' ? facts.map((fact, index) => (
              <motion.article
                key={fact.kind}
                initial={reduce ? false : { opacity: 0, y: 6 }}
                animate={{ opacity: 1, y: 0 }}
                transition={reduce ? REDUCED_RAIL_TRANSITION : { duration: 0.24, delay: Math.min(index * 0.024, 0.12), ease: SOFT_EASE }}
                data-testid={`lead-v3-feederboard-fact-${fact.kind}`}
                className="group relative aspect-[4/5] w-[220px] min-w-[220px] snap-start overflow-hidden rounded-[18px] border border-white/12 bg-black shadow-[0_22px_44px_-26px_rgba(0,0,0,.95)] sm:w-[236px] sm:min-w-[236px] lg:w-auto lg:min-w-0"
              >
                {fact.thumbnailUrl ? (
                  // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
                  <img src={fact.thumbnailUrl} alt="" className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-[1.02]" loading="lazy" decoding="async" />
                ) : null}
                <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,.06)_0%,transparent_54%,rgba(0,0,0,.65)_72%,rgba(0,0,0,.94)_100%)]" />
                <span className="absolute left-2.5 top-2.5 rounded-md bg-black/62 px-2 py-1 text-[9px] font-black uppercase tracking-[0.1em] text-[var(--fm-accent-bright)] backdrop-blur-sm">{fact.kind.replace(/-/g, ' ')}</span>
                {fact.postUrl ? <a href={fact.postUrl} target="_blank" rel="noreferrer" aria-label="Open fact evidence on Instagram" className="absolute right-2.5 top-2.5 rounded-md bg-black/62 px-2 py-1 text-[9px] font-black uppercase tracking-[0.1em] text-white/78 backdrop-blur-sm">Instagram</a> : null}
                <div className="absolute inset-x-0 bottom-0 flex min-h-[112px] flex-col justify-end p-3.5">
                  <div className="text-[20px] font-black leading-[1.02] tracking-[-0.035em] text-white [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden sm:text-[22px]">{fact.headline}</div>
                  <div className="mt-2 text-[10px] font-bold leading-[1.3] text-white/78 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden">{fact.detail}</div>
                </div>
              </motion.article>
            )) : records.map((post, index) => {
              const display = recordDisplay(post, view);
              return (
                <motion.a
                  key={post.postKey}
                  initial={reduce ? false : { opacity: 0, y: 6 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={reduce ? REDUCED_RAIL_TRANSITION : { duration: 0.24, delay: Math.min(index * 0.024, 0.12), ease: SOFT_EASE }}
                  href={post.postUrl || '#'}
                  target={post.postUrl ? '_blank' : undefined}
                  rel={post.postUrl ? 'noreferrer' : undefined}
                  onClick={(event) => { if (!post.postUrl) event.preventDefault(); }}
                  data-testid={`lead-v3-feederboard-record-${index + 1}`}
                  aria-label={post.postUrl ? `Open tracked post ranked ${index + 1} by ${display.label}` : `Tracked post ranked ${index + 1} by ${display.label}`}
                  className="group relative aspect-[4/5] w-[220px] min-w-[220px] snap-start overflow-hidden rounded-[18px] border border-white/12 bg-black shadow-[0_22px_44px_-26px_rgba(0,0,0,.95)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)] sm:w-[236px] sm:min-w-[236px] lg:w-auto lg:min-w-0"
                >
                  {post.thumbnailUrl ? (
                    // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
                    <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-[1.02]" loading="lazy" decoding="async" />
                  ) : null}
                  <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,.06)_0%,transparent_54%,rgba(0,0,0,.65)_72%,rgba(0,0,0,.94)_100%)]" />
                  <span className="absolute left-2.5 top-2.5 grid h-7 min-w-7 place-items-center rounded-md bg-black/62 px-1.5 text-[11px] font-black tabular-nums text-white/88 backdrop-blur-sm">{index + 1}</span>
                  <span className="absolute right-2.5 top-2.5 rounded-md bg-black/62 px-2 py-1 text-[9px] font-black uppercase tracking-[0.1em] text-white/72 backdrop-blur-sm">{activeFeedId === 'all' ? post.feedName : (post.latestCheckpoint || 'latest').toUpperCase()}</span>
                  <div className="absolute inset-x-0 bottom-0 flex min-h-[112px] flex-col justify-end p-3.5">
                    <div className="fm-depth-title text-[28px] font-black tabular-nums leading-[0.9] tracking-[-0.04em] text-white sm:text-[30px]">{display.value}</div>
                    <div className="mt-2 text-[10px] font-black text-white/78">@{normalizeHandle(post.handle) || post.feedName.toLowerCase()}</div>
                    <div className="mt-1 text-[10px] leading-[1.3] text-white/54 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2] overflow-hidden">{display.label} · {(post.latestCheckpoint || 'latest').toUpperCase()} read</div>
                  </div>
                </motion.a>
              );
            })}
            {!cards.length ? <div className="flex h-[300px] w-full items-center justify-center text-center text-[10px] font-black uppercase tracking-[0.16em] text-white/28">{view === 'feats' ? 'No contextual facts in this scope' : 'No tracked posts in this scope'}</div> : null}
          </motion.div>
        </AnimatePresence>
      </div>
    </section>
  );
}

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
    <span className={cn('inline-flex shrink-0 items-center gap-1 text-[8px] font-black tabular-nums tracking-[0.08em]', up ? 'text-[var(--fm-accent-bright)]' : 'text-white/36')}>
      <span className="text-[7px]">{up ? '▲' : '▼'}</span>
      {percentLabel(post.firstPercentile)}%→{percentLabel(post.latestPercentile)}%
    </span>
  );
}

function PostRowExpansion({ post, onCollapse }: { post: TrackedPost; onCollapse: () => void }) {
  const reduce = Boolean(useReducedMotion());
  const metrics = [
    { label: 'Likes', value: compactNumber(numeric(post.likes)), multiple: post.likesMultiple },
    { label: 'Comments', value: compactNumber(numeric(post.comments)), multiple: post.commentsMultiple },
    { label: 'Eng. rate', value: formatEngagementRate(post.engagementRate), multiple: post.engagementRateMultiple },
  ];
  return (
    <motion.div
      data-testid="lead-v3-post-expansion"
      initial={reduce ? false : { height: 0, opacity: 0 }}
      animate={{ height: 'auto', opacity: 1 }}
      exit={{ height: 0, opacity: 0 }}
      transition={reduce ? { duration: 0 } : { height: { duration: 0.4, ease: SOFT_EASE }, opacity: { duration: 0.24, delay: 0.05 } }}
      className="overflow-hidden"
    >
      <div onClick={onCollapse} className="relative cursor-pointer border-t border-white/[0.1] bg-[#0a0a0a] p-4 sm:p-6">
        <div className="pointer-events-none absolute inset-y-0 left-0 w-1 bg-[var(--fm-accent)] shadow-[0_0_34px_rgb(var(--fm-accent-rgb)/.48)]" />
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
            className="relative mx-auto block aspect-[4/5] w-[62%] max-w-[200px] overflow-hidden rounded-[20px] border border-white/12 bg-black shadow-[0_26px_50px_-28px_rgba(0,0,0,.95)] sm:mx-0 sm:w-full"
          >
            {post.thumbnailUrl ? (
              // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
              <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" loading="lazy" decoding="async" />
            ) : null}
            <div className="absolute inset-0 bg-[linear-gradient(180deg,transparent_40%,rgba(0,0,0,.85))]" />
            <span className="absolute bottom-3 left-3 text-[12px] font-black text-white">{post.latestPercentile == null ? '—' : `TOP ${percentLabel(post.latestPercentile)}%`}</span>
          </a>
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1.5 text-[8px] font-black uppercase tracking-[0.16em] text-white/30">
              <span>{post.mediaType} · {postDateLabel(post)}</span>
              {post.firstCheckpoint && post.firstPercentile != null ? (
                <span className="text-white/44">
                  {post.firstCheckpoint} top {percentLabel(post.firstPercentile)}% → {(post.latestCheckpoint || '').toUpperCase()} top {percentLabel(post.latestPercentile)}%
                </span>
              ) : (
                <span>{(post.latestCheckpoint || '').toUpperCase()} read</span>
              )}
            </div>
            <dl className="mt-4 grid grid-cols-3 gap-4">
              {metrics.map((metric) => (
                <div key={metric.label} className="min-w-0">
                  <dt className="truncate text-[8px] font-black uppercase tracking-[0.18em] text-white/30">{metric.label}</dt>
                  <dd className="fm-depth-title mt-2 truncate text-[22px] font-black tabular-nums leading-none text-white sm:text-[26px]"><SlotText value={metric.value} /></dd>
                  <div className="mt-2 text-[8px] font-black tabular-nums text-white/34">{metric.multiple == null ? '—' : `${formatMultiple(metric.multiple)} usual`}</div>
                </div>
              ))}
            </dl>
          </div>
        </div>
      </div>
    </motion.div>
  );
}

function FeederboardPostRows({ posts, days, selectedKey, onSelect }: { posts: TrackedPost[]; days: number; selectedKey: string; onSelect: (key: string) => void }) {
  const reduce = Boolean(useReducedMotion());
  const ranked = [...posts].sort(bestFirst).slice(0, 10);

  return (
    <section aria-labelledby="lead-v3-postladder-title" className="min-w-0 max-w-full overflow-hidden bg-[#070707]">
      <div className="flex items-center justify-between gap-4 border-b border-white/[0.08] px-4 py-4 sm:px-5">
        <div>
          <h2 id="lead-v3-postladder-title" className="text-[10px] font-black uppercase tracking-[0.2em] text-[var(--fm-accent-bright)]">Feederboard · posts</h2>
          <p className="mt-1 text-[9px] font-bold text-white/28">This account&apos;s posts compete · tap a row for the receipts</p>
        </div>
        <div className="shrink-0 text-right text-[8px] font-black uppercase tracking-[0.16em] text-white/24">{days}D · lower is stronger</div>
      </div>

      <LayoutGroup id="lead-v3-postladder">
        <div>
          {ranked.map((post, index) => {
            const rank = index + 1;
            const selected = post.postKey === selectedKey;
            const throne = rank === 1;
            return (
              <motion.div
                key={post.postKey}
                layout="position"
                initial={reduce ? false : { y: 22, opacity: 0 }}
                animate={{ y: 0, opacity: 1 }}
                exit={reduce
                  ? { opacity: 0, transition: { duration: 0.12 } }
                  : { y: -16, opacity: 0, transition: { duration: 0.38, ease: SOFT_EASE, delay: (rank - 1) * 0.04 } }}
                transition={reduce
                  ? { layout: LADDER_SPRING, duration: 0.12 }
                  : { layout: LADDER_SPRING, duration: 0.68, ease: SOFT_EASE, delay: 0.08 + (rank - 1) * 0.08 }}
                className="border-b border-white/[0.07] last:border-b-0"
              >
                <button
                  type="button"
                  onClick={() => onSelect(selected ? '' : post.postKey)}
                  aria-expanded={selected}
                  data-testid={`lead-v3-post-row-${rank}`}
                  className={cn(
                    'relative isolate block w-full overflow-hidden text-left transition-colors duration-300 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]',
                    throne
                      ? 'bg-[radial-gradient(circle_at_0_0,rgb(var(--fm-accent-rgb)/.14),transparent_44%),linear-gradient(90deg,rgba(255,255,255,.035),rgba(255,255,255,.01))]'
                      : selected ? 'bg-white/[0.03]' : 'hover:bg-white/[0.022]',
                  )}
                >
                  {throne ? <span className="absolute inset-y-0 left-0 z-20 w-1 bg-[var(--fm-accent)] shadow-[0_0_26px_rgb(var(--fm-accent-rgb)/.46)]" /> : null}
                  <div className={cn(
                    'relative z-10 grid items-center gap-2.5 px-3 py-3 sm:gap-3 sm:px-4',
                    throne
                      ? 'min-h-[100px] grid-cols-[34px_58px_minmax(0,1fr)_auto] sm:grid-cols-[42px_67px_minmax(0,1fr)_auto]'
                      : 'min-h-[76px] grid-cols-[34px_45px_minmax(0,1fr)_auto] sm:grid-cols-[42px_45px_minmax(0,1fr)_auto]',
                  )}>
                    <span className="relative h-full">
                      <span className={cn(
                        'pointer-events-none absolute left-0 top-1/2 -translate-y-1/2 select-none font-black tabular-nums leading-none tracking-[-0.08em]',
                        throne ? 'text-[54px] text-[var(--fm-accent)]/[0.16]' : 'text-[44px] text-white/[0.055]',
                      )}>{String(rank).padStart(2, '0')}</span>
                    </span>
                    <span className={cn(
                      'relative block shrink-0 overflow-hidden rounded-[14px] border border-white/12 bg-[#101010] shadow-[0_14px_28px_-16px_rgba(0,0,0,.9)]',
                      throne ? 'h-[72px] w-[58px] rounded-[16px] sm:h-[84px] sm:w-[67px]' : 'h-[56px] w-[45px]',
                    )}>
                      <CrossfadeImage src={post.thumbnailUrl} />
                    </span>
                    <span className="min-w-0">
                      <span className={cn('block truncate font-black capitalize leading-none tracking-[-0.02em] text-white', throne ? 'text-[17px]' : 'text-[15px]')}>{post.mediaType} · {postDateLabel(post)}</span>
                      <span className="mt-1.5 flex min-w-0 items-center gap-1.5">
                        <span className="truncate text-[8px] font-black uppercase tracking-[0.13em] text-white/34">{(post.latestCheckpoint || '').toUpperCase()} read</span>
                        <span className="text-white/16">·</span>
                        <ClimbChip post={post} />
                      </span>
                    </span>
                    <span className="flex items-center gap-2 text-right">
                      <span className="min-w-0">
                        <span className="block text-[7px] font-black uppercase tracking-[0.15em] text-white/26">Placed</span>
                        <span className={cn('fm-depth-title mt-1 block font-black tabular-nums leading-none text-white', throne ? 'text-[32px]' : 'text-[24px]')}>
                          <SlotText value={percentLabel(post.latestPercentile)} />
                          {post.latestPercentile == null ? null : <i className="ml-0.5 not-italic text-[0.5em] text-[var(--fm-accent-bright)]">%</i>}
                        </span>
                        <span className="mt-1 block text-[9px] font-black tabular-nums text-[var(--fm-accent-bright)]/80"><SlotText value={formatMultiple(post.rankingMultiple)} /> <i className="not-italic text-[7px] uppercase tracking-[0.1em] text-white/22">usual</i></span>
                      </span>
                      <ChevronDown className={cn('h-4 w-4 shrink-0 text-white/36 transition-transform duration-300', selected && 'rotate-180 text-white')} />
                    </span>
                  </div>
                </button>
                <AnimatePresence initial={false} mode="popLayout">
                  {selected && <PostRowExpansion key={post.postKey} post={post} onCollapse={() => onSelect('')} />}
                </AnimatePresence>
              </motion.div>
            );
          })}
          {!ranked.length ? (
            <div className="flex h-[140px] items-center justify-center text-[9px] font-black uppercase tracking-[0.18em] text-white/26">No tracked posts in this window</div>
          ) : null}
        </div>
      </LayoutGroup>
    </section>
  );
}

/* ── Post mortem — the recent drops, tap through the stack ───────────────── */

type StackCard = {
  id: string;
  feedName: string;
  handle: string;
  profilePicUrl: string | null;
  thumbnailUrl: string | null;
  checkpoint: string;
  topPercent: number | null;
  rankingMetric: string | null;
  rankingMultiple: number | null;
  likes: number | null;
  comments: number | null;
  engagementRate: number | null;
};

function stackMetric(card: StackCard) {
  if (card.rankingMetric === 'comments') return { label: 'comments', value: compactNumber(numeric(card.comments)) };
  if (card.rankingMetric === 'engagement_rate') return { label: 'eng. rate', value: formatEngagementRate(card.engagementRate) };
  return { label: 'likes', value: compactNumber(numeric(card.likes)) };
}

function PostStackCardFace({ card, overlay, active, reduce }: { card: StackCard; overlay: number; active: boolean; reduce: boolean }) {
  const metric = stackMetric(card);
  const imageUrl = card.thumbnailUrl || card.profilePicUrl;
  return (
    <>
      {imageUrl ? (
        // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
        <img src={imageUrl} alt="" className="absolute inset-0 h-full w-full rounded-[28px] object-cover" loading="lazy" decoding="async" />
      ) : (
        <div className="absolute inset-0 flex items-center justify-center rounded-[28px] bg-[#101010]">
          <span className="h-24 w-24 rounded-full border border-white/10 p-1">
            <FeederStoryAvatar feeder={{ handle: card.handle, profilePicUrl: card.profilePicUrl }} className="text-[24px]" />
          </span>
        </div>
      )}
      <motion.div aria-hidden="true" className="absolute inset-0 bg-black" initial={false} animate={{ opacity: overlay }} transition={reduce ? { duration: 0 } : { duration: 0.86, ease: CARD_STACK_EASE }} />
      <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,.03),rgba(0,0,0,.84)_78%)]" />
      <motion.div
        className="relative z-10 flex h-full flex-col justify-between p-4 text-white sm:p-5"
        initial={false}
        animate={{ opacity: active ? 1 : 0.34, y: active ? 0 : 10 }}
        transition={reduce ? { duration: 0 } : { duration: 0.32, ease: SOFT_EASE }}
      >
        <div className="flex items-start justify-between gap-3">
          <span className="rounded-full border border-white/10 bg-black/28 px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] backdrop-blur-md">{card.feedName}</span>
          <span className="rounded-full bg-[var(--fm-accent)] px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.12em] text-white">{card.checkpoint || 'D3'}</span>
        </div>
        <div>
          <div className="text-[64px] font-black leading-[0.78] tracking-[-0.06em] sm:text-[70px]">
            {card.topPercent == null ? '—' : Math.max(1, Math.round(card.topPercent))}
            {card.topPercent != null && <span className="ml-1 text-[0.34em] tracking-normal text-[var(--fm-accent-bright)]">%</span>}
          </div>
          <div className="mt-2 truncate text-[18px] font-black leading-none">@{normalizeHandle(card.handle)}</div>
          <div className="mt-4 grid grid-cols-2 gap-2">
            <span className="rounded-[16px] border border-white/10 bg-black/26 p-3 backdrop-blur-md">
              <span className="block text-[21px] font-black leading-none">{metric.value}</span>
              <span className="mt-1 block text-[8px] font-black uppercase tracking-[0.13em] text-white/48">{metric.label}</span>
            </span>
            <span className="rounded-[16px] border border-white/10 bg-black/26 p-3 backdrop-blur-md">
              <span className="block text-[21px] font-black leading-none">{formatMultiple(card.rankingMultiple)}</span>
              <span className="mt-1 block text-[8px] font-black uppercase tracking-[0.13em] text-white/48">vs usual</span>
            </span>
          </div>
        </div>
      </motion.div>
    </>
  );
}

type StackExit = { index: number; key: number };

function PostCardStack({ cards, activeIndex, onAdvance }: { cards: StackCard[]; activeIndex: number; onAdvance: () => void }) {
  const reduce = Boolean(useReducedMotion());
  const visibleCards = cards.slice(0, 5);
  const count = visibleCards.length;
  const signature = visibleCards.map((card) => card.id).join('|');
  const [state, setState] = useState<{ signature: string; exits: StackExit[]; exitKey: number }>({ signature: '', exits: [], exitKey: 0 });
  const activeCardIndex = count > 0 ? activeIndex % count : 0;
  const exitingCards = !reduce && count > 0 && state.signature === signature
    ? state.exits.map((exit) => ({ ...exit, index: exit.index % count, card: visibleCards[exit.index % count] }))
    : [];
  const latestExitingIndex = exitingCards.at(-1)?.index ?? null;

  const advance = () => {
    if (count <= 1) return;
    setState((current) => {
      const exitKey = current.signature === signature ? current.exitKey + 1 : 1;
      const exits = current.signature === signature ? current.exits : [];
      return { signature, exits: [...exits, { index: activeCardIndex, key: exitKey }].slice(-3), exitKey };
    });
    onAdvance();
  };

  useEffect(() => {
    if (reduce || state.signature !== signature || state.exits.length === 0) return undefined;
    const exitKey = state.exitKey;
    const timeout = window.setTimeout(() => {
      setState((current) => current.exitKey === exitKey ? { ...current, exits: [] } : current);
    }, CARD_MASK_CLEANUP_MS);
    return () => window.clearTimeout(timeout);
  }, [reduce, signature, state.exitKey, state.exits.length, state.signature]);

  if (!count) {
    return <div className="flex h-[320px] items-center justify-center rounded-[28px] border border-dashed border-white/10 text-[9px] font-black uppercase tracking-[0.18em] text-white/24">No tracked posts in this window</div>;
  }

  return (
    <div className="relative min-w-0">
      <div className="pointer-events-none absolute inset-x-3 top-3 z-[70] flex items-center justify-between gap-3 sm:static sm:pointer-events-auto sm:mb-3 sm:px-0">
        <div>
          <div className="hidden text-[9px] font-black uppercase tracking-[0.2em] text-white/30 sm:block">Latest drops</div>
          <div className="text-[11px] font-black leading-none text-white/76 sm:mt-1 sm:text-[13px]">Tap to move the stack</div>
        </div>
        <motion.span key={activeCardIndex} initial={reduce ? false : { y: 6, opacity: 0 }} animate={{ y: 0, opacity: 1 }} transition={{ duration: reduce ? 0.15 : 0.22, ease: SOFT_EASE }} className="rounded-full border border-white/10 bg-black/42 px-2.5 py-1 text-[9px] font-black tracking-[0.13em] text-white/66 backdrop-blur-sm sm:bg-white/[0.05] sm:text-white/44">{activeCardIndex + 1}/{count}</motion.span>
      </div>

      <motion.button
        type="button"
        onClick={advance}
        whileTap={reduce ? undefined : { scale: 0.986 }}
        transition={{ duration: 0.16, ease: SOFT_EASE }}
        className="relative block h-[390px] w-full touch-manipulation overflow-hidden rounded-[28px] bg-transparent text-left outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)] sm:h-[430px] lg:h-[460px]"
        aria-label="Advance the post mortem stack"
        data-testid="lead-v3-postmortem-stack"
      >
        <motion.span aria-hidden="true" className="absolute bottom-4 left-[5%] h-14 w-[82%] rounded-full bg-black/62 blur-2xl" animate={reduce ? undefined : { opacity: [0.2, 0.34, 0.2], scaleX: [0.92, 1, 0.92] }} transition={{ duration: 1.6, repeat: Infinity, ease: SOFT_EASE }} />
        {visibleCards.map((card, index) => {
          const order = (index - activeCardIndex + count) % count;
          const isFront = order === 0;
          const coveredByExit = latestExitingIndex === index && index !== activeCardIndex;
          const pose = order === 0
            ? { x: '0%', y: 0, scale: 1, opacity: 1, zIndex: 40, overlay: 0 }
            : order === 1
              ? { x: '26%', y: 0, scale: 0.72, opacity: 0.94, zIndex: 30, overlay: 0.34 }
              : order === 2
                ? { x: '52%', y: 28, scale: 0.64, opacity: 0.8, zIndex: 24, overlay: 0.5 }
                : order === 3
                  ? { x: '76%', y: 56, scale: 0.56, opacity: 0.54, zIndex: 16, overlay: 0.64 }
                  : { x: '88%', y: 56, scale: 0.54, opacity: 0, zIndex: 8, overlay: 0.7 };
          const transition = reduce
            ? { duration: 0 }
            : coveredByExit
              ? { duration: 0 }
              : exitingCards.length
                ? { duration: CARD_MASK_DURATION, ease: CARD_MASK_EASE }
                : { duration: isFront ? 0.72 : 0.96, ease: CARD_STACK_EASE };
          return (
            <motion.div
              key={card.id}
              data-stack-order={order}
              className={cn('absolute bottom-1.5 left-0 top-1.5 aspect-[4/5] origin-center overflow-hidden rounded-[28px] border bg-[#0b0b0b] shadow-[0_34px_78px_-36px_rgba(0,0,0,.98)]', coveredByExit ? 'border-transparent' : 'border-white/12')}
              initial={false}
              animate={{ x: pose.x, y: pose.y, scale: pose.scale, opacity: coveredByExit ? 0 : pose.opacity, zIndex: pose.zIndex }}
              transition={transition}
              style={{ pointerEvents: isFront ? 'auto' : 'none' }}
            >
              <PostStackCardFace card={card} overlay={pose.overlay} active={isFront} reduce={reduce} />
            </motion.div>
          );
        })}
        {exitingCards.map(({ card, key }) => (
          <motion.div
            key={`exit:${key}:${card.id}`}
            className="absolute bottom-1.5 left-0 top-1.5 aspect-[4/5] origin-center overflow-hidden rounded-[28px] border border-transparent bg-[#0b0b0b]"
            initial={{ x: '0%', y: 0, scale: 1, opacity: 1, zIndex: 60 }}
            animate={{ x: '-104%', y: 0, scale: 1, opacity: 1, zIndex: 60 }}
            transition={{ duration: CARD_MASK_DURATION, ease: CARD_MASK_EASE }}
            style={{ pointerEvents: 'none' }}
          >
            <PostStackCardFace card={card} overlay={0} active reduce={reduce} />
          </motion.div>
        ))}
      </motion.button>
    </div>
  );
}

type PostMortemStats = { posts: number; best: number | null; peak: number | null };

const POST_MORTEM_SLOT_CSS = `
.fm-pm-slot{position:relative;display:inline-flex;height:1.08em;flex:none;justify-content:center;overflow:hidden;line-height:1;vertical-align:baseline}
.fm-pm-slot__sizer{visibility:hidden;white-space:pre}
.fm-pm-slot__face{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;white-space:pre}
.fm-pm-slot__face{will-change:transform,opacity}
.fm-pm-slot--leave .fm-pm-slot__face{animation:fm-pm-slot-old 640ms cubic-bezier(.32,.72,0,1) both;animation-delay:var(--pm-slot-delay,0ms)}
.fm-pm-slot--enter .fm-pm-slot__face{animation:fm-pm-slot-new 640ms cubic-bezier(.32,.72,0,1) both;animation-delay:calc(var(--pm-slot-delay,0ms) + 40ms)}
@keyframes fm-pm-slot-old{0%{opacity:1;transform:translateY(0)}100%{opacity:0;transform:translateY(112%)}}
@keyframes fm-pm-slot-new{0%{opacity:0;transform:translateY(-112%)}100%{opacity:1;transform:translateY(0)}}
@media (prefers-reduced-motion:reduce){.fm-pm-slot__face{animation:none!important;transform:translateY(0)!important}}
`;

function postMortemHeadlineSize(value: string) {
  if (value.length > 76) return 'text-[26px] sm:text-[31px] lg:text-[36px]';
  if (value.length < 49) return 'text-[34px] sm:text-[39px] lg:text-[46px]';
  return 'text-[30px] sm:text-[35px] lg:text-[41px]';
}

/* Whole words roll, not characters \u2014 a sentence changing glyph-by-glyph reads
   frantic; word slots keep the same mechanism but feel deliberate. */
function PostMortemSentenceLayer({ value, revision, leaving }: { value: string; revision: number; leaving: boolean }) {
  const tokens = value.split(/(\s+)/).filter(Boolean);
  return <span aria-hidden="true" className="absolute inset-0 flex flex-wrap content-start">{tokens.map((token, tokenIndex) => {
    if (/^\s+$/.test(token)) return <span key={`${revision}:${tokenIndex}:space`} className="whitespace-pre">{token}</span>;
    const wordIndex = tokens.slice(0, tokenIndex).filter((previousToken) => !/^\s+$/.test(previousToken)).length;
    const delay = Math.min(380, wordIndex * 42);
    return (
      <span key={`${revision}:${tokenIndex}:${token}`} className={cn('fm-pm-slot', leaving ? 'fm-pm-slot--leave' : 'fm-pm-slot--enter')} style={{ '--pm-slot-delay': `${delay}ms` } as CSSProperties}>
        <span className="fm-pm-slot__sizer">{token}</span>
        <span className="fm-pm-slot__face">{token}</span>
      </span>
    );
  })}</span>;
}

function PostMortemStatement({ value, reduce }: { value: string; reduce: boolean }) {
  const [slotText, setSlotText] = useState({ previous: value, current: value, revision: 0 });

  useEffect(() => {
    if (slotText.current === value) return;
    const timeout = window.setTimeout(() => {
      setSlotText((current) => current.current === value ? current : { previous: current.current, current: value, revision: current.revision + 1 });
    }, reduce ? 0 : 30);
    return () => window.clearTimeout(timeout);
  }, [reduce, slotText, value]);

  useEffect(() => {
    if (slotText.previous === slotText.current) return;
    const timeout = window.setTimeout(() => {
      setSlotText((current) => current.revision === slotText.revision ? { ...current, previous: current.current } : current);
    }, reduce ? 150 : 1120);
    return () => window.clearTimeout(timeout);
  }, [reduce, slotText]);

  const changing = slotText.previous !== slotText.current;
  return (
    <div className="relative mt-3 h-[116px] max-w-full sm:h-[120px] lg:h-[192px]">
      <style>{POST_MORTEM_SLOT_CSS}</style>
      <h2 id="lead-v3-postmortem-title" aria-label={slotText.current} className={cn('fm-depth-title absolute inset-0 font-black leading-[0.96] tracking-[-0.055em] text-white', postMortemHeadlineSize(slotText.current))}>
        {reduce ? <motion.span key={slotText.current} initial={{ opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.15 }}>{slotText.current}</motion.span> : <>
          {changing ? <PostMortemSentenceLayer value={slotText.previous} revision={slotText.revision} leaving /> : null}
          <PostMortemSentenceLayer value={slotText.current} revision={slotText.revision} leaving={false} />
        </>}
      </h2>
    </div>
  );
}

function PostMortem({ cards, deck, stats, days }: { cards: StackCard[]; deck: PostMortemItem[]; stats: PostMortemStats; days: number }) {
  const { posts: totalPosts, best, peak } = stats;
  const reduce = Boolean(useReducedMotion());
  const [activeIndex, setActiveIndex] = useState(0);
  const activeCardIndex = deck.length ? activeIndex % deck.length : 0;
  const active = deck[activeCardIndex];
  const scope = active?.handle ? `@${normalizeHandle(active.handle)}` : active?.feedName || 'FEED';
  const advance = () => setActiveIndex((current) => deck.length ? (current + 1) % deck.length : 0);
  const callToAction = (
    <Link href="/fire" data-testid="lead-v3-postmortem-link" className="group inline-flex items-center gap-2.5 rounded-full border border-white/12 bg-black/30 px-4 py-2.5 transition-colors hover:border-[var(--fm-accent)]/50 hover:text-white">
      <Flame className="h-4 w-4 text-[var(--fm-accent-bright)]" />
      <span className="text-[9px] font-black uppercase tracking-[0.15em] text-white/64 transition-colors group-hover:text-white">Open the wall · day by day</span>
      <ChevronRight className="h-3.5 w-3.5 text-white/36 transition-transform group-hover:translate-x-0.5" />
    </Link>
  );
  return (
    <section aria-labelledby="lead-v3-postmortem-title" className="relative isolate min-h-[500px] overflow-hidden border-t border-white/[0.09] bg-[#060606] p-5 sm:p-7 lg:p-9">
      <span className="pointer-events-none absolute inset-0 scale-105 opacity-[0.13]">
        <CrossfadeImage src={cards[activeCardIndex]?.thumbnailUrl || null} />
      </span>
      <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(90deg,#060606_0%,rgba(6,6,6,.97)_34%,rgba(6,6,6,.74)_68%,rgba(6,6,6,.9))]" />
      <div className="pointer-events-none absolute -bottom-10 left-2 select-none text-[180px] font-black leading-none tracking-[-0.08em] text-white/[0.025] sm:text-[260px]">PM</div>

      <div className="relative grid gap-3 lg:grid-cols-[minmax(480px,46fr)_minmax(0,54fr)] lg:items-center lg:gap-[clamp(56px,5vw,80px)]">
        <div className="relative min-w-0">
          <div className="text-[9px] font-black uppercase tracking-[0.22em] text-white/48">Post Mortem · {scope} · {days}D</div>
          {active ? (
            <>
              <motion.span key={active.tag} initial={reduce ? { opacity: 0 } : { opacity: 0, y: -4 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: reduce ? 0.15 : 0.16, ease: SOFT_EASE }} className="mt-3 inline-flex rounded-full border border-[var(--fm-accent-bright)]/32 bg-[var(--fm-accent)]/16 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.15em] text-[var(--fm-accent-bright)]">{active.tag}</motion.span>
              <PostMortemStatement value={active.statement} reduce={reduce} />
              {active.proof ? <motion.p key={active.proof} initial={reduce ? { opacity: 0 } : { opacity: 0, y: 4 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: reduce ? 0.15 : 0.18, delay: reduce ? 0 : 0.16, ease: SOFT_EASE }} className="max-w-[32rem] text-[13px] font-black leading-[1.25] text-white/58">{active.proof}</motion.p> : null}
            </>
          ) : <p className="mt-5 text-[14px] font-black text-white/52">No proven trigger in this window.</p>}

          <div className="mt-4 flex flex-wrap items-center gap-x-5 gap-y-2 border-t border-white/[0.09] pt-3 text-[9px] font-black uppercase tracking-[0.14em] text-white/32 lg:mt-7 lg:pt-4">
            <span><b className="mr-1 text-[15px] text-white">{compactNumber(totalPosts)}</b> posts</span>
            <span><b className="mr-1 text-[15px] text-white">{best == null ? '—' : `${percentLabel(best)}%`}</b> best</span>
            <span><b className="mr-1 text-[15px] text-white">{formatMultiple(peak)}</b> peak</span>
            <span>{days}D</span>
          </div>

          <div className="mt-6 hidden lg:block">{callToAction}</div>
        </div>
        <div className="mt-2 w-full min-w-0 lg:mt-0 lg:justify-self-end lg:max-w-[680px]">
          <PostCardStack cards={cards} activeIndex={activeCardIndex} onAdvance={advance} />
          <div className="mt-5 lg:hidden">{callToAction}</div>
        </div>
      </div>
    </section>
  );
}

/* ── States + page ───────────────────────────────────────────────────────── */

function LoadingState() {
  return (
    <div className="flex min-h-[55vh] items-center justify-center">
      <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="text-center">
        <motion.span className="mx-auto block h-2 w-2 rounded-full bg-[var(--fm-accent)]" animate={{ opacity: [0.25, 1, 0.25], scale: [0.8, 1, 0.8] }} transition={{ duration: 1.4, repeat: Infinity }} />
        <div className="mt-4 text-[9px] font-black uppercase tracking-[0.22em] text-white/30">Building the board</div>
      </motion.div>
    </div>
  );
}

export default function LeadBoardPage() {
  const reduce = Boolean(useReducedMotion());
  const pageScrollRef = useRef<HTMLDivElement | null>(null);
  const { appShellStyle, useBrowserPageScroll, useTranslucentBrowserChrome } = useMobileImmersiveViewport();
  const [days, setDays] = useState<(typeof TIMEFRAMES)[number]>(30);
  const [feeds, setFeeds] = useState<RuntimeFeed[]>([]);
  const [activeFeedId, setActiveFeedId] = useState('all');
  const [activeFeeder, setActiveFeeder] = useState('');
  const [selectedId, setSelectedId] = useState('');
  const [selectedPostKey, setSelectedPostKey] = useState('');
  const [feederboardFacts, setFeederboardFacts] = useState<FeederboardFact[]>([]);
  const [postMortemDeck, setPostMortemDeck] = useState<PostMortemItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [timeframePending, setTimeframePending] = useState(false);
  const [dealId, setDealId] = useState(0);
  const [error, setError] = useState('');
  const dealSignatureRef = useRef('');
  const headerCompressed = useCompressedOnScroll(pageScrollRef, useBrowserPageScroll, {
    collapseDistance: 150,
    expandDistance: 64,
    topGuard: 30,
  });

  usePageReady(true);

  useEffect(() => {
    document.title = 'Lead | FeedMe';
  }, []);

  useEffect(() => {
    const controller = new AbortController();
    let cancelled = false;
    setLoading(true);
    setError('');

    async function load() {
      try {
        const feedResponse = await fetch('/api/feed', { cache: 'no-store', credentials: 'include', signal: controller.signal });
        if (!feedResponse.ok) throw new Error(feedResponse.status === 401 ? 'Sign in to load this board.' : 'Feed data could not be loaded.');
        const feedPayload = await feedResponse.json() as { feeds?: ApiFeed[] };
        const apiFeeds = (feedPayload.feeds || []).filter((feed) => Boolean(feed.id));
        const runtimeFeeds = await Promise.all(apiFeeds.map(async (feed) => {
          // Double window: the selected span plus the equal span before it —
          // every level on the page is read against that prior window.
          const params = new URLSearchParams({ feedId: feed.id, handle: 'all', timeframe: `${days * 2}D` });
          const dashboardParams = new URLSearchParams({ feedId: feed.id, days: String(days * 2) });
          const [postsResponse, dashboardResponse] = await Promise.all([
            fetch(`/api/feed/feeder-posts?${params.toString()}`, { cache: 'no-store', credentials: 'include', signal: controller.signal }),
            fetch(`/api/feed/dashboard?${dashboardParams.toString()}`, { cache: 'no-store', credentials: 'include', signal: controller.signal }),
          ]);
          const postsPayload = postsResponse.ok ? await postsResponse.json() as { posts?: TrackedPost[] } : {};
          const dashboardPayload = dashboardResponse.ok ? await dashboardResponse.json() as { dashboard?: { ascent_series?: AscentPoint[] } } : {};
          return {
            ...feed,
            feeders: Array.isArray(feed.feeders) ? feed.feeders : [],
            posts: Array.isArray(postsPayload.posts) ? postsPayload.posts : [],
            ascent: Array.isArray(dashboardPayload.dashboard?.ascent_series) ? dashboardPayload.dashboard.ascent_series : [],
          } satisfies RuntimeFeed;
        }));
        if (cancelled) return;
        setFeeds(runtimeFeeds);
        setActiveFeedId((current) => current === 'all' || runtimeFeeds.some((feed) => feed.id === current) ? current : 'all');
      } catch (caught) {
        if (controller.signal.aborted || cancelled) return;
        setError(caught instanceof Error ? caught.message : 'Feed data could not be loaded.');
      } finally {
        if (!cancelled) {
          setLoading(false);
          setTimeframePending(false);
        }
      }
    }

    void load();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [days]);

  const rows = useMemo(() => buildRows(feeds, activeFeedId, days), [activeFeedId, days, feeds]);
  const windows = useMemo(() => growthWindows(feeds, activeFeedId, days, activeFeeder), [activeFeedId, activeFeeder, days, feeds]);
  const leaderCards = useMemo(() => buildLeaderCards(rows, feeds, activeFeedId, days, activeFeeder), [activeFeedId, activeFeeder, days, feeds, rows]);
  const feederPosts = useMemo(() => {
    if (!activeFeeder) return [];
    return scopedPostsFor(feeds, activeFeedId, days).filter((post) => normalizeHandle(post.handle) === activeFeeder);
  }, [activeFeedId, activeFeeder, days, feeds]);
  const feederboardScopePosts = useMemo(
    () => scopedPostsFor(feeds, activeFeedId, days).filter((post) => !activeFeeder || normalizeHandle(post.handle) === activeFeeder),
    [activeFeedId, activeFeeder, days, feeds],
  );

  useEffect(() => {
    const controller = new AbortController();
    if (!feederboardScopePosts.length) {
      setFeederboardFacts([]);
      return () => controller.abort();
    }
    void fetch('/api/feed/feederboard', {
      method: 'POST',
      cache: 'no-store',
      credentials: 'include',
      signal: controller.signal,
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ posts: feederboardScopePosts, days }),
    })
      .then(async (response) => response.ok ? response.json() as Promise<{ facts?: FeederboardFact[] }> : { facts: [] })
      .then((payload) => {
        if (!controller.signal.aborted) setFeederboardFacts(Array.isArray(payload.facts) ? payload.facts : []);
      })
      .catch(() => {
        if (!controller.signal.aborted) setFeederboardFacts([]);
      });
    return () => controller.abort();
  }, [days, feederboardScopePosts]);

  useEffect(() => {
    const controller = new AbortController();
    if (!feederboardScopePosts.length) {
      setPostMortemDeck([]);
      return () => controller.abort();
    }
    void fetch('/api/lead/post-mortem', {
      method: 'POST',
      cache: 'no-store',
      credentials: 'include',
      signal: controller.signal,
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ posts: feederboardScopePosts, days }),
    })
      .then(async (response) => response.ok ? response.json() as Promise<{ deck?: PostMortemItem[] }> : { deck: [] })
      .then((payload) => {
        if (!controller.signal.aborted) setPostMortemDeck(Array.isArray(payload.deck) ? payload.deck : []);
      })
      .catch(() => {
        if (!controller.signal.aborted) setPostMortemDeck([]);
      });
    return () => controller.abort();
  }, [days, feederboardScopePosts]);
  const tapeEvents = useMemo(() => buildTapeEvents(feeds, activeFeedId, activeFeeder, days), [activeFeedId, activeFeeder, days, feeds]);
  const wireLine = useMemo(
    () => activeFeeder ? composeFeederWireLine(feederPosts) : composeWireLine(rows),
    [activeFeeder, feederPosts, rows],
  );
  const postMortemStats = useMemo<PostMortemStats>(() => {
    const scoped = activeFeeder ? feederPosts : scopedPostsFor(feeds, activeFeedId, days);
    const best = scoped.reduce<number | null>((min, post) => post.latestPercentile == null ? min : min == null || post.latestPercentile < min ? post.latestPercentile : min, null);
    const peak = scoped.reduce<number | null>((max, post) => post.rankingMultiple == null ? max : max == null || post.rankingMultiple > max ? post.rankingMultiple : max, null);
    return { posts: scoped.length, best, peak };
  }, [activeFeedId, activeFeeder, days, feederPosts, feeds]);
  const stackCards = useMemo(() => postMortemDeck.map((item) => {
    const post = feederboardScopePosts.find((candidate) => candidate.postKey === item.postKey);
    const feed = feeds.find((candidate) => candidate.id === item.feedId || titleCase(candidate.title) === item.feedName);
    const feeder = feed?.feeders.find((candidate) => normalizeHandle(candidate.handle) === normalizeHandle(item.handle));
    return {
      id: item.id,
      feedName: item.feedName,
      handle: item.handle || feeder?.handle || item.feedName,
      profilePicUrl: feeder?.profilePicUrl || null,
      thumbnailUrl: item.thumbnailUrl || post?.thumbnailUrl || feeder?.thumbnailUrl || null,
      checkpoint: post?.latestCheckpoint || '',
      topPercent: post?.latestPercentile ?? null,
      rankingMetric: post?.rankingMetric || null,
      rankingMultiple: post?.rankingMultiple ?? null,
      likes: post?.likes ?? null,
      comments: post?.comments ?? null,
      engagementRate: post?.engagementRate ?? null,
    } satisfies StackCard;
  }), [feederboardScopePosts, feeds, postMortemDeck]);

  const dealSignature = useMemo(() => [
    activeFeedId,
    activeFeeder,
    days,
    rows.map((row) => `${row.id}:${row.rank}:${row.typical}:${row.peak}:${row.followerDelta}`).join('|'),
    leaderCards.map((card) => `${card.key}:${card.value}`).join('|'),
    windows.map((window) => `${window.id}:${window.delta}`).join('|'),
  ].join('::'), [activeFeedId, activeFeeder, days, leaderCards, rows, windows]);

  useEffect(() => {
    if (loading || timeframePending || dealSignatureRef.current === dealSignature) return;
    dealSignatureRef.current = dealSignature;
    setDealId((current) => current + 1);
  }, [dealSignature, loading, timeframePending]);

  useEffect(() => {
    setSelectedId((current) => rows.some((row) => row.id === current) ? current : '');
  }, [rows]);

  const handleFeedChange = (id: string) => {
    if (id === activeFeedId) return;
    setActiveFeedId(id);
    setActiveFeeder('');
    setSelectedId('');
    setSelectedPostKey('');
  };

  return (
    <div
      ref={pageScrollRef}
      data-testid="lead-v3-page"
      className={cn(
        'fm-dashboard-mesh w-full min-w-0 max-w-[100vw] scroll-pb-[calc(126px+env(safe-area-inset-bottom))] scroll-pt-[calc(76px+env(safe-area-inset-top))] text-white',
        useTranslucentBrowserChrome ? 'bg-transparent' : 'bg-[#030303]',
        useBrowserPageScroll
          ? 'min-h-[100lvh] overflow-x-hidden overflow-y-visible'
          : 'h-[var(--fm-app-height,100dvh)] overflow-x-hidden overflow-y-auto overscroll-y-contain [-webkit-overflow-scrolling:touch]',
      )}
      style={appShellStyle}
    >
      <LeadHeader
        feeds={feeds}
        activeFeedId={activeFeedId}
        activeFeeder={activeFeeder}
        days={days}
        compressed={headerCompressed}
        pending={timeframePending}
        onFeedChange={handleFeedChange}
        onFeederChange={(handle) => {
          setActiveFeeder(handle);
          setSelectedId('');
          setSelectedPostKey('');
        }}
        onDaysChange={(nextDays) => {
          if (nextDays === days) return;
          setTimeframePending(true);
          setDays(nextDays);
        }}
      />
      <main className="relative z-10 mx-auto w-full min-w-0 max-w-[1500px] px-3 pb-[calc(148px+env(safe-area-inset-bottom))] pt-[calc(172px+env(safe-area-inset-top))] sm:px-5 sm:pt-[calc(172px+env(safe-area-inset-top))] md:pt-[calc(178px+env(safe-area-inset-top))] lg:px-7 lg:pt-[calc(214px+env(safe-area-inset-top))]">
        {loading && !feeds.length ? <LoadingState /> : error ? (
          <div className="flex min-h-[55vh] items-center justify-center px-6 text-center">
            <div>
              <div className="text-[11px] font-black uppercase tracking-[0.2em] text-white/58">Board unavailable</div>
              <p className="mt-3 text-[12px] font-bold text-white/32">{error}</p>
              <Link href="/" className="mt-6 inline-flex rounded-full border border-white/12 px-4 py-2.5 text-[9px] font-black uppercase tracking-[0.15em] text-white/52 hover:text-white">Return to Feed</Link>
            </div>
          </div>
        ) : rows.length ? (
          <motion.div initial={reduce ? false : { opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.42, ease: SOFT_EASE }} className="min-w-0 max-w-full">
            <LeadTape events={tapeEvents} dealId={dealId} />

            <section aria-labelledby="lead-v3-leaders-title" className="min-w-0 max-w-full">
              <h2 id="lead-v3-leaders-title" className="mb-3 flex items-center gap-2 px-1 text-[10px] font-black uppercase tracking-[0.18em] text-white/76"><span className="h-1.5 w-1.5 rounded-full bg-[var(--fm-accent)] shadow-[0_0_14px_rgb(var(--fm-accent-rgb)/.8)]" />Today&apos;s leaders</h2>
              <LeadersStrip cards={leaderCards} />
            </section>

            <div className="relative mt-4 min-w-0 max-w-full overflow-hidden rounded-[28px] border border-white/[0.09] bg-[#050505] shadow-[0_32px_80px_-46px_rgba(0,0,0,.98)]">
              <WireLine line={wireLine} dealId={dealId} />
              {/* Scope switches re-deal the ladder: the outgoing rows cascade
                  out top-down (each row's own staggered exit) while the new
                  set staggers in — never one monolithic swap. */}
              <AnimatePresence initial={false} mode="popLayout">
                <motion.div
                  key={`${activeFeedId}|${activeFeeder || 'board'}`}
                  className="min-w-0 max-w-full"
                  exit={{ opacity: 0, transition: { duration: 0.32, delay: 0.42, ease: SOFT_EASE } }}
                >
                  {activeFeeder ? (
                    <FeederboardPostRows posts={feederPosts} days={days} selectedKey={selectedPostKey} onSelect={setSelectedPostKey} />
                  ) : (
                    <FeederboardRows rows={rows} selectedId={selectedId} onSelect={setSelectedId} days={days} />
                  )}
                </motion.div>
              </AnimatePresence>
              <FeederboardCards feeds={feeds} activeFeedId={activeFeedId} activeFeeder={activeFeeder} days={days} facts={feederboardFacts} />
              <GrowthWindows windows={windows} days={days} dealId={dealId} />
              <PostMortem cards={stackCards} deck={postMortemDeck} stats={postMortemStats} days={days} />
            </div>
          </motion.div>
        ) : (
          <div className="flex min-h-[55vh] items-center justify-center text-center">
            <div>
              <div className="text-[11px] font-black uppercase tracking-[0.2em] text-white/58">No feeds to compare yet</div>
              <p className="mt-3 text-[12px] font-bold text-white/30">Add feeders in Feed, then this board will fill itself.</p>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
