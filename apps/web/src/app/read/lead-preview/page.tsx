'use client';

import Link from 'next/link';
import { CSSProperties, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import {
  BookOpenText,
  ChevronDown,
  LayoutGrid,
  Trophy,
  Users,
} from 'lucide-react';
import { AnimatePresence, LayoutGroup, motion, useReducedMotion } from 'framer-motion';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { SLOT_CONTAINER, SLOT_ITEM } from '@/lib/motion';
import { useCompressedOnScroll } from '@/lib/useCompressedOnScroll';
import { cn } from '@/lib/utils';

const TIMEFRAMES = [7, 30, 60, 90] as const;
const SOFT_EASE = [0.22, 1, 0.36, 1] as const;
const CARD_STACK_EASE = [0.18, 0.88, 0.28, 1] as const;
const CARD_MASK_EASE = [0.32, 0.72, 0, 1] as const;
const CARD_MASK_DURATION = 0.92;
const CARD_MASK_CLEANUP_MS = 1040;
const LADDER_SPRING = { type: 'spring', stiffness: 400, damping: 36, mass: 0.9 } as const;

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
  name: string;
  handle: string;
  feedName: string;
  feederCount: number;
  profilePicUrl: string | null;
  thumbnailUrl: string | null;
  topPercent: number | null;
  usual: number | null;
  likes: number;
  comments: number;
  engagementRate: number | null;
  likesMultiple: number | null;
  commentsMultiple: number | null;
  engagementRateMultiple: number | null;
  followers: number;
  followerDelta: number | null;
  postCount: number;
  posts: TrackedPost[];
  proofPosts: TrackedPost[];
};

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

function normalizeHandle(value: string | null | undefined) {
  return String(value || '').trim().replace(/^@+/, '').toLowerCase();
}

function titleCase(value: string) {
  return value
    .toLowerCase()
    .replace(/(^|\s|[-_/])\w/g, (character) => character.toUpperCase());
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

function createBoardRow({
  id,
  name,
  handle,
  feedName,
  feederCount,
  profilePicUrl,
  thumbnailUrl,
  followers,
  followerDelta,
  posts,
  fallbackMetrics,
}: {
  id: string;
  name: string;
  handle: string;
  feedName: string;
  feederCount: number;
  profilePicUrl: string | null;
  thumbnailUrl: string | null;
  followers: number;
  followerDelta: number | null;
  posts: TrackedPost[];
  fallbackMetrics?: MetricTotals;
}): BoardRow {
  const sortedPosts = [...posts].sort(bestFirst);
  const proofPosts = sortedPosts.filter((post) => post.thumbnailUrl).slice(0, 4);
  const percentileValues = posts
    .map((post) => post.latestPercentile)
    .filter((value): value is number => value != null && Number.isFinite(value));
  const multipleValues = posts
    .map((post) => post.rankingMultiple)
    .filter((value): value is number => value != null && Number.isFinite(value));

  return {
    id,
    rank: 0,
    name,
    handle,
    feedName,
    feederCount,
    profilePicUrl,
    thumbnailUrl: proofPosts[0]?.thumbnailUrl || thumbnailUrl,
    topPercent: percentileValues.length ? Math.min(...percentileValues) : null,
    usual: multipleValues.length ? Math.max(...multipleValues) : null,
    likes: posts.length ? posts.reduce((sum, post) => sum + numeric(post.likes), 0) : numeric(fallbackMetrics?.likes),
    comments: posts.length ? posts.reduce((sum, post) => sum + numeric(post.comments), 0) : numeric(fallbackMetrics?.comments),
    engagementRate: median(posts.map((post) => post.engagementRate)),
    likesMultiple: median(posts.map((post) => post.likesMultiple)),
    commentsMultiple: median(posts.map((post) => post.commentsMultiple)),
    engagementRateMultiple: median(posts.map((post) => post.engagementRateMultiple)),
    followers,
    followerDelta,
    postCount: posts.length || numeric(fallbackMetrics?.postsTracked),
    posts: sortedPosts,
    proofPosts,
  };
}

function rankRows(rows: BoardRow[]) {
  return [...rows]
    .sort((a, b) => {
      const percentile = (a.topPercent ?? 101) - (b.topPercent ?? 101);
      if (percentile !== 0) return percentile;
      const multiple = (b.usual ?? 0) - (a.usual ?? 0);
      if (multiple !== 0) return multiple;
      return b.comments - a.comments;
    })
    .slice(0, 10)
    .map((row, index) => ({ ...row, rank: index + 1 }));
}

function buildRows(feeds: RuntimeFeed[], activeFeedId: string, days: number) {
  if (activeFeedId === 'all') {
    return rankRows(feeds.map((feed) => {
      const posts = postsInWindow(feed.posts, days);
      const followers = feed.feeders.reduce((sum, feeder) => sum + numeric(feeder.followerCount), 0);
      const identity = feed.feeders[0];
      return createBoardRow({
        id: `feed:${feed.id}`,
        name: titleCase(feed.title),
        handle: `${feed.feeders.length} ${feed.feeders.length === 1 ? 'feeder' : 'feeders'}`,
        feedName: titleCase(feed.title),
        feederCount: feed.feeders.length,
        profilePicUrl: identity?.profilePicUrl || null,
        thumbnailUrl: identity?.thumbnailUrl || null,
        followers,
        followerDelta: ascentDelta(feed.ascent),
        posts,
        fallbackMetrics: feed.metrics,
      });
    }));
  }

  const feed = feeds.find((candidate) => candidate.id === activeFeedId);
  if (!feed) return [];

  return rankRows(feed.feeders.map((feeder) => {
    const handle = normalizeHandle(feeder.handle);
    const posts = postsInWindow(feed.posts, days).filter((post) => normalizeHandle(post.handle) === handle);
    return createBoardRow({
      id: `feeder:${feed.id}:${handle}`,
      name: `@${handle}`,
      handle: titleCase(feed.title),
      feedName: titleCase(feed.title),
      feederCount: 1,
      profilePicUrl: feeder.profilePicUrl || null,
      thumbnailUrl: feeder.thumbnailUrl || null,
      followers: numeric(feeder.followerCount),
      followerDelta: null,
      posts,
      fallbackMetrics: feeder.metrics,
    });
  }));
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

function growthWindows(feeds: RuntimeFeed[], activeFeedId: string, days: number): GrowthWindow[] {
  const scopedFeeds = activeFeedId === 'all' ? feeds : feeds.filter((feed) => feed.id === activeFeedId);
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
          return time >= start && time < end;
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

function RowIdentity({ row, compact = false }: { row: BoardRow; compact?: boolean }) {
  return (
    <div className={cn('flex min-w-0 items-center', compact ? 'gap-2.5' : 'gap-3')}>
      <span className={cn(
        'relative shrink-0 rounded-full border border-white/18 bg-[#0b0b0b] p-[3px] shadow-[0_0_0_3px_rgba(255,255,255,.025)]',
        compact ? 'h-10 w-10 sm:h-11 sm:w-11' : 'h-11 w-11',
      )}>
        <FeederStoryAvatar feeder={{ handle: row.name, profilePicUrl: row.profilePicUrl }} className={compact ? 'text-[12px]' : 'text-[13px]'} />
      </span>
      <span className="min-w-0 text-left">
        <span className="block truncate text-[15px] font-black leading-none tracking-[-0.02em] text-white sm:text-[17px]">{row.name}</span>
        <span className="mt-1.5 block truncate text-[8px] font-black uppercase tracking-[0.13em] text-white/34">{row.handle}</span>
      </span>
    </div>
  );
}

function LeadAppHeader({
  feeds,
  activeFeedId,
  days,
  compressed,
  onFeedChange,
  onDaysChange,
}: {
  feeds: RuntimeFeed[];
  activeFeedId: string;
  days: (typeof TIMEFRAMES)[number];
  compressed: boolean;
  onFeedChange: (id: string) => void;
  onDaysChange: (days: (typeof TIMEFRAMES)[number]) => void;
}) {
  const activeFeed = feeds.find((feed) => feed.id === activeFeedId) || null;
  const scopeLabel = activeFeed ? titleCase(activeFeed.title) : 'All Feedboards';

  return (
    <div className="pointer-events-none fixed inset-x-0 top-0 z-[160] flex justify-center px-2 pt-[calc(10px+env(safe-area-inset-top))] sm:px-4 sm:pt-[calc(14px+env(safe-area-inset-top))] md:pt-[calc(20px+env(safe-area-inset-top))]">
      <div className="fm-tab-header-shell">
        <motion.header
          data-testid="lead-preview-header"
          className={cn(
            'fm-depth-chrome fm-depth-chrome--header pointer-events-auto px-3.5 py-3 sm:px-4 lg:px-5',
            compressed && 'fm-depth-chrome--header-compressed',
          )}
          style={{
            '--fm-mobile-header-chrome-height': '176px',
            '--fm-desktop-header-chrome-height': '176px',
            '--fm-mobile-header-chrome-compressed-height': '76px',
          } as CSSProperties}
        >
          <div className="relative z-10 flex h-full flex-col gap-2.5">
            <div className="flex h-[52px] min-h-[52px] items-center justify-between gap-2 lg:h-[56px] lg:min-h-[56px]">
              <div className="flex min-w-0 items-center gap-2.5">
                <h1 className="fm-depth-title shrink-0 text-[27px] font-black leading-[0.88] tracking-[0.2em] text-white lg:text-[30px]">LEAD</h1>
                <div className="min-w-0 border-l border-white/10 pl-2.5 max-[360px]:hidden">
                  <div className="text-[7px] font-black uppercase tracking-[0.22em] text-[var(--fm-accent-bright)]">Feederboard</div>
                  <div className="mt-0.5 max-w-[130px] truncate text-[12px] font-black leading-none text-white/80 sm:max-w-[210px] sm:text-[14px]">{scopeLabel}</div>
                </div>
              </div>

              <div className="flex shrink-0 rounded-[14px] border border-white/[0.07] bg-black/38 p-1 shadow-[inset_0_2px_10px_rgba(0,0,0,.38)]">
                {TIMEFRAMES.map((value) => (
                  <button
                    key={value}
                    type="button"
                    onClick={() => onDaysChange(value)}
                    aria-pressed={days === value}
                    data-testid={`timeframe-${value}`}
                    className={cn(
                      'rounded-[10px] px-2 py-1.5 text-[8px] font-black uppercase tracking-[0.12em] transition sm:px-2.5 sm:text-[9px]',
                      days === value
                        ? 'bg-[var(--fm-accent-bright)] text-black shadow-[0_7px_18px_-8px_rgba(251,113,133,.75)]'
                        : 'text-white/36 hover:text-white/72',
                    )}
                  >
                    {value}D
                  </button>
                ))}
              </div>
            </div>

            <motion.div
              initial={false}
              animate={{
                opacity: compressed ? 0 : 1,
                y: compressed ? -12 : 0,
                clipPath: compressed ? 'inset(0 0 100% 0 round 22px)' : 'inset(0 0 0% 0 round 22px)',
              }}
              transition={{ duration: compressed ? 0.2 : 0.36, ease: SOFT_EASE }}
              className="min-w-0 overflow-hidden"
              style={{ pointerEvents: compressed ? 'none' : 'auto' }}
            >
              <div className="flex min-w-0 gap-4 overflow-x-auto pb-1 [scrollbar-width:none] sm:gap-5 [&::-webkit-scrollbar]:hidden">
                <button type="button" onClick={() => onFeedChange('all')} aria-pressed={activeFeedId === 'all'} data-testid="scope-all" className="group shrink-0 text-center focus-visible:outline-none">
                  <span className={cn('mx-auto flex h-12 w-12 items-center justify-center rounded-full border bg-[#111] p-[3px] transition-all', activeFeedId === 'all' ? 'border-[var(--fm-accent-bright)] shadow-[0_0_0_2px_rgba(251,113,133,.2),0_11px_24px_-12px_rgba(251,113,133,.78)]' : 'border-white/12 group-hover:border-white/24')}>
                    <span className="flex h-full w-full items-center justify-center rounded-full bg-[#e8e8e8] text-[9px] font-black text-black">ALL</span>
                  </span>
                  <span className={cn('mt-1.5 block text-[7px] font-black uppercase tracking-[0.14em]', activeFeedId === 'all' ? 'text-[var(--fm-accent-bright)]' : 'text-white/28')}>All</span>
                </button>
                {feeds.map((feed) => {
                  const active = activeFeedId === feed.id;
                  return (
                    <button key={feed.id} type="button" onClick={() => onFeedChange(feed.id)} aria-pressed={active} data-testid={`scope-feed-${feed.id}`} className="group shrink-0 text-center focus-visible:outline-none">
                      <span className={cn('mx-auto flex h-12 w-12 items-center justify-center rounded-full border bg-[#111] p-[3px] transition-all', active ? 'border-[var(--fm-accent-bright)] shadow-[0_0_0_2px_rgba(251,113,133,.2),0_11px_24px_-12px_rgba(251,113,133,.78)]' : 'border-white/12 group-hover:border-white/24')}>
                        <span className="flex h-full w-full items-center justify-center rounded-full bg-[#d8d8d8] text-[8px] font-black text-black">{feedInitials(feed.title)}</span>
                      </span>
                      <span className={cn('mt-1.5 block max-w-[68px] truncate text-[7px] font-black uppercase tracking-[0.14em]', active ? 'text-[var(--fm-accent-bright)]' : 'text-white/28')}>{titleCase(feed.title)}</span>
                    </button>
                  );
                })}
              </div>
            </motion.div>
          </div>
        </motion.header>
      </div>
    </div>
  );
}

const PREVIEW_NAV_ITEMS = [
  { label: 'Feed', href: '/', icon: LayoutGrid },
  { label: 'Read', href: '/read', icon: BookOpenText },
  { label: 'Lead', href: '/read/lead-preview', icon: Trophy },
] as const;

function LeadPreviewNav() {
  return (
    <div className="pointer-events-none fixed bottom-[calc(12px+env(safe-area-inset-bottom))] left-0 right-0 z-[180] flex justify-center md:bottom-5">
      <nav aria-label="Preview app navigation" className="fm-depth-chrome fm-depth-chrome--nav pointer-events-auto px-1 py-1">
        <div className="relative grid grid-cols-3 gap-0.5">
          <motion.span layoutId="lead-preview-nav-pill" aria-hidden="true" className="absolute inset-y-0 left-[66.666%] w-1/3 rounded-[22px] bg-[var(--fm-accent)] shadow-[0_4px_20px_rgb(var(--fm-accent-rgb)/.28),0_8px_24px_rgba(0,0,0,.45)]" />
          {PREVIEW_NAV_ITEMS.map((item) => {
            const active = item.label === 'Lead';
            return (
              <Link key={item.label} href={item.href} scroll={false} aria-label={item.label} className="group relative z-10">
                <motion.span whileTap={{ scale: 0.95 }} className={cn('flex min-w-[78px] flex-col items-center justify-center rounded-[22px] px-3 py-2 text-[10px] font-black uppercase tracking-[0.14em] transition-colors sm:min-w-[86px] sm:py-2.5', active ? 'text-white' : 'text-[#a3828b]')}>
                  <item.icon className="h-5 w-5" strokeWidth={2.75} />
                  <span className="mt-1">{item.label}</span>
                </motion.span>
              </Link>
            );
          })}
        </div>
      </nav>
    </div>
  );
}

function ProofMosaic({ row }: { row: BoardRow }) {
  const images = row.proofPosts.slice(0, 3);
  if (!images.length) {
    return (
      <div className="flex h-full min-h-[160px] items-center justify-center bg-white/[0.035]">
        <span className="h-20 w-20 rounded-full border border-white/10 p-1">
          <FeederStoryAvatar feeder={{ handle: row.name, profilePicUrl: row.profilePicUrl }} className="text-[20px]" />
        </span>
      </div>
    );
  }
  return (
    <div className="relative h-full min-h-[160px] overflow-hidden bg-white/[0.025]">
      {images.map((post, index) => (
        <span
          key={post.postKey}
          className="absolute top-1/2 h-[82%] -translate-y-1/2 overflow-hidden rounded-[18px] border border-white/12 bg-black shadow-[0_18px_36px_-18px_rgba(0,0,0,.9)] aspect-[4/5]"
          style={{ left: `${4 + index * 24}%`, zIndex: 30 - index * 10, transform: `translateY(-50%) scale(${1 - index * 0.08})` }}
        >
          {/* eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URLs */}
          <img src={post.thumbnailUrl || ''} alt="" className="h-full w-full object-cover" loading={index === 0 ? 'eager' : 'lazy'} decoding="async" />
        </span>
      ))}
    </div>
  );
}

function LeaderCards({ rows, days }: { rows: BoardRow[]; days: number }) {
  const reduce = Boolean(useReducedMotion());
  const overall = rows[0] || null;
  const strongest = [...rows].filter((row) => row.usual != null).sort((a, b) => (b.usual ?? 0) - (a.usual ?? 0))[0] || overall;
  const mover = [...rows]
    .sort((a, b) => ((b.followerDelta ?? -Infinity) - (a.followerDelta ?? -Infinity)) || b.followers - a.followers)[0] || overall;

  if (!overall || !strongest || !mover) return null;

  return (
    <motion.div
      variants={SLOT_CONTAINER}
      initial={reduce ? false : 'hidden'}
      animate="visible"
      className="flex snap-x snap-mandatory gap-3 overflow-x-auto pb-2 [scrollbar-width:none] lg:grid lg:grid-cols-[1.08fr_.94fr_.98fr] lg:overflow-visible lg:pb-0 [&::-webkit-scrollbar]:hidden"
    >
      <motion.article variants={SLOT_ITEM} className="fm-depth-glass relative min-h-[232px] min-w-[86vw] snap-center overflow-hidden rounded-[28px] border-white/10 sm:min-w-[440px] lg:min-w-0">
        <div className="grid h-full grid-cols-[1.02fr_.98fr]">
          <div className="relative z-10 flex flex-col justify-between p-5 pb-[58px] sm:p-6 sm:pb-[62px]">
            <div className="text-[9px] font-black uppercase tracking-[0.2em] text-[var(--fm-accent-bright)]">Overall leader</div>
            <div>
              <div className="fm-depth-title flex items-end font-black leading-[0.76] tracking-[-0.055em] text-white">
                <span className="mr-2 pb-1 text-[13px] tracking-normal text-white/54">TOP</span>
                <span className="text-[72px] sm:text-[82px]">{overall.topPercent == null ? '—' : Math.max(1, Math.round(overall.topPercent))}</span>
                {overall.topPercent != null ? <span className="ml-1 text-[32px] text-[var(--fm-accent-bright)]">%</span> : null}
              </div>
              <div className="mt-2 text-[10px] font-black uppercase tracking-[0.14em] text-white/36">Lower is stronger</div>
            </div>
          </div>
          <div className="relative overflow-hidden border-l border-white/[0.07]">
            <ProofMosaic row={overall} />
            <div className="absolute inset-0 bg-[linear-gradient(90deg,rgba(4,4,4,.58),transparent_46%)]" />
          </div>
        </div>
        <div className="absolute inset-x-0 bottom-0 z-20 flex h-[50px] items-center justify-between gap-4 border-t border-white/[0.09] bg-black/48 px-5 backdrop-blur-md sm:px-6">
          <span className="truncate text-[11px] font-black text-white">{overall.name}</span>
          <span className="shrink-0 text-[8px] font-black uppercase tracking-[0.16em] text-white/38">{overall.postCount} posts · {days}D</span>
        </div>
      </motion.article>

      <motion.article variants={SLOT_ITEM} className="fm-depth-glass relative min-h-[232px] min-w-[86vw] snap-center overflow-hidden rounded-[28px] border-white/10 sm:min-w-[440px] lg:min-w-0">
        {strongest.thumbnailUrl ? (
          <span className="absolute inset-y-0 right-0 aspect-[4/5] overflow-hidden">
            {/* eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL */}
            <img src={strongest.thumbnailUrl} alt="" className="h-full w-full object-cover opacity-90" loading="eager" decoding="async" />
          </span>
        ) : null}
        <div className="absolute inset-0 bg-[linear-gradient(90deg,#080808_0%,#080808_43%,rgba(8,8,8,.52)_68%,rgba(8,8,8,.08))]" />
        <div className="relative z-10 flex h-full max-w-[70%] flex-col justify-between p-5 pb-[62px] sm:p-6 sm:pb-[64px]">
          <div className="text-[9px] font-black uppercase tracking-[0.2em] text-[var(--fm-accent-bright)]">Strongest vs usual</div>
          <div>
            <div className="fm-depth-title font-black leading-[0.78] tracking-[-0.055em] text-white">
              <span className="text-[72px] sm:text-[82px]">{strongest.usual == null ? '—' : strongest.usual.toFixed(strongest.usual >= 10 ? 0 : 1).replace(/\.0$/, '')}</span>
              {strongest.usual != null ? <span className="ml-1 text-[32px] text-[var(--fm-accent-bright)]">×</span> : null}
            </div>
            <div className="mt-2 text-[10px] font-black uppercase tracking-[0.14em] text-white/36">Above 1× is stronger</div>
          </div>
        </div>
        <div className="absolute inset-x-0 bottom-0 z-20 flex h-[50px] items-center justify-between gap-4 border-t border-white/[0.09] bg-black/48 px-5 backdrop-blur-md sm:px-6">
          <span className="truncate text-[11px] font-black text-white">{strongest.name}</span>
          <span className="shrink-0 text-[8px] font-black uppercase tracking-[0.16em] text-white/38">Best post</span>
        </div>
      </motion.article>

      <motion.article variants={SLOT_ITEM} className="fm-depth-glass relative min-h-[232px] min-w-[86vw] snap-center overflow-hidden rounded-[28px] border-white/10 sm:min-w-[440px] lg:min-w-0">
        {mover.thumbnailUrl ? (
          <span className="absolute inset-y-0 right-0 aspect-[4/5] overflow-hidden">
            {/* eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL */}
            <img src={mover.thumbnailUrl} alt="" className="h-full w-full object-cover opacity-82" loading="lazy" decoding="async" />
          </span>
        ) : null}
        <div className="absolute inset-0 bg-[linear-gradient(90deg,#080808_0%,#080808_43%,rgba(8,8,8,.54)_70%,rgba(8,8,8,.12))]" />
        <div className="relative z-10 flex h-full max-w-[72%] flex-col justify-between p-5 pb-[62px] sm:p-6 sm:pb-[64px]">
          <div className="text-[9px] font-black uppercase tracking-[0.2em] text-[var(--fm-accent-bright)]">
            {mover.followerDelta == null ? 'Audience leader' : 'Follower mover'}
          </div>
          <div>
            <div className="fm-depth-title font-black leading-[0.78] tracking-[-0.055em] text-white">
              <span className="text-[60px] sm:text-[70px]">{mover.followerDelta == null ? compactNumber(mover.followers) : signedNumber(mover.followerDelta)}</span>
            </div>
            <div className="mt-2 text-[10px] font-black uppercase tracking-[0.14em] text-white/36">
              {mover.followerDelta == null ? 'Current followers' : `${days}D net movement`}
            </div>
          </div>
        </div>
        <div className="absolute inset-x-0 bottom-0 z-20 flex h-[50px] items-center justify-between gap-4 border-t border-white/[0.09] bg-black/48 px-5 backdrop-blur-md sm:px-6">
          <span className="truncate text-[11px] font-black text-white">{mover.name}</span>
          <span className="shrink-0 text-[8px] font-black uppercase tracking-[0.16em] text-white/38">Followers</span>
        </div>
      </motion.article>
    </motion.div>
  );
}

function proofMetric(post: TrackedPost) {
  if (post.rankingMetric === 'comments') return `${compactNumber(numeric(post.comments))} comments`;
  if (post.rankingMetric === 'engagement_rate') return `${formatEngagementRate(post.engagementRate)} eng. rate`;
  return `${compactNumber(numeric(post.likes))} likes`;
}

function MetricLedger({ row }: { row: BoardRow }) {
  const metrics = [
    { label: 'Comments', value: compactNumber(row.comments), multiple: row.commentsMultiple },
    { label: 'Likes', value: compactNumber(row.likes), multiple: row.likesMultiple },
    { label: 'Eng. rate', value: formatEngagementRate(row.engagementRate), multiple: row.engagementRateMultiple },
    { label: row.followerDelta == null ? 'Followers' : 'Follower move', value: row.followerDelta == null ? compactNumber(row.followers) : signedNumber(row.followerDelta), multiple: null },
  ];
  return (
    <motion.dl variants={SLOT_ITEM} className="order-3 -mx-4 flex min-w-0 overflow-x-auto border-y border-white/[0.08] px-4 [scrollbar-width:none] sm:-mx-6 sm:px-6 lg:order-2 lg:mx-0 lg:grid lg:grid-cols-2 lg:gap-x-6 lg:gap-y-7 lg:overflow-visible lg:border-y-0 lg:border-l lg:px-0 lg:pl-7 [&::-webkit-scrollbar]:hidden">
      {metrics.map((metric) => (
        <div key={metric.label} className="min-w-[124px] border-r border-white/[0.08] py-4 pr-4 last:border-r-0 lg:min-w-0 lg:border-r-0 lg:py-0 lg:pr-0">
          <dt className="truncate text-[8px] font-black uppercase tracking-[0.2em] text-white/30">{metric.label}</dt>
          <dd className="fm-depth-title mt-2 truncate text-[24px] font-black tabular-nums leading-none text-white sm:text-[28px]">{metric.value}</dd>
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
  const topPercent = row.topPercent == null ? null : Math.max(1, Math.min(100, row.topPercent));

  return (
    <motion.div
      data-testid="feederboard-expansion"
      initial={reduce ? false : { height: 0, opacity: 0 }}
      animate={{ height: 'auto', opacity: 1 }}
      exit={{ height: 0, opacity: 0 }}
      transition={reduce ? { duration: 0 } : { height: { duration: 0.46, ease: SOFT_EASE }, opacity: { duration: 0.28, delay: 0.06 } }}
      className="overflow-hidden"
    >
      <motion.div
        variants={SLOT_CONTAINER}
        initial={reduce ? false : 'hidden'}
        animate="visible"
        exit="exit"
        onClick={onCollapse}
        className="relative isolate cursor-pointer overflow-hidden border-t border-white/[0.1] bg-[#0a0a0a] p-4 sm:p-6 lg:min-h-[374px]"
      >
        <div className="pointer-events-none absolute inset-y-0 left-0 w-1 bg-[var(--fm-accent-bright)] shadow-[0_0_34px_rgba(251,113,133,.48)]" />
        <div className="pointer-events-none absolute -left-16 top-4 h-56 w-56 rounded-full bg-[var(--fm-accent-bright)]/[0.055] blur-[70px]" />
        <div className="pointer-events-none absolute -bottom-14 left-3 select-none text-[164px] font-black leading-none tracking-[-0.1em] text-white/[0.025] sm:text-[210px]">0{row.rank}</div>

        <motion.div variants={SLOT_ITEM} className="relative grid gap-6 lg:grid-cols-[220px_minmax(240px,.78fr)_minmax(400px,1.3fr)] lg:items-center lg:gap-8">
          <div className="order-1 min-w-0">
            <div className="flex items-center justify-between gap-3 lg:block">
              <div className="flex min-w-0 items-center gap-3">
                <span className="h-12 w-12 shrink-0 rounded-full border border-white/24 bg-black p-[3px] shadow-[0_0_0_4px_rgba(251,113,133,.08),0_15px_30px_-18px_rgba(0,0,0,.95)]">
                  <FeederStoryAvatar feeder={{ handle: row.name, profilePicUrl: row.profilePicUrl }} className="text-[13px]" />
                </span>
                <span className="min-w-0">
                  <span className="block truncate text-[15px] font-black leading-none text-white">{row.name}</span>
                  <span className="mt-1.5 block truncate text-[9px] font-black uppercase tracking-[0.14em] text-white/34">{row.handle}</span>
                </span>
              </div>
              <span className="text-right text-[8px] font-black uppercase tracking-[0.18em] text-white/24 lg:mt-5 lg:block">{row.postCount} posts · {days}D<br /><i className="mt-1 block not-italic text-white/16">tap outside media to close</i></span>
            </div>

            <div className="mt-7 flex items-end gap-4 lg:mt-8 lg:block">
              <div className="min-w-0">
                <div className="text-[8px] font-black uppercase tracking-[0.2em] text-white/30">Best post</div>
                <div className="fm-depth-title mt-2 flex items-end font-black leading-[0.68] tracking-[-0.08em] text-white">
                  <span className="mr-2 pb-2 text-[11px] tracking-normal text-white/40">TOP</span>
                  <span className="text-[82px] sm:text-[100px] lg:text-[112px]">{topPercent == null ? '—' : Math.round(topPercent)}</span>
                  {topPercent != null ? <span className="ml-1 pb-1 text-[34px] tracking-normal text-[var(--fm-accent-bright)] lg:text-[42px]">%</span> : null}
                </div>
              </div>
              <div className="shrink-0 border-l border-white/10 pl-4 lg:mt-4 lg:border-l-0 lg:border-t lg:pl-0 lg:pt-4">
                <div className="fm-depth-title text-[34px] font-black leading-none tracking-[-0.05em] text-white">{formatMultiple(row.usual)}</div>
                <div className="mt-1.5 text-[8px] font-black uppercase tracking-[0.16em] text-white/30">strongest vs usual</div>
              </div>
            </div>
          </div>

          <MetricLedger row={row} />

          <motion.div variants={SLOT_ITEM} className="order-2 min-w-0 lg:order-3">
            <div className="mb-3 flex items-center justify-between gap-3">
              <span className="text-[8px] font-black uppercase tracking-[0.2em] text-white/32">Posts behind the lead</span>
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
                      data-testid={'proof-post-mobile-' + index}
                      aria-label={post.postUrl ? 'Open tracked post' : 'Tracked post'}
                      className="relative aspect-[4/5] w-[58%] min-w-[58%] snap-center overflow-hidden rounded-[22px] border border-white/12 bg-black shadow-[0_24px_48px_-28px_rgba(0,0,0,.95)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
                    >
                      {post.thumbnailUrl ? (
                        // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
                        <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" loading="lazy" decoding="async" />
                      ) : null}
                      <div className="absolute inset-0 bg-[linear-gradient(180deg,transparent_30%,rgba(0,0,0,.9))]" />
                      <div className="absolute inset-x-0 bottom-0 flex items-end justify-between gap-3 p-4">
                        <span className="text-[13px] font-black text-white">{post.latestPercentile == null ? '—' : `TOP ${Math.max(1, Math.round(post.latestPercentile))}%`}</span>
                        <span className="text-[8px] font-black uppercase tracking-[0.12em] text-white/54">{proofMetric(post)}</span>
                      </div>
                    </a>
                  ))}
                </div>

                <div className="relative hidden h-[286px] overflow-hidden sm:block">
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
                      data-testid={'proof-post-desktop-' + index}
                      aria-label={post.postUrl ? 'Open tracked post' : 'Tracked post'}
                      initial={reduce ? false : { x: 38, y: 26, scale: 0.9, opacity: 0 }}
                      animate={{ x: 0, y: index * 11, scale: 1 - index * 0.07, opacity: 1 }}
                      transition={reduce ? { duration: 0 } : { delay: 0.12 + index * 0.065, duration: 0.58, ease: SOFT_EASE }}
                      className="group absolute top-0 h-[260px] w-[208px] aspect-[4/5] origin-center overflow-hidden rounded-[22px] border border-white/12 bg-black shadow-[0_30px_54px_-30px_rgba(0,0,0,.98)] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
                      style={{ left: index === 0 ? '0%' : index === 1 ? '26%' : '52%', zIndex: 30 - index * 10 }}
                    >
                      {post.thumbnailUrl ? (
                        // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
                        <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover transition-transform duration-700 group-hover:scale-[1.035]" loading="lazy" decoding="async" />
                      ) : null}
                      <div className="absolute inset-0 bg-[linear-gradient(180deg,transparent_28%,rgba(0,0,0,.9))]" />
                      <div className="absolute inset-x-0 bottom-0 flex items-end justify-between gap-3 p-4">
                        <span className="text-[13px] font-black text-white">{post.latestPercentile == null ? '—' : `TOP ${Math.max(1, Math.round(post.latestPercentile))}%`}</span>
                        <span className="text-[8px] font-black uppercase tracking-[0.12em] text-white/54">{proofMetric(post)}</span>
                      </div>
                    </motion.a>
                  ))}
                </div>
              </>
            ) : (
              <div className="flex h-[188px] items-center justify-center border-y border-white/[0.07] text-[9px] font-black uppercase tracking-[0.18em] text-white/26">No tracked media in this window</div>
            )}
          </motion.div>
        </motion.div>
      </motion.div>
    </motion.div>
  );
}

function Feederboard({ rows, selectedId, onSelect, days }: { rows: BoardRow[]; selectedId: string; onSelect: (id: string) => void; days: number }) {
  const reduce = Boolean(useReducedMotion());
  return (
    <section aria-labelledby="feederboard-title" className="min-w-0 max-w-full overflow-hidden bg-[#070707]">
      <div className="flex items-center justify-between gap-4 border-b border-white/[0.08] px-4 py-4 sm:px-5">
        <div>
          <h2 id="feederboard-title" className="text-[10px] font-black uppercase tracking-[0.2em] text-[var(--fm-accent-bright)]">Feederboard</h2>
          <p className="mt-1 text-[9px] font-bold text-white/28">Top 10 · tap a row to see the proof</p>
        </div>
        <div className="hidden text-[8px] font-black uppercase tracking-[0.16em] text-white/24 sm:block">Top % is overall post position</div>
      </div>

      <div className="hidden grid-cols-[72px_minmax(210px,1.15fr)_110px_110px_minmax(320px,1fr)_30px] items-end gap-4 border-b border-white/[0.07] px-5 py-3 text-[7px] font-black uppercase tracking-[0.17em] text-white/24 lg:grid">
        <span>Rank</span><span>Feed / feeder</span><span>Best post</span><span>Vs usual</span><span>Response · posts · follower move</span><span />
      </div>

      <LayoutGroup id="lead-preview-feederboard">
        <div>
          {rows.map((row) => {
            const selected = row.id === selectedId;
            const rowStrength = row.topPercent == null ? 0.08 : Math.max(0.08, Math.min(1, (101 - row.topPercent) / 100));
            return (
              <motion.div key={row.id} layout="position" transition={{ layout: LADDER_SPRING }} className="border-b border-white/[0.07] last:border-b-0">
                <button
                  type="button"
                  onClick={() => onSelect(selected ? '' : row.id)}
                  aria-expanded={selected}
                  data-testid={`feederboard-row-${row.rank}`}
                  className={cn(
                    'relative isolate block w-full overflow-hidden text-left transition-colors duration-300 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]',
                    selected
                      ? 'bg-[radial-gradient(circle_at_0_50%,rgba(251,113,133,.12),transparent_34%),linear-gradient(90deg,rgba(255,255,255,.04),rgba(255,255,255,.015))]'
                      : 'hover:bg-white/[0.022]',
                  )}
                >
                  <motion.span
                    aria-hidden="true"
                    className={cn('absolute inset-y-0 left-0 z-0 w-full origin-left', selected ? 'bg-[var(--fm-accent-bright)]/[0.075]' : 'bg-[var(--fm-accent-bright)]/[0.035]')}
                    initial={reduce ? false : { scaleX: 0 }}
                    animate={{ scaleX: rowStrength }}
                    transition={reduce ? { duration: 0 } : { delay: row.rank * 0.035, duration: 0.72, ease: SOFT_EASE }}
                  />
                  {selected ? <motion.span layoutId="lead-board-selected-edge" className="absolute inset-y-0 left-0 z-20 w-1 bg-[var(--fm-accent-bright)] shadow-[0_0_26px_rgba(251,113,133,.46)]" /> : null}
                  <div className={cn('relative z-10 grid min-h-[70px] grid-cols-[50px_minmax(0,1fr)_82px_22px] items-center gap-2 px-3 py-3 transition-[min-height,padding] sm:grid-cols-[58px_minmax(0,1fr)_98px_26px] sm:px-4 lg:hidden', selected && 'min-h-[82px] py-4')}>
                    <span className="relative h-full">
                      <span className={cn('pointer-events-none absolute left-0 top-1/2 -translate-y-1/2 select-none text-[56px] font-black tabular-nums leading-none tracking-[-0.08em]', selected ? 'text-[var(--fm-accent-bright)]/[0.12]' : 'text-white/[0.055]')}>{String(row.rank).padStart(2, '0')}</span>
                    </span>
                    <RowIdentity row={row} compact />
                    <span className="min-w-0 text-right">
                      <span className="block text-[7px] font-black uppercase tracking-[0.16em] text-white/26">Best post</span>
                      <span className={cn('fm-depth-title mt-1 block font-black tabular-nums leading-none', selected ? 'text-[25px] text-white sm:text-[29px]' : 'text-[22px] text-[var(--fm-accent-bright)] sm:text-[25px]')}>
                        {row.topPercent == null ? '—' : Math.max(1, Math.round(row.topPercent))}
                        {row.topPercent == null ? null : <i className="ml-0.5 not-italic text-[0.55em] text-[var(--fm-accent-bright)]">%</i>}
                      </span>
                      <span className="mt-1 block text-[9px] font-black tabular-nums text-white/42">{formatMultiple(row.usual)} <i className="not-italic text-[7px] uppercase tracking-[0.1em] text-white/22">usual</i></span>
                    </span>
                    <ChevronDown className={cn('h-4 w-4 text-white/36 transition-transform duration-300', selected && 'rotate-180 text-white')} />
                  </div>

                  <div className={cn('relative z-10 hidden min-h-[78px] grid-cols-[72px_minmax(210px,1.15fr)_110px_110px_minmax(320px,1fr)_30px] items-center gap-4 px-5 py-3 transition-[min-height,padding] lg:grid', selected && 'min-h-[94px] py-4')}>
                    <span className="relative h-full">
                      <span className={cn('pointer-events-none absolute left-0 top-1/2 -translate-y-1/2 select-none text-[58px] font-black tabular-nums leading-none tracking-[-0.08em]', selected ? 'text-[var(--fm-accent-bright)]/[0.14]' : 'text-white/[0.055]')}>{String(row.rank).padStart(2, '0')}</span>
                    </span>
                    <RowIdentity row={row} compact />
                    <span className="min-w-0">
                      <span className="block text-[7px] font-black uppercase tracking-[0.17em] text-white/24">Top · lower stronger</span>
                      <span className={cn('fm-depth-title mt-1 block font-black tabular-nums leading-none', selected ? 'text-[34px] text-white' : 'text-[27px] text-[var(--fm-accent-bright)]')}>
                        {row.topPercent == null ? '—' : Math.max(1, Math.round(row.topPercent))}
                        {row.topPercent == null ? null : <i className="ml-0.5 not-italic text-[0.52em] text-[var(--fm-accent-bright)]">%</i>}
                      </span>
                    </span>
                    <span className="min-w-0">
                      <span className="block text-[7px] font-black uppercase tracking-[0.17em] text-white/24">Baseline</span>
                      <span className={cn('fm-depth-title mt-1 block font-black tabular-nums leading-none text-white', selected ? 'text-[31px]' : 'text-[25px]')}>{formatMultiple(row.usual)}</span>
                    </span>
                    <span className="min-w-0 overflow-hidden">
                      <span className="flex items-center justify-between gap-4">
                        <span className="text-[7px] font-black uppercase tracking-[0.17em] text-white/24">Response · {row.postCount} posts</span>
                        <span className={cn('shrink-0 text-[11px] font-black tabular-nums', (row.followerDelta ?? 0) >= 0 ? 'text-[var(--fm-accent-bright)]' : 'text-white/44')}>{row.followerDelta == null ? compactNumber(row.followers) : signedNumber(row.followerDelta)} <i className="not-italic text-[7px] uppercase tracking-[0.1em] text-white/22">followers</i></span>
                      </span>
                      <span className="mt-2 flex min-w-0 items-baseline gap-4 overflow-hidden whitespace-nowrap">
                        <b className="text-[16px] font-black tabular-nums text-white/76">{compactNumber(row.comments)} <i className="not-italic text-[7px] uppercase tracking-[0.11em] text-white/24">cmts</i></b>
                        <b className="text-[16px] font-black tabular-nums text-white/76">{compactNumber(row.likes)} <i className="not-italic text-[7px] uppercase tracking-[0.11em] text-white/24">likes</i></b>
                        <b className="text-[16px] font-black tabular-nums text-white/76">{formatEngagementRate(row.engagementRate)} <i className="not-italic text-[7px] uppercase tracking-[0.11em] text-white/24">eng.</i></b>
                      </span>
                    </span>
                    <span className="grid h-7 w-7 place-items-center rounded-full border border-white/10 bg-black/20">
                      <ChevronDown className={cn('h-4 w-4 text-white/36 transition-transform duration-300', selected && 'rotate-180 text-white')} />
                    </span>
                  </div>
                </button>
                <AnimatePresence initial={false} mode="popLayout">
                  {selected && <RowExpansion key={`${row.id}:${days}`} row={row} days={days} onCollapse={() => onSelect('')} />}
                </AnimatePresence>
              </motion.div>
            );
          })}
        </div>
      </LayoutGroup>
    </section>
  );
}

function GrowthWindows({ windows, days }: { windows: GrowthWindow[]; days: number }) {
  const reduce = Boolean(useReducedMotion());
  const total = windows.reduce((sum, window) => sum + (window.delta ?? 0), 0);
  const hasDelta = windows.some((window) => window.delta != null);
  const highlightIndex = windows.reduce((bestIndex, window, index) => (
    Math.abs(window.delta ?? 0) >= Math.abs(windows[bestIndex]?.delta ?? 0) ? index : bestIndex
  ), 0);
  return (
    <section aria-labelledby="growth-windows-title" className="relative isolate overflow-hidden border-t border-white/[0.09] bg-[#070707] p-5 sm:p-6 lg:p-7">
      <div className="flex items-end justify-between gap-6">
        <div>
          <div className="flex items-center gap-2 text-white/44"><Users className="h-4 w-4" /><span className="text-[8px] font-black uppercase tracking-[0.22em]">Follower windows</span></div>
          <h2 id="growth-windows-title" className="mt-2 text-[22px] font-black leading-none tracking-[-0.035em] text-white sm:text-[26px]">What landed, when.</h2>
        </div>
        <div className="text-right">
          <div className="fm-depth-title text-[42px] font-black tabular-nums leading-[0.78] tracking-[-0.045em] text-white sm:text-[50px]">{hasDelta ? signedNumber(total) : '—'}</div>
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
        <motion.div variants={SLOT_CONTAINER} initial={reduce ? false : 'hidden'} animate="visible" className="grid min-w-max auto-cols-[230px] grid-flow-col lg:min-w-0 lg:auto-cols-fr">
          {windows.map((window, index) => {
            const highlighted = index === highlightIndex;
            return (
              <motion.article key={window.id} variants={SLOT_ITEM} className="relative min-h-[176px] snap-start px-5 first:pl-1 last:pr-1">
                <div className="text-[8px] font-black uppercase tracking-[0.17em] text-white/34">{window.range}</div>
                <span className={cn('absolute top-[47px] h-[9px] w-[9px] rounded-full border-2 border-[#070707]', highlighted ? 'bg-[var(--fm-accent-bright)] shadow-[0_0_18px_rgba(251,113,133,.78)]' : 'bg-white/36')} />
                <div className={cn('fm-depth-title mt-8 text-[30px] font-black tabular-nums leading-none tracking-[-0.035em]', highlighted ? 'text-[var(--fm-accent-bright)]' : 'text-white')}>{signedNumber(window.delta)}</div>
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
          <span className="rounded-full bg-[var(--fm-accent-bright)] px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.12em] text-black">{card.checkpoint || 'D3'}</span>
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

function PostCardStack({ cards }: { cards: StackCard[] }) {
  const reduce = Boolean(useReducedMotion());
  const visibleCards = cards.slice(0, 5);
  const count = visibleCards.length;
  const signature = visibleCards.map((card) => card.id).join('|');
  const [state, setState] = useState<{ signature: string; index: number; exits: StackExit[]; exitKey: number }>({ signature: '', index: 0, exits: [], exitKey: 0 });
  const activeIndex = count > 0 && state.signature === signature ? state.index % count : 0;
  const exitingCards = !reduce && count > 0 && state.signature === signature
    ? state.exits.map((exit) => ({ ...exit, index: exit.index % count, card: visibleCards[exit.index % count] }))
    : [];
  const latestExitingIndex = exitingCards.at(-1)?.index ?? null;

  const advance = () => {
    if (count <= 1) return;
    setState((current) => {
      const currentIndex = current.signature === signature ? current.index % count : 0;
      const exitKey = current.signature === signature ? current.exitKey + 1 : 1;
      const exits = current.signature === signature ? current.exits : [];
      return { signature, index: (currentIndex + 1) % count, exits: [...exits, { index: currentIndex, key: exitKey }].slice(-3), exitKey };
    });
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
    <div className="min-w-0">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div>
          <div className="text-[9px] font-black uppercase tracking-[0.2em] text-white/30">Top post cards</div>
          <div className="mt-1 text-[13px] font-black leading-none text-white/76">Tap to move the stack</div>
        </div>
        <motion.span key={activeIndex} initial={reduce ? false : { y: 6, opacity: 0 }} animate={{ y: 0, opacity: 1 }} transition={{ duration: 0.34, ease: SOFT_EASE }} className="rounded-full border border-white/10 bg-white/[0.05] px-2.5 py-1 text-[9px] font-black tracking-[0.13em] text-white/44">{activeIndex + 1}/{count}</motion.span>
      </div>

      <motion.button
        type="button"
        onClick={advance}
        whileTap={reduce ? undefined : { scale: 0.986 }}
        transition={{ duration: 0.16, ease: SOFT_EASE }}
        className="relative block h-[390px] w-full touch-manipulation overflow-hidden rounded-[28px] bg-transparent text-left outline-none focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)] sm:h-[430px] lg:h-[460px]"
        aria-label="Advance Post Mortem card stack"
        data-testid="post-mortem-stack"
      >
        <motion.span aria-hidden="true" className="absolute bottom-4 left-[5%] h-14 w-[82%] rounded-full bg-black/62 blur-2xl" animate={reduce ? undefined : { opacity: [0.2, 0.34, 0.2], scaleX: [0.92, 1, 0.92] }} transition={{ duration: 1.6, repeat: Infinity, ease: SOFT_EASE }} />
        {visibleCards.map((card, index) => {
          const order = (index - activeIndex + count) % count;
          const isFront = order === 0;
          const coveredByExit = latestExitingIndex === index && index !== activeIndex;
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

function PostMortem({ cards, rows, days }: { cards: StackCard[]; rows: BoardRow[]; days: number }) {
  const topPosts = rows.reduce((sum, row) => sum + row.postCount, 0);
  const topPercent = rows[0]?.topPercent ?? null;
  const strongest = [...rows].sort((a, b) => (b.usual ?? 0) - (a.usual ?? 0))[0]?.usual ?? null;
  return (
    <section aria-labelledby="post-mortem-title" className="relative isolate min-h-[500px] overflow-hidden border-t border-white/[0.09] bg-[#060606] p-5 sm:p-7 lg:p-9">
      {cards[0]?.thumbnailUrl ? (
        // eslint-disable-next-line @next/next/no-img-element -- authenticated media proxy URL
        <img src={cards[0].thumbnailUrl} alt="" className="pointer-events-none absolute inset-0 h-full w-full scale-105 object-cover opacity-[0.13] blur-[3px]" loading="lazy" decoding="async" />
      ) : null}
      <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(90deg,#060606_0%,rgba(6,6,6,.97)_34%,rgba(6,6,6,.74)_68%,rgba(6,6,6,.9))]" />
      <div className="pointer-events-none absolute -bottom-10 left-2 select-none text-[180px] font-black leading-none tracking-[-0.08em] text-white/[0.025] sm:text-[260px]">PM</div>

      <div className="relative grid gap-5 lg:grid-cols-[.6fr_1.4fr] lg:items-center lg:gap-9">
        <div className="relative min-w-0 lg:pr-3">
          <div className="text-[9px] font-black uppercase tracking-[0.22em] text-[var(--fm-accent-bright)]">After the number</div>
          <h2 id="post-mortem-title" className="fm-depth-title mt-4 text-[48px] font-black leading-[0.78] tracking-[0.13em] text-white sm:text-[62px] lg:text-[72px]">POST<br />MORTEM</h2>
          <p className="mt-5 text-[14px] font-black text-white/52">What actually moved the board.</p>

          <div className="mt-7 flex flex-wrap items-center gap-x-5 gap-y-2 border-t border-white/[0.09] pt-4 text-[9px] font-black uppercase tracking-[0.14em] text-white/32">
            <span><b className="mr-1 text-[15px] text-white">{compactNumber(topPosts)}</b> posts</span>
            <span><b className="mr-1 text-[15px] text-white">{topPercent == null ? '—' : `${Math.max(1, Math.round(topPercent))}%`}</b> best</span>
            <span><b className="mr-1 text-[15px] text-white">{formatMultiple(strongest)}</b> usual</span>
            <span>{days}D</span>
          </div>
        </div>
        <PostCardStack cards={cards} />
      </div>
    </section>
  );
}

function LoadingState() {
  return (
    <div className="flex min-h-[55vh] items-center justify-center">
      <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} className="text-center">
        <motion.span className="mx-auto block h-2 w-2 rounded-full bg-[var(--fm-accent-bright)]" animate={{ opacity: [0.25, 1, 0.25], scale: [0.8, 1, 0.8] }} transition={{ duration: 1.4, repeat: Infinity }} />
        <div className="mt-4 text-[9px] font-black uppercase tracking-[0.22em] text-white/30">Building the board</div>
      </motion.div>
    </div>
  );
}

export default function LeadPreviewPage() {
  const reduce = Boolean(useReducedMotion());
  const pageScrollRef = useRef<HTMLDivElement | null>(null);
  const [days, setDays] = useState<(typeof TIMEFRAMES)[number]>(30);
  const [feeds, setFeeds] = useState<RuntimeFeed[]>([]);
  const [activeFeedId, setActiveFeedId] = useState('all');
  const [selectedId, setSelectedId] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const [portalTarget, setPortalTarget] = useState<HTMLElement | null>(null);
  const headerCompressed = useCompressedOnScroll(pageScrollRef, false, {
    collapseDistance: 150,
    expandDistance: 64,
    topGuard: 30,
  });

  useEffect(() => {
    document.title = 'Lead Preview | FeedMe';
    setPortalTarget(document.body);
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
          const params = new URLSearchParams({ feedId: feed.id, handle: 'all', timeframe: `${days}D` });
          const dashboardParams = new URLSearchParams({ feedId: feed.id, days: String(days) });
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
        if (!cancelled) setLoading(false);
      }
    }

    void load();
    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [days]);

  const rows = useMemo(() => buildRows(feeds, activeFeedId, days), [activeFeedId, days, feeds]);
  const windows = useMemo(() => growthWindows(feeds, activeFeedId, days), [activeFeedId, days, feeds]);
  const activeFeeds = useMemo(() => activeFeedId === 'all' ? feeds : feeds.filter((feed) => feed.id === activeFeedId), [activeFeedId, feeds]);
  const stackCards = useMemo(() => activeFeeds
    .flatMap((feed) => postsInWindow(feed.posts, days).map((post) => {
      const feeder = feed.feeders.find((candidate) => normalizeHandle(candidate.handle) === normalizeHandle(post.handle));
      return {
        id: `${feed.id}:${post.postKey}`,
        feedName: titleCase(feed.title),
        handle: post.handle || feeder?.handle || feed.title,
        profilePicUrl: feeder?.profilePicUrl || null,
        thumbnailUrl: post.thumbnailUrl || feeder?.thumbnailUrl || null,
        checkpoint: post.latestCheckpoint,
        topPercent: post.latestPercentile,
        rankingMetric: post.rankingMetric,
        rankingMultiple: post.rankingMultiple,
        likes: post.likes,
        comments: post.comments,
        engagementRate: post.engagementRate,
      } satisfies StackCard;
    }))
    .sort((a, b) => (a.topPercent ?? 101) - (b.topPercent ?? 101) || (b.rankingMultiple ?? 0) - (a.rankingMultiple ?? 0))
    .slice(0, 5), [activeFeeds, days]);

  useEffect(() => {
    if (!rows.length) {
      setSelectedId('');
      return;
    }
    setSelectedId((current) => rows.some((row) => row.id === current) ? current : rows[0].id);
  }, [rows]);

  const chrome = (
    <>
      <LeadAppHeader
        feeds={feeds}
        activeFeedId={activeFeedId}
        days={days}
        compressed={headerCompressed}
        onFeedChange={(id) => {
          setActiveFeedId(id);
          setSelectedId('');
        }}
        onDaysChange={setDays}
      />
      <LeadPreviewNav />
    </>
  );

  return (
    <div ref={pageScrollRef} data-testid="lead-preview-page" className="fm-dashboard-mesh h-[100dvh] w-screen min-w-0 max-w-[100vw] scroll-pb-[calc(96px+env(safe-area-inset-bottom))] scroll-pt-[calc(88px+env(safe-area-inset-top))] overscroll-y-contain overflow-x-hidden overflow-y-auto bg-[#030303] text-white [-webkit-overflow-scrolling:touch] [--fm-accent:#E11D48] [--fm-accent-rgb:225_29_72] [--fm-accent-bright:#FB7185]">
      {portalTarget ? createPortal(chrome, portalTarget) : null}
      <main className="relative z-10 mx-auto w-full min-w-0 max-w-[1500px] px-3 pb-[calc(126px+env(safe-area-inset-bottom))] pt-[calc(198px+env(safe-area-inset-top))] sm:px-5 lg:px-7">
        {loading && !feeds.length ? <LoadingState /> : error ? (
          <div className="flex min-h-[55vh] items-center justify-center px-6 text-center">
            <div>
              <div className="text-[11px] font-black uppercase tracking-[0.2em] text-white/58">Board unavailable</div>
              <p className="mt-3 text-[12px] font-bold text-white/32">{error}</p>
              <Link href="/read" className="mt-6 inline-flex rounded-full border border-white/12 px-4 py-2.5 text-[9px] font-black uppercase tracking-[0.15em] text-white/52 hover:text-white">Return to Read</Link>
            </div>
          </div>
        ) : rows.length ? (
          <motion.div initial={reduce ? false : { opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.42, ease: SOFT_EASE }} className="min-w-0 max-w-full overflow-hidden rounded-[32px] border border-white/[0.09] bg-[#050505] shadow-[0_32px_80px_-46px_rgba(0,0,0,.98)]">
            <section aria-labelledby="leaders-title" className="min-w-0 max-w-full overflow-hidden border-b border-white/[0.09] bg-[#050505] p-3 sm:p-4">
              <h2 id="leaders-title" className="mb-3 flex items-center gap-2 px-1 text-[9px] font-black uppercase tracking-[0.22em] text-white/68"><span className="h-1.5 w-1.5 rounded-full bg-[var(--fm-accent-bright)] shadow-[0_0_14px_rgba(251,113,133,.8)]" />Today&apos;s leaders</h2>
              <LeaderCards rows={rows} days={days} />
            </section>
            <Feederboard rows={rows} selectedId={selectedId} onSelect={setSelectedId} days={days} />
            <GrowthWindows windows={windows} days={days} />
            <PostMortem cards={stackCards} rows={rows} days={days} />
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
