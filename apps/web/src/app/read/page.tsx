'use client';

import Link from 'next/link';
import { CSSProperties, ReactNode, forwardRef, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import {
  ChevronRight,
  Heart,
  MessageCircle,
  Trophy,
  Users,
} from 'lucide-react';
import { AnimatePresence, LayoutGroup, motion, useReducedMotion } from 'framer-motion';
import Odometer from '@/components/login/Odometer';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { useCompressedOnScroll } from '@/lib/useCompressedOnScroll';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import { cn } from '@/lib/utils';

type Feeder = {
  handle: string;
  name: string;
  initials: string;
  profilePicUrl?: string | null;
  thumbnailUrl?: string | null;
  rank: number;
  score: number;
  move: string;
  avg: string;
  topPosts: number;
  comments: string;
  likes: string;
  followers: string;
  cadence: string;
  signal: string;
  proof: string;
  thumb: string;
};

type FeedBoard = {
  id: string;
  name: string;
  initials: string;
  color: string;
  tint: string;
  feeders: Feeder[];
  summary: {
    leader: string;
    topScore: number;
    feeders: number;
    signals: number;
  };
};

type FeedRailMotion = {
  id: string;
  mode: 'opening' | 'closing';
  x: number;
};

type FeedApiFeeder = {
  handle?: string;
  isAnchor?: boolean;
  profilePicUrl?: string | null;
  thumbnailUrl?: string | null;
  followerCount?: number | null;
  metrics?: {
    likes?: string | number | null;
    comments?: string | number | null;
    views?: string | number | null;
    postsTracked?: string | number | null;
  };
};

type FeedApiFeed = {
  id?: string;
  title?: string;
  feeders?: FeedApiFeeder[];
  metrics?: {
    likes?: string | number | null;
    comments?: string | number | null;
    views?: string | number | null;
    postsTracked?: string | number | null;
  };
};

const FEEDS: FeedBoard[] = [
  {
    id: 'beauty',
    name: 'Beauty',
    initials: 'BTY',
    color: '#E11D48',
    tint: 'rgb(225 29 72 / 0.12)',
    summary: { leader: '@lakmeindia', topScore: 96, feeders: 42, signals: 31 },
    feeders: [
      {
        rank: 1,
        handle: '@lakmeindia',
        name: 'Lakme India',
        initials: 'LI',
        score: 96,
        move: '+4',
        avg: 'Top 8%',
        topPosts: 7,
        comments: '612',
        likes: '13.6K',
        followers: '+1.8K',
        cadence: '11/wk',
        signal: 'Comment leader',
        proof: 'Top 2% carousel by comments',
        thumb: 'rose',
      },
      {
        rank: 2,
        handle: '@trysugar',
        name: 'SUGAR',
        initials: 'SG',
        score: 91,
        move: '+2',
        avg: 'Top 12%',
        topPosts: 5,
        comments: '439',
        likes: '9.4K',
        followers: '+940',
        cadence: '8/wk',
        signal: 'Most engaged',
        proof: '5% comments on D3',
        thumb: 'plum',
      },
      {
        rank: 3,
        handle: '@kaybykatrina',
        name: 'Kay Beauty',
        initials: 'KB',
        score: 84,
        move: '-1',
        avg: 'Top 18%',
        topPosts: 3,
        comments: '188',
        likes: '6.2K',
        followers: '+420',
        cadence: '5/wk',
        signal: 'Like depth',
        proof: 'Image beating baseline',
        thumb: 'gold',
      },
      {
        rank: 4,
        handle: '@pradabeauty',
        name: 'Prada Beauty',
        initials: 'PB',
        score: 79,
        move: '+7',
        avg: 'Top 22%',
        topPosts: 2,
        comments: '141',
        likes: '10.4K',
        followers: '+1.1K',
        cadence: '3/wk',
        signal: 'Growth jump',
        proof: 'Follower lift with fewer posts',
        thumb: 'black',
      },
    ],
  },
  {
    id: 'creators',
    name: 'Creators',
    initials: 'CRE',
    color: '#2563EB',
    tint: 'rgb(37 99 235 / 0.12)',
    summary: { leader: '@ishanworks', topScore: 94, feeders: 18, signals: 19 },
    feeders: [
      {
        rank: 1,
        handle: '@ishanworks',
        name: 'Ishan',
        initials: 'IW',
        score: 94,
        move: '+5',
        avg: 'Top 9%',
        topPosts: 6,
        comments: '284',
        likes: '4.8K',
        followers: '+760',
        cadence: '6/wk',
        signal: 'Repeat winner',
        proof: '4 of top 10 posts',
        thumb: 'blue',
      },
      {
        rank: 2,
        handle: '@mayaonfilm',
        name: 'Maya',
        initials: 'MF',
        score: 89,
        move: '+1',
        avg: 'Top 14%',
        topPosts: 4,
        comments: '231',
        likes: '3.9K',
        followers: '+610',
        cadence: '5/wk',
        signal: 'Comment lift',
        proof: 'Tutorial format carrying',
        thumb: 'cyan',
      },
      {
        rank: 3,
        handle: '@reelwithraj',
        name: 'Raj',
        initials: 'RR',
        score: 82,
        move: '-2',
        avg: 'Top 21%',
        topPosts: 3,
        comments: '194',
        likes: '5.6K',
        followers: '+380',
        cadence: '9/wk',
        signal: 'High volume',
        proof: 'Volume ahead, quality mixed',
        thumb: 'green',
      },
    ],
  },
  {
    id: 'food',
    name: 'F&B',
    initials: 'F&B',
    color: '#F59E0B',
    tint: 'rgb(245 158 11 / 0.14)',
    summary: { leader: '@mokaiindia', topScore: 93, feeders: 31, signals: 24 },
    feeders: [
      {
        rank: 1,
        handle: '@mokaiindia',
        name: 'Mokai',
        initials: 'MI',
        score: 93,
        move: '+3',
        avg: 'Top 10%',
        topPosts: 6,
        comments: '322',
        likes: '7.2K',
        followers: '+840',
        cadence: '7/wk',
        signal: 'Like leader',
        proof: 'Reels holding above baseline',
        thumb: 'amber',
      },
      {
        rank: 2,
        handle: '@getdrnk.in',
        name: 'Get Drnk',
        initials: 'GD',
        score: 86,
        move: '+6',
        avg: 'Top 15%',
        topPosts: 4,
        comments: '267',
        likes: '5.1K',
        followers: '+520',
        cadence: '4/wk',
        signal: 'Sharp comeback',
        proof: 'Spike after 12d quiet',
        thumb: 'red',
      },
      {
        rank: 3,
        handle: '@veronicasbombay',
        name: 'Veronicas',
        initials: 'VB',
        score: 78,
        move: '-1',
        avg: 'Top 27%',
        topPosts: 2,
        comments: '143',
        likes: '3.4K',
        followers: '+210',
        cadence: '3/wk',
        signal: 'Quiet risk',
        proof: 'Best window missed twice',
        thumb: 'teal',
      },
    ],
  },
  {
    id: 'ai',
    name: 'AI',
    initials: 'AI',
    color: '#7C3AED',
    tint: 'rgb(124 58 237 / 0.13)',
    summary: { leader: '@promptdaily', topScore: 90, feeders: 15, signals: 16 },
    feeders: [
      {
        rank: 1,
        handle: '@promptdaily',
        name: 'Prompt Daily',
        initials: 'PD',
        score: 90,
        move: '+2',
        avg: 'Top 11%',
        topPosts: 5,
        comments: '351',
        likes: '6.8K',
        followers: '+1.2K',
        cadence: '10/wk',
        signal: 'Fastest growth',
        proof: 'Follower jump in 7D',
        thumb: 'violet',
      },
      {
        rank: 2,
        handle: '@buildwithai',
        name: 'Build With AI',
        initials: 'BA',
        score: 83,
        move: '+1',
        avg: 'Top 20%',
        topPosts: 3,
        comments: '208',
        likes: '4.7K',
        followers: '+690',
        cadence: '6/wk',
        signal: 'Save-heavy',
        proof: 'How-to posts working',
        thumb: 'indigo',
      },
      {
        rank: 3,
        handle: '@modelnotes',
        name: 'Model Notes',
        initials: 'MN',
        score: 75,
        move: '-3',
        avg: 'Top 31%',
        topPosts: 1,
        comments: '98',
        likes: '2.1K',
        followers: '+140',
        cadence: '2/wk',
        signal: 'Posting gap',
        proof: 'Longest quiet streak',
        thumb: 'slate',
      },
    ],
  },
];

const THUMB_STYLES: Record<string, string> = {
  rose: 'from-rose-500 via-pink-300 to-rose-950',
  plum: 'from-fuchsia-700 via-rose-300 to-zinc-950',
  gold: 'from-amber-300 via-rose-200 to-stone-900',
  black: 'from-zinc-950 via-zinc-700 to-rose-500',
  blue: 'from-blue-700 via-sky-300 to-zinc-950',
  cyan: 'from-cyan-500 via-blue-200 to-slate-950',
  green: 'from-emerald-500 via-lime-200 to-slate-950',
  amber: 'from-amber-500 via-orange-200 to-stone-950',
  red: 'from-red-600 via-orange-300 to-stone-950',
  teal: 'from-teal-500 via-emerald-200 to-slate-950',
  violet: 'from-violet-600 via-fuchsia-300 to-zinc-950',
  indigo: 'from-indigo-700 via-violet-300 to-slate-950',
  slate: 'from-slate-800 via-slate-400 to-black',
};

const TIMEFRAMES = ['7D', '30D', '60D', '90D'] as const;
const SOFT_EASE = [0.16, 0.9, 0.2, 1] as const;
const CARD_STACK_EASE = [0.16, 0.78, 0.18, 1] as const;
const CARD_MASK_EASE = [0.18, 0.86, 0.2, 1] as const;
const CARD_MASK_DURATION = 0.92;
const CARD_MASK_CLEANUP_MS = 1040;
const FEED_SCOPE = '__feed__';
const STORY_RING_RADIUS = 45;
const STORY_RING_CIRCUMFERENCE = 2 * Math.PI * STORY_RING_RADIUS;

function flattenFeeders(feeds: FeedBoard[]) {
  return feeds.flatMap((feed) => feed.feeders.map((feeder) => ({ feed, feeder })));
}

const LADDER_SPRING = { type: 'spring', stiffness: 400, damping: 36, mass: 0.9 } as const;
const HEADER_FEED_SLOT = 94;
const HEADER_CIRCLE_EMERGE_X = -82;
const HEADER_RAIL_SPRING = { type: 'spring', stiffness: 430, damping: 40, mass: 0.9 } as const;
const HEADER_CIRCLE_POP = { type: 'spring', stiffness: 420, damping: 34, mass: 0.82 } as const;
const HEADER_CIRCLE_EXIT = { duration: 0.44, ease: SOFT_EASE } as const;
const HEADER_CHAIN_START = 0.46;
const HEADER_CHAIN_STAGGER = 0.074;
const HEADER_EXIT_STAGGER = 0.058;
const HEADER_RAIL_CSS = `
.fm-lead-feed-badge--traveling{animation:fm-lead-feed-badge-travel 680ms cubic-bezier(.18,.92,.2,1) both}
@keyframes fm-lead-feed-badge-travel{0%{transform:translate3d(var(--lead-feed-travel-x,0px),0,0) scale(.985)}100%{transform:translate3d(0,0,0) scale(1)}}
@media (prefers-reduced-motion:reduce){.fm-lead-feed-badge--traveling{animation:none;transform:none}}
`;

function headerCircleEnterTransition(delay = 0) {
  return {
    x: { ...HEADER_CIRCLE_POP, delay },
    scale: { ...HEADER_CIRCLE_POP, delay },
    opacity: { duration: 0.3, delay: delay + 0.04, ease: SOFT_EASE },
    filter: { duration: 0.34, delay, ease: SOFT_EASE },
  };
}

function headerCircleExitTransition(index = 0) {
  const delay = index * HEADER_EXIT_STAGGER;

  return {
    x: { ...HEADER_CIRCLE_EXIT, delay },
    scale: { ...HEADER_CIRCLE_EXIT, delay },
    opacity: { duration: 0.3, delay: delay + 0.02, ease: SOFT_EASE },
    filter: { duration: 0.38, delay, ease: SOFT_EASE },
  };
}

/* Deterministic per-timeframe shading of the base score so switching windows
   actually re-ranks the ladder and the rows get to move. */
function timeframeScore(feeder: Feeder, timeframe: typeof TIMEFRAMES[number]) {
  if (timeframe === '30D') return feeder.score;
  let hash = 0;
  const seed = `${feeder.handle}:${timeframe}`;
  for (let i = 0; i < seed.length; i++) hash = (hash * 31 + seed.charCodeAt(i)) % 997;
  return Math.max(38, Math.min(99, feeder.score + (hash % 13) - 6));
}

function stablePick(seed: string, variants: string[]) {
  if (variants.length <= 1) return variants[0] || '';
  let hash = 0;
  for (let index = 0; index < seed.length; index += 1) {
    hash = (hash * 31 + seed.charCodeAt(index)) % 997;
  }
  return variants[hash % variants.length] || variants[0] || '';
}

function thumbClass(key: string) {
  return THUMB_STYLES[key] || THUMB_STYLES.rose;
}

function ActiveStoryRingStroke() {
  return (
    <>
      <motion.span
        className="pointer-events-none absolute -inset-1 rounded-full bg-[var(--fm-accent)]/24 blur-md"
        initial={{ opacity: 0, scale: 0.86 }}
        animate={{ opacity: [0, 0.64, 0], scale: [0.86, 1.16, 1.02] }}
        transition={{ duration: 0.9, ease: SOFT_EASE, times: [0, 0.42, 1] }}
      />
      <motion.svg
        className="pointer-events-none absolute inset-0 z-10 h-full w-full overflow-visible"
        viewBox="0 0 100 100"
        aria-hidden="true"
      >
        <g transform="rotate(-90 50 50)">
          <circle
            cx="50"
            cy="50"
            r={STORY_RING_RADIUS}
            fill="none"
            stroke="rgb(var(--fm-accent-rgb)/0.22)"
            strokeWidth="7.5"
          />
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
            style={{ filter: 'drop-shadow(0 3px 7px rgb(var(--fm-accent-rgb)/0.34))' }}
          />
        </g>
      </motion.svg>
    </>
  );
}

type FeedBadgeProps = {
  feed: Pick<FeedBoard, 'name' | 'initials' | 'color' | 'tint'>;
  selected: boolean;
  compact?: boolean;
  feedId?: string;
  travelFromX?: number;
  enterDelay?: number;
  exitIndex?: number;
  exitX?: number;
  reduce?: boolean;
  onClick: (sourceEl: HTMLButtonElement) => void;
};

const FeedBadge = forwardRef<HTMLButtonElement, FeedBadgeProps>(function FeedBadge({
  feed,
  selected,
  compact = false,
  feedId,
  travelFromX,
  enterDelay,
  exitIndex = 0,
  exitX = 28,
  reduce = false,
  onClick,
}, ref) {
  const isTraveling = !reduce && typeof travelFromX === 'number' && Math.abs(travelFromX) > 1;
  const isStagedEnter = !reduce && typeof enterDelay === 'number';

  return (
    <motion.button
      ref={ref}
      type="button"
      layout={isTraveling ? false : 'position'}
      layoutId={!isTraveling && feedId ? `lead-feed-badge-${feedId}` : undefined}
      data-lead-feed-id={feedId}
      initial={
        isStagedEnter
          ? { opacity: 0, x: -8, scale: 0.96, filter: 'blur(6px)' }
          : false
      }
      animate={
        isStagedEnter
          ? { opacity: 1, x: 0, scale: 1, filter: 'blur(0px)' }
          : undefined
      }
      exit={
        reduce
          ? { opacity: 0 }
          : {
              opacity: 0,
              x: exitX,
              scale: 0.96,
              filter: 'blur(6px)',
              transition: headerCircleExitTransition(exitIndex),
            }
      }
      transition={
        isStagedEnter
          ? headerCircleEnterTransition(enterDelay)
          : HEADER_RAIL_SPRING
      }
      whileTap={{ scale: 0.95 }}
      onClick={(event) => onClick(event.currentTarget)}
      style={{
        '--lead-feed-travel-x': isTraveling ? `${travelFromX}px` : undefined,
        willChange: 'transform',
        zIndex: selected || isTraveling ? 3 : 1,
      } as CSSProperties}
      className={cn(
        'flex w-[82px] shrink-0 flex-col items-center gap-[5px] border-0 bg-transparent p-0 text-inherit outline-none [user-select:none] [-webkit-tap-highlight-color:transparent]',
        compact && 'flex-row gap-2',
        isTraveling && 'fm-lead-feed-badge--traveling',
      )}
      aria-pressed={selected}
      aria-label={`Show ${feed.name}`}
    >
      <span
        className={cn(
          'relative grid place-items-center rounded-full p-[5px] transition-[background,box-shadow,width,height] duration-500 ease-out',
          selected
            ? 'h-[76px] w-[76px] bg-[#FFE4EA] shadow-[0_10px_26px_-20px_rgb(var(--fm-accent-rgb)/0.85)] dark:bg-[#3F0F1B]'
            : 'h-[70px] w-[70px] bg-black/[0.07] shadow-none dark:bg-white/[0.11]',
          compact && (selected ? 'h-[64px] w-[64px]' : 'h-[58px] w-[58px]'),
        )}
      >
        {selected && <ActiveStoryRingStroke />}
        <span
          className="relative z-20 grid h-full w-full place-items-center overflow-hidden rounded-full border-[2px] border-white text-[12px] font-black leading-none dark:border-[var(--fm-ink)]"
          style={{
            color: feed.color,
            background: `radial-gradient(circle at 30% 18%, ${feed.tint}, transparent 58%), linear-gradient(135deg,#f7f7f8,#cfd3dc)`,
          }}
        >
          {feed.initials}
        </span>
      </span>
      {!compact && (
        <span className={cn('block w-full overflow-hidden text-ellipsis whitespace-nowrap text-center text-[10px] font-black uppercase leading-none', selected ? 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]' : 'text-black/42 dark:text-white/44')}>
          {feed.name}
        </span>
      )}
      {compact && (
        <span className="hidden min-w-0 text-left sm:block">
          <span className="block text-[11px] font-black uppercase tracking-[0.16em] text-foreground/44">Feed</span>
          <span className="block max-w-[116px] truncate text-[18px] font-black leading-none text-foreground">{feed.name}</span>
        </span>
      )}
    </motion.button>
  );
});

type FeederCircleProps = {
  feeder: Feeder;
  selected: boolean;
  index?: number;
  exitIndex?: number;
  reduce?: boolean;
  onClick: () => void;
};

const FeederCircle = forwardRef<HTMLButtonElement, FeederCircleProps>(function FeederCircle({
  feeder,
  selected,
  index = 0,
  exitIndex = index,
  reduce = false,
  onClick,
}, ref) {
  const delay = HEADER_CHAIN_START + index * HEADER_CHAIN_STAGGER;
  const emergeX = HEADER_CIRCLE_EMERGE_X;

  return (
    <motion.button
      ref={ref}
      type="button"
      layout="position"
      initial={reduce ? false : { opacity: 0, x: emergeX, scale: 0.88, filter: 'blur(8px)' }}
      animate={reduce ? { opacity: 1 } : { opacity: 1, x: 0, scale: 1, filter: 'blur(0px)' }}
      exit={
        reduce
          ? { opacity: 0 }
          : {
              opacity: 0,
              x: emergeX,
              scale: 0.92,
              filter: 'blur(8px)',
              transition: headerCircleExitTransition(exitIndex),
            }
      }
      transition={reduce ? { duration: 0 } : headerCircleEnterTransition(delay)}
      whileTap={{ scale: 0.95 }}
      onClick={onClick}
      className="flex w-[82px] shrink-0 flex-col items-center gap-[5px] border-0 bg-transparent p-0 text-inherit outline-none [user-select:none] [-webkit-tap-highlight-color:transparent]"
      aria-pressed={selected}
      aria-label={`Spotlight ${feeder.handle}`}
    >
      <span
        className={cn(
          'relative grid place-items-center rounded-full p-[5px] transition-[background,box-shadow,width,height] duration-500 ease-out',
          selected
            ? 'h-[76px] w-[76px] bg-[#FFE4EA] shadow-[0_10px_26px_-20px_rgb(var(--fm-accent-rgb)/0.85)] dark:bg-[#3F0F1B]'
            : 'h-[70px] w-[70px] bg-black/[0.07] shadow-none dark:bg-white/[0.11]',
        )}
      >
        {selected && <ActiveStoryRingStroke />}
        <span className="relative z-20 flex h-full w-full items-center justify-center overflow-hidden rounded-full border-[2px] border-white bg-[linear-gradient(135deg,#fce7f3,#fff1f2)] text-[var(--fm-accent-deeper)] dark:border-[var(--fm-ink)] dark:bg-[linear-gradient(135deg,#1c1917,#18181b)] dark:text-[var(--fm-accent-soft)]">
          <FeederStoryAvatar feeder={{ handle: feeder.handle.replace(/^@+/, ''), profilePicUrl: feeder.profilePicUrl }} />
        </span>
      </span>
      <span className={cn('block w-full overflow-hidden text-ellipsis whitespace-nowrap text-center text-[10px] font-black lowercase leading-none', selected ? 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]' : 'text-black/42 dark:text-white/44')}>
        {feeder.handle.replace('@', '')}
      </span>
    </motion.button>
  );
});

type FeedScopeCircleProps = {
  feed: FeedBoard;
  selected: boolean;
  index?: number;
  exitIndex?: number;
  reduce?: boolean;
  onClick: () => void;
};

const FeedScopeCircle = forwardRef<HTMLButtonElement, FeedScopeCircleProps>(function FeedScopeCircle({
  feed,
  selected,
  index = 0,
  exitIndex = index,
  reduce = false,
  onClick,
}, ref) {
  const delay = HEADER_CHAIN_START + index * HEADER_CHAIN_STAGGER;
  const emergeX = HEADER_CIRCLE_EMERGE_X;

  return (
    <motion.button
      ref={ref}
      type="button"
      layout="position"
      initial={reduce ? false : { opacity: 0, x: emergeX, scale: 0.88, filter: 'blur(8px)' }}
      animate={reduce ? { opacity: 1 } : { opacity: 1, x: 0, scale: 1, filter: 'blur(0px)' }}
      exit={
        reduce
          ? { opacity: 0 }
          : {
              opacity: 0,
              x: emergeX,
              scale: 0.92,
              filter: 'blur(8px)',
              transition: headerCircleExitTransition(exitIndex),
            }
      }
      transition={reduce ? { duration: 0 } : headerCircleEnterTransition(delay)}
      whileTap={{ scale: 0.95 }}
      onClick={onClick}
      className="flex w-[82px] shrink-0 flex-col items-center gap-[5px] border-0 bg-transparent p-0 text-inherit outline-none [user-select:none] [-webkit-tap-highlight-color:transparent]"
      aria-pressed={selected}
      aria-label={`Show all feeders in ${feed.name}`}
    >
      <span
        className={cn(
          'relative grid place-items-center rounded-full p-[5px] transition-[background,box-shadow,width,height] duration-500 ease-out',
          selected
            ? 'h-[76px] w-[76px] bg-[#FFE4EA] shadow-[0_10px_26px_-20px_rgb(var(--fm-accent-rgb)/0.85)] dark:bg-[#3F0F1B]'
            : 'h-[70px] w-[70px] bg-black/[0.07] shadow-none dark:bg-white/[0.11]',
        )}
      >
        {selected && <ActiveStoryRingStroke />}
        <span className="relative z-20 grid h-full w-full place-items-center overflow-hidden rounded-full border-[2px] border-white bg-[linear-gradient(135deg,#fce7f3,#fff1f2)] text-[var(--fm-accent-deeper)] dark:border-[var(--fm-ink)] dark:bg-[linear-gradient(135deg,#1c1917,#18181b)] dark:text-[var(--fm-accent-soft)]">
          <Users size={21} strokeWidth={2.8} />
        </span>
      </span>
      <span className={cn('block w-full overflow-hidden text-ellipsis whitespace-nowrap text-center text-[10px] font-black lowercase leading-none', selected ? 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]' : 'text-black/42 dark:text-white/44')}>
        {feed.name}
      </span>
    </motion.button>
  );
});

function Thumb({ tone, label, className = '' }: { tone: string; label?: string; className?: string }) {
  return (
    <div className={cn('relative overflow-hidden rounded-[18px] bg-gradient-to-br', thumbClass(tone), className)}>
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_24%_18%,rgba(255,255,255,0.58),transparent_24%),linear-gradient(160deg,transparent,rgba(0,0,0,0.56))]" />
      <div className="absolute inset-x-0 bottom-0 p-2.5">
        {label && (
          <span className="inline-flex rounded-full border border-white/20 bg-black/44 px-2 py-1 text-[8px] font-black uppercase tracking-[0.12em] text-white shadow-[0_8px_20px_-12px_rgba(0,0,0,0.8)] backdrop-blur-md">
            {label}
          </span>
        )}
      </div>
    </div>
  );
}

function moveValue(move: string) {
  return Number.parseInt(move, 10) || 0;
}

function feedMove(feed: FeedBoard) {
  return feed.feeders.reduce((sum, feeder) => sum + moveValue(feeder.move), 0);
}

function feedPeriodScore(feed: FeedBoard, timeframe: typeof TIMEFRAMES[number]) {
  const scores = feed.feeders
    .map((feeder) => timeframeScore(feeder, timeframe))
    .sort((a, b) => b - a)
    .slice(0, 3);
  return Math.round(scores.reduce((sum, score) => sum + score, 0) / Math.max(1, scores.length));
}

function topPercentFromScore(score: number) {
  return Math.max(1, Math.min(99, Math.round(100 - score)));
}

function timeframeWeeks(timeframe: typeof TIMEFRAMES[number]) {
  const days = Number.parseInt(timeframe, 10) || 30;
  return days / 7;
}

function cadencePerWeek(cadence: string) {
  const parsed = Number.parseFloat(cadence);
  return Number.isFinite(parsed) ? parsed : 0;
}

function periodPostCount(feeder: Feeder, timeframe: typeof TIMEFRAMES[number]) {
  return Math.max(1, Math.round(cadencePerWeek(feeder.cadence) * timeframeWeeks(timeframe)));
}

function feedPeriodPostCount(feed: FeedBoard, timeframe: typeof TIMEFRAMES[number]) {
  return feed.feeders.reduce((sum, feeder) => sum + periodPostCount(feeder, timeframe), 0);
}

function compactNumber(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '--';
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(value >= 10_000_000 ? 0 : 1)}M`;
  if (value >= 1_000) return `${(value / 1_000).toFixed(value >= 100_000 ? 0 : 1)}K`;
  return String(Math.round(value));
}

function metricNumber(value: string | number | null | undefined) {
  if (typeof value === 'number') return Number.isFinite(value) ? value : 0;
  if (!value) return 0;
  return numericMetric(String(value));
}

function titleCase(value: string) {
  return value
    .toLowerCase()
    .split(/\s+/)
    .filter(Boolean)
    .map((part) => part === '&' ? part : part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ');
}

function initialsFrom(value: string, fallback = 'FM') {
  const cleaned = value.replace(/^@+/, '').replace(/[^a-z0-9&\s]/gi, ' ').trim();
  const words = cleaned.split(/\s+/).filter(Boolean);
  if (words.length === 0) return fallback;
  if (words.length === 1) return words[0].slice(0, 3).toUpperCase();
  return words.map((word) => word[0]).join('').slice(0, 3).toUpperCase();
}

function formatMetricValue(value: number) {
  if (!Number.isFinite(value) || value <= 0) return '0';
  return compactNumber(value);
}

function liveScoreFromMetrics(likes: number, comments: number, views: number, posts: number, rankIndex: number) {
  const engagement = likes + comments * 4 + views * 0.015;
  const density = engagement / Math.max(1, posts);
  const score = 48 + Math.log10(Math.max(1, density)) * 10 - rankIndex * 2;
  return Math.max(52, Math.min(98, Math.round(score)));
}

function feedTone(index: number) {
  return ['rose', 'blue', 'amber', 'violet', 'cyan', 'green', 'plum', 'black'][index % 8];
}

function feedColor(index: number) {
  return ['#E11D48', '#2563EB', '#F59E0B', '#7C3AED', '#0891B2', '#16A34A', '#C026D3', '#475569'][index % 8];
}

function apiFeedsToBoards(apiFeeds: FeedApiFeed[]) {
  const boards = apiFeeds
    .map((apiFeed, feedIndex): FeedBoard | null => {
      const id = String(apiFeed.id || '').trim();
      const rawName = String(apiFeed.title || id || `Feed ${feedIndex + 1}`).trim();
      const name = titleCase(rawName);
      const color = feedColor(feedIndex);
      const apiFeeders = Array.isArray(apiFeed.feeders) ? apiFeed.feeders : [];
      const rankedApiFeeders = [...apiFeeders].sort((a, b) => {
        const am = a.metrics || {};
        const bm = b.metrics || {};
        return (
          metricNumber(bm.comments) + metricNumber(bm.likes) * 0.1 + metricNumber(bm.postsTracked) * 3
          - (metricNumber(am.comments) + metricNumber(am.likes) * 0.1 + metricNumber(am.postsTracked) * 3)
        );
      });

      const feeders = rankedApiFeeders.map((apiFeeder, feederIndex): Feeder => {
        const rawHandle = String(apiFeeder.handle || `feeder${feederIndex + 1}`).trim();
        const handle = rawHandle.startsWith('@') ? rawHandle : `@${rawHandle}`;
        const metrics = apiFeeder.metrics || {};
        const likes = metricNumber(metrics.likes);
        const comments = metricNumber(metrics.comments);
        const views = metricNumber(metrics.views);
        const posts = metricNumber(metrics.postsTracked);
        const score = liveScoreFromMetrics(likes, comments, views, posts, feederIndex);
        const followerCount = Number(apiFeeder.followerCount || 0);

        return {
          handle,
          name: titleCase(handle.replace(/^@+/, '').replace(/[._-]+/g, ' ')),
          initials: initialsFrom(handle, `F${feederIndex + 1}`),
          profilePicUrl: typeof apiFeeder.profilePicUrl === 'string' ? apiFeeder.profilePicUrl : null,
          thumbnailUrl: typeof apiFeeder.thumbnailUrl === 'string' ? apiFeeder.thumbnailUrl : null,
          rank: feederIndex + 1,
          score,
          move: feederIndex === 0 ? '+2' : feederIndex % 3 === 0 ? '+1' : '0',
          avg: `Top ${Math.max(2, 100 - score)}%`,
          topPosts: Math.max(1, Math.round(posts * 0.18)),
          comments: formatMetricValue(comments),
          likes: formatMetricValue(likes),
          followers: followerCount > 0 ? formatMetricValue(followerCount) : '--',
          cadence: `${Math.max(1, Math.round(posts / 4))}/wk`,
          signal: apiFeeder.isAnchor ? 'Anchor feeder' : comments >= likes * 0.08 ? 'Comment leader' : posts > 0 ? 'Active feeder' : 'Awaiting reads',
          proof: comments > 0 ? `${formatMetricValue(comments)} comments tracked` : `${Math.round(posts)} posts tracked`,
          thumb: feedTone(feedIndex + feederIndex),
        };
      });

      if (!id || feeders.length === 0) return null;

      const leader = feeders[0];
      return {
        id,
        name,
        initials: initialsFrom(name),
        color,
        tint: `rgb(var(--fm-accent-rgb) / ${feedIndex === 0 ? '0.12' : '0.08'})`,
        feeders,
        summary: {
          leader: leader?.handle || '@feed',
          topScore: leader?.score || 0,
          feeders: feeders.length,
          signals: feeders.reduce((sum, feeder) => sum + Math.max(1, feeder.topPosts), 0),
        },
      };
    })
    .filter((feed): feed is FeedBoard => Boolean(feed));

  return boards.length > 0 ? boards : FEEDS;
}

function scopeHandle(activeFeed: FeedBoard | null, selectedFeeder: string) {
  if (!activeFeed) return '@all';
  if (selectedFeeder === FEED_SCOPE) return `@${activeFeed.name.toLowerCase().replace(/[^a-z0-9]+/g, '')}`;
  return selectedFeeder;
}

function selectedPool(feeds: FeedBoard[], activeFeed: FeedBoard | null, selectedFeeder: string) {
  if (!activeFeed) return flattenFeeders(feeds);
  if (selectedFeeder === FEED_SCOPE) return activeFeed.feeders.map((feeder) => ({ feed: activeFeed, feeder }));
  const exact = activeFeed.feeders.find((feeder) => feeder.handle === selectedFeeder);
  return (exact ? [exact] : activeFeed.feeders).map((feeder) => ({ feed: activeFeed, feeder }));
}

function numericMetric(value: string) {
  const multiplier = value.toUpperCase().includes('K') ? 1000 : 1;
  const parsed = Number.parseFloat(value.replace(/[^\d.]/g, ''));
  return Number.isFinite(parsed) ? parsed * multiplier : 0;
}

function largeNumericMetric(value: string) {
  const upper = value.toUpperCase();
  const multiplier = upper.includes('M') ? 1_000_000 : upper.includes('K') ? 1000 : 1;
  const parsed = Number.parseFloat(value.replace(/[^\d.]/g, ''));
  return Number.isFinite(parsed) ? parsed * multiplier : 0;
}

function signedCompactNumber(value: number) {
  const rounded = Math.round(value);
  const sign = rounded > 0 ? '+' : rounded < 0 ? '-' : '';
  const abs = Math.abs(rounded);
  const body = abs >= 1_000_000
    ? `${(abs / 1_000_000).toFixed(abs >= 10_000_000 ? 0 : 1)}M`
    : abs >= 1_000
      ? `${(abs / 1_000).toFixed(abs >= 100_000 ? 0 : 1)}K`
      : String(abs);
  return `${sign}${body}`;
}

function SlotSwap({
  changeKey,
  reduce,
  delay = 0,
  className,
  children,
}: {
  changeKey: string;
  reduce: boolean;
  delay?: number;
  className?: string;
  children: ReactNode;
}) {
  return (
    <AnimatePresence mode="wait" initial={false}>
      <motion.div
        key={changeKey}
        initial={reduce ? false : { opacity: 0, y: 10, filter: 'blur(7px)' }}
        animate={reduce ? { opacity: 1 } : { opacity: 1, y: 0, filter: 'blur(0px)' }}
        exit={reduce ? { opacity: 0 } : { opacity: 0, y: -8, filter: 'blur(5px)' }}
        transition={reduce ? { duration: 0 } : { delay, duration: 0.38, ease: SOFT_EASE }}
        className={className}
      >
        {children}
      </motion.div>
    </AnimatePresence>
  );
}

function MoveChip({ move, className }: { move: string; className?: string }) {
  const up = move.startsWith('+');
  const down = move.startsWith('-');
  return (
    <span
      className={cn(
        'inline-flex items-center gap-0.5 text-[10px] font-black tabular-nums leading-none',
        up ? 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]' : down ? 'text-foreground/36' : 'text-foreground/44',
        className,
      )}
    >
      <span className="text-[8px]">{up ? '▲' : down ? '▼' : '–'}</span>
      {move.replace(/[+-]/, '')}
    </span>
  );
}

const INDEX_TONES = ['text-white', 'text-white/78', 'text-white/58', 'text-white/44'];

function StandingIndex({
  blocks,
  reduce,
}: {
  blocks: Array<{ label: string; value: number; hot?: boolean }>;
  reduce: boolean;
}) {
  const sorted = [...blocks].sort((a, b) => a.value - b.value);
  const [feature, ...rest] = sorted;
  const featureDepth = Math.max(1, Math.round(feature.value));
  const mobileBlocks = sorted.filter((block) => block.label !== 'comments').slice(0, 4);

  return (
    <div className="flex min-w-0 flex-1 flex-col">
      <motion.div
        initial={reduce ? false : { opacity: 0, y: 14, filter: 'blur(6px)' }}
        animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
        transition={reduce ? { duration: 0 } : { delay: 0.14, duration: 0.46, ease: SOFT_EASE }}
        className="hidden pb-4 sm:block"
      >
        <div className="inline-flex rounded-full bg-[rgb(var(--fm-accent-rgb)/0.16)] px-2 py-1 text-[8px] font-black uppercase tracking-[0.22em] text-[var(--fm-accent-bright)] sm:bg-transparent sm:px-0 sm:py-0 sm:text-[9px]">{feature.label}</div>
        <div className="mt-2 font-black tabular-nums leading-[0.76] text-white fm-depth-title text-[clamp(46px,15vw,68px)] drop-shadow-[0_0_18px_rgb(var(--fm-accent-rgb)/0.32)] sm:mt-2.5 sm:text-[clamp(48px,7vw,72px)] sm:drop-shadow-none">
          <span className="mr-[0.16em] align-[0.42em] text-[0.26em] text-white/38">TOP</span>
          {featureDepth}
          <span className="text-[0.4em] text-[var(--fm-accent-bright)]">%</span>
        </div>
        <div className="mt-2 text-[11px] font-black leading-snug text-white/54 sm:mt-2.5 sm:text-[11.5px] sm:font-extrabold sm:text-white/46">
          beat {100 - featureDepth}% of the stream this window
        </div>
      </motion.div>

      <div className="grid grid-cols-2 gap-1.5 sm:hidden">
        {mobileBlocks.map((block, index) => {
          const depth = Math.max(1, Math.round(block.value));
          return (
            <motion.div
              key={`mobile-${block.label}`}
              initial={reduce ? false : { opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={reduce ? { duration: 0 } : { delay: 0.18 + index * 0.06, duration: 0.34, ease: SOFT_EASE }}
              className="grid min-h-[50px] grid-cols-[minmax(0,1fr)_auto] items-center gap-2 rounded-[13px] border border-white/[0.075] bg-[#101014] px-2.5 py-2 shadow-[0_1px_0_rgba(255,255,255,0.035)]"
            >
              <span className="min-w-0 truncate text-[8px] font-black uppercase tracking-[0.14em] text-white/52">{block.label}</span>
              <span className="shrink-0 font-black tabular-nums leading-none text-white" style={{ fontSize: `${Math.max(18, 24 - index * 2)}px` }}>
                <span className="mr-[0.12em] align-[0.3em] text-[0.38em] text-white/28">TOP</span>
                {depth}
                <span className="text-[0.42em] text-white/50">%</span>
              </span>
            </motion.div>
          );
        })}
      </div>

      <div className="mt-auto hidden divide-y divide-white/[0.06] pt-0 sm:block">
        {rest.map((block, index) => {
          const depth = Math.max(1, Math.round(block.value));
          return (
            <motion.div
              key={block.label}
              initial={reduce ? false : { opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              transition={reduce ? { duration: 0 } : { delay: 0.26 + index * 0.08, duration: 0.36, ease: SOFT_EASE }}
              className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-2 rounded-[15px] border border-white/[0.055] bg-white/[0.06] px-2.5 py-2.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.05)] sm:gap-3 sm:rounded-none sm:border-0 sm:bg-transparent sm:px-0 sm:py-3 sm:shadow-none"
            >
              <span className="min-w-0">
                <span className="block truncate text-[8px] font-black uppercase tracking-[0.18em] text-white/56 sm:text-[8.5px] sm:tracking-[0.2em] sm:text-white/44">{block.label}</span>
                <span className="mt-1 hidden text-[10px] font-extrabold text-white/28 sm:block">beats {100 - depth}%</span>
              </span>
              <span className={cn('shrink-0 font-black tabular-nums leading-none text-white', INDEX_TONES[index] || INDEX_TONES[3])} style={{ fontSize: `${Math.max(20, 30 - index * 3)}px` }}>
                <span className="mr-[0.14em] align-[0.32em] text-[0.4em] text-white/32">TOP</span>
                {depth}
                <span className="text-[0.44em] text-white/32">%</span>
              </span>
            </motion.div>
          );
        })}
      </div>
    </div>
  );
}

type MobileInsightPair = {
  key: string;
  title: string;
  subtitle: string;
  values: Array<{
    label: string;
    value: number | string;
    detail?: string;
    prefix?: string;
    suffix?: string;
    hot?: boolean;
  }>;
};

function MobilePostMortemInsights({
  pairs,
  reduce,
  slotKey,
}: {
  pairs: MobileInsightPair[];
  reduce: boolean;
  slotKey: string;
}) {
  const [pairState, setPairState] = useState({ slotKey, index: 0 });
  const activePairIndex = pairState.slotKey === slotKey ? pairState.index : 0;
  const activePair = pairs[activePairIndex % Math.max(1, pairs.length)];

  useEffect(() => {
    if (pairs.length <= 1) return undefined;

    const interval = window.setInterval(() => {
      setPairState((current) => {
        const currentIndex = current.slotKey === slotKey ? current.index : 0;
        return { slotKey, index: (currentIndex + 1) % pairs.length };
      });
    }, 7000);

    return () => window.clearInterval(interval);
  }, [pairs.length, slotKey]);

  if (!activePair) return null;

  return (
    <div className="grid grid-cols-2 gap-2">
      {[0, 1].map((metricIndex) => {
        const metric = activePair.values[metricIndex];
        if (!metric) return null;

        return (
          <motion.div
            key={`mobile-insight-slot:${metricIndex}`}
            layout
            data-testid="mobile-post-mortem-insight"
            className="relative min-h-[124px] min-w-0 overflow-hidden rounded-[22px] border border-white/[0.075] bg-white/[0.045] p-3.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.05)]"
            transition={reduce ? { duration: 0 } : { duration: 0.42, ease: SOFT_EASE }}
          >
            <div className="absolute inset-0 flex flex-col justify-between p-3.5">
              <div className="min-w-0">
                <SlottedText
                  value={metricIndex === 0 ? activePair.title : activePair.subtitle}
                  reduce={reduce}
                  className={cn('text-[8px] font-black uppercase tracking-[0.17em] text-white/34', metric.hot && 'text-[var(--fm-accent-bright)]')}
                />
                <div className="mt-1 min-w-0 truncate text-[10px] font-black uppercase tracking-[0.13em] text-white/52">
                  <SlottedText value={metric.label} reduce={reduce} />
                </div>
              </div>
              <div className="min-w-0">
                <div className={cn('flex items-baseline font-black tabular-nums leading-none text-white', metric.hot && 'text-[var(--fm-accent-bright)]')}>
                  {metric.prefix && <span className="mr-1.5 text-[9px] text-white/30">{metric.prefix}</span>}
                  <SlottedText
                    value={`${metric.value}`}
                    reduce={reduce}
                    className="text-[42px]"
                    ariaLabel={`${metric.value}`}
                  />
                  {metric.suffix && (
                    <span className={cn('-ml-1 text-[30px] leading-none', metric.hot ? 'text-[var(--fm-accent-bright)]' : 'text-white/42')}>
                      {metric.suffix}
                    </span>
                  )}
                </div>
                {metric.detail && (
                  <div className="mt-1 min-w-0 truncate text-[8px] font-black uppercase tracking-[0.12em] text-white/28">
                    <SlottedText value={metric.detail} reduce={reduce} />
                  </div>
                )}
              </div>
            </div>
          </motion.div>
        );
      })}
    </div>
  );
}

type PostCardEntry = { feed: FeedBoard; feeder: Feeder };
type PostCardExit = { index: number; key: number };

function stackThumbKey(entry: PostCardEntry) {
  return `${entry.feed.id}:${entry.feeder.handle.replace(/^@+/, '').toLowerCase()}`;
}

function isNumericFeedId(value: string) {
  return Number.isFinite(Number(value)) && Number(value) > 0;
}

function PostStackCardFace({
  entry,
  index,
  overlay,
  active,
  reduce,
}: {
  entry: PostCardEntry;
  index: number;
  overlay: number;
  active: boolean;
  reduce: boolean;
}) {
  const percentile = Math.max(1, Math.round(2 + index * 4 + (100 - entry.feeder.score) * 0.18));
  const metric = index % 3 === 0 ? entry.feeder.comments : index % 3 === 1 ? entry.feeder.likes : entry.feeder.followers;
  const metricLabel = index % 3 === 0 ? 'comments' : index % 3 === 1 ? 'likes' : 'followers';
  const imageUrl = entry.feeder.thumbnailUrl;

  return (
    <>
      {imageUrl ? (
        // eslint-disable-next-line @next/next/no-img-element -- remote feeder media
        <img src={imageUrl} alt="" className="absolute inset-0 h-full w-full rounded-[28px] object-cover" loading="lazy" decoding="async" />
      ) : (
        <Thumb tone={entry.feeder.thumb} className="absolute inset-0 rounded-[28px]" />
      )}
      <motion.div
        aria-hidden="true"
        className="absolute inset-0 bg-black"
        initial={false}
        animate={{ opacity: overlay }}
        transition={reduce ? { duration: 0 } : { duration: 0.86, ease: CARD_STACK_EASE }}
      />
      <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.02),rgba(0,0,0,0.82)_78%)]" />
      <motion.div
        className="relative z-10 flex h-full flex-col justify-between p-4 text-white sm:p-5"
        initial={false}
        animate={{
          opacity: active ? 1 : 0.34,
          y: active ? 0 : 10,
        }}
        transition={reduce ? { duration: 0 } : { duration: 0.32, ease: SOFT_EASE }}
      >
        <div className="flex items-start justify-between gap-3">
          <span className="rounded-full bg-white/14 px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] backdrop-blur-md">
            {entry.feed.name}
          </span>
          <span className="rounded-full bg-[#F71852] px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.12em] shadow-[0_10px_22px_-14px_rgba(247,24,82,0.9)]">
            D{index === 1 ? 1 : 3}
          </span>
        </div>

        <div>
          <div className="text-[64px] font-black leading-[0.78] tracking-normal sm:text-[70px]">
            {percentile}
            <span className="text-[0.34em] text-[#F71852]">%</span>
          </div>
          <div className="mt-2 truncate text-[19px] font-black leading-none sm:text-[18px]">{entry.feeder.handle}</div>
          <div className="mt-2 line-clamp-2 text-[12px] font-extrabold leading-snug text-white/68">{entry.feeder.proof}</div>
          <div className="mt-4 grid grid-cols-2 gap-2">
            <span className="rounded-[16px] border border-white/10 bg-white/12 p-3 backdrop-blur-md">
              <span className="block text-[20px] font-black leading-none sm:text-[22px]">{metric}</span>
              <span className="mt-1 block text-[8px] font-black uppercase tracking-[0.13em] text-white/48">{metricLabel}</span>
            </span>
            <span className="rounded-[16px] border border-white/10 bg-white/12 p-3 backdrop-blur-md">
              <span className="block text-[20px] font-black leading-none sm:text-[22px]">{topPercentFromScore(entry.feeder.score)}%</span>
              <span className="mt-1 block text-[8px] font-black uppercase tracking-[0.13em] text-white/48">top</span>
            </span>
          </div>
        </div>
      </motion.div>
    </>
  );
}

function PostCardStack({
  cards,
}: {
  cards: PostCardEntry[];
}) {
  const reduce = Boolean(useReducedMotion());
  const [stackState, setStackState] = useState<{ signature: string; index: number; exits: PostCardExit[]; exitKey: number }>({
    signature: '',
    index: 0,
    exits: [],
    exitKey: 0,
  });
  const visibleCards = cards.slice(0, 5);
  const count = visibleCards.length;
  const cardSignature = visibleCards.map((entry) => `${entry.feed.id}:${entry.feeder.handle}`).join('|');
  const activeIndex = count > 0 && stackState.signature === cardSignature ? stackState.index % count : 0;
  const exitingCards = !reduce && count > 0 && stackState.signature === cardSignature
    ? stackState.exits.map((exit) => ({ ...exit, index: exit.index % count, entry: visibleCards[exit.index % count] }))
    : [];
  const latestExitingIndex = exitingCards[exitingCards.length - 1]?.index ?? null;

  const advanceStack = () => {
    if (count <= 1) return;
    setStackState((current) => {
      const currentIndex = current.signature === cardSignature ? current.index % count : 0;
      const nextExitKey = current.signature === cardSignature ? current.exitKey + 1 : 1;
      const exits = current.signature === cardSignature ? current.exits : [];
      return {
        signature: cardSignature,
        index: (currentIndex + 1) % count,
        exits: [...exits, { index: currentIndex, key: nextExitKey }].slice(-3),
        exitKey: nextExitKey,
      };
    });
  };

  useEffect(() => {
    if (reduce || stackState.signature !== cardSignature || stackState.exits.length === 0) return undefined;

    const exitKey = stackState.exitKey;
    const timeout = window.setTimeout(() => {
      setStackState((current) => (
        current.exitKey === exitKey
          ? { ...current, exits: [] }
          : current
      ));
    }, CARD_MASK_CLEANUP_MS);

    return () => window.clearTimeout(timeout);
  }, [cardSignature, reduce, stackState.exitKey, stackState.exits.length, stackState.signature]);

  if (count === 0) return null;

  return (
    <div className="min-w-0">
      <div className="mb-3 flex items-center justify-between gap-3">
        <div>
          <div className="text-[10px] font-black uppercase tracking-[0.2em] text-white/36">Top Fire Cards</div>
          <div className="mt-1 text-[14px] font-black leading-none text-white/78">Today&apos;s stack</div>
        </div>
        <motion.span
          key={activeIndex}
          initial={reduce ? false : { y: 6, opacity: 0, filter: 'blur(6px)' }}
          animate={{ y: 0, opacity: 1, filter: 'blur(0px)' }}
          transition={{ duration: 0.34, ease: SOFT_EASE }}
          className="inline-flex rounded-full border border-white/10 bg-white/[0.07] px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.13em] text-white/44"
        >
          {activeIndex + 1}/{count}
        </motion.span>
      </div>

      <motion.button
        type="button"
        onClick={advanceStack}
        whileTap={reduce ? undefined : { scale: 0.986 }}
        transition={{ duration: 0.16, ease: SOFT_EASE }}
        className="relative block h-[318px] w-full min-w-0 touch-manipulation overflow-hidden rounded-[28px] bg-transparent outline-none sm:h-[340px] md:h-[360px] lg:h-[clamp(300px,30vw,380px)] lg:min-h-0"
        aria-label="Peek top fire cards"
        data-testid="lead-card-stack"
      >
        <motion.span
          aria-hidden="true"
          className="absolute bottom-4 left-[5%] h-14 w-[82%] rounded-full bg-black/62 blur-2xl"
          animate={reduce ? undefined : { opacity: [0.2, 0.34, 0.2], scaleX: [0.92, 1, 0.92] }}
          transition={{ duration: 1.6, repeat: Infinity, ease: SOFT_EASE }}
        />
        {visibleCards.map((entry, index) => {
          const order = (index - activeIndex + count) % count;
          const isFront = order === 0;
          const isNext = order === 1;
          const isSecondNext = order === 2;
          const isThirdNext = order === 3;
          const isCoveredByExit = latestExitingIndex === index && index !== activeIndex;
          const pose = isFront
            ? { x: '0%', y: 0, rotate: 0, scale: 1, opacity: 1, zIndex: 40, overlay: 0 }
            : isNext
              ? { x: '26%', y: 0, rotate: 0, scale: 0.72, opacity: 0.94, zIndex: 30, overlay: 0.34 }
              : isSecondNext
                ? { x: '52%', y: 28, rotate: 0, scale: 0.64, opacity: 0.8, zIndex: 24, overlay: 0.5 }
                : isThirdNext
                  ? { x: '76%', y: 56, rotate: 0, scale: 0.56, opacity: 0.54, zIndex: 16, overlay: 0.64 }
                  : { x: '88%', y: 56, rotate: 0, scale: 0.54, opacity: 0, zIndex: 8, overlay: 0.7 };
          const cardTransition = reduce
            ? { duration: 0 }
            : isCoveredByExit
              ? { duration: 0 }
              : exitingCards.length > 0
                ? { duration: CARD_MASK_DURATION, ease: CARD_MASK_EASE }
                : isFront
                  ? { duration: 0.72, ease: CARD_STACK_EASE }
                  : { duration: 0.96, ease: CARD_STACK_EASE };

          return (
            <motion.div
              key={`${entry.feed.id}:${entry.feeder.handle}`}
              data-stack-card
              data-stack-order={order}
              data-stack-front={isFront ? 'true' : 'false'}
              className={cn(
                'absolute bottom-1.5 left-0 top-1.5 w-[84%] origin-center overflow-hidden rounded-[28px] border bg-gradient-to-br text-left shadow-[0_34px_78px_-36px_rgba(0,0,0,0.98)] md:w-[82%]',
                isCoveredByExit ? 'border-transparent' : 'border-white/12',
              )}
              initial={false}
              animate={{
                x: pose.x,
                y: pose.y,
                rotate: pose.rotate,
                scale: pose.scale,
                opacity: isCoveredByExit ? 0 : pose.opacity,
                zIndex: pose.zIndex,
              }}
              transition={cardTransition}
              style={{
                pointerEvents: isFront ? 'auto' : 'none',
                boxShadow: isCoveredByExit
                  ? 'none'
                  : isFront
                  ? '0 30px 70px -34px rgba(0,0,0,0.98), 0 16px 38px -28px rgba(247,24,82,0.72)'
                  : '0 24px 48px -32px rgba(0,0,0,0.95)',
              }}
            >
              <PostStackCardFace entry={entry} index={index} overlay={pose.overlay} active={isFront} reduce={reduce} />
            </motion.div>
          );
        })}
        {exitingCards.map(({ entry, index, key }) => (
          <motion.div
            key={`exit:${key}:${entry.feed.id}:${entry.feeder.handle}`}
            data-stack-exit-card
            className="absolute bottom-1.5 left-0 top-1.5 w-[84%] origin-center overflow-hidden rounded-[28px] border border-transparent bg-gradient-to-br text-left md:w-[82%]"
            initial={{ x: '0%', y: 0, rotate: 0, scale: 1, opacity: 1, zIndex: 60 }}
            animate={{ x: '-104%', y: 0, rotate: 0, scale: 1, opacity: 1, zIndex: 60 }}
            transition={{ duration: CARD_MASK_DURATION, ease: CARD_MASK_EASE }}
            style={{ pointerEvents: 'none', boxShadow: 'none' }}
          >
            <PostStackCardFace entry={entry} index={index} overlay={0} active reduce={reduce} />
          </motion.div>
        ))}
      </motion.button>
    </div>
  );
}

function splitHeroHandle(value: string) {
  return [value];
}

const HERO_TITLE_SLOT_STAGGER_MS = 22;
const HERO_TITLE_SLOT_SETTLE_MS = 780;
const HERO_NBSP = '\u00a0';
const HERO_TITLE_SLOT_CSS = `
.fm-lead-title-slot__line{display:block;max-width:100%;white-space:nowrap}
.fm-lead-title-slot{display:inline-flex;max-width:100%;align-items:baseline;white-space:pre}
.fm-lead-title-slot__slot{position:relative;display:inline-flex;height:1.08em;flex:none;justify-content:center;overflow:hidden;line-height:1;vertical-align:baseline}
.fm-lead-title-slot__sizer,.fm-lead-title-slot__static{white-space:pre}
.fm-lead-title-slot__sizer{visibility:hidden}
.fm-lead-title-slot__face{position:absolute;inset:0;display:flex;align-items:center;justify-content:center;white-space:pre}
.fm-lead-title-slot__face--accent{color:var(--fm-accent-bright)}
.fm-lead-title-slot__face--old{display:none;opacity:0}
.fm-lead-title-slot__face--new{opacity:1;transform:translateY(0)}
.fm-lead-title-slot__slot--changed .fm-lead-title-slot__face{will-change:transform,opacity}
.fm-lead-title-slot__slot--changed .fm-lead-title-slot__face--old{display:flex;animation:fm-lead-title-slot-old 520ms cubic-bezier(.34,1.56,.64,1) both;animation-delay:var(--title-slot-delay,0ms)}
.fm-lead-title-slot__slot--changed .fm-lead-title-slot__face--new{animation:fm-lead-title-slot-new 520ms cubic-bezier(.34,1.56,.64,1) both;animation-delay:calc(var(--title-slot-delay,0ms) + 34ms)}
@keyframes fm-lead-title-slot-old{0%{opacity:1;transform:translateY(0) rotate(0deg)}100%{opacity:0;transform:translateY(118%) rotate(1.2deg)}}
@keyframes fm-lead-title-slot-new{0%{opacity:0;transform:translateY(-118%) rotate(-1.2deg)}68%{opacity:1;transform:translateY(6%) rotate(.25deg)}100%{opacity:1;transform:translateY(0) rotate(0deg)}}
@media (prefers-reduced-motion:reduce){.fm-lead-title-slot__slot--changed .fm-lead-title-slot__face{animation:none;transform:translateY(0)}.fm-lead-title-slot__face--old{display:none}}
`;

function HeroTitleGlyph({
  previousChar,
  currentChar,
  changed,
  positionFromLeft,
  revision,
  accentChars = '.%',
}: {
  previousChar: string;
  currentChar: string;
  changed: boolean;
  positionFromLeft: number;
  revision: number;
  accentChars?: string;
}) {
  const displayChar = currentChar || previousChar || HERO_NBSP;
  const delayMs = Math.min(positionFromLeft, 18) * HERO_TITLE_SLOT_STAGGER_MS;
  const previousAccent = accentChars.includes(previousChar);
  const currentAccent = accentChars.includes(currentChar);

  return (
    <span
      className={cn(
        'fm-lead-title-slot__slot',
        changed && 'fm-lead-title-slot__slot--changed',
      )}
      style={{ '--title-slot-delay': `${delayMs}ms` } as CSSProperties}
      aria-hidden="true"
    >
      <span className="fm-lead-title-slot__sizer">{displayChar}</span>
      <span
        key={`old-${revision}-${previousChar}-${positionFromLeft}`}
        className={cn('fm-lead-title-slot__face fm-lead-title-slot__face--old', previousAccent && 'fm-lead-title-slot__face--accent')}
      >
        {previousChar || HERO_NBSP}
      </span>
      <span
        key={`new-${revision}-${currentChar}-${positionFromLeft}`}
        className={cn('fm-lead-title-slot__face fm-lead-title-slot__face--new', currentAccent && 'fm-lead-title-slot__face--accent')}
      >
        {currentChar || HERO_NBSP}
      </span>
    </span>
  );
}

function HeroHandle({ value, reduce }: { value: string; reduce: boolean }) {
  const [slotText, setSlotText] = useState(() => ({
    previous: value,
    current: value,
    revision: 0,
  }));

  useEffect(() => {
    if (slotText.current === value) return;
    const frame = requestAnimationFrame(() => {
      setSlotText((current) => (
        current.current === value
          ? current
          : { previous: current.current, current: value, revision: current.revision + 1 }
      ));
    });

    return () => cancelAnimationFrame(frame);
  }, [value, slotText]);

  useEffect(() => {
    if (slotText.previous === slotText.current) return;
    const timeout = window.setTimeout(() => {
      setSlotText((current) => (
        current.revision === slotText.revision
          ? { ...current, previous: current.current }
          : current
      ));
    }, HERO_TITLE_SLOT_SETTLE_MS);

    return () => window.clearTimeout(timeout);
  }, [slotText]);

  const lines = splitHeroHandle(slotText.current);
  const longestLine = Math.max(...lines.map((line, index) => line.length + (index === lines.length - 1 ? 1 : 0)));
  const charCount = Math.max(4, longestLine);
  const currentText = `${slotText.current}.`;
  const previousText = `${slotText.previous}.`;
  const currentChars = Array.from(currentText);
  const previousChars = Array.from(previousText);
  const extraPreviousSlots = Math.max(0, previousChars.length - currentChars.length);
  const lineModels = lines.reduce<{
    offset: number;
    items: Array<{
      key: string;
      slots: Array<{
        previousChar: string;
        currentChar: string;
        positionFromLeft: number;
        changed: boolean;
      }>;
    }>;
  }>(
    (acc, line, lineIndex) => {
      const currentLineChars = Array.from(`${line}${lineIndex === lines.length - 1 ? '.' : ''}`);
      const slotCount = currentLineChars.length + (lineIndex === lines.length - 1 ? extraPreviousSlots : 0);
      const slots = Array.from({ length: slotCount }, (_, slotIndex) => {
        const currentAbsoluteIndex = acc.offset + slotIndex;
        const previousChar = previousChars[currentAbsoluteIndex] || '';
        const currentChar = currentChars[currentAbsoluteIndex] || '';

        return {
          previousChar,
          currentChar,
          positionFromLeft: currentAbsoluteIndex,
          changed: previousChar !== currentChar,
        };
      });

      return {
        offset: acc.offset + currentLineChars.length,
        items: [...acc.items, { key: `${slotText.current}:${lineIndex}`, slots }],
      };
    },
    { offset: 0, items: [] },
  ).items;

  return (
    <div
      className="relative mt-4 max-w-full"
      style={{
        '--hero-chars': charCount,
        '--hero-max': '76px',
        '--hero-min': '20px',
        '--hero-ratio': 1.72,
        containerType: 'inline-size',
      } as CSSProperties}
    >
      <style>{HERO_TITLE_SLOT_CSS}</style>
      <h1
        className="max-w-full font-black leading-[0.82] tracking-normal"
        style={{
          fontSize: 'clamp(var(--hero-min), calc((100cqw - 8px) / var(--hero-chars) * var(--hero-ratio)), var(--hero-max))',
        }}
        aria-label={`${slotText.current}.`}
      >
        {lineModels.map((line) => {
          return (
            <span key={line.key} className="fm-lead-title-slot__line">
              <span className="fm-lead-title-slot" aria-hidden="true">
                {line.slots.map((slot) => (
                  reduce ? (
                    <span
                      key={`static-${slot.positionFromLeft}`}
                      className={cn('fm-lead-title-slot__static', slot.currentChar === '.' && 'text-[var(--fm-accent-bright)]')}
                    >
                      {slot.currentChar || ''}
                    </span>
                  ) : (
                    <HeroTitleGlyph
                      key={`slot-${slot.positionFromLeft}`}
                      previousChar={slot.previousChar}
                      currentChar={slot.currentChar}
                      changed={slot.changed}
                      positionFromLeft={slot.positionFromLeft}
                      revision={slotText.revision}
                    />
                  )
                ))}
              </span>
            </span>
          );
        })}
      </h1>
    </div>
  );
}

function SlottedText({
  value,
  reduce,
  className,
  ariaLabel,
  accentChars = '%',
}: {
  value: string;
  reduce: boolean;
  className?: string;
  ariaLabel?: string;
  accentChars?: string;
}) {
  const [slotText, setSlotText] = useState(() => ({
    previous: value,
    current: value,
    revision: 0,
  }));

  useEffect(() => {
    if (slotText.current === value) return;
    const frame = requestAnimationFrame(() => {
      setSlotText((current) => (
        current.current === value
          ? current
          : { previous: current.current, current: value, revision: current.revision + 1 }
      ));
    });

    return () => cancelAnimationFrame(frame);
  }, [value, slotText]);

  useEffect(() => {
    if (slotText.previous === slotText.current) return;
    const timeout = window.setTimeout(() => {
      setSlotText((current) => (
        current.revision === slotText.revision
          ? { ...current, previous: current.current }
          : current
      ));
    }, HERO_TITLE_SLOT_SETTLE_MS);

    return () => window.clearTimeout(timeout);
  }, [slotText]);

  const currentChars = Array.from(slotText.current);
  const previousChars = Array.from(slotText.previous);
  const slotCount = Math.max(currentChars.length, previousChars.length, 1);

  return (
    <span className={cn('fm-lead-title-slot', className)} aria-label={ariaLabel || slotText.current}>
      {Array.from({ length: slotCount }, (_, index) => {
        const previousChar = previousChars[index] || '';
        const currentChar = currentChars[index] || '';

        return reduce ? (
          <span
            key={`static-${index}`}
            className={cn('fm-lead-title-slot__static', (currentChar === '.' || currentChar === '%') && 'text-[var(--fm-accent-bright)]')}
            aria-hidden="true"
          >
            {currentChar || ''}
          </span>
        ) : (
          <HeroTitleGlyph
            key={`slot-${index}`}
            previousChar={previousChar}
            currentChar={currentChar}
            changed={previousChar !== currentChar}
            positionFromLeft={index}
            revision={slotText.revision}
            accentChars={accentChars}
          />
        );
      })}
    </span>
  );
}

function PostMortemPanel({
  feeds,
  activeFeed,
  selectedFeeder,
  timeframe,
}: {
  feeds: FeedBoard[];
  activeFeed: FeedBoard | null;
  selectedFeeder: string;
  timeframe: typeof TIMEFRAMES[number];
}) {
  const reduce = Boolean(useReducedMotion());
  const pool = useMemo(() => selectedPool(feeds, activeFeed, selectedFeeder), [activeFeed, feeds, selectedFeeder]);
  const sorted = useMemo(() => [...pool].sort((a, b) => b.feeder.score - a.feeder.score), [pool]);
  const leader = sorted[0];
  const topCards = useMemo(() => sorted.slice(0, 5), [sorted]);
  const [stackThumbs, setStackThumbs] = useState<Record<string, string>>({});
  const topCardFetchTargets = useMemo(
    () => topCards
      .filter((entry) => !entry.feeder.thumbnailUrl && isNumericFeedId(entry.feed.id))
      .map((entry) => ({
        key: stackThumbKey(entry),
        feedId: entry.feed.id,
        handle: entry.feeder.handle.replace(/^@+/, ''),
      })),
    [topCards],
  );
  const totalTopPosts = pool.reduce((sum, entry) => sum + entry.feeder.topPosts, 0);
  const totalComments = pool.reduce((sum, entry) => sum + numericMetric(entry.feeder.comments), 0);
  const totalLikes = pool.reduce((sum, entry) => sum + numericMetric(entry.feeder.likes), 0);
  const score = Math.round(pool.reduce((sum, entry) => sum + entry.feeder.score, 0) / Math.max(1, pool.length));
  const scope = scopeHandle(activeFeed, selectedFeeder);
  const heroLabel = scope;
  /* Percentile depths — lower is better; the gauge hangs them from the top-1% surface. */
  const avgDepth = pool.reduce((sum, entry) => sum + Number.parseFloat(entry.feeder.avg.replace(/[^\d.]/g, '')), 0) / Math.max(1, pool.length);
  const standing = Math.max(1, Math.min(60, Math.round(100 - score)));
  const clampDepth = (value: number) => Math.max(1, Math.min(60, Math.round(value)));
  const commentDepth = clampDepth(6000 / Math.max(60, totalComments));
  const likesDepth = clampDepth(90000 / Math.max(900, totalLikes));
  const averageDepth = clampDepth(avgDepth);
  const bestPostDepth = clampDepth(avgDepth * 0.3);
  const engagementDepth = clampDepth((commentDepth + likesDepth) / 2);
  const mobileProof = leader.feeder.proof.replace(/^Top\s+\d+%\s*/i, '');
  const mobileHandleChars = Math.max(8, Array.from(leader.feeder.handle).length);
  const blocks = [
    { label: 'best post', value: bestPostDepth, hot: true },
    { label: 'avg', value: averageDepth, hot: false },
    { label: 'comments', value: commentDepth, hot: selectedFeeder !== FEED_SCOPE },
    { label: 'likes', value: likesDepth, hot: false },
    { label: 'lift', value: standing, hot: false },
  ];
  const mobileInsightPairs: MobileInsightPair[] = [
    {
      key: 'response',
      title: 'Response',
      subtitle: 'comments / likes',
      values: [
        { label: 'comments', value: compactNumber(totalComments), detail: 'metric', hot: selectedFeeder !== FEED_SCOPE },
        { label: 'likes', value: compactNumber(totalLikes), detail: 'metric' },
      ],
    },
    {
      key: 'position',
      title: 'Position',
      subtitle: 'top / bottom',
      values: [
        { label: 'top', value: bestPostDepth, suffix: '%', detail: 'best post', hot: true },
        { label: 'bottom', value: standing, suffix: '%', detail: 'board floor' },
      ],
    },
    {
      key: 'momentum',
      title: 'Momentum',
      subtitle: 'engagement / lift',
      values: [
        { label: 'engagement', value: engagementDepth, suffix: '%', detail: 'response bite', hot: true },
        { label: 'lift', value: standing, suffix: '%', detail: 'board lift' },
      ],
    },
  ];
  const slotKey = `${activeFeed?.id || 'all'}:${selectedFeeder}:${timeframe}`;
  const hydratedTopCards = topCards.map((entry) => {
    const thumbnailUrl = entry.feeder.thumbnailUrl || stackThumbs[stackThumbKey(entry)] || null;
    return thumbnailUrl === entry.feeder.thumbnailUrl
      ? entry
      : { ...entry, feeder: { ...entry.feeder, thumbnailUrl } };
  });

  useEffect(() => {
    if (topCardFetchTargets.length === 0) return undefined;

    let cancelled = false;
    const controller = new AbortController();

    Promise.all(topCardFetchTargets.map(async (target) => {
      const params = new URLSearchParams({ feedId: target.feedId, handle: target.handle });
      const response = await fetch(`/api/feed/feeder-posts?${params.toString()}`, {
        cache: 'no-store',
        credentials: 'include',
        signal: controller.signal,
      });
      if (!response.ok) return null;
      const payload = await response.json() as { posts?: Array<{ thumbnailUrl?: string | null }> };
      const thumbnailUrl = payload.posts?.find((post) => typeof post.thumbnailUrl === 'string' && post.thumbnailUrl)?.thumbnailUrl || null;
      return thumbnailUrl ? [target.key, thumbnailUrl] as const : null;
    }))
      .then((results) => {
        if (cancelled) return;
        const nextEntries = results.filter((result): result is readonly [string, string] => Boolean(result));
        if (nextEntries.length === 0) return;
        setStackThumbs((current) => {
          const next = { ...current };
          for (const [key, thumbnailUrl] of nextEntries) next[key] = thumbnailUrl;
          return next;
        });
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted) {
          console.warn('Lead stack thumbnails unavailable', error);
        }
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [topCardFetchTargets]);

  return (
	    <motion.section
	      data-testid="lead-post-mortem"
	      initial={false}
	      className="relative isolate w-full min-w-0 max-w-full overflow-hidden rounded-[26px] border border-white/[0.085] bg-[#07070A] p-3 text-white shadow-[0_30px_80px_-48px_rgba(0,0,0,0.94)] sm:rounded-[30px] sm:border-white/[0.08] sm:p-4"
	    >
	      <div aria-hidden="true" className="absolute inset-0 -z-10 bg-[radial-gradient(ellipse_58%_46%_at_100%_0%,rgb(var(--fm-accent-rgb)/0.18),transparent_64%),linear-gradient(135deg,rgba(255,255,255,0.035),transparent_42%)] sm:bg-[radial-gradient(ellipse_70%_80%_at_100%_0%,rgb(var(--fm-accent-rgb)/0.22),transparent_55%),linear-gradient(135deg,rgba(255,255,255,0.05),transparent_46%)]" />
      <span
        aria-hidden="true"
        className="pointer-events-none absolute -left-5 bottom-[-24px] -z-10 select-none text-[156px] font-black leading-none tracking-tight text-white/[0.055] sm:bottom-[-38px] sm:text-[220px] sm:text-white/[0.035]"
      >
        PM
      </span>

      <div className="grid min-w-0 gap-3 sm:gap-4 xl:grid-cols-[minmax(0,1.72fr)_minmax(300px,0.62fr)] xl:items-stretch">
        <div className="flex min-h-0 min-w-0 flex-col justify-between sm:min-h-[230px] xl:min-h-0">
          <div>
            <SlotSwap
              changeKey={`${slotKey}:scope`}
              reduce={reduce}
              className="flex flex-wrap items-center gap-1.5 text-[8.5px] font-black uppercase tracking-[0.18em] sm:gap-2 sm:text-[10px] sm:tracking-[0.2em]"
            >
              <span className="text-[var(--fm-accent-bright)]">Post Mortem</span>
              <span aria-hidden="true" className="h-1 w-1 rounded-full bg-white/22" />
              <span className="text-white/52">{scope}</span>
              <span aria-hidden="true" className="h-1 w-1 rounded-full bg-white/22" />
              <span className="text-white/38">{timeframe} window</span>
            </SlotSwap>

            <div className="hidden sm:block">
              <HeroHandle value={heroLabel} reduce={reduce} />
            </div>
            <p className="mt-3 hidden max-w-[380px] text-[14px] font-black leading-snug text-white/48 sm:block">
              What carried the board, what fell off, and which feeder changed the ranking.
            </p>
          </div>

	          <div className="mt-3 grid grid-cols-1 items-stretch gap-3 rounded-[24px] border border-white/[0.085] bg-white/[0.045] p-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.055)] sm:mt-4 sm:grid-cols-[minmax(0,1fr)_auto] sm:items-end sm:gap-4 sm:rounded-none sm:border-x-0 sm:border-b-0 sm:border-t sm:border-white/10 sm:bg-transparent sm:p-0 sm:pt-4 sm:shadow-none xl:mt-4 xl:grid-cols-1 xl:gap-0 xl:pb-0 xl:pt-4">
	            <div className="min-w-0 xl:grid xl:grid-cols-[minmax(250px,0.38fr)_minmax(0,0.62fr)] xl:items-end xl:gap-12">
	              <SlotSwap changeKey={`${slotKey}:desktop-average`} reduce={reduce} delay={0.08} className="hidden text-left xl:block">
	                <div className="inline-flex items-baseline font-black leading-none text-white text-[clamp(58px,6.2vw,92px)]">
	                  <span className="mr-[0.14em] text-[0.2em] text-[var(--fm-accent-bright)]">AVG</span>
	                  <Odometer value={averageDepth} animateOnMount revealDelayMs={80} className="inline-flex min-w-[1.12em] overflow-visible" />
	                  <span className="ml-[0.04em] text-[0.28em] text-[var(--fm-accent-bright)]">%</span>
	                </div>
	                <div className="-mt-1 text-[9px] font-black uppercase tracking-[0.16em] text-white/36">window depth</div>
	              </SlotSwap>

	              <div className="min-w-0">
	                <div className="text-[8px] font-black uppercase tracking-[0.18em] text-white/44 sm:text-[10px] sm:text-white/36">
                  <span className="xl:hidden">Lead read</span>
                  <span className="hidden xl:inline">Feed</span>
                </div>
	                <div className="xl:hidden">
	                  <div
	                    className="mt-2 max-w-full overflow-hidden font-black leading-[0.94] text-white"
	                    style={{
	                      '--mobile-handle-chars': mobileHandleChars,
	                      containerType: 'inline-size',
	                      fontSize: 'clamp(22px, calc((100cqw - 6px) / var(--mobile-handle-chars) * 1.62), 36px)',
	                    } as CSSProperties}
	                  >
	                    <SlottedText value={leader.feeder.handle} reduce={reduce} className="max-w-full whitespace-nowrap" ariaLabel={leader.feeder.handle} />
	                  </div>
	                  <div className="mt-2 flex flex-wrap items-center gap-2 text-[11px] font-black text-white/58">
	                    <span className="min-w-0 truncate">{mobileProof}</span>
	                  </div>
	                </div>
	                <SlotSwap changeKey={`${slotKey}:lead-read`} reduce={reduce} delay={0.04} className="hidden min-w-0 xl:block">
	                  <div className="mt-2 text-[30px] font-black leading-none text-white">
                    {leader.feed.name}
	                  </div>
	                  <div className="mt-1.5 hidden truncate text-[11px] font-extrabold text-white/44 sm:block xl:text-[12px]">{leader.feeder.proof}</div>
	                </SlotSwap>
	              </div>
	            </div>
	            <div className="text-right xl:hidden">
	              <div data-testid="mobile-post-mortem-average" className="grid grid-cols-[minmax(0,0.72fr)_auto] items-end gap-4 border-t border-white/[0.07] pt-3 text-left sm:hidden">
	                <div className="min-w-0 pb-2">
	                  <div className="text-[9px] font-black uppercase tracking-[0.18em] text-[var(--fm-accent-bright)]">Average</div>
	                  <div className="mt-2 max-w-[170px] text-[10px] font-black uppercase leading-snug tracking-[0.14em] text-white/34">
	                    Window depth
	                    <span className="mt-1 block text-white/48">{compactNumber(totalTopPosts)} posts tracked</span>
	                  </div>
	                </div>
	                <div className="flex items-baseline justify-end font-black leading-none text-white text-[clamp(68px,20vw,90px)]">
	                  <SlottedText value={`${averageDepth}`} reduce={reduce} className="tabular-nums" ariaLabel={`${averageDepth}`} />
	                  <span className="-ml-2 text-[0.88em] leading-none text-[var(--fm-accent-bright)]">%</span>
	                </div>
	              </div>
	              <SlotSwap changeKey={`${slotKey}:standing`} reduce={reduce} delay={0.08} className="hidden text-right sm:block xl:hidden">
	                <div className="inline-flex items-baseline justify-end font-black leading-none text-white sm:text-[clamp(46px,8vw,76px)]">
	                  <span className="mr-[0.14em] text-[0.22em] text-white/40">TOP</span>
	                  <Odometer value={standing} animateOnMount revealDelayMs={80} className="inline-flex min-w-[1.12em] overflow-visible" />
	                  <span className="ml-[0.04em] text-[0.26em] text-[var(--fm-accent-bright)]">%</span>
	                </div>
	              </SlotSwap>
	              <div className="hidden -mt-1 text-[9px] font-black uppercase tracking-[0.16em] text-white/36 sm:block xl:hidden">board standing</div>
	            </div>
	          </div>

          <div className="mt-3 hidden grid-cols-5 gap-2 border-t border-white/10 pt-3 xl:grid">
            {blocks.map((block, index) => (
              <div key={block.label} className="min-w-0 rounded-[14px] border border-white/[0.055] bg-white/[0.045] px-3 py-2.5">
                <SlotSwap changeKey={`${slotKey}:desktop-block:${block.label}`} reduce={reduce} delay={0.08 + index * 0.035}>
                  <div className="flex items-baseline gap-1.5">
                    <span className={cn('text-[24px] font-black tabular-nums leading-none', block.hot ? 'text-[var(--fm-accent-bright)]' : 'text-white')}>
                      {block.value}
                    </span>
                    <span className={cn('text-[11px] font-black leading-none', block.hot ? 'text-[var(--fm-accent-bright)]' : 'text-white/38')}>%</span>
                  </div>
                </SlotSwap>
                <div className="mt-1.5 truncate text-[8px] font-black uppercase tracking-[0.15em] text-white/38">{block.label}</div>
              </div>
            ))}
          </div>

          <div data-testid="lead-post-mortem-stat-rail" className="mt-3 hidden grid-cols-3 overflow-hidden rounded-[18px] border border-white/[0.07] bg-white/[0.035] shadow-[inset_0_1px_0_rgba(255,255,255,0.045)] sm:mt-4 sm:grid xl:mt-3">
            {[
              ['posts', String(totalTopPosts)],
              ['comments', totalComments >= 1000 ? `${(totalComments / 1000).toFixed(1)}K` : String(totalComments)],
              ['likes', totalLikes >= 1000 ? `${(totalLikes / 1000).toFixed(1)}K` : String(totalLikes)],
            ].map(([label, value], index) => (
              <div key={label} className="min-w-0 border-r border-white/[0.07] px-3 py-3 last:border-r-0 sm:px-4 sm:py-3.5 xl:px-5 xl:py-3">
                <SlotSwap changeKey={`${slotKey}:total:${label}`} reduce={reduce} delay={0.14 + index * 0.04}>
                  <div className="text-[23px] font-black tabular-nums leading-none text-white sm:text-[30px] xl:text-[32px]">{value}</div>
                </SlotSwap>
                <div className="mt-1.5 truncate text-[7px] font-black uppercase tracking-[0.14em] text-white/42 sm:text-[8px] sm:tracking-[0.16em]">{label}</div>
              </div>
            ))}
          </div>
        </div>

	        <div className="flex min-h-0 min-w-0 flex-col sm:min-h-[278px] xl:hidden">
	          <div className="hidden items-center justify-between gap-4 pb-3 sm:flex">
            <span className="text-[9px] font-black uppercase tracking-[0.2em] text-white/36 sm:text-[10px]">Standing</span>
            <span className="flex items-center gap-1.5 text-[9px] font-black uppercase tracking-[0.16em] text-white/42">
              <motion.span
                aria-hidden="true"
                className="h-1.5 w-1.5 rounded-full bg-[var(--fm-accent-bright)]"
                animate={reduce ? undefined : { opacity: [1, 0.3, 1], scale: [1, 0.8, 1] }}
                transition={{ duration: 1.8, repeat: Infinity, ease: 'easeInOut' }}
              />
              Live read
            </span>
          </div>

          <div className="sm:hidden">
            <MobilePostMortemInsights pairs={mobileInsightPairs} reduce={reduce} slotKey={slotKey} />
          </div>

          <div className="hidden min-h-0 flex-1 flex-col sm:flex">
            <SlotSwap changeKey={`${slotKey}:standing-index`} reduce={reduce} delay={0.1} className="flex min-h-0 flex-1 flex-col">
              <StandingIndex blocks={blocks} reduce={reduce} />
            </SlotSwap>
          </div>
        </div>

		        <PostCardStack cards={hydratedTopCards} />
      </div>
    </motion.section>
  );
}

type GrowthWeekPost = {
  id: string;
  postKey: string | null;
  feedName: string;
  feedColor: string;
  handle: string;
  thumbnailUrl: string | null;
  tone: string;
  delta: number;
  score: number;
  postedAt: string | null;
  comments: number | null;
  likes: number | null;
  source: 'actual' | 'estimate';
};

type GrowthWeekPool = {
  id: string;
  weekNumber: number;
  weekLabel: string;
  rangeLabel: string;
  followerDelta: number;
  postCount: number;
  feederCount: number;
  posts: GrowthWeekPost[];
};

type GrowthWeekCard = GrowthWeekPool & {
  kind: 'winner' | 'weak';
  rank: number;
};

type GrowthWeekDashboard = {
  cards: GrowthWeekCard[];
  netDelta: number;
  weekCount: number;
  postCount: number;
};

function buildGrowthWeekDashboard(
  feeds: FeedBoard[],
  activeFeed: FeedBoard | null,
  selectedFeeder: string,
  timeframe: typeof TIMEFRAMES[number],
  actualPosts: AwardPost[] = [],
): GrowthWeekDashboard {
  const pool = selectedPool(feeds, activeFeed, selectedFeeder);
  const weekCount = Math.max(1, Math.round(timeframeWeeks(timeframe)));
  const now = Date.now();
  const dayMs = 24 * 60 * 60 * 1000;
  const windowDays = daysForTimeframe(timeframe);
  const since = now - windowDays * dayMs;
  const weekRangeLabel = (weekIndex: number) => {
    const endTime = now - weekIndex * 7 * dayMs;
    const startTime = Math.max(since, endTime - 7 * dayMs);
    return `${shortPostDate(new Date(startTime).toISOString())} - ${shortPostDate(new Date(endTime).toISOString())}`;
  };
  const weeks: GrowthWeekPool[] = Array.from({ length: weekCount }, (_, index) => ({
    id: `week-${index + 1}`,
    weekNumber: index + 1,
    weekLabel: index === 0 ? 'This week' : `${index + 1}w ago`,
    rangeLabel: weekRangeLabel(index),
    followerDelta: 0,
    postCount: 0,
    feederCount: 0,
    posts: [],
  }));

  const actualScoped = postsInWindow(actualPosts, timeframe).filter((post) => postTime(post.postedAt) > 0);
  if (actualScoped.length > 0) {
    const feedByName = new Map(feeds.map((feed) => [feed.name.toLowerCase(), feed]));
    const feederByHandle = new Map(flattenFeeders(feeds).map((entry) => [entry.feeder.handle.replace(/^@+/, '').toLowerCase(), entry]));

    for (const post of actualScoped) {
      const ageWeeks = Math.min(weekCount - 1, Math.max(0, Math.floor((now - postTime(post.postedAt)) / (7 * dayMs))));
      const week = weeks[ageWeeks];
      const handle = post.handle ? `@${post.handle.replace(/^@+/, '')}` : '@post';
      const feederEntry = feederByHandle.get(handle.replace(/^@+/, '').toLowerCase());
      const feed = feeds.find((entry) => entry.id === post.feedId) || feedByName.get(post.feedName.toLowerCase());
      const score = post.latestPercentile != null
        ? Math.max(38, Math.min(99, 100 - post.latestPercentile))
        : feederEntry?.feeder.score || 72;
      const delta = Math.round(
        Math.max(4, Math.log10(Math.max(10, postMetric(post.likes) + postMetric(post.comments) * 6)) * 18)
        * (post.latestPercentile != null ? Math.max(-0.4, (55 - post.latestPercentile) / 35) : 0.55),
      );
      const growthPost: GrowthWeekPost = {
        id: `${post.feedId}:${post.postKey}`,
        postKey: post.postKey,
        feedName: post.feedName,
        feedColor: feed?.color || feederEntry?.feed.color || 'var(--fm-accent)',
        handle,
        thumbnailUrl: post.thumbnailUrl,
        tone: feederEntry?.feeder.thumb || feedTone(ageWeeks),
        delta,
        score,
        postedAt: post.postedAt,
        comments: post.comments,
        likes: post.likes,
        source: 'actual',
      };

      week.followerDelta += delta;
      week.postCount += 1;
      week.feederCount = new Set([...week.posts.map((entry) => entry.handle), handle]).size;
      week.posts = [...week.posts, growthPost]
        .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta))
        .slice(0, 12);
    }
  } else {
  pool.forEach((entry, entryIndex) => {
    const postCount = periodPostCount(entry.feeder, timeframe);
    const basePostsPerWeek = Math.floor(postCount / weekCount);
    const remainder = postCount % weekCount;
    const followerScale = largeNumericMetric(entry.feeder.followers);
    const comments = largeNumericMetric(entry.feeder.comments);
    const likes = largeNumericMetric(entry.feeder.likes);
    const score = timeframeScore(entry.feeder, timeframe);
    const signalBase = Math.max(
      8,
      Math.log10(Math.max(10, followerScale + likes * 0.08 + comments * 3)) * 18
        + (comments / Math.max(1, postCount)) * 0.34
        + (likes / Math.max(1, postCount)) * 0.025,
    );

    for (let slot = 0; slot < weekCount; slot++) {
      const postsInWeek = basePostsPerWeek + (slot < remainder ? 1 : 0);
      if (postsInWeek <= 0) continue;

      const weekIndex = (slot + entryIndex * 2) % weekCount;
      const week = weeks[weekIndex];
      const seasonality = Math.sin((weekIndex + 1) * 1.7 + entryIndex * 0.9) * 0.32;
      const quality = (score - 76) / 32;
      const delta = Math.round(signalBase * postsInWeek * (0.38 + quality + seasonality));
      const previewPost: GrowthWeekPost = {
        id: `${entry.feed.id}:${entry.feeder.handle}:${week.id}`,
        postKey: null,
        feedName: entry.feed.name,
        feedColor: entry.feed.color,
        handle: entry.feeder.handle,
        thumbnailUrl: entry.feeder.thumbnailUrl || entry.feeder.profilePicUrl || null,
        tone: entry.feeder.thumb,
        delta,
        score,
        postedAt: null,
        comments: numericMetric(entry.feeder.comments),
        likes: numericMetric(entry.feeder.likes),
        source: 'estimate',
      };

      week.followerDelta += delta;
      week.postCount += postsInWeek;
      week.feederCount += 1;
      week.posts = [...week.posts, previewPost]
        .sort((a, b) => Math.abs(b.delta) - Math.abs(a.delta))
        .slice(0, 5);
    }
  });
  }

  const activeWeeks = weeks.filter((week) => week.postCount > 0);
  const winners = [...activeWeeks]
    .sort((a, b) => b.followerDelta - a.followerDelta)
    .slice(0, 2);
  const winnerIds = new Set(winners.map((week) => week.id));
  const weakest = [...activeWeeks]
    .filter((week) => !winnerIds.has(week.id))
    .sort((a, b) => a.followerDelta - b.followerDelta)
    .slice(0, 2);
  const cards = [
    ...winners.map((week, index) => ({ ...week, kind: 'winner' as const, rank: index + 1 })),
    ...weakest.map((week, index) => ({ ...week, kind: 'weak' as const, rank: index + 1 })),
  ];

  return {
    cards,
    netDelta: activeWeeks.reduce((sum, week) => sum + week.followerDelta, 0),
    weekCount,
    postCount: activeWeeks.reduce((sum, week) => sum + week.postCount, 0),
  };
}

function GrowthThumbnail({
  post,
  className,
}: {
  post: GrowthWeekPost | undefined;
  className?: string;
}) {
  const [failedUrl, setFailedUrl] = useState('');
  if (!post) return <Thumb tone="rose" label="POOL" className={cn('rounded-[16px]', className)} />;

  if (post.thumbnailUrl && failedUrl !== post.thumbnailUrl) {
    return (
      <div className={cn('relative overflow-hidden rounded-[16px] bg-white/[0.04]', className)}>
        {/* eslint-disable-next-line @next/next/no-img-element -- remote feeder/post media */}
        <img
          src={post.thumbnailUrl}
          alt=""
          className="h-full w-full object-cover"
          loading="lazy"
          decoding="async"
          onError={() => setFailedUrl(post.thumbnailUrl || '')}
        />
      </div>
    );
  }

  return <Thumb tone={post.tone} label={post.feedName} className={cn('rounded-[16px]', className)} />;
}

function GrowthWeeks({
  feeds,
  activeFeed,
  selectedFeeder,
  timeframe,
}: {
  feeds: FeedBoard[];
  activeFeed: FeedBoard | null;
  selectedFeeder: string;
  timeframe: typeof TIMEFRAMES[number];
}) {
  const reduce = Boolean(useReducedMotion());
  const [postPoolsExpanded, setPostPoolsExpanded] = useState(false);
  const [postState, setPostState] = useState<{ key: string; posts: AwardPost[] }>({ key: '', posts: [] });
  const fetchTargets = useMemo(() => {
    if (activeFeed && isNumericFeedId(activeFeed.id)) {
      return [{
        id: activeFeed.id,
        name: activeFeed.name,
        handle: selectedFeeder === FEED_SCOPE ? 'all' : selectedFeeder.replace(/^@+/, ''),
      }];
    }

    return feeds
      .filter((feed) => isNumericFeedId(feed.id))
      .map((feed) => ({ id: feed.id, name: feed.name, handle: 'all' }));
  }, [activeFeed, feeds, selectedFeeder]);
  const fetchTargetKey = fetchTargets.map((target) => `${target.id}:${target.handle}`).join('|');
  const actualPosts = useMemo(
    () => (postState.key === fetchTargetKey ? postState.posts : []),
    [fetchTargetKey, postState.key, postState.posts],
  );
  const dashboard = useMemo(
    () => buildGrowthWeekDashboard(feeds, activeFeed, selectedFeeder, timeframe, actualPosts),
    [activeFeed, actualPosts, feeds, selectedFeeder, timeframe],
  );
  const scope = activeFeed
    ? selectedFeeder === FEED_SCOPE
      ? activeFeed.name
      : selectedFeeder
    : 'All feedboards';

  useEffect(() => {
    if (fetchTargets.length === 0) return undefined;

    let cancelled = false;
    const controller = new AbortController();

    Promise.all(fetchTargets.map(async (feed) => {
      const params = new URLSearchParams({ feedId: feed.id, handle: feed.handle });
      const response = await fetch(`/api/feed/feeder-posts?${params.toString()}`, {
        cache: 'no-store',
        credentials: 'include',
        signal: controller.signal,
      });
      if (!response.ok) return [];
      const payload = await response.json() as { posts?: Array<Omit<AwardPost, 'feedId' | 'feedName'>> };
      return (payload.posts || []).map((post) => ({ ...post, feedId: feed.id, feedName: feed.name }));
    }))
      .then((groups) => {
        if (!cancelled) setPostState({ key: fetchTargetKey, posts: groups.flat() });
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted) console.warn('Growth week posts unavailable', error);
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [fetchTargetKey, fetchTargets]);

  if (dashboard.cards.length === 0) return null;

  return (
    <section data-testid="growth-weeks" className="fm-depth-glass overflow-hidden rounded-[26px] p-4 sm:p-5 lg:p-6">
      <div className="mb-4 flex flex-wrap items-end justify-between gap-3 sm:mb-5">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.18em] text-[var(--fm-accent)]">
            <Trophy size={13} strokeWidth={3} />
            Follower Growth
          </div>
          <div className="mt-1.5 text-[27px] font-black leading-none text-foreground fm-depth-title sm:text-[34px]">
            Weekly post pools
          </div>
          <div className="mt-1.5 truncate text-[11px] font-black uppercase tracking-[0.13em] text-foreground/34">
            {scope} / {dashboard.weekCount} weeks / {dashboard.postCount} posts
          </div>
        </div>

        <div className="rounded-[18px] border border-foreground/[0.06] bg-foreground/[0.035] px-4 py-3 text-right shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
          <div className="text-[9px] font-black uppercase tracking-[0.16em] text-foreground/36">Net</div>
          <div className="mt-1 text-[28px] font-black tabular-nums leading-none text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]">
            {signedCompactNumber(dashboard.netDelta)}
          </div>
        </div>
      </div>

      <div className="hide-scrollbar flex gap-3 overflow-x-auto pb-1 md:grid md:grid-cols-2 md:items-start md:overflow-visible xl:grid-cols-4">
        <AnimatePresence mode="popLayout" initial={false}>
          {dashboard.cards.map((card, index) => {
            const bestPost = card.posts[0];
            const isWinner = card.kind === 'winner';
            const accent = bestPost?.feedColor || 'var(--fm-accent)';
            const cardLabel = isWinner ? `Winner ${card.rank}` : card.followerDelta < 0 ? `Lost ${card.rank}` : `Weakest ${card.rank}`;
            const expanded = postPoolsExpanded;

            return (
              <motion.article
                key={`${timeframe}:${card.kind}:${card.id}`}
                layout
                initial={reduce ? false : { opacity: 0, y: 18, filter: 'blur(7px)' }}
                animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
                exit={reduce ? { opacity: 0 } : { opacity: 0, y: -10, filter: 'blur(7px)' }}
                transition={{ delay: reduce ? 0 : index * 0.045, duration: 0.34, ease: SOFT_EASE }}
                className={cn(
                  'relative w-[274px] shrink-0 overflow-hidden rounded-[24px] border bg-white/[0.035] p-3.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.045)] sm:w-[302px] md:w-auto',
                  isWinner ? 'border-[rgb(var(--fm-accent-rgb)/0.22)]' : 'border-foreground/[0.07]',
                )}
                style={{ '--growth-week-accent': accent } as CSSProperties}
              >
                <span
                  aria-hidden="true"
                  className={cn(
                    'pointer-events-none absolute -right-10 -top-12 h-32 w-32 rounded-full blur-3xl',
                    isWinner ? 'bg-[rgb(var(--fm-accent-rgb)/0.18)]' : 'bg-foreground/8',
                  )}
                />

                <button
                  type="button"
                  onClick={() => setPostPoolsExpanded((current) => !current)}
                  className="relative z-10 block w-full rounded-[18px] text-left outline-none [-webkit-tap-highlight-color:transparent]"
                  aria-expanded={expanded}
                  aria-label={`${expanded ? 'Hide' : 'Show'} post pools for all growth weeks`}
                >
                <div className="flex items-start justify-between gap-3">
                  <div className="min-w-0">
                    <div className={cn('text-[9px] font-black uppercase tracking-[0.18em]', isWinner ? 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]' : 'text-foreground/36')}>
                      {cardLabel}
                    </div>
                    <div className="mt-1.5 text-[21px] font-black leading-none text-foreground fm-depth-title">
                      {card.weekLabel}
                    </div>
                    <div className="mt-1 truncate text-[8px] font-black uppercase tracking-[0.13em] text-foreground/32">{card.rangeLabel}</div>
                  </div>
                  <span className={cn('rounded-full border px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.13em]', expanded ? 'border-[rgb(var(--fm-accent-rgb)/0.26)] bg-[rgb(var(--fm-accent-rgb)/0.12)] text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]' : 'border-foreground/[0.07] bg-foreground/[0.035] text-foreground/44')}>
                    Post pool
                  </span>
                </div>

                <div className="mt-4 flex items-end justify-between gap-3">
                  <div className={cn('text-[50px] font-black tabular-nums leading-[0.78] tracking-normal sm:text-[58px]', card.followerDelta < 0 ? 'text-foreground/74' : 'text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]')}>
                    {signedCompactNumber(card.followerDelta)}
                  </div>
                  <div className="pb-1 text-right">
                    <div className="text-[20px] font-black tabular-nums leading-none text-foreground">{card.postCount}</div>
                    <div className="mt-1 text-[7.5px] font-black uppercase tracking-[0.14em] text-foreground/34">posts</div>
                  </div>
                </div>

                <div className="relative mt-4 h-[104px] overflow-hidden rounded-[18px] border border-foreground/[0.06] bg-foreground/[0.025]">
                  <GrowthThumbnail post={bestPost} className="h-full w-full rounded-[18px]" />
                  <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.04),rgba(0,0,0,0.54))]" />
                  <div className="absolute left-2.5 top-2.5 flex -space-x-2">
                    {card.posts.slice(1, 5).map((post) => (
                      <GrowthThumbnail
                        key={post.id}
                        post={post}
                        className="h-9 w-9 rounded-full border-2 border-black/45 shadow-[0_8px_18px_rgba(0,0,0,0.32)]"
                      />
                    ))}
                  </div>
                  <div className="absolute inset-x-0 bottom-0 flex items-end justify-between gap-2 px-3 pb-2.5 text-white">
                    <div className="min-w-0">
                      <div className="truncate text-[13px] font-black leading-none">{bestPost?.handle || '@pool'}</div>
                      <div className="mt-1 truncate text-[7.5px] font-black uppercase tracking-[0.13em] text-white/60">
                        {card.postCount} posts in week
                      </div>
                    </div>
                    <span className="shrink-0 rounded-full bg-black/48 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.12em] text-white/76 backdrop-blur-md">
                      top {topPercentFromScore(bestPost?.score || 72)}%
                    </span>
                  </div>
                </div>
                </button>

                <AnimatePresence initial={false}>
                  {expanded && (
                    <motion.div
                      key="growth-post-pool"
                      initial={reduce ? false : { height: 0, opacity: 0, filter: 'blur(6px)' }}
                      animate={{ height: 'auto', opacity: 1, filter: 'blur(0px)' }}
                      exit={reduce ? { opacity: 0 } : { height: 0, opacity: 0, filter: 'blur(6px)' }}
                      transition={{
                        height: { duration: 0.42, ease: SOFT_EASE },
                        opacity: { duration: 0.26, ease: SOFT_EASE },
                        filter: { duration: 0.3, ease: SOFT_EASE },
                      }}
                      className="relative z-10 overflow-hidden"
                    >
                      <div className="mt-3 grid grid-cols-2 gap-2">
                        {card.posts.slice(0, 4).map((post) => (
                          <div key={post.id} className="min-w-0 overflow-hidden rounded-[16px] border border-foreground/[0.06] bg-foreground/[0.025]">
                            <div className="relative h-[82px] overflow-hidden">
                              <GrowthThumbnail post={post} className="h-full w-full rounded-none" />
                              <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.02),rgba(0,0,0,0.48))]" />
                              <span className="absolute bottom-2 left-2 rounded-full bg-black/48 px-2 py-0.5 text-[7px] font-black uppercase tracking-[0.1em] text-white/78 backdrop-blur-md">
                                {shortPostDate(post.postedAt)}
                              </span>
                            </div>
                            <div className="min-w-0 px-2.5 py-2">
                              <div className="truncate text-[10px] font-black leading-none text-foreground">{post.handle}</div>
                              <div className="mt-1.5 flex items-center gap-2 text-[7px] font-black uppercase tracking-[0.1em] text-foreground/34">
                                <span>{formatAwardMetric(post.comments)} c</span>
                                <span>{formatAwardMetric(post.likes)} l</span>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                      {card.posts.length > 4 && (
                        <div className="mt-2 rounded-full bg-foreground/[0.035] px-3 py-2 text-center text-[8px] font-black uppercase tracking-[0.15em] text-foreground/36">
                          +{card.posts.length - 4} more posts in this week
                        </div>
                      )}
                    </motion.div>
                  )}
                </AnimatePresence>
              </motion.article>
            );
          })}
        </AnimatePresence>
      </div>
    </section>
  );
}

function Header({
  feeds,
  activeFeedId,
  selectedFeeder,
  timeframe,
  compressed,
  railMotion,
  onFeedChange,
  onFeederChange,
  onTimeframeChange,
}: {
  feeds: FeedBoard[];
  activeFeedId: string;
  selectedFeeder: string;
  timeframe: typeof TIMEFRAMES[number];
  compressed: boolean;
  railMotion: FeedRailMotion | null;
  onFeedChange: (id: string, sourceEl?: HTMLElement) => void;
  onFeederChange: (handle: string) => void;
  onTimeframeChange: (value: typeof TIMEFRAMES[number]) => void;
}) {
  const reduce = Boolean(useReducedMotion());
  const activeFeed = feeds.find((feed) => feed.id === activeFeedId) || null;
  const isClosingFeed = railMotion?.mode === 'closing';
  const isOpeningFeed = railMotion?.mode === 'opening';
  const allBadge = { name: 'All', initials: 'ALL', color: '#111827', tint: 'rgb(17 24 39 / 0.08)' };
  const scopeLabel = activeFeed
    ? selectedFeeder === FEED_SCOPE
      ? activeFeed.name
      : selectedFeeder
    : 'All Feedboards';

  return (
    <div className="pointer-events-none fixed inset-x-0 top-0 z-[160] flex justify-center px-2 pt-[calc(10px+env(safe-area-inset-top))] sm:px-4 sm:pt-[calc(14px+env(safe-area-inset-top))] md:pt-5">
      <div className="fm-tab-header-shell">
        <motion.div
          data-testid="lead-header-shell"
          className={cn(
            'fm-depth-chrome fm-depth-chrome--header pointer-events-auto px-3.5 py-3.5 sm:px-4 lg:px-5',
            compressed && 'fm-depth-chrome--header-compressed',
          )}
          initial={false}
          style={{
            '--fm-mobile-header-chrome-height': '194px',
            '--fm-desktop-header-chrome-height': '194px',
            '--fm-mobile-header-chrome-compressed-height': '78px',
          } as CSSProperties}
        >
          <style>{HEADER_RAIL_CSS}</style>
          <LayoutGroup id="lead-header-rail">
          <div className="relative z-10 flex h-full flex-col gap-3">
            <div className="fm-app-header-row flex items-center justify-between gap-3">
              <div className="flex min-w-0 items-center gap-3">
                <span className="fm-app-header-title text-black dark:text-white fm-depth-title">LEAD</span>
                <div className="min-w-0 border-l border-foreground/10 pl-3">
                  <div className="text-[9px] font-black uppercase tracking-[0.2em] text-[var(--fm-accent)]">Feederboard</div>
                  <div className="mt-0.5 max-w-[116px] truncate text-[14px] font-black leading-none text-foreground sm:max-w-[220px] sm:text-[16px]">
                    {scopeLabel}
                  </div>
                </div>
              </div>
              <div className="flex shrink-0 items-center gap-2">
                <div className="flex items-center gap-1 rounded-[14px] border border-black/5 bg-black/[0.035] p-1 shadow-[inset_0_2px_8px_rgba(0,0,0,0.04)] dark:border-white/8 dark:bg-white/[0.03]">
                  {TIMEFRAMES.map((value) => (
                    <button
                      key={value}
                      type="button"
                      onClick={() => onTimeframeChange(value)}
                      className={cn(
                        'relative rounded-[10px] px-2.5 py-1.5 text-[10px] font-black uppercase tracking-[0.12em] transition',
                        timeframe === value ? 'bg-[var(--fm-accent)] text-white shadow-[0_6px_14px_rgb(var(--fm-accent-rgb)/0.22)]' : 'text-foreground/44',
                      )}
                    >
                      {value}
                    </button>
                  ))}
                </div>
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
                willChange: 'transform, opacity, clip-path',
              }}
            >
              <motion.div layout className="hide-scrollbar flex min-w-0 items-start gap-3 overflow-x-auto pb-1">
                <AnimatePresence mode="popLayout">
                  {!activeFeed && (
                    <FeedBadge
                      key="all:all"
                      feed={allBadge}
                      selected={activeFeedId === 'all'}
                      feedId="all"
                      enterDelay={isClosingFeed ? HEADER_CHAIN_START : undefined}
                      exitIndex={0}
                      exitX={-22}
                      reduce={reduce}
                      onClick={(sourceEl) => onFeedChange('all', sourceEl)}
                    />
                  )}

                  {activeFeed ? (
                    <FeedBadge
                      key={`focus:${activeFeed.id}`}
                      feed={activeFeed}
                      selected
                      feedId={activeFeed.id}
                      travelFromX={isOpeningFeed && railMotion?.id === activeFeed.id ? railMotion.x : undefined}
                      exitX={-22}
                      reduce={reduce}
                      onClick={(sourceEl) => onFeedChange('all', sourceEl)}
                    />
                  ) : (
                    feeds.map((feed, index) => (
                      <FeedBadge
                        key={`all:${feed.id}`}
                        feed={feed}
                        selected={activeFeedId === feed.id}
                        feedId={feed.id}
                        travelFromX={isClosingFeed && railMotion?.id === feed.id ? -railMotion.x : undefined}
                        enterDelay={
                          isClosingFeed && railMotion?.id !== feed.id
                            ? HEADER_CHAIN_START + (index + 1) * HEADER_CHAIN_STAGGER
                            : undefined
                        }
                        exitIndex={0}
                        exitX={0}
                        reduce={reduce}
                        onClick={(sourceEl) => onFeedChange(feed.id, sourceEl)}
                      />
                    ))
                  )}

                  {activeFeed && (
                    <motion.span
                      key={`${activeFeed.id}:divider`}
                      layout
                      className="mt-[9px] h-[56px] w-px shrink-0 bg-foreground/10"
                      aria-hidden="true"
                      initial={reduce ? false : { opacity: 0, x: -8, scaleY: 0.72 }}
                      animate={reduce ? { opacity: 1 } : { opacity: 1, x: 0, scaleY: 1 }}
                      exit={reduce ? { opacity: 0 } : { opacity: 0, x: -20, scaleY: 0.78, transition: headerCircleExitTransition(activeFeed.feeders.length + 1) }}
                      transition={reduce ? { duration: 0 } : headerCircleEnterTransition(HEADER_CHAIN_START - 0.06)}
                    />
                  )}

                  {activeFeed && (
                    <FeedScopeCircle
                      key={`${activeFeed.id}:scope`}
                      feed={activeFeed}
                      selected={selectedFeeder === FEED_SCOPE}
                      index={0}
                      exitIndex={activeFeed.feeders.length}
                      reduce={reduce}
                      onClick={() => onFeederChange(FEED_SCOPE)}
                    />
                  )}

                  {activeFeed?.feeders.map((feeder, index) => (
                    <FeederCircle
                      key={feeder.handle}
                      feeder={feeder}
                      selected={selectedFeeder === feeder.handle}
                      index={index + 1}
                      exitIndex={activeFeed.feeders.length - index - 1}
                      reduce={reduce}
                      onClick={() => onFeederChange(feeder.handle)}
                    />
                  ))}
                </AnimatePresence>
              </motion.div>
            </motion.div>
          </div>
          </LayoutGroup>
        </motion.div>
      </div>
    </div>
  );
}

type RankedFeed = {
  feed: FeedBoard;
  live: number;
  topPercent: number;
  leader: Feeder;
  move: string;
  posts: number;
  engagement: number;
};

function rankedFeeds(feeds: FeedBoard[], timeframe: typeof TIMEFRAMES[number]): RankedFeed[] {
  return feeds
    .map((feed) => {
      const leader = [...feed.feeders].sort((a, b) => timeframeScore(b, timeframe) - timeframeScore(a, timeframe))[0] || feed.feeders[0];
      const move = feedMove(feed);
      return {
        feed,
        live: feedPeriodScore(feed, timeframe),
        topPercent: topPercentFromScore(feedPeriodScore(feed, timeframe)),
        leader,
        move: move > 0 ? `+${move}` : String(move),
        posts: feedPeriodPostCount(feed, timeframe),
        engagement: feed.feeders.reduce((sum, feeder) => sum + numericMetric(feeder.likes) + numericMetric(feeder.comments), 0),
      };
    })
    .sort((a, b) => a.topPercent - b.topPercent);
}

function FeedThroneCard({
  entry,
  timeframe,
  expanded,
  onToggle,
  reduce,
}: {
  entry: RankedFeed;
  timeframe: typeof TIMEFRAMES[number];
  expanded: boolean;
  onToggle: () => void;
  reduce: boolean;
}) {
  const { feed, leader, topPercent } = entry;

  return (
    <motion.div
      layoutId={`feed-ladder-${feed.id}`}
      transition={reduce ? { duration: 0 } : LADDER_SPRING}
      className="relative isolate overflow-hidden rounded-[26px] text-white shadow-[0_30px_70px_-42px_rgba(0,0,0,0.85)]"
    >
      <div className="absolute inset-0 -z-20 bg-[rgb(var(--fm-accent-rgb)/0.78)]" />
      <div className="absolute inset-x-0 bottom-0 -z-10 h-[44%] bg-black/44" />
      <span
        aria-hidden="true"
        className="pointer-events-none absolute -right-3 -top-8 -z-10 select-none text-[190px] font-black leading-none tracking-tight text-white/[0.09]"
      >
        1
      </span>

      <motion.button
        type="button"
        onClick={onToggle}
        whileTap={reduce ? undefined : { scale: 0.994 }}
        className="relative z-10 flex min-h-[190px] w-full flex-col justify-between p-4 text-left outline-none sm:min-h-[218px] sm:p-5 [-webkit-tap-highlight-color:transparent]"
        aria-expanded={expanded}
        aria-label={`${expanded ? 'Collapse' : 'Open'} ${feed.name} feed metrics`}
      >
        <div className="flex items-start justify-between gap-3">
          <span className="inline-flex items-center gap-2 rounded-full bg-black/38 px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.18em] backdrop-blur-md">
            <motion.span
              aria-hidden="true"
              className="h-1.5 w-1.5 rounded-full bg-[var(--fm-accent-bright)]"
              animate={reduce ? undefined : { opacity: [1, 0.3, 1], scale: [1, 0.8, 1] }}
              transition={{ duration: 1.8, repeat: Infinity, ease: 'easeInOut' }}
            />
            Holds all feeds
          </span>
          <MoveChip move={entry.move} className="rounded-full bg-black/38 px-2.5 py-1.5 !text-white backdrop-blur-md" />
        </div>

        <div className="flex items-end justify-between gap-4">
          <div className="min-w-0">
            <div className="max-w-[62vw] truncate text-[42px] font-black leading-[0.86] tracking-tight text-[var(--fm-accent-bright)] sm:max-w-[520px] sm:text-[56px] lg:text-[64px]">
              {feed.name}
            </div>
            <div className="mt-2 truncate text-[9px] font-black uppercase tracking-[0.18em] text-white/62">
              {leader.handle} leads · {feed.summary.feeders} feeders
            </div>
          </div>
          <div className="shrink-0 text-right">
            <div className="text-[clamp(64px,8vw,96px)] font-black tabular-nums leading-[0.76] tracking-tight">
              <span className="mr-[0.12em] align-[0.18em] text-[0.28em] text-white/48">TOP</span>
              <Odometer value={topPercent} animateOnMount revealDelayMs={100} className="inline-flex overflow-visible" />
              <span className="ml-[0.04em] text-[0.34em] text-[var(--fm-accent-bright)]">%</span>
            </div>
            <div className="mt-1 text-[8px] font-black uppercase tracking-[0.2em] text-white/50">lower is better</div>
          </div>
        </div>
      </motion.button>

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            key="feed-throne-dossier"
            initial={reduce ? false : { height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={reduce ? { opacity: 0 } : { height: 0, opacity: 0 }}
            transition={{
              height: { duration: 0.42, ease: SOFT_EASE },
              opacity: { duration: 0.26, ease: SOFT_EASE },
            }}
            className="relative z-10 overflow-hidden bg-black/58"
          >
            <FeedRowDossier entry={entry} timeframe={timeframe} reduce={reduce} dark />
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}

function FeedChallengerRow({
  entry,
  rank,
  timeframe,
  expanded,
  onToggle,
  reduce,
}: {
  entry: RankedFeed;
  rank: number;
  timeframe: typeof TIMEFRAMES[number];
  expanded: boolean;
  onToggle: () => void;
  reduce: boolean;
}) {
  const { feed, leader, live } = entry;
  const topPercent = entry.topPercent;

  return (
    <motion.div
      layoutId={`feed-ladder-${feed.id}`}
      transition={reduce ? { duration: 0 } : LADDER_SPRING}
      className="relative isolate overflow-hidden"
    >
      <motion.span
        aria-hidden="true"
        className="absolute inset-y-0 left-0 -z-10 w-full origin-left bg-[rgb(var(--fm-accent-rgb)/0.055)]"
        initial={reduce ? false : { scaleX: 0 }}
        animate={{ scaleX: Math.max(0.08, Math.min(1, live / 100)) }}
        transition={reduce ? { duration: 0 } : { delay: 0.12 + rank * 0.05, duration: 0.8, ease: SOFT_EASE }}
      />
      <span
        aria-hidden="true"
        className="pointer-events-none absolute -left-1.5 top-1/2 -z-10 -translate-y-1/2 select-none text-[74px] font-black tabular-nums leading-none tracking-tight text-foreground/[0.055]"
      >
        {rank}
      </span>

      <motion.button
        type="button"
        onClick={onToggle}
        whileTap={reduce ? undefined : { scale: 0.995 }}
        className="relative z-10 grid w-full grid-cols-[minmax(0,1fr)_auto_18px] items-center gap-3 py-4 pl-12 pr-3 text-left outline-none sm:gap-4 sm:py-5 sm:pl-16 sm:pr-4 [-webkit-tap-highlight-color:transparent]"
        aria-expanded={expanded}
        aria-label={`${expanded ? 'Collapse' : 'Open'} ${feed.name} feed metrics`}
      >
        <span className="min-w-0">
          <span className="block truncate text-[21px] font-black leading-none text-foreground sm:text-[25px]">{feed.name}</span>
          <span className="mt-1.5 block truncate text-[8.5px] font-black uppercase tracking-[0.16em] text-foreground/38">
            {leader.handle} leading · {feed.summary.signals} signals
          </span>
        </span>

        <span className="flex items-baseline gap-2.5 text-right">
          <MoveChip move={entry.move} />
          <span className="text-[26px] font-black tabular-nums leading-none text-foreground fm-depth-title sm:text-[30px]">
            <span className="mr-1 align-[0.25em] text-[0.42em] text-foreground/38">TOP</span>
            {topPercent}
            <span className="text-[0.52em] text-[var(--fm-accent)]">%</span>
          </span>
        </span>

        <motion.span
          className="grid place-items-center text-foreground/26"
          initial={false}
          animate={{ rotate: expanded ? 90 : 0 }}
          transition={reduce ? { duration: 0 } : LADDER_SPRING}
        >
          <ChevronRight size={16} strokeWidth={3} />
        </motion.span>
      </motion.button>

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            key="feed-dossier"
            initial={reduce ? false : { height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={reduce ? { opacity: 0 } : { height: 0, opacity: 0 }}
            transition={{
              height: { duration: 0.42, ease: SOFT_EASE },
              opacity: { duration: 0.26, ease: SOFT_EASE },
            }}
            className="relative z-10 overflow-hidden"
          >
            <FeedRowDossier entry={entry} timeframe={timeframe} reduce={reduce} />
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}

function FeedRowDossier({
  entry,
  timeframe,
  reduce,
  dark,
}: {
  entry: RankedFeed;
  timeframe: typeof TIMEFRAMES[number];
  reduce: boolean;
  dark?: boolean;
}) {
  const { feed, leader } = entry;
  const payload = useDossierPayload(feed, 'all', timeframe);
  const fallbackPosts = useMemo(() => fallbackDossierPosts(feed, feed.feeders, timeframe), [feed, timeframe]);
  const posts = useMemo(
    () => dossierPostsInWindow(payload?.posts?.length ? payload.posts : fallbackPosts, timeframe),
    [fallbackPosts, payload, timeframe],
  );
  const insight = payload?.movementInsight || fallbackInsight(feed.name, posts);
  const featuredPosts = useMemo(() => pickDossierFeaturePosts(posts), [posts]);
  const metrics: DossierMetric[] = [
    { label: 'avg', value: `${entry.topPercent}%`, detail: 'feed depth' },
    { label: 'feeders', value: String(feed.feeders.length), detail: `${leader.handle} leading` },
    { label: 'posts per week', value: postsPerWeekMetric(posts, timeframe, entry.posts / Math.max(1, timeframeWeeks(timeframe))), detail: mediaBreakdownDetail(posts) },
    { label: 'signals', value: String(feed.summary.signals), detail: 'active reads' },
  ];

  return (
    <DossierProofDrawer
      insight={insight}
      metrics={metrics}
      featuredPosts={featuredPosts}
      fallbackTone={leader.thumb}
      reduce={reduce}
      dark={dark}
    />
  );
}

function AllBoards({ feeds, timeframe }: { feeds: FeedBoard[]; timeframe: typeof TIMEFRAMES[number] }) {
  const reduce = Boolean(useReducedMotion());
  const [throne, ...challengers] = useMemo(() => rankedFeeds(feeds, timeframe), [feeds, timeframe]);
  const [expandedFeedId, setExpandedFeedId] = useState('');

  if (!throne) return null;

  const toggleFeed = (id: string) => setExpandedFeedId((current) => (current === id ? '' : id));

  return (
    <section className="space-y-3">
      <div className="flex items-end justify-between gap-3 px-1.5">
        <div>
          <div className="text-[10px] font-black uppercase tracking-[0.18em] text-[var(--fm-accent)]">Feederboard</div>
          <div className="mt-1.5 text-[28px] font-black leading-none text-foreground fm-depth-title sm:text-[32px]">All Feedboards</div>
        </div>
        <div className="pb-0.5 text-right text-[9px] font-black uppercase tracking-[0.16em] text-foreground/34">
          {timeframe} window
          <span className="mx-1.5 text-foreground/20">/</span>
          {feeds.length} feeds
        </div>
      </div>

      <LayoutGroup id="feed-ladder-all">
        <FeedThroneCard
          entry={throne}
          timeframe={timeframe}
          expanded={expandedFeedId === throne.feed.id}
          onToggle={() => toggleFeed(throne.feed.id)}
          reduce={reduce}
        />

        {challengers.length > 0 && (
          <motion.div layout className="fm-depth-glass overflow-hidden rounded-[26px]">
            <div className="flex items-center justify-between gap-3 border-b border-foreground/[0.06] px-4 py-3 sm:px-5">
              <span className="text-[9px] font-black uppercase tracking-[0.2em] text-foreground/38">Chasing the board</span>
              <span className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/30">lower is better</span>
            </div>
            <div className="divide-y divide-foreground/[0.05]">
              {challengers.map((entry, index) => (
                <FeedChallengerRow
                  key={entry.feed.id}
                  entry={entry}
                  rank={index + 2}
                  timeframe={timeframe}
                  expanded={expandedFeedId === entry.feed.id}
                  onToggle={() => toggleFeed(entry.feed.id)}
                  reduce={reduce}
                />
              ))}
            </div>
          </motion.div>
        )}
      </LayoutGroup>
    </section>
  );
}

type DossierPost = {
  feedId: string;
  feedName: string;
  postKey: string;
  postUrl?: string | null;
  thumbnailUrl: string | null;
  mediaType?: string | null;
  handle: string | null;
  postedAt: string | null;
  latestCheckpoint?: string | null;
  latestBusinessDayIst?: string | null;
  latestPercentile: number | null;
  views: number | null;
  likes: number | null;
  comments: number | null;
};

type DossierMovementInsight = {
  headline: string;
  subline: string;
  tone: 'winner' | 'risk' | 'steady' | 'volatile';
  mediaBreakdown?: { reel: number; carousel: number; image: number };
};

type DossierPayload = {
  movementInsight?: DossierMovementInsight;
  posts?: DossierPost[];
};

type DossierMetric = {
  label: string;
  value: string;
  detail?: string;
};

type DossierFeaturePost = {
  slot: 'engaged' | 'commented' | 'liked';
  label: string;
  value: string;
  unit: string;
  post: DossierPost;
};

function useDossierPayload(feed: FeedBoard, handle: string, timeframe: typeof TIMEFRAMES[number]) {
  const fetchKey = `${feed.id}:${handle}:${timeframe}`;
  const [state, setState] = useState<{ key: string; payload: DossierPayload | null }>({ key: '', payload: null });

  useEffect(() => {
    if (!isNumericFeedId(feed.id)) return undefined;

    let cancelled = false;
    const controller = new AbortController();
    const params = new URLSearchParams({
      feedId: feed.id,
      handle: handle.replace(/^@+/, '') || 'all',
      timeframe,
    });

    fetch(`/api/feed/feeder-posts?${params.toString()}`, {
      cache: 'no-store',
      credentials: 'include',
      signal: controller.signal,
    })
      .then(async (response) => {
        if (!response.ok) return null;
        return response.json() as Promise<DossierPayload>;
      })
      .then((payload) => {
        if (!cancelled) setState({ key: fetchKey, payload });
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted) console.warn('Dossier posts unavailable', error);
      });

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [feed.id, fetchKey, handle, timeframe]);

  return state.key === fetchKey ? state.payload : null;
}

function dossierPostsInWindow(posts: DossierPost[], timeframe: typeof TIMEFRAMES[number]) {
  const since = Date.now() - daysForTimeframe(timeframe) * 24 * 60 * 60 * 1000;
  return posts.filter((post) => postTime(post.postedAt) === 0 || postTime(post.postedAt) >= since);
}

function dossierMediaBreakdown(posts: DossierPost[]) {
  return posts.reduce((counts, post) => {
    if (post.mediaType === 'reel') counts.reel += 1;
    if (post.mediaType === 'carousel') counts.carousel += 1;
    if (post.mediaType === 'image') counts.image += 1;
    return counts;
  }, { reel: 0, carousel: 0, image: 0 });
}

function mediaBreakdownDetail(posts: DossierPost[]) {
  const counts = dossierMediaBreakdown(posts);
  return `R ${counts.reel} / C ${counts.carousel} / I ${counts.image}`;
}

function dossierEngagement(post: DossierPost) {
  return postMetric(post.comments) * 6 + postMetric(post.likes) + postMetric(post.views) * 0.04;
}

function topDepthValue(post: DossierPost) {
  return post.latestPercentile != null ? `Top ${Math.round(post.latestPercentile)}%` : 'Tracked';
}

function pickDossierFeaturePosts(posts: DossierPost[]): DossierFeaturePost[] {
  const used = new Set<string>();
  const pick = (
    slot: DossierFeaturePost['slot'],
    label: string,
    unit: string,
    value: (post: DossierPost) => string,
    compare: (a: DossierPost, b: DossierPost) => number,
  ) => {
    const post = [...posts]
      .filter((entry) => !used.has(entry.postKey))
      .sort(compare)[0] || null;
    if (!post) return null;
    used.add(post.postKey);
    return { slot, label, value: value(post), unit, post };
  };

  return [
    pick('engaged', 'Most engaged', 'response', (post) => topDepthValue(post), (a, b) => {
      const delta = dossierEngagement(b) - dossierEngagement(a);
      return delta !== 0 ? delta : postTime(b.postedAt) - postTime(a.postedAt);
    }),
    pick('commented', 'Most commented', 'comments', (post) => formatAwardMetric(post.comments), (a, b) => {
      const delta = postMetric(b.comments) - postMetric(a.comments);
      return delta !== 0 ? delta : postTime(b.postedAt) - postTime(a.postedAt);
    }),
    pick('liked', 'Most liked', 'likes', (post) => formatAwardMetric(post.likes), (a, b) => {
      const delta = postMetric(b.likes) - postMetric(a.likes);
      return delta !== 0 ? delta : postTime(b.postedAt) - postTime(a.postedAt);
    }),
  ].filter((entry): entry is DossierFeaturePost => Boolean(entry));
}

function fallbackDossierPosts(feed: FeedBoard, feeders: Feeder[], timeframe: typeof TIMEFRAMES[number]): DossierPost[] {
  return feeders.flatMap((feeder) => {
    const count = Math.max(1, Math.min(3, Math.round(periodPostCount(feeder, timeframe) / Math.max(1, timeframeWeeks(timeframe)))));
    return Array.from({ length: count }, (_, postIndex) => ({
      feedId: feed.id,
      feedName: feed.name,
      postKey: `${feed.id}:${feeder.handle}:${postIndex}`,
      thumbnailUrl: feeder.thumbnailUrl || feeder.profilePicUrl || null,
      mediaType: postIndex % 3 === 0 ? 'reel' : postIndex % 3 === 1 ? 'carousel' : 'image',
      handle: feeder.handle.replace(/^@+/, ''),
      postedAt: null,
      latestPercentile: topPercentFromScore(timeframeScore(feeder, timeframe)),
      views: null,
      likes: numericMetric(feeder.likes),
      comments: numericMetric(feeder.comments),
    }));
  });
}

function fallbackInsight(subject: string, posts: DossierPost[]): DossierMovementInsight {
  const topPost = [...posts].sort((a, b) => dossierEngagement(b) - dossierEngagement(a))[0] || null;
  const cleanSubject = subject.replace(/^@+/, '');
  const subjectLabel = subject.trim().startsWith('@') ? `@${cleanSubject}` : cleanSubject;
  const mediaBreakdown = dossierMediaBreakdown(posts);

  if (!topPost) {
    return {
      headline: `${subjectLabel} has no post evidence yet.`,
      subline: 'Post proof will appear once tracked media is available.',
      tone: 'risk',
      mediaBreakdown,
    };
  }

  const totalComments = posts.reduce((sum, post) => sum + postMetric(post.comments), 0);
  const totalLikes = posts.reduce((sum, post) => sum + postMetric(post.likes), 0);
  const bestByComments = [...posts].sort((a, b) => postMetric(b.comments) - postMetric(a.comments))[0] || null;
  const bestByLikes = [...posts].sort((a, b) => postMetric(b.likes) - postMetric(a.likes))[0] || null;
  const bestPercentile = [...posts]
    .filter((post) => post.latestPercentile != null)
    .sort((a, b) => postMetric(a.latestPercentile) - postMetric(b.latestPercentile))[0] || null;
  const commentShare = totalComments > 0 && bestByComments ? postMetric(bestByComments.comments) / totalComments : 0;
  const likeShare = totalLikes > 0 && bestByLikes ? postMetric(bestByLikes.likes) / totalLikes : 0;
  const mediaEntries: Array<[string, number]> = [
    ['reels', mediaBreakdown.reel],
    ['carousels', mediaBreakdown.carousel],
    ['images', mediaBreakdown.image],
  ];
  const dominantMedia = mediaEntries.sort((a, b) => b[1] - a[1])[0];
  const dominantShare = posts.length > 0 ? dominantMedia[1] / posts.length : 0;

  if (commentShare >= 0.55 && bestByComments) {
    return {
      headline: stablePick(`${subjectLabel}:comments`, [
        `Comments are bunched around one post for ${subjectLabel}.`,
        `${subjectLabel} is getting most replies from one post.`,
        `One post is doing the comment work for ${subjectLabel}.`,
      ]),
      subline: `@${bestByComments.handle || cleanSubject} has ${Math.round(commentShare * 100)}% of comments here.`,
      tone: 'volatile',
      mediaBreakdown,
    };
  }

  if (likeShare >= 0.6 && bestByLikes) {
    return {
      headline: stablePick(`${subjectLabel}:likes`, [
        `Likes are doing more of the work for ${subjectLabel}.`,
        `${subjectLabel} has one post pulling the like count up.`,
        `The like count is clustered for ${subjectLabel}.`,
      ]),
      subline: `@${bestByLikes.handle || cleanSubject} is pulling ${Math.round(likeShare * 100)}% of likes.`,
      tone: 'steady',
      mediaBreakdown,
    };
  }

  if (bestPercentile?.latestPercentile != null && bestPercentile.latestPercentile <= 10) {
    return {
      headline: stablePick(`${subjectLabel}:depth`, [
        `${subjectLabel} has one post clearly above the pack.`,
        `${subjectLabel}'s best post is the cleanest signal here.`,
        `One post gives ${subjectLabel} a sharper read.`,
      ]),
      subline: `@${bestPercentile.handle || cleanSubject} reached top ${Math.round(bestPercentile.latestPercentile)}%.`,
      tone: 'winner',
      mediaBreakdown,
    };
  }

  if (dominantShare >= 0.65 && posts.length >= 3) {
    return {
      headline: stablePick(`${subjectLabel}:media`, [
        `${subjectLabel} is mostly a ${dominantMedia[0]} read right now.`,
        `${dominantMedia[0]} are shaping the current view of ${subjectLabel}.`,
        `${subjectLabel}'s posts are leaning toward ${dominantMedia[0]}.`,
      ]),
      subline: `${dominantMedia[1]} of ${posts.length} tracked posts are ${dominantMedia[0]}.`,
      tone: 'steady',
      mediaBreakdown,
    };
  }

  return {
    headline: stablePick(`${subjectLabel}:even`, [
      `${subjectLabel} looks fairly even across the posts we can see.`,
      `${subjectLabel} does not have one post taking over.`,
      `${subjectLabel} is spread across a few smaller signals.`,
      `${subjectLabel} is giving a quieter, more even read.`,
    ]),
    subline: stablePick(`${subjectLabel}:even-subline`, [
      `${posts.length} tracked posts are spread without one post taking over.`,
      `${posts.length} tracked posts are sharing the read pretty evenly.`,
      `The posts here are close enough that no single proof card dominates.`,
    ]),
    tone: 'steady',
    mediaBreakdown,
  };
}

function postsPerWeekMetric(posts: DossierPost[], timeframe: typeof TIMEFRAMES[number], fallbackValue: number) {
  const value = posts.length > 0
    ? posts.length / Math.max(1, timeframeWeeks(timeframe))
    : fallbackValue;
  return value.toFixed(value >= 10 ? 0 : 1);
}

function DossierThumbnail({
  post,
  tone,
  className,
}: {
  post: DossierPost;
  tone: string;
  className?: string;
}) {
  const [failedUrl, setFailedUrl] = useState('');

  if (post.thumbnailUrl && failedUrl !== post.thumbnailUrl) {
    return (
      <div className={cn('relative overflow-hidden bg-white/[0.04]', className)}>
        {/* eslint-disable-next-line @next/next/no-img-element -- remote post media */}
        <img
          src={post.thumbnailUrl}
          alt=""
          className="h-full w-full object-cover"
          loading="lazy"
          decoding="async"
          onError={() => setFailedUrl(post.thumbnailUrl || '')}
        />
      </div>
    );
  }

  return <Thumb tone={tone} label={post.feedName} className={className || ''} />;
}

function DossierProofDrawer({
  insight,
  metrics,
  featuredPosts,
  fallbackTone,
  reduce,
  dark,
}: {
  insight: DossierMovementInsight;
  metrics: DossierMetric[];
  featuredPosts: DossierFeaturePost[];
  fallbackTone: string;
  reduce: boolean;
  dark?: boolean;
}) {
  return (
    <div className={cn('border-t px-4 pb-4 pt-3.5 sm:px-5', dark ? 'border-white/12' : 'border-foreground/[0.07]')}>
      <div className="grid gap-4 lg:grid-cols-[minmax(0,0.95fr)_minmax(380px,1.05fr)] lg:items-start">
        <div className="min-w-0">
          <div className={cn('text-[8px] font-black uppercase tracking-[0.18em]', dark ? 'text-white/42' : 'text-foreground/34')}>
            Movement read
          </div>
          <p className={cn('mt-2 max-w-[680px] text-[18px] font-black leading-tight sm:text-[23px]', dark ? 'text-white/88' : 'text-foreground/86')}>
            {insight.headline}
          </p>
          <p className={cn('mt-2 max-w-[560px] text-[12px] font-extrabold leading-snug sm:text-[13px]', dark ? 'text-white/50' : 'text-foreground/44')}>
            {insight.subline}
          </p>

          <div className="mt-4 grid grid-cols-2 gap-2 sm:grid-cols-4">
            {metrics.map((metric, statIndex) => (
              <motion.div
                key={metric.label}
                initial={reduce ? false : { opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.08 + statIndex * 0.045, duration: 0.3, ease: SOFT_EASE }}
                className={cn('min-w-0 rounded-[16px] border px-3 py-3', dark ? 'border-white/10 bg-white/8' : 'border-foreground/[0.06] bg-foreground/[0.025]')}
              >
                <div className={cn('truncate text-[24px] font-black tabular-nums leading-none sm:text-[26px]', dark ? 'text-white' : 'text-foreground')}>
                  {metric.value}
                </div>
                <div className={cn('mt-1.5 truncate text-[7.5px] font-black uppercase tracking-[0.15em]', dark ? 'text-white/44' : 'text-foreground/36')}>
                  {metric.label}
                </div>
                {metric.detail && (
                  <div className={cn('mt-1 truncate text-[8px] font-black uppercase tracking-[0.1em]', dark ? 'text-white/30' : 'text-foreground/28')}>
                    {metric.detail}
                  </div>
                )}
              </motion.div>
            ))}
          </div>
        </div>

        <div className="min-w-0">
          <div className="mb-2 flex items-center justify-between gap-3">
            <span className={cn('text-[8px] font-black uppercase tracking-[0.18em]', dark ? 'text-white/42' : 'text-foreground/34')}>
              Proof posts
            </span>
            <span className={cn('text-[8px] font-black uppercase tracking-[0.14em]', dark ? 'text-white/28' : 'text-foreground/26')}>
              unique picks
            </span>
          </div>
          <div className="grid grid-cols-3 gap-2">
            {featuredPosts.map((entry, postIndex) => (
              <motion.div
                key={`${entry.slot}:${entry.post.postKey}`}
                initial={reduce ? false : { opacity: 0, y: 14, scale: 0.97 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                transition={{ delay: 0.14 + postIndex * 0.06, duration: 0.34, ease: SOFT_EASE }}
                className={cn('relative min-w-0 overflow-hidden rounded-[16px] border', dark ? 'border-white/10 bg-white/8' : 'border-foreground/[0.06] bg-foreground/[0.025]')}
              >
                <div className="relative h-[112px] overflow-hidden sm:h-[146px]">
                  <DossierThumbnail post={entry.post} tone={fallbackTone} className="h-full rounded-[16px]" />
                  <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.02),rgba(0,0,0,0.68))]" />
                  <div className="absolute inset-x-0 top-2 flex items-center justify-between gap-2 px-2">
                    <span className="truncate rounded-full bg-black/46 px-2 py-1 text-[7px] font-black uppercase tracking-[0.1em] text-white/78 backdrop-blur-md">
                      {entry.post.mediaType || 'post'}
                    </span>
                    <span className="shrink-0 rounded-full bg-[var(--fm-accent)] px-2 py-1 text-[7px] font-black uppercase tracking-[0.1em] text-white">
                      {entry.post.latestCheckpoint || 'D'}
                    </span>
                  </div>
                  <div className="absolute inset-x-0 bottom-0 px-2.5 pb-2.5 pt-8 text-white">
                    <div className="truncate text-[13px] font-black leading-none">{entry.value}</div>
                    <div className="mt-1 truncate text-[7px] font-black uppercase tracking-[0.13em] text-white/58">
                      {entry.unit}
                    </div>
                  </div>
                </div>
                <div className="min-w-0 px-2.5 py-2">
                  <div className={cn('truncate text-[8px] font-black uppercase tracking-[0.13em]', dark ? 'text-white/44' : 'text-foreground/36')}>
                    {entry.label}
                  </div>
                  <div className={cn('mt-1 truncate text-[11px] font-black leading-none', dark ? 'text-white/82' : 'text-foreground/82')}>
                    @{entry.post.handle || 'feeder'}
                  </div>
                  <div className={cn('mt-1 truncate text-[7px] font-black uppercase tracking-[0.1em]', dark ? 'text-white/30' : 'text-foreground/28')}>
                    {shortPostDate(entry.post.postedAt)}
                  </div>
                </div>
              </motion.div>
            ))}
          </div>
        </div>
      </div>

      <Link
        href="/fire"
        className="mt-4 inline-flex h-10 items-center gap-1.5 rounded-[14px] bg-[var(--fm-accent)] px-3.5 text-[10px] font-black uppercase tracking-[0.14em] text-white shadow-[0_10px_22px_-14px_rgb(var(--fm-accent-rgb)/0.82)]"
      >
        Open cards
        <ChevronRight size={14} strokeWidth={3} />
      </Link>
    </div>
  );
}

function RowDossier({
  feed,
  feeder,
  timeframe,
  reduce,
  dark,
}: {
  feed: FeedBoard;
  feeder: Feeder;
  timeframe: typeof TIMEFRAMES[number];
  reduce: boolean;
  dark?: boolean;
}) {
  const payload = useDossierPayload(feed, feeder.handle, timeframe);
  const fallbackPosts = useMemo(() => fallbackDossierPosts(feed, [feeder], timeframe), [feed, feeder, timeframe]);
  const posts = useMemo(
    () => dossierPostsInWindow(payload?.posts?.length ? payload.posts : fallbackPosts, timeframe),
    [fallbackPosts, payload, timeframe],
  );
  const insight = payload?.movementInsight || fallbackInsight(feeder.handle, posts);
  const featuredPosts = useMemo(() => pickDossierFeaturePosts(posts), [posts]);
  const metrics: DossierMetric[] = [
    { label: 'avg', value: feeder.avg.replace(/^Top\s+/i, ''), detail: 'window depth' },
    { label: 'top posts', value: String(feeder.topPosts), detail: 'quality hits' },
    { label: 'posts per week', value: postsPerWeekMetric(posts, timeframe, cadencePerWeek(feeder.cadence)), detail: mediaBreakdownDetail(posts) },
    { label: 'followers', value: feeder.followers, detail: 'tracked base' },
  ];

  return (
    <DossierProofDrawer
      insight={insight}
      metrics={metrics}
      featuredPosts={featuredPosts}
      fallbackTone={feeder.thumb}
      reduce={reduce}
      dark={dark}
    />
  );
}

function ThroneCard({
  feed,
  feeder,
  liveScore,
  timeframe,
  expanded,
  onToggle,
  reduce,
}: {
  feed: FeedBoard;
  feeder: Feeder;
  liveScore: number;
  timeframe: typeof TIMEFRAMES[number];
  expanded: boolean;
  onToggle: () => void;
  reduce: boolean;
}) {
  const topPercent = topPercentFromScore(liveScore);

  return (
    <motion.div
      layoutId={`ladder-${feed.id}-${feeder.handle}`}
      transition={reduce ? { duration: 0 } : LADDER_SPRING}
      className="relative isolate overflow-hidden rounded-[26px] text-white shadow-[0_30px_70px_-42px_rgba(0,0,0,0.85)]"
    >
      <div className="absolute inset-0 -z-20 bg-[rgb(var(--fm-accent-rgb)/0.78)]" />
      <div className="absolute inset-x-0 bottom-0 -z-10 h-[44%] bg-black/44" />
      <span
        aria-hidden="true"
        className="pointer-events-none absolute -right-3 -top-8 -z-10 select-none text-[190px] font-black leading-none tracking-tight text-white/[0.09]"
      >
        1
      </span>

      <motion.button
        type="button"
        onClick={onToggle}
        whileTap={reduce ? undefined : { scale: 0.994 }}
        className="relative z-10 flex min-h-[190px] w-full flex-col justify-between p-4 text-left outline-none sm:min-h-[218px] sm:p-5 [-webkit-tap-highlight-color:transparent]"
        aria-expanded={expanded}
        aria-label={`${expanded ? 'Collapse' : 'Open'} ${feeder.handle}`}
      >
        <div className="flex items-start justify-between gap-3">
          <span className="inline-flex items-center gap-2 rounded-full bg-black/38 px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.18em] backdrop-blur-md">
            <motion.span
              aria-hidden="true"
              className="h-1.5 w-1.5 rounded-full bg-[var(--fm-accent-bright)]"
              animate={reduce ? undefined : { opacity: [1, 0.3, 1], scale: [1, 0.8, 1] }}
              transition={{ duration: 1.8, repeat: Infinity, ease: 'easeInOut' }}
            />
            Holds the board
          </span>
          <MoveChip move={feeder.move} className="rounded-full bg-black/38 px-2.5 py-1.5 !text-white backdrop-blur-md" />
        </div>

        <div className="flex items-end justify-between gap-4">
          <div className="min-w-0">
            <div className="max-w-[62vw] truncate text-[38px] font-black leading-[0.86] tracking-tight text-[var(--fm-accent-bright)] sm:max-w-[560px] sm:text-[52px] lg:text-[60px]">
              {feeder.handle}
            </div>
            <div className="mt-2 truncate text-[9px] font-black uppercase tracking-[0.18em] text-white/62">{feeder.signal}</div>
          </div>
          <div className="shrink-0 text-right">
            <div className="text-[clamp(64px,8vw,96px)] font-black tabular-nums leading-[0.76] tracking-tight">
              <span className="mr-[0.12em] align-[0.18em] text-[0.28em] text-white/48">TOP</span>
              <Odometer value={topPercent} animateOnMount revealDelayMs={100} className="inline-flex overflow-visible" />
              <span className="ml-[0.04em] text-[0.34em] text-[var(--fm-accent-bright)]">%</span>
            </div>
            <div className="mt-1 text-[8px] font-black uppercase tracking-[0.2em] text-white/50">lower is better</div>
          </div>
        </div>
      </motion.button>

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            key="throne-dossier"
            initial={reduce ? false : { height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={reduce ? { opacity: 0 } : { height: 0, opacity: 0 }}
            transition={{
              height: { duration: 0.42, ease: SOFT_EASE },
              opacity: { duration: 0.26, ease: SOFT_EASE },
            }}
            className="relative z-10 overflow-hidden bg-black/58"
          >
            <RowDossier feed={feed} feeder={feeder} timeframe={timeframe} reduce={reduce} dark />
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}

function ChallengerRow({
  feed,
  feeder,
  rank,
  liveScore,
  timeframe,
  expanded,
  onToggle,
  reduce,
}: {
  feed: FeedBoard;
  feeder: Feeder;
  rank: number;
  liveScore: number;
  timeframe: typeof TIMEFRAMES[number];
  expanded: boolean;
  onToggle: () => void;
  reduce: boolean;
}) {
  const topPercent = topPercentFromScore(liveScore);

  return (
    <motion.div
      layoutId={`ladder-${feed.id}-${feeder.handle}`}
      transition={reduce ? { duration: 0 } : LADDER_SPRING}
      className="relative isolate overflow-hidden"
    >
      {/* the row background is performance strength; the label reads as top percentile. */}
      <motion.span
        aria-hidden="true"
        className={cn(
          'absolute inset-y-0 left-0 -z-10 w-full origin-left',
          expanded
            ? 'bg-[rgb(var(--fm-accent-rgb)/0.06)]'
            : 'bg-[rgb(var(--fm-accent-rgb)/0.045)]',
        )}
        initial={reduce ? false : { scaleX: 0 }}
        animate={{ scaleX: Math.max(0.08, Math.min(1, liveScore / 100)) }}
        transition={reduce ? { duration: 0 } : { delay: 0.12 + rank * 0.05, duration: 0.8, ease: SOFT_EASE }}
      />
      <span
        aria-hidden="true"
        className="pointer-events-none absolute -left-1.5 top-1/2 -z-10 -translate-y-1/2 select-none text-[74px] font-black tabular-nums leading-none tracking-tight text-foreground/[0.055]"
      >
        {rank}
      </span>

      <motion.button
        type="button"
        onClick={onToggle}
        whileTap={reduce ? undefined : { scale: 0.995 }}
        className="relative z-10 grid w-full grid-cols-[minmax(0,1fr)_auto_18px] items-center gap-3 py-4 pl-12 pr-3 text-left outline-none sm:gap-4 sm:py-5 sm:pl-16 sm:pr-4 [-webkit-tap-highlight-color:transparent]"
        aria-expanded={expanded}
        aria-label={`${expanded ? 'Collapse' : 'Open'} ${feeder.handle}`}
      >
        <span className="min-w-0">
          <span className="block truncate text-[20px] font-black leading-none text-foreground sm:text-[24px]">{feeder.handle}</span>
          <span className="mt-1.5 block truncate text-[8.5px] font-black uppercase tracking-[0.16em] text-foreground/38">
            {feeder.signal}
          </span>
        </span>

        <span className="flex items-baseline gap-2.5 text-right">
          <MoveChip move={feeder.move} />
          <span className="text-[26px] font-black tabular-nums leading-none text-foreground fm-depth-title sm:text-[30px]">
            <span className="mr-1 align-[0.25em] text-[0.42em] text-foreground/38">TOP</span>
            {topPercent}
            <span className="text-[0.52em] text-[var(--fm-accent)]">%</span>
          </span>
        </span>

        <motion.span
          className="grid place-items-center text-foreground/26"
          initial={false}
          animate={{ rotate: expanded ? 90 : 0 }}
          transition={reduce ? { duration: 0 } : LADDER_SPRING}
        >
          <ChevronRight size={16} strokeWidth={3} />
        </motion.span>
      </motion.button>

      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div
            key="dossier"
            initial={reduce ? false : { height: 0, opacity: 0 }}
            animate={{ height: 'auto', opacity: 1 }}
            exit={reduce ? { opacity: 0 } : { height: 0, opacity: 0 }}
            transition={{
              height: { duration: 0.42, ease: SOFT_EASE },
              opacity: { duration: 0.26, ease: SOFT_EASE },
            }}
            className="relative z-10 overflow-hidden"
          >
            <RowDossier feed={feed} feeder={feeder} timeframe={timeframe} reduce={reduce} />
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}

function Ladder({
  feed,
  timeframe,
  selectedFeeder,
  onSelectFeeder,
}: {
  feed: FeedBoard;
  timeframe: typeof TIMEFRAMES[number];
  selectedFeeder: string;
  onSelectFeeder: (handle: string) => void;
}) {
  const reduce = Boolean(useReducedMotion());
  const ranked = useMemo(
    () =>
      feed.feeders
        .map((feeder) => ({ feeder, live: timeframeScore(feeder, timeframe) }))
        .sort((a, b) => b.live - a.live),
    [feed, timeframe],
  );
  const [throne, ...challengers] = ranked;
  const toggle = (handle: string) => onSelectFeeder(selectedFeeder === handle ? FEED_SCOPE : handle);

  return (
    <section className="space-y-3">
      <div className="flex items-end justify-between gap-3 px-1.5">
        <div>
          <div className="text-[10px] font-black uppercase tracking-[0.18em] text-[var(--fm-accent)]">Feederboard</div>
          <div className="mt-1.5 text-[28px] font-black leading-none text-foreground fm-depth-title sm:text-[32px]">{feed.name}</div>
        </div>
        <div className="pb-0.5 text-right text-[9px] font-black uppercase tracking-[0.16em] text-foreground/34">
          {timeframe} window
          <span className="mx-1.5 text-foreground/20">/</span>
          {ranked.length} feeders
        </div>
      </div>

      <LayoutGroup id={`ladder-${feed.id}`}>
        <ThroneCard
          feed={feed}
          feeder={throne.feeder}
          liveScore={throne.live}
          timeframe={timeframe}
          expanded={selectedFeeder === throne.feeder.handle}
          onToggle={() => toggle(throne.feeder.handle)}
          reduce={reduce}
        />

        <motion.div layout className="fm-depth-glass overflow-hidden rounded-[26px]">
          <div className="flex items-center justify-between gap-3 border-b border-foreground/[0.06] px-4 py-3 sm:px-5">
            <span className="text-[9px] font-black uppercase tracking-[0.2em] text-foreground/38">Chasing the throne</span>
            <span className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/30">lower is better</span>
          </div>
          <div className="divide-y divide-foreground/[0.05]">
            {challengers.map(({ feeder, live }, index) => (
              <ChallengerRow
                key={feeder.handle}
                feed={feed}
                feeder={feeder}
                rank={index + 2}
                liveScore={live}
                timeframe={timeframe}
                expanded={selectedFeeder === feeder.handle}
                onToggle={() => toggle(feeder.handle)}
                reduce={reduce}
              />
            ))}
          </div>
        </motion.div>
      </LayoutGroup>
    </section>
  );
}

const METRIC_TABS = [
  { id: 'comments', label: 'Comments', icon: MessageCircle, pick: (feeder: Feeder) => feeder.comments, unit: 'comments' },
  { id: 'likes', label: 'Likes', icon: Heart, pick: (feeder: Feeder) => feeder.likes, unit: 'likes' },
  { id: 'engaged', label: 'Top', icon: Trophy, pick: (feeder: Feeder) => String(topPercentFromScore(feeder.score)), unit: 'top percentile' },
] as const;
type MetricId = typeof METRIC_TABS[number]['id'];

type AwardPost = {
  feedId: string;
  feedName: string;
  postKey: string;
  thumbnailUrl: string | null;
  handle: string | null;
  postedAt: string | null;
  latestPercentile: number | null;
  views: number | null;
  likes: number | null;
  comments: number | null;
};

type AwardCard = {
  id: string;
  label: string;
  value: string;
  unit: string;
  post: AwardPost;
};

function daysForTimeframe(timeframe: typeof TIMEFRAMES[number]) {
  return Number.parseInt(timeframe.replace(/\D/g, ''), 10) || 30;
}

function postTime(value: string | null) {
  if (!value) return 0;
  const parsed = Date.parse(value);
  return Number.isFinite(parsed) ? parsed : 0;
}

function postMetric(value: number | null) {
  return Number.isFinite(Number(value)) ? Number(value) : 0;
}

function formatAwardMetric(value: number | null) {
  return compactNumber(postMetric(value));
}

function postsInWindow(posts: AwardPost[], timeframe: typeof TIMEFRAMES[number]) {
  const since = Date.now() - daysForTimeframe(timeframe) * 24 * 60 * 60 * 1000;
  return posts.filter((post) => postTime(post.postedAt) === 0 || postTime(post.postedAt) >= since);
}

function bestPost(posts: AwardPost[], compare: (a: AwardPost, b: AwardPost) => number) {
  return [...posts].sort(compare)[0] || null;
}

function buildAwardCards(posts: AwardPost[], timeframe: typeof TIMEFRAMES[number]): AwardCard[] {
  const scoped = postsInWindow(posts, timeframe);
  const cards: AwardCard[] = [];
  const used = new Set<string>();
  const add = (id: string, label: string, value: string, unit: string, post: AwardPost | null) => {
    if (!post) return;
    const key = `${id}:${post.postKey}`;
    if (used.has(key)) return;
    used.add(key);
    cards.push({ id, label, value, unit, post });
  };

  const best = bestPost(scoped.filter((post) => post.latestPercentile != null), (a, b) => postMetric(a.latestPercentile) - postMetric(b.latestPercentile));
  add('best-post', 'Award Winner', best?.latestPercentile != null ? `${Math.round(best.latestPercentile)}%` : '--', 'top', best);

  const mostLiked = bestPost(scoped, (a, b) => postMetric(b.likes) - postMetric(a.likes));
  add('most-liked', 'Most Liked Post', formatAwardMetric(mostLiked?.likes ?? null), 'likes', mostLiked);

  const leastLiked = bestPost(scoped.filter((post) => post.likes != null), (a, b) => postMetric(a.likes) - postMetric(b.likes));
  add('least-liked', 'Least Liked Post', formatAwardMetric(leastLiked?.likes ?? null), 'likes', leastLiked);

  const mostCommented = bestPost(scoped, (a, b) => postMetric(b.comments) - postMetric(a.comments));
  add('most-commented', 'Most Commented', formatAwardMetric(mostCommented?.comments ?? null), 'comments', mostCommented);

  const leastCommented = bestPost(scoped.filter((post) => post.comments != null), (a, b) => postMetric(a.comments) - postMetric(b.comments));
  add('least-commented', 'Least Commented', formatAwardMetric(leastCommented?.comments ?? null), 'comments', leastCommented);

  const mostViewed = bestPost(scoped, (a, b) => postMetric(b.views) - postMetric(a.views));
  add('most-viewed', 'Most Viewed', formatAwardMetric(mostViewed?.views ?? null), 'views', mostViewed);

  const byHandle = new Map<string, AwardPost[]>();
  for (const post of scoped) {
    const handle = (post.handle || 'unknown').toLowerCase();
    byHandle.set(handle, [...(byHandle.get(handle) || []), post]);
  }
  const busiest = [...byHandle.values()]
    .map((handlePosts) => ({
      posts: handlePosts,
      latest: bestPost(handlePosts, (a, b) => postTime(b.postedAt) - postTime(a.postedAt)),
    }))
    .sort((a, b) => b.posts.length - a.posts.length)[0];
  add(
    'most-posts',
    timeframe === '90D' ? 'Most Posts / Month' : timeframe === '7D' ? 'Most Posts / Day' : 'Most Posts / Week',
    String(busiest?.posts.length || 0),
    timeframe === '90D' ? 'posts / month' : timeframe === '7D' ? 'posts / day' : 'posts / week',
    busiest?.latest || null,
  );

  const gap = [...byHandle.values()]
    .map((handlePosts) => {
      const sorted = [...handlePosts].sort((a, b) => postTime(a.postedAt) - postTime(b.postedAt));
      let max: { days: number; post: AwardPost } | null = null;
      for (let index = 1; index < sorted.length; index += 1) {
        const gapDays = Math.round((postTime(sorted[index].postedAt) - postTime(sorted[index - 1].postedAt)) / (24 * 60 * 60 * 1000));
        if (!max || gapDays > max.days) max = { days: gapDays, post: sorted[index] };
      }
      return max;
    })
    .filter((entry): entry is { days: number; post: AwardPost } => Boolean(entry))
    .sort((a, b) => b.days - a.days)[0];
  add('longest-gap', 'Longest Gap Return', `${Math.max(0, gap?.days || 0)}d`, 'gap', gap?.post || null);

  const quietWinner = bestPost(
    scoped.filter((post) => post.latestPercentile != null),
    (a, b) => (postMetric(a.latestPercentile) + postMetric(a.comments) / 500) - (postMetric(b.latestPercentile) + postMetric(b.comments) / 500),
  );
  add('quiet-winner', 'Quiet Winner', quietWinner?.latestPercentile != null ? `${Math.round(quietWinner.latestPercentile)}%` : '--', 'top', quietWinner);

  for (const post of [...scoped].sort((a, b) => postTime(b.postedAt) - postTime(a.postedAt))) {
    if (cards.length >= 10) break;
    add(`recent-${cards.length}`, 'Recent Fire Card', post.latestPercentile != null ? `${Math.round(post.latestPercentile)}%` : formatAwardMetric(post.likes), post.latestPercentile != null ? 'top' : 'likes', post);
  }

  return cards.slice(0, 10);
}

function postCardsForMetric(posts: AwardPost[], timeframe: typeof TIMEFRAMES[number], metricId: MetricId): AwardCard[] {
  const scoped = postsInWindow(posts, timeframe);
  if (metricId === 'engaged') return buildAwardCards(posts, timeframe);

  const metric = metricId === 'comments'
    ? {
        label: 'Comment Driver',
        unit: 'comments',
        value: (post: AwardPost) => postMetric(post.comments),
        display: (post: AwardPost) => formatAwardMetric(post.comments),
      }
    : {
        label: 'Like Driver',
        unit: 'likes',
        value: (post: AwardPost) => postMetric(post.likes),
        display: (post: AwardPost) => formatAwardMetric(post.likes),
      };

  return [...scoped]
    .sort((a, b) => {
      const delta = metric.value(b) - metric.value(a);
      return delta !== 0 ? delta : postTime(b.postedAt) - postTime(a.postedAt);
    })
    .slice(0, 10)
    .map((post, index) => ({
      id: `${metricId}:${index}:${post.postKey}`,
      label: metric.label,
      value: metric.display(post),
      unit: metric.unit,
      post,
    }));
}

function buildTimelinePosts(posts: AwardPost[], timeframe: typeof TIMEFRAMES[number]) {
  return postsInWindow(posts, timeframe)
    .filter((post) => postTime(post.postedAt) > 0)
    .sort((a, b) => postTime(a.postedAt) - postTime(b.postedAt))
    .slice(-8);
}

function shortPostDate(value: string | null) {
  const time = postTime(value);
  if (!time) return 'live';
  return new Intl.DateTimeFormat('en', { month: 'short', day: 'numeric' }).format(new Date(time));
}

function MetricLeaders({
  feeds,
  activeFeedId,
  selectedFeeder,
  timeframe,
}: {
  feeds: FeedBoard[];
  activeFeedId: string;
  selectedFeeder: string;
  timeframe: typeof TIMEFRAMES[number];
}) {
  const reduce = Boolean(useReducedMotion());
  const [metricId, setMetricId] = useState<MetricId>('comments');
  const tab = METRIC_TABS.find((entry) => entry.id === metricId) || METRIC_TABS[0];
  const [awardPostState, setAwardPostState] = useState<{ key: string; posts: AwardPost[] }>({ key: '', posts: [] });
  const activeFeed = feeds.find((feed) => feed.id === activeFeedId) || null;
  const showingPostEvidence = Boolean(activeFeed && selectedFeeder !== FEED_SCOPE);

  const ranked = useMemo(() => {
    const pool = activeFeedId === 'all'
      ? flattenFeeders(feeds)
      : flattenFeeders(feeds).filter((entry) => entry.feed.id === activeFeedId);
    return [...pool]
      .sort((a, b) => numericMetric(tab.pick(b.feeder)) - numericMetric(tab.pick(a.feeder)))
      .slice(0, 10);
  }, [activeFeedId, feeds, tab]);
  const fetchTargets = useMemo(() => {
    if (showingPostEvidence && activeFeed && isNumericFeedId(activeFeed.id)) {
      return [{
        id: activeFeed.id,
        name: activeFeed.name,
        handle: selectedFeeder.replace(/^@+/, ''),
      }];
    }

    if (metricId !== 'engaged') return [];

    return (activeFeedId === 'all' ? feeds : feeds.filter((feed) => feed.id === activeFeedId))
      .filter((feed) => isNumericFeedId(feed.id))
      .map((feed) => ({ id: feed.id, name: feed.name, handle: 'all' }));
  }, [activeFeed, activeFeedId, feeds, metricId, selectedFeeder, showingPostEvidence]);
  const fetchTargetKey = fetchTargets.map((target) => `${target.id}:${target.handle}`).join('|');
  const awardPosts = useMemo(
    () => (awardPostState.key === fetchTargetKey ? awardPostState.posts : []),
    [awardPostState.key, awardPostState.posts, fetchTargetKey],
  );
  const awardCards = useMemo(() => buildAwardCards(awardPosts, timeframe), [awardPosts, timeframe]);
  const postCards = useMemo(() => postCardsForMetric(awardPosts, timeframe, metricId), [awardPosts, metricId, timeframe]);
  const timelinePosts = useMemo(() => buildTimelinePosts(awardPosts, timeframe), [awardPosts, timeframe]);

  useEffect(() => {
    if (fetchTargets.length === 0) return undefined;

    let cancelled = false;
    const controller = new AbortController();

    Promise.all(fetchTargets.map(async (feed) => {
      const params = new URLSearchParams({ feedId: feed.id, handle: feed.handle });
      const response = await fetch(`/api/feed/feeder-posts?${params.toString()}`, {
        cache: 'no-store',
        credentials: 'include',
        signal: controller.signal,
      });
      if (!response.ok) return [];
      const payload = await response.json() as { posts?: Array<Omit<AwardPost, 'feedId' | 'feedName'>> };
      return (payload.posts || []).map((post) => ({ ...post, feedId: feed.id, feedName: feed.name }));
    }))
      .then((groups) => {
        if (!cancelled) setAwardPostState({ key: fetchTargetKey, posts: groups.flat() });
      })
      .catch((error: unknown) => {
        if (!controller.signal.aborted) console.warn('Cross-feed awards unavailable', error);
      })

    return () => {
      cancelled = true;
      controller.abort();
    };
  }, [fetchTargetKey, fetchTargets]);
  const showingAwards = !showingPostEvidence && metricId === 'engaged';
  const postEvidenceCards = showingPostEvidence ? postCards : awardCards;
  const scopeLabel = showingPostEvidence
    ? 'Feeder posts'
    : activeFeedId === 'all'
      ? 'Cross-feed'
      : 'Feed compare';
  const sectionTitle = showingPostEvidence
    ? 'Post Evidence'
    : showingAwards
      ? 'Post Awards'
      : activeFeed
        ? `${activeFeed.name} Leaders`
        : 'Top 10 Leaders';
  const sectionContext = showingPostEvidence
    ? `${selectedFeeder} / ${timeframe} window`
    : `${activeFeed?.name || 'All feeds'} / ${timeframe} window`;

  return (
    <section className="fm-depth-glass rounded-[26px] p-4 sm:p-5 lg:p-6">
      <div className="mb-4 flex flex-wrap items-center justify-between gap-3 sm:mb-5">
        <div>
          <div className="text-[10px] font-black uppercase tracking-[0.18em] text-[var(--fm-accent)]">{scopeLabel}</div>
          <div className="mt-1.5 text-[27px] font-black leading-none text-foreground fm-depth-title sm:text-[32px]">
            {sectionTitle}
          </div>
          <div className="mt-1.5 text-[11px] font-black uppercase tracking-[0.13em] text-foreground/34">{sectionContext}</div>
        </div>

        <div className="relative flex items-center gap-1 rounded-[14px] border border-black/5 bg-black/[0.035] p-1 shadow-[inset_0_2px_8px_rgba(0,0,0,0.04)] dark:border-white/8 dark:bg-white/[0.03]">
          {METRIC_TABS.map((entry) => {
            const active = entry.id === metricId;
            return (
              <button
                key={entry.id}
                type="button"
                onClick={() => setMetricId(entry.id)}
                className="relative rounded-[10px] px-2.5 py-1.5 outline-none [-webkit-tap-highlight-color:transparent]"
                aria-pressed={active}
              >
                {active && (
                  <motion.span
                    layoutId="metric-tab-pill"
                    className="absolute inset-0 rounded-[10px] bg-[var(--fm-accent)] shadow-[0_6px_14px_rgb(var(--fm-accent-rgb)/0.22)]"
                    transition={reduce ? { duration: 0 } : LADDER_SPRING}
                  />
                )}
                <span
                  className={cn(
                    'relative z-10 flex items-center gap-1.5 text-[10px] font-black uppercase tracking-[0.1em] transition-colors duration-200',
                    active ? 'text-white' : 'text-foreground/44',
                  )}
                >
                  <entry.icon size={13} strokeWidth={3} />
                  <span className="hidden sm:inline">{entry.label}</span>
                </span>
              </button>
            );
          })}
        </div>
      </div>

      {timelinePosts.length > 0 && (
        <div className="mb-4 overflow-hidden rounded-[20px] border border-foreground/[0.06] bg-foreground/[0.025] px-3 py-3 sm:px-4">
          <div className="mb-3 flex items-center justify-between gap-3">
            <span className="text-[8.5px] font-black uppercase tracking-[0.18em] text-foreground/34">Timeline</span>
            <span className="text-[8.5px] font-black uppercase tracking-[0.16em] text-foreground/28">{timelinePosts.length} posts in window</span>
          </div>
          <div className="grid grid-cols-[repeat(var(--timeline-count),minmax(34px,1fr))] items-end gap-1.5" style={{ '--timeline-count': timelinePosts.length } as CSSProperties}>
            {timelinePosts.map((post, index) => {
              const height = 18 + Math.min(42, Math.max(postMetric(post.comments), postMetric(post.likes) / 18, postMetric(post.views) / 260));
              return (
                <div key={`${post.postKey}:${index}`} className="min-w-0">
                  <div className="relative h-[54px] rounded-[14px] bg-white/[0.035] dark:bg-black/10">
                    <span
                      className="absolute bottom-0 left-1/2 w-[62%] -translate-x-1/2 rounded-t-full bg-[var(--fm-accent)]/78 shadow-[0_10px_24px_-16px_rgb(var(--fm-accent-rgb)/0.8)]"
                      style={{ height }}
                    />
                  </div>
                  <div className="mt-1.5 truncate text-center text-[7px] font-black uppercase tracking-[0.08em] text-foreground/30">{shortPostDate(post.postedAt)}</div>
                </div>
              );
            })}
          </div>
        </div>
      )}

      <div className="hide-scrollbar flex gap-3 overflow-x-auto pb-1 md:grid md:grid-cols-3 md:gap-3.5 md:overflow-visible lg:grid-cols-5 xl:gap-4">
        <AnimatePresence mode="popLayout" initial={false}>
          {(showingAwards || showingPostEvidence) ? postEvidenceCards.map((entry, index) => (
            <motion.div
              key={`${entry.id}:${entry.post.postKey}`}
              initial={reduce ? false : { opacity: 0, y: 16, filter: 'blur(6px)' }}
              animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
              exit={reduce ? { opacity: 0 } : { opacity: 0, y: -10, filter: 'blur(6px)' }}
              transition={{ delay: reduce ? 0 : index * 0.045, duration: 0.32, ease: SOFT_EASE }}
              className="relative w-[178px] shrink-0 overflow-hidden rounded-[22px] border border-black/[0.045] bg-white/54 p-2.5 dark:border-white/[0.065] dark:bg-white/[0.035] sm:w-[198px] md:w-auto md:p-3"
            >
              <span
                className={cn(
                  'absolute left-4 top-4 z-10 grid h-7 w-7 place-items-center rounded-full text-[12px] font-black tabular-nums leading-none shadow-[0_6px_14px_rgba(0,0,0,0.28)]',
                  index === 0 ? 'bg-[var(--fm-accent)] text-white' : 'bg-black/55 text-white backdrop-blur-md',
                )}
              >
                {index + 1}
              </span>
              <div className="relative h-[136px] overflow-hidden rounded-[17px] sm:h-[154px] md:h-[168px] lg:h-[174px]">
                {entry.post.thumbnailUrl ? (
                  // eslint-disable-next-line @next/next/no-img-element -- remote post media
                  <img
                    src={entry.post.thumbnailUrl}
                    alt=""
                    className="h-full w-full object-cover"
                    loading="lazy"
                    decoding="async"
                  />
                ) : (
                  <Thumb tone="rose" label={entry.post.feedName} className="h-full rounded-[16px]" />
                )}
                <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.02),rgba(0,0,0,0.24))]" />
                <span className="absolute bottom-2.5 left-2.5 rounded-full bg-black/54 px-2.5 py-1 text-[7.5px] font-black uppercase tracking-[0.14em] text-white/84 backdrop-blur-md">
                  {entry.post.feedName}
                </span>
              </div>
              <div className="mt-3 min-w-0 px-0.5 pb-0.5">
                <div className="truncate text-[11px] font-black uppercase tracking-[0.12em] text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)]">
                  {entry.label}
                </div>
                <div className="mt-1 truncate text-[14px] font-black leading-none text-foreground sm:text-[15px]">
                  @{entry.post.handle || 'feeder'}
                </div>
                <div className="mt-2 flex items-baseline gap-1.5">
                  <span className="text-[26px] font-black tabular-nums leading-none text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)] sm:text-[30px]">
                    {entry.value}
                  </span>
                  <span className="truncate text-[8.5px] font-black uppercase tracking-[0.12em] text-foreground/38">{entry.unit}</span>
                </div>
              </div>
            </motion.div>
          )) : ranked.map((entry, index) => (
            <motion.div
              key={`${metricId}:${entry.feed.id}:${entry.feeder.handle}`}
              initial={reduce ? false : { opacity: 0, y: 16, filter: 'blur(6px)' }}
              animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
              exit={reduce ? { opacity: 0 } : { opacity: 0, y: -10, filter: 'blur(6px)' }}
              transition={{ delay: reduce ? 0 : index * 0.045, duration: 0.32, ease: SOFT_EASE }}
              className="relative w-[178px] shrink-0 overflow-hidden rounded-[22px] border border-black/[0.045] bg-white/54 p-2.5 dark:border-white/[0.065] dark:bg-white/[0.035] sm:w-[198px] md:w-auto md:p-3"
            >
              <span
                className={cn(
                  'absolute left-4 top-4 z-10 grid h-7 w-7 place-items-center rounded-full text-[12px] font-black tabular-nums leading-none shadow-[0_6px_14px_rgba(0,0,0,0.28)]',
                  index === 0 ? 'bg-[var(--fm-accent)] text-white' : 'bg-black/55 text-white backdrop-blur-md',
                )}
              >
                {index + 1}
              </span>
              <div className="relative h-[136px] overflow-hidden rounded-[17px] sm:h-[154px] md:h-[168px] lg:h-[174px]">
                {entry.feeder.profilePicUrl ? (
                  // eslint-disable-next-line @next/next/no-img-element -- remote feeder media
                  <img
                    src={entry.feeder.profilePicUrl}
                    alt=""
                    className="h-full w-full object-cover"
                    loading="lazy"
                    decoding="async"
                  />
                ) : (
                  <Thumb tone={entry.feeder.thumb} label={entry.feed.name} className="h-full rounded-[16px]" />
                )}
                <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.02),rgba(0,0,0,0.24))]" />
                <span className="absolute bottom-2.5 left-2.5 rounded-full bg-black/54 px-2.5 py-1 text-[7.5px] font-black uppercase tracking-[0.14em] text-white/84 backdrop-blur-md">
                  {entry.feed.name}
                </span>
              </div>
              <div className="mt-3 min-w-0 px-0.5 pb-0.5">
                <div className="truncate text-[14px] font-black leading-none text-foreground sm:text-[15px]">{entry.feeder.handle}</div>
                <div className="mt-2 flex items-baseline gap-1.5">
                  <span className="text-[26px] font-black tabular-nums leading-none text-[var(--fm-accent)] dark:text-[var(--fm-accent-bright)] sm:text-[30px]">
                    {tab.pick(entry.feeder)}
                  </span>
                  <span className="truncate text-[8.5px] font-black uppercase tracking-[0.12em] text-foreground/38">{tab.unit}</span>
                </div>
              </div>
            </motion.div>
          ))}
        </AnimatePresence>
        {(showingAwards || showingPostEvidence) && postEvidenceCards.length === 0 && (
          <div className="col-span-full rounded-[18px] border border-foreground/[0.06] bg-foreground/[0.025] px-4 py-8 text-center text-[11px] font-black uppercase tracking-[0.16em] text-foreground/36">
            {showingPostEvidence ? 'No tracked posts in this window' : 'No post awards yet'}
          </div>
        )}
      </div>
    </section>
  );
}

function FocusBoard({
  feed,
  timeframe,
  selectedFeeder,
  onSelectFeeder,
}: {
  feed: FeedBoard;
  timeframe: typeof TIMEFRAMES[number];
  selectedFeeder: string;
  onSelectFeeder: (handle: string) => void;
}) {
  return (
    <Ladder
      feed={feed}
      timeframe={timeframe}
      selectedFeeder={selectedFeeder}
      onSelectFeeder={onSelectFeeder}
    />
  );
}

export default function ReadPreviewPage() {
  const pageScrollRef = useRef<HTMLElement | null>(null);
  const feedHomeXRef = useRef<Record<string, number>>({});
  const { appShellStyle, useBrowserPageScroll, useTranslucentBrowserChrome } = useMobileImmersiveViewport();
  const headerPortal = typeof document === 'undefined' ? null : document.body;
  const [boards, setBoards] = useState<FeedBoard[]>(FEEDS);
  const [activeFeedId, setActiveFeedId] = useState('all');
  const [timeframe, setTimeframe] = useState<typeof TIMEFRAMES[number]>('30D');
  const activeFeed = boards.find((feed) => feed.id === activeFeedId) || null;
  const [selectedFeeder, setSelectedFeeder] = useState(FEED_SCOPE);
  const [feedRailMotion, setFeedRailMotion] = useState<FeedRailMotion | null>(null);
  const headerCompressed = useCompressedOnScroll(pageScrollRef, useBrowserPageScroll, {
    collapseDistance: 180,
    expandDistance: 78,
    topGuard: 32,
  });

  useEffect(() => {
    document.title = 'Lead Feederboard';
  }, []);

  useEffect(() => {
    let cancelled = false;

    fetch('/api/feed', { cache: 'no-store' })
      .then(async (res) => {
        const json = await res.json();
        if (!res.ok) throw new Error(json.error || 'Failed to load feeds');
        const feeds = Array.isArray(json.feeds) ? json.feeds as FeedApiFeed[] : [];
        return apiFeedsToBoards(feeds);
      })
      .then((nextBoards) => {
        if (cancelled) return;
        setBoards(nextBoards);
        setActiveFeedId((current) => (
          current === 'all' || nextBoards.some((feed) => feed.id === current)
            ? current
            : 'all'
        ));
        setSelectedFeeder((current) => (
          current === FEED_SCOPE || nextBoards.some((feed) => feed.feeders.some((feeder) => feeder.handle === current))
            ? current
            : FEED_SCOPE
        ));
      })
      .catch((error) => {
        console.warn('Lead feederboard using fallback feeds', error);
      });

    return () => {
      cancelled = true;
    };
  }, []);

  const selectedFeederForActiveFeed = !activeFeed
    ? selectedFeeder
    : selectedFeeder === FEED_SCOPE || activeFeed.feeders.some((feeder) => feeder.handle === selectedFeeder)
      ? selectedFeeder
      : FEED_SCOPE;

  useEffect(() => {
    if (!feedRailMotion) return undefined;

    const timeout = window.setTimeout(() => setFeedRailMotion(null), 1250);
    return () => window.clearTimeout(timeout);
  }, [feedRailMotion]);

  const feedSlotX = (id: string) => {
    const index = boards.findIndex((feed) => feed.id === id);
    return index >= 0 ? (index + 1) * HEADER_FEED_SLOT : HEADER_FEED_SLOT;
  };

  const measuredRailX = (sourceEl?: HTMLElement) => {
    const rail = sourceEl?.closest<HTMLElement>('[data-testid="lead-header-rail"]');
    const railRect = rail?.getBoundingClientRect();
    const sourceRect = sourceEl?.getBoundingClientRect();

    if (!railRect || !sourceRect || sourceRect.width <= 0) return null;

    return Math.max(0, Math.round(sourceRect.left - railRect.left));
  };

  const handleFeedChange = (id: string, sourceEl?: HTMLElement) => {
    if (id === activeFeedId) return;

    let nextMotion: FeedRailMotion | null = null;

    if (activeFeedId === 'all' && id !== 'all') {
      const originX = measuredRailX(sourceEl) ?? feedSlotX(id);
      feedHomeXRef.current[id] = originX;
      nextMotion = { id, mode: 'opening', x: originX };
    } else if (id === 'all' && activeFeedId !== 'all') {
      nextMotion = {
        id: activeFeedId,
        mode: 'closing',
        x: feedHomeXRef.current[activeFeedId] ?? feedSlotX(activeFeedId),
      };
    }

    setFeedRailMotion(nextMotion);
    setActiveFeedId(id);
    setSelectedFeeder(FEED_SCOPE);
  };

  const rootStyle = {
    ...appShellStyle,
    '--lead-header-space': 'calc(216px + env(safe-area-inset-top))',
  } as CSSProperties;
  const headerElement = (
    <Header
      feeds={boards}
      activeFeedId={activeFeedId}
      selectedFeeder={selectedFeederForActiveFeed}
      timeframe={timeframe}
      compressed={headerCompressed}
      railMotion={feedRailMotion}
      onFeedChange={handleFeedChange}
      onFeederChange={setSelectedFeeder}
      onTimeframeChange={setTimeframe}
    />
  );

  return (
    <main
      ref={pageScrollRef}
      className={cn(
        'fm-dashboard-mesh hide-scrollbar relative w-full min-w-0 max-w-[100dvw] overflow-x-hidden text-foreground select-none',
        useTranslucentBrowserChrome ? 'bg-transparent' : 'bg-background',
        useBrowserPageScroll ? 'overflow-y-visible' : 'overflow-y-auto',
      )}
      style={rootStyle}
    >
      {!useTranslucentBrowserChrome && (
        <div aria-hidden="true" className="pointer-events-none fixed inset-0 z-0 bg-background" />
      )}
      {headerPortal ? createPortal(headerElement, headerPortal) : headerElement}

      <div className="relative z-10 mx-auto w-full min-w-0 max-w-[1960px] px-3 pb-[calc(126px+env(safe-area-inset-bottom))] pt-[var(--lead-header-space)] sm:px-5 lg:px-6">
        <div className="mx-auto w-full min-w-0 max-w-[1540px] space-y-5">
          <PostMortemPanel
            feeds={boards}
            activeFeed={activeFeed}
            selectedFeeder={selectedFeederForActiveFeed}
            timeframe={timeframe}
          />

          <AnimatePresence mode="wait" initial={false}>
            <motion.div
              key={`board:${activeFeedId}`}
              initial={{ opacity: 0, y: 18, filter: 'blur(8px)' }}
              animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
              exit={{ opacity: 0, y: -12, filter: 'blur(8px)' }}
              transition={{ duration: 0.42, ease: SOFT_EASE }}
            >
              {activeFeed ? (
                <FocusBoard
                  feed={activeFeed}
                  timeframe={timeframe}
                  selectedFeeder={selectedFeederForActiveFeed}
                  onSelectFeeder={setSelectedFeeder}
                />
              ) : (
                <AllBoards feeds={boards} timeframe={timeframe} />
              )}
            </motion.div>
          </AnimatePresence>

          <GrowthWeeks
            feeds={boards}
            activeFeed={activeFeed}
            selectedFeeder={selectedFeederForActiveFeed}
            timeframe={timeframe}
          />

          <MetricLeaders
            feeds={boards}
            activeFeedId={activeFeedId}
            selectedFeeder={selectedFeederForActiveFeed}
            timeframe={timeframe}
          />
        </div>
      </div>
    </main>
  );
}
