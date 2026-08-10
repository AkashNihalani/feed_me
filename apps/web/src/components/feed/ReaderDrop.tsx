'use client';

/* ─────────────────────────────────────────────────────────────
   READER DROP — the weekly Feeder Reader release, staged as a
   full-screen story. Everything on screen is the reader's own
   output; the drop only decides weight and order.

   The grammar, in reading order:
   1  cover        — the week's header, big, plus the run's shape
   2  callback     — last run's next_watch settled by this run (run ≥ 2)
   3  portrait     — where the account stands: the active Bites, one screen
   ·  per Bite     — the read (title + current_read), then ONE proof beat
                     chosen by movement: flip / held / widened / entered.
                     Every proof beat carries the SPAN (where its evidence
                     lands in the 90-day memory) and the posts themselves.
   ·  what moved   — the week's developments (only when there are any)
   ·  the field    — rank pulse + signals, with the lane split and cadence
   ·  the frame    — visible recurrences and tripped triggers
   ·  next watch   — the cliffhanger, stamped on record
   ·  sign-off

   Rank language: users only ever see OVERALL rank (the 90-day memory).
   Recent rank is the model's internal positioning signal — it stays in
   the payload and off the screen.

   Motion rules: transform/opacity only, one shared ease, slow
   deliberate stagger. A held week must LOOK calm; a recast week
   must look like something broke.

   Mobile: every beat must fit a toolbar-shrunk Safari viewport —
   tight vertical rhythm, capped list rows (nth-child, no JS), and
   safe-area bottom padding. A beat never scrolls internally.
   ───────────────────────────────────────────────────────────── */

import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from 'react';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { cn } from '@/lib/utils';
import { Eyebrow, HunchRibbon, wedgeGradient, VISIT_EASE } from '@/components/feed/FeederVisit';
import Odometer from '@/components/login/Odometer';
import LaneLeaders, { type LaneLeaderPost } from '@/app/drop/artifacts/lane-leaders/LaneLeaders';
import Followers, { type FollowerRunWindow } from '@/app/drop/artifacts/followers/Followers';
import type { DropBite, DropPost, DropProof, DropStats, ReaderDropModel } from '@/lib/readerDropModel';

export type ReaderDropArtifacts = {
  laneLeaders?: boolean;
  followers?: FollowerRunWindow[];
};

/* ── shared motion ──────────────────────────────────────────── */

const CONTAINER = {
  hidden: {},
  visible: { transition: { staggerChildren: 0.16, delayChildren: 0.12 } },
} as const;

const LINE = {
  hidden: { opacity: 0, transform: 'translate3d(0, 20px, 0)' },
  visible: {
    opacity: 1,
    transform: 'translate3d(0, 0, 0)',
    transition: {
      opacity: { duration: 0.45, ease: VISIT_EASE },
      transform: { duration: 0.6, ease: VISIT_EASE },
    },
  },
} as const;

const num = (value: number) => String(value).padStart(2, '0');

function parseOverall(value?: string): { rank: number; pool: number } | null {
  const match = value?.match(/^(\d+)\/(\d+)/);
  if (!match) return null;
  const rank = Number(match[1]);
  const pool = Number(match[2]);
  return pool > 1 ? { rank, pool } : null;
}

/* ── the one shell — every beat renders into the same skeleton ─
   Bottom padding reserves room for browser toolbars + the app's
   floating chrome; a beat's content must never sit under either. */

const WRAP =
  'relative mx-auto box-border w-full max-w-[640px] px-6 pt-12 pb-[calc(env(safe-area-inset-bottom,0px)+104px)] lg:w-[min(1120px,86vw)] lg:max-w-none lg:px-0 lg:py-16 xl:w-[min(1120px,calc(100vw-320px))]';

function Beat({
  eyebrow,
  accent,
  children,
  stage,
  align = 'center',
}: {
  eyebrow: React.ReactNode;
  accent?: boolean;
  children: React.ReactNode;
  stage?: React.ReactNode;
  align?: 'center' | 'end';
}) {
  return (
    <section className={cn('relative flex min-h-[100dvh] w-screen max-w-[100vw]', align === 'end' ? 'items-end' : 'items-center')}>
      <motion.div
        className={cn(WRAP, stage && 'lg:grid lg:grid-cols-[minmax(0,1fr)_minmax(0,0.88fr)] lg:items-center lg:gap-x-[clamp(48px,6vw,120px)]')}
        variants={CONTAINER}
        initial="hidden"
        animate="visible"
      >
        <div className="min-w-0">
          <motion.div variants={LINE}>
            <Eyebrow accent={accent}>{eyebrow}</Eyebrow>
          </motion.div>
          {children}
        </div>
        {stage && <div className="mt-8 min-w-0 lg:mt-0">{stage}</div>}
      </motion.div>
    </section>
  );
}

/* ── evidence primitives — posts as content, never counts ──────
   Only overall rank is shown: where the post sits in the 90-day
   memory. The title is the object; the rank is context. */

/* An evidence row is a closed card: title + rank at rest, and the post's
   full context on tap — summary, hook, mechanic chain — cascading open
   with a CSS grid-rows transition (no layout thrash, no snap). */
function PostSourceLabel({ post, className }: { post: DropPost; className?: string }) {
  const label = (
    <>
      <span className="truncate">{post.title}</span>
      {post.url && <span aria-hidden className="shrink-0 text-[0.72em] opacity-55">↗</span>}
    </>
  );
  return post.url ? (
    <a
      href={post.url}
      target="_blank"
      rel="noopener noreferrer"
      aria-label={`Open ${post.title} on Instagram`}
      className={cn('inline-flex min-w-0 items-baseline gap-1.5 rounded-sm underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]/55', className)}
    >
      {label}
    </a>
  ) : (
    <span className={cn('inline-flex min-w-0 items-baseline gap-1.5', className)}>{label}</span>
  );
}

function ExpandedPostCard({ post }: { post: DropPost }) {
  const overall = parseOverall(post.overallRank);

  return (
    <div className="flex h-full min-w-0 gap-4 overflow-hidden pb-3 pl-[26px] pr-3 pt-3 lg:gap-5">
      <PostThumbCard post={post} size="lg" accent className="!w-[74px] !rounded-[13px] self-start" />
      <div className="flex min-w-0 flex-1 flex-col overflow-hidden py-0.5">
        <PostSourceLabel post={post} className="w-full text-[16px] font-black leading-[1.02] tracking-[-0.02em] text-foreground lg:text-[18px]" />
        {post.summary && (
          <p
            className="mt-2 text-[11.5px] font-bold leading-[1.42] text-foreground/55 lg:text-[12.5px]"
            style={{ display: '-webkit-box', WebkitLineClamp: 2, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}
          >
            {post.summary}
          </p>
        )}
        <div className="mt-3 flex min-w-0 items-end justify-between gap-4">
          {post.hook && (
            <p className="min-w-0 flex-1 truncate text-[11.5px] font-black leading-snug tracking-tight text-[#E11D48]/85">“{post.hook}”</p>
          )}
          {overall && (
            <span className="inline-flex shrink-0 items-baseline font-black leading-none">
              <span className="mr-1.5 text-[8px] uppercase tracking-[0.16em] text-foreground/38">landed</span>
              <span className="text-[25px] tabular-nums text-[#E11D48]">{overall.rank}</span>
              <span className="ml-1 text-[12px] tabular-nums text-foreground/35">/{overall.pool}</span>
            </span>
          )}
        </div>
      </div>
    </div>
  );
}

function PostRow({
  post,
  index,
  tone = 'plain',
  expanded = false,
  onToggle,
  constrained = false,
}: {
  post: DropPost;
  index?: number;
  tone?: 'plain' | 'accent' | 'boundary' | 'dim';
  expanded?: boolean;
  onToggle?: () => void;
  constrained?: boolean;
}) {
  const overall = parseOverall(post.overallRank);
  const expandable = Boolean(onToggle && (post.summary || post.hook || post.mechanic));
  const titleClass = cn(
    'min-w-0 flex-1 text-[14px] font-black leading-tight tracking-tight lg:text-[17px]',
    tone === 'accent' && 'text-foreground',
    tone === 'plain' && 'text-foreground/78',
    tone === 'dim' && 'text-foreground/45',
    tone === 'boundary' && 'text-foreground/60',
  );
  const rowContents = (
    <>
      {index != null && (
        <span className={cn('shrink-0 text-[11px] font-black tabular-nums', tone === 'accent' ? 'text-[#E11D48]' : 'text-foreground/28')}>
          {num(index)}
        </span>
      )}
      {expandable ? (
        <span className={cn(titleClass, 'truncate')}>{post.title}</span>
      ) : (
        <PostSourceLabel post={post} className={titleClass} />
      )}
      <span className="flex shrink-0 items-baseline gap-2">
        {tone === 'boundary' && (
          <span className="text-[8px] font-black uppercase tracking-[0.14em] text-[#E11D48]/80">the boundary</span>
        )}
        {overall && (
          <span className={cn('inline-flex items-baseline font-black tabular-nums leading-none', tone === 'accent' ? 'text-[#E11D48]' : 'text-foreground/55')}>
            <span className="text-[15px] lg:text-[17px]">{overall.rank}</span>
            <span className="ml-0.5 text-[10px] text-foreground/35">/{overall.pool}</span>
          </span>
        )}
      </span>
      {expandable && (
        <span
          aria-hidden
          className={cn('grid h-8 w-8 shrink-0 place-items-center rounded-full text-[10px] text-foreground/35 transition-[transform,color,background-color] duration-200', expanded && 'rotate-180 bg-[#E11D48]/10 text-[#E11D48]')}
        >
          ▾
        </span>
      )}
    </>
  );

  return (
    <motion.div
      variants={LINE}
      className={cn(
        'min-w-0 overflow-hidden border-t',
        constrained && (expanded ? 'grid h-full grid-rows-[54px_1fr]' : 'h-full'),
        tone === 'boundary' ? 'border-[#E11D48]/25' : 'border-foreground/10',
      )}
    >
      {expandable ? (
        <button
          type="button"
          onClick={onToggle}
          aria-expanded={expanded}
          aria-label={`${expanded ? 'Collapse' : 'Expand'} ${post.title}`}
          className={cn(
            'flex h-[54px] w-full min-w-0 items-center gap-3 text-left transition-colors duration-200 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[#E11D48]/55',
            expanded ? 'bg-foreground/[0.035]' : 'hover:bg-foreground/[0.025]',
          )}
        >
          {rowContents}
        </button>
      ) : (
        <div className="flex h-[54px] w-full min-w-0 items-center gap-3 text-left">{rowContents}</div>
      )}
      {expandable && (
        constrained ? (
          <div
            aria-hidden={!expanded}
            className={cn(
              'min-h-0 overflow-hidden transition-[opacity,transform] duration-200 [transition-timing-function:cubic-bezier(0.23,1,0.32,1)]',
              expanded ? 'translate-y-0 opacity-100' : '-translate-y-1 opacity-0',
            )}
          >
            <ExpandedPostCard post={post} />
          </div>
        ) : (
          <div
            aria-hidden={!expanded}
            className="grid transition-[grid-template-rows] duration-[240ms] [transition-timing-function:cubic-bezier(0.23,1,0.32,1)]"
            style={{ gridTemplateRows: expanded ? '1fr' : '0fr' }}
          >
            <div className="min-h-0 overflow-hidden">
              <div className={cn('max-w-[520px] pb-4 pr-6', index != null ? 'pl-[26px]' : 'pl-0')}>
                {post.summary && (
                  <p className="text-[12.5px] font-bold leading-relaxed text-foreground/60 lg:text-[13.5px]">{post.summary}</p>
                )}
                {post.hook && (
                  <p className="mt-2 text-[12px] font-black leading-snug tracking-tight text-[#E11D48]/85">“{post.hook}”</p>
                )}
                {post.mechanic && (
                  <p className="mt-2 text-[10.5px] font-black uppercase leading-relaxed tracking-[0.06em] text-foreground/38">{post.mechanic}</p>
                )}
              </div>
            </div>
          </div>
        )
      )}
    </motion.div>
  );
}

/* Tenure ticks — one dot per run the reading has been alive. */
function RunTicks({ ticks, className }: { ticks: DropBite['ticks']; className?: string }) {
  return (
    <span className={cn('inline-flex items-center gap-2', className)}>
      <span className="inline-flex items-center gap-[5px]">
        {ticks.map((tick, i) => (
          <span
            key={tick.run}
            className={cn('h-[7px] w-[7px] rounded-full', i === ticks.length - 1 ? 'bg-[#E11D48]' : 'bg-foreground/22')}
          />
        ))}
      </span>
      <span className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/35">
        {ticks.length === 1 ? 'first run on record' : `alive ${ticks.length} runs`}
      </span>
    </span>
  );
}

/* ── THE ARTIFACT KIT ──────────────────────────────────────────
   Small deterministic performances, Feed Me styled and labelled,
   computed from the run's real posts — never invented.

   span     — where a Bite's proof lands in the 90-day memory
   lanes    — what the account posts (media lane split)
   cadence  — how often it posts (last six weeks)                */

/* THE POST CARD LANGUAGE — the artifact IS the post. A thumbnail card
   (real media once the 90-day thumbnail store is wired; a quiet filmic
   placeholder until then), the title, the one-line WORK summary, and the
   overall rank rolling up. No abstract geometry: the data comes to life
   as the content it describes. Motion is weighty and eased — no snaps. */

function PostThumbCard({ post, size = 'md', accent, className }: { post: DropPost; size?: 'sm' | 'md' | 'lg'; accent?: boolean; className?: string }) {
  const [imageFailed, setImageFailed] = useState(false);
  const width = size === 'lg' ? 'w-[clamp(88px,22vw,116px)]' : size === 'md' ? 'w-[clamp(72px,18vw,96px)]' : 'w-[clamp(56px,14vw,74px)]';
  const frameClass = cn(
    'relative block shrink-0 overflow-hidden rounded-[14px] border',
    accent ? 'border-[#E11D48]/50 shadow-[0_18px_44px_-18px_rgba(225,29,72,0.55)]' : 'border-foreground/12 shadow-[0_16px_36px_-20px_rgba(15,23,42,0.5)]',
    post.url && 'transition-transform duration-300 hover:-translate-y-0.5 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]/60 focus-visible:ring-offset-2 focus-visible:ring-offset-background',
    width,
    className,
  );
  const content = (
    <>
      {post.thumbnail && !imageFailed ? (
        // eslint-disable-next-line @next/next/no-img-element
        <img
          src={post.thumbnail}
          alt={post.title}
          className="h-full w-full object-cover"
          onError={() => setImageFailed(true)}
        />
      ) : (
        <span aria-hidden className="absolute inset-0" style={{ background: 'linear-gradient(160deg, rgba(127,127,127,0.14), rgba(127,127,127,0.05) 55%, rgba(225,29,72,0.08))' }}>
          <span className="absolute inset-0 flex items-center justify-center">
            <span className={cn('block h-8 w-8 rounded-full border', accent ? 'border-[#E11D48]/45' : 'border-foreground/22')} />
            <span className={cn('absolute ml-[2px] border-y-[6px] border-l-[9px] border-y-transparent', accent ? 'border-l-[#E11D48]/70' : 'border-l-foreground/35')} />
          </span>
        </span>
      )}
      <span aria-hidden className="absolute inset-x-0 top-0 h-px bg-white/25" />
      {post.url && (
        <span aria-hidden className="absolute right-1.5 top-1.5 grid h-5 w-5 place-items-center rounded-full border border-white/25 bg-black/55 text-[9px] font-black text-white backdrop-blur-sm">
          ↗
        </span>
      )}
    </>
  );
  return post.url ? (
    <a
      href={post.url}
      target="_blank"
      rel="noopener noreferrer"
      aria-label={`Open ${post.title} on Instagram`}
      className={frameClass}
      style={{ aspectRatio: '9 / 16' }}
    >
      {content}
    </a>
  ) : (
    <span className={frameClass} style={{ aspectRatio: '9 / 16' }}>
      {content}
    </span>
  );
}

function BiteEvidencePreview({ posts }: { posts: DropPost[] }) {
  const cited = posts.slice(0, 2);
  if (cited.length === 0) return null;
  return (
    <div className="mt-5">
      <Eyebrow>cited in this read</Eyebrow>
      <div className="mt-3 grid grid-cols-2 gap-2.5">
        {cited.map((post) => (
          <div key={post.title} className="flex min-w-0 items-center gap-2.5 border-t border-foreground/10 pt-2.5">
            <PostThumbCard post={post} size="sm" className="!w-[38px] !rounded-[9px] sm:!w-[42px]" />
            <span className="min-w-0 flex-1">
              <PostSourceLabel post={post} className="w-full text-[10.5px] font-black leading-[1.12] tracking-tight text-foreground/64" />
              <span className="mt-1 block text-[7.5px] font-black uppercase tracking-[0.12em] text-foreground/30">
                {post.url ? 'open source' : 'evidence post'}
              </span>
            </span>
          </div>
        ))}
      </div>
    </div>
  );
}

/* One post, in full: thumb, title, what it is, where it landed. */
function FeaturedPostCard({
  post,
  label = 'the post to know',
  compact = false,
  contracted = false,
  className,
}: {
  post: DropPost;
  label?: string;
  compact?: boolean;
  contracted?: boolean;
  className?: string;
}) {
  const overall = parseOverall(post.overallRank);
  const tight = compact || contracted;
  return (
    <motion.div variants={compact ? undefined : LINE} className={cn(tight && 'h-full overflow-hidden', className)}>
      <Eyebrow accent>{label}</Eyebrow>
      <div className={cn('mt-4 flex items-stretch gap-4 overflow-hidden lg:gap-5', compact && 'h-[146px]', contracted && 'h-[106px] lg:h-[116px]')}>
        <PostThumbCard
          post={post}
          size={tight ? 'md' : 'lg'}
          accent
          className={cn(compact && '!w-[72px]', contracted && '!w-[58px] !rounded-[11px] lg:!w-[64px]')}
        />
        <div className="flex min-w-0 flex-1 flex-col overflow-hidden py-0.5">
          <PostSourceLabel
            post={post}
            className={cn(
              'w-full font-black leading-[1.02] tracking-[-0.02em] text-foreground',
              contracted ? 'text-[15px] lg:text-[17px]' : 'text-[17px] lg:text-[21px]',
            )}
          />
          {post.summary && (
            <p
              className={cn('mt-2 text-[12px] font-bold leading-relaxed text-foreground/55 lg:text-[13.5px]', compact && 'line-clamp-3', contracted && 'line-clamp-2 !text-[11.5px] !leading-[1.35] lg:!text-[12px]')}
              style={tight ? undefined : { display: '-webkit-box', WebkitLineClamp: 3, WebkitBoxOrient: 'vertical', overflow: 'hidden' }}
            >
              {post.summary}
            </p>
          )}
          {post.hook && (
            <p className={cn('mt-1.5 truncate text-[11.5px] font-black leading-snug tracking-tight text-[#E11D48]/85', contracted && 'hidden lg:block')}>
              “{post.hook}”
            </p>
          )}
          {overall && (
            <span className={cn('mt-auto inline-flex shrink-0 items-baseline pt-2 font-black leading-none', contracted && 'pt-1.5')}>
              <span className="mr-1.5 text-[9px] uppercase tracking-[0.16em] text-foreground/38">landed</span>
              <Odometer
                value={overall.rank}
                animateOnMount={!contracted}
                className={cn('inline-flex justify-end tabular-nums text-[#E11D48]', contracted ? 'text-[22px] lg:text-[26px]' : 'text-[30px] lg:text-[36px]')}
              />
              <span className="ml-1 text-[13px] tabular-nums text-foreground/35">/{overall.pool}</span>
            </span>
          )}
        </div>
      </div>
    </motion.div>
  );
}

function EvidenceRailThumb({ post, active }: { post: DropPost; active: boolean }) {
  const [imageFailed, setImageFailed] = useState(false);

  return (
    <span
      aria-hidden
      className={cn(
        'relative block h-[58px] w-[40px] shrink-0 overflow-hidden rounded-[8px] border bg-foreground/[0.035]',
        active ? 'border-[#E11D48]/55' : 'border-foreground/10',
      )}
    >
      {post.thumbnail && !imageFailed ? (
        // eslint-disable-next-line @next/next/no-img-element
        <img
          src={post.thumbnail}
          alt=""
          className="h-full w-full object-cover"
          onError={() => setImageFailed(true)}
        />
      ) : (
        <span
          className="absolute inset-0"
          style={{ background: 'linear-gradient(155deg, rgba(127,127,127,0.13), rgba(127,127,127,0.04) 58%, rgba(225,29,72,0.1))' }}
        />
      )}
    </span>
  );
}

/* Mobile proof navigation. The featured post remains the stable reading
   surface; every receipt stays reachable in a native, snap-scrolling rail. */
function EvidenceRail({
  posts,
  selectedTitle,
  boundaryTitles,
  onSelect,
}: {
  posts: DropPost[];
  selectedTitle: string;
  boundaryTitles: Set<string>;
  onSelect: (title: string) => void;
}) {
  return (
    <div
      data-reader-native-scroll
      className="absolute -left-6 -right-6 top-[208px] z-20 h-[78px] touch-pan-x overflow-x-auto overflow-y-hidden px-6 pb-2 [scrollbar-width:none] [-webkit-overflow-scrolling:touch] [&::-webkit-scrollbar]:hidden"
    >
      <div role="list" aria-label="Evidence posts" className="flex w-max snap-x snap-mandatory gap-3">
        {posts.map((post, i) => {
          const active = post.title === selectedTitle;
          const overall = parseOverall(post.overallRank);
          const boundary = boundaryTitles.has(post.title);

          return (
            <div key={post.title} role="listitem" className="shrink-0 snap-start">
              <button
                type="button"
                aria-pressed={active}
                aria-label={`Show ${post.title} as the main evidence`}
                onClick={() => onSelect(post.title)}
                className={cn(
                  'relative flex w-[132px] items-start gap-2 border-t pt-2 text-left transition-transform duration-150 active:scale-[0.97] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]/55 focus-visible:ring-offset-4 focus-visible:ring-offset-background motion-reduce:transition-none',
                  active ? 'border-[#E11D48]/65' : 'border-foreground/12',
                )}
              >
                {active && <span aria-hidden className="absolute inset-x-0 -top-px h-[2px] bg-[#E11D48]" />}
                <EvidenceRailThumb post={post} active={active} />
                <span className="min-w-0 flex-1 pt-0.5">
                  <span className={cn('block text-[8px] font-black uppercase tracking-[0.14em]', active ? 'text-[#E11D48]' : 'text-foreground/30')}>
                    {boundary ? 'boundary' : num(i + 1)}
                  </span>
                  <span className={cn('mt-1 line-clamp-2 block text-[10.5px] font-black leading-[1.08] tracking-tight', active ? 'text-foreground' : 'text-foreground/58')}>
                    {post.title}
                  </span>
                  {overall && (
                    <span className="mt-1.5 block text-[9px] font-black tabular-nums text-foreground/32">
                      {overall.rank}/{overall.pool}
                    </span>
                  )}
                </span>
              </button>
            </div>
          );
        })}
      </div>
    </div>
  );
}

/* THE FILMSTRIP — the run's posts as a cascade of thumb cards, strongest
   landing leading with the accent. Each card settles in slowly with a
   slight rotation, like prints laid on a table. */
function Filmstrip({ posts, label }: { posts: DropPost[]; label: string }) {
  const reduce = Boolean(useReducedMotion());
  const strip = posts.slice(0, 6);
  return (
    <motion.div variants={LINE}>
      <Eyebrow>{label}</Eyebrow>
      <div className="mt-5 flex items-end">
        {strip.map((post, i) => {
          const overall = parseOverall(post.overallRank);
          return (
            <motion.div
              key={post.title}
              className={cn('relative', i > 0 && '-ml-3 lg:-ml-4')}
              style={{ zIndex: strip.length - i }}
              initial={reduce ? false : { opacity: 0, transform: `translate3d(0, 26px, 0) rotate(${i % 2 === 0 ? -4 : 3}deg)` }}
              animate={{ opacity: 1, transform: `translate3d(0, 0, 0) rotate(${(i % 2 === 0 ? -1 : 1) * (i === 0 ? 0 : 1.6)}deg)` }}
              transition={reduce ? { duration: 0 } : { delay: 0.55 + i * 0.14, duration: 0.7, ease: VISIT_EASE }}
            >
              <PostThumbCard post={post} size={i === 0 ? 'md' : 'sm'} accent={i === 0} />
              {overall && (
                <span
                  className={cn(
                    'absolute -bottom-2 left-1/2 -translate-x-1/2 whitespace-nowrap rounded-full border px-1.5 py-0.5 text-[9px] font-black tabular-nums leading-none backdrop-blur-sm',
                    i === 0 ? 'border-[#E11D48]/45 bg-background/85 text-[#E11D48]' : 'border-foreground/15 bg-background/85 text-foreground/55',
                  )}
                >
                  {overall.rank}
                </span>
              )}
            </motion.div>
          );
        })}
      </div>
    </motion.div>
  );
}

/* THE LANES — a dedicated head-to-head of the account's lanes: how much
   it posts in each, how each lands on average, and each lane's strongest
   post as a card. Server-computed; renders only when there are two or
   more lanes to compare (an all-reels account has no race to stage). */
function LanesBeat({ drop, page = 'all' }: { drop: ReaderDropModel; page?: 'all' | 'first' | 'rest' }) {
  const reduce = Boolean(useReducedMotion());
  const allLanes = drop.stats.lanes.filter((lane) => lane.avgTopPct != null);
  const lanes = page === 'first' ? allLanes.slice(0, 2) : page === 'rest' ? allLanes.slice(2) : allLanes;
  const total = drop.stats.lanes.reduce((sum, lane) => sum + lane.count, 0);
  const continued = page === 'rest';
  return (
    <Beat eyebrow={continued ? 'the lanes · remaining formats' : 'the lanes · which format feeds'} accent>
      <motion.h2 variants={LINE} className="mt-4 max-w-[14ch] text-[clamp(34px,8.4vw,58px)] font-black leading-[0.9] tracking-[-0.05em] text-foreground lg:text-[68px]">
        {continued ? (
          <>The rest of<br />the mix.</>
        ) : (
          <>Not every lane<br />eats the same.</>
        )}
      </motion.h2>
      <div className="mt-7 lg:mt-10">
        {lanes.map((lane, i) => (
          <motion.div
            key={lane.label}
            variants={LINE}
            className="grid grid-cols-[minmax(0,1fr)_auto] items-center gap-x-5 border-t border-foreground/10 py-4 lg:grid-cols-[130px_minmax(0,1fr)_auto] lg:gap-x-8 lg:py-5"
          >
            <span className="min-w-0 lg:contents">
              <span className="flex items-baseline gap-2.5">
                <span className="text-[clamp(19px,4.6vw,26px)] font-black leading-none tracking-[-0.02em] text-foreground lg:text-[30px]">{lane.label}</span>
                <span className="text-[10px] font-black uppercase tracking-[0.12em] text-foreground/35">{lane.count} of {total}</span>
              </span>
              <span className="mt-3 block lg:mt-0">
                <span className="relative block h-[9px] max-w-[380px] overflow-hidden rounded-full bg-foreground/[0.07]">
                  <motion.span
                    className={cn('absolute inset-y-0 left-0 origin-left rounded-full', lane === allLanes[0] ? 'bg-[#E11D48]' : 'bg-foreground/35')}
                    style={{ width: `${100 - (lane.avgTopPct ?? 100)}%`, boxShadow: lane === allLanes[0] ? '0 0 14px rgba(225,29,72,0.4)' : undefined }}
                    initial={reduce ? false : { transform: 'scaleX(0)' }}
                    animate={{ transform: 'scaleX(1)' }}
                    transition={reduce ? { duration: 0 } : { delay: 0.6 + i * 0.16, duration: 0.75, ease: VISIT_EASE }}
                  />
                </span>
                <span className="mt-1.5 block text-[8px] font-black uppercase tracking-[0.14em] text-foreground/35">
                  lands top <span className={lane === allLanes[0] ? 'text-[#E11D48]' : 'text-foreground/60'}>{lane.avgTopPct}%</span> on average
                </span>
              </span>
            </span>
            {lane.best && (
              <span className="flex items-center gap-3">
                <span className="hidden max-w-[190px] text-right sm:block">
                  <span className="block truncate text-[12px] font-black tracking-tight text-foreground/70">{lane.best.title}</span>
                  <span className="mt-0.5 block text-[8px] font-black uppercase tracking-[0.12em] text-foreground/35">lane best · {lane.best.overallRank}</span>
                </span>
                <PostThumbCard post={lane.best} size="sm" accent={lane === allLanes[0]} />
              </span>
            )}
          </motion.div>
        ))}
      </div>
    </Beat>
  );
}

/* THE CADENCE — posts per week, last six weeks. Columns grow on entry. */
function CadenceCols({ stats, showLabel = true }: { stats: DropStats; showLabel?: boolean }) {
  const reduce = Boolean(useReducedMotion());
  const max = Math.max(...stats.cadence.map((week) => week.count));
  if (!max) return null;
  return (
    <motion.div variants={LINE}>
      {showLabel && <Eyebrow>how often it posts · by week</Eyebrow>}
      <div className={cn('grid h-[86px] grid-cols-6 items-end gap-2', showLabel && 'mt-4')}>
        {stats.cadence.map((week, i) => {
          const now = week.weeksAgo === 0;
          return (
            <div key={week.weeksAgo} className="relative flex h-full flex-col items-center justify-end">
              {week.count > 0 && (
                <span className={cn('mb-1 text-[11px] font-black tabular-nums leading-none', now ? 'text-[#E11D48]' : 'text-foreground/40')}>
                  {week.count}
                </span>
              )}
              <motion.span
                className={cn('w-full origin-bottom rounded-t-[5px]', now ? 'bg-[#E11D48]' : 'bg-foreground/18')}
                style={{ height: `${Math.max(6, (week.count / max) * 72)}%` }}
                initial={reduce ? false : { transform: 'scaleY(0)' }}
                animate={{ transform: 'scaleY(1)' }}
                transition={reduce ? { duration: 0 } : { delay: 0.55 + i * 0.07, duration: 0.45, ease: VISIT_EASE }}
              />
            </div>
          );
        })}
      </div>
      <div className="mt-1.5 flex justify-between text-[8px] font-black uppercase tracking-[0.14em] text-foreground/30">
        <span>earlier</span>
        <span className="text-[#E11D48]/80">latest posts</span>
      </div>
    </motion.div>
  );
}

/* ── beat 1 · cover ─────────────────────────────────────────── */

function CoverBeat({ drop }: { drop: ReaderDropModel }) {
  return (
    <section className="relative flex min-h-[100dvh] w-screen max-w-[100vw] items-end overflow-hidden">
      {/* identity wedge, cropped top-right — fixed aperture, never a gauge */}
      <span
        aria-hidden
        className="absolute rounded-full opacity-90"
        style={{
          width: 'clamp(240px, 52vw, 560px)',
          height: 'clamp(240px, 52vw, 560px)',
          right: 'calc(-0.26 * clamp(240px, 52vw, 560px))',
          top: 'calc(-0.22 * clamp(240px, 52vw, 560px))',
          background: wedgeGradient(64),
          rotate: '180deg',
        }}
      />
      <motion.div className={WRAP} variants={CONTAINER} initial="hidden" animate="visible">
        <motion.div variants={LINE}>
          <Eyebrow accent>Feeder Reader · run {num(drop.run)}</Eyebrow>
          <Eyebrow className="mt-1.5">@{drop.handle}</Eyebrow>
        </motion.div>
        <motion.h1
          variants={LINE}
          className="mt-6 max-w-[13ch] text-[clamp(44px,11vw,84px)] font-black leading-[0.9] tracking-[-0.05em] text-foreground lg:text-[104px]"
        >
          {drop.cover.headline.map((line) => (
            <span key={line} className="block">{line}</span>
          ))}
        </motion.h1>
        <motion.p variants={LINE} className="mt-6 max-w-[340px] text-[14px] font-black leading-snug text-foreground/48 lg:max-w-[440px] lg:text-[18px]">
          {drop.cover.tagline}
        </motion.p>
        <motion.div variants={LINE} className="mt-7 flex items-end gap-8 border-t border-foreground/10 pt-5">
          <span>
            <Odometer value={drop.stats.memory} animateOnMount className="block text-[30px] font-black tabular-nums leading-none text-foreground lg:text-[38px]" />
            <span className="mt-1.5 block text-[8px] font-black uppercase tracking-[0.16em] text-foreground/38">posts in memory</span>
          </span>
          {drop.stats.newPosts != null && drop.stats.newPosts > 0 && (
            <span>
              <span className="flex items-baseline text-[30px] font-black tabular-nums leading-none text-[#E11D48] lg:text-[38px]">
                +<Odometer value={drop.stats.newPosts} animateOnMount className="inline-flex" />
              </span>
              <span className="mt-1.5 block text-[8px] font-black uppercase tracking-[0.16em] text-foreground/38">new this run</span>
            </span>
          )}
          <span>
            <Odometer value={drop.portrait.length} animateOnMount className="block text-[30px] font-black tabular-nums leading-none text-foreground/70 lg:text-[38px]" />
            <span className="mt-1.5 block text-[8px] font-black uppercase tracking-[0.16em] text-foreground/38">readings hold</span>
          </span>
        </motion.div>
      </motion.div>
      <motion.span
        aria-hidden
        className="absolute inset-x-0 text-center text-[10px] font-black uppercase tracking-[0.18em] text-foreground/28"
        style={{ bottom: 'calc(env(safe-area-inset-bottom, 0px) + 76px)' }}
        initial={{ opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={{ delay: 0.9, duration: 0.4, ease: VISIT_EASE }}
      >
        scroll — the account explains itself
      </motion.span>
    </section>
  );
}

/* ── the landing — the server's own read of the run ────────────
   Not a word of this comes from the model: everything below is
   computed from overall ranks in code.

   THE ORBIT — the 90-day memory as concentric quartile rings, rank 1
   at the centre. A radar sweep passes once and the run's posts ignite
   where they landed, placed on a golden-angle spiral so any count of
   posts scatters beautifully. The strongest landing burns brightest.

   THE MOTION LINE — average landing per run, drawn as a single path:
   the account's trajectory in one stroke. */

const ORBIT_C = 170;
const ORBIT_MIN_R = 18;
const ORBIT_MAX_R = 148;
const ORBIT_SWEEP_S = 1.5;
const ORBIT_SWEEP_DELAY = 0.5;

function LandingOrbit({ landing, showLabel = true }: { landing: NonNullable<DropStats['landing']>; showLabel?: boolean }) {
  const reduce = Boolean(useReducedMotion());
  const rings = [
    { pct: 25, label: 'top quarter', count: landing.shape.top, accent: true },
    { pct: 50, label: 'upper mid', count: landing.shape.upper, accent: false },
    { pct: 75, label: 'lower mid', count: landing.shape.lower, accent: false },
    { pct: 100, label: 'the floor', count: landing.shape.floor, accent: false },
  ];
  const dots = landing.posts.flatMap((post, i) => {
    const overall = parseOverall(post.overallRank);
    if (!overall) return [];
    const pct = (overall.rank / overall.pool) * 100;
    const deg = (i * 137.508) % 360; // golden angle — even scatter at any count
    const radius = ORBIT_MIN_R + (pct / 100) * (ORBIT_MAX_R - ORBIT_MIN_R);
    const rad = (deg * Math.PI) / 180;
    return [{
      post,
      rank: overall.rank,
      x: ORBIT_C + radius * Math.sin(rad),
      y: ORBIT_C - radius * Math.cos(rad),
      delay: reduce ? 0 : ORBIT_SWEEP_DELAY + (deg / 360) * ORBIT_SWEEP_S,
      best: i === 0,
    }];
  });
  const bestOverall = parseOverall(landing.best.overallRank);

  return (
    <motion.div variants={LINE}>
      {showLabel && <Eyebrow>the orbit · closer is stronger</Eyebrow>}
      <div className={cn('relative aspect-square w-full max-w-[352px]', showLabel && 'mt-5')}>
        {/* the radar sweep — one pass, igniting dots as it crosses them */}
        {!reduce && (
          <motion.span
            aria-hidden
            className="absolute inset-0 rounded-full"
            style={{ background: 'conic-gradient(from 0deg, rgba(225,29,72,0.2), rgba(225,29,72,0.04) 46deg, transparent 60deg)' }}
            initial={{ opacity: 0, rotate: 0 }}
            animate={{ opacity: [0, 1, 1, 0], rotate: 360 }}
            transition={{ delay: ORBIT_SWEEP_DELAY, duration: ORBIT_SWEEP_S, ease: 'linear', times: [0, 0.06, 0.94, 1] }}
          />
        )}
        <svg viewBox="0 0 340 340" className="block h-full w-full overflow-visible">
          {rings.map((ring) => {
            const radius = ORBIT_MIN_R + (ring.pct / 100) * (ORBIT_MAX_R - ORBIT_MIN_R);
            return (
              <g key={ring.pct}>
                <circle cx={ORBIT_C} cy={ORBIT_C} r={radius} fill="none" stroke="rgba(127,127,127,0.22)" strokeWidth="1" strokeDasharray="1 5" strokeLinecap="round" />
                <text x={ORBIT_C} y={ORBIT_C - radius + 11} textAnchor="middle" className="font-black uppercase" style={{ fontSize: 7.5, letterSpacing: '0.1em', fill: 'rgba(127,127,127,0.6)' }}>
                  {ring.label} · <tspan style={{ fill: ring.accent && ring.count > 0 ? '#E11D48' : 'rgba(127,127,127,0.75)' }}>{ring.count}</tspan>
                </text>
              </g>
            );
          })}
          {/* rank 1 — the centre of the memory */}
          <circle cx={ORBIT_C} cy={ORBIT_C} r={3} fill="#E11D48" opacity={0.85} />
          {dots.map((dot) => (
            <motion.g
              key={dot.post.title}
              style={{ transformBox: 'fill-box', transformOrigin: 'center' }}
              initial={reduce ? false : { opacity: 0, scale: 0.2 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={reduce ? { duration: 0 } : { delay: dot.delay, duration: 0.55, ease: VISIT_EASE }}
            >
              {dot.best && <circle cx={dot.x} cy={dot.y} r={13} fill="#E11D48" opacity={0.16} />}
              <circle cx={dot.x} cy={dot.y} r={dot.best ? 7 : 4.5} fill={dot.best ? '#E11D48' : 'rgba(127,127,127,0.6)'} />
              {dot.best && !reduce && (
                <motion.circle
                  cx={dot.x}
                  cy={dot.y}
                  r={7}
                  fill="none"
                  stroke="#E11D48"
                  strokeWidth="1.5"
                  style={{ transformBox: 'fill-box', transformOrigin: 'center' }}
                  initial={{ opacity: 0, scale: 1 }}
                  animate={{ opacity: [0, 0.6, 0], scale: [1, 2.4, 3.1] }}
                  transition={{ delay: dot.delay + 0.4, duration: 0.9, ease: VISIT_EASE }}
                />
              )}
            </motion.g>
          ))}
        </svg>
      </div>
      <div className="mt-3 flex max-w-[352px] items-baseline gap-3">
        <span className="shrink-0 text-[9px] font-black uppercase tracking-[0.16em] text-[#E11D48]">strongest</span>
        <span className="min-w-0 truncate text-[13px] font-black tracking-tight text-foreground lg:text-[15px]">{landing.best.title}</span>
        {bestOverall && (
          <span className="inline-flex shrink-0 items-baseline font-black tabular-nums leading-none text-[#E11D48]">
            <span className="text-[15px]">{bestOverall.rank}</span>
            <span className="ml-0.5 text-[10px] text-foreground/35">/{bestOverall.pool}</span>
          </span>
        )}
      </div>
    </motion.div>
  );
}

function TrendLine({ trend }: { trend: { run: number; avgTopPct: number }[] }) {
  const reduce = Boolean(useReducedMotion());
  const width = 280;
  const step = trend.length > 1 ? (width - 40) / (trend.length - 1) : 0;
  const points = trend.map((item, i) => ({
    ...item,
    x: 20 + i * step,
    y: 16 + (item.avgTopPct / 100) * 58,
  }));
  const path = points.map((point, i) => `${i === 0 ? 'M' : 'L'} ${point.x} ${point.y}`).join(' ');
  const last = points[points.length - 1];
  const rising = points.length > 1 && last.y < points[points.length - 2].y;

  return (
    <motion.div variants={LINE}>
      <Eyebrow>the motion · average landing, run over run</Eyebrow>
      <svg viewBox="0 0 280 96" className="mt-3 w-full max-w-[340px] overflow-visible">
        <line x1="20" y1="16" x2={width - 20} y2="16" stroke="rgba(127,127,127,0.18)" strokeWidth="1" strokeDasharray="1 5" />
        <motion.path
          d={path}
          fill="none"
          stroke="#E11D48"
          strokeWidth="2.5"
          strokeLinecap="round"
          strokeLinejoin="round"
          initial={reduce ? false : { pathLength: 0 }}
          animate={{ pathLength: 1 }}
          transition={reduce ? { duration: 0 } : { delay: 0.55, duration: 0.9, ease: VISIT_EASE }}
        />
        {points.map((point, i) => {
          const current = i === points.length - 1;
          return (
            <motion.g
              key={point.run}
              style={{ transformBox: 'fill-box', transformOrigin: 'center' }}
              initial={reduce ? false : { opacity: 0, scale: 0.3 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={reduce ? { duration: 0 } : { delay: 0.55 + (i / Math.max(1, points.length - 1)) * 0.9, duration: 0.4, ease: VISIT_EASE }}
            >
              <circle cx={point.x} cy={point.y} r={current ? 6 : 3.5} fill={current ? '#E11D48' : 'rgba(127,127,127,0.5)'} />
              <text x={point.x} y={point.y - 10} textAnchor="middle" className="font-black tabular-nums" style={{ fontSize: current ? 11 : 8.5, fill: current ? '#E11D48' : 'rgba(127,127,127,0.6)' }}>
                {point.avgTopPct}%
              </text>
              <text x={point.x} y={92} textAnchor="middle" className="font-black uppercase" style={{ fontSize: 7, letterSpacing: '0.08em', fill: 'rgba(127,127,127,0.5)' }}>
                run {num(point.run)}
              </text>
            </motion.g>
          );
        })}
      </svg>
      <span className="mt-1 block text-[9px] font-black uppercase tracking-[0.14em] text-foreground/38">
        higher on the chart is closer to the top of the memory{rising ? ' — the account is climbing' : ''}
      </span>
    </motion.div>
  );
}

function LandingBeat({ drop, part = 'both' }: { drop: ReaderDropModel; part?: 'both' | 'summary' | 'orbit' }) {
  const landing = drop.stats.landing!;
  const trend = drop.stats.trend;

  if (part === 'orbit') {
    return (
      <Beat eyebrow="the orbit · closer is stronger" accent>
        <motion.div variants={LINE} className="mt-6">
          <LandingOrbit landing={landing} showLabel={false} />
        </motion.div>
      </Beat>
    );
  }

  return (
    <Beat
      eyebrow="the landing · this run, measured"
      accent
      stage={part === 'both' ? (
        <div className="lg:border-l lg:border-foreground/10 lg:pl-[clamp(28px,3vw,56px)]">
          <LandingOrbit landing={landing} />
          <div className="mt-9 hidden lg:block">
            <Filmstrip posts={landing.posts} label="the run's posts · strongest first" />
          </div>
        </div>
      ) : undefined}
    >
      <motion.h2 variants={LINE} className="mt-5 max-w-[13ch] text-[clamp(34px,8.6vw,60px)] font-black leading-[0.9] tracking-[-0.05em] text-foreground lg:text-[72px]">
        Where the week
        <br />
        landed.
      </motion.h2>
      <motion.div variants={LINE} className="mt-5 lg:mt-8">
        <span className="inline-flex items-baseline font-black leading-none">
          <span className="text-[clamp(38px,9.6vw,64px)] tracking-[-0.05em] text-foreground">Top&nbsp;</span>
          <Odometer value={landing.avgTopPct} animateOnMount className="inline-flex text-[clamp(38px,9.6vw,64px)] tabular-nums tracking-[-0.05em] text-[#E11D48]" />
          <span className="text-[clamp(38px,9.6vw,64px)] tracking-[-0.05em] text-[#E11D48]">%</span>
        </span>
        <span className="mt-2 block text-[9px] font-black uppercase tracking-[0.16em] text-foreground/38">
          average landing · this run&apos;s posts in the 90-day memory
        </span>
      </motion.div>
      {trend.length >= 2 && (
        <div className="mt-6 lg:mt-9">
          <TrendLine trend={trend} />
        </div>
      )}
    </Beat>
  );
}

/* ── beat 2 · the callback — last run's watch, settled ─────── */

function CallbackBeat({ drop }: { drop: ReaderDropModel }) {
  const callback = drop.callback!;
  return (
    <Beat
      eyebrow={<>on the record · placed run {num(callback.placedRun)}</>}
      stage={
        <motion.div variants={LINE} className="lg:border-l lg:border-foreground/10 lg:pl-[clamp(28px,3vw,56px)]">
          <Eyebrow accent>this run answered</Eyebrow>
          <p className="mt-4 max-w-[460px] text-[16px] font-black leading-snug tracking-tight text-foreground/80 lg:text-[21px]">
            {callback.answer}
          </p>
          <HunchRibbon
            fromLabel={`run ${num(callback.placedRun)} · placed`}
            toLabel={`run ${num(drop.run)} · settled`}
            settled
            className="mt-7"
          />
        </motion.div>
      }
    >
      <motion.p variants={LINE} className="mt-3 text-[11px] font-black uppercase tracking-[0.16em] text-foreground/35">
        we were watching this
      </motion.p>
      <motion.blockquote
        variants={LINE}
        className="mt-4 max-w-[560px] border-l-2 border-foreground/15 pl-5 text-[20px] font-black leading-[1.15] tracking-[-0.02em] text-foreground/62 lg:pl-7 lg:text-[30px]"
      >
        {callback.watch}
      </motion.blockquote>
    </Beat>
  );
}

/* ── beat 3 · the portrait — where the account stands ───────── */

function PortraitBeat({ drop }: { drop: ReaderDropModel }) {
  return (
    <Beat eyebrow={<>the portrait · {drop.portrait.length} readings hold</>} accent>
      <motion.h2 variants={LINE} className="mt-4 text-[clamp(30px,7.4vw,54px)] font-black leading-[0.94] tracking-[-0.045em] text-foreground lg:text-[62px]">
        Right now, this account
        <br />
        makes sense like this.
      </motion.h2>
      <div className="mt-7 lg:mt-10">
        {drop.portrait.map((item, i) => (
          <motion.div
            key={item.id}
            variants={LINE}
            className="flex items-baseline gap-4 border-t border-foreground/10 py-4 lg:gap-6 lg:py-6"
          >
            <span className="shrink-0 text-[14px] font-black tabular-nums text-[#E11D48] lg:text-[18px]">{num(i + 1)}</span>
            <span className="min-w-0 flex-1">
              <span className="block text-[clamp(20px,5vw,32px)] font-black leading-[0.98] tracking-[-0.03em] text-foreground lg:text-[38px]">
                {item.title}
              </span>
              <span className="mt-1.5 flex items-baseline gap-3 sm:hidden">
                <span className={cn('text-[9px] font-black uppercase tracking-[0.14em]', item.movement === 'held' ? 'text-foreground/40' : 'text-[#E11D48]')}>
                  {item.movementPhrase}
                </span>
                <span className="text-[9px] font-black uppercase tracking-[0.12em] text-foreground/30">
                  {item.runsActive === 1 ? 'new on record' : `run ${item.runsActive}`}
                </span>
              </span>
            </span>
            <span className="hidden shrink-0 text-right sm:block">
              <span className={cn('block text-[10px] font-black uppercase tracking-[0.14em]', item.movement === 'held' ? 'text-foreground/40' : 'text-[#E11D48]')}>
                {item.movementPhrase}
              </span>
              <span className="mt-1 block text-[9px] font-black uppercase tracking-[0.12em] text-foreground/30">
                {item.runsActive === 1 ? 'new on record' : `run ${item.runsActive} on record`}
              </span>
            </span>
          </motion.div>
        ))}
      </div>
      <motion.p variants={LINE} className="mt-6 max-w-[560px] text-[12.5px] font-black leading-relaxed text-foreground/42 lg:mt-8 lg:text-[14px]">
        {drop.cover.summary}
      </motion.p>
    </Beat>
  );
}

/* ── per Bite · the read ────────────────────────────────────── */

function BiteReadBeat({ bite, count, part = 'both' }: { bite: DropBite; count: number; part?: 'both' | 'title' | 'context' }) {
  const currentReadStage = (
    <motion.div variants={LINE} className={part === 'context' ? undefined : 'lg:border-l lg:border-foreground/10 lg:pl-[clamp(28px,3vw,56px)]'}>
      {part !== 'context' && <Eyebrow>the current read</Eyebrow>}
      <p
        className={cn(
          'font-black tracking-[-0.02em] text-foreground/84',
          part === 'context'
            ? 'mt-6 text-[clamp(21px,5.4vw,30px)] leading-[1.16]'
            : 'mt-3 text-[clamp(16px,4.2vw,24px)] leading-[1.26] lg:mt-4 lg:text-[26px]',
        )}
      >
        {bite.currentRead}
      </p>
      <BiteEvidencePreview posts={bite.evidence} />
    </motion.div>
  );

  if (part === 'context') {
    return (
      <Beat
        eyebrow={
          <span className={bite.movement === 'held' ? undefined : 'text-[#E11D48]'}>
            <span className="block">reading {num(bite.order)} of {num(count)}</span>
            <span className="mt-1 block">the current read</span>
          </span>
        }
      >
        {currentReadStage}
      </Beat>
    );
  }

  return (
    <Beat
      eyebrow={
        <span className={bite.movement === 'held' ? undefined : 'text-[#E11D48]'}>
          reading {num(bite.order)} of {num(count)} · {bite.movementPhrase}
        </span>
      }
      stage={part === 'both' ? currentReadStage : undefined}
    >
      {bite.previousTitle && (
        <motion.p variants={LINE} className="mt-5 text-[13px] font-black tracking-tight text-foreground/38 line-through decoration-foreground/25 decoration-2">
          {bite.previousTitle}
        </motion.p>
      )}
      <motion.h2
        variants={LINE}
        className={cn(
          'max-w-[12ch] text-[clamp(38px,9.6vw,72px)] font-black leading-[0.88] tracking-[-0.055em] text-foreground lg:text-[86px]',
          bite.previousTitle ? 'mt-2' : 'mt-5',
        )}
      >
        {bite.title}
      </motion.h2>
      <motion.div variants={LINE} className="mt-6 lg:mt-8">
        <RunTicks ticks={bite.ticks} />
      </motion.div>
      <motion.p variants={LINE} className="mt-5 max-w-[420px] border-l-2 border-[#E11D48]/60 pl-4 text-[12.5px] font-black leading-relaxed text-foreground/50 lg:mt-6 lg:text-[14px]">
        {bite.whyItMattersNow}
      </motion.p>
    </Beat>
  );
}

/* ── per Bite · the proof — one grammar per movement ───────────

   Left: the verdict and its narrative (flip's then→now, held's calm
   confirmation, widened/entered's changed_because). Stage: the SPAN
   plus the posts themselves — the list stays, the numbers get a home. */

function FlipNarrative({ proof }: { proof: Extract<DropProof, { kind: 'flip' }> }) {
  return (
    <>
      <motion.div variants={LINE} className="mt-6 lg:mt-8">
        <Eyebrow>we used to read it as</Eyebrow>
        <p className="relative mt-2.5 inline-block max-w-[520px] text-[16px] font-black leading-snug tracking-tight text-foreground/40 line-through decoration-[#E11D48]/70 decoration-2 sm:no-underline lg:text-[21px]">
          {proof.oldRead}
          <motion.span
            aria-hidden
            className="absolute left-0 top-1/2 hidden h-[2px] w-full origin-left bg-[#E11D48]/70 sm:block"
            initial={{ transform: 'scaleX(0)' }}
            animate={{ transform: 'scaleX(1)' }}
            transition={{ delay: 0.9, duration: 0.5, ease: VISIT_EASE }}
          />
        </p>
      </motion.div>
      <motion.div variants={LINE} className="mt-5 lg:mt-7">
        <Eyebrow accent>now it reads as</Eyebrow>
        <p className="mt-2.5 max-w-[540px] text-[clamp(19px,4.8vw,28px)] font-black leading-[1.08] tracking-[-0.03em] text-foreground lg:text-[32px]">
          {proof.newRead}
        </p>
      </motion.div>
    </>
  );
}

function HeldNarrative({ bite }: { bite: DropBite }) {
  return (
    <>
      <motion.p variants={LINE} className="mt-6 max-w-[520px] text-[15px] font-black leading-snug tracking-tight text-foreground/70 lg:mt-8 lg:text-[20px]">
        {bite.changedBecause}
      </motion.p>
      <motion.div variants={LINE} className="mt-6 flex max-w-[440px] items-stretch border-y border-foreground/10 lg:mt-8">
        {bite.ticks.map((tick, i) => (
          <div key={tick.run} className={cn('min-w-0 flex-1 py-3', i > 0 && 'border-l border-foreground/10 pl-3.5')}>
            <span className="block text-[8px] font-black uppercase tracking-[0.12em] text-foreground/28">run {num(tick.run)}</span>
            <span className={cn('mt-1 block text-[9.5px] font-black uppercase tracking-[0.06em]', i === bite.ticks.length - 1 ? 'text-foreground/70' : 'text-foreground/40')}>
              {tick.movement}
            </span>
          </div>
        ))}
      </motion.div>
    </>
  );
}

function BiteProofBeat({ bite, part = 'both' }: { bite: DropBite; part?: 'both' | 'narrative' | 'evidence' }) {
  const heads: Record<DropProof['kind'], { eyebrow: string; head: string[] }> = {
    flip: { eyebrow: 'the evidence turned', head: ['The old read', 'broke.'] },
    held: { eyebrow: 'nothing had to move', head: ['Still', 'standing.'] },
    widened: { eyebrow: 'the proof spread', head: ['More posts', 'say so.'] },
    entered: { eyebrow: 'a reading was born', head: ['This one', 'is new.'] },
  };
  const { eyebrow, head } = heads[bite.proof.kind];
  const calm = bite.proof.kind === 'held';
  const [openRow, setOpenRow] = useState<string | null>(null);
  const reduce = Boolean(useReducedMotion());

  /* accent = the posts doing the new work this run */
  const accentTitles = new Set<string>(
    bite.proof.kind === 'flip'
      ? bite.proof.posts.map((post) => post.title)
      : bite.proof.kind === 'widened'
        ? bite.proof.added.map((post) => post.title)
        : [],
  );

  /* the featured post: the strongest landing among the posts doing the
     new work — the one that deserves its card */
  const pctOf = (post: DropPost) => {
    const overall = parseOverall(post.overallRank);
    return overall ? overall.rank / overall.pool : 2;
  };
  const featuredPool = bite.evidence.filter((post) => accentTitles.has(post.title));
  const featured = (featuredPool.length ? featuredPool : bite.evidence).reduce(
    (a, b) => (pctOf(a) <= pctOf(b) ? a : b),
    (featuredPool.length ? featuredPool : bite.evidence)[0],
  );
  const rest = bite.evidence.filter((post) => post.title !== featured?.title);
  const railPosts = [...bite.evidence, ...bite.boundary].filter(
    (post, i, posts) => posts.findIndex((candidate) => candidate.title === post.title) === i,
  );
  const boundaryTitles = new Set(bite.boundary.map((post) => post.title));
  const desktopRows = [
    ...rest.slice(0, bite.boundary.length ? 4 : 5),
    ...bite.boundary.slice(0, 1),
  ];
  const [selectedEvidenceTitle, setSelectedEvidenceTitle] = useState(featured?.title ?? '');
  const selectedEvidence = railPosts.find((post) => post.title === selectedEvidenceTitle) ?? featured;
  const featuredLabel = {
    flip: 'the post that flipped it',
    held: 'still carrying the read',
    widened: 'the new proof to know',
    entered: 'the post to know',
  }[bite.proof.kind];
  const selectedLabel = selectedEvidence && boundaryTitles.has(selectedEvidence.title)
    ? 'where the reading stops'
    : selectedEvidence?.title === featured?.title
      ? featuredLabel
      : 'another post saying so';

  const proofEyebrow = (
    <span className={calm ? undefined : 'text-[#E11D48]'}>
      <span className="block sm:inline">{bite.title}</span>
      <span className="mt-1 block sm:mt-0 sm:inline">
        <span className="hidden sm:inline"> · </span>
        {eyebrow}
      </span>
    </span>
  );

  const evidenceStage = (
    <div className={part === 'evidence' ? 'relative h-[286px]' : 'lg:border-l lg:border-foreground/10 lg:pl-[clamp(28px,3vw,56px)]'}>
      {part === 'evidence' ? (
        <>
          <div aria-live="polite" className="h-[188px] overflow-hidden">
            <AnimatePresence initial={false} mode="popLayout">
              {selectedEvidence && (
                <motion.div
                  key={selectedEvidence.title}
                  initial={reduce ? { opacity: 0 } : { opacity: 0, transform: 'translate3d(0, 10px, 0)' }}
                  animate={{ opacity: 1, transform: 'translate3d(0, 0, 0)' }}
                  exit={reduce ? { opacity: 0 } : { opacity: 0, transform: 'translate3d(0, -6px, 0)' }}
                  transition={{ duration: reduce ? 0.16 : 0.24, ease: VISIT_EASE }}
                >
                  <FeaturedPostCard post={selectedEvidence} label={selectedLabel} compact />
                </motion.div>
              )}
            </AnimatePresence>
          </div>
          {railPosts.length > 1 && (
            <EvidenceRail
              posts={railPosts}
              selectedTitle={selectedEvidence?.title ?? ''}
              boundaryTitles={boundaryTitles}
              onSelect={setSelectedEvidenceTitle}
            />
          )}
        </>
      ) : (
        <div
          className="grid h-[592px] max-w-[560px] overflow-hidden transition-[grid-template-rows] duration-[260ms] [transition-timing-function:cubic-bezier(0.23,1,0.32,1)]"
          style={{ gridTemplateRows: openRow ? '72px 1fr' : '212px 1fr' }}
        >
          <div className="min-h-0 overflow-hidden">
            {selectedEvidence && !openRow && <FeaturedPostCard post={selectedEvidence} label={selectedLabel} />}
          </div>
          <div
            className={cn(
              'grid min-h-0 overflow-hidden transition-[grid-template-rows,padding-top] duration-[260ms] [transition-timing-function:cubic-bezier(0.23,1,0.32,1)]',
              openRow ? 'pt-0' : 'pt-5 lg:pt-7',
            )}
            style={{
              gridTemplateRows: desktopRows.map((post) => (
                openRow === post.title
                  ? '224px'
                  : '54px'
              )).join(' '),
            }}
          >
            {desktopRows.map((post, i) => (
              <PostRow
                key={post.title}
                post={post}
                index={boundaryTitles.has(post.title) ? undefined : i + 1}
                tone={boundaryTitles.has(post.title) ? 'boundary' : accentTitles.has(post.title) ? 'accent' : bite.proof.kind === 'widened' ? 'dim' : 'plain'}
                expanded={openRow === post.title}
                onToggle={() => {
                  setSelectedEvidenceTitle(post.title);
                  setOpenRow((current) => current === post.title ? null : post.title);
                }}
                constrained
              />
            ))}
          </div>
        </div>
      )}
    </div>
  );

  if (part === 'evidence') {
    return (
      <Beat
        eyebrow={
          <span>
            <span className="block sm:inline">{bite.title}</span>
            <span className="mt-1 block sm:mt-0 sm:inline">
              <span className="hidden sm:inline"> · </span>
              evidence on record
            </span>
          </span>
        }
        accent={!calm}
      >
        <div className="mt-7">{evidenceStage}</div>
      </Beat>
    );
  }

  return (
    <Beat
      eyebrow={proofEyebrow}
      stage={part === 'both' ? evidenceStage : undefined}
    >
      <motion.h2
        variants={LINE}
        className={cn(
          'mt-5 text-[clamp(40px,10vw,76px)] font-black leading-[0.86] tracking-[-0.06em] lg:mt-6 lg:text-[92px]',
          calm ? 'text-foreground/85' : 'text-foreground',
        )}
      >
        {head.map((line) => (
          <span key={line} className="block">{line}</span>
        ))}
      </motion.h2>
      {bite.proof.kind === 'flip' ? (
        <FlipNarrative proof={bite.proof} />
      ) : bite.proof.kind === 'held' ? (
        <HeldNarrative bite={bite} />
      ) : (
        <motion.p variants={LINE} className="mt-6 max-w-[520px] text-[15px] font-black leading-snug tracking-tight text-foreground/70 lg:mt-8 lg:text-[20px]">
          {bite.changedBecause}
        </motion.p>
      )}
    </Beat>
  );
}

/* ── what moved this run ────────────────────────────────────── */

function ChangedBeat({ drop, itemIndex }: { drop: ReaderDropModel; itemIndex?: number }) {
  const items = itemIndex == null ? drop.changed : drop.changed.slice(itemIndex, itemIndex + 1);
  return (
    <Beat
      eyebrow={itemIndex == null ? 'meanwhile, this run' : <>meanwhile, this run · {num(itemIndex + 1)} of {num(drop.changed.length)}</>}
      accent
    >
      <motion.h2 variants={LINE} className="mt-4 text-[clamp(34px,8.4vw,58px)] font-black leading-[0.9] tracking-[-0.05em] text-foreground lg:text-[68px]">
        What moved.
      </motion.h2>
      <div className="mt-6 max-w-[760px] lg:mt-10">
        {items.map((item) => (
          <motion.div key={item.movement} variants={LINE} className="border-t border-foreground/10 py-5 lg:py-7">
            <p className="max-w-[620px] text-[clamp(18px,4.6vw,27px)] font-black leading-[1.02] tracking-[-0.03em] text-foreground lg:text-[30px]">
              {item.movement}.
            </p>
            <p className="mt-2.5 max-w-[560px] text-[13px] font-black leading-relaxed text-foreground/55 lg:text-[15px]">{item.detail}</p>
            <div className="mt-3 grid sm:flex sm:flex-wrap sm:gap-x-3 sm:gap-y-1.5">
              {item.posts.map((post) => (
                <span
                  key={post.title}
                  className="flex w-full items-baseline justify-between gap-3 border-t border-foreground/10 py-2.5 text-[12px] font-black tracking-tight text-[#E11D48] sm:inline-flex sm:w-auto sm:justify-start sm:border-0 sm:py-0"
                >
                  <span>{post.title}</span>
                  {parseOverall(post.overallRank) && (
                    <span className="shrink-0 text-[10px] tabular-nums text-foreground/35">{post.overallRank}</span>
                  )}
                </span>
              ))}
            </div>
          </motion.div>
        ))}
      </div>
    </Beat>
  );
}

/* ── the field — how it landed, what it posts, how often ────── */

function FieldBeat({ drop, part = 'both' }: { drop: ReaderDropModel; part?: 'both' | 'signals' | 'cadence' }) {
  const { watch, stats } = drop;

  if (part === 'cadence') {
    return (
      <Beat eyebrow="how often it posts · by week" accent>
        <motion.div variants={LINE} className="mt-8">
          <CadenceCols stats={stats} showLabel={false} />
        </motion.div>
      </Beat>
    );
  }

  return (
    <Beat
      eyebrow="the field · how it landed"
      stage={part === 'both' ? (
        <div className="grid gap-8 lg:border-l lg:border-foreground/10 lg:pl-[clamp(28px,3vw,56px)]">
          <CadenceCols stats={stats} />
        </div>
      ) : undefined}
    >
      {watch.pulse && (
        <motion.p variants={LINE} className="mt-5 max-w-[540px] text-[clamp(19px,4.8vw,30px)] font-black leading-[1.12] tracking-[-0.03em] text-foreground lg:text-[36px]">
          {watch.pulse}
        </motion.p>
      )}
      {watch.signals.length > 0 && (
        <div className="mt-6 max-w-[540px] lg:mt-9">
          <motion.div variants={LINE}>
            <Eyebrow>signals worth a look</Eyebrow>
          </motion.div>
          {watch.signals.slice(0, 3).map((signal) => (
            <motion.div key={signal} variants={LINE} className="flex items-baseline gap-3 border-t border-foreground/10 py-3 first-of-type:mt-3 lg:py-3.5">
              <span aria-hidden className="h-[7px] w-[7px] shrink-0 translate-y-[-1px] rounded-full bg-[#E11D48]/70" />
              <p className="min-w-0 text-[13px] font-black leading-snug text-foreground/68 lg:text-[15px]">{signal}</p>
            </motion.div>
          ))}
        </div>
      )}
    </Beat>
  );
}

/* ── the frame — visible recurrences and tripped triggers ───── */

function FrameBeat({ drop }: { drop: ReaderDropModel }) {
  const { watch } = drop;
  const hasObservations = watch.observations.length > 0;
  const triggerList = (
    <div className="lg:border-l lg:border-foreground/10 lg:pl-[clamp(28px,3vw,56px)]">
      <motion.div variants={LINE}>
        <Eyebrow accent>tripped a wire</Eyebrow>
      </motion.div>
      {watch.triggers.map((trigger) => (
        <motion.div key={trigger.label} variants={LINE} className="mt-4 flex min-w-0 items-baseline gap-3">
          <span className="shrink-0 rounded-full border border-[#E11D48]/35 px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.1em] text-[#E11D48]">
            {trigger.label}
          </span>
          <span className="min-w-0 text-[12.5px] font-black leading-snug text-foreground/55">{trigger.note}</span>
        </motion.div>
      ))}
    </div>
  );

  return (
    <Beat
      eyebrow="in the frame, on repeat"
      stage={hasObservations && watch.triggers.length > 0 ? triggerList : undefined}
    >
      {(hasObservations ? watch.observations : []).map((observation) => (
        <motion.div key={observation.text} variants={LINE} className="mt-5 border-t border-foreground/10 pt-4 first-of-type:mt-6 first-of-type:border-t-0 first-of-type:pt-0 lg:mt-6">
          <p className="max-w-[560px] text-[clamp(17px,4.4vw,24px)] font-black leading-[1.16] tracking-[-0.02em] text-foreground/82 lg:text-[27px]">
            {observation.text}
          </p>
          <div className="mt-2 flex flex-wrap gap-x-3 gap-y-1">
            {observation.posts.slice(0, 4).map((post) => (
              <span key={post.title} className="text-[11px] font-black tracking-tight text-foreground/38">{post.title}</span>
            ))}
          </div>
        </motion.div>
      ))}
      {!hasObservations &&
        watch.triggers.map((trigger) => (
          <motion.div key={trigger.label} variants={LINE} className="mt-5 flex min-w-0 items-baseline gap-3 first-of-type:mt-6">
            <span className="shrink-0 rounded-full border border-[#E11D48]/35 px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.1em] text-[#E11D48]">
              {trigger.label}
            </span>
            <span className="min-w-0 text-[13px] font-black leading-snug text-foreground/55">{trigger.note}</span>
          </motion.div>
        ))}
    </Beat>
  );
}

/* ── next watch — the cliffhanger, stamped ──────────────────── */

function NextWatchBeat({ drop }: { drop: ReaderDropModel }) {
  return (
    <Beat
      eyebrow="next watch · going on record"
      accent
      stage={
        <motion.div variants={LINE} className="lg:border-l lg:border-foreground/10 lg:pl-[clamp(28px,3vw,56px)]">
          <Eyebrow>open until the next run</Eyebrow>
          <HunchRibbon
            fromLabel={`run ${num(drop.run)} · placed now`}
            toLabel="next run · unresolved"
            settled={false}
            className="mt-5"
          />
          <p className="mt-5 max-w-[400px] text-[12px] font-black leading-relaxed text-foreground/38">
            Not a forecast. This is the tension the next posts have to answer — and the first thing the next drop will settle.
          </p>
        </motion.div>
      }
    >
      <motion.p variants={LINE} className="mt-5 max-w-[720px] text-[clamp(24px,6.2vw,44px)] font-black leading-[1.04] tracking-[-0.035em] text-foreground lg:text-[52px]">
        {drop.nextWatch}
      </motion.p>
    </Beat>
  );
}

/* ── sign-off ───────────────────────────────────────────────── */

function SignOffBeat({ drop }: { drop: ReaderDropModel }) {
  return (
    <section className="relative flex min-h-[100dvh] w-screen max-w-[100vw] items-center">
      <motion.div
        className="relative mx-auto box-border w-full max-w-[820px] px-6 pt-12 pb-[calc(env(safe-area-inset-bottom,0px)+104px)] text-center lg:py-20"
        variants={CONTAINER}
        initial="hidden"
        animate="visible"
      >
        <motion.span aria-hidden variants={LINE} className="mx-auto block h-[3px] w-24 bg-[#E11D48]" />
        <motion.div variants={LINE}>
          <Eyebrow className="mt-6">portrait closed · run {num(drop.run)}</Eyebrow>
          <p className="mt-5 text-[clamp(34px,8.4vw,68px)] font-black leading-[0.92] tracking-[-0.055em] text-foreground">
            That’s the account,
            <br />
            as it stands.
          </p>
          <p className="mt-5 text-[10px] font-black uppercase tracking-[0.16em] text-foreground/35">
            @{drop.handle} · {drop.portrait.length} readings hold · watch on record
          </p>
        </motion.div>
        {drop.shelf.length > 0 && (
          <motion.div variants={LINE} className="mx-auto mt-9 max-w-[440px] border-t border-foreground/10 pt-5 text-left lg:mt-12">
            <Eyebrow>previous portraits</Eyebrow>
            {drop.shelf.map((item) => (
              <p key={item.run} className="mt-3 flex items-baseline gap-3">
                <span className="shrink-0 text-[10px] font-black uppercase tracking-[0.14em] text-foreground/30">run {num(item.run)}</span>
                <span className="min-w-0 text-[14px] font-black tracking-tight text-foreground/55">{item.header}</span>
              </p>
            ))}
          </motion.div>
        )}
      </motion.div>
    </section>
  );
}

/* ── deck machinery — one beat on screen, wheel/swipe/keys ───── */

const DECK_VARIANTS = {
  enter: (direction: number) => ({
    opacity: 0,
    transform: `translate3d(0, ${direction > 0 ? '16vh' : '-16vh'}, 0)`,
  }),
  center: {
    opacity: 1,
    transform: 'translate3d(0, 0, 0)',
    transition: {
      opacity: { duration: 0.5, ease: VISIT_EASE },
      transform: { duration: 0.65, ease: VISIT_EASE },
    },
  },
  exit: (direction: number) => ({
    opacity: 0,
    transform: `translate3d(0, ${direction > 0 ? '-12vh' : '12vh'}, 0)`,
    transition: { duration: 0.4, ease: VISIT_EASE },
  }),
} as const;

const WHEEL_COOLDOWN_MS = 760;
const WHEEL_MIN_DELTA = 24;
const COMPACT_DECK_QUERY = '(max-width: 1023px)';

function subscribeCompactDeck(onStoreChange: () => void) {
  if (typeof window === 'undefined') return () => undefined;
  const media = window.matchMedia(COMPACT_DECK_QUERY);
  media.addEventListener('change', onStoreChange);
  return () => media.removeEventListener('change', onStoreChange);
}

function getCompactDeckSnapshot() {
  return typeof window !== 'undefined' && window.matchMedia(COMPACT_DECK_QUERY).matches;
}

function useCompactDeck() {
  return useSyncExternalStore(subscribeCompactDeck, getCompactDeckSnapshot, () => false);
}

function laneLeaderPostsFor(drop: ReaderDropModel): LaneLeaderPost[] {
  const candidates = [
    ...(drop.stats.landing?.posts ?? []),
    ...drop.stats.lanes.flatMap((lane) => lane.best ? [lane.best] : []),
    ...drop.bites.flatMap((bite) => [...bite.evidence, ...bite.boundary]),
  ];
  const seen = new Set<string>();

  return candidates.flatMap((post) => {
    const id = post.url || post.title;
    if (seen.has(id)) return [];
    seen.add(id);
    return [{ id, title: post.title, thumbnail: post.thumbnail, url: post.url }];
  }).slice(0, 12);
}

function buildSteps(drop: ReaderDropModel, artifacts: ReaderDropArtifacts | undefined, compact: boolean): { key: string; node: React.ReactNode }[] {
  const steps: { key: string; node: React.ReactNode }[] = [{ key: 'cover', node: <CoverBeat drop={drop} /> }];
  if (drop.callback) steps.push({ key: 'callback', node: <CallbackBeat drop={drop} /> });
  if (drop.stats.landing) {
    if (compact) {
      steps.push(
        { key: 'landing-summary', node: <LandingBeat drop={drop} part="summary" /> },
        { key: 'landing-orbit', node: <LandingBeat drop={drop} part="orbit" /> },
      );
    } else {
      steps.push({ key: 'landing', node: <LandingBeat drop={drop} /> });
    }
  }
  const rankedLanes = drop.stats.lanes.filter((lane) => lane.avgTopPct != null);
  if (rankedLanes.length >= 2) {
    if (compact && rankedLanes.length > 3) {
      steps.push(
        { key: 'lanes-first', node: <LanesBeat drop={drop} page="first" /> },
        { key: 'lanes-rest', node: <LanesBeat drop={drop} page="rest" /> },
      );
    } else {
      steps.push({ key: 'lanes', node: <LanesBeat drop={drop} /> });
    }
  }
  if (drop.portrait.length > 0) steps.push({ key: 'portrait', node: <PortraitBeat drop={drop} /> });
  drop.bites.forEach((bite) => {
    if (compact) {
      steps.push(
        { key: `${bite.id}-read-title`, node: <BiteReadBeat bite={bite} count={drop.bites.length} part="title" /> },
        { key: `${bite.id}-read-context`, node: <BiteReadBeat bite={bite} count={drop.bites.length} part="context" /> },
        { key: `${bite.id}-proof-narrative`, node: <BiteProofBeat bite={bite} part="narrative" /> },
        { key: `${bite.id}-proof-evidence`, node: <BiteProofBeat bite={bite} part="evidence" /> },
      );
    } else {
      steps.push(
        { key: `${bite.id}-read`, node: <BiteReadBeat bite={bite} count={drop.bites.length} /> },
        { key: `${bite.id}-proof`, node: <BiteProofBeat bite={bite} /> },
      );
    }
  });
  if (drop.changed.length > 0) {
    if (compact && drop.changed.length > 1) {
      drop.changed.forEach((_, itemIndex) => {
        steps.push({ key: `changed-${itemIndex + 1}`, node: <ChangedBeat drop={drop} itemIndex={itemIndex} /> });
      });
    } else {
      steps.push({ key: 'changed', node: <ChangedBeat drop={drop} /> });
    }
  }
  if (drop.watch.pulse || drop.watch.signals.length > 0 || drop.stats.lanes.length > 0) {
    if (compact && (drop.watch.pulse || drop.watch.signals.length > 0) && drop.stats.cadence.some((week) => week.count > 0)) {
      steps.push(
        { key: 'field-signals', node: <FieldBeat drop={drop} part="signals" /> },
        { key: 'field-cadence', node: <FieldBeat drop={drop} part="cadence" /> },
      );
    } else {
      steps.push({ key: 'field', node: <FieldBeat drop={drop} /> });
    }
  }
  if (drop.watch.observations.length > 0 || drop.watch.triggers.length > 0) {
    steps.push({ key: 'frame', node: <FrameBeat drop={drop} /> });
  }
  if (drop.nextWatch) steps.push({ key: 'next-watch', node: <NextWatchBeat drop={drop} /> });
  if (artifacts?.laneLeaders) {
    const posts = laneLeaderPostsFor(drop);
    const contextLabel = `Feeder Reader · run ${num(drop.run)}`;
    steps.push(
      {
        key: 'lane-leaders-landing',
        node: <LaneLeaders contextLabel={contextLabel} embedded mode="landing" posts={posts} />,
      },
      {
        key: 'lane-leaders-volume',
        node: <LaneLeaders contextLabel={contextLabel} embedded mode="volume" posts={posts} />,
      },
    );
  }
  if (artifacts?.followers?.length) {
    steps.push({
      key: 'followers',
      node: (
        <Followers
          contextLabel={`Feeder Reader · run ${num(drop.run)}`}
          embedded
          windows={artifacts.followers}
        />
      ),
    });
  }
  steps.push({ key: 'sign-off', node: <SignOffBeat drop={drop} /> });
  return steps;
}

function DotRail({ count, active, onSelect }: { count: number; active: number; onSelect: (index: number) => void }) {
  return (
    <div
      aria-label="Drop beats"
      className="absolute top-1/2 z-20 hidden -translate-y-1/2 flex-col items-center gap-[10px] sm:flex"
      style={{ right: 'max(14px, env(safe-area-inset-right))' }}
    >
      {Array.from({ length: count }).map((_, i) => (
        <button
          key={i}
          type="button"
          aria-label={`Go to beat ${i + 1}`}
          onClick={() => onSelect(i)}
          className="grid h-[18px] w-[14px] place-items-center"
        >
          <span
            className={cn(
              'block w-[6px] rounded-full transition-all duration-300',
              i === active ? 'h-[18px] bg-[#E11D48]' : 'h-[6px] bg-foreground/30',
            )}
          />
        </button>
      ))}
    </div>
  );
}

export default function ReaderDrop({ drop, artifacts }: { drop: ReaderDropModel; artifacts?: ReaderDropArtifacts }) {
  const reduce = Boolean(useReducedMotion());
  const compact = useCompactDeck();
  const [active, setActive] = useState(0);
  const [direction, setDirection] = useState(1);
  const activeRef = useRef(0);
  const wheelLockRef = useRef(0);
  const gestureRef = useRef<{ x: number; y: number } | null>(null);

  /* Consumers must key <ReaderDrop> by handle+run — a new drop is a new
     deck, and the remount resets the step state. */
  const steps = buildSteps(drop, artifacts, compact);
  const renderedActive = Math.min(active, steps.length - 1);
  const activeStep = steps[renderedActive];
  const sceneKey = activeStep?.key.startsWith('lane-leaders-') ? 'lane-leaders' : activeStep?.key;

  const goTo = useCallback((index: number) => {
    const clamped = Math.max(0, Math.min(steps.length - 1, index));
    if (clamped === activeRef.current) return;
    setDirection(clamped > activeRef.current ? 1 : -1);
    activeRef.current = clamped;
    setActive(clamped);
  }, [steps.length]);

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.defaultPrevented) return;
      const target = event.target as HTMLElement | null;
      if (target?.closest('button, a, input, textarea, select, [role="button"]')) return;
      const forward = event.key === 'ArrowDown' || event.key === 'PageDown' || event.key === ' ';
      const backward = event.key === 'ArrowUp' || event.key === 'PageUp';
      if (!forward && !backward) return;
      event.preventDefault();
      goTo(activeRef.current + (forward ? 1 : -1));
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [goTo]);

  const onWheel = (event: React.WheelEvent<HTMLDivElement>) => {
    if ((event.target as HTMLElement).closest('[data-reader-native-scroll]')) return;
    if (Math.abs(event.deltaY) < WHEEL_MIN_DELTA) return;
    const now = performance.now();
    if (now - wheelLockRef.current < WHEEL_COOLDOWN_MS) return;
    wheelLockRef.current = now;
    goTo(activeRef.current + (event.deltaY > 0 ? 1 : -1));
  };

  const onPointerDown = (event: React.PointerEvent<HTMLDivElement>) => {
    if (event.pointerType === 'mouse' && event.button !== 0) return;
    const target = event.target as HTMLElement;
    if (target.closest('button, a, input, textarea, select, [role="button"], [data-reader-native-scroll]')) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    gestureRef.current = { x: event.clientX, y: event.clientY };
  };

  const onPointerUp = (event: React.PointerEvent<HTMLDivElement>) => {
    const start = gestureRef.current;
    gestureRef.current = null;
    if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId);
    if (!start) return;
    const deltaY = start.y - event.clientY;
    const deltaX = start.x - event.clientX;
    if (Math.abs(deltaY) < 46 || Math.abs(deltaY) < Math.abs(deltaX) * 1.2) return;
    const now = performance.now();
    if (now - wheelLockRef.current < WHEEL_COOLDOWN_MS) return;
    wheelLockRef.current = now;
    goTo(activeRef.current + (deltaY > 0 ? 1 : -1));
  };

  return (
    <div className="relative h-[100dvh] overflow-hidden bg-background text-foreground" style={{ width: '100vw', maxWidth: '100vw' }}>
      <DotRail count={steps.length} active={renderedActive} onSelect={goTo} />
      <div
        className="relative h-[100dvh] w-screen max-w-[100vw] touch-pan-x overflow-hidden"
        onWheel={onWheel}
        onPointerDown={onPointerDown}
        onPointerUp={onPointerUp}
        onPointerCancel={() => { gestureRef.current = null; }}
      >
        <AnimatePresence initial={false} custom={direction} mode="popLayout">
          <motion.div
            key={`${drop.handle}:${drop.run}:${sceneKey}`}
            custom={direction}
            variants={DECK_VARIANTS}
            initial={reduce ? { opacity: 0 } : 'enter'}
            animate={reduce ? { opacity: 1 } : 'center'}
            exit={reduce ? { opacity: 0, transition: { duration: 0.16 } } : 'exit'}
            className="absolute inset-0 flex overflow-hidden"
          >
            <div className="m-auto min-w-0 w-screen max-w-[100vw]">{activeStep?.node}</div>
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}
