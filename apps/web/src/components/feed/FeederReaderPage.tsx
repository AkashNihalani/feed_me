'use client';

/* ─────────────────────────────────────────────────────────────
   FEEDER READER PAGE — the editorial read of one bite run.
   A bite run = the latest 10 posts, read against the last 30 (a 40-post
   memory). One visual language runs the whole page: LANDING (deep hit / held /
   soft / low) drives bar height, bar colour, the composition headline, and
   every evidence post — so the chart and the prose are the same object seen
   twice.

   Top section is a fixed instrument: trajectory on the left, a fixed-height
   metric panel on the right. Click a bar and the panel crossfades IN PLACE to
   that post — the metrics never leave the top and nothing below shifts.

   Below: the run_report lead, the read (what got bit / bite size), the crumbs
   as numbered rows, and the Feed / Fade moves. Thumbnails are slots we fill
   from the 90-day per-post thumbnail store; placeholder until wired.
   ───────────────────────────────────────────────────────────── */

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import Link from 'next/link';
import { motion, AnimatePresence, useReducedMotion, type PanInfo } from 'framer-motion';
import { cn } from '@/lib/utils';
import Odometer from '@/components/login/Odometer';

const ACCENT = '#E11D48';
const SOFT_EASE = [0.16, 0.9, 0.2, 1] as const;
const SLOT_EASE = [0.34, 1.56, 0.64, 1] as const;

type BitePostRef = { id: string; display_tag: string; age?: string; url?: string; thumbnail?: string };
type PerfPost = { id: string; rank: string | number; landing?: string; job?: string; thumbnail?: string };
type FeedFadeItem = { read: string; next: string; posts?: BitePostRef[] };
type Crumb = { label: string; read: string; posts?: BitePostRef[] };

export type BiteRunReport = {
  headline?: string;
  run_report: string;
  crumbs: Crumb[];
  what_got_bit: string;
  bite_size: string;
  performance?: {
    window?: string;
    memory_size?: number;
    drop_shape?: string;
    period_read?: string;
    posts?: PerfPost[];
  };
  feed_fade: { feed: FeedFadeItem[]; fade: FeedFadeItem[] };
};

export type FeederReaderPageProps = {
  onBack?: () => void;
  backLabel?: string;
  feederLabel?: string;
  showHeader?: boolean;
  report?: BiteRunReport | null;
};

/* ── landing: the one language ───────────────────────────────────────────────
   Every "where did it land" signal collapses to a 0–3 tier. Bar height comes
   from the real rank; tier only decides colour + label, so the chart, the
   composition strip, and the evidence chips all read the same. */
type Tier = 0 | 1 | 2 | 3;

function landingTier(landing?: string): Tier {
  const l = (landing || '').toLowerCase();
  if (l.includes('hit') || l.includes('deep') || l.includes('top')) return 3;
  if (l.includes('low')) return 0;
  if (l.includes('soft')) return 1;
  return 2; // held / useful middle
}

const TIER_META: Record<Tier, { label: string; bar: string; dot: string; text: string; faded: string }> = {
  3: { label: 'Deep hit', bar: 'bg-[#F71852]', dot: 'bg-[#F71852]', text: 'text-[#E11D48]', faded: 'bg-[#F71852]/15' },
  2: { label: 'Held', bar: 'bg-foreground/55', dot: 'bg-foreground/55', text: 'text-foreground/70', faded: 'bg-foreground/10' },
  1: { label: 'Soft', bar: 'bg-foreground/28', dot: 'bg-foreground/30', text: 'text-foreground/45', faded: 'bg-foreground/[0.06]' },
  0: { label: 'Low', bar: 'bg-foreground/14', dot: 'bg-foreground/18', text: 'text-foreground/35', faded: 'bg-foreground/[0.04]' },
};

function parseRank(rank: string | number): { value: number; window: number } {
  if (typeof rank === 'number') return { value: rank, window: 40 };
  const [v, w] = String(rank).split('/');
  return { value: Math.max(1, Math.round(Number(v) || 50)), window: Math.max(2, Math.round(Number(w) || 40)) };
}

// Taller = better (lower rank). Full range so a 4/40 towers over a 39/40 — no
// baseline compression. A small floor keeps the worst post a visible stub.
function barHeight(rank: number, window: number) {
  return 0.08 + 0.92 * ((window - rank) / (window - 1));
}

// "5 top, 2 held, 2 soft, 1 low" → ordered tier counts, highest tier first.
function parseDropShape(shape?: string): { tier: Tier; label: string; count: number }[] {
  const labels: Record<string, Tier> = { top: 3, held: 2, soft: 1, low: 0 };
  const found: Record<Tier, number> = { 0: 0, 1: 0, 2: 0, 3: 0 };
  (shape || '').split(',').forEach((part) => {
    const m = part.trim().match(/(\d+)\s+(\w+)/);
    if (!m) return;
    const tier = labels[m[2].toLowerCase()];
    if (tier != null) found[tier] += Number(m[1]);
  });
  return ([3, 2, 1, 0] as Tier[]).map((tier) => ({ tier, label: TIER_META[tier].label, count: found[tier] })).filter((s) => s.count > 0);
}

/* A real report renders as one run. Historical navigation can be populated once
   the API supplies verified prior reports; the frontend does not invent them. */
type RunView = { key: string; deckLabel: string; span: string; live: boolean; report: BiteRunReport };

function buildRuns(report: BiteRunReport): RunView[] {
  return [{ key: 'latest', deckLabel: 'latest run', span: 'Last 10 posts · this week', live: false, report }];
}

function Eyebrow({ children, accent = false, className }: { children: React.ReactNode; accent?: boolean; className?: string }) {
  return <span className={cn('text-[10px] font-black uppercase tracking-[0.22em]', accent ? 'text-[#E11D48]' : 'text-foreground/40', className)}>{children}</span>;
}

/* ── post thumbnail slot — filled from the 90-day per-post store; graceful
   placeholder until wired. Never a fabricated metric tile. */
// fill = size to the parent's height (used in the fixed-height instrument panel so
// selecting a post can never grow the section). Otherwise width-based.
function PostThumb({ post, fill }: { post: { display_tag?: string; thumbnail?: string }; fill?: boolean }) {
  return (
    <div className={cn('relative shrink-0 overflow-hidden rounded-[16px] border border-foreground/12 bg-foreground/[0.05]', fill ? 'h-full w-auto' : 'w-[120px]')} style={{ aspectRatio: '9 / 16' }}>
      {post.thumbnail ? (
        // eslint-disable-next-line @next/next/no-img-element
        <img src={post.thumbnail} alt={post.display_tag || ''} className="h-full w-full object-cover" />
      ) : (
        <span aria-hidden className="absolute inset-0 flex items-center justify-center">
          <span className="block h-9 w-9 rounded-full border border-foreground/20" />
          <span className="absolute ml-[2px] border-y-[7px] border-l-[11px] border-y-transparent border-l-foreground/35" />
        </span>
      )}
      <span aria-hidden className="absolute inset-x-0 top-0 h-px bg-white/25" />
    </div>
  );
}

/* ── mentions — wherever the read pins posts (crumbs, feed, fade) we name them
   inline instead of repeating thumbnails. A run post jumps to the top instrument
   and selects itself (the one rich view lives there); a memory post opens out.
   Text pills wrap, so any number of pinned posts is fine. */
function Mentions({ posts, runIndexById, onMention, className }: { posts?: BitePostRef[]; runIndexById: Map<string, number>; onMention: (i: number, el: HTMLElement) => void; className?: string }) {
  if (!posts || posts.length === 0) return null;
  return (
    <div className={cn('flex flex-wrap items-center gap-x-2.5 gap-y-2', className ?? 'mt-3.5')}>
      <span className="text-[9px] font-black uppercase tracking-[0.18em] text-foreground/30">pinned</span>
      {posts.map((post) => {
        const idx = runIndexById.get(post.id);
        const inRun = idx != null;
        const base = 'group inline-flex items-center gap-1 text-[12.5px] font-black leading-tight tracking-tight no-underline transition-colors';
        const name = <span className="underline decoration-1 underline-offset-[3px] decoration-foreground/20 group-hover:decoration-[#E11D48]">{post.display_tag}</span>;
        if (inRun) {
          return (
            <button key={`${post.id}:${post.display_tag}`} type="button" onClick={(e) => onMention(idx!, e.currentTarget)} className={cn(base, 'text-foreground/72 hover:text-[#E11D48]')}>
              {name}<span aria-hidden className="text-[10px] text-foreground/35 group-hover:text-[#E11D48]">↑</span>
            </button>
          );
        }
        if (post.url) {
          return (
            <a key={`${post.id}:${post.display_tag}`} href={post.url} target="_blank" rel="noreferrer" className={cn(base, 'text-foreground/55 hover:text-[#E11D48]')}>
              {name}<span aria-hidden className="text-[10px] text-foreground/35 group-hover:text-[#E11D48]">↗</span>
            </a>
          );
        }
        return <span key={`${post.id}:${post.display_tag}`} className={cn(base, 'text-foreground/55')}>{name}</span>;
      })}
    </div>
  );
}

/* ── the instrument: trajectory + a fixed-height metric panel, side by side.
   Selecting a bar swaps the panel in place. Nothing below ever moves. */
function Bars({ posts, selected, onSelect, reduce, className }: { posts: PerfPost[]; selected: number | null; onSelect: (i: number) => void; reduce: boolean; className?: string }) {
  const window = parseRank(posts[0]?.rank ?? '20/40').window;
  const midHeight = barHeight(window / 2, window);
  const ranks = posts.map((p) => parseRank(p.rank).value);
  const bestIndex = ranks.indexOf(Math.min(...ranks));

  return (
    <div className={cn('relative h-[clamp(176px,30vw,236px)] w-full', className)}>
      <div className="pointer-events-none absolute inset-x-0 -top-0.5 flex justify-between text-[8px] font-black uppercase tracking-[0.16em] text-foreground/25">
        <span>best · rank 1</span>
        <span>memory · {window}</span>
      </div>
      <span aria-hidden className="pointer-events-none absolute inset-x-0 z-10 h-px bg-foreground/15" style={{ bottom: `${midHeight * 100}%` }} />
      <span className="pointer-events-none absolute right-0 z-10 -translate-y-1/2 text-[7px] font-black uppercase tracking-[0.12em] text-foreground/30" style={{ bottom: `${midHeight * 100}%` }}>top half</span>

      <div className="absolute inset-x-0 bottom-0 top-4 grid grid-cols-10 items-end gap-[5px] sm:gap-2">
        {posts.map((post, i) => {
          const { value, window: w } = parseRank(post.rank);
          const tier = landingTier(post.landing);
          const meta = TIER_META[tier];
          const h = barHeight(value, w);
          const active = selected === i;
          const best = i === bestIndex;
          return (
            <button
              key={post.id}
              type="button"
              onClick={() => onSelect(i)}
              aria-label={`Post ${i + 1}, rank ${value} of ${w}, ${meta.label}`}
              className="group relative flex h-full min-w-0 items-end justify-center rounded-[10px] outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]/40"
            >
              <span aria-hidden className="absolute inset-x-0 bottom-0 top-0 rounded-[10px] bg-foreground/[0.04]" />
              <motion.span
                aria-hidden
                className={cn('absolute inset-x-[14%] bottom-0 origin-bottom rounded-t-[7px]', meta.bar, (best || tier === 3) && 'shadow-[0_6px_22px_rgba(225,29,72,0.28)]')}
                style={{ height: '100%' }}
                initial={reduce ? false : { scaleY: 0 }}
                animate={{ scaleY: h, opacity: active || selected == null ? 1 : 0.5 }}
                transition={reduce ? { duration: 0 } : { scaleY: { delay: 0.038 + i * 0.026, duration: 0.41, ease: SOFT_EASE }, opacity: { duration: 0.16 } }}
              />
              <span
                className={cn(
                  'pointer-events-none absolute left-1/2 z-20 -translate-x-1/2 text-[8px] font-black uppercase tabular-nums tracking-[0.04em] transition-opacity',
                  active || best ? meta.text : 'text-transparent group-hover:text-foreground/45',
                )}
                style={{ bottom: `${Math.min(94, h * 100 + 3)}%` }}
              >
                {value}
              </span>
            </button>
          );
        })}
      </div>
    </div>
  );
}

function CompositionStrip({ segments }: { segments: { tier: Tier; label: string; count: number }[] }) {
  const total = segments.reduce((s, x) => s + x.count, 0) || 1;
  return (
    <div>
      <div className="flex h-2 w-full overflow-hidden rounded-full">
        {segments.map((s) => (
          <span key={s.tier} className={cn('h-full', TIER_META[s.tier].bar)} style={{ width: `${(s.count / total) * 100}%` }} />
        ))}
      </div>
      <div className="mt-2.5 flex flex-wrap gap-x-3.5 gap-y-1">
        {segments.map((s) => (
          <span key={s.tier} className="inline-flex items-center gap-1.5 text-[10px] font-black uppercase tracking-[0.12em] text-foreground/55">
            <span className={cn('h-1.5 w-1.5 rounded-full', TIER_META[s.tier].dot)} />
            <span className="tabular-nums text-foreground/80">{s.count}</span> {s.label}
          </span>
        ))}
      </div>
    </div>
  );
}

function MetricPanel({ report, posts, selected, reduce }: { report: BiteRunReport; posts: PerfPost[]; selected: number | null; reduce: boolean }) {
  const segments = parseDropShape(report.performance?.drop_shape);
  const ranks = posts.map((p) => parseRank(p.rank).value);
  const ceiling = Math.min(...ranks);
  const floor = Math.max(...ranks);
  const window = parseRank(posts[0]?.rank ?? '20/40').window;

  const post = selected != null ? posts[selected] : null;
  const allRefs = useMemo(
    () => report.crumbs.flatMap((c) => c.posts || []).concat(report.feed_fade.feed.concat(report.feed_fade.fade).flatMap((f) => f.posts || [])),
    [report],
  );
  const tag = post ? allRefs.find((p) => p.id === post.id) : undefined;
  const showPost = post != null;

  return (
    <div className="relative h-[228px] overflow-hidden">
      <motion.div
        className="absolute inset-0"
        animate={{ opacity: showPost ? 0 : 1 }}
        transition={reduce ? { duration: 0 } : { duration: 0.17, ease: SOFT_EASE }}
        style={{ pointerEvents: showPost ? 'none' : 'auto' }}
        aria-hidden={showPost}
      >
        <Eyebrow accent>This run · 10 vs the last 30</Eyebrow>
        <div className="mt-3"><CompositionStrip segments={segments} /></div>
        <div className="mt-5 grid grid-cols-2 gap-3">
          {[{ k: 'ceiling', v: ceiling }, { k: 'floor', v: floor }].map((c) => (
            <div key={c.k} className="rounded-[14px] border border-foreground/10 bg-foreground/[0.03] px-3.5 py-3">
              <div className="inline-flex items-baseline font-black leading-none text-foreground">
                <span className="text-[30px] tabular-nums xl:text-[34px]">{c.v}</span>
                <span className="ml-0.5 text-[12px] text-foreground/40">/{window}</span>
              </div>
              <div className="mt-1.5 text-[8px] font-black uppercase tracking-[0.16em] text-foreground/40">{c.k} rank</div>
            </div>
          ))}
        </div>
        {report.performance?.period_read && (
          <p className="mt-4 text-[12px] font-black leading-snug tracking-tight text-foreground/55">{report.performance.period_read}</p>
        )}
      </motion.div>

      <motion.div
        className="absolute inset-0"
        animate={{ opacity: showPost ? 1 : 0 }}
        transition={reduce ? { duration: 0 } : { duration: 0.17, ease: SOFT_EASE }}
        style={{ pointerEvents: showPost ? 'auto' : 'none' }}
        aria-hidden={!showPost}
      >
        {post && (() => {
          const { value, window: w } = parseRank(post.rank);
          const tier = landingTier(post.landing);
          const meta = TIER_META[tier];
          return (
            <div className="flex h-full gap-4 sm:gap-5">
              <PostThumb post={{ display_tag: tag?.display_tag, thumbnail: tag?.thumbnail || post.thumbnail }} fill />
              <div className="flex min-w-0 flex-1 flex-col py-0.5">
                <span className="text-[9px] font-black uppercase tracking-[0.16em] text-foreground/35">post {(selected ?? 0) + 1} of 10 · this run</span>
                {tag?.display_tag && <p className="mt-1.5 text-[17px] font-black leading-tight tracking-tight text-foreground sm:text-[19px]">{tag.display_tag}</p>}
                <div className="mt-2 flex flex-wrap items-center gap-1.5">
                  <span className={cn('inline-flex items-center gap-1 rounded-full px-2 py-0.5 text-[9px] font-black uppercase tracking-[0.12em]', meta.faded, meta.text)}>
                    <span className={cn('h-1 w-1 rounded-full', meta.dot)} /> {meta.label}
                  </span>
                  {post.job && (
                    <span className={cn('inline-flex rounded-full border px-2 py-0.5 text-[9px] font-black uppercase tracking-[0.12em]', post.job === 'overdelivered' ? 'border-[#E11D48]/30 text-[#E11D48]' : post.job === 'underdelivered' ? 'border-foreground/12 text-foreground/40' : 'border-foreground/15 text-foreground/55')}>{post.job}</span>
                  )}
                </div>
                <div className="mt-auto flex items-end justify-between gap-2 pt-3">
                  <span className="inline-flex items-baseline font-black leading-none text-foreground">
                    <span className="mr-1 text-[9px] font-black uppercase tracking-[0.18em] text-foreground/40">rank</span>
                    <Odometer value={value} animateOnMount className="inline-flex min-w-[1.2em] justify-end text-right text-[40px] tabular-nums xl:text-[46px]" />
                    <span className="ml-0.5 text-[15px] text-foreground/40">/{w}</span>
                  </span>
                  {tag?.url && (
                    <a href={tag.url} target="_blank" rel="noreferrer" className="inline-flex shrink-0 items-center gap-1 rounded-full border border-foreground/15 bg-foreground/[0.03] px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.12em] text-foreground/65 no-underline transition-colors hover:border-[#E11D48]/40 hover:bg-[#E11D48]/[0.08] hover:text-[#E11D48]">
                      Open post <span aria-hidden>↗</span>
                    </a>
                  )}
                </div>
              </div>
            </div>
          );
        })()}
      </motion.div>
    </div>
  );
}

function SlotCopy({ slotKey, delay = 0, reduce, className, children }: { slotKey: string; delay?: number; reduce: boolean; className?: string; children: React.ReactNode }) {
  return (
    <div className={cn('relative block overflow-hidden', className)}>
      <div aria-hidden className="invisible block">
        {children}
      </div>
      <AnimatePresence initial={false}>
        <motion.div
          key={slotKey}
          className="absolute inset-x-0 top-0 block"
          initial={reduce ? false : { opacity: 0, y: '-118%', rotate: -1.6 }}
          animate={{ opacity: 1, y: '0%', rotate: 0 }}
          exit={reduce ? undefined : { opacity: 0, y: '118%', rotate: 1.6 }}
          transition={reduce ? { duration: 0 } : { duration: 0.42, ease: SLOT_EASE, delay }}
        >
          {children}
        </motion.div>
      </AnimatePresence>
    </div>
  );
}

function DesktopTrajectoryPanel({
  view,
  report,
  posts,
  selected,
  selectFromBar,
  reduce,
  runIndex,
  runs,
  goRun,
}: {
  view: RunView;
  report: BiteRunReport;
  posts: PerfPost[];
  selected: number | null;
  selectFromBar: (i: number | null) => void;
  reduce: boolean;
  runIndex: number;
  runs: RunView[];
  goRun: (next: number) => void;
}) {
  const segments = parseDropShape(report.performance?.drop_shape);
  const ranks = posts.map((p) => parseRank(p.rank).value);
  const ceiling = Math.min(...ranks);
  const floor = Math.max(...ranks);
  const window = parseRank(posts[0]?.rank ?? '20/40').window;
  const post = selected != null ? posts[selected] : null;
  const refs = report.crumbs.flatMap((c) => c.posts || []).concat(report.feed_fade.feed.concat(report.feed_fade.fade).flatMap((f) => f.posts || []));
  const tag = post ? refs.find((p) => p.id === post.id) : undefined;
  const onDragEnd = (_: unknown, info: PanInfo) => {
    if (info.offset.x < -64 && runIndex < runs.length - 1) goRun(runIndex + 1);
    else if (info.offset.x > 64 && runIndex > 0) goRun(runIndex - 1);
  };

  return (
    <motion.div
      drag={reduce ? false : 'x'}
      dragConstraints={{ left: 0, right: 0 }}
      dragElastic={0.12}
      dragSnapToOrigin
      onDragEnd={onDragEnd}
      whileTap={reduce ? undefined : { scale: 0.992 }}
      className="fm-depth-glass relative cursor-grab overflow-hidden rounded-[30px] border border-white/80 p-6 shadow-[0_34px_90px_-34px_rgba(15,23,42,0.58),0_16px_36px_-24px_rgba(225,29,72,0.72),inset_0_1px_0_rgba(255,255,255,0.86)] ring-1 ring-[#E11D48]/10 active:cursor-grabbing dark:border-white/10"
      style={{ touchAction: 'pan-y' }}
    >
      <span aria-hidden className="pointer-events-none absolute left-2 top-1/2 z-20 grid h-16 w-5 -translate-y-1/2 place-items-center rounded-full border border-foreground/8 bg-white/68 text-[18px] font-black text-foreground/24 shadow-sm dark:bg-white/[0.08]">‹</span>
      <span aria-hidden className="pointer-events-none absolute right-2 top-1/2 z-20 grid h-16 w-5 -translate-y-1/2 place-items-center rounded-full border border-foreground/8 bg-white/68 text-[18px] font-black text-foreground/24 shadow-sm dark:bg-white/[0.08]">›</span>
      <div className="mb-3 flex items-center justify-between">
        <Eyebrow>Trajectory</Eyebrow>
        <Eyebrow className="text-foreground/28">{view.span}</Eyebrow>
      </div>
      <div className="grid grid-cols-[minmax(0,1fr)_280px] gap-5">
        <Bars posts={posts} selected={selected} onSelect={(i) => selectFromBar(i === selected ? null : i)} reduce={reduce} className="h-[min(318px,34vh)] min-h-[240px]" />
        <div className="flex min-h-0 flex-col border-l border-foreground/8 pl-5">
          <AnimatePresence mode="popLayout" initial={false}>
            {post ? (
              <motion.div
                key={`post:${post.id}`}
                initial={reduce ? false : { opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={reduce ? undefined : { opacity: 0, y: -8 }}
                transition={reduce ? { duration: 0 } : { duration: 0.19, ease: SOFT_EASE }}
                className="flex min-h-[214px] gap-4"
              >
                <PostThumb post={{ display_tag: tag?.display_tag, thumbnail: tag?.thumbnail || post.thumbnail }} fill />
                <div className="flex min-w-0 flex-1 flex-col">
                  <Eyebrow accent>Post {selected! + 1}</Eyebrow>
                  <p className="mt-2 text-[24px] font-black leading-[0.96] tracking-tight text-foreground">
                    {tag?.display_tag || `Rank ${parseRank(post.rank).value}`}
                  </p>
                  <p className="mt-auto inline-flex items-baseline font-black leading-none text-foreground">
                    <span className="mr-1 text-[9px] uppercase tracking-[0.16em] text-foreground/38">rank</span>
                    <span className="text-[54px] tabular-nums text-[#E11D48]">{parseRank(post.rank).value}</span>
                    <span className="text-[16px] text-foreground/38">/{parseRank(post.rank).window}</span>
                  </p>
                </div>
              </motion.div>
            ) : (
              <motion.div
                key={`run:${view.key}`}
                initial={reduce ? false : { opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                exit={reduce ? undefined : { opacity: 0, y: -8 }}
                transition={reduce ? { duration: 0 } : { duration: 0.19, ease: SOFT_EASE }}
              >
                <Eyebrow accent>This run</Eyebrow>
                <div className="mt-3">
                  <CompositionStrip segments={segments} />
                </div>
                <div className="mt-5 grid grid-cols-2 gap-2.5">
                  {[{ k: 'ceiling', v: ceiling }, { k: 'floor', v: floor }].map((c) => (
                    <div key={c.k} className="rounded-[14px] border border-foreground/10 bg-foreground/[0.03] px-3 py-3">
                      <div className="inline-flex items-baseline font-black leading-none text-foreground">
                        <span className="text-[31px] tabular-nums">{c.v}</span>
                        <span className="ml-0.5 text-[11px] text-foreground/40">/{window}</span>
                      </div>
                      <div className="mt-1.5 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/40">{c.k}</div>
                    </div>
                  ))}
                </div>
                {report.performance?.period_read && (
                  <p className="mt-5 text-[12px] font-black leading-snug tracking-tight text-foreground/56">{report.performance.period_read}</p>
                )}
              </motion.div>
            )}
          </AnimatePresence>
          <div className="mt-auto flex items-center gap-1.5 pt-5">
            {runs.map((r, i) => (
              <button
                key={r.key}
                type="button"
                aria-label={r.deckLabel}
                onClick={() => goRun(i)}
                className={cn('h-2 rounded-full transition-all', i === runIndex ? 'w-8 bg-[#E11D48]' : 'w-2 bg-foreground/18 hover:bg-foreground/35')}
              />
            ))}
          </div>
        </div>
      </div>
    </motion.div>
  );
}

function Instrument({ report, posts, selected, onSelect, reduce, instrumentRef, canReturn, onReturn }: {
  report: BiteRunReport; posts: PerfPost[]; selected: number | null; onSelect: (i: number | null) => void; reduce: boolean; instrumentRef: React.RefObject<HTMLDivElement | null>; canReturn: boolean; onReturn: () => void;
}) {
  return (
    <section
      ref={instrumentRef}
      className="fm-depth-glass relative scroll-mt-4 overflow-hidden rounded-[26px] p-5 sm:rounded-[30px] sm:p-7"
      style={{ background: 'linear-gradient(180deg, rgba(247,24,82,0.03), rgba(255,255,255,0.01) 30%, rgba(0,0,0,0.02) 100%)' }}
    >
      <div aria-hidden className="pointer-events-none absolute inset-0 -z-10" style={{ background: 'radial-gradient(ellipse 60% 60% at 100% 0%, rgba(225,29,72,0.08), transparent 60%)' }} />
      <div className="mb-5 flex items-center justify-between gap-3">
        <div className="flex items-center gap-2.5">
          <span className="h-2 w-2 rounded-full bg-[#E11D48] fm-live-dot" />
          <Eyebrow>Trajectory · post by post</Eyebrow>
        </div>
        {canReturn ? (
          <button type="button" onClick={onReturn} className="inline-flex items-center gap-1 rounded-full bg-[#E11D48]/[0.08] px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.14em] text-[#E11D48] transition-colors hover:bg-[#E11D48]/[0.14]">↩ back to the read</button>
        ) : selected != null ? (
          <button type="button" onClick={() => onSelect(null)} className="text-[9px] font-black uppercase tracking-[0.16em] text-foreground/45 transition-colors hover:text-foreground/75">← whole run</button>
        ) : (
          <span className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/30">tap a bar</span>
        )}
      </div>
      <div className="grid gap-6 lg:grid-cols-[minmax(0,1.15fr)_minmax(280px,0.85fr)] lg:gap-9">
        <Bars posts={posts} selected={selected} onSelect={(i) => onSelect(i === selected ? null : i)} reduce={reduce} />
        <div className="lg:border-l lg:border-foreground/10 lg:pl-9">
          <MetricPanel report={report} posts={posts} selected={selected} reduce={reduce} />
        </div>
      </div>
    </section>
  );
}

/* ── the read ────────────────────────────────────────────────────────────── */
function reveal(delay: number, reduce: boolean) {
  if (reduce) return {};
  return { initial: { opacity: 0, y: 14 }, whileInView: { opacity: 1, y: 0 }, viewport: { once: true, margin: '-60px' }, transition: { duration: 0.33, ease: SOFT_EASE, delay } };
}

function Crumbs({ report, runIndexById, onMention, reduce }: { report: BiteRunReport; runIndexById: Map<string, number>; onMention: (i: number, el: HTMLElement) => void; reduce: boolean }) {
  return (
    <div>
      <Eyebrow accent>What got bit</Eyebrow>
      <div className="mt-5">
        {report.crumbs.map((crumb, i) => (
          <motion.div
            key={crumb.label}
            {...reveal(i * 0.06, reduce)}
            className="grid gap-x-10 gap-y-3 border-t border-foreground/10 py-7 sm:py-9 lg:grid-cols-[minmax(0,7fr)_minmax(0,9fr)]"
          >
            <div className="flex items-start gap-4">
              <span className="text-[44px] font-black leading-[0.8] tabular-nums text-foreground/[0.15] sm:text-[64px]">{String(i + 1).padStart(2, '0')}</span>
              <h3 className="pt-1 text-[21px] font-black leading-tight tracking-tight text-foreground sm:text-[27px]">{crumb.label}</h3>
            </div>
            <div className="min-w-0">
              <p className="text-[15px] font-bold leading-relaxed text-foreground/68 sm:text-[16.5px]">{crumb.read}</p>
              <Mentions posts={crumb.posts} runIndexById={runIndexById} onMention={onMention} />
            </div>
          </motion.div>
        ))}
      </div>
    </div>
  );
}

function FeedFade({ report, runIndexById, onMention, reduce }: { report: BiteRunReport; runIndexById: Map<string, number>; onMention: (i: number, el: HTMLElement) => void; reduce: boolean }) {
  const cols: { kind: 'Feed' | 'Fade'; sub: string; items: FeedFadeItem[] }[] = [
    { kind: 'Feed', sub: 'give the audience more of this', items: report.feed_fade.feed },
    { kind: 'Fade', sub: 'lift the ones starting to fade', items: report.feed_fade.fade },
  ];
  return (
    <div className="grid gap-10 lg:grid-cols-2 lg:gap-12">
      {cols.map((col) => {
        const feed = col.kind === 'Feed';
        return (
          <div key={col.kind}>
            <div className="border-t-2 pt-4" style={{ borderColor: feed ? ACCENT : 'rgba(127,127,127,0.28)' }}>
              <h3 className={cn('text-[22px] font-black leading-none tracking-tight sm:text-[26px]', feed ? 'text-[#E11D48]' : 'text-foreground/80')}>{col.kind}</h3>
              <span className="mt-1.5 block text-[10px] font-black uppercase tracking-[0.14em] text-foreground/40">{col.sub}</span>
            </div>
            <div className="mt-6 space-y-7">
              {col.items.map((item, i) => (
                <motion.div key={`${item.read}:${i}`} {...reveal(i * 0.06, reduce)} className="min-w-0">
                  <p className="max-w-[520px] text-[15px] font-black leading-snug tracking-tight text-foreground sm:text-[16px]">{item.read}</p>
                  <div className="mt-3 flex gap-2.5">
                    <span className={cn('mt-0.5 shrink-0 text-[13px] font-black leading-none', feed ? 'text-[#E11D48]' : 'text-foreground/40')}>↳</span>
                    <p className="max-w-[500px] text-[13.5px] font-bold leading-relaxed text-foreground/55">{item.next}</p>
                  </div>
                  <Mentions posts={item.posts} runIndexById={runIndexById} onMention={onMention} />
                </motion.div>
              ))}
            </div>
          </div>
        );
      })}
    </div>
  );
}

function DesktopReaderConsole({
  feederLabel,
  view,
  runs,
  runIndex,
  goRun,
  report,
  posts,
  selected,
  selectFromBar,
  reduce,
}: {
  feederLabel: string;
  view: RunView;
  runs: RunView[];
  runIndex: number;
  goRun: (next: number) => void;
  report: BiteRunReport;
  posts: PerfPost[];
  selected: number | null;
  selectFromBar: (i: number | null) => void;
  reduce: boolean;
}) {
  return (
    <section className="hidden lg:block">
      <div className="mx-auto grid min-h-[calc(var(--fm-app-height,100dvh)-var(--fm-reader-desktop-offset)-28px)] max-w-[1320px] grid-rows-[auto_auto_auto] gap-4 pb-8">
        <div className="flex items-end justify-between gap-6 px-2 pt-1">
          <div className="min-w-0 max-w-[1040px]">
            <SlotCopy slotKey={`deck:${view.key}`} reduce={reduce}>
              <Eyebrow accent>{view.deckLabel}</Eyebrow>
            </SlotCopy>
            <SlotCopy slotKey={`headline:${view.key}`} delay={0.08} reduce={reduce} className="mt-2">
              <h1 className="text-[66px] font-black leading-[0.86] tracking-tight text-foreground xl:text-[76px]">
                {report.headline || feederLabel}
              </h1>
            </SlotCopy>
          </div>
          <div className="mb-2 flex shrink-0 items-center gap-2">
            {runs.map((r, i) => (
              <button
                key={r.key}
                type="button"
                aria-label={r.deckLabel}
                onClick={() => goRun(i)}
                className={cn('h-2 rounded-full transition-all', i === runIndex ? 'w-7 bg-[#E11D48]' : 'w-2 bg-foreground/18 hover:bg-foreground/35')}
              />
            ))}
            <button type="button" aria-label="Newer run" disabled={runIndex === 0} onClick={() => goRun(runIndex - 1)} className="ml-1 inline-flex h-7 w-7 items-center justify-center rounded-full border border-foreground/12 text-[14px] font-black text-foreground/55 disabled:opacity-25">‹</button>
            <button type="button" aria-label="Older run" disabled={runIndex === runs.length - 1} onClick={() => goRun(runIndex + 1)} className="inline-flex h-7 w-7 items-center justify-center rounded-full border border-foreground/12 text-[14px] font-black text-foreground/55 disabled:opacity-25">›</button>
          </div>
        </div>
        <div className="grid grid-cols-[minmax(0,0.95fr)_minmax(500px,0.95fr)] gap-5">
          <div className="px-2 py-2 pr-4">
            <div className="grid grid-cols-[minmax(0,1.15fr)_minmax(260px,0.75fr)] gap-8">
              <div>
                <Eyebrow>The run read</Eyebrow>
                <SlotCopy slotKey={`run-read:${view.key}`} delay={0.18} reduce={reduce}>
                  <p className="mt-3 text-[21px] font-black leading-[1.22] tracking-tight text-foreground/84">
                    {report.run_report}
                  </p>
                </SlotCopy>
              </div>
              <div>
                <Eyebrow accent>Bite size</Eyebrow>
                <SlotCopy slotKey={`bite-size:${view.key}`} delay={0.28} reduce={reduce}>
                  <p className="mt-3 text-[15px] font-black leading-snug tracking-tight text-foreground/68">
                    {report.bite_size}
                  </p>
                </SlotCopy>
              </div>
            </div>
          </div>

          <DesktopTrajectoryPanel
            view={view}
            report={report}
            posts={posts}
            selected={selected}
            selectFromBar={selectFromBar}
            reduce={reduce}
            runIndex={runIndex}
            runs={runs}
            goRun={goRun}
          />
        </div>

        <div className="grid min-h-0 gap-7">
          <div className="min-h-0 px-2 py-1">
            <Eyebrow accent>What got bit</Eyebrow>
            <div className="mt-3 grid grid-cols-3 gap-9">
              {report.crumbs.map((crumb, i) => (
                <SlotCopy key={crumb.label} slotKey={`crumb:${view.key}:${crumb.label}`} delay={0.34 + i * 0.07} reduce={reduce}>
                  <div className="min-w-0">
                    <div className="flex items-baseline gap-2">
                      <span className="text-[24px] font-black leading-none tabular-nums text-[#E11D48]">{String(i + 1).padStart(2, '0')}</span>
                      <h2 className="text-[31px] font-black leading-[0.94] tracking-tight text-foreground">{crumb.label}</h2>
                    </div>
                    <p className="mt-3 text-[18px] font-black leading-[1.18] tracking-tight text-foreground/74">{crumb.read}</p>
                    <div className="mt-4 flex flex-wrap gap-x-3 gap-y-1.5">
                      {(crumb.posts || []).map((post) => (
                        <span key={`${post.id}:${post.display_tag}`} className="text-[13.5px] font-black leading-tight text-[#E11D48]">
                          {post.display_tag}
                        </span>
                      ))}
                    </div>
                  </div>
                </SlotCopy>
              ))}
            </div>
          </div>

          <aside className="grid min-h-0 grid-cols-2 gap-10 px-2">
            {(['Feed', 'Fade'] as const).map((kind) => {
              const items = kind === 'Feed' ? report.feed_fade.feed : report.feed_fade.fade;
              return (
                <div key={kind} className="py-2">
                  <SlotCopy slotKey={`desktop-${kind}:${view.key}`} delay={0.48 + (kind === 'Fade' ? 0.08 : 0)} reduce={reduce}>
                    <h3 className={cn('text-[54px] font-black leading-none tracking-tight', kind === 'Feed' ? 'text-[#E11D48]' : 'text-foreground/72')}>{kind}</h3>
                    <div className="mt-4 grid grid-cols-2 gap-7">
                      {items.map((item, i) => (
                        <div key={i} className="border-t border-foreground/8 pt-3">
                          <p className="text-[18px] font-black leading-[1.14] tracking-tight text-foreground/78">{item.read}</p>
                          <p className="mt-2 text-[15px] font-bold leading-snug text-foreground/50">{item.next}</p>
                        </div>
                      ))}
                    </div>
                  </SlotCopy>
                </div>
              );
            })}
          </aside>
        </div>
      </div>
    </section>
  );
}

function FeederReaderReport({
  baseReport,
  onBack,
  backLabel = 'Dashboard',
  feederLabel = '@anuj',
  showHeader = true,
}: Omit<FeederReaderPageProps, 'report'> & { baseReport: BiteRunReport }) {
  const reduce = Boolean(useReducedMotion());
  const runs = useMemo(() => buildRuns(baseReport), [baseReport]);

  const [runIndex, setRunIndex] = useState(0);
  const view = runs[runIndex];
  const report = view.report;
  const posts = useMemo(() => report.performance?.posts?.slice(0, 10) ?? [], [report]);
  const runIndexById = useMemo(() => new Map(posts.map((p, i) => [p.id, i] as const)), [posts]);

  const [selected, setSelected] = useState<number | null>(null);
  const instrumentRef = useRef<HTMLDivElement | null>(null);
  const originRef = useRef<HTMLElement | null>(null);
  const mobileTopRef = useRef<HTMLDivElement | null>(null);
  const mobileSwipeRef = useRef<{ x: number; y: number } | null>(null);
  const [canReturn, setCanReturn] = useState(false);

  const goRun = useCallback((next: number) => {
    setRunIndex((next + runs.length) % runs.length);
    setSelected(null);
    setCanReturn(false);
    originRef.current = null;
  }, [runs.length]);
  const scrollMobileTop = useCallback(() => {
    window.requestAnimationFrame(() => {
      window.requestAnimationFrame(() => mobileTopRef.current?.scrollIntoView({ behavior: 'auto', block: 'start' }));
    });
  }, []);
  const goMobileRun = useCallback((next: number) => {
    goRun(next);
    scrollMobileTop();
  }, [goRun, scrollMobileTop]);
  useEffect(() => {
    const node = mobileTopRef.current;
    if (!node) return undefined;
    const onStart = (event: TouchEvent) => {
      const touch = event.touches[0];
      mobileSwipeRef.current = touch ? { x: touch.clientX, y: touch.clientY } : null;
    };
    const onEnd = (event: TouchEvent) => {
      const start = mobileSwipeRef.current;
      const touch = event.changedTouches[0];
      mobileSwipeRef.current = null;
      if (!start || !touch) return;
      const dx = touch.clientX - start.x;
      const dy = touch.clientY - start.y;
      if (Math.abs(dx) < 82 || Math.abs(dx) < Math.abs(dy) * 1.45) return;
      if (dx < 0 && runIndex < runs.length - 1) goMobileRun(runIndex + 1);
      else if (dx > 0 && runIndex > 0) goMobileRun(runIndex - 1);
    };
    const onCancel = () => { mobileSwipeRef.current = null; };
    node.addEventListener('touchstart', onStart, { passive: true });
    node.addEventListener('touchend', onEnd, { passive: true });
    node.addEventListener('touchcancel', onCancel, { passive: true });
    return () => {
      node.removeEventListener('touchstart', onStart);
      node.removeEventListener('touchend', onEnd);
      node.removeEventListener('touchcancel', onCancel);
    };
  }, [goMobileRun, runIndex, runs.length]);
  const selectFromBar = (i: number | null) => {
    setSelected(i);
    setCanReturn(false);
    originRef.current = null;
  };
  const onMention = (i: number, el: HTMLElement) => {
    originRef.current = el;
    setSelected(i);
    setCanReturn(true);
    instrumentRef.current?.scrollIntoView({ behavior: 'auto', block: 'start' });
  };
  const returnToRead = () => {
    originRef.current?.scrollIntoView({ behavior: 'auto', block: 'center' });
    setCanReturn(false);
  };
  const backClass = 'flex min-w-0 items-center gap-1.5 text-[10px] font-black uppercase tracking-[0.16em] text-foreground/45 no-underline transition-colors hover:text-foreground/70 sm:text-[11px] sm:tracking-[0.2em]';

  return (
    <div className="mx-auto min-w-0 max-w-full overflow-x-hidden text-foreground lg:max-w-[1160px] xl:max-w-[1280px]">
      {showHeader && (
        <div className="mb-6 grid min-w-0 grid-cols-[1fr_auto_1fr] items-center gap-2">
          {onBack ? (
            <button type="button" onClick={onBack} className={cn(backClass, 'bg-transparent p-0 text-left')}><span className="text-[14px]">‹</span><span className="truncate">{backLabel}</span></button>
          ) : (
            <Link href="/" className={backClass}><span className="text-[14px]">‹</span><span className="truncate">{backLabel}</span></Link>
          )}
          <div className="flex min-w-0 items-center gap-2"><span className="h-2 w-2 shrink-0 rounded-full bg-[#E11D48] fm-live-dot" /><span className="truncate text-[10px] font-black uppercase tracking-[0.18em] text-foreground/55 sm:text-[11px] sm:tracking-[0.26em]">Feeder Reader</span></div>
          <span className="min-w-0 truncate text-right text-[11px] font-black tracking-tight text-foreground/70">{feederLabel}</span>
        </div>
      )}

      <DesktopReaderConsole
        feederLabel={feederLabel}
        view={view}
        runs={runs}
        runIndex={runIndex}
        goRun={goRun}
        report={report}
        posts={posts}
        selected={selected}
        selectFromBar={selectFromBar}
        reduce={reduce}
      />

      <div
        ref={mobileTopRef}
        className="lg:hidden"
      >
      {/* masthead + run navigator */}
      <div className="mb-6">
        <div className="flex items-end justify-between gap-4">
          <Eyebrow accent>{view.deckLabel}</Eyebrow>
          <div className="flex items-center gap-3">
            <div className="flex items-center gap-1.5">
              {runs.map((r, i) => (
                <button key={r.key} type="button" aria-label={r.deckLabel} onClick={() => goMobileRun(i)} className={cn('h-1.5 rounded-full transition-all', i === runIndex ? 'w-5 bg-[#E11D48]' : 'w-1.5 bg-foreground/20 hover:bg-foreground/40')} />
              ))}
            </div>
            <div className="flex items-center gap-1">
              <button type="button" aria-label="Newer run" disabled={runIndex === 0} onClick={() => goMobileRun(runIndex - 1)} className="inline-flex h-6 w-6 items-center justify-center rounded-full border border-foreground/14 text-[13px] font-black text-foreground/60 transition-colors hover:bg-foreground/[0.05] disabled:opacity-25">‹</button>
              <button type="button" aria-label="Older run" disabled={runIndex === runs.length - 1} onClick={() => goMobileRun(runIndex + 1)} className="inline-flex h-6 w-6 items-center justify-center rounded-full border border-foreground/14 text-[13px] font-black text-foreground/60 transition-colors hover:bg-foreground/[0.05] disabled:opacity-25">›</button>
            </div>
          </div>
        </div>
        <h1 className="mt-2 text-[42px] font-black leading-[0.9] tracking-tight text-foreground sm:text-[52px]">{report.headline || feederLabel}</h1>
        {report.headline && <p className="mt-1 text-[13px] font-black text-foreground/45">{feederLabel}</p>}
        <p className="mt-2 text-[11px] font-black uppercase tracking-[0.16em] text-foreground/40">{view.span} · read against the last 30</p>
      </div>

      <div key={view.key} className="min-w-0 overflow-x-clip">
          <Instrument report={report} posts={posts} selected={selected} onSelect={selectFromBar} reduce={reduce} instrumentRef={instrumentRef} canReturn={canReturn} onReturn={returnToRead} />

          {/* setup + verdict — twin-length fields paired so neither leaves a gutter */}
          <div className="mt-9 grid gap-7">
            <motion.p {...reveal(0, reduce)} className="text-[22px] font-black leading-[1.22] tracking-tight text-foreground/84 first-letter:float-left first-letter:mr-2.5 first-letter:text-[64px] first-letter:font-black first-letter:leading-[0.78] first-letter:text-[#E11D48] sm:text-[24px]">
              {report.run_report}
            </motion.p>
            <motion.div {...reveal(0.06, reduce)}>
              <Eyebrow accent>Bite size</Eyebrow>
              <p className="mt-3 text-[17px] font-black leading-snug tracking-tight text-foreground/[0.68] sm:text-[18px]">{report.bite_size}</p>
            </motion.div>
          </div>

          {/* the long read — two columns so it fills the width and stays readable */}
          <motion.div {...reveal(0, reduce)} className="mt-12 border-t border-foreground/10 pt-9">
            <Eyebrow>The read</Eyebrow>
            <p className="mt-4 text-[15.5px] font-bold leading-relaxed text-foreground/[0.74] sm:text-[16.5px] lg:columns-2 lg:gap-12 [&]:[column-fill:balance]">{report.what_got_bit}</p>
          </motion.div>

          <div className="mt-12 border-t border-foreground/10 pt-9">
            <Crumbs report={report} runIndexById={runIndexById} onMention={onMention} reduce={reduce} />
          </div>

          <div className="mt-12 border-t border-foreground/10 pt-9">
            <FeedFade report={report} runIndexById={runIndexById} onMention={onMention} reduce={reduce} />
          </div>
      </div>
      </div>
    </div>
  );
}

function EmptyReader({
  onBack,
  backLabel = 'Dashboard',
  feederLabel = '@anuj',
  showHeader = true,
}: Omit<FeederReaderPageProps, 'report'>) {
  const backClass = 'flex min-w-0 items-center gap-1.5 text-[10px] font-black uppercase tracking-[0.16em] text-foreground/45 no-underline transition-colors hover:text-foreground/70 sm:text-[11px] sm:tracking-[0.2em]';

  return (
    <div className="mx-auto min-w-0 max-w-full overflow-x-hidden text-foreground lg:max-w-[1160px] xl:max-w-[1280px]">
      {showHeader ? (
        <div className="mb-6 grid min-w-0 grid-cols-[1fr_auto_1fr] items-center gap-2">
          {onBack ? (
            <button type="button" onClick={onBack} className={cn(backClass, 'bg-transparent p-0 text-left')}><span className="text-[14px]">‹</span><span className="truncate">{backLabel}</span></button>
          ) : (
            <Link href="/" className={backClass}><span className="text-[14px]">‹</span><span className="truncate">{backLabel}</span></Link>
          )}
          <div className="flex min-w-0 items-center gap-2"><span className="h-2 w-2 shrink-0 rounded-full bg-[#E11D48]" /><span className="truncate text-[10px] font-black uppercase tracking-[0.18em] text-foreground/55 sm:text-[11px] sm:tracking-[0.26em]">Feeder Reader</span></div>
          <span className="min-w-0 truncate text-right text-[11px] font-black tracking-tight text-foreground/70">{feederLabel}</span>
        </div>
      ) : null}
      <div className="flex min-h-[420px] items-center justify-center rounded-[24px] border border-foreground/10 bg-foreground/[0.025] px-6 text-center">
        <div>
          <p className="text-[10px] font-black uppercase tracking-[0.2em] text-[#E11D48]">Reader ready</p>
          <h2 className="mt-3 text-[30px] font-black tracking-[-0.04em] text-foreground">No stored run</h2>
          <p className="mx-auto mt-3 max-w-md text-[13px] font-bold leading-relaxed text-foreground/48">The reader design is preserved. A verified run will populate it when live data is connected.</p>
        </div>
      </div>
    </div>
  );
}

export default function FeederReaderPage({ report, ...props }: FeederReaderPageProps = {}) {
  return report ? <FeederReaderReport {...props} baseReport={report} /> : <EmptyReader {...props} />;
}
