'use client';

/* ─────────────────────────────────────────────────────────────
   POST CELL — one post on the Feed, the same element in every
   view (WEEK, MONTH and the Day view), so a move between views
   carries the post itself, its picture drawn once.

   It is a box at its place (FeedStream lays it out; nothing here
   moves it) holding layers the moves can take hold of:
     frame   — the picture's box: a grid's whole tile, a phone
               card's 4:5 across its top, a row's 4:5 at its left;
     bg      — a phone card's surface and wash of its picture, the
               card's whole box;
     chrome  — what reads it: a tile's number (inside its frame,
               so it scales with it from one grid to the other),
               a phone card's foot (who, the number, the trail,
               the counts) and marks, a row's reading beside it.
   During a move out of a view, that view's chrome is drawn as it
   was, at its old size (the *-out layers), for the move to hold
   where it stood while it goes; the new view's chrome comes in a
   piece at a time ([data-arriving], stream.css).

   The wide Day view's row on the focus line has the spotlight
   (spot): its reading comes in a piece at a time and its picture
   is lit; the rest of the column is dimmed, its reading away.
   ───────────────────────────────────────────────────────────── */

import { memo, useEffect, useLayoutEffect, useRef, useState, type CSSProperties, type KeyboardEvent as ReactKeyboardEvent } from 'react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import CheckpointTrail from '@/components/feed/stream/CheckpointTrail';
import { PostPicture, Wash } from '@/components/feed/stream/PostMedia';
import PostReading from '@/components/feed/stream/PostReading';
import { press } from '@/components/feed/stream/press';
import TopBadge from '@/components/feed/stream/TopBadge';
import {
  formatCount,
  formatMultiple,
  formatTopPercent,
  groupParts,
  isWinner,
  istDay,
  relativeTime,
  shiftDay,
  type FeedMode,
  type StreamFeeder,
  type StreamMediaType,
  type StreamPost,
} from '@/lib/feedStream/contract';
import { titleCase } from '@/lib/feedLabels';
import { POST_ASPECT, READING_GAP } from '@/lib/feedStream/layout';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

const useIsoLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

// type over a picture: the scrim does the work, a soft shadow keeps it crisp on the brightest spots
const ON_PICTURE = '[text-shadow:0_1px_2px_rgb(0_0_0/0.35)]';

/* ── the box a post has in a view ─────────────────────────── */

export type CellShape = { w: number; h: number; cols: number; rows: number; mediaWidth: number; wide: boolean };

// the picture's frame inside the cell, and its corners
function frameOf(mode: FeedMode, shape: CellShape): { width: number; height: number; radius: string } {
  if (mode !== 'today') return { width: shape.w, height: shape.h, radius: '6px' };
  if (shape.wide) return { width: shape.mediaWidth, height: shape.h, radius: '28px' };
  const height = Math.min(shape.h, shape.w / POST_ASPECT);
  return { width: shape.w, height, radius: height >= shape.h - 0.5 ? '28px' : '28px 28px 0 0' };
}

/* ── a tile's chrome (WEEK, MONTH): minimal but bold ──────────
   1×1 — the number alone · 1×2 — a TOP kicker over it · 2×2 — TOP · D3 over it, and in a mixed scope a tiny face
   and @handle top-left. The number is sized by the tile's width; a winner's is rose. A reel has a small ▶. */

type Shape = 'one' | 'tall' | 'big';
const NUMBER: Record<Shape, { share: number; min: number; max: number }> = {
  one: { share: 0.2, min: 20, max: 60 },
  tall: { share: 0.22, min: 24, max: 64 },
  big: { share: 0.2, min: 44, max: 104 },
};
const SIGN_SHARE = 0.64;
const INSET: Record<Shape, number> = { one: 8, tall: 9, big: 12 };
const SCRIM: Record<Shape, string> = { one: 'h-[68%]', tall: 'h-[44%]', big: 'h-[52%]' };

function GridChips({ post, cols, rows, width, feeder }: { post: StreamPost; cols: number; rows: number; width: number; feeder: StreamFeeder | null }) {
  const shape: Shape = cols >= 2 && rows >= 2 ? 'big' : rows >= 2 ? 'tall' : 'one';
  const top = post.topPercent;
  const measured = top != null && Number.isFinite(top);
  const winner = isWinner(top);
  const rule = NUMBER[shape];
  const size = Math.round(Math.min(rule.max, Math.max(rule.min, width * rule.share)));
  const inset = INSET[shape];
  const kicker = shape === 'big' ? (post.latestCheckpoint ? `Top · ${post.latestCheckpoint}` : 'Top') : shape === 'tall' ? 'Top' : null;
  const showFace = shape === 'big' && feeder != null;
  const reel = post.mediaType === 'reel';
  const value = measured ? formatTopPercent(top).slice(0, -1) : '';
  return (
    <>
      {showFace ? (
        <>
          <span aria-hidden="true" className="st-scrim-top pointer-events-none absolute inset-x-0 top-0 h-[34%]" />
          <span data-st="" className={cn('absolute flex items-center gap-1.5', ON_PICTURE)} style={{ top: inset, left: inset, maxWidth: `calc(100% - ${inset * 2 + (reel ? 18 : 0)}px)` }}>
            <span className="relative h-[18px] w-[18px] shrink-0 overflow-hidden rounded-full shadow-[0_0_0_1px_rgba(255,255,255,0.24)]">
              <FeederStoryAvatar feeder={feeder} className="text-[8px]" />
            </span>
            <span className="truncate text-[10px] font-bold leading-none text-white">@{feeder.handle}</span>
          </span>
        </>
      ) : null}
      {reel ? (
        // a white ▶ with a faint dark edge painted under it (no plate, no filter)
        <svg data-st="" aria-hidden="true" viewBox="0 0 12 12" width={12} height={12} className="pointer-events-none absolute" style={{ top: inset, right: inset }}>
          <path d="M3.4 2.1 10 6l-6.6 3.9Z" fill="#fff" stroke="rgba(0, 0, 0, 0.32)" strokeWidth={2} strokeLinejoin="round" paintOrder="stroke" />
        </svg>
      ) : null}
      {measured ? (
        <>
          <span aria-hidden="true" className={cn('st-scrim-soft pointer-events-none absolute inset-x-0 bottom-0', SCRIM[shape])} />
          <span data-st="" className={cn('pointer-events-none absolute flex flex-col items-start', ON_PICTURE)} style={{ left: inset, bottom: inset - 1 }}>
            {kicker ? <span className="mb-1.5 text-[10px] font-black uppercase leading-none tracking-[0.14em] text-white/75">{kicker}</span> : null}
            {/* past the face's heaviest cut (700): a hairline stroke in its own colour, painted under the fill */}
            <span
              className={cn(
                'flex items-baseline font-black leading-[0.82] tracking-[-0.03em] tabular-nums [-webkit-text-stroke:0.045em_currentColor] [paint-order:stroke_fill]',
                winner ? 'text-[var(--fm-accent-bright)]' : 'text-white',
              )}
              style={{ fontSize: size }}
            >
              {value}
              <span className="ml-[0.04em] tracking-[-0.02em]" style={{ fontSize: Math.round(size * SIGN_SHARE) }}>%</span>
            </span>
          </span>
        </>
      ) : null}
    </>
  );
}

/* ── a phone card's chrome: who and when, the number, the trail, the counts; its date and standout marks ── */

const ROOMY_MIN = 500;
const TRAIL_MIN = 380;

function landedNote(post: StreamPost, today: string) {
  if (!post.latestCheckpoint || !post.resultDay) return null;
  if (post.resultDay === today) return { text: `${post.latestCheckpoint} landed today`, today: true };
  if (post.resultDay === shiftDay(today, -1)) return { text: `${post.latestCheckpoint} landed yesterday`, today: false };
  return null;
}

function standoutText(post: StreamPost, today: string) {
  if (post.day === today || post.resultDay === today) return "Today's standout";
  const yesterday = shiftDay(today, -1);
  if (post.day === yesterday || post.resultDay === yesterday) return "Yesterday's standout";
  return 'Standout';
}

function Marks({ post, standout, dayStart }: { post: StreamPost; standout: boolean; dayStart: { kicker: string; title: string } | null }) {
  if (!standout && !dayStart) return null;
  return (
    <span className="pointer-events-none absolute left-4 top-4 z-[4] flex flex-col gap-2">
      {dayStart ? (
        <span data-st="" className="inline-flex w-fit flex-col rounded-[14px] bg-black/55 px-3 pb-2.5 pt-2 shadow-[inset_0_0_0_1px_rgba(255,255,255,0.1)]" style={{ '--i': 0 } as CSSProperties}>
          <span className="text-[10px] font-black uppercase leading-none tracking-[0.22em] text-[var(--fm-accent-bright)]">{dayStart.kicker}</span>
          <span className="mt-1.5 text-[22px] font-black leading-none tracking-[-0.04em] text-white">{dayStart.title}</span>
        </span>
      ) : null}
      {standout ? (
        <span data-st="" className="inline-flex w-fit items-center gap-1.5 rounded-[6px] bg-black/55 px-2 py-[5px] text-[10px] font-black uppercase leading-none tracking-[0.14em] text-[var(--fm-accent-bright)]" style={{ '--i': 1 } as CSSProperties}>
          <span aria-hidden="true" className="h-1.5 w-1.5 shrink-0 rounded-full bg-[var(--fm-accent-bright)]" />
          {standoutText(post, istDay())}
        </span>
      ) : null}
    </span>
  );
}

function CardChrome({ post, feeder, width, height, standout, dayStart }: {
  post: StreamPost;
  feeder: StreamFeeder | null;
  width: number;
  height: number;
  standout: boolean;
  dayStart: { kicker: string; title: string } | null;
}) {
  const today = istDay();
  const roomy = height >= ROOMY_MIN;
  const face = feeder ?? { handle: post.handle, profilePicUrl: null };
  const meta = [feeder?.feedTitle ? titleCase(feeder.feedTitle) : null, relativeTime(post.postedAt)].filter(Boolean).join(' · ');
  const landed = landedNote(post, today);
  const stats = [
    post.multiple != null ? { key: 'multiple', value: formatMultiple(post.multiple), unit: 'usual' } : null,
    post.likes != null ? { key: 'likes', value: formatCount(post.likes), unit: post.likes === 1 ? 'like' : 'likes' } : null,
    post.comments != null ? { key: 'comments', value: formatCount(post.comments), unit: post.comments === 1 ? 'comment' : 'comments' } : null,
  ].filter((stat): stat is { key: string; value: string; unit: string } => stat != null);
  return (
    <>
      {/* the post's 4:5 frame ends above the reading, so the scrim only needs to cover the foot */}
      <span aria-hidden="true" className="st-scrim pointer-events-none absolute inset-x-0 bottom-0 h-[46%] rounded-b-[28px]" />
      <Marks post={post} standout={standout} dayStart={dayStart} />
      <span className={cn('st-on-media pointer-events-none absolute inset-x-0 bottom-0 flex flex-col p-4 sm:p-5', roomy ? 'gap-3' : 'gap-2.5')} style={{ maxWidth: Math.min(width, 460) }}>
        <span data-st="" className={cn('flex min-w-0 items-center gap-2.5', ON_PICTURE)} style={{ '--i': 0 } as CSSProperties}>
          <span className="relative h-8 w-8 shrink-0 overflow-hidden rounded-full shadow-[0_0_0_1px_rgba(255,255,255,0.22)]">
            <FeederStoryAvatar feeder={face} className="text-[12px]" />
          </span>
          <span className="min-w-0">
            <span className="block truncate text-[14px] font-bold leading-tight text-white">@{post.handle}</span>
            {meta ? <span className="mt-0.5 block truncate text-[12px] font-medium leading-tight text-white/65">{meta}</span> : null}
          </span>
        </span>
        <span data-st="" className="flex min-w-0 items-end gap-3" style={{ '--i': 1 } as CSSProperties}>
          <TopBadge size={roomy ? 'xl' : 'lg'} topPercent={post.topPercent} checkpoint={post.latestCheckpoint} />
          {landed ? (
            <span className={cn('flex min-w-0 items-center gap-1.5 pb-1 text-[12px] font-medium leading-tight text-white/65', ON_PICTURE)}>
              {landed.today ? <span aria-hidden="true" className="h-1.5 w-1.5 shrink-0 rounded-full bg-[var(--fm-accent-bright)]" /> : null}
              <span className="truncate">{landed.text}</span>
            </span>
          ) : null}
        </span>
        {height >= TRAIL_MIN ? (
          <span data-st="" className="block" style={{ '--i': 2 } as CSSProperties}>
            <CheckpointTrail results={post.results} latest={post.latestCheckpoint} size="md" />
          </span>
        ) : null}
        {stats.length ? (
          <span data-st="" className={cn('flex min-w-0 flex-wrap items-baseline gap-x-1.5 text-[12px] font-medium leading-snug text-white/65', ON_PICTURE)} style={{ '--i': 3 } as CSSProperties}>
            {stats.map((stat, index) => (
              <span key={stat.key} className="whitespace-nowrap">
                {index ? <span aria-hidden="true" className="mr-1.5 text-white/45">·</span> : null}
                <b className="font-bold tabular-nums text-white">{stat.value}</b> {stat.unit}
              </span>
            ))}
          </span>
        ) : null}
      </span>
      {isWinner(post.topPercent) ? (
        <span aria-hidden="true" className="pointer-events-none absolute inset-0 z-[4] rounded-[28px] shadow-[inset_0_0_0_1.5px_rgb(var(--fm-accent-rgb)/0.85)]" />
      ) : null}
    </>
  );
}

/* ── a row's reading (the wide Day view): measured to fit the row, never cut ── */

const READING_SIZES = ['stage', 'sheet', 'compact', 'tight'] as const;
type ReadingSize = (typeof READING_SIZES)[number];
// where to start for the room it has (the measuring corrects it either way)
function readingStart(room: number): ReadingSize {
  if (room >= 680) return 'stage';
  if (room >= 560) return 'sheet';
  return 'compact';
}

function RowReading({ post, feeder, height }: { post: StreamPost; feeder: StreamFeeder | null; height: number }) {
  const boxRef = useRef<HTMLDivElement | null>(null);
  const [reading, setReading] = useState<{ height: number; size: ReadingSize }>(() => ({ height, size: readingStart(height) }));
  if (reading.height !== height) setReading({ height, size: readingStart(height) });
  useIsoLayoutEffect(() => {
    const box = boxRef.current;
    const content = box?.firstElementChild as HTMLElement | null;
    if (!box || !content) return;
    const at = READING_SIZES.indexOf(reading.size);
    if (content.offsetHeight > box.clientHeight && at < READING_SIZES.length - 1) setReading({ height, size: READING_SIZES[at + 1] });
  }, [height, reading.size, post.key, post.caption]);
  return (
    <div ref={boxRef} className="flex h-full min-w-0 items-center overflow-hidden py-1">
      <div className="w-full min-w-0 max-w-[520px]">
        <PostReading post={post} feeder={feeder} size={reading.size} />
      </div>
    </div>
  );
}

/* ── the cell ──────────────────────────────────────────────── */

export type PostCellProps = {
  cellKey: string;
  // the post's place across views (day:slot)
  zid: string | null;
  post: StreamPost;
  mode: FeedMode;
  // its box in this view (plain values, so a cell whose box didn't change never draws again)
  x: number;
  y: number;
  w: number;
  h: number;
  cols: number;
  rows: number;
  // a row (the wide Day view): the media frame's width, at its left
  mediaWidth: number;
  wide: boolean;
  // a move out of a view: the view being left and the post's box in it (its chrome drawn there, going)
  leaving: FeedMode | null;
  leavingShape: CellShape | null;
  // a move into this view: its chrome comes in a piece at a time
  arriving: boolean;
  // the post a move centres on: above the rest while it moves
  focal: boolean;
  // the Day view's post in view: its preview plays, its slides swipe
  active: boolean;
  // the wide Day view's post on the focus line: lit at once (the rest of the column dimmed), its reading in (spot) a
  // moment after a move lands
  lit: boolean;
  spot: boolean;
  standout: boolean;
  // a phone card that opens its day: the day (it wears the date)
  dayStart: string | null;
  eager: boolean;
  feeder: StreamFeeder | null;
  // the old view's leftover during a move (hidden unless the move lets it go from where it was)
  gone?: boolean;
  onOpenTile: (post: StreamPost, element: HTMLElement) => void;
  onOpenCard: (post: StreamPost) => void;
};

const KIND: Record<StreamMediaType, string> = { reel: 'Reel', carousel: 'Carousel', image: 'Post', unknown: 'Post' };
const FOCUS = 'outline-none [-webkit-tap-highlight-color:transparent] focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]';

function PostCell({
  cellKey,
  zid,
  post,
  mode,
  x,
  y,
  w,
  h,
  cols,
  rows,
  mediaWidth,
  wide,
  leaving,
  leavingShape,
  arriving,
  focal,
  active,
  lit,
  spot,
  standout,
  dayStart,
  eager,
  feeder,
  gone = false,
  onOpenTile,
  onOpenCard,
}: PostCellProps) {
  const shape: CellShape = { w, h, cols, rows, mediaWidth, wide };
  const dayParts = dayStart ? groupParts(dayStart, 'day', istDay()) : null;
  const grid = mode !== 'today';
  const card = mode === 'today' && !shape.wide;
  const row = mode === 'today' && shape.wide;
  const frame = frameOf(mode, shape);
  const winner = isWinner(post.topPercent);
  // what of the view being left goes, drawn as it was
  const was = leaving && leavingShape ? { grid: leaving !== 'today', card: leaving === 'today' && !leavingShape.wide, row: leaving === 'today' && leavingShape.wide } : null;
  const chipsOut = was?.grid && !grid;
  const cardOut = was?.card && !card;
  const rowOut = was?.row && !row;
  const bg = card || cardOut;

  const style: CSSProperties = { left: x, top: y, width: shape.w, height: shape.h };
  const top = post.topPercent;
  const measured = top != null && Number.isFinite(top);
  const label = `${KIND[post.mediaType]} by @${post.handle}, ${measured ? `top ${formatTopPercent(top)}${post.latestCheckpoint ? ` at ${post.latestCheckpoint}` : ''}` : 'not measured yet'}`;
  const openCard = () => onOpenCard(post);
  const onKey = (event: ReactKeyboardEvent<HTMLElement>) => {
    if (event.target !== event.currentTarget || (event.key !== 'Enter' && event.key !== ' ')) return;
    event.preventDefault();
    openCard();
  };

  return (
    <div
      data-cell={cellKey}
      data-zid={zid ?? undefined}
      data-post-key={post.key}
      data-kind="post"
      data-focal={focal ? '' : undefined}
      data-lit={lit ? '' : undefined}
      data-spot={spot ? '' : undefined}
      data-arriving={arriving ? '' : undefined}
      data-gone={gone ? '' : undefined}
      // a phone's card opens its details from anywhere on it (its chrome lets touches through, to a carousel's slides)
      role={card ? 'button' : undefined}
      tabIndex={card ? 0 : undefined}
      aria-label={card ? `${label}. Open details` : undefined}
      onClick={card ? openCard : undefined}
      onKeyDown={card ? onKey : undefined}
      className={cn('st-cell', row && 'st-row', card && cn('cursor-pointer rounded-[28px]', FOCUS))}
      style={style}
    >
      {bg ? (
        <div data-layer="bg" className="overflow-hidden rounded-[28px] bg-[var(--st-surface)]" style={{ left: 0, top: 0, width: shape.w, height: shape.h }}>
          <Wash src={post.thumbnailUrl} />
        </div>
      ) : null}

      <div
        data-layer="frame"
        className={cn('group overflow-hidden bg-[var(--st-surface)] [contain:strict]', !grid && 'isolate')}
        style={{ left: 0, top: 0, width: frame.width, height: frame.height, borderRadius: frame.radius }}
        {...(grid ? press : null)}
      >
        {/* a row's frame shows a picture wider than 4:5 whole, over its own wash */}
        {row ? <Wash src={post.thumbnailUrl} /> : null}
        {/* the picture gives under the finger (inside the frame: the moves own the frame's transform), and never while
            the finger scrolls the page (press.ts) */}
        <span className={cn('absolute inset-0 block transition-transform duration-200 ease-out', grid ? 'group-data-[pressed]:scale-[0.97]' : null)}>
          <PostPicture post={post} active={active && !grid} eager={eager} framed={!grid} controls={card ? 'top' : 'bottom'} />
        </span>
        {grid ? (
          <span data-layer="chips" className="pointer-events-none absolute inset-0 block">
            <GridChips post={post} cols={shape.cols} rows={shape.rows} width={shape.w} feeder={feeder} />
          </span>
        ) : null}
        {row ? <Marks post={post} standout={standout} dayStart={null} /> : null}
        {winner && !card ? (
          <span aria-hidden="true" className={cn('pointer-events-none absolute inset-0 z-[4] shadow-[inset_0_0_0_1.5px_var(--fm-accent-bright)]', grid ? 'rounded-[6px]' : 'rounded-[28px]')} />
        ) : null}
        {/* the wide Day view's column out of the spotlight: dimmed */}
        {row ? <span aria-hidden="true" className="st-dim pointer-events-none absolute inset-0 z-[5] bg-black" /> : null}
        {grid ? (
          <button
            type="button"
            aria-label={label}
            onClick={(event) => onOpenTile(post, event.currentTarget)}
            className={cn('absolute inset-0 z-[6] block rounded-[inherit] bg-transparent p-0', FOCUS)}
          />
        ) : null}
      </div>

      {card ? (
        <div data-layer="chrome" className="pointer-events-none rounded-[28px]" style={{ left: 0, top: 0, width: shape.w, height: shape.h }}>
          <CardChrome post={post} feeder={feeder} width={shape.w} height={shape.h} standout={standout} dayStart={dayParts} />
        </div>
      ) : null}

      {row ? (
        <div data-layer="reading" data-reading="" style={{ left: shape.mediaWidth + READING_GAP, top: 0, width: Math.max(0, shape.w - shape.mediaWidth - READING_GAP), height: shape.h }}>
          <RowReading post={post} feeder={feeder} height={shape.h} />
        </div>
      ) : null}

      {/* the view being left: its chrome as it was, for the move to hold where it stood while it goes */}
      {chipsOut && leavingShape ? (
        <div data-layer="chips-out" className="pointer-events-none" style={{ left: 0, top: 0, width: leavingShape.w, height: leavingShape.h }}>
          <GridChips post={post} cols={leavingShape.cols} rows={leavingShape.rows} width={leavingShape.w} feeder={feeder} />
        </div>
      ) : null}
      {cardOut && leavingShape ? (
        <div data-layer="chrome-out" className="pointer-events-none" style={{ left: 0, top: 0, width: leavingShape.w, height: leavingShape.h }}>
          <CardChrome post={post} feeder={feeder} width={leavingShape.w} height={leavingShape.h} standout={standout} dayStart={dayParts} />
        </div>
      ) : null}
      {rowOut && leavingShape ? (
        <div
          data-layer="reading-out"
          className="pointer-events-none"
          style={{ left: 0, top: 0, width: Math.max(0, leavingShape.w - leavingShape.mediaWidth - READING_GAP), height: leavingShape.h }}
        >
          <RowReading post={post} feeder={feeder} height={leavingShape.h} />
        </div>
      ) : null}
    </div>
  );
}

export default memo(PostCell);

/* ── a slot whose day hasn't loaded: a still block where the post will be ── */

export const PlaceholderCell = memo(function PlaceholderCell({ cellKey, mode, x, y, w, h, mediaWidth, wide, gone = false }: {
  cellKey: string;
  mode: FeedMode;
  x: number;
  y: number;
  w: number;
  h: number;
  mediaWidth: number;
  wide: boolean;
  gone?: boolean;
}) {
  const shape: CellShape = { w, h, cols: 1, rows: 1, mediaWidth, wide };
  const frame = frameOf(mode, shape);
  const style: CSSProperties = { left: x, top: y, width: shape.w, height: shape.h };
  return (
    <div data-cell={cellKey} data-kind="placeholder" data-gone={gone ? '' : undefined} aria-hidden="true" className="st-cell" style={style}>
      {mode === 'today' && !shape.wide ? <div className="st-placeholder absolute inset-0 rounded-[28px]" /> : null}
      <div data-layer="frame" className="st-placeholder" style={{ left: 0, top: 0, width: frame.width, height: frame.height, borderRadius: frame.radius }} />
    </div>
  );
});
