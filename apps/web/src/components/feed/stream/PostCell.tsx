'use client';

/* ─────────────────────────────────────────────────────────────
   POST CELL — one post on the Feed, the same element in every
   view (WEEK, MONTH and the Day view), so a move between views
   carries the post itself, its picture drawn once.

   It is a box at its place (FeedStream lays it out; nothing here
   moves it) holding layers the moves can take hold of:
     frame   — the picture's box: a grid's whole tile, a phone
               card's whole 4:5 (a tablet's across its top), a
               row's 4:5 at its left. Under the picture it is the
               media's dark ground in both themes (never the page's
               light surface), and in the Day view the picture's
               wash, which a landscape picture shows whole over;
     bg      — a tablet card's surface and wash of its picture,
               below its frame (a phone's card is all frame);
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

   A card (a phone's or a tablet's Day view) reads on itself: one
   tap anywhere on it lifts its foot into the whole reading (the
   rest coming up from under it, the picture's top left clear),
   the next tap puts it back, from wherever it is mid-way. A reel
   with a preview clip, and a carousel on a video slide, has a ▶
   that clears the reading off the card while the clip plays; a
   carousel clears it as its slides are swiped, until a tap or its
   first slide again. A phone's card also stands forward or sits
   back with the scroll (depth: stream.css, st-depth), and the
   marks over its top step down clear of the header while that is
   open over it (stream.css, data-head-open).
   ───────────────────────────────────────────────────────────── */

import { Fragment, memo, useCallback, useEffect, useLayoutEffect, useRef, useState, type CSSProperties, type MouseEvent as ReactMouseEvent, type RefObject } from 'react';
import { ArrowUpRight, ChevronUp, Pause, Play } from 'lucide-react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import CheckpointTrail from '@/components/feed/stream/CheckpointTrail';
import { PostPicture, Wash } from '@/components/feed/stream/PostMedia';
import PostReading, { formatRate, postedLabel } from '@/components/feed/stream/PostReading';
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
  withinDay,
} from '@/lib/feedStream/contract';
import { titleCase } from '@/lib/feedLabels';
import { POST_ASPECT, READING_GAP } from '@/lib/feedStream/layout';
import { CARRY_EASE_CSS, reducedMotion, releaseFlight, run, startFlight } from '@/lib/feedStream/motion';
import { ms } from '@/lib/tempo';
import { FLIGHT_HOLD_MS, afterDrawn, holdsFlights } from '@/lib/webkitMotion';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

const useIsoLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

// type over a picture: the scrim does the work, a soft shadow keeps it crisp on the brightest spots
const ON_PICTURE = '[text-shadow:0_1px_2px_rgb(0_0_0/0.35)]';
// something new (the last 24 hours) on a card: its words in a red pill (red is never a pick)
const FRESH_PILL = 'rounded-full bg-[var(--fm-accent)] px-2 py-[3px] text-[11px] font-bold leading-none text-white [text-shadow:none]';

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
   and @handle top-left. The number is sized by the tile's width, white on the picture; a winner wears a white ring. A reel has a small ▶. */

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
          <span data-st-fade="" aria-hidden="true" className="st-scrim-top pointer-events-none absolute inset-x-0 top-0 h-[34%]" />
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
          <span data-st-fade="" aria-hidden="true" className={cn('st-scrim-soft pointer-events-none absolute inset-x-0 bottom-0', SCRIM[shape])} />
          <span data-st="" className={cn('pointer-events-none absolute flex flex-col items-start', ON_PICTURE)} style={{ left: inset, bottom: inset - 1 }}>
            {kicker ? <span className="mb-1.5 text-[10px] font-black uppercase leading-none tracking-[0.14em] text-white/75">{kicker}</span> : null}
            {/* past the face's heaviest cut (700): a hairline stroke in its own colour, painted under the fill */}
            <span
              className={cn(
                'flex items-baseline font-black leading-[0.82] tracking-[-0.03em] tabular-nums [-webkit-text-stroke:0.045em_currentColor] [paint-order:stroke_fill]',
                // white on every picture (an accent number over a bright photo reads ~2:1); a winner is marked by its ring
                'text-white',
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

function standoutText(post: StreamPost, today: string) {
  if (post.day === today || post.resultDay === today) return "Today's standout";
  const yesterday = shiftDay(today, -1);
  if (post.day === yesterday || post.resultDay === yesterday) return "Yesterday's standout";
  return 'Standout';
}

// a phone card's tab: what the post is and the day it went up (every card wears one)
function Marks({ post, standout, date }: { post: StreamPost; standout: boolean; date: { kicker: string; title: string } | null }) {
  if (!standout && !date) return null;
  return (
    <span className="st-card-marks pointer-events-none absolute left-4 top-4 z-[4] flex flex-col gap-2">
      {date ? (
        <span data-st="" className="inline-flex w-fit flex-col rounded-[14px] bg-black/55 px-3 pb-2.5 pt-2 shadow-[inset_0_0_0_1px_rgba(255,255,255,0.1)]" style={{ '--i': 0 } as CSSProperties}>
          <span className="text-[10px] font-black uppercase leading-none tracking-[0.22em] text-white/75">{KIND[post.mediaType]} · {date.kicker}</span>
          <span className="mt-1.5 text-[22px] font-black leading-none tracking-[-0.04em] text-white">{date.title}</span>
        </span>
      ) : null}
      {standout ? (
        <span data-st="" className="inline-flex w-fit items-center gap-1.5 rounded-[6px] bg-black/55 px-2 py-[5px] text-[10px] font-black uppercase leading-none tracking-[0.14em] text-white" style={{ '--i': 1 } as CSSProperties}>
          <span aria-hidden="true" className="h-1.5 w-1.5 shrink-0 rounded-full bg-[var(--fm-accent)]" />
          {standoutText(post, istDay())}
        </span>
      ) : null}
    </span>
  );
}

// the reading's pieces a live card moves itself (PostCell, setRead)
type ReadParts = {
  read: RefObject<HTMLSpanElement | null>;
  inner: RefObject<HTMLSpanElement | null>;
  more: RefObject<HTMLSpanElement | null>;
  shade: RefObject<HTMLSpanElement | null>;
  toggle: RefObject<HTMLButtonElement | null>;
  ring: RefObject<SVGCircleElement | null>;
};

// the play button's ring (r 20.5): its length, run round as the clip plays
const RING = 2 * Math.PI * 20.5;

/* A card's chrome. Live (the card on the page, not the copy a move out of the Day view lets go): the rest of its
   reading waits under its foot for a tap, a ▶ sits on a reel that has a clip, and the reading clears its foot of the
   folded nav (clear) */
function CardChrome({ post, feeder, width, height, standout, date, live = false, clear = 0, parts, playable = false, playSlides = false, playOff = false, watching = false, onToggle, onPlay }: {
  post: StreamPost;
  feeder: StreamFeeder | null;
  width: number;
  height: number;
  standout: boolean;
  date: { kicker: string; title: string } | null;
  live?: boolean;
  clear?: number;
  parts?: ReadParts;
  playable?: boolean;
  // a carousel's ▶: under its count, and away while its slide is a picture
  playSlides?: boolean;
  playOff?: boolean;
  // its preview playing (its ▶ now a pause)
  watching?: boolean;
  onToggle?: () => void;
  onPlay?: () => void;
}) {
  const roomy = height >= ROOMY_MIN;
  const face = feeder ?? { handle: post.handle, profilePicUrl: null };
  const feedName = feeder?.feedTitle ? titleCase(feeder.feedTitle) : null;
  // posted in the last 24 hours: its age is marked red (something new)
  const fresh = withinDay(post.postedAt);
  // its numbers, as figures: each a number over what it counts
  const figures = [
    post.likes != null ? { key: 'likes', value: formatCount(post.likes), label: post.likes === 1 ? 'like' : 'likes' } : null,
    post.comments != null ? { key: 'comments', value: formatCount(post.comments), label: post.comments === 1 ? 'comment' : 'comments' } : null,
    post.engagementRate != null ? { key: 'er', value: formatRate(post.engagementRate), label: 'engagement' } : null,
    post.multiple != null ? { key: 'usual', value: formatMultiple(post.multiple), label: 'vs usual' } : null,
  ].filter((figure): figure is { key: string; value: string; label: string } => figure != null);
  const when = [KIND[post.mediaType], postedLabel(post.postedAt)].filter(Boolean).join(' · ');
  // a tap on these is theirs, not the card's
  const own = (action?: () => void) => (event: ReactMouseEvent) => {
    event.stopPropagation();
    action?.();
  };
  return (
    <>
      {/* the scrim only covers the foot (who, and the number): the rest of the picture stays bright */}
      <span data-st-fade="" aria-hidden="true" className="st-scrim st-card-scrim pointer-events-none absolute inset-x-0 bottom-0 h-[40%] rounded-b-[28px]" />
      {/* the open reading's fade, risen with it (its top set as it opens) */}
      {live ? <span ref={parts?.shade} aria-hidden="true" className="st-read-shade pointer-events-none absolute inset-0" /> : null}
      <Marks post={post} standout={standout} date={date} />
      {live && playable ? (
        <button
          type="button"
          onClick={own(onPlay)}
          aria-label={watching ? 'Pause the preview' : 'Play the preview'}
          aria-pressed={watching}
          data-off={playOff ? '' : undefined}
          inert={playOff}
          className={cn(
            'st-play pointer-events-auto absolute right-3 z-[5] grid h-11 w-11 place-items-center rounded-full bg-black/45 text-white shadow-[inset_0_0_0_1px_rgba(255,255,255,0.2)] outline-none [-webkit-tap-highlight-color:transparent] active:scale-[0.92] focus-visible:ring-2 focus-visible:ring-white/80',
            playSlides ? 'top-[39px]' : 'top-3',
          )}
        >
          <svg aria-hidden="true" viewBox="0 0 44 44" className="pointer-events-none absolute inset-0 h-11 w-11 -rotate-90">
            <circle ref={parts?.ring} className="st-play-ring" cx="22" cy="22" r="20.5" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeDasharray={RING} strokeDashoffset={RING} />
          </svg>
          <Play aria-hidden="true" className="st-play-on ml-0.5 h-[18px] w-[18px]" fill="currentColor" strokeWidth={0} />
          <Pause aria-hidden="true" className="st-play-off h-[18px] w-[18px]" fill="currentColor" strokeWidth={0} />
        </button>
      ) : null}
      <span
        ref={parts?.read}
        className="st-read st-on-media absolute inset-x-0 bottom-0 block px-4 pt-4 pb-[calc(16px+var(--st-clear,0px))] sm:px-5 sm:pt-5 sm:pb-[calc(20px+var(--st-clear,0px))]"
        style={{ '--st-clear': `${clear}px` } as CSSProperties}
      >
        <span ref={parts?.inner} className="st-read-in relative block" style={{ maxWidth: Math.min(width, 460) }}>
          <span className={cn('flex min-w-0 flex-col', roomy ? 'gap-3' : 'gap-2.5')}>
            <span data-st="" className={cn('flex min-w-0 items-center gap-2.5', ON_PICTURE)} style={{ '--i': 0 } as CSSProperties}>
              <span className="relative h-8 w-8 shrink-0 overflow-hidden rounded-full shadow-[0_0_0_1px_rgba(255,255,255,0.22)]">
                <FeederStoryAvatar feeder={face} className="text-[12px]" />
              </span>
              <span className="min-w-0 flex-1">
                <span className="block truncate text-[14px] font-bold leading-tight text-white">@{post.handle}</span>
                <span className="mt-0.5 flex min-w-0 items-center gap-1.5 text-[12px] font-medium leading-tight text-white/65">
                  {feedName ? <span className="truncate">{feedName}</span> : null}
                  {feedName ? <span aria-hidden="true">·</span> : null}
                  <span className={cn('shrink-0', fresh && FRESH_PILL)}>{relativeTime(post.postedAt)}</span>
                </span>
              </span>
              {live ? (
                <button
                  ref={parts?.toggle}
                  type="button"
                  onClick={own(onToggle)}
                  aria-expanded="false"
                  aria-label="Show the details"
                  className="st-read-toggle pointer-events-auto grid h-8 w-8 shrink-0 place-items-center rounded-full bg-white/[0.14] text-white shadow-[inset_0_0_0_1px_rgba(255,255,255,0.16)] outline-none [-webkit-tap-highlight-color:transparent] [text-shadow:none] active:bg-white/25 focus-visible:ring-2 focus-visible:ring-white/80"
                >
                  <ChevronUp aria-hidden="true" className="h-4 w-4" strokeWidth={2.6} />
                </button>
              ) : null}
            </span>
            <span data-st="" className="flex min-w-0 items-end" style={{ '--i': 1 } as CSSProperties}>
              <TopBadge size={roomy ? 'xl' : 'lg'} topPercent={post.topPercent} checkpoint={post.latestCheckpoint} />
            </span>
          </span>
          {/* the rest of the reading, under the foot (the card itself stays minimal: who, and the number): the four reads,
              the counts as figures, the caption whole, when, the way out */}
          {live ? (
            <span ref={parts?.more} className="st-read-more flex flex-col gap-3.5">
              {height >= TRAIL_MIN ? (
                <span className="block">
                  <CheckpointTrail results={post.results} latest={post.latestCheckpoint} postedDay={post.day} size="lg" />
                </span>
              ) : null}
              {figures.length ? (
                // (each as wide as it needs, spread across the card, a hairline centred between each two)
                <span className="flex min-w-0 items-stretch justify-between gap-2">
                  {figures.map((figure, index) => (
                    <Fragment key={figure.key}>
                      {index ? <span aria-hidden="true" className="w-px shrink-0 bg-white/[0.16]" /> : null}
                      <span data-figure="" className={cn('flex min-w-0 flex-col', ON_PICTURE)}>
                        <span className="whitespace-nowrap text-[19px] font-black leading-[0.9] tracking-[-0.04em] tabular-nums text-white">{figure.value}</span>
                        <span className="mt-1.5 whitespace-nowrap text-[11px] font-medium leading-none text-white/60">{figure.label}</span>
                      </span>
                    </Fragment>
                  ))}
                </span>
              ) : null}
              {post.caption ? (
                <span className={cn('block whitespace-pre-line break-words text-[14px] font-medium leading-[1.5] text-white/[0.88]', ON_PICTURE)}>{post.caption}</span>
              ) : null}
              <span className="flex flex-wrap items-center justify-between gap-x-3 gap-y-2.5">
                {when ? <span className={cn('text-[12px] font-medium leading-tight text-white/60', ON_PICTURE)}>{when}</span> : null}
                {post.url ? (
                  <a
                    href={post.url}
                    target="_blank"
                    rel="noopener noreferrer"
                    onClick={own()}
                    className="group/ig inline-flex h-10 shrink-0 items-center gap-2 rounded-full bg-white pl-4 pr-3.5 text-[13px] font-bold text-[#0b0b0f] outline-none transition-transform duration-150 ease-out [-webkit-tap-highlight-color:transparent] active:scale-[0.97] focus-visible:ring-2 focus-visible:ring-white/80"
                  >
                    Open on Instagram
                    <ArrowUpRight aria-hidden="true" className="h-4 w-4 transition-transform duration-200 ease-out group-hover/ig:-translate-y-px group-hover/ig:translate-x-px" strokeWidth={2.4} />
                  </a>
                ) : null}
              </span>
            </span>
          ) : null}
        </span>
      </span>
      {/* a winner (top 10%): a white ring */}
      {isWinner(post.topPercent) ? (
        <span data-st-fade="" aria-hidden="true" className="pointer-events-none absolute inset-0 z-[4] rounded-[28px] shadow-[inset_0_0_0_1.5px_rgb(255_255_255/0.8)]" />
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
  // a phone card: the day it sits under (it wears the date)
  date: string | null;
  eager: boolean;
  feeder: StreamFeeder | null;
  // the old view's leftover during a move (hidden unless the move lets it go from where it was)
  gone?: boolean;
  // a phone's Day view, not mid-move: the card stands forward or sits back with the scroll (the first post rests lower)
  depth?: 'first' | 'on' | null;
  // the Day view's first card (it rests under the order tabs, clear of the header however it stands)
  first?: boolean;
  // a phone's card: how far its reading is lifted to clear the folded nav (a short screen)
  clear?: number;
  onOpenTile: (post: StreamPost, element: HTMLElement) => void;
  // a tap on a card that isn't the one in view: the page brings it there
  onFocusCard: (post: StreamPost) => void;
};

const KIND: Record<StreamMediaType, string> = { reel: 'Reel', carousel: 'Carousel', image: 'Post', unknown: 'Post' };
const FOCUS = 'outline-none [-webkit-tap-highlight-color:transparent] focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]';

// a card's reading opening and closing: one even glide each way (the carry's curve), started on the tap and turned
// round from wherever it is by the next. Its rest comes in a line at a time as it rises, and fades as it sinks
const READ_OPEN_MS = ms(460);
const READ_CLOSE_MS = ms(440);
const READ_LINE_MS = ms(380);
const READ_LINE_AT_MS = ms(110);
const READ_LINE_STEP_MS = ms(60);
const READ_LINE_EASE = 'cubic-bezier(0.16, 0.9, 0.2, 1)';
const READ_FADE_MS = ms(300);
const READ_FADE_EASE = 'cubic-bezier(0.3, 0, 0.4, 1)';
// inside a line: the trail's reads and the figures come in one after another, a beat into their line
const READ_PIECE_MS = ms(340);
const READ_PIECE_AT_MS = ms(50);
const READ_PIECE_STEP_MS = ms(45);
// the open reading's fade starts this far above its first line
const SHADE_LEAD = 84;
// the open reading leaves at least this much of the picture's top clear (past that, its rest scrolls)
const READ_TOP_ROOM = 56;
// a reading open as its card clears is shut once it has slid off (stream.css, .st-read in [data-clean])
const CLEAR_MS = ms(420);

type Clean = 'watch' | 'slides' | null;

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
  date,
  eager,
  feeder,
  gone = false,
  depth = null,
  first = false,
  clear = 0,
  onOpenTile,
  onFocusCard,
}: PostCellProps) {
  const shape: CellShape = { w, h, cols, rows, mediaWidth, wide };
  const dateParts = date ? groupParts(date, 'day', istDay()) : null;
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
  // a card whose frame is all of it (a phone's) has no surface of its own to show
  const fullFrame = (cardShape: CellShape) => frameOf('today', cardShape).height >= cardShape.h - 0.5;
  const bg = card ? !fullFrame(shape) : Boolean(cardOut && leavingShape && !fullFrame(leavingShape));

  const style: CSSProperties = { left: x, top: y, width: shape.w, height: shape.h };
  const top = post.topPercent;
  const measured = top != null && Number.isFinite(top);
  const label = `${KIND[post.mediaType]} by @${post.handle}, ${measured ? `top ${formatTopPercent(top)}${post.latestCheckpoint ? ` at ${post.latestCheckpoint}` : ''}` : 'not measured yet'}`;

  /* ── a card's reading: open or shut, moved from wherever it is (a tap mid-way turns it round) ── */
  const cellRef = useRef<HTMLDivElement | null>(null);
  const readRef = useRef<HTMLSpanElement | null>(null);
  const innerRef = useRef<HTMLSpanElement | null>(null);
  const moreRef = useRef<HTMLSpanElement | null>(null);
  const shadeRef = useRef<HTMLSpanElement | null>(null);
  const toggleRef = useRef<HTMLButtonElement | null>(null);
  const ringRef = useRef<SVGCircleElement | null>(null);
  const openRef = useRef(false);
  const setRead = useCallback((open: boolean, animate = true) => {
    const cell = cellRef.current;
    const read = readRef;
    const more = moreRef;
    const shade = shadeRef;
    const toggle = toggleRef;
    const block = innerRef.current;
    if (!cell || !read.current || !block || openRef.current === open) return;
    openRef.current = open;
    // where it is on screen now (mid-way, if it is moving), then where it belongs: it goes from the one to the other
    const before = block.getBoundingClientRect().top;
    const shadeAt = shade.current ? Number(getComputedStyle(shade.current).opacity) : 0;
    const moreAt = more.current ? Number(getComputedStyle(more.current).opacity) : 0;
    const lines = more.current ? (Array.from(more.current.children) as HTMLElement[]) : [];
    const pieces = more.current ? Array.from(more.current.querySelectorAll<HTMLElement>('[data-trail-seg], [data-figure]')) : [];
    [block, shade.current, more.current, ...lines, ...pieces].forEach((el) => el?.getAnimations().forEach((animation) => animation.cancel()));
    // opening: the rest gets the room the card has left under the foot (past it, it scrolls), from its top
    if (open && more.current) {
      const pad = getComputedStyle(read.current);
      const foot = (block.firstElementChild as HTMLElement | null)?.offsetHeight ?? 0;
      const room = cell.clientHeight - READ_TOP_ROOM - parseFloat(pad.paddingTop) - parseFloat(pad.paddingBottom) - foot - 16;
      more.current.style.setProperty('--st-more-max', `${Math.max(96, Math.floor(room))}px`);
      more.current.scrollTop = 0;
    }
    if (open) cell.dataset.read = '';
    else delete cell.dataset.read;
    if (more.current) more.current.inert = !open;
    toggle.current?.setAttribute('aria-expanded', String(open));
    toggle.current?.setAttribute('aria-label', open ? 'Hide the details' : 'Show the details');
    if (open && more.current) more.current.toggleAttribute('data-scrolls', more.current.scrollHeight > more.current.clientHeight + 1);
    const after = block.getBoundingClientRect().top;
    // the fade's top sits a little over the reading's, open or shut, so it goes up and comes down with it. Placed by
    // translate, never by top: it keeps the card's height whole, its foot covered all the way (shut, its top used to
    // stay up there, and the glide down began with it lifted a whole reading higher: dark over the picture's top)
    if (shade.current) shade.current.style.translate = `0px ${Math.max(0, after - cell.getBoundingClientRect().top - SHADE_LEAD).toFixed(1)}px`;
    const dy = before - after;
    if (!animate || reducedMotion() || Math.abs(dy) < 0.5) return;
    // (WebKit starts a glide's clock before the frame that draws it is on screen: held at its start for that frame, it
    // is seen whole)
    const flight = startFlight(holdsFlights() ? FLIGHT_HOLD_MS : 0);
    const glide: KeyframeAnimationOptions = { duration: open ? READ_OPEN_MS : READ_CLOSE_MS, easing: CARRY_EASE_CSS, fill: 'backwards' };
    const from = `translate3d(0px, ${dy.toFixed(1)}px, 0px)`;
    run(flight, block, [{ transform: from }, { transform: 'translate3d(0px, 0px, 0px)' }], glide);
    if (shade.current) run(flight, shade.current, [{ opacity: shadeAt, transform: from }, { opacity: open ? 1 : 0, transform: 'translate3d(0px, 0px, 0px)' }], glide);
    if (open && moreAt > 0.05) {
      // (a close turned round mid-way: it is all there already, it only rises back)
      lines.forEach((line) => run(flight, line, [{ opacity: moreAt }, { opacity: 1 }], { duration: READ_LINE_MS, easing: READ_LINE_EASE, fill: 'backwards' }));
    } else if (open) {
      // a line at a time under the foot as it rises; in the trail's line and the figures' line, their pieces one after
      // another a beat into it
      lines.forEach((line, index) => {
        const at = READ_LINE_AT_MS + index * READ_LINE_STEP_MS;
        const own = Array.from(line.querySelectorAll<HTMLElement>('[data-trail-seg], [data-figure]'));
        const rise = [{ transform: 'translate3d(0px, 12px, 0px)' }, { transform: 'translate3d(0px, 0px, 0px)' }];
        if (!own.length) {
          run(flight, line, [{ ...rise[0], opacity: 0 }, { ...rise[1], opacity: 1 }], { duration: READ_LINE_MS, delay: at, easing: READ_LINE_EASE, fill: 'backwards' });
          return;
        }
        run(flight, line, rise, { duration: READ_LINE_MS, delay: at, easing: READ_LINE_EASE, fill: 'backwards' });
        own.forEach((piece, step) => run(
          flight,
          piece,
          piece.dataset.figure !== undefined
            ? [{ opacity: 0, transform: 'translate3d(0px, 8px, 0px)' }, { opacity: 1, transform: 'translate3d(0px, 0px, 0px)' }]
            : [{ opacity: 0, transform: 'translate3d(-10px, 0px, 0px)' }, { opacity: 1, transform: 'translate3d(0px, 0px, 0px)' }],
          { duration: READ_PIECE_MS, delay: at + READ_PIECE_AT_MS + step * READ_PIECE_STEP_MS, easing: READ_LINE_EASE, fill: 'backwards' },
        ));
      });
    } else if (more.current) {
      run(flight, more.current, [{ opacity: moreAt }, { opacity: 0 }], { duration: READ_FADE_MS, easing: READ_FADE_EASE, fill: 'backwards' });
    }
    if (flight.hold) afterDrawn(() => releaseFlight(flight));
  }, []);

  /* ── cleared: a clip playing (its ▶), or a carousel's slides past the first ── */
  const [clean, setClean] = useState<Clean>(null);
  // the slide a card's carousel shows: one with video slides has its ▶ on those
  const [slideAt, setSlideAt] = useState(0);
  const slideClips = card && post.slides.length > 1 && post.slides.some((slide) => slide.video);
  const playable = card && ((post.mediaType === 'reel' && Boolean(post.previewUrl) && post.slides.length <= 1) || slideClips);
  const playOff = slideClips && !post.slides[slideAt]?.video;
  // what a card is once its clip stops: cleared past a carousel's first slide, itself on it
  const rest: Clean = slideAt > 0 ? 'slides' : null;
  // a reading open as the card clears goes down with the rest, and is shut once it is out of sight
  const shutWhenClear = useCallback(() => {
    if (!openRef.current) return;
    window.setTimeout(() => {
      if (cellRef.current?.dataset.clean) setRead(false, false);
    }, CLEAR_MS);
  }, [setRead]);
  const togglePlay = useCallback(() => {
    // (a card peeking under the one in view is brought into view first)
    if (!active) {
      onFocusCard(post);
      return;
    }
    setClean((current) => (current === 'watch' ? rest : 'watch'));
    shutWhenClear();
  }, [active, onFocusCard, post, shutWhenClear, rest, setClean]);
  const onProgress = useCallback((fraction: number) => {
    const ring = ringRef.current;
    if (ring) ring.style.strokeDashoffset = String(RING * (1 - Math.min(1, Math.max(0, fraction))));
  }, []);
  const onPlayFail = useCallback(() => setClean((current) => (current === 'watch' ? rest : current)), [rest, setClean]);
  // a swipe stops a slide's clip too: cleared past the first slide, itself again on it
  const onSlide = useCallback((index: number) => {
    if (index > 0) shutWhenClear();
    setSlideAt(index);
    setClean(index > 0 ? 'slides' : null);
  }, [shutWhenClear, setClean]);
  // the ring empty again whenever the clip isn't playing
  useEffect(() => {
    const ring = ringRef.current;
    if (clean !== 'watch' && ring) ring.style.strokeDashoffset = String(RING);
  }, [clean]);

  // the card leaving view (a swipe on, a move): it is itself again, its reading shut
  const [wasActive, setWasActive] = useState(active);
  if (active !== wasActive) {
    setWasActive(active);
    if (!active) {
      setClean(null);
      setSlideAt(0);
    }
  }
  useEffect(() => {
    if (!active) setRead(false, card);
  }, [active, card, setRead]);
  // no longer a card (a move to a grid or a row): nothing of its reading stays on it
  useIsoLayoutEffect(() => {
    if (card) {
      const more = moreRef.current;
      if (more && !openRef.current) more.inert = true;
      return;
    }
    openRef.current = false;
    if (cellRef.current) delete cellRef.current.dataset.read;
  }, [card]);

  // a tap anywhere on a card: the one in view reads (or, cleared, comes back); another is brought into view
  const onCardTap = (event: ReactMouseEvent<HTMLDivElement>) => {
    if ((event.target as Element).closest('a, button')) return;
    if (!active) {
      onFocusCard(post);
      return;
    }
    if (clean) {
      setClean(null);
      return;
    }
    setRead(!openRef.current);
  };

  return (
    <div
      ref={cellRef}
      data-cell={cellKey}
      data-zid={zid ?? undefined}
      data-post-key={post.key}
      data-kind="post"
      data-focal={focal ? '' : undefined}
      data-card={card ? '' : undefined}
      data-lit={lit ? '' : undefined}
      data-spot={spot ? '' : undefined}
      data-arriving={arriving ? '' : undefined}
      data-gone={gone ? '' : undefined}
      data-depth={card && depth ? depth : undefined}
      data-first={card && first ? '' : undefined}
      data-clean={card && clean ? clean : undefined}
      // a card reads from anywhere on it (its chrome lets touches through, to a carousel's slides); the keys have its
      // arrow button
      onClick={card ? onCardTap : undefined}
      className={cn('st-cell', row && 'st-row', card && 'cursor-pointer rounded-[28px] [-webkit-tap-highlight-color:transparent]')}
      style={style}
    >
      {bg ? (
        <div data-layer="bg" className="overflow-hidden rounded-[28px] bg-[var(--st-surface)]" style={{ left: 0, top: 0, width: shape.w, height: shape.h }}>
          <Wash src={post.thumbnailUrl} />
        </div>
      ) : null}

      <div
        data-layer="frame"
        className={cn('group overflow-hidden [contain:strict]', grid ? 'bg-[var(--st-surface)]' : 'isolate bg-[var(--st-media)]')}
        style={{ left: 0, top: 0, width: frame.width, height: frame.height, borderRadius: frame.radius }}
        {...(grid ? press : null)}
      >
        {/* the Day view's frame shows a landscape picture whole, over its own wash */}
        {row || card ? <Wash src={post.thumbnailUrl} /> : null}
        {/* the picture gives under the finger (inside the frame: the moves own the frame's transform), and never while
            the finger scrolls the page (press.ts) */}
        <span className={cn('absolute inset-0 block transition-transform duration-200 ease-out', grid ? 'group-data-[pressed]:scale-[0.97]' : null)}>
          <PostPicture
            post={post}
            active={active && !grid}
            eager={eager}
            framed={!grid}
            controls={card ? 'top' : 'bottom'}
            // a card's preview plays on its ▶ (a row's plays as it comes into view)
            play={card ? clean === 'watch' : undefined}
            onProgress={card ? onProgress : undefined}
            onPlayFail={card ? onPlayFail : undefined}
            onSlide={card ? onSlide : undefined}
          />
        </span>
        {grid ? (
          <span data-layer="chips" className="pointer-events-none absolute inset-0 block">
            <GridChips post={post} cols={shape.cols} rows={shape.rows} width={shape.w} feeder={feeder} />
          </span>
        ) : null}
        {row ? <Marks post={post} standout={standout} date={null} /> : null}
        {/* a winner (top 10%): a white ring (what is new is marked on its feeder's circle, not on the post) */}
        {winner && !card ? (
          <span aria-hidden="true" className={cn('pointer-events-none absolute inset-0 z-[4] shadow-[inset_0_0_0_1.5px_rgb(255_255_255/0.8)]', grid ? 'rounded-[6px]' : 'rounded-[28px]')} />
        ) : null}
        {/* the wide Day view's column out of the spotlight: it recedes into the page (dimmed on dark, washed out on light) */}
        {row ? <span aria-hidden="true" className="st-dim pointer-events-none absolute inset-0 z-[5] bg-black light:bg-[var(--st-bg)]" /> : null}
        {/* a phone's card sitting back (st-depth-dim); kept in every view, so a move can let it go as it flies */}
        <span data-depth-dim="" aria-hidden="true" className="pointer-events-none absolute inset-0 z-[5] bg-black opacity-0" />
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
        <div data-layer="chrome" className="pointer-events-none overflow-hidden rounded-[28px]" style={{ left: 0, top: 0, width: shape.w, height: shape.h }}>
          <CardChrome
            post={post}
            feeder={feeder}
            width={shape.w}
            height={shape.h}
            standout={standout}
            date={dateParts}
            live
            clear={clear}
            parts={{ read: readRef, inner: innerRef, more: moreRef, shade: shadeRef, toggle: toggleRef, ring: ringRef }}
            playable={playable}
            playSlides={slideClips}
            playOff={playOff}
            watching={clean === 'watch'}
            onToggle={() => {
              if (!active) {
                onFocusCard(post);
                return;
              }
              // (a cleared card: its reading back, then open)
              if (clean) setClean(null);
              setRead(!openRef.current);
            }}
            onPlay={togglePlay}
          />
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
          <CardChrome post={post} feeder={feeder} width={leavingShape.w} height={leavingShape.h} standout={standout} date={dateParts} />
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
  const card = mode === 'today' && !shape.wide;
  const style: CSSProperties = { left: x, top: y, width: shape.w, height: shape.h };
  return (
    <div data-cell={cellKey} data-kind="placeholder" data-card={card ? '' : undefined} data-gone={gone ? '' : undefined} aria-hidden="true" className={cn('st-cell', card && 'rounded-[28px]')} style={style}>
      {card && frame.height < shape.h - 0.5 ? <div className="st-placeholder absolute inset-0 rounded-[28px]" /> : null}
      <div data-layer="frame" className="st-placeholder" style={{ left: 0, top: 0, width: frame.width, height: frame.height, borderRadius: frame.radius }} />
    </div>
  );
});
