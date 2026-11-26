'use client';

/* ─────────────────────────────────────────────────────────────
   POST CELL — one post on the Feed, the same element in every
   view (WEEK, MONTH and the Day view), so a move between views
   carries the post itself, its picture drawn once.

   It is a box at its place (FeedStream lays it out; nothing here
   moves it) holding layers the moves can take hold of:
     frame   — the picture's box: a grid's whole tile, a card's
               4:5 across its top, a row's 4:5 at its left. Under
               the picture it is the media's dark ground in both
               themes (never the page's light surface), and in the
               Day view the picture's wash, which a landscape
               picture shows whole over;
     bg      — a card's ground under its frame: the picture's own
               colour, taken deep (PostMedia, Tone), which the
               picture melts into;
     chrome  — what reads it: a tile's number (inside its frame,
               so it scales with it from one grid to the other),
               a card's reading (who, its lead, the rest of it on
               a tap), a row's reading beside it.
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
   that clears the picture of its melt while the clip plays, and a
   carousel clears it as its slides are swiped, until a tap or its
   first slide again (the reading stays under the picture; where it
   stands over it, on a short screen, it goes too). A phone's cards
   also stack as they scroll, each over the one before (depth:
   stream.css, st-yield).
   ───────────────────────────────────────────────────────────── */

import { memo, useCallback, useEffect, useLayoutEffect, useRef, useState, type CSSProperties, type MouseEvent as ReactMouseEvent, type RefObject, type TouchEvent as ReactTouchEvent } from 'react';
import { ArrowUpRight, ChevronUp, Pause, Play } from 'lucide-react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import CheckpointStrip, { LineSwap } from '@/components/feed/stream/CheckpointStrip';
import CheckpointTrail from '@/components/feed/stream/CheckpointTrail';
import { gainOf, moveOf, readAt, readLine, type PostRead } from '@/components/feed/stream/checkpointPick';
import { Bleed, PostPicture, Tone, Wash, useMediaRatio, type Fall } from '@/components/feed/stream/PostMedia';
import RollText from '@/components/feed/stream/RollText';
import PostReading, { formatRate, postedLabel } from '@/components/feed/stream/PostReading';
import { pageGliding, press } from '@/components/feed/stream/press';
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
  type StreamCheckpoint,
  type StreamFeeder,
  type StreamMediaType,
  type StreamPost,
  withinDay,
} from '@/lib/feedStream/contract';
import { POST_ASPECT, READING_GAP } from '@/lib/feedStream/layout';
import { CARRY_EASE_CSS, reducedMotion } from '@/lib/feedStream/motion';
import { ms } from '@/lib/tempo';
import { holdsFlights } from '@/lib/webkitMotion';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

const useIsoLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

// type over a picture: the scrim does the work, a soft shadow keeps it crisp on the brightest spots
const ON_PICTURE = '[text-shadow:0_1px_2px_rgb(0_0_0/0.35)]';
// something new (the last 24 hours) on a card: its words in a red pill (red is never a pick)
const FRESH_PILL = 'rounded-full bg-[var(--fm-accent)] px-2 py-[3px] text-[11px] font-bold leading-none text-white [text-shadow:none]';

/* ── the box a post has in a view ─────────────────────────── */

// (clear: a phone's card, how far its reading is lifted to clear the dock)
export type CellShape = { w: number; h: number; cols: number; rows: number; mediaWidth: number; wide: boolean; clear?: number; lift?: number; rise?: number };

// the picture's frame inside the cell, and its corners
function frameOf(mode: FeedMode, shape: CellShape): { width: number; height: number; radius: string } {
  if (mode !== 'today') return { width: shape.w, height: shape.h, radius: '6px' };
  if (shape.wide) return { width: shape.mediaWidth, height: shape.h, radius: '28px' };
  const height = Math.min(shape.h, Math.round(shape.w / POST_ASPECT));
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

/* ── a card's chrome: its reading, under its picture ── */

const TRAIL_MIN = 380;
// the reading's foot stands this far over the dock, and is cut off this far under its foot: just where the rest of it,
// waiting under there, begins (stream.css .st-read-more's margin: its reads' glow showed as a line in any more)
const FOOT_PAD = 8;
const POCKET_PAD = 2;
// the room under the picture (over the dock) a reading needs to stand clear of it, and to carry a line of the caption too
const FOOT_ROOM = 84;
const FOOT_ROOMY = 168;
// a card's reading, shut, is a box this tall (who, the number, and on a roomy screen a line of what it says), its pieces
// from its top: the same box whatever a post has in it, so nothing stands higher on one card than on the next
const FOOT_BOX = 110;
const FOOT_BOX_ROOMY = 178;
// the picture melts into the card going to a grid over this share of its height, at its foot (a card's own fall, as it
// rested, once it is known)
const MELT_SHARE = 0.3;
/* A phone card's ground (PostMedia groundAt): its fall begins this far over the reading's first line and is as deep as
   it goes this far under it. Where the picture ends before that, it dissolves into its bleed over its last BLEED_FEATHER */
const GROUND_ABOVE = 180;
const GROUND_UNDER = 40;
const BLEED_FEATHER = 44;
// the open reading's ground comes up out of the picture over this much, and starts this far above its first line
const SHADE_MELT = 190;
const SHADE_LEAD = 170;
// a stacked card's ground runs on this far past its foot
const DECK_TAIL = 48;
// the shade for the clock over a picture under the status bar goes on this far past the bar
const STATUS_SHADE_PAST = 28;
// a reel's picture (9:16), guessed until its own is known (anything else: 4:5)
const REEL_RATIO = 16 / 9;

function standoutText(post: StreamPost, today: string) {
  if (post.day === today || post.resultDay === today) return "Today's standout";
  const yesterday = shiftDay(today, -1);
  if (post.day === yesterday || post.resultDay === yesterday) return "Yesterday's standout";
  return 'Standout';
}

// a row's standout mark
function Marks({ post }: { post: StreamPost }) {
  return (
    <span className="st-card-marks pointer-events-none absolute left-4 top-4 z-[4] flex flex-col gap-2">
      <span data-st="" className="inline-flex w-fit items-center gap-1.5 rounded-[6px] bg-black/55 px-2 py-[5px] text-[10px] font-black uppercase leading-none tracking-[0.14em] text-white" style={{ '--i': 1 } as CSSProperties}>
        <span aria-hidden="true" className="h-1.5 w-1.5 shrink-0 rounded-full bg-[var(--fm-accent)]" />
        {standoutText(post, istDay())}
      </span>
    </span>
  );
}

/* A phone card's foot, the same on every card of a screen whatever its picture's shape (worked out from the card alone,
   as a 4:5 window drawn `lift` up): whether its reading has room under the picture (over: it stands on it; roomy: a line
   of what it says too), the box it stands in, where its first line is, and where its ground falls (the card's own px,
   from its top). Its chrome is drawn `rise` up, and stands `clear` over the card's foot */
type DeckFoot = { over: boolean; roomy: boolean; box: number; top: number; fall: number; deep: number };
function deckFootOf(width: number, height: number, clear: number, lift: number, rise: number): DeckFoot {
  const picture = Math.min(height, Math.round(width / POST_ASPECT)) - lift;
  const bottom = height - rise - clear - FOOT_PAD;
  const room = bottom - picture;
  const roomy = room >= FOOT_ROOMY;
  const box = roomy ? FOOT_BOX_ROOMY : FOOT_BOX;
  const top = bottom - box;
  return { over: room < FOOT_ROOM, roomy, box, top, fall: top - GROUND_ABOVE, deep: top + GROUND_UNDER };
}
// the share of a card's ground's fall at y (the card's own px)
const fallAt = (foot: DeckFoot, y: number) => (y - foot.fall) / Math.max(1, foot.deep - foot.fall);

/* The card's lead: where the post ranks, as one line of type, rolling to another of its reads as it is picked. Its size
   leaves the strip its room at the row's end, the same for every read of a post (its longest) */
const STRIP_ROOM = 170;
function numberSize(post: StreamPost, width: number, wanted: number) {
  const digits = Math.max(1, ...post.results.map((result) => (result.topPercent == null ? 0 : formatTopPercent(result.topPercent).length - 1)));
  const ems = digits >= 3 ? 3.1 : digits === 2 ? 2.5 : 1.95;
  return Math.round(Math.min(wanted, (Math.min(width, 460) - 32 - STRIP_ROOM) / ems));
}

function CardNumber({ topPercent, at, size }: { topPercent: number | null; at: number; size: number }) {
  const value = topPercent != null && Number.isFinite(topPercent) ? formatTopPercent(topPercent).slice(0, -1) : '—';
  return (
    <span aria-live="polite" className={cn('flex items-baseline whitespace-nowrap font-black leading-none text-white', ON_PICTURE)}>
      <span className="mr-[0.2em] tracking-[-0.03em] text-white/70" style={{ fontSize: Math.round(size * 0.5) }}>Top</span>
      <RollText
        value={value}
        at={at}
        className="-mb-[0.12em] -mt-[0.1em] leading-none tracking-[-0.05em] tabular-nums"
        style={{ fontSize: size }}
        after={<span className="ml-[0.04em]" style={{ fontSize: Math.round(size * 0.5) }}>%</span>}
      />
    </span>
  );
}

/* How its rank moved since the read before, and when this one landed: at the row's end in the open reading, where the
   strip is on the closed card */
function CardMove({ read }: { read: PostRead }) {
  const move = moveOf(read);
  if (!move) return null;
  return (
    <span className={cn('flex flex-col items-end text-right', ON_PICTURE)}>
      <LineSwap id={`${read.checkpoint}:${move.way}:${move.text}`} className="justify-items-end text-[15px] font-bold leading-tight text-white">
        {move.way === 'up' || move.way === 'down' ? <span className={cn('mr-1 align-[1px] text-[10px]', move.way === 'up' ? 'text-[var(--fm-accent-bright)]' : 'text-white/55')}>{move.way === 'up' ? '▲' : '▼'}</span> : null}
        {move.text}
      </LineSwap>
      <LineSwap id={`${read.checkpoint}:${read.day ?? ''}`} className="mt-0.5 justify-items-end text-[12px] font-medium leading-tight text-white/60">
        {[read.checkpoint, read.day].filter(Boolean).join(' · ')}
      </LineSwap>
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
  strip: RefObject<HTMLSpanElement | null>;
};

const NO_PICK = () => {};

// the play button's ring (r 20.5): its length, run round as the clip plays
const RING = 2 * Math.PI * 20.5;

/* A card's chrome: its reading between the picture and the dock (over the picture's foot on a screen too short for
   that). Who (unless the whole feed is one feeder's: solo), what and when; then one lead, where the post ranks, or, not
   measured yet, what it says. The lead is of the read picked (pick, its latest by default), on the strip beside it. Live
   (the card on the page, not the copy a move out of the Day view lets go): the rest of its reading waits under its foot
   for a tap, a ▶ sits on a reel that has a clip, and the reading clears its foot of the dock (clear). It stands in one
   box of the card's (foot: deckFootOf), the same on every card, and is cut off at its foot: what rises out of it as it
   opens rises from there, and sinks back under it, never down over the dock */
function CardChrome({ post, feeder, solo = false, width, height, standout, date, live = false, clear = 0, foot, parts, pick = null, onPick = NO_PICK, playable = false, playSlides = false, playOff = false, watching = false, onToggle, onPlay }: {
  post: StreamPost;
  feeder: StreamFeeder | null;
  solo?: boolean;
  width: number;
  height: number;
  standout: boolean;
  date: { kicker: string; title: string } | null;
  live?: boolean;
  clear?: number;
  parts?: ReadParts;
  pick?: StreamCheckpoint | null;
  onPick?: (checkpoint: StreamCheckpoint) => void;
  playable?: boolean;
  // a carousel's ▶: under its count, and away while its slide is a picture
  playSlides?: boolean;
  playOff?: boolean;
  // the card's foot (deckFootOf): whether the screen has room for a line of what it says, and the box it stands in
  foot: Pick<DeckFoot, 'roomy' | 'box'>;
  // its preview playing (its ▶ now a pause)
  watching?: boolean;
  onToggle?: () => void;
  onPlay?: () => void;
}) {
  const { roomy } = foot;
  const face = feeder ?? { handle: post.handle, profilePicUrl: null };
  const measured = post.topPercent != null && Number.isFinite(post.topPercent);
  const says = post.caption ? post.caption.replace(/\s+/g, ' ').trim() : '';
  // when it went up: today, yesterday, or its date
  const day = date ? (date.kicker === 'Today' || date.kicker === 'Yesterday' ? date.kicker : date.title) : null;
  // posted in the last 24 hours: its age is marked red (something new)
  const fresh = withinDay(post.postedAt);
  // the read it shows, and its numbers there as figures: each a number over what it counts, and what it gained since the
  // read before (a figure stays put whichever read is picked)
  const read = readAt(post, pick);
  const before = read.before;
  const figures = [
    post.likes != null ? { key: 'likes', value: formatCount(read.likes), label: read.likes === 1 ? 'like' : 'likes', gain: gainOf(read.likes, before?.likes) } : null,
    post.comments != null ? { key: 'comments', value: formatCount(read.comments), label: read.comments === 1 ? 'comment' : 'comments', gain: gainOf(read.comments, before?.comments) } : null,
    post.engagementRate != null ? { key: 'er', value: formatRate(read.engagementRate), label: 'engagement', gain: null } : null,
    post.multiple != null ? { key: 'usual', value: formatMultiple(read.multiple), label: 'vs usual', gain: null } : null,
  ].filter((figure): figure is { key: string; value: string; label: string; gain: string | null } => figure != null);
  // what and when; and, an earlier read picked, what it had then in its place (the open reading has that as its figures)
  const told = [KIND[post.mediaType], day, measured ? null : 'Measuring'].filter(Boolean).join(' · ');
  const then = read.latest ? null : [read.day, readLine(read)].filter(Boolean).join(' · ');
  const line = (
    <span className="flex min-w-0 items-center gap-1.5">
      <span className="st-line grid min-w-0" data-then={then ? '' : undefined}>
        <span className="st-told col-start-1 row-start-1 truncate">{told}</span>
        {measured ? <LineSwap id={then ?? ''} className="st-then col-start-1 row-start-1">{then ?? ' '}</LineSwap> : null}
      </span>
      {fresh ? <span className={cn('shrink-0', FRESH_PILL)}>{relativeTime(post.postedAt)}</span> : null}
    </span>
  );
  const when = postedLabel(post.postedAt);
  // a tap on these is theirs, not the card's
  const own = (action?: () => void) => (event: ReactMouseEvent) => {
    event.stopPropagation();
    action?.();
  };
  return (
    <>
      {/* the open reading's ground: the card's own colour, risen over the picture with it (its place set as it opens), the
          picture melting into it as it does into the card's foot */}
      {live ? (
        <span ref={parts?.shade} aria-hidden="true" className="st-read-shade pointer-events-none absolute inset-0 block">
          <Tone src={post.thumbnailUrl} melt className="inset-x-0 top-0" style={{ height: SHADE_MELT + 1 }} />
          <Tone src={post.thumbnailUrl} className="inset-x-0 bottom-0" style={{ top: SHADE_MELT }} />
        </span>
      ) : null}
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
            playSlides ? 'top-[calc(39px+var(--st-chrome-drop,0px))]' : 'top-[calc(0.75rem+var(--st-chrome-drop,0px))]',
          )}
        >
          <svg aria-hidden="true" viewBox="0 0 44 44" className="pointer-events-none absolute inset-0 h-11 w-11 -rotate-90">
            <circle ref={parts?.ring} className="st-play-ring" cx="22" cy="22" r="20.5" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeDasharray={RING} strokeDashoffset={RING} />
          </svg>
          <Play aria-hidden="true" className="st-play-on ml-0.5 h-[18px] w-[18px]" fill="currentColor" strokeWidth={0} />
          <Pause aria-hidden="true" className="st-play-off h-[18px] w-[18px]" fill="currentColor" strokeWidth={0} />
        </button>
      ) : null}
      {/* (cut off a hair under its foot: room for its type's shadow, never for the rest of it waiting under the foot) */}
      <span className="st-read-pocket pointer-events-none absolute inset-x-0 top-0 block overflow-clip" style={{ bottom: clear + POCKET_PAD }}>
      <span
        ref={parts?.read}
        className="st-read st-on-media absolute inset-x-0 bottom-0 block px-4 pt-4 sm:px-5"
        style={{ paddingBottom: FOOT_PAD - POCKET_PAD }}
      >
        <span ref={parts?.inner} className="st-read-in relative block" style={{ maxWidth: Math.min(width, 460) }}>
          <span className="st-foot relative flex min-w-0 flex-col gap-(--st-gap)" style={{ '--st-gap': roomy ? '16px' : '12px', '--st-foot-box': `${foot.box}px` } as CSSProperties}>
            {/* (over the box, not in it: a standout's first line stands where every other card's does) */}
            {standout ? (
              <span data-st="" className={cn('absolute bottom-full left-0 mb-(--st-gap) flex items-center gap-1.5 text-[12px] font-bold leading-none text-white', ON_PICTURE)} style={{ '--i': 0 } as CSSProperties}>
                <span aria-hidden="true" className="h-1.5 w-1.5 shrink-0 rounded-full bg-[var(--fm-accent)]" />
                {standoutText(post, istDay())}
              </span>
            ) : null}
            <span data-st="" className={cn('flex min-w-0 items-center gap-3', ON_PICTURE)} style={{ '--i': 0 } as CSSProperties}>
              {solo ? (
                // (the dock says whose feed it is)
                <span className="min-w-0 flex-1 text-[16px] font-semibold leading-tight text-white/80">{line}</span>
              ) : (
                <>
                  <span className="relative h-10 w-10 shrink-0 overflow-hidden rounded-full shadow-[0_0_0_1px_rgba(255,255,255,0.22)]">
                    <FeederStoryAvatar feeder={face} className="text-[14px]" />
                  </span>
                  <span className="min-w-0 flex-1">
                    <span className="block truncate text-[17px] font-bold leading-tight tracking-[-0.01em] text-white">@{post.handle}</span>
                    <span className="mt-0.5 block min-w-0 text-[13px] font-medium leading-tight text-white/65">{line}</span>
                  </span>
                </>
              )}
              {live ? (
                <button
                  ref={parts?.toggle}
                  type="button"
                  onClick={own(onToggle)}
                  aria-expanded="false"
                  aria-label="Show the details"
                  className={cn(
                    'st-read-toggle pointer-events-auto -mr-1 grid shrink-0 place-items-center rounded-full bg-white/[0.12] text-white outline-none [-webkit-tap-highlight-color:transparent] [text-shadow:none] active:bg-white/25 focus-visible:ring-2 focus-visible:ring-white/80',
                    solo ? 'h-9 w-9' : 'h-10 w-10',
                  )}
                >
                  <ChevronUp aria-hidden="true" className="h-[18px] w-[18px]" strokeWidth={2.4} />
                </button>
              ) : null}
            </span>
            {measured ? (
              <span data-st="" className="flex min-w-0 items-end justify-between gap-3" style={{ '--i': 1 } as CSSProperties}>
                <CardNumber topPercent={read.topPercent} at={read.order} size={numberSize(post, width, roomy ? 76 : solo ? 72 : 60)} />
                <span className="grid shrink-0 items-end justify-items-end">
                  <span ref={parts?.strip} inert={!live} className="st-strip-place col-start-1 row-start-1 block">
                    <CheckpointStrip post={post} at={read.checkpoint} topPercent={read.topPercent} onPick={onPick} onOpen={onToggle ?? NO_PICK} />
                  </span>
                  {live ? <span className="st-move col-start-1 row-start-1 block"><CardMove read={read} /></span> : null}
                </span>
              </span>
            ) : null}
            {/* what it says, in its own words: the lead while it has no number yet, a line under the number where there
                is room. The open reading has it whole, and takes its place (st-says) */}
            {says && (!measured || roomy) ? (
              <span data-st="" className={cn('st-says block', ON_PICTURE)} style={{ '--i': measured ? 2 : 1 } as CSSProperties}>
                <span className={cn('break-words', solo && !measured ? 'line-clamp-3' : 'line-clamp-2', measured ? 'text-[15px] font-medium leading-[1.4] text-white/75' : cn('font-semibold tracking-[-0.01em] text-white', roomy ? 'text-[20px] leading-[1.3]' : 'text-[17px] leading-[1.32]'))}>{says}</span>
              </span>
            ) : null}
            {!says && !measured ? (
              <span data-st="" className={cn('block text-[20px] font-semibold leading-[1.3] tracking-[-0.01em] text-white/65', ON_PICTURE)} style={{ '--i': 1 } as CSSProperties}>Measuring</span>
            ) : null}
          </span>
          {/* the rest of the reading, under the foot (the card itself stays minimal: who, and the number): the four reads,
              the counts as figures, the caption whole, when, the way out */}
          {live ? (
            <span ref={parts?.more} className="st-read-more flex flex-col gap-3.5">
              {height >= TRAIL_MIN ? (
                <span className="block">
                  <CheckpointTrail results={post.results} latest={post.latestCheckpoint} postedDay={post.day} size="lg" at={read.checkpoint} onPick={onPick} />
                </span>
              ) : null}
              {figures.length ? (
                // (as wide as each other, so none shifts as they roll; a hairline before each but the first)
                <span className="grid min-w-0" style={{ gridTemplateColumns: `repeat(${figures.length}, minmax(0, 1fr))` }}>
                  {figures.map((figure, index) => (
                    <span key={figure.key} data-figure="" className={cn('flex min-w-0 flex-col', index && 'border-l border-white/[0.16] pl-3', ON_PICTURE)}>
                      <RollText value={figure.value} at={read.order} className="w-fit text-[19px] font-black leading-none tracking-[-0.04em] tabular-nums text-white" />
                      <span className="mt-[3px] truncate text-[11px] font-medium leading-[14px] text-white/60">{figure.label}</span>
                      <LineSwap id={`${read.checkpoint}:${figure.gain ?? ''}`} className="mt-0.5 h-[14px] text-[11px] font-bold leading-[14px] tabular-nums text-white/85">{figure.gain ?? ' '}</LineSwap>
                    </span>
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
      </span>
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

function RowReading({ post, feeder, solo, height }: { post: StreamPost; feeder: StreamFeeder | null; solo: boolean; height: number }) {
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
        <PostReading post={post} feeder={feeder} solo={solo} size={reading.size} />
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
  leavingDate?: string | null;
  leavingFeeder?: StreamFeeder | null;
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
  // the whole feed is one feeder's: a post never says whose it is
  solo?: boolean;
  // the old view's leftover during a move (hidden unless the move lets it go from where it was)
  gone?: boolean;
  // a phone's Day view, not mid-move: the cards stack as they scroll, each over the one before (stream.css, st-yield), in
  // their order
  depth?: boolean;
  order?: number;
  // a phone's card: how far its reading is lifted to clear the dock
  clear?: number;
  // a phone's stacked card: how far it runs on over its picture (its head, up to the screen's top as it rests)
  head?: number;
  // a phone's card: how far its picture is drawn up into its head, and its reading and buttons
  lift?: number;
  rise?: number;
  onOpenTile: (post: StreamPost, element: HTMLElement) => void;
  // a tap on a card that isn't the one in view: the page brings it there. True when it is the one in view now after
  // all (the page rests on it, the scroll that brought it there not settled yet; or the tap stopped the page gliding
  // onto it: gliding), and the tap goes on to read it
  onFocusCard: (post: StreamPost, gliding?: boolean) => boolean;
};

const KIND: Record<StreamMediaType, string> = { reel: 'Reel', carousel: 'Carousel', image: 'Post', unknown: 'Post' };
const FOCUS = 'outline-none [-webkit-tap-highlight-color:transparent] focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]';

/* A card's reading opening and closing: one glide each way (the carry's curve), every piece of it on the one clock,
   started on the tap and turned round from wherever it is by the next. The rest of it rises with it out from under its
   foot, and sinks back under it (cut off there: CardChrome), nothing in it fading in or out on its own. Its ground rises
   with it, deepening; the strip gives its place to how the rank moved in its first part, that comes in its rest */
const READ_OPEN_MS = ms(460);
const READ_CLOSE_MS = ms(440);
/* WebKit shows a glide's first frame a while after its clock starts, and ends it on the clock: the start was skipped
   (a jump, then a hitch) and the end cut. Held at its first pose this long and at its last this long, inside its
   keyframes, it is seen whole (as the Read page's moves: readOneEngine LEAD, TAIL) */
const READ_LEAD_MS = 100;
const READ_TAIL_MS = 240;
// how deep the open reading's ground lies (stream.css, .st-read-shade opened): the picture shows through it a little
const READ_SHADE_OPEN = 0.86;
// the strip going, in the glide's first part; how the rank moved coming, in the rest (and back)
const READ_SWAP_OUT = 0.4;
const READ_SWAP_IN = 0.3;
const READ_SWAP_OUT_EASE = 'cubic-bezier(0.4, 0, 1, 1)';
// the open reading leaves at least this much of the picture's top clear (past that, its rest scrolls)
const READ_TOP_ROOM = 56;
// a reading open as its card clears is shut once it has slid off (stream.css, .st-read in [data-clean])
const CLEAR_MS = ms(420);
// a touch that stops the page gliding is a tap if it lifts this near where it landed, this soon; a click that follows
// it anyway, this soon, is that tap's
const TAP_SLOP_PX = 10;
const TAP_MAX_MS = 500;
const TAP_CLICK_MS = 600;

type Clean = 'watch' | 'slides' | null;

// a reading's glide: its whole length, and where in it a share of its move falls
type ReadGlide = { all: number; at: (share: number) => number };
function readGlide(move: number): ReadGlide {
  const webkit = holdsFlights();
  const lead = webkit ? READ_LEAD_MS : 0;
  const all = lead + move + (webkit ? READ_TAIL_MS : 0);
  return { all, at: (share) => (lead + share * move) / all };
}

// one piece of it: at `from` until `start` of the move, at `to` by `end` (on `easing`), and there after. Once it has
// ended it lets go: what it ends on is where the piece stands by itself (its own style), so nothing jumps
function glideOn(el: Element, glide: ReadGlide, from: Keyframe, to: Keyframe, start = 0, end = 1, easing = CARRY_EASE_CSS) {
  const a = glide.at(start);
  const b = glide.at(end);
  const frames: Keyframe[] = [];
  if (a > 0) frames.push({ ...from, offset: 0, easing: 'linear' });
  frames.push({ ...from, offset: a, easing });
  frames.push({ ...to, offset: b, easing: 'linear' });
  if (b < 1) frames.push({ ...to, offset: 1 });
  const animation = el.animate(frames, { duration: glide.all, easing: 'linear', fill: 'both' });
  animation.finished.then(() => animation.cancel(), () => {});
}

// where a piece is drawn now (mid-way, if it is moving): its opacity, how far it is lifted, how far turned
function drawnAt(el: Element | null) {
  if (!el) return null;
  const style = getComputedStyle(el);
  const matrix = style.transform && style.transform !== 'none' ? new DOMMatrixReadOnly(style.transform) : null;
  let turn = matrix ? (Math.atan2(matrix.m12, matrix.m11) * 180) / Math.PI : 0;
  if (turn < -90) turn += 360;
  return { opacity: Number(style.opacity), y: matrix ? matrix.m42 : 0, turn };
}

const lifted = (y: number) => `translate3d(0px, ${y.toFixed(1)}px, 0px)`;

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
  leavingDate = null,
  leavingFeeder = null,
  arriving,
  focal,
  active,
  lit,
  spot,
  standout,
  date,
  eager,
  feeder,
  solo = false,
  gone = false,
  depth = false,
  order = 0,
  clear = 0,
  head = 0,
  lift = 0,
  rise = 0,
  onOpenTile,
  onFocusCard,
}: PostCellProps) {
  const shape: CellShape = { w, h, cols, rows, mediaWidth, wide };
  const dateParts = date ? groupParts(date, 'day', istDay()) : null;
  const grid = mode !== 'today';
  const card = mode === 'today' && !shape.wide;
  const row = mode === 'today' && shape.wide;
  const frame = frameOf(mode, shape);
  const stacked = card && depth;
  const headed = stacked && head > 0;
  /* A phone's card: its picture runs on up to the screen's very top (under the status bar), never anything made up in
     its place. Above its 4:5 is as much of the picture's own frame as there is (a reel's 9:16 has plenty, shown once it
     rests: `grown`); what is missing (a photo, 4:5 or 3:4) it is drawn higher for, its own top under the clock. Its
     chrome and reading are drawn `upChrome` up, the same on every card */
  const ratio = useMediaRatio(post.thumbnailUrl, post.mediaType === 'reel' ? REEL_RATIO : 1 / POST_ASPECT);
  const reach = card && head > 0 ? Math.max(0, head - lift) : 0;
  const above = Math.min(reach, Math.max(0, Math.floor((ratio * frame.width - frame.height) / 2)));
  const up = card ? lift + reach - above : 0;
  const grown = headed ? above : 0;
  const upChrome = card ? rise : 0;
  /* Its foot is the same on every card (deckFootOf): the reading stands in one place, and the ground falls over the
     picture and the picture's bleed in one place, whatever its picture's shape. Where the picture ends, and what of the
     ground's fall lies over it (and over the card under it) */
  const deck = card ? deckFootOf(shape.w, shape.h, clear, lift, rise) : null;
  const pictureFoot = frame.height - up;
  const pictureFall = deck ? fallAt(deck, pictureFoot) : 1;
  const groundTop = frame.height - 2 - up;
  const groundHeight = card ? shape.h - frame.height + 2 + up + (stacked ? DECK_TAIL : 0) : shape.h;
  // ...kept, in shares of its 4:5, for the picture to let go of as it flies to a grid (its melt as it rested)
  const restMelt = useRef<{ top: number; height: number; fall: Fall } | null>(null);
  if (deck) restMelt.current = { top: (deck.fall + up) / frame.height, height: (pictureFoot - deck.fall) / frame.height, fall: [0, pictureFall] };
  const winner = isWinner(post.topPercent);
  // what of the view being left goes, drawn as it was
  const was = leaving && leavingShape ? { grid: leaving !== 'today', card: leaving === 'today' && !leavingShape.wide, row: leaving === 'today' && leavingShape.wide } : null;
  const chipsOut = was?.grid && !grid;
  const cardOut = was?.card && !card;
  const rowOut = was?.row && !row;
  // a card whose frame is all of it (a phone's) has no surface of its own to show
  const fullFrame = (cardShape: CellShape) => frameOf('today', cardShape).height >= cardShape.h - 0.5;
  const bg = card ? !fullFrame(shape) : Boolean(cardOut && leavingShape && !fullFrame(leavingShape));

  const style = { left: x, top: y, width: shape.w, height: shape.h, '--st-z': stacked ? order + 1 : undefined } as CSSProperties;
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
  const stripRef = useRef<HTMLSpanElement | null>(null);
  const openRef = useRef(false);
  const setRead = useCallback((open: boolean, animate = true) => {
    const cell = cellRef.current;
    const read = readRef.current;
    const block = innerRef.current;
    if (!cell || !read || !block || openRef.current === open) return;
    openRef.current = open;
    const more = moreRef.current;
    const shade = shadeRef.current;
    const strip = stripRef.current;
    const toggle = toggleRef.current;
    const turner = toggle?.querySelector('svg') ?? null;
    const move = block.querySelector<HTMLElement>('.st-move');
    const says = block.querySelector<HTMLElement>('.st-says');
    // where each piece is drawn now (mid-way, if it is moving), then where it belongs: it goes from the one to the other
    const before = block.getBoundingClientRect().top;
    const shadeAt = drawnAt(shade);
    const stripAt = drawnAt(strip);
    const moveAt = drawnAt(move);
    const saysAt = drawnAt(says);
    const turnerAt = drawnAt(turner);
    [block, shade, strip, move, says, turner].forEach((el) => el?.getAnimations().forEach((animation) => animation.cancel()));
    if (open) cell.dataset.read = '';
    else delete cell.dataset.read;
    // opening: the rest gets the room the card has left under the foot (past it, it scrolls), from its top
    if (open && more) {
      const pad = getComputedStyle(read);
      const foot = (block.firstElementChild as HTMLElement | null)?.offsetHeight ?? 0;
      const room = (read.parentElement?.clientHeight ?? cell.clientHeight) - READ_TOP_ROOM - parseFloat(pad.paddingTop) - parseFloat(pad.paddingBottom) - foot - 16;
      more.style.setProperty('--st-more-max', `${Math.max(96, Math.floor(room))}px`);
      more.scrollTop = 0;
    }
    if (more) more.inert = !open;
    // (the open reading's own reads take its strip's place)
    if (strip) strip.inert = open;
    toggle?.setAttribute('aria-expanded', String(open));
    toggle?.setAttribute('aria-label', open ? 'Hide the details' : 'Show the details');
    if (open && more) more.toggleAttribute('data-scrolls', more.scrollHeight > more.clientHeight + 1);
    const after = block.getBoundingClientRect().top;
    // the ground's top sits a little over the reading's, open or shut, so it goes up and comes down with it. Placed by
    // translate, never by top: it keeps the card's height whole, its foot covered all the way (shut, its top used to
    // stay up there, and the glide down began with it lifted a whole reading higher: dark over the picture's top)
    if (shade) shade.style.translate = `0px ${Math.max(0, after - cell.getBoundingClientRect().top - SHADE_LEAD).toFixed(1)}px`;
    const dy = before - after;
    if (!animate || reducedMotion() || Math.abs(dy) < 0.5) return;
    const glide = readGlide(open ? READ_OPEN_MS : READ_CLOSE_MS);
    glideOn(block, glide, { transform: lifted(dy) }, { transform: lifted(0) });
    if (shade && shadeAt) glideOn(shade, glide, { opacity: shadeAt.opacity, transform: lifted(dy) }, { opacity: open ? READ_SHADE_OPEN : 0, transform: lifted(0) });
    if (turner && turnerAt) glideOn(turner, glide, { transform: `rotate(${turnerAt.turn.toFixed(1)}deg)` }, { transform: `rotate(${open ? 180 : 0}deg)` });
    // the strip gives its place to how the rank moved (and takes it back): the one going, then the one coming
    const [going, goingAt, coming, comingAt] = open ? [strip, stripAt, move, moveAt] : [move, moveAt, strip, stripAt];
    if (going && goingAt) glideOn(going, glide, { opacity: goingAt.opacity, transform: lifted(goingAt.y) }, { opacity: 0, transform: lifted(open ? -10 : 10) }, 0, READ_SWAP_OUT, READ_SWAP_OUT_EASE);
    if (coming && comingAt) glideOn(coming, glide, { opacity: comingAt.opacity, transform: lifted(comingAt.y) }, { opacity: 1, transform: lifted(0) }, READ_SWAP_IN, 1);
    // what it says under the number (a roomy screen): the open reading has it whole in its place
    if (says && saysAt) glideOn(says, glide, { opacity: saysAt.opacity }, { opacity: open ? 0 : 1 }, open ? 0 : 0.55, open ? 0.25 : 1);
  }, []);

  /* ── cleared: a clip playing (its ▶), or a carousel's slides past the first ── */
  const [clean, setClean] = useState<Clean>(null);
  // the read picked (its latest until one is)
  const [pick, setPick] = useState<StreamCheckpoint | null>(null);
  // the slide a card's carousel shows: one with video slides has its ▶ on those
  const [slideAt, setSlideAt] = useState(0);
  const slideClips = card && post.slides.length > 1 && post.slides.some((slide) => slide.video);
  const playable = card && ((post.mediaType === 'reel' && Boolean(post.previewUrl) && post.slides.length <= 1) || slideClips);
  const playOff = slideClips && !post.slides[slideAt]?.video;
  // what a card is once its clip stops: cleared past a carousel's first slide, itself on it
  const rest: Clean = slideAt > 0 ? 'slides' : null;
  // a reading open as the card clears closes with it; one that stands over the picture (a short screen) goes down off the
  // card with the rest instead, and is shut once it is out of sight
  const shutWhenClear = useCallback(() => {
    if (!openRef.current) return;
    if (cellRef.current?.dataset.over === undefined) {
      setRead(false);
      return;
    }
    window.setTimeout(() => {
      if (cellRef.current?.dataset.clean) setRead(false, false);
    }, CLEAR_MS);
  }, [setRead]);
  // a tap taken as its touch lifted, the page gliding (onCardTouchEnd, below): the card it lands on is the one in view
  const glideTapRef = useRef(false);
  const togglePlay = useCallback(() => {
    // (a card peeking under the one in view is brought into view first)
    if (!active && !onFocusCard(post, glideTapRef.current)) return;
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
      setPick(null);
    }
  }
  useEffect(() => {
    if (!active) setRead(false, card);
  }, [active, card, setRead]);
  // the card another size (a rotation): its open reading was placed for the old one
  useIsoLayoutEffect(() => {
    setRead(false, false);
  }, [w, h, clear, setRead]);
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

  // a tap anywhere on a card: the one in view reads (or, its clip playing, comes back); another is brought into view
  const tap = () => {
    if (!active && !onFocusCard(post, glideTapRef.current)) return;
    if (clean) {
      setClean(null);
      // (a tap on a clip only stops it; on a swiped carousel whose reading never left, it reads)
      if (clean === 'watch' || deck?.over) return;
    }
    setRead(!openRef.current);
  };
  // (a click after a tap its touch already gave the card is that tap's)
  const tookTapRef = useRef(Number.NEGATIVE_INFINITY);
  const onCardTap = (event: ReactMouseEvent<HTMLDivElement>) => {
    if ((event.target as Element).closest('a, button')) return;
    if (performance.now() - tookTapRef.current < TAP_CLICK_MS) return;
    tap();
  };
  /* A touch that lands while the page is still gliding (the swipe that brought the card, its snap settling) is spent by
     iOS on stopping the glide, and no click follows: lifted where it landed, it is the card's tap here (a control's
     too: its ▶, its arrow). Links are left to the browser */
  const touchRef = useRef<{ x: number; y: number; t: number } | null>(null);
  const onCardTouchStart = (event: ReactTouchEvent<HTMLDivElement>) => {
    const touch = event.touches[0];
    touchRef.current = event.touches.length === 1 && touch && pageGliding() ? { x: touch.clientX, y: touch.clientY, t: performance.now() } : null;
  };
  const onCardTouchEnd = (event: ReactTouchEvent<HTMLDivElement>) => {
    const start = touchRef.current;
    touchRef.current = null;
    const touch = event.changedTouches[0];
    if (!start || !touch || event.touches.length) return;
    if (Math.hypot(touch.clientX - start.x, touch.clientY - start.y) > TAP_SLOP_PX || performance.now() - start.t > TAP_MAX_MS) return;
    const control = (event.target as Element).closest('a, button');
    if (control instanceof HTMLAnchorElement) return;
    event.preventDefault();
    tookTapRef.current = performance.now();
    glideTapRef.current = true;
    try {
      if (control instanceof HTMLButtonElement) control.click();
      else tap();
    } finally {
      glideTapRef.current = false;
    }
  };
  const onCardTouchCancel = () => {
    touchRef.current = null;
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
      data-depth={stacked ? '' : undefined}
      data-over={deck?.over ? '' : undefined}
      data-clean={card && clean ? clean : undefined}
      // a card reads from anywhere on it (its chrome lets touches through, to a carousel's slides); the keys have its
      // arrow button
      onClick={card ? onCardTap : undefined}
      onTouchStart={card ? onCardTouchStart : undefined}
      onTouchEnd={card ? onCardTouchEnd : undefined}
      onTouchCancel={card ? onCardTouchCancel : undefined}
      className={cn('st-cell', row && 'st-row', card && 'cursor-pointer rounded-[28px] [-webkit-tap-highlight-color:transparent]')}
      style={style}
    >
      {bg ? (
        // (from a hair under the picture's foot, so no seam shows between them, and never behind its round top corners,
        // where its dark would show as a fringe; stacked, on past the card's own foot, behind the next one's corners)
        <div
          data-layer="bg"
          className={cn('overflow-hidden bg-[#17171a]', !stacked && 'rounded-b-[28px]')}
          style={{ left: 0, top: card ? groundTop : 0, width: shape.w, height: groundHeight }}
        >
          {deck ? (
            // the picture's own colours run on under the reading, the ground falling over them (never whole)
            <>
              <Bleed src={post.thumbnailUrl} className="inset-0" />
              <Tone src={post.thumbnailUrl} fall={[fallAt(deck, groundTop), fallAt(deck, groundTop + groundHeight)]} deep className="inset-0" />
            </>
          ) : (
            <Tone src={post.thumbnailUrl} className="inset-0" />
          )}
        </div>
      ) : null}

      <div
        data-layer="frame"
        className={cn('group overflow-hidden [contain:strict]', grid ? 'bg-[var(--st-surface)]' : 'isolate', row && 'bg-[var(--st-media)]')}
        // (grown, its picture is placed in it just where it stood in its 4:5 (--st-pic-y, stream.css), and the frame is
        // measured as its 4:5 for a move (data-grown). Drawn up, the picture's own buttons stay below the status bar:
        // --st-pic-top)
        data-grown={grown || undefined}
        style={{
          left: 0,
          top: -up - grown,
          width: frame.width,
          height: frame.height + grown,
          borderRadius: frame.radius,
          '--st-pic-top': up + grown ? `${up + grown}px` : undefined,
          '--st-pic-y': grown ? `${(grown - (ratio * frame.width - frame.height + 2) / 2).toFixed(2)}px` : undefined,
        } as CSSProperties}
        {...(grid ? press : null)}
      >
        {/* the Day view's frame shows a landscape picture whole, over its own wash */}
        {/* (a card's, and its picture, stop a hair short of the frame's foot and its wash of its corners: drawn to the edge,
            their last pixels bleed past what covers them, as a line and a fringe) */}
        {row || card ? <Wash src={post.thumbnailUrl} className={card ? 'bottom-[2px] rounded-t-[36px]' : undefined} /> : null}
        {/* the picture gives under the finger (inside the frame: the moves own the frame's transform), and never while
            the finger scrolls the page (press.ts) */}
        <span className={cn('absolute inset-x-0 top-0 block', card ? 'bottom-[2px]' : 'bottom-0 transition-transform duration-200 ease-out', grid ? 'group-data-[pressed]:scale-[0.97]' : null)}>
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
        {/* a card's picture melts into what the card runs on in under it: dissolving into its own colours over its foot
            (where the ground over it is light enough to show them), the ground falling over it */}
        {deck && pictureFall < 1 ? <Bleed src={post.thumbnailUrl} ramp className="st-card-scrim" style={{ left: 0, right: 0, bottom: 0, height: BLEED_FEATHER }} /> : null}
        {deck && pictureFoot > deck.fall ? (
          <Tone src={post.thumbnailUrl} fall={[0, pictureFall]} className="st-card-scrim" style={{ left: 0, right: 0, bottom: 0, height: pictureFoot - deck.fall }} />
        ) : null}
        {/* ...and, under the status bar, is shaded for the clock, gone a little way below it */}
        {up - lift + grown > 0 ? (
          <span aria-hidden="true" className="st-status-shade pointer-events-none absolute inset-x-0 top-0 block" style={{ height: up - lift + grown + STATUS_SHADE_PAST }} />
        ) : null}
        {/* ...and, going to a grid, lets go of it as it flies (drawn in shares of the frame: it is carried with it) */}
        {cardOut && restMelt.current ? (
          <Tone src={post.thumbnailUrl} fall={restMelt.current.fall} className="st-melt-out" style={{ left: 0, right: 0, top: `${restMelt.current.top * 100}%`, height: `${restMelt.current.height * 100}%` }} />
        ) : cardOut ? (
          <Tone src={post.thumbnailUrl} melt className="st-melt-out" style={{ left: 0, right: 0, bottom: 0, height: `${MELT_SHARE * 100}%` }} />
        ) : null}
        {row && standout ? <Marks post={post} /> : null}
        {/* a winner (top 10%): a white ring (what is new is marked on its feeder's circle, not on the post) */}
        {winner && !card ? (
          <span aria-hidden="true" className={cn('pointer-events-none absolute inset-0 z-[4] shadow-[inset_0_0_0_1.5px_rgb(255_255_255/0.8)]', grid ? 'rounded-[6px]' : 'rounded-[28px]')} />
        ) : null}
        {/* the wide Day view's column out of the spotlight: it recedes into the page (dimmed on dark, washed out on light) */}
        {row ? <span aria-hidden="true" className="st-dim pointer-events-none absolute inset-0 z-[5] bg-black light:bg-[var(--st-bg)]" /> : null}
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
        <div data-layer="chrome" className="pointer-events-none overflow-hidden rounded-[28px]" style={{ left: 0, top: -upChrome, width: shape.w, height: shape.h, '--st-chrome-drop': upChrome ? `${upChrome}px` : undefined } as CSSProperties}>
          <CardChrome
            post={post}
            feeder={feeder}
            solo={solo}
            width={shape.w}
            height={shape.h}
            standout={standout}
            date={dateParts}
            live
            clear={clear}
            foot={deck ?? { roomy: false, box: 0 }}
            parts={{ read: readRef, inner: innerRef, more: moreRef, shade: shadeRef, toggle: toggleRef, ring: ringRef, strip: stripRef }}
            pick={pick}
            onPick={setPick}
            playable={playable}
            playSlides={slideClips}
            playOff={playOff}
            watching={clean === 'watch'}
            onToggle={() => {
              if (!active && !onFocusCard(post, glideTapRef.current)) return;
              // (a cleared card: its reading back, then open)
              if (clean) setClean(null);
              setRead(!openRef.current);
            }}
            onPlay={togglePlay}
          />
        </div>
      ) : null}

      {/* a phone's card going under the next one (st-sink) */}
      {card ? <span data-depth-dim="" aria-hidden="true" className="pointer-events-none absolute inset-0 z-[6] rounded-[28px] bg-black opacity-0" style={headed ? { top: -head } : undefined} /> : null}

      {row ? (
        <div data-layer="reading" data-reading="" style={{ left: shape.mediaWidth + READING_GAP, top: 0, width: Math.max(0, shape.w - shape.mediaWidth - READING_GAP), height: shape.h }}>
          <RowReading post={post} feeder={feeder} solo={solo} height={shape.h} />
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
          <CardChrome post={post} feeder={leavingFeeder ?? feeder} solo={solo} width={leavingShape.w} height={leavingShape.h} standout={standout} date={leavingDate ? groupParts(leavingDate, 'day', istDay()) : dateParts} clear={leavingShape.clear} foot={deckFootOf(leavingShape.w, leavingShape.h, leavingShape.clear ?? 0, leavingShape.lift ?? 0, leavingShape.rise ?? 0)} />
        </div>
      ) : null}
      {rowOut && leavingShape ? (
        <div
          data-layer="reading-out"
          className="pointer-events-none"
          style={{ left: 0, top: 0, width: Math.max(0, leavingShape.w - leavingShape.mediaWidth - READING_GAP), height: leavingShape.h }}
        >
          <RowReading post={post} feeder={feeder} solo={solo} height={leavingShape.h} />
        </div>
      ) : null}
    </div>
  );
}

export default memo(PostCell);

/* ── a slot whose day hasn't loaded: a still block where the post will be ── */

export const PlaceholderCell = memo(function PlaceholderCell({ cellKey, mode, x, y, w, h, mediaWidth, wide, gone = false, lift = 0 }: {
  cellKey: string;
  mode: FeedMode;
  x: number;
  y: number;
  w: number;
  h: number;
  mediaWidth: number;
  wide: boolean;
  gone?: boolean;
  // a phone's card: its picture drawn this far up, where the post's will be (PostCell)
  lift?: number;
}) {
  const shape: CellShape = { w, h, cols: 1, rows: 1, mediaWidth, wide };
  const frame = frameOf(mode, shape);
  const card = mode === 'today' && !shape.wide;
  const style: CSSProperties = { left: x, top: y, width: shape.w, height: shape.h };
  return (
    <div data-cell={cellKey} data-kind="placeholder" data-card={card ? '' : undefined} data-gone={gone ? '' : undefined} aria-hidden="true" className={cn('st-cell', card && 'rounded-[28px]')} style={style}>
      {card && frame.height < shape.h - 0.5 ? <div className="st-placeholder absolute inset-0 rounded-[28px]" /> : null}
      <div data-layer="frame" className="st-placeholder" style={{ left: 0, top: card ? -lift : 0, width: frame.width, height: frame.height, borderRadius: frame.radius }} />
    </div>
  );
});
