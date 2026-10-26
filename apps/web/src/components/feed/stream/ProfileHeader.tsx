'use client';

/* ─────────────────────────────────────────────────────────────
   PROFILE HEADER — the top of the feed: whose it is, as an
   Instagram profile, but cooler. Every number comes from the same
   posts and checkpoints the feed shows.

   A feeder: their big face (an accent story ring when they posted in
   the last day) with a time badge. A feed: its feeders' faces,
   stacked. All feeds: the feed badges, stacked, and what happened
   today. Beside or under it: posts, followers and the typical
   post, each with its move. Then the name, highlights as little
   circles, and the way into the other two tabs, each showing
   something of its own rather than a button (profileDoors): Read's
   trajectory, its runs of 10 as shaded boxes, and the first win of
   Lead's Hall of Feed with its post. Last, the order as Instagram's
   tab row (Newest posts · Latest results).

   'page' sits above the grids at a fixed height for its width
   (profileHeaderSize: loaded or not, nothing below it moves).
   'card' is the today zoom's first card: the same, centred and
   compact, filling its box, ending in "Swipe up"; on a short
   screen it lets go of its least needed rows (stream.css).

   While the profile loads the frame is drawn with still
   placeholders, never a number, and the rows keep their heights
   whatever arrives, so a phone's snapping Day view never moves
   under the finger. Nothing here animates but the order's
   underline and pill (transform), and presses.
   ───────────────────────────────────────────────────────────── */

import { memo, useState, type CSSProperties, type ReactNode } from 'react';
import { ChevronRight, ChevronUp, Clock, Ellipsis, Target, Trophy, type LucideIcon } from 'lucide-react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { useProfileDoors, type ReadDoor } from '@/components/feed/stream/profileDoors';
import PostCover from '@/components/feed/stream/PostCover';
import { press } from '@/components/feed/stream/press';
import { PROFILE_ROW, profileFrame } from '@/components/feed/stream/profileHeaderSize';
import { HALL_DAYS, LEAD_AT_KEY, WIN_LABEL, type HallFact } from '@/components/lead/leadHall';
import { RUN_SIZE, RUN_SLOTS, noteFor, type BoardRun, type Momentum } from '@/components/read/readBoard';
import { shade } from '@/components/read/readShade';
import {
  STREAM_WINDOW_DAYS,
  formatCount,
  formatTopPercent,
  isWinner,
  relativeTime,
  type FeedOrder,
  type ProfileHeaderProps,
  type StreamFeeder,
  type StreamHighlight,
  type StreamProfile,
  type StreamScope,
} from '@/lib/feedStream/contract';
import { feedInitials } from '@/lib/feedLabels';
import { useAppHaptics } from '@/lib/haptics';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

type Kind = 'feeder' | 'feed' | 'all';
type Size = 'page' | 'card' | 'spread';
type Face = { handle: string; profilePicUrl: string | null };
type Note = { text: string; up: boolean } | null;

const ROW = PROFILE_ROW;
// the big face, and the ring round it (3px of ring, 3px of gap); 'spread' is a wide screen's, with the stats beside it
const FACE_PX: Record<Size, number> = { page: 108, card: 124, spread: 132 };
const RING = 6;
const ORDERS: ReadonlyArray<{ value: FeedOrder; label: string; Icon: LucideIcon }> = [
  { value: 'posted', label: 'Newest posts', Icon: Clock },
  { value: 'results', label: 'Latest results', Icon: Target },
];
// a feed's badge: the rail's light disc, its initials in ink (the disc is light in both themes)
const DISC_BG = 'linear-gradient(135deg, #f7f7f8, #cfd3dc)';
// the card: its surface (stream.css --st-card), lit softly from above the face (a neutral light: the accent is for meaning)
const CARD_BG = 'radial-gradient(90% 55% at 50% 0%, rgb(var(--fm-fg-rgb) / calc(var(--st-card-glow) * 0.4)), transparent 70%), var(--st-card)';
const EASE = 'ease-[cubic-bezier(0.16,0.9,0.2,1)]';
// a glide at the app's tempo (lib/tempo)
const GLIDE = 'duration-[calc(300ms*var(--fm-tempo))]';
/* the order's underline (and the card's fill) goes with the list it marks: it moves when the new order's list slides
   in (FeedStream: SLIDE_MS on the carry curve), not at the tap, which only lights the tab's label at once */
const SLIDE_GLIDE = 'duration-[calc(460ms*var(--fm-tempo))] ease-[cubic-bezier(0.3,0.3,0.2,1)]';
const MARKER = 'text-[10px] font-black uppercase leading-none tracking-[0.14em] text-[var(--st-text-3)]';
const ACTION = 'flex h-11 min-w-0 flex-1 items-center justify-center gap-2 rounded-[14px] bg-[var(--st-surface-2)] px-3 text-[14px] font-bold text-[var(--st-text)] shadow-[inset_0_0_0_1px_var(--st-line)] outline-none [-webkit-tap-highlight-color:transparent] transition-transform duration-150 ease-out active:scale-[0.97] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]';

/* ── words and numbers ────────────────────────────────────────────────────── */

const kindOf = (scope: StreamScope): Kind => (scope.handle ? 'feeder' : scope.feedId ? 'feed' : 'all');
const plural = (count: number, word: string) => `${word}${count === 1 ? '' : 's'}`;
const ranked = (value: number | null | undefined): value is number => value != null && Number.isFinite(value);

function postsValue(posts: number) {
  return posts < 10_000 ? posts.toLocaleString('en-US') : formatCount(posts);
}

// "▲ 3.0% · 30d" with an accent ▲, "▼ 1.2% · 30d" muted
function followerNote(delta: number | null): Note {
  if (!ranked(delta)) return null;
  const size = Math.abs(delta).toFixed(1);
  if (delta >= 0.05) return { text: `▲ ${size}% · 30d`, up: true };
  if (delta <= -0.05) return { text: `▼ ${size}% · 30d`, up: false };
  return { text: `${size}% · 30d`, up: false };
}

// lower is stronger: "▲ from 55%" when the typical post climbed, "▼ from 35%" when it slipped
function typicalNote(typical: number | null, previous: number | null): Note {
  if (!ranked(typical) || !ranked(previous)) return null;
  const from = formatTopPercent(previous);
  if (typical < previous - 0.5) return { text: `▲ from ${from}`, up: true };
  if (typical > previous + 0.5) return { text: `▼ from ${from}`, up: false };
  return { text: `steady at ${from}`, up: false };
}

/* ── the face ─────────────────────────────────────────────────────────────── */

// where each of up to three stacked circles sits in the face's box: one fills it, more step down and right
function stackSpots(count: number, box: number) {
  if (count <= 1) return [{ px: box - RING * 2, at: RING }];
  if (count === 2) {
    const px = Math.round(box * 0.68);
    return [{ px, at: 0 }, { px, at: box - px }];
  }
  const px = Math.round(box * 0.58);
  return [{ px, at: 0 }, { px, at: Math.round((box - px) / 2) }, { px, at: box - px }];
}

const initialsSize = (px: number) => (px >= 100 ? 'text-[28px]' : px >= 76 ? 'text-[22px]' : 'text-[18px]');

// only the front disc of a stack is lettered: the ones behind it peek out as discs, never as cut-off words
function FeedDisc({ title, px, front }: { title: string; px: number; front: boolean }) {
  return (
    <span
      className={cn('grid h-full w-full place-items-center font-black leading-none tracking-[-0.04em] text-[#0b0b0f]', px >= 100 ? 'text-[34px]' : px >= 76 ? 'text-[22px]' : 'text-[18px]')}
      style={{ background: DISC_BG }}
    >
      {front ? feedInitials(title) : null}
    </span>
  );
}

function Stack({ box, items }: { box: number; items: Array<{ key: string; draw: (px: number, front: boolean) => ReactNode }> }) {
  const spots = stackSpots(items.length, box);
  return (
    <>
      {items.map((item, index) => {
        const spot = spots[index];
        return (
          <span
            key={item.key}
            className="absolute block overflow-hidden rounded-full shadow-[0_0_0_3px_var(--st-cut)] light:shadow-[0_0_0_3px_var(--st-cut),0_10px_22px_-12px_rgb(15_23_42/0.35)]"
            style={{ width: spot.px, height: spot.px, left: spot.at, top: spot.at, zIndex: items.length - index }}
          >
            {item.draw(spot.px, index === 0)}
          </span>
        );
      })}
    </>
  );
}

function FaceBlock({ kind, size, scope, feeder, feeds, faces, fresh, lastPostAt }: {
  kind: Kind;
  size: Size;
  scope: StreamScope;
  feeder: StreamFeeder | null;
  feeds: ProfileHeaderProps['feeds'];
  faces: Face[];
  fresh: boolean;
  lastPostAt: string | null;
}) {
  const box = FACE_PX[size] + RING * 2;
  let body: ReactNode;
  if (kind === 'feeder') {
    const face = feeder ?? { handle: scope.handle ?? '', profilePicUrl: null };
    body = (
      <>
        {fresh ? (
          <span aria-hidden="true" className="st-story-ring absolute inset-0 rounded-full" />
        ) : (
          <span aria-hidden="true" className="absolute inset-[1.5px] rounded-full shadow-[inset_0_0_0_1.5px_rgb(var(--fm-fg-rgb)/0.2)]" />
        )}
        <span className="absolute block overflow-hidden rounded-full" style={{ inset: RING }}>
          <FeederStoryAvatar feeder={face} className={size === 'page' ? 'text-[28px]' : 'text-[34px]'} />
        </span>
      </>
    );
  } else if (kind === 'feed' && faces.length) {
    // a face behind the front one shows its photo, or its tone without letters (no half-hidden initials)
    body = <Stack box={box} items={faces.slice(0, 3).map((face) => ({ key: face.handle, draw: (px, front) => <FeederStoryAvatar feeder={face} className={cn(initialsSize(px), !front && 'text-transparent [text-shadow:none]')} /> }))} />;
  } else {
    const list = feeds.length ? feeds : [{ id: 'all', title: '', feederCount: 0 }];
    body = <Stack box={box} items={list.slice(0, 3).map((feed) => ({ key: feed.id, draw: (px, front) => <FeedDisc title={feed.title} px={px} front={front} /> }))} />;
  }
  return (
    <span className="relative block shrink-0" style={{ width: box, height: box }}>
      {body}
      {lastPostAt ? (
        <span
          className={cn(
            'absolute left-1/2 top-full z-10 -translate-x-1/2 -translate-y-1/2 whitespace-nowrap rounded-full px-2 py-[3px] text-[10px] font-black leading-none shadow-[0_0_0_3px_var(--st-cut)]',
            fresh ? 'bg-[var(--fm-accent)] text-white' : 'bg-[var(--st-surface-2)] text-[var(--st-text-2)]',
          )}
        >
          {relativeTime(lastPostAt)}
        </span>
      ) : null}
    </span>
  );
}

/* ── the rows ─────────────────────────────────────────────────────────────── */

function TitleBlock({ title, subtitle, wide, center = false }: { title: string; subtitle: string; wide: boolean; center?: boolean }) {
  return (
    <div className={cn('min-w-0', center && 'text-center')}>
      <h2 className={cn('truncate font-black tracking-[-0.04em] text-[var(--st-text)]', wide ? 'text-[28px] leading-[30px]' : 'text-[22px] leading-[24px]')}>{title}</h2>
      {subtitle ? (
        <p className={cn('mt-1.5 truncate font-medium text-[var(--st-text-2)]', wide ? 'text-[14px] leading-[20px]' : 'text-[12px] leading-[18px]')}>{subtitle}</p>
      ) : null}
    </div>
  );
}

function TodayLine({ today, center = false }: { today: StreamProfile['today'] | null; center?: boolean }) {
  if (!today) return <span className={cn('st-bone mt-1 block h-2.5 w-52 max-w-full', center && 'mx-auto')} />;
  const live = today.posts + today.results > 0;
  return (
    <p className={cn('flex min-w-0 items-center gap-1.5 text-[12px] font-medium leading-[18px] text-[var(--st-text-2)]', center && 'justify-center')}>
      {/* something new today: "Today" in a red pill (red is never a pick) */}
      <b className={cn('shrink-0 font-bold', live ? 'rounded-full bg-[var(--fm-accent)] px-1.5 py-[3px] text-[11px] leading-none text-white' : 'text-[var(--st-text)]')}>Today</b>
      <span className="truncate">
        {'· '}
        <b className="font-bold tabular-nums text-[var(--st-text)]">{today.posts}</b> {plural(today.posts, 'post')} went up
        {' · '}
        <b className="font-bold tabular-nums text-[var(--st-text)]">{today.results}</b> {plural(today.results, 'result')} landed
      </span>
    </p>
  );
}

/* the numbers at two sizes: 'sm' (a wide screen's stacked top, the Day card) and 'md' (a phone, beside the face: support,
   never the lead (the name is): semibold at a quiet size, a hairline between the three, the labels and notes muted).
   A spread top has Instagram's one line of them instead (InlineStats) */
type StatSize = 'sm' | 'md';
const STAT: Record<StatSize, { height: number; value: string; bone: string; top: string; unit: string; label: string; note: string; rule: boolean }> = {
  sm: { height: ROW.stats, value: 'h-[22px] font-black tracking-[-0.04em] text-[18px] leading-[22px]', bone: 'mt-1 h-3.5 w-10', top: 'text-[12px]', unit: 'text-[12px]', label: 'mt-1 text-[12px] leading-4', note: 'mt-0.5', rule: false },
  md: { height: ROW.statsPhone, value: 'h-[24px] font-semibold tracking-[-0.02em] text-[20px] leading-[24px]', bone: 'mt-1 h-4 w-11', top: 'text-[12px]', unit: 'text-[14px]', label: 'mt-1 text-[12px] leading-4', note: 'mt-0.5', rule: true },
};
// a hairline between two numbers, three quarters of their height
const STAT_RULE = "relative before:absolute before:left-0 before:top-[12%] before:h-[76%] before:w-px before:bg-[var(--st-line)] first:before:hidden";

function StatsRow({ profile, center, size = 'sm', className }: { profile: StreamProfile | null; center: boolean; size?: StatSize; className?: string }) {
  const look = STAT[size];
  const typical = profile && ranked(profile.typical) ? formatTopPercent(profile.typical).slice(0, -1) : null;
  const stats: Array<{ key: string; value: ReactNode; label: string; note: Note }> = [
    {
      key: 'posts',
      value: profile ? postsValue(profile.posts) : null,
      label: plural(profile?.posts ?? 0, 'post'),
      note: profile ? { text: `${STREAM_WINDOW_DAYS} days`, up: false } : null,
    },
    {
      key: 'followers',
      value: profile ? formatCount(profile.followers) : null,
      label: 'followers',
      note: profile ? followerNote(profile.followerDelta30d) : null,
    },
    {
      key: 'typical',
      value: profile ? (typical ? (
        <>
          <span className={cn('mr-1 font-medium tracking-normal text-[var(--st-text-2)]', look.top)}>Top</span>
          {typical}
          <span className={cn('tracking-normal', look.unit)}>%</span>
        </>
      ) : '—') : null,
      label: 'typical post',
      note: profile ? typicalNote(profile.typical, profile.typicalPrev) : null,
    },
  ];
  // a phone's column is narrow: "▲0.3% 30d", the glyph tight to its number
  const noteText = (text: string) => (size === 'md' ? text.replace(' · ', ' ') : text);
  return (
    <div className={cn('grid grid-cols-3', look.rule ? 'gap-0' : 'gap-1', className)} style={{ height: look.height }}>
      {stats.map((stat) => (
        <div
          key={stat.key}
          className={cn(
            'flex min-w-0 flex-col justify-center',
            center ? 'items-center text-center' : 'items-start',
            look.rule && STAT_RULE,
            look.rule && !center && 'pl-5 pr-2 first:pl-0',
          )}
        >
          <span className={cn('block max-w-full truncate tabular-nums text-[var(--st-text)]', look.value)}>
            {stat.value ?? <span className={cn('st-bone inline-block align-top', look.bone)} />}
          </span>
          <span className={cn('block max-w-full truncate font-medium text-[var(--st-text-2)]', look.label)}>{stat.label}</span>
          {/* a rise: the ▲ carries the accent, the words stay ink (small type is ink). The arrow is a glyph, drawn at
              8px as Lead's are, so the 12px words fit a phone's column */}
          <span className={cn('block h-4 max-w-full truncate text-[12px] font-semibold leading-4', look.note, stat.note?.up ? 'text-[var(--st-text)]' : 'text-[var(--st-text-3)]')}>
            {stat.note && /^[▲▼] /.test(stat.note.text) ? (
              <>
                <span className={cn('mr-0.5 align-[1px] text-[8px] leading-none', stat.note.up && 'text-[var(--fm-accent)]')}>{stat.note.text[0]}</span>
                {noteText(stat.note.text.slice(2))}
              </>
            ) : (stat.note ? noteText(stat.note.text) : '')}
          </span>
        </div>
      ))}
    </div>
  );
}

/* Instagram's line of numbers, under the name on a spread top: each number in semibold before its word, its move after
   it in small muted type (the ▲ carries the accent) */
function InlineStats({ profile }: { profile: StreamProfile | null }) {
  const typical = profile && ranked(profile.typical) ? formatTopPercent(profile.typical) : null;
  const items: Array<{ key: string; value: string | null; label: string; note: Note }> = [
    { key: 'posts', value: profile ? postsValue(profile.posts) : null, label: plural(profile?.posts ?? 0, 'post'), note: null },
    { key: 'followers', value: profile ? formatCount(profile.followers) : null, label: 'followers', note: profile ? followerNote(profile.followerDelta30d) : null },
    { key: 'typical', value: profile ? (typical ? `Top ${typical}` : '—') : null, label: 'typical post', note: profile ? typicalNote(profile.typical, profile.typicalPrev) : null },
  ];
  return (
    <p className="flex flex-wrap items-baseline gap-x-6 gap-y-1 text-[14px] leading-6 text-[var(--st-text-2)]">
      {items.map((item) => (
        <span key={item.key} className="whitespace-nowrap">
          {item.value == null ? <span className="st-bone mr-1.5 inline-block h-3.5 w-10 align-middle" /> : <b className="mr-1 text-[16px] font-semibold tabular-nums text-[var(--st-text)]">{item.value}</b>}
          {item.label}
          {item.note ? (
            <span className={cn('ml-2 text-[12px] font-semibold', item.note.up ? 'text-[var(--st-text-2)]' : 'text-[var(--st-text-3)]')}>
              {/^[▲▼] /.test(item.note.text) ? (
                <>
                  <span className={cn('mr-0.5 align-[1px] text-[8px] leading-none', item.note.up && 'text-[var(--fm-accent)]')}>{item.note.text[0]}</span>
                  {item.note.text.slice(2).replace(' · ', ' ')}
                </>
              ) : item.note.text}
            </span>
          ) : null}
        </span>
      ))}
    </p>
  );
}

/* a highlight: a little Instagram circle (a neutral ring, a gap, the cover), its number in a badge over the
   ring's foot, its label under it */
function Highlight({ item, onOpenPost }: { item: StreamHighlight; onOpenPost: (key: string) => void }) {
  const top = ranked(item.topPercent) ? formatTopPercent(item.topPercent) : null;
  // no picture to show, or it failed: the post's drawn cover (its feeder's tone and initials)
  const [failed, setFailed] = useState<string | null>(null);
  const picture = item.thumbnailUrl && item.thumbnailUrl !== failed ? item.thumbnailUrl : null;
  return (
    <button
      type="button"
      onClick={() => onOpenPost(item.key)}
      {...press}
      aria-label={`${item.label}: @${item.handle}${top ? `, top ${top}` : ''}`}
      className="group flex min-w-[72px] max-w-[96px] shrink-0 flex-col items-center rounded-[14px] outline-none [-webkit-tap-highlight-color:transparent] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
    >
      <span className="relative block h-[62px] w-[62px] shrink-0">
        <span aria-hidden="true" className="absolute inset-0 rounded-full shadow-[inset_0_0_0_1.5px_rgb(var(--fm-fg-rgb)/0.2)]" />
        <span className="absolute inset-[4.5px] block overflow-hidden rounded-full bg-[var(--st-surface)] transition-transform duration-200 ease-out group-data-[pressed]:scale-[0.94]">
          {picture ? (
            // eslint-disable-next-line @next/next/no-img-element -- the stable media proxy, not a static asset
            <img
              src={picture}
              alt=""
              draggable={false}
              loading="lazy"
              decoding="async"
              data-media-fallback="off"
              onError={() => setFailed(picture)}
              className="block h-full w-full object-cover"
            />
          ) : (
            <PostCover handle={item.handle} chip />
          )}
        </span>
        {top ? (
          <span className={cn('absolute left-1/2 top-full -translate-x-1/2 -translate-y-1/2 whitespace-nowrap rounded-full px-1.5 py-[3px] text-[10px] font-black leading-none tabular-nums shadow-[0_0_0_2px_var(--st-cut)]', isWinner(item.topPercent) ? 'bg-[var(--fm-accent)] text-white' : 'bg-(--fm-raised) text-fg')}>
            {top}
          </span>
        ) : null}
      </span>
      <span className="mt-3 block max-w-full truncate whitespace-nowrap text-center text-[12px] font-medium leading-[18px] text-[var(--st-text-2)]">{item.label}</span>
    </button>
  );
}

function HighlightBone() {
  return (
    <span aria-hidden="true" className="flex w-[72px] shrink-0 flex-col items-center">
      <span className="relative block h-[62px] w-[62px] shrink-0">
        <span className="absolute inset-0 rounded-full shadow-[inset_0_0_0_1.5px_rgb(var(--fm-fg-rgb)/0.1)]" />
        <span className="st-bone absolute inset-[4.5px] rounded-full" />
      </span>
      <span className="st-bone mt-[17px] block h-2 w-10" />
    </span>
  );
}

function Highlights({ items, loading, center, onOpenPost }: { items: StreamHighlight[]; loading: boolean; center: boolean; onOpenPost: (key: string) => void }) {
  let body: ReactNode;
  if (loading) body = Array.from({ length: 5 }, (_, index) => <HighlightBone key={index} />);
  else if (!items.length) body = <span className="flex h-full items-center text-[12px] font-medium text-[var(--st-text-3)]">No highlights yet</span>;
  else body = items.slice(0, 5).map((item) => <Highlight key={item.key} item={item} onOpenPost={onOpenPost} />);
  return (
    <div className="hide-scrollbar overflow-x-auto overflow-y-hidden overscroll-x-contain" style={{ height: ROW.highlights }}>
      <div className={cn('flex h-full w-max gap-2', center && 'mx-auto')}>{body}</div>
    </div>
  );
}

/* ── the way into Read and Lead ───────────────────────────────────────────── */

const MOMENTUM: Record<Momentum, { text: string; glyph: '▲' | '▼' | null }> = {
  up: { text: 'On the up', glyph: '▲' },
  down: { text: 'Sliding', glyph: '▼' },
  level: { text: 'Holding', glyph: null },
  early: { text: 'Too early to call', glyph: null },
};
// a door is the whole row: it gives a little under the finger (press: never while the page scrolls)
const DOOR = 'flex w-full min-w-0 text-left outline-none [-webkit-tap-highlight-color:transparent] transition-transform duration-150 ease-out data-[pressed]:scale-[0.985] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]';
// from a laptop up the two doors sit side by side, each on its own surface (TILE_PAD a side)
const TILE_PAD = 16;
const TILE = 'rounded-[18px] bg-[var(--st-surface-2)] p-4 shadow-[inset_0_0_0_1px_var(--st-line)]';
const CHEVRON = 'h-3.5 w-3.5 shrink-0 text-[var(--st-text-3)]';

// a run's box in Read's shades (its typical post's top %), the run the move is read from ringed in ink; or an empty slot
function RunBox({ run, ring, big, small = false, large = false }: { run: BoardRun | null; ring: boolean; big: boolean; small?: boolean; large?: boolean }) {
  const edge = ring ? 'inset 0 0 0 1.5px rgb(var(--fm-fg-rgb) / 0.62)' : null;
  if (!run || run.tp == null) {
    return <span className="block min-w-0 flex-1 rounded-[6px]" style={{ boxShadow: edge ?? 'inset 0 0 0 1px var(--st-line)' }} />;
  }
  const tone = shade(run.tp);
  return (
    <span
      className={cn('flex min-w-0 flex-1 items-center justify-center overflow-hidden rounded-[6px] font-bold leading-none tracking-[-0.02em] tabular-nums', big || large ? 'text-[14px]' : small ? 'text-[10px]' : 'text-[12px]')}
      style={{ background: tone.c, color: tone.tx, boxShadow: [edge, `inset 0 1px 0 rgba(255, 255, 255, ${tone.gi})`].filter(Boolean).join(', ') }}
    >
      {run.tp}
    </span>
  );
}

const slots = <T,>(items: T[]) => [...items, ...Array.from({ length: Math.max(0, RUN_SLOTS - items.length) }, () => null)].slice(0, RUN_SLOTS);

/* Read's door: a feeder's runs of 10 oldest first, as Read's board draws them, with which way the latest moved; a feed
   or all feeds, each feeder's latest run (strongest first) with how the group splits. On a phone (compact) it is one
   line: what it is and where it stands on the left, the boxes beside it. `cell` is a box's width */
const READ_LABEL_PX = 88;
type Glyph = '▲' | '▼' | null;
// what Read's door says and draws, as a row (compact: the phone's short words) or as a card
function readParts(door: ReadDoor | null | undefined, compact: boolean) {
  let marker = `Runs of ${RUN_SIZE}`;
  let note: { text: string; glyph: Glyph } | null = null;
  let boxes: Array<{ key: string; run: BoardRun | null; ring: boolean }> = slots<BoardRun>([]).map((_, k) => ({ key: `e${k}`, run: null, ring: false }));
  let label = 'Read';
  if (door?.kind === 'feeder') {
    const { row } = door;
    note = MOMENTUM[row.momentum];
    boxes = slots(row.runs ?? []).map((run, k) => ({ key: run ? `r${run.r}` : `e${k}`, run, ring: Boolean(run && run === row.now) }));
    label = `Read: @${row.handle}'s runs of ${RUN_SIZE}, ${note.text.toLowerCase()}${row.now?.tp != null ? `, latest typical post top ${row.now.tp}%` : ''}`;
  } else if (door?.kind === 'group') {
    marker = 'Latest runs';
    // the phone's line is short: who is on the up (or sliding) stands for the group's split
    const short = door.up ? `${door.up} on the up` : door.down ? `${door.down} sliding` : 'Holding';
    note = { text: compact ? short : door.head, glyph: compact && door.up ? '▲' : compact && door.down ? '▼' : null };
    boxes = slots(door.latest ?? []).map((item, k) => ({ key: item ? item.handle : `e${k}`, run: item?.run ?? null, ring: false }));
    label = `Read: ${door.head}. Each feeder's latest run, strongest first`;
  }
  return { marker, note, boxes, label };
}

function Note({ note, empty }: { note: { text: string; glyph: Glyph } | null; empty: string }) {
  return (
    <span className={cn('block truncate', !note && 'text-[var(--st-text-3)]')}>
      {note?.glyph ? <span className={cn('mr-0.5 align-[1px] text-[8px] leading-none', note.glyph === '▲' && 'text-[var(--fm-accent)]')}>{note.glyph}</span> : null}
      {note ? note.text : empty}
    </span>
  );
}

function ReadDoorRow({ door, onRead, cell, tile, compact = false, height }: { door: ReadDoor | null | undefined; onRead: () => void; cell: number; tile: boolean; compact?: boolean; height: number | string }) {
  const big = cell >= 28;
  const { marker, note, boxes, label } = readParts(door, compact);
  const where = door === undefined ? <span className="st-bone block h-2.5 w-20" /> : <Note note={note} empty="Nothing yet" />;
  const row = (
    <span aria-hidden="true" className={cn('flex gap-[3px]', compact ? 'h-[30px] min-w-0 flex-1' : tile ? 'mt-2.5 h-[32px] w-full' : 'mt-2 h-[30px] w-full')}>
      {door === undefined
        ? boxes.map((box) => <span key={box.key} className="st-bone block min-w-0 flex-1" />)
        : boxes.map((box) => <RunBox key={box.key} run={box.run} ring={box.ring} big={big} small={cell < 22} large={tile && cell >= 40} />)}
    </span>
  );
  if (compact) {
    return (
      <button type="button" onClick={onRead} {...press} aria-label={`${label}. Open Read`} className={cn(DOOR, 'items-center gap-2.5', tile && TILE)} style={{ height }}>
        <span className="flex shrink-0 flex-col gap-1" style={{ width: READ_LABEL_PX }}>
          <span className={cn(MARKER, 'block truncate leading-[12px]')}>{marker}</span>
          <span className="block text-[12px] font-semibold leading-4 text-[var(--st-text)]">{where}</span>
        </span>
        {row}
        <ChevronRight className={CHEVRON} strokeWidth={2.4} aria-hidden="true" />
      </button>
    );
  }
  return (
    <button type="button" onClick={onRead} {...press} aria-label={`${label}. Open Read`} className={cn(DOOR, 'flex-col', tile && TILE)} style={{ height }}>
      <span className="flex h-[14px] w-full items-center justify-between gap-3">
        <span className={cn(MARKER, 'block shrink-0 leading-[14px]')}>{marker}</span>
        <span className="flex min-w-0 items-center gap-1 text-[12px] font-semibold leading-4 text-[var(--st-text)]">
          {where}
          <ChevronRight className={CHEVRON} strokeWidth={2.4} aria-hidden="true" />
        </span>
      </span>
      {row}
    </button>
  );
}

/* Lead's door: the first win of its Hall of Feed for the same pick and window, with the post it stands on. Tapping it opens
   Lead at the Hall (LEAD_AT_KEY) */
// Lead opens at its Hall of Feed (LEAD_AT_KEY), on the pick the tabs share
function openHall(onLead: () => void) {
  try {
    sessionStorage.setItem(LEAD_AT_KEY, 'hall');
  } catch {}
  onLead();
}

function HallCover({ win, className, style }: { win: HallFact | null | undefined; className?: string; style?: CSSProperties }) {
  const [failed, setFailed] = useState<string | null>(null);
  const picture = win?.thumbnailUrl && win.thumbnailUrl !== failed ? win.thumbnailUrl : null;
  return (
    <span className={cn('relative block shrink-0 overflow-hidden rounded-[12px] bg-[var(--st-surface)] shadow-[inset_0_0_0_1px_var(--st-line)]', className)} style={style}>
      {win === undefined ? <span className="st-bone absolute inset-0" /> : picture ? (
        // eslint-disable-next-line @next/next/no-img-element -- the post's own thumbnail, as Lead's Hall draws it
        <img src={picture} alt="" draggable={false} loading="lazy" decoding="async" data-media-fallback="off" onError={() => setFailed(picture)} className="block h-full w-full object-cover" />
      ) : (
        <span className="grid h-full w-full place-items-center text-[var(--st-text-3)]"><Trophy className="h-5 w-5" strokeWidth={2.2} aria-hidden="true" /></span>
      )}
    </span>
  );
}

const hallLabel = (win: HallFact | null | undefined) => {
  const kind = win ? WIN_LABEL[win.kind] ?? null : null;
  return { kind, aria: `${win ? `Lead, Hall of Feed${kind ? `, ${kind}` : ''}: ${win.headline}` : 'Lead: Hall of Feed'} Open Lead` };
};

function LeadDoorRow({ win, onLead, tile, height }: { win: HallFact | null | undefined; onLead: () => void; tile: boolean; height: number | string }) {
  const { kind, aria } = hallLabel(win);
  const cover = tile ? 56 : 48;
  return (
    <button type="button" onClick={() => openHall(onLead)} {...press} aria-label={aria} className={cn(DOOR, 'items-center gap-3', tile && TILE)} style={{ height }}>
      <HallCover win={win} style={{ width: cover, height: cover }} />
      <span className="flex min-w-0 flex-1 flex-col justify-center gap-1">
        <span className={cn(MARKER, 'block truncate leading-[12px]')}>{kind ? `Hall of Feed · ${kind}` : 'Hall of Feed'}</span>
        {win === undefined ? (
          <span aria-hidden="true" className="flex flex-col gap-1.5 pt-0.5">
            <span className="st-bone block h-2.5 w-full max-w-[240px]" />
            <span className="st-bone block h-2.5 w-2/3" />
          </span>
        ) : (
          <span className={cn('block overflow-hidden font-semibold [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2]', tile ? 'text-[14px] leading-5' : 'text-[12px] leading-4', win ? 'text-[var(--st-text)]' : 'text-[var(--st-text-3)]')}>
            {win ? win.headline : `No wins in the last ${HALL_DAYS} days yet`}
          </span>
        )}
      </span>
      <ChevronRight className={cn(CHEVRON, 'self-start')} strokeWidth={2.4} aria-hidden="true" />
    </button>
  );
}

/* a spread top's right column: the two doors as cards, one over the other, sharing the top's height. Each says a little
   more than a row: Read's what it is, where the account stands, then its boxes taller; Lead's the post's cover, the win,
   and the line under it in Lead's Hall */
function ReadDoorCard({ door, onRead, cell }: { door: ReadDoor | null | undefined; onRead: () => void; cell: number }) {
  const { marker, note, boxes, label } = readParts(door, false);
  // a feeder: the move, then where the latest run sits ("▲ On the up · top 42%, was 55%")
  const status = door?.kind === 'feeder' && note && door.row.now?.tp != null ? { ...note, text: `${note.text} · ${noteFor(door.row).core}` } : note;
  return (
    <button type="button" onClick={onRead} {...press} aria-label={`${label}. Open Read`} className={cn(DOOR, TILE, 'min-h-0 flex-1 flex-col justify-between')}>
      <span className="flex w-full items-center justify-between gap-3">
        <span className={cn(MARKER, 'block truncate leading-[14px]')}>{marker}</span>
        <ChevronRight className={CHEVRON} strokeWidth={2.4} aria-hidden="true" />
      </span>
      <span className="mt-1.5 block w-full text-[14px] font-semibold leading-5 text-[var(--st-text)]">
        {door === undefined ? <span className="st-bone mt-1.5 block h-3 w-40" /> : <Note note={status} empty="Nothing to read yet" />}
      </span>
      <span aria-hidden="true" className="mt-3 flex h-[40px] w-full gap-[3px]">
        {door === undefined
          ? boxes.map((box) => <span key={box.key} className="st-bone block min-w-0 flex-1" />)
          : boxes.map((box) => <RunBox key={box.key} run={box.run} ring={box.ring} big={cell >= 28} small={cell < 22} />)}
      </span>
    </button>
  );
}

// `cover`: the cover's side on a narrow column (its words need the room); otherwise the cover is the card's height
function LeadDoorCard({ win, onLead, cover }: { win: HallFact | null | undefined; onLead: () => void; cover: number | null }) {
  const { kind, aria } = hallLabel(win);
  return (
    <button type="button" onClick={() => openHall(onLead)} {...press} aria-label={aria} className={cn(DOOR, TILE, 'min-h-0 flex-1 items-stretch gap-3')}>
      {cover ? <HallCover win={win} className="self-center" style={{ width: cover, height: cover }} /> : <HallCover win={win} className="aspect-square h-full" />}
      <span className="flex min-w-0 flex-1 flex-col justify-center gap-1.5">
        <span className="flex w-full items-center justify-between gap-3">
          <span className={cn(MARKER, 'block truncate leading-[14px]')}>{kind ? `Hall of Feed · ${kind}` : 'Hall of Feed'}</span>
          <ChevronRight className={CHEVRON} strokeWidth={2.4} aria-hidden="true" />
        </span>
        {win === undefined ? (
          <span aria-hidden="true" className="flex flex-col gap-1.5 pt-0.5">
            <span className="st-bone block h-3 w-full max-w-[260px]" />
            <span className="st-bone block h-3 w-2/3" />
          </span>
        ) : (
          <>
            <span className={cn('block overflow-hidden text-[14px] font-semibold leading-5 [display:-webkit-box] [-webkit-box-orient:vertical] [-webkit-line-clamp:2]', win ? 'text-[var(--st-text)]' : 'text-[var(--st-text-3)]')}>
              {win ? win.headline : `No wins in the last ${HALL_DAYS} days yet`}
            </span>
            {win?.detail ? <span className="block truncate text-[12px] font-medium leading-4 text-[var(--st-text-3)]">{win.detail}</span> : null}
          </>
        )}
      </span>
    </button>
  );
}

function ManageButton({ onManage, className }: { onManage: () => void; className?: string }) {
  return (
    <button type="button" onClick={onManage} aria-label="Manage feeds and feeders" className={cn(ACTION, 'w-11 flex-none px-0', className)}>
      <Ellipsis className="h-5 w-5" strokeWidth={2.4} aria-hidden="true" />
    </button>
  );
}

/* the order, as Instagram's tab row under a bio: two equal tabs, the current one over an ink underline that
   glides between them (transform), the other dimmed; a hairline under the row. `order`: the one tapped (its label
   lights at once); `shown`: the one whose list is on screen (the underline is under it) */
function OrderTabs({ order, shown, onChange }: { order: FeedOrder; shown: FeedOrder; onChange: (order: FeedOrder) => void }) {
  const { play } = useAppHaptics();
  const pick = (next: FeedOrder) => {
    if (next === order) return;
    play('snapLock');
    onChange(next);
  };
  return (
    <div
      role="tablist"
      aria-label="Order"
      className="relative -mx-[var(--st-inset)] grid grid-cols-2 shadow-[inset_0_-1px_0_var(--st-line)]"
      style={{ height: ROW.tabs }}
    >
      {ORDERS.map(({ value, label, Icon }) => {
        const on = value === order;
        return (
          <button
            key={value}
            type="button"
            role="tab"
            aria-selected={on}
            onClick={() => pick(value)}
            className="group flex h-full min-w-0 items-center justify-center px-2 outline-none [-webkit-tap-highlight-color:transparent] focus-visible:bg-fg/[0.05]"
          >
            <span className={cn('flex min-w-0 items-center gap-1.5 text-[14px] font-bold leading-none text-[var(--st-text)] transition-[opacity,scale] duration-[calc(300ms*var(--fm-tempo)),300ms] group-active:scale-[0.96]', EASE, on ? 'opacity-100' : 'opacity-60')}>
              <Icon className="h-4 w-4 shrink-0" strokeWidth={2.4} aria-hidden="true" />
              <span className="truncate">{label}</span>
            </span>
          </button>
        );
      })}
      {/* (data-order-glide: FeedStream holds it with the list's slide on WebKit, and lands it with the slide) */}
      <span
        aria-hidden="true"
        data-order-glide=""
        className={cn('pointer-events-none absolute bottom-0 left-0 h-[2px] w-1/2 bg-[var(--st-text)] transition-transform motion-reduce:transition-none', SLIDE_GLIDE)}
        style={{ transform: shown === 'results' ? 'translateX(100%)' : 'translateX(0%)' }}
      />
    </div>
  );
}

/* the card's order: a compact two-way pill, the current half filled ink (the fill glides, transform only, with the list).
   The labels are drawn twice: in ink on the well, and in the page's colour inside the fill, held still while it glides
   (they move back by as much as it moves on), so each turns over exactly where the fill's edge crosses it */
function OrderPill({ order, shown, onChange }: { order: FeedOrder; shown: FeedOrder; onChange: (order: FeedOrder) => void }) {
  const { play } = useAppHaptics();
  const pick = (next: FeedOrder) => {
    if (next === order) return;
    play('snapLock');
    onChange(next);
  };
  return (
    <div
      role="tablist"
      aria-label="Order"
      className="relative grid h-9 w-[240px] max-w-full grid-cols-2 rounded-[14px] border border-fg/[0.07] bg-(--fm-well) p-[3px] shadow-(--fm-well-shade)"
    >
      {ORDERS.map(({ value, label }) => {
        const on = value === order;
        return (
          <button
            key={value}
            type="button"
            role="tab"
            aria-selected={on}
            onClick={() => pick(value)}
            // (an outset ring: it shows in the well's padding round the fill, which lies over the selected button; the
            // other one, focused, comes up over the fill, so its ring shows whole where the two meet)
            className={cn('relative z-10 grid h-full min-w-0 place-items-center rounded-[10px] px-2 text-[12px] font-bold leading-none text-fg outline-none [-webkit-tap-highlight-color:transparent] transition-transform duration-150 ease-out active:scale-[0.96] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]', value !== shown && 'focus-visible:z-30')}
          >
            <span className={cn('truncate transition-opacity', GLIDE, on ? 'opacity-100' : 'opacity-60')}>{label}</span>
          </button>
        );
      })}
      <span
        aria-hidden="true"
        data-order-glide=""
        className={cn('pointer-events-none absolute bottom-[3px] left-[3px] top-[3px] z-20 w-[calc(50%-3px)] overflow-hidden rounded-[10px] bg-fg shadow-[inset_0_1px_0_rgb(255_255_255_/_0.16)] transition-transform motion-reduce:transition-none', SLIDE_GLIDE)}
        style={{ transform: shown === 'results' ? 'translateX(100%)' : 'translateX(0%)' }}
      >
        <span
          data-order-glide=""
          className={cn('absolute left-[-3px] top-[-3px] grid h-[calc(100%+6px)] w-[calc(200%+6px)] grid-cols-2 p-[3px] text-(--fm-page) transition-transform motion-reduce:transition-none', SLIDE_GLIDE)}
          style={{ transform: shown === 'results' ? 'translateX(calc((100% - 6px) / -2))' : 'translateX(0px)' }}
        >
          {ORDERS.map(({ value, label }) => (
            <span key={value} className="grid min-w-0 place-items-center px-2 text-[12px] font-bold leading-none">
              <span className="truncate">{label}</span>
            </span>
          ))}
        </span>
      </span>
    </div>
  );
}

/* ── the two variants ─────────────────────────────────────────────────────── */

function ProfilePage({ scope, title, subtitle, feeder, feeds, faces, profile, fresh, order, onOrderChange, width, onLead, onRead, onManage, onOpenPost }: ProfileHeaderProps) {
  const kind = kindOf(scope);
  const frame = profileFrame(width, scope);
  const loading = profile == null;
  const doors = useProfileDoors(scope);
  const content = width - 8;
  const face = <FaceBlock kind={kind} size="page" scope={scope} feeder={feeder} feeds={feeds} faces={faces} fresh={fresh} lastPostAt={profile?.lastPostAt ?? null} />;
  const highlights = <Highlights items={profile?.highlights ?? []} loading={loading} center={false} onOpenPost={onOpenPost} />;
  const tabs = <OrderTabs order={order} shown={scope.order ?? 'posted'} onChange={onOrderChange} />;
  const style = { width, height: frame.height, '--st-cut': 'var(--st-bg)' } as CSSProperties;

  if (!frame.wide) {
    // ten boxes beside the door's label (and its gap and chevron), 3px apart
    const cell = (content - READ_LABEL_PX - 10 - 10 - 14 - (RUN_SLOTS - 1) * 3) / RUN_SLOTS;
    return (
      <section aria-label={title} className="relative flex flex-col px-[var(--st-inset)]" style={style}>
        <div className="flex items-center gap-3" style={{ marginTop: ROW.pad, height: ROW.face }}>
          {face}
          <StatsRow profile={profile} center size="md" className="min-w-0 flex-1" />
        </div>
        {/* on a phone the name carries the settings button alone: the way to Read and Lead is the two rows under the
            highlights, each as short as it can be, so the profile stays one stop of the Day view's snap */}
        <div className="flex items-center gap-3" style={{ marginTop: ROW.gap, height: ROW.title }}>
          <div className="min-w-0 flex-1">
            <TitleBlock title={title} subtitle={subtitle} wide={false} />
          </div>
          <ManageButton onManage={onManage} />
        </div>
        {kind === 'all' ? (
          <div className="pt-1.5" style={{ height: ROW.today }}>
            <TodayLine today={profile?.today ?? null} />
          </div>
        ) : null}
        <div style={{ marginTop: ROW.gap + 4 }}>{highlights}</div>
        <div style={{ marginTop: ROW.gap }}>
          <ReadDoorRow door={doors.read} onRead={onRead} cell={cell} tile={false} compact height={ROW.read} />
        </div>
        <div style={{ marginTop: ROW.doorGap }}>
          <LeadDoorRow win={doors.lead} onLead={onLead} tile={false} height={ROW.lead} />
        </div>
        <div style={{ marginTop: ROW.gap }}>{tabs}</div>
      </section>
    );
  }

  // the two doors side by side, 12px apart, each a tile
  const cell = ((content - 12) / 2 - TILE_PAD * 2 - (RUN_SLOTS - 1) * 3) / RUN_SLOTS;
  const doorRow = (
    <div className="grid grid-cols-2 gap-3" style={{ marginTop: ROW.wideGap, height: ROW.doorsWide }}>
      <ReadDoorRow door={doors.read} onRead={onRead} cell={cell} tile height="100%" />
      <LeadDoorRow win={doors.lead} onLead={onLead} tile height="100%" />
    </div>
  );

  /* a wide column: two sides. On the left, Instagram's profile, tucked in: the face; beside it the name (with the
     settings), the numbers on one line, the bio (the line under the name, all feeds' Today); the highlights under it all.
     On the right, the two doors as cards, one over the other, the left side's height between them, so both sides start
     and end on the same lines */
  if (frame.spread) {
    const doorsWidth = Math.min(ROW.doorsColumnMax, Math.max(ROW.doorsColumnMin, Math.round(content * 0.34)));
    const doorCell = (doorsWidth - TILE_PAD * 2 - (RUN_SLOTS - 1) * 3) / RUN_SLOTS;
    const faceSize: Size = content >= ROW.bigFaceFrom ? 'spread' : 'page';
    return (
      <section aria-label={title} className="relative flex flex-col px-[var(--st-inset)]" style={style}>
        <div className="grid gap-10" style={{ marginTop: ROW.pad, height: frame.hero, gridTemplateColumns: `minmax(0, 1fr) ${doorsWidth}px` }}>
          <div className="flex min-w-0 flex-col">
            <div className="flex min-w-0 items-center gap-7" style={{ height: ROW.profileTop }}>
              <FaceBlock kind={kind} size={faceSize} scope={scope} feeder={feeder} feeds={feeds} faces={faces} fresh={fresh} lastPostAt={profile?.lastPostAt ?? null} />
              <div className="flex min-w-0 flex-1 flex-col">
                <div className="flex min-w-0 items-center gap-3">
                  <h2 className="min-w-0 truncate text-[28px] font-black leading-[34px] tracking-[-0.04em] text-[var(--st-text)]">{title}</h2>
                  <ManageButton onManage={onManage} className="h-9 w-9 rounded-[10px]" />
                </div>
                <div className="mt-3">
                  <InlineStats profile={profile} />
                </div>
                {subtitle ? <p className="mt-2 truncate text-[14px] font-medium leading-5 text-[var(--st-text-2)]">{subtitle}</p> : null}
                {kind === 'all' ? (
                  <div className="mt-0.5">
                    <TodayLine today={profile?.today ?? null} />
                  </div>
                ) : null}
              </div>
            </div>
            <div style={{ marginTop: ROW.profileGap }}>{highlights}</div>
          </div>
          <div className="flex min-h-0 min-w-0 flex-col gap-3">
            <ReadDoorCard door={doors.read} onRead={onRead} cell={doorCell} />
            <LeadDoorCard win={doors.lead} onLead={onLead} cover={doorsWidth < ROW.fullCoverFrom ? 80 : null} />
          </div>
        </div>
        <div style={{ marginTop: ROW.gap + 6 }}>{tabs}</div>
      </section>
    );
  }

  return (
    <section aria-label={title} className="relative flex flex-col px-[var(--st-inset)]" style={style}>
      <div className="flex items-center gap-8" style={{ marginTop: ROW.pad, height: frame.hero }}>
        {face}
        <div className="flex min-w-0 flex-1 flex-col">
          <div className="flex items-center gap-6" style={{ height: ROW.titleWide }}>
            <div className="min-w-0 flex-1">
              <TitleBlock title={title} subtitle={subtitle} wide />
            </div>
            <ManageButton onManage={onManage} />
          </div>
          <div style={{ marginTop: ROW.gap }}>
            <StatsRow profile={profile} center={false} className="max-w-[460px]" />
          </div>
          {kind === 'all' ? (
            <div className="pt-3" style={{ height: ROW.todayWide }}>
              <TodayLine today={profile?.today ?? null} />
            </div>
          ) : null}
        </div>
      </div>
      <div style={{ marginTop: ROW.wideGap }}>{highlights}</div>
      {doorRow}
      <div style={{ marginTop: ROW.gap + 6 }}>{tabs}</div>
    </section>
  );
}

function ProfileCard({ scope, title, subtitle, feeder, feeds, faces, profile, fresh, order, onOrderChange, width, onLead, onRead, onManage, onOpenPost }: ProfileHeaderProps) {
  const kind = kindOf(scope);
  const loading = profile == null;
  const doors = useProfileDoors(scope);
  const stripWidth = Math.min(width - 40, 420);
  return (
    <section
      aria-label={title}
      className="st-profile-card relative flex h-full flex-col items-center justify-between overflow-hidden rounded-[28px] px-5 pb-4 pt-5 text-center shadow-[inset_0_0_0_1px_var(--st-line)] light:shadow-[inset_0_0_0_1px_var(--st-line),0_18px_40px_-26px_rgb(15_23_42/0.3)]"
      // inside the card, what is raised sits in the card's own inset tone (on light: grey on a white card)
      style={{ width, background: CARD_BG, '--st-cut': 'var(--st-card)', '--st-surface-2': 'var(--st-card-inset)' } as CSSProperties}
    >
      {/* the settings: the only way to feeds, feeders, the plan and the appearance from here, so always on the card */}
      <ManageButton onManage={onManage} className="absolute right-3 top-3 z-10" />
      <div className="flex w-full flex-col items-center">
        <FaceBlock kind={kind} size="card" scope={scope} feeder={feeder} feeds={feeds} faces={faces} fresh={fresh} lastPostAt={profile?.lastPostAt ?? null} />
        <div className="mt-4 w-full" style={{ height: ROW.title }}>
          <TitleBlock title={title} subtitle={subtitle} wide={false} center />
        </div>
        {kind === 'all' ? (
          <div className="st-pc-today w-full pt-1.5" style={{ height: ROW.today }}>
            <TodayLine today={profile?.today ?? null} center />
          </div>
        ) : null}
        <StatsRow profile={profile} center className="mt-3 w-full max-w-[360px]" />
        <div className="st-pc-lead mt-3 w-full max-w-[400px]">
          <LeadDoorRow win={doors.lead} onLead={onLead} tile height={ROW.doorsWide} />
        </div>
      </div>

      <div className="flex w-full flex-col items-center">
        <div className="st-pc-highlights w-full">
          <Highlights items={profile?.highlights ?? []} loading={loading} center onOpenPost={onOpenPost} />
        </div>
        <div className="st-pc-strip mt-2.5 w-full" style={{ maxWidth: stripWidth }}>
          <ReadDoorRow door={doors.read} onRead={onRead} cell={(stripWidth - (RUN_SLOTS - 1) * 3) / RUN_SLOTS} tile={false} height={ROW.read} />
        </div>
      </div>

      <div className="flex flex-col items-center gap-2.5">
        <OrderPill order={order} shown={scope.order ?? 'posted'} onChange={onOrderChange} />
        <span className="flex flex-col items-center text-[var(--st-text-2)]">
          <ChevronUp className="h-4 w-4" strokeWidth={2.4} aria-hidden="true" />
          <span className="mt-0.5 text-[12px] font-medium leading-[18px]">
            Swipe up{profile ? ` · ${postsValue(profile.posts)} ${plural(profile.posts, 'post')}` : ''}
          </span>
        </span>
      </div>
    </section>
  );
}

function ProfileHeader(props: ProfileHeaderProps) {
  return props.variant === 'card' ? <ProfileCard {...props} /> : <ProfilePage {...props} />;
}

export default memo(ProfileHeader);
