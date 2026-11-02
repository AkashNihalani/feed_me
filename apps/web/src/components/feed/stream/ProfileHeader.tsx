'use client';

/* ─────────────────────────────────────────────────────────────
   PROFILE HEADER — the top of the feed: whose it is, as an
   Instagram profile, but cooler. Every number comes from the same
   posts and checkpoints the feed shows.

   A feeder: their big face (an accent story ring when they posted in
   the last day, or when their read has a story they haven't watched:
   a tap plays it out of the face, FeederStory) with a time badge. A feed: its feeders' faces,
   stacked. All feeds: the feed badges, stacked, and what happened
   today. Beside or under it: posts, followers and the typical
   post, each with its move. Then the name, highlights as little
   circles, and where the pick stands right now: its place on Lead's
   board for the last 30 days (profileStanding), a feeder's own, or
   who leads. The posts start right under it, newest first, as
   Instagram's grid does.

   It sits above the grids at a fixed height for its width
   (profileHeaderSize: loaded or not, nothing below it moves). While
   the profile loads the frame is drawn with still placeholders, never
   a number, and the rows keep their heights whatever arrives, so a
   phone's snapping Day view never moves under the finger. Nothing
   here animates but presses.
   ───────────────────────────────────────────────────────────── */

import { memo, useState, type CSSProperties, type ReactNode } from 'react';
import { Ellipsis } from 'lucide-react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { FeederStoryLayer, useFeederStory } from '@/components/feed/stream/FeederStory';
import { useStanding } from '@/components/feed/stream/profileStanding';
import PostCover from '@/components/feed/stream/PostCover';
import { press } from '@/components/feed/stream/press';
import { PROFILE_ROW, profileFrame } from '@/components/feed/stream/profileHeaderSize';
import { STANDING_DAYS, type Standing, type StandingRow } from '@/components/lead/leadStanding';
import {
  STREAM_WINDOW_DAYS,
  formatCount,
  formatTopPercent,
  isWinner,
  relativeTime,
  type ProfileHeaderProps,
  type StreamFeeder,
  type StreamHighlight,
  type StreamProfile,
  type StreamScope,
} from '@/lib/feedStream/contract';
import { feedInitials } from '@/lib/feedLabels';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

type Kind = 'feeder' | 'feed' | 'all';
type Size = 'page' | 'spread';
type Face = { handle: string; profilePicUrl: string | null };
type Note = { text: string; up: boolean } | null;

const ROW = PROFILE_ROW;
// the big face, and the ring round it (3px of ring, 3px of gap); 'spread' is a wide screen's, with the stats beside it
const FACE_PX: Record<Size, number> = { page: 108, spread: 132 };
const RING = 6;
// a feed's badge: the rail's light disc, its initials in ink (the disc is light in both themes)
const DISC_BG = 'linear-gradient(135deg, #f7f7f8, #cfd3dc)';
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

// a rise's ▲ carries the accent, drawn at 8px as Lead's arrows are; the words stay ink
function Arrow({ glyph }: { glyph: string }) {
  return <span className={cn('mr-0.5 align-[1px] text-[8px] leading-none', glyph === '▲' && 'text-[var(--fm-accent)]')}>{glyph}</span>;
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
  // a feeder whose read has a story: it plays out of this face, Instagram's way (FeederStory)
  const handle = kind === 'feeder' ? feeder?.handle ?? scope.handle : null;
  const story = useFeederStory(handle);
  const [faceEl, setFaceEl] = useState<HTMLSpanElement | null>(null);
  let body: ReactNode;
  if (kind === 'feeder') {
    const face = feeder ?? { handle: scope.handle ?? '', profilePicUrl: null };
    body = (
      <>
        {story.ring || fresh ? (
          <span aria-hidden="true" className="st-story-ring absolute inset-0 rounded-full" />
        ) : (
          <span aria-hidden="true" className="absolute inset-[1.5px] rounded-full shadow-[inset_0_0_0_1.5px_rgb(var(--fm-fg-rgb)/0.2)]" />
        )}
        <span ref={setFaceEl} className="absolute block overflow-hidden rounded-full" style={{ inset: RING }}>
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
  const badge = lastPostAt ? (
    <span
      className={cn(
        'absolute left-1/2 top-full z-10 -translate-x-1/2 -translate-y-1/2 whitespace-nowrap rounded-full px-2 py-[3px] text-[10px] font-black leading-none shadow-[0_0_0_3px_var(--st-cut)]',
        fresh ? 'bg-[var(--fm-accent)] text-white' : 'bg-[var(--st-surface-2)] text-[var(--st-text-2)]',
      )}
    >
      {relativeTime(lastPostAt)}
    </span>
  ) : null;
  if (story.acct) {
    return (
      <>
        <button
          type="button"
          {...press}
          onClick={story.start}
          aria-label={handle ? `Watch @${handle}’s story` : 'Watch the story'}
          className="group relative block shrink-0 rounded-full outline-none [-webkit-tap-highlight-color:transparent] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
          style={{ width: box, height: box }}
        >
          <span className="absolute inset-0 block transition-transform duration-200 ease-out group-data-[pressed]:scale-[0.95]">{body}</span>
          {badge}
        </button>
        {story.open ? <FeederStoryLayer acct={story.acct} handle={handle} face={faceEl} onClosed={story.closed} /> : null}
      </>
    );
  }
  return (
    <span className="relative block shrink-0" style={{ width: box, height: box }}>
      {body}
      {badge}
    </span>
  );
}

/* ── the rows ─────────────────────────────────────────────────────────────── */

function TitleBlock({ title, subtitle, wide }: { title: string; subtitle: string; wide: boolean }) {
  return (
    <div className="min-w-0">
      <h2 className={cn('truncate font-black tracking-[-0.045em] text-[var(--st-text)]', wide ? 'text-[30px] leading-[34px]' : 'text-[24px] leading-[28px]')}>{title}</h2>
      {subtitle ? (
        <p className={cn('mt-1.5 truncate font-medium text-[var(--st-text-2)]', wide ? 'text-[14px] leading-[20px]' : 'text-[13px] leading-[18px]')}>{subtitle}</p>
      ) : null}
    </div>
  );
}

function TodayLine({ today }: { today: StreamProfile['today'] | null }) {
  if (!today) return <span className="st-bone mt-1 block h-2.5 w-52 max-w-full" />;
  const live = today.posts + today.results > 0;
  return (
    <p className="flex min-w-0 items-center gap-1.5 text-[12px] font-medium leading-[18px] text-[var(--st-text-2)]">
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

/* the numbers at two sizes: 'sm' (a wide screen's stacked top) and 'md' (a phone, beside the face: support, never the
   lead (the name is): semibold at a quiet size, a hairline between the three, the labels and notes muted). A spread
   top has Instagram's one line of them instead (InlineStats) */
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
          <span className={cn('block h-4 max-w-full truncate text-[12px] font-semibold leading-4', look.note, stat.note?.up ? 'text-[var(--st-text)]' : 'text-[var(--st-text-3)]')}>
            {stat.note && /^[▲▼] /.test(stat.note.text) ? (
              <>
                <Arrow glyph={stat.note.text[0]} />
                {noteText(stat.note.text.slice(2))}
              </>
            ) : (stat.note ? noteText(stat.note.text) : '')}
          </span>
        </div>
      ))}
    </div>
  );
}

/* Instagram's line of numbers, under the name on a spread top: each number in bold before its word, its move after it
   in small muted type (the ▲ carries the accent) */
function InlineStats({ profile }: { profile: StreamProfile | null }) {
  const typical = profile && ranked(profile.typical) ? formatTopPercent(profile.typical) : null;
  const items: Array<{ key: string; value: string | null; label: string; note: Note }> = [
    { key: 'posts', value: profile ? postsValue(profile.posts) : null, label: plural(profile?.posts ?? 0, 'post'), note: null },
    { key: 'followers', value: profile ? formatCount(profile.followers) : null, label: 'followers', note: profile ? followerNote(profile.followerDelta30d) : null },
    { key: 'typical', value: profile ? (typical ? `Top ${typical}` : '—') : null, label: 'typical post', note: profile ? typicalNote(profile.typical, profile.typicalPrev) : null },
  ];
  return (
    <p className="flex flex-wrap items-baseline gap-x-8 gap-y-1 text-[14px] leading-7 text-[var(--st-text-2)]">
      {items.map((item) => (
        <span key={item.key} className="whitespace-nowrap">
          {item.value == null ? <span className="st-bone mr-1.5 inline-block h-4 w-12 align-middle" /> : <b className="mr-1.5 text-[19px] font-bold tracking-[-0.02em] tabular-nums text-[var(--st-text)]">{item.value}</b>}
          {item.label}
          {item.note ? (
            <span className={cn('ml-2 text-[12px] font-semibold', item.note.up ? 'text-[var(--st-text-2)]' : 'text-[var(--st-text-3)]')}>
              {/^[▲▼] /.test(item.note.text) ? (
                <>
                  <Arrow glyph={item.note.text[0]} />
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

function Highlights({ items, loading, onOpenPost }: { items: StreamHighlight[]; loading: boolean; onOpenPost: (key: string) => void }) {
  let body: ReactNode;
  if (loading) body = Array.from({ length: 5 }, (_, index) => <HighlightBone key={index} />);
  else if (!items.length) body = <span className="flex h-full items-center text-[12px] font-medium text-[var(--st-text-3)]">No highlights yet</span>;
  else body = items.slice(0, 5).map((item) => <Highlight key={item.key} item={item} onOpenPost={onOpenPost} />);
  return (
    <div className="hide-scrollbar overflow-x-auto overflow-y-hidden overscroll-x-contain" style={{ height: ROW.highlights }}>
      <div className="flex h-full w-max gap-2">{body}</div>
    </div>
  );
}

/* ── the standing ─────────────────────────────────────────────────────────── */

/* Where the pick stands right now: its places on Lead's board for the last 30 days (profileStanding), never a link.
   A feeder: their own place in their feed, the ones round it. A feed, or all of them: who leads. Every place's move
   is against the 30 days before: ▲ climbed (the accent), ▼ slipped, – held, "new" when it had no posts then */
const STANDING_SHOWN = 5;

function moveOf(row: StandingRow): { glyph: '▲' | '▼' | null; text: string } {
  if (row.move == null) return { glyph: null, text: 'new' };
  if (row.move > 0) return { glyph: '▲', text: String(row.move) };
  if (row.move < 0) return { glyph: '▼', text: String(-row.move) };
  return { glyph: null, text: '–' };
}

// the move in words, for the line under a place ("▲ 2 places since last month")
function moveWords(row: StandingRow): ReactNode {
  if (row.move == null) return 'new this month';
  if (row.move === 0) return 'held its place';
  const places = Math.abs(row.move);
  return (
    <>
      <Arrow glyph={row.move > 0 ? '▲' : '▼'} />
      {`${places} ${plural(places, 'place')} since last month`}
    </>
  );
}

// a face on the board, ringed in ink when it is the one the profile is about
function StandingFace({ row, px, ring = false }: { row: StandingRow; px: number; ring?: boolean }) {
  return (
    <span
      className={cn('relative block shrink-0 overflow-hidden rounded-full', ring ? 'shadow-[0_0_0_2px_var(--st-cut),0_0_0_4px_var(--st-text)]' : 'shadow-[0_0_0_2px_var(--st-cut)]')}
      style={{ width: px, height: px }}
    >
      <FeederStoryAvatar feeder={{ handle: row.handle, profilePicUrl: row.profilePicUrl }} className="text-[11px]" />
    </span>
  );
}

// a place, as a number set big: the # small and quiet before it
function Place({ place, size }: { place: number; size: 'line' | 'list' }) {
  return (
    <span className="flex shrink-0 items-baseline font-black leading-none tracking-[-0.05em] tabular-nums text-[var(--st-text)]">
      <span className={cn('text-[var(--st-text-3)]', size === 'line' ? 'mr-0.5 text-[18px]' : 'mr-px text-[12px]')}>#</span>
      <span className={size === 'line' ? 'text-[38px]' : 'text-[20px]'}>{place}</span>
    </span>
  );
}

/* a phone's (and a wide top's) one line: the place set big, what it is, its move; the board's first three faces at
   the end, the one it's about ringed */
function StandingLine({ standing }: { standing: Standing | null | undefined }) {
  if (standing === undefined) {
    return (
      <div aria-hidden="true" className="flex items-center gap-4" style={{ height: ROW.standing }}>
        <span className="st-bone block h-9 w-12 shrink-0" />
        <span className="flex min-w-0 flex-1 flex-col gap-2">
          <span className="st-bone block h-3 w-40 max-w-full" />
          <span className="st-bone block h-2.5 w-52 max-w-full" />
        </span>
      </div>
    );
  }
  const rows = standing?.rows ?? [];
  const you = standing?.you ?? null;
  const focus = you ?? rows[0] ?? null;
  if (!standing || !focus || focus.posts === 0) {
    return (
      <p className="flex items-center text-[13px] font-medium text-[var(--st-text-3)]" style={{ height: ROW.standing }}>
        No ranked posts in the last {STANDING_DAYS} days
      </p>
    );
  }
  const where = standing.where ?? 'all feeds';
  const top = rows.slice(0, 3);
  const label = you
    ? `${focus.place} of ${rows.length} in ${where}`
    : `@${focus.handle} leads ${where}`;
  return (
    <div role="group" aria-label={`Standing, last ${STANDING_DAYS} days: ${label}`} className="flex items-center gap-4" style={{ height: ROW.standing }}>
      <Place place={focus.place} size="line" />
      <span className="flex min-w-0 flex-1 flex-col gap-1">
        {/* a feeder: where it is (the place says which); a leader: who, then what it leads, under it */}
        <span className="truncate text-[15px] font-bold leading-5 text-[var(--st-text)]">
          {you ? `in ${where}` : `@${focus.handle}`}
          {you ? <span className="font-medium text-[var(--st-text-2)]">{` · of ${rows.length}`}</span> : null}
        </span>
        <span className="truncate text-[12px] font-semibold leading-4 text-[var(--st-text-2)]">
          {you ? null : `leads ${where} · `}
          {moveWords(focus)}
        </span>
      </span>
      <span aria-hidden="true" className="flex shrink-0 -space-x-3 pr-1">
        {top.map((row) => <StandingFace key={row.handle} row={row} px={30} ring={row.handle === focus.handle} />)}
      </span>
    </div>
  );
}

/* a spread top's right column: the board itself, five places, each with its move. A feeder's are the ones round it,
   its row raised; a feed's, or every feed's, the first five. (No Top % beside them: Lead counts its month by when posts
   went out, the profile by their IST day, and the two can differ by a point on the same screen) */
function StandingList({ standing }: { standing: Standing | null | undefined }) {
  const rows = standing?.rows ?? [];
  const you = standing?.you ?? null;
  const from = you ? Math.max(0, Math.min(you.place - 3, rows.length - STANDING_SHOWN)) : 0;
  const shown = rows.slice(from, from + STANDING_SHOWN);
  let body: ReactNode;
  if (standing === undefined) {
    body = Array.from({ length: STANDING_SHOWN }, (_, index) => (
      <li key={index} aria-hidden="true" className="flex items-center gap-3 px-2" style={{ height: ROW.standingRow }}>
        <span className="st-bone block h-4 w-6 shrink-0" />
        <span className="st-bone block h-8 w-8 shrink-0 rounded-full" />
        <span className="st-bone block h-3 w-32" />
      </li>
    ));
  } else if (!shown.length || !shown.some((row) => row.posts > 0)) {
    body = <li className="flex items-center px-2 text-[13px] font-medium text-[var(--st-text-3)]" style={{ height: ROW.standingRow }}>No ranked posts in the last {STANDING_DAYS} days</li>;
  } else {
    body = shown.map((row) => {
      const mine = row.handle === you?.handle;
      const move = moveOf(row);
      return (
        <li
          key={row.handle}
          aria-current={mine ? 'true' : undefined}
          className={cn('flex items-center gap-3 rounded-[14px] pl-2 pr-3', mine && 'bg-[var(--st-surface-2)] shadow-[inset_0_0_0_1px_var(--st-line)]')}
          style={{ height: ROW.standingRow }}
        >
          <span className="flex w-8 shrink-0 justify-end"><Place place={row.place} size="list" /></span>
          <StandingFace row={row} px={32} ring={mine} />
          <span className={cn('min-w-0 flex-1 truncate text-[15px] leading-5 text-[var(--st-text)]', mine ? 'font-black' : 'font-bold')}>@{row.handle}</span>
          <span className={cn('w-8 shrink-0 text-right text-[12px] font-bold tabular-nums', move.glyph === '▲' ? 'text-[var(--st-text)]' : 'text-[var(--st-text-3)]')}>
            {move.glyph ? <Arrow glyph={move.glyph} /> : null}
            {move.text}
          </span>
        </li>
      );
    });
  }
  return (
    <div role="group" aria-label={`Standing, last ${STANDING_DAYS} days`} className="flex min-w-0 flex-col">
      <div className="flex items-center justify-between gap-3 px-2" style={{ height: ROW.standingHead }}>
        <span className={MARKER}>Standing · {STANDING_DAYS} days</span>
        {standing ? <span className="truncate text-[12px] font-medium text-[var(--st-text-3)]">{standing.where ?? 'All feeds'} · {rows.length} {plural(rows.length, 'feeder')}</span> : null}
      </div>
      <ol className="flex flex-col">{body}</ol>
    </div>
  );
}

function ManageButton({ onManage, className }: { onManage: () => void; className?: string }) {
  return (
    <button type="button" onClick={onManage} aria-label="Manage feeds and feeders" className={cn(ACTION, 'w-11 flex-none px-0', className)}>
      <Ellipsis className="h-5 w-5" strokeWidth={2.4} aria-hidden="true" />
    </button>
  );
}

/* ── the profile ──────────────────────────────────────────────────────────── */

function ProfileHeader({ scope, title, subtitle, feeder, feeds, faces, profile, fresh, width, onManage, onOpenPost }: ProfileHeaderProps) {
  const kind = kindOf(scope);
  const frame = profileFrame(width, scope);
  const loading = profile == null;
  const standing = useStanding(scope);
  const content = width - 8;
  const highlights = <Highlights items={profile?.highlights ?? []} loading={loading} onOpenPost={onOpenPost} />;
  const style = { width, height: frame.height, '--st-cut': 'var(--st-bg)' } as CSSProperties;

  if (!frame.wide) {
    return (
      <section aria-label={title} className="relative flex flex-col px-[var(--st-inset)]" style={style}>
        <div className="flex items-center gap-3" style={{ marginTop: ROW.pad, height: ROW.face }}>
          <FaceBlock kind={kind} size="page" scope={scope} feeder={feeder} feeds={feeds} faces={faces} fresh={fresh} lastPostAt={profile?.lastPostAt ?? null} />
          <StatsRow profile={profile} center size="md" className="min-w-0 flex-1" />
        </div>
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
        <div style={{ marginTop: ROW.band }}>{highlights}</div>
        <div style={{ marginTop: ROW.band }}>
          <StandingLine standing={standing} />
        </div>
      </section>
    );
  }

  /* a wide column: two sides. On the left, Instagram's profile, tucked in: the face; beside it the name (with the
     settings), the numbers on one line, the bio (the line under the name, all feeds' Today); the highlights under it all.
     On the right, where the pick stands, the board's five places, the left side's height, so both sides start and end
     on the same lines */
  if (frame.spread) {
    const boardWidth = Math.min(ROW.boardMax, Math.max(ROW.boardMin, Math.round(content * 0.34)));
    const faceSize: Size = content >= ROW.bigFaceFrom ? 'spread' : 'page';
    return (
      <section aria-label={title} className="relative flex flex-col px-[var(--st-inset)]" style={style}>
        <div className="grid gap-14" style={{ marginTop: ROW.pad, height: frame.hero, gridTemplateColumns: `minmax(0, 1fr) ${boardWidth}px` }}>
          <div className="flex min-w-0 flex-col">
            <div className="flex min-w-0 items-center gap-8" style={{ height: ROW.profileTop }}>
              <FaceBlock kind={kind} size={faceSize} scope={scope} feeder={feeder} feeds={feeds} faces={faces} fresh={fresh} lastPostAt={profile?.lastPostAt ?? null} />
              <div className="flex min-w-0 flex-1 flex-col">
                <div className="flex min-w-0 items-center gap-3">
                  <h2 className="min-w-0 truncate text-[32px] font-black leading-[38px] tracking-[-0.045em] text-[var(--st-text)]">{title}</h2>
                  <ManageButton onManage={onManage} className="h-9 w-9 rounded-[10px]" />
                </div>
                <div className="mt-3">
                  <InlineStats profile={profile} />
                </div>
                {subtitle ? <p className="mt-2 truncate text-[14px] font-medium leading-5 text-[var(--st-text-2)]">{subtitle}</p> : null}
                {kind === 'all' ? (
                  <div className="mt-1">
                    <TodayLine today={profile?.today ?? null} />
                  </div>
                ) : null}
              </div>
            </div>
            <div style={{ marginTop: ROW.profileGap }}>{highlights}</div>
          </div>
          <StandingList standing={standing} />
        </div>
      </section>
    );
  }

  return (
    <section aria-label={title} className="relative flex flex-col px-[var(--st-inset)]" style={style}>
      <div className="flex items-center gap-8" style={{ marginTop: ROW.pad, height: frame.hero }}>
        <FaceBlock kind={kind} size="page" scope={scope} feeder={feeder} feeds={feeds} faces={faces} fresh={fresh} lastPostAt={profile?.lastPostAt ?? null} />
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
      <div style={{ marginTop: ROW.wideGap }}>
        <StandingLine standing={standing} />
      </div>
    </section>
  );
}

export default memo(ProfileHeader);
