'use client';

/* ─────────────────────────────────────────────────────────────
   PROFILE HEADER — the top of the feed: whose it is, as an
   Instagram profile, but cooler. Every number comes from the same
   posts and checkpoints the feed shows.

   A feeder: their big face (a rose story ring when they posted in
   the last day) with a time badge. A feed: its feeders' faces,
   stacked. All feeds: the feed badges, stacked, and what happened
   today. Beside or under it: posts, followers and the typical
   post, each with its move. Then the name, the way to the numbers
   (Lead) and the story (Read), highlights as little circles, the
   last 13 weeks as shaded boxes, three habits, and the order as
   Instagram's tab row (Newest posts · Latest results).

   'page' sits above the grids at a fixed height for its width
   (profileHeaderSize: loaded or not, nothing below it moves).
   'card' is the today zoom's first card: the same, centred and
   compact, filling its box, ending in "Swipe up"; on a short
   screen it lets go of its least needed rows (stream.css).

   While the profile loads the frame is drawn with still
   placeholders, never a number. Nothing here animates but the
   order's underline and pill (transform), and presses.
   ───────────────────────────────────────────────────────────── */

import { memo, useState, type CSSProperties, type ReactNode } from 'react';
import { BookOpen, ChevronUp, Clock, Ellipsis, Target, Trophy, type LucideIcon } from 'lucide-react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import PostCover from '@/components/feed/stream/PostCover';
import { press } from '@/components/feed/stream/press';
import { PROFILE_ROW, profileFrame } from '@/components/feed/stream/profileHeaderSize';
import { shade } from '@/components/read/readShade';
import {
  STREAM_WINDOW_DAYS,
  dayLabel,
  formatCount,
  formatTopPercent,
  relativeTime,
  type FeedOrder,
  type ProfileHeaderProps,
  type StreamFeeder,
  type StreamHighlight,
  type StreamMediaType,
  type StreamProfile,
  type StreamScope,
} from '@/lib/feedStream/contract';
import { feedInitials } from '@/lib/feedLabels';
import { useAppHaptics } from '@/lib/haptics';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

type Kind = 'feeder' | 'feed' | 'all';
type Size = 'page' | 'card';
type Face = { handle: string; profilePicUrl: string | null };
type Week = StreamProfile['weeks'][number];
type Note = { text: string; up: boolean } | null;

const ROW = PROFILE_ROW;
// the big face, and the ring round it (3px of ring, 3px of gap)
const FACE_PX: Record<Size, number> = { page: 108, card: 124 };
const RING = 6;
const WEEKS = 13;
const ORDERS: ReadonlyArray<{ value: FeedOrder; label: string; Icon: LucideIcon }> = [
  { value: 'posted', label: 'Newest posts', Icon: Clock },
  { value: 'results', label: 'Latest results', Icon: Target },
];
const FORMAT_NAME: Record<StreamMediaType, string> = { reel: 'Reels', carousel: 'Carousels', image: 'Images', unknown: 'Posts' };
// a feed's badge: the rail's light disc, its initials in rose
const DISC_BG = 'radial-gradient(circle at 30% 18%, rgb(var(--fm-accent-rgb) / 0.12), transparent 58%), linear-gradient(135deg, #f7f7f8, #cfd3dc)';
// the card: its surface (stream.css --st-card), lit rose from above the face
const CARD_BG = 'radial-gradient(90% 55% at 50% 0%, rgb(var(--fm-accent-rgb) / var(--st-card-glow)), transparent 70%), var(--st-card)';
const EASE = 'ease-[cubic-bezier(0.16,0.9,0.2,1)]';
const MARKER = 'text-[10px] font-black uppercase leading-none tracking-[0.14em] text-[var(--st-text-3)]';
const ACTION = 'flex h-11 min-w-0 flex-1 items-center justify-center gap-2 rounded-[14px] bg-[var(--st-surface-2)] px-3 text-[14px] font-bold text-[var(--st-text)] shadow-[inset_0_0_0_1px_var(--st-line)] outline-none [-webkit-tap-highlight-color:transparent] transition-transform duration-150 ease-out active:scale-[0.97] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]';

/* ── words and numbers ────────────────────────────────────────────────────── */

const kindOf = (scope: StreamScope): Kind => (scope.handle ? 'feeder' : scope.feedId ? 'feed' : 'all');
const plural = (count: number, word: string) => `${word}${count === 1 ? '' : 's'}`;
const ranked = (value: number | null | undefined): value is number => value != null && Number.isFinite(value);

function postsValue(posts: number) {
  return posts < 10_000 ? posts.toLocaleString('en-US') : formatCount(posts);
}

// "▲ 3.0% · 30d" in rose, "▼ 1.2% · 30d" muted
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

function typicalLine(value: number | null) {
  return ranked(value) ? `Typical Top ${formatTopPercent(value)}` : 'Not measured yet';
}

// an IST hour window: "6–9 pm", "11 am–2 pm"
function hoursLabel(start: number, end: number) {
  const part = (hour: number) => {
    const at = ((Math.round(hour) % 24) + 24) % 24;
    return { n: at % 12 === 0 ? 12 : at % 12, half: at < 12 ? 'am' : 'pm' };
  };
  const from = part(start);
  const to = part(end);
  return from.half === to.half ? `${from.n}–${to.n} ${to.half}` : `${from.n} ${from.half}–${to.n} ${to.half}`;
}

function perWeekLabel(value: number) {
  return Number.isFinite(value) ? value.toFixed(1).replace(/\.0$/, '') : '—';
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
      className={cn('grid h-full w-full place-items-center font-black leading-none tracking-[-0.04em] text-[var(--fm-accent)]', px >= 100 ? 'text-[34px]' : px >= 76 ? 'text-[22px]' : 'text-[18px]')}
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
          <FeederStoryAvatar feeder={face} className={size === 'card' ? 'text-[34px]' : 'text-[28px]'} />
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
      {live ? <span aria-hidden="true" className="h-1.5 w-1.5 shrink-0 rounded-full bg-[var(--fm-accent-text)]" /> : null}
      <span className="truncate">
        <b className="font-bold text-[var(--st-text)]">Today</b>
        {' · '}
        <b className="font-bold tabular-nums text-[var(--st-text)]">{today.posts}</b> {plural(today.posts, 'post')} went up
        {' · '}
        <b className="font-bold tabular-nums text-[var(--st-text)]">{today.results}</b> {plural(today.results, 'result')} landed
      </span>
    </p>
  );
}

function StatsRow({ profile, center, className }: { profile: StreamProfile | null; center: boolean; className?: string }) {
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
          <span className="mr-1 text-[12px] font-bold tracking-normal text-[var(--st-text-2)]">Top</span>
          {typical}
          <span className="text-[12px] tracking-normal">%</span>
        </>
      ) : '—') : null,
      label: 'typical post',
      note: profile ? typicalNote(profile.typical, profile.typicalPrev) : null,
    },
  ];
  return (
    <div className={cn('grid grid-cols-3 gap-2', className)} style={{ height: ROW.stats }}>
      {stats.map((stat) => (
        <div key={stat.key} className={cn('flex min-w-0 flex-col', center ? 'items-center text-center' : 'items-start')}>
          <span className="block h-[22px] max-w-full truncate text-[18px] font-black leading-[22px] tracking-[-0.04em] tabular-nums text-[var(--st-text)]">
            {stat.value ?? <span className="st-bone mt-1 inline-block h-3.5 w-10 align-top" />}
          </span>
          <span className="mt-1 block max-w-full truncate text-[12px] font-medium leading-4 text-[var(--st-text-2)]">{stat.label}</span>
          <span className={cn('mt-0.5 block h-[14px] max-w-full truncate text-[10px] font-bold leading-[14px]', stat.note?.up ? 'text-[var(--fm-accent-text)]' : 'text-[var(--st-text-3)]')}>
            {stat.note?.text ?? ''}
          </span>
        </div>
      ))}
    </div>
  );
}

function Actions({ onLead, onRead, onManage, className, style }: {
  onLead: () => void;
  onRead: () => void;
  onManage: () => void;
  className?: string;
  style?: CSSProperties;
}) {
  return (
    <div className={cn('flex gap-2', className)} style={{ height: ROW.actions, ...style }}>
      <button type="button" onClick={onLead} className={ACTION}>
        <Trophy className="h-4 w-4 shrink-0" strokeWidth={2.4} aria-hidden="true" />
        {/* a phone's column has room for the tab's name alone; never a word cut in half */}
        <span className="truncate"><span className="hidden sm:inline">Numbers in </span>Lead</span>
      </button>
      <button type="button" onClick={onRead} className={ACTION}>
        <BookOpen className="h-4 w-4 shrink-0" strokeWidth={2.4} aria-hidden="true" />
        <span className="truncate"><span className="hidden sm:inline">Story in </span>Read</span>
      </button>
      <button type="button" onClick={onManage} aria-label="Manage feeds and feeders" className={cn(ACTION, 'w-11 flex-none px-0')}>
        <Ellipsis className="h-5 w-5" strokeWidth={2.4} aria-hidden="true" />
      </button>
    </div>
  );
}

/* a highlight: a little Instagram circle (a neutral ring, a gap, the cover), its number in a rose badge over the
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
      className="group flex w-[72px] shrink-0 flex-col items-center rounded-[14px] outline-none [-webkit-tap-highlight-color:transparent] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
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
          <span className="absolute left-1/2 top-full -translate-x-1/2 -translate-y-1/2 whitespace-nowrap rounded-full bg-[var(--fm-accent)] px-1.5 py-[3px] text-[10px] font-black leading-none tabular-nums text-white shadow-[0_0_0_2px_var(--st-cut)]">
            {top}
          </span>
        ) : null}
      </span>
      <span className="mt-3 block w-full truncate text-center text-[12px] font-medium leading-[18px] text-[var(--st-text-2)]">{item.label}</span>
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

function WeekBox({ week, latest, big }: { week: Week | null; latest: boolean; big: boolean }) {
  const ring = latest ? 'inset 0 0 0 1.5px var(--fm-accent-text)' : null;
  const when = week ? `Week of ${dayLabel(week.start).long}` : 'Before the window';
  if (!week || !ranked(week.typical)) {
    return (
      <span
        role="listitem"
        aria-label={`${when}: ${week?.count ? `${week.count} ${plural(week.count, 'post')}, not measured yet` : 'no posts'}`}
        className="block min-w-0 flex-1 rounded-[6px]"
        style={{ boxShadow: ring ?? 'inset 0 0 0 1px var(--st-line)' }}
      />
    );
  }
  const tone = shade(week.typical);
  const value = formatTopPercent(week.typical).slice(0, -1);
  return (
    <span
      role="listitem"
      aria-label={`${when}: typical top ${value}%, ${week.count} ${plural(week.count, 'post')}`}
      className={cn('flex min-w-0 flex-1 items-center justify-center overflow-hidden rounded-[6px] font-black leading-none tracking-[-0.04em] tabular-nums', big ? 'text-[12px]' : 'text-[10px]')}
      style={{ background: tone.c, color: tone.tx, boxShadow: [ring, `inset 0 1px 0 rgba(255, 255, 255, ${tone.gi})`].filter(Boolean).join(', ') }}
    >
      {value}
    </span>
  );
}

// the last 13 weeks, oldest first, the latest ringed rose; `cell` is a box's width, which picks the type size
function WeekStrip({ weeks, loading, cell, className }: { weeks: Week[]; loading: boolean; cell: number; className?: string }) {
  const last = weeks.slice(-WEEKS);
  const cells: Array<Week | null> = [...Array.from({ length: WEEKS - last.length }, () => null), ...last];
  return (
    <div className={cn('flex flex-col', className)} style={{ height: ROW.strip }}>
      <span className={cn(MARKER, 'block h-[14px] leading-[14px]')}>Typical post · 13 weeks</span>
      <span role="list" aria-label="Typical post, week by week" className="mt-2 flex h-[30px] gap-[3px]">
        {loading
          ? cells.map((_, index) => <span key={index} className="st-bone block min-w-0 flex-1" />)
          : cells.map((week, index) => <WeekBox key={week?.start ?? `before:${index}`} week={week} latest={index === WEEKS - 1} big={cell >= 28} />)}
      </span>
    </div>
  );
}

function habitsOf(kind: Kind, profile: StreamProfile | null) {
  const bestTime = {
    marker: 'Best time',
    value: profile?.bestHours ? hoursLabel(profile.bestHours.start, profile.bestHours.end) : '—',
    note: profile?.bestHours ? typicalLine(profile.bestHours.typical) : 'Not enough posts yet',
  };
  if (kind === 'feeder') {
    return [
      bestTime,
      {
        marker: 'Strongest',
        value: profile?.strongestFormat ? FORMAT_NAME[profile.strongestFormat.type] : '—',
        note: profile?.strongestFormat ? typicalLine(profile.strongestFormat.typical) : 'Not enough posts yet',
      },
      {
        marker: 'Rhythm',
        value: profile ? `${perWeekLabel(profile.perWeek)} a week` : '—',
        note: `Over ${STREAM_WINDOW_DAYS} days`,
      },
    ];
  }
  return [
    {
      marker: 'Leading',
      value: profile?.leader ? `@${profile.leader.handle}` : '—',
      note: profile?.leader ? typicalLine(profile.leader.typical) : 'Needs 5+ posts',
    },
    {
      marker: 'On a run',
      value: profile?.onRun ? `@${profile.onRun.handle}` : '—',
      note: profile?.onRun ? `${profile.onRun.winners} top-10% ${plural(profile.onRun.winners, 'post')} · 7d` : 'No one this week',
    },
    bestTime,
  ];
}

function HabitCards({ kind, profile, className }: { kind: Kind; profile: StreamProfile | null; className?: string }) {
  return (
    <div className={cn('grid grid-cols-3 gap-2', className)} style={{ height: ROW.habits }}>
      {habitsOf(kind, profile).map((card) => (
        <div key={card.marker} className="flex min-w-0 flex-col justify-between rounded-[18px] bg-[var(--st-surface-2)] p-3 text-left shadow-[inset_0_0_0_1px_var(--st-line)]">
          <span className={cn(MARKER, 'block truncate')}>{card.marker}</span>
          {profile ? (
            <span className="block min-w-0">
              <span className="block truncate text-[16px] font-black leading-5 tracking-[-0.04em] text-[var(--st-text)]">{card.value}</span>
              <span className="mt-0.5 block truncate text-[12px] font-medium leading-4 text-[var(--st-text-2)]">{card.note}</span>
            </span>
          ) : (
            <span aria-hidden="true" className="flex flex-col gap-1.5">
              <span className="st-bone block h-3.5 w-14 max-w-full" />
              <span className="st-bone block h-2.5 w-20 max-w-full" />
            </span>
          )}
        </div>
      ))}
    </div>
  );
}

/* the order, as Instagram's tab row under a bio: two equal tabs, the current one white over a rose underline that
   glides between them (transform), the other dimmed; a hairline under the row */
function OrderTabs({ order, onChange }: { order: FeedOrder; onChange: (order: FeedOrder) => void }) {
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
            <span className={cn('flex min-w-0 items-center gap-1.5 text-[14px] font-bold leading-none text-[var(--st-text)] transition-[opacity,scale] duration-300 group-active:scale-[0.96]', EASE, on ? 'opacity-100' : 'opacity-45')}>
              <Icon className="h-4 w-4 shrink-0" strokeWidth={2.4} aria-hidden="true" />
              <span className="truncate">{label}</span>
            </span>
          </button>
        );
      })}
      <span
        aria-hidden="true"
        className={cn('pointer-events-none absolute bottom-0 left-0 h-[2px] w-1/2 bg-[var(--fm-accent-text)] transition-transform duration-300', EASE)}
        style={{ transform: order === 'results' ? 'translateX(100%)' : 'translateX(0%)' }}
      />
    </div>
  );
}

// the card's order: a compact two-way pill, the current half filled rose (the fill glides, transform only)
function OrderPill({ order, onChange }: { order: FeedOrder; onChange: (order: FeedOrder) => void }) {
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
      <span
        aria-hidden="true"
        className={cn('pointer-events-none absolute bottom-[3px] left-[3px] top-[3px] w-[calc(50%-3px)] rounded-[10px] bg-[var(--fm-accent)] shadow-[inset_0_1px_0_rgb(255_255_255_/_0.28)] transition-transform duration-300', EASE)}
        style={{ transform: order === 'results' ? 'translateX(100%)' : 'translateX(0%)' }}
      />
      {ORDERS.map(({ value, label }) => {
        const on = value === order;
        return (
          <button
            key={value}
            type="button"
            role="tab"
            aria-selected={on}
            onClick={() => pick(value)}
            className={cn('relative z-10 grid h-full min-w-0 place-items-center rounded-[10px] px-2 text-[12px] font-bold leading-none outline-none [-webkit-tap-highlight-color:transparent] transition-transform duration-150 ease-out active:scale-[0.96] focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]', on ? 'text-white' : 'text-fg')}
          >
            <span className={cn('truncate transition-opacity duration-300', on ? 'opacity-100' : 'opacity-60')}>{label}</span>
          </button>
        );
      })}
    </div>
  );
}

/* ── the two variants ─────────────────────────────────────────────────────── */

function ProfilePage({ scope, title, subtitle, feeder, feeds, faces, profile, fresh, order, onOrderChange, width, onLead, onRead, onManage, onOpenPost }: ProfileHeaderProps) {
  const kind = kindOf(scope);
  const frame = profileFrame(width, scope);
  const loading = profile == null;
  const content = width - 8;
  const stripWidth = frame.widest ? (content - ROW.wideGap) * (1.15 / 2.15) : content;
  const face = <FaceBlock kind={kind} size="page" scope={scope} feeder={feeder} feeds={feeds} faces={faces} fresh={fresh} lastPostAt={profile?.lastPostAt ?? null} />;
  const highlights = <Highlights items={profile?.highlights ?? []} loading={loading} center={false} onOpenPost={onOpenPost} />;
  const strip = <WeekStrip weeks={profile?.weeks ?? []} loading={loading} cell={(stripWidth - (WEEKS - 1) * 3) / WEEKS} />;
  const habits = <HabitCards kind={kind} profile={profile} />;
  const tabs = <OrderTabs order={order} onChange={onOrderChange} />;
  const style = { width, height: frame.height, '--st-cut': 'var(--st-bg)' } as CSSProperties;

  if (!frame.wide) {
    return (
      <section aria-label={title} className="relative flex flex-col px-[var(--st-inset)]" style={style}>
        <div className="flex items-center gap-4" style={{ marginTop: ROW.pad, height: ROW.face }}>
          {face}
          <StatsRow profile={profile} center className="min-w-0 flex-1" />
        </div>
        <div style={{ marginTop: ROW.gap, height: ROW.title }}>
          <TitleBlock title={title} subtitle={subtitle} wide={false} />
        </div>
        {kind === 'all' ? (
          <div className="pt-1.5" style={{ height: ROW.today }}>
            <TodayLine today={profile?.today ?? null} />
          </div>
        ) : null}
        <Actions onLead={onLead} onRead={onRead} onManage={onManage} style={{ marginTop: ROW.gap }} />
        <div style={{ marginTop: ROW.gap + 4 }}>{highlights}</div>
        <div style={{ marginTop: ROW.gap }}>{strip}</div>
        <div style={{ marginTop: ROW.gap }}>{habits}</div>
        <div style={{ marginTop: ROW.gap + 4 }}>{tabs}</div>
      </section>
    );
  }

  const info = content - FACE_PX.page - RING * 2 - 32;
  return (
    <section aria-label={title} className="relative flex flex-col px-[var(--st-inset)]" style={style}>
      <div className="flex items-center gap-8" style={{ marginTop: ROW.pad, height: frame.hero }}>
        {face}
        <div className="flex min-w-0 flex-1 flex-col">
          <div className="flex items-center gap-6" style={{ height: ROW.titleWide }}>
            <div className="min-w-0 flex-1">
              <TitleBlock title={title} subtitle={subtitle} wide />
            </div>
            <Actions onLead={onLead} onRead={onRead} onManage={onManage} className="shrink-0" style={{ width: Math.min(420, Math.round(info * 0.56)) }} />
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
      {frame.widest ? (
        <div
          className="grid items-center gap-6"
          style={{ marginTop: ROW.wideGap, height: Math.max(ROW.strip, ROW.habits), gridTemplateColumns: 'minmax(0, 1.15fr) minmax(0, 1fr)' }}
        >
          {strip}
          {habits}
        </div>
      ) : (
        <>
          <div style={{ marginTop: ROW.wideGap }}>{strip}</div>
          <div style={{ marginTop: ROW.gap }}>{habits}</div>
        </>
      )}
      <div style={{ marginTop: ROW.gap + 6 }}>{tabs}</div>
    </section>
  );
}

function ProfileCard({ scope, title, subtitle, feeder, feeds, faces, profile, fresh, order, onOrderChange, width, onLead, onRead, onManage, onOpenPost }: ProfileHeaderProps) {
  const kind = kindOf(scope);
  const loading = profile == null;
  const stripWidth = Math.min(width - 40, 420);
  return (
    <section
      aria-label={title}
      className="st-profile-card relative flex h-full flex-col items-center justify-between overflow-hidden rounded-[28px] px-5 pb-4 pt-5 text-center shadow-[inset_0_0_0_1px_var(--st-line)] light:shadow-[inset_0_0_0_1px_var(--st-line),0_18px_40px_-26px_rgb(15_23_42/0.3)]"
      // inside the card, what is raised sits in the card's own inset tone (on light: grey on a white card)
      style={{ width, background: CARD_BG, '--st-cut': 'var(--st-card)', '--st-surface-2': 'var(--st-card-inset)' } as CSSProperties}
    >
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
        <div className="st-pc-actions mt-3 w-full max-w-[400px]">
          <Actions onLead={onLead} onRead={onRead} onManage={onManage} className="w-full" />
        </div>
      </div>

      <div className="flex w-full flex-col items-center">
        <div className="st-pc-highlights w-full">
          <Highlights items={profile?.highlights ?? []} loading={loading} center onOpenPost={onOpenPost} />
        </div>
        <div className="st-pc-strip mt-2.5 w-full text-left" style={{ maxWidth: stripWidth }}>
          <WeekStrip weeks={profile?.weeks ?? []} loading={loading} cell={(stripWidth - (WEEKS - 1) * 3) / WEEKS} />
        </div>
      </div>

      <div className="flex flex-col items-center gap-2.5">
        <OrderPill order={order} onChange={onOrderChange} />
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
