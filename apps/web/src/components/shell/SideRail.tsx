'use client';

/* ─────────────────────────────────────────────────────────────
   SIDE RAIL — the app's chrome from a laptop up. The browser
   already stacks its own bars across the top, and a laptop is
   short of height with width to spare, so the tabs, your feeds
   and the tab's setting stand down the left, in one floating
   glass panel, and the page is all content from its first line.

   Shut, it is a quiet column of big circles: the tabs' icons,
   your feeds (a feed's badge, a feeder's face), Manage and the
   tab's setting. Point at it (or tab into it, or tap its ground)
   and it opens over the page as glass, every name riding in
   beside its circle; leave it and it shuts. The page never moves
   for it. Every icon and circle sits on one line 48px in from the
   rail's edge, open or shut: opening only uncovers what is to
   their right.

   How it moves, all on the carry curve:
   - a tab: the ink pill glides to it at the click (before the
     page has answered), and the icon and name turn to the page's
     colour exactly where the pill is, as it passes; the page is
     simply there, as you left it (AppTabHost, lib/tabSheet), and
     the setting at the foot rolls over the way the pill went;
   - a feed: the phone's story rail stood on end. Its circle
     rises to the top while the other feeds tuck into its place
     and its feeders fan out of it, from behind it; closing, they
     go back in and the feeds come out again (Ledger, fan). All
     feeds, at the top, is the way back: its wedge turns round to
     point back, as the phone's does;
   - a pick: the one you're on grows a touch, wears the pick's
     gradient ring and a soft shadow, and (open) one raised row
     glides to it from the last;
   - the setting at the foot (Lead's time range, Feed's zoom; Read
     keeps its view on the page) crosses over in a place that is
     always there, so nothing moves around it.
   ───────────────────────────────────────────────────────────── */

import dynamic from 'next/dynamic';
import { memo, useCallback, useEffect, useLayoutEffect, useRef, useState, type FocusEvent as ReactFocusEvent, type PointerEvent as ReactPointerEvent, type ReactNode, type RefObject, type SyntheticEvent } from 'react';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { BookOpenText, LayoutGrid, SlidersHorizontal, Trophy, type LucideIcon } from 'lucide-react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { FAN_MS, Ledger } from '@/components/motion/Ledger';
import { CARRY_EASE_CSS, carryCurve } from '@/lib/feedStream/motion';
import { feedInitials, titleCase } from '@/lib/feedLabels';
import { ms, sec } from '@/lib/tempo';
import { useFreshHandles } from '@/lib/feedStream/fresh';
import { leaveTab, stayOnTab } from '@/lib/tabSheet';
import { cn } from '@/lib/utils';
import {
  ArmTrack,
  ControlPlacementContext,
  PICK_EASE,
  PICK_MS,
  StoryRing,
  FreshRing,
  WEDGE_PATH,
  pickFade,
  useHeldPick,
  useShownHeader,
  useWideChrome,
  whoOf,
  type HeaderFeed,
  type HeaderFeeder,
  type TabHeaderConfig,
} from './TabHeader';

// Feed's own sheet for your feeds (a new one, what's in one, the plan, the theme), fetched the first time it is asked for
const FeedManageSheet = dynamic(() => import('@/components/feed/stream/FeedManageSheet'), { ssr: false });

const useIsoLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;
const REDUCED = { duration: 0.14 } as const;

/* ── shut or open ─────────────────────────────────────────────────────────── */

/* The rail is laid out open (296px) and cut to the column (96px) by a clip; a row's own grounds (the tab pill, the
   hover wash, the feed's well) are cut the same way, to the column's 72px. Opening is that clip letting go, with the
   names riding in on its edge; shutting is quicker */
const FOLD_PX = 200;
const OPEN_MS = ms(360);
const SHUT_MS = ms(260);
const foldClip = (open: boolean, round: string) => `inset(0 ${open ? 0 : FOLD_PX}px 0 0 round ${round})`;
const foldTime = (open: boolean) => (open ? OPEN_MS : SHUT_MS);
const foldMove = (open: boolean, reduce: boolean) => (reduce ? undefined : `clip-path ${foldTime(open)}ms ${CARRY_EASE_CSS}`);
/* The names sit just past the column's edge (100px in: the column shows 96), so shut they are simply out of sight, and
   opening uncovers them with everything else, crisp, in the one sweep: nothing fades or slides on its own */
// a pointer passing over the rail on its way somewhere doesn't open it: it opens once the pointer rests a moment, and
// shuts a moment after it leaves (coming back in time keeps it open)
const OPEN_AFTER_MS = 110;
const SHUT_AFTER_MS = 200;

// a dialog actually on screen (Read keeps its closed sheet in the page, marked modal, under an aria-hidden layer)
function aModalIsOpen() {
  return Array.from(document.querySelectorAll<HTMLElement>('[aria-modal="true"]')).some((el) => {
    if (el.closest('[aria-hidden="true"], [inert]') || !el.getClientRects().length) return false;
    const style = getComputedStyle(el);
    return style.visibility !== 'hidden' && style.opacity !== '0';
  });
}

/* ── the tabs ─────────────────────────────────────────────────────────────── */

const TABS: ReadonlyArray<{ key: string; label: string; href: string; icon: LucideIcon }> = [
  { key: 'feed', label: 'Feed', href: '/', icon: LayoutGrid },
  { key: 'lead', label: 'Lead', href: '/lead', icon: Trophy },
  { key: 'read', label: 'Read', href: '/read', icon: BookOpenText },
];
// where a tab stands in the rail, top to bottom
const tabOrder = (key: string | null) => TABS.findIndex((item) => item.key === key);
// a tab's row (48px) and the gap under it: what the ink pill glides by, row to row
const TAB_PITCH = 52;
const TAB_PILL_MS = ms(400);
const pillGlide = (reduce: boolean, open: boolean) => (reduce ? undefined : `transform ${TAB_PILL_MS}ms ${PICK_EASE}, clip-path ${foldTime(open)}ms ${CARRY_EASE_CSS}, opacity ${foldTime(open)}ms ${CARRY_EASE_CSS}`);
// an icon in a big circle's box: icons and circles share the rail's one centre line
const ICON_BOX = 'grid h-full w-14 shrink-0 place-items-center';
const TAB_ROW_CLASS = 'flex h-12 items-center gap-6 rounded-[18px] pl-2';
const TAB_NAME = 'min-w-0 truncate text-[16px] font-bold leading-none tracking-[-0.01em]';

/* Between the tabs (all kept alive) the address changes in place, as the bottom bar does it; Feed again goes back to
   its newest post */
function goToTab(href: string) {
  try {
    sessionStorage.setItem('feedme:intent', href);
    sessionStorage.setItem('feedme:intent-ts', String(Date.now()));
  } catch {}
  if (window.location.pathname === href) {
    if (href === '/') window.dispatchEvent(new CustomEvent('feedme:feed-tab-reselect'));
    return;
  }
  window.history.pushState(null, '', href);
}

// F, L and R switch tabs from the keyboard (not while typing, or under a dialog on screen)
function useTabKeys(onTab: (key: string) => void) {
  const latest = useRef(onTab);
  useIsoLayoutEffect(() => {
    latest.current = onTab;
  });
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.metaKey || event.ctrlKey || event.altKey || event.shiftKey || event.repeat) return;
      const target = event.target as HTMLElement | null;
      if (target && (target.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName))) return;
      if (aModalIsOpen()) return;
      const item = TABS.find((entry) => entry.label[0].toLowerCase() === event.key.toLowerCase());
      if (item) latest.current(item.key);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);
}

// a row's hover wash, cut to the column's 72px while the rail is shut
function RowWash({ open, reduce, round = '16px' }: { open: boolean; reduce: boolean; round?: string }) {
  return (
    <span
      aria-hidden="true"
      className="pointer-events-none absolute inset-y-[3px] left-0 right-0 bg-fg/[0.055] opacity-0 transition-opacity duration-150 group-hover/row:opacity-100"
      style={{ clipPath: foldClip(open, round), transition: reduce ? undefined : `opacity 150ms ease, clip-path ${foldTime(open)}ms ${CARRY_EASE_CSS}` }}
    />
  );
}

// a name beside its icon or circle, past the column's edge
function SideName({ className, children }: { className: string; children: ReactNode }) {
  return <span className={cn('relative min-w-0 truncate', className)}>{children}</span>;
}

function RailTabs({ tab, open, reduce }: { tab: string | null; open: boolean; reduce: boolean }) {
  /* The pill answers the click at once, in a render of its own: the address (and the page) follow in a heavier one a
     moment later, and a pill waiting for that set off late. It wears the tab tapped until the page arrives; the tab
     you're on, chosen again, takes back any pill still waiting */
  const [pending, setPending] = useState<{ key: string; from: string | null } | null>(null);
  if (pending && pending.from !== tab) setPending(null);
  const shownKey = pending ? pending.key : tab;
  const index = TABS.findIndex((item) => item.key === shownKey);
  // the last tab tapped: a switch still copying the page when another tab is tapped doesn't go through
  const goes = useRef(0);
  const go = (key: string) => {
    const item = TABS.find((entry) => entry.key === key);
    if (!item) return;
    setPending(key === tab ? null : { key, from: tab });
    const turn = ++goes.current;
    if (key === tab || !tab || tabOrder(tab) < 0) {
      stayOnTab();
      goToTab(item.href);
      return;
    }
    // (the page you're on is copied first when the switch has a motion that needs it: lib/tabSheet; none by default)
    void leaveTab(tab, key, tabOrder(key) > tabOrder(tab) ? 1 : -1).then(() => {
      if (turn === goes.current) goToTab(item.href);
    });
  };
  useTabKeys(go);
  const y = Math.max(0, index) * TAB_PITCH;
  return (
    <nav aria-label="Tabs" className="relative grid shrink-0 gap-1 p-3">
      {index >= 0 ? (
        <>
          {/* the pill's lift, on plates its clip can't cut: the column's, and the open rail's */}
          <span
            aria-hidden="true"
            className="pointer-events-none absolute left-3 top-3 h-12 w-[72px] rounded-[18px] shadow-[0_4px_14px_rgb(var(--fm-shadow-rgb)/0.22),0_1px_2px_rgba(0,0,0,0.08)] dark:shadow-[0_6px_20px_rgba(0,0,0,0.45)]"
            style={{ transform: `translateY(${y}px)`, opacity: open ? 0 : 1, transition: pillGlide(reduce, open) }}
          />
          <span
            aria-hidden="true"
            className="pointer-events-none absolute left-3 right-3 top-3 h-12 rounded-[18px] shadow-[0_4px_14px_rgb(var(--fm-shadow-rgb)/0.22),0_1px_2px_rgba(0,0,0,0.08)] dark:shadow-[0_6px_20px_rgba(0,0,0,0.45)]"
            style={{ transform: `translateY(${y}px)`, opacity: open ? 1 : 0, transition: pillGlide(reduce, open) }}
          />
          {/* the ink pill, and in it the same rows in the page's colour, held still while it glides (they move back
              by as much as it moves on): the tab you're on turns over exactly where the pill is, as it passes, and
              both moves are the compositor's */}
          <span
            aria-hidden="true"
            className="pointer-events-none absolute left-3 right-3 top-3 z-[15] h-12 overflow-hidden rounded-[18px] bg-fg"
            style={{ transform: `translateY(${y}px)`, clipPath: foldClip(open, '18px'), transition: pillGlide(reduce, open) }}
          >
            <span
              className="absolute inset-x-0 top-0 grid gap-1 text-(--fm-page)"
              style={{ transform: `translateY(${-y}px)`, transition: reduce ? undefined : `transform ${TAB_PILL_MS}ms ${PICK_EASE}` }}
            >
              {TABS.map((item) => (
                <span key={item.key} className={TAB_ROW_CLASS}>
                  <span className={ICON_BOX}>
                    <item.icon aria-hidden="true" size={23} strokeWidth={2.3} />
                  </span>
                  <SideName className={TAB_NAME}>
                    {item.label}
                  </SideName>
                </span>
              ))}
            </span>
          </span>
        </>
      ) : null}
      {TABS.map((item, at) => (
        <a
          key={item.key}
          href={item.href}
          onClick={(event) => {
            if (event.metaKey || event.ctrlKey || event.shiftKey || event.altKey) return;
            event.preventDefault();
            go(item.key);
          }}
          aria-current={tab === item.key ? 'page' : undefined}
          aria-label={`${item.label} (${item.label[0]})`}
          className={cn('group/row relative z-10 text-fg/62 outline-none hover:text-fg focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]', TAB_ROW_CLASS)}
        >
          {at === index ? null : <RowWash open={open} reduce={reduce} round="18px" />}
          <span className={cn(ICON_BOX, 'relative')}>
            <item.icon aria-hidden="true" size={23} strokeWidth={2.3} />
          </span>
          <SideName className={TAB_NAME}>
            {item.label}
          </SideName>
        </a>
      ))}
    </nav>
  );
}

/* ── the feeds ────────────────────────────────────────────────────────────── */

/* The list is the phone's story rail stood on end, and moves the same way (Ledger, fan): one glide on the carry curve.
   Opening a feed, its circle travels to the top, under All feeds, while the other feeds tuck back into the place it
   was in and its feeders slide out of that place, from behind it, each to its own row; closing it, they go back in and
   the feeds come out again. Each row's name goes before its circle and comes in beside it once it lands, so names
   never pass over each other. All feeds, at the top, is the way back. Every row stacks over the ones after it */
const ROW = 'group/row relative flex w-full shrink-0 origin-[36px_50%] items-center rounded-[18px] border-0 bg-transparent p-0 text-left text-inherit outline-none [-webkit-tap-highlight-color:transparent] [will-change:transform] focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]';
const NAME = 'truncate leading-[1.15]';
// the one you're on grows a touch over the rest (with its ring and a shadow under it: the pick's weight)
const PICKED_SCALE = 1.08;
// first over last: what comes out of a row comes out from behind it
const stackOf = (index: number) => 40 - Math.min(index, 39);

/* a row's circle: the pick's ring round it (drawn round when it is picked, back when something else is), its lift, the
   faint track while what was tapped waits for its page, and the accent dot for something new */
function RowCircle({ ring, lit, marked, armed, reduce, className, children }: { ring: boolean; lit: boolean; marked: boolean; armed: boolean; reduce: boolean; className: string; children: ReactNode }) {
  const lift = reduce ? undefined : `transform ${PICK_MS}ms ${PICK_EASE}`;
  return (
    <span className={cn('relative z-10 grid shrink-0 place-items-center rounded-full p-[3px]', className)} style={{ transform: `scale(${lit ? PICKED_SCALE : 1})`, transition: lift }}>
      <span
        aria-hidden="true"
        className="pointer-events-none absolute inset-[4px] rounded-full shadow-[0_10px_22px_-8px_rgb(var(--fm-shadow-rgb)/0.55)] dark:shadow-[0_10px_24px_-6px_rgba(0,0,0,0.8)]"
        style={{ opacity: lit ? 1 : 0, transition: pickFade(reduce) }}
      />
      <StoryRing on={ring} reduce={reduce} />
      {armed ? <ArmTrack reduce={reduce} /> : null}
      {/* something new: the red story ring (not round what you're on: that wears the ink ring) */}
      {/* (round the whole circle: its face sits 3px in, a step closer to the edge than the header rail's) */}
      {marked && !ring && !lit ? <FreshRing className="inset-0" /> : null}
      <span className="relative z-20 grid h-full w-full place-items-center overflow-hidden rounded-full border-2 border-white dark:border-[var(--fm-ink)]">
        {children}
      </span>
    </span>
  );
}

/* a row's name, in bold ink, past the column's edge (the one you're on is told by its ring, its lift and its raised
   tile, never by its name changing); a feed's count of feeders under it. It moves on its own when its row comes or
   goes (data-ledger-label) */
function RowName({ text, sub, className }: { text: string; sub?: string; className: string }) {
  return (
    <span data-ledger-label="" className="relative z-10 grid min-w-0 flex-1 gap-1 pr-3">
      <span className={cn(NAME, 'font-bold text-fg', className)}>{text}</span>
      {sub ? <span className="truncate text-[12px] font-semibold leading-none text-fg/60">{sub}</span> : null}
    </span>
  );
}

const plural = (count: number, word: string) => `${count} ${word}${count === 1 ? '' : 's'}`;

/* All feeds: with a feed open it is the way back, and its wedge itself turns round to point back, on the fan's clock,
   the way the phone's does (TabHeader's WedgeMark) */
const AllRow = memo(function AllRow({ count, back, current, armed, leaving, open, reduce, stack, onPick }: { count: number | null; back: boolean; current: boolean; armed: boolean; leaving: boolean; open: boolean; reduce: boolean; stack: number; onPick: () => void }) {
  const lit = (current && !leaving) || armed;
  const ref = useRef<HTMLButtonElement | null>(null);
  useHeldPick(ref, lit);
  const turn = reduce ? undefined : `transform ${FAN_MS}ms ${PICK_EASE}`;
  return (
    <button ref={ref} type="button" data-ledger="rail:all" data-rail-all="" onClick={onPick} aria-pressed={current} aria-label={back ? 'Back to all feeds' : 'All feeds'} className={cn(ROW, 'h-16 gap-6 pl-2')} style={{ zIndex: stack }}>
      <RowTile lit={lit} open={open} reduce={reduce} />
      <RowWash open={open} reduce={reduce} />
      <RowCircle ring={current && !leaving} lit={lit} marked={false} armed={armed} reduce={reduce} className="h-14 w-14">
        <span className="relative grid h-full w-full place-items-center" style={{ background: 'var(--lead-wedge-surface)' }}>
          <svg aria-hidden="true" viewBox="0 0 42 42" className="block h-[26px] w-[26px]" style={{ transform: back ? 'rotate(180deg)' : 'rotate(0deg)', transition: turn }}>
            <path d={WEDGE_PATH} fill="none" stroke="var(--lead-wedge-mark)" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </span>
      </RowCircle>
      <RowName text="All feeds" sub={back ? 'Back' : count == null ? undefined : plural(count, 'feed')} className="text-[16px] tracking-[-0.015em]" />
    </button>
  );
});

const FeedRow = memo(function FeedRow({ feed, expanded, current, marked, armed, leaving, open, reduce, stack, onPick }: { feed: HeaderFeed; expanded: boolean; current: boolean; marked: boolean; armed: boolean; leaving: boolean; open: boolean; reduce: boolean; stack: number; onPick: (id: string) => void }) {
  const lit = (current && !leaving) || armed;
  const ref = useRef<HTMLButtonElement | null>(null);
  useHeldPick(ref, lit);
  const name = titleCase(feed.title);
  return (
    <button
      ref={ref}
      type="button"
      data-ledger={`rail:feed:${feed.id}`}
      data-rail-badge={feed.id}
      onClick={() => onPick(feed.id)}
      aria-pressed={current}
      aria-expanded={expanded}
      aria-label={!expanded ? `Open ${name}` : current ? `${name}, every feeder. Back to all feeds` : `Show every feeder in ${name}`}
      className={cn(ROW, 'h-16 gap-6 pl-2')}
      style={{ zIndex: stack }}
    >
      <RowTile lit={lit} open={open} reduce={reduce} />
      <RowWash open={open} reduce={reduce} />
      <RowCircle ring={current && !leaving} lit={lit} marked={marked} armed={armed} reduce={reduce} className="h-14 w-14">
        <span className="grid h-full w-full place-items-center text-[16px] font-black leading-none tracking-[-0.02em]" style={{ color: 'rgb(11 11 15 / 0.82)', background: 'linear-gradient(135deg,#f7f7f8,#cfd3dc)' }}>
          {feedInitials(feed.title)}
        </span>
      </RowCircle>
      <RowName text={name} sub={plural(feed.feeders.length, 'feeder')} className="text-[16px] tracking-[-0.015em]" />
    </button>
  );
});

// a feeder's place in the list carries the feed it sits in (the same handle in two feeds is two rows)
const feederId = (feedId: string | null, handle: string) => `rail:feeder:${feedId ?? ''}:${handle}`;

const FeederRow = memo(function FeederRow({ feeder, rowId, current, marked, armed, leaving, open, reduce, stack, label, onPick }: { feeder: HeaderFeeder; rowId: string; current: boolean; marked: boolean; armed: boolean; leaving: boolean; open: boolean; reduce: boolean; stack: number; label: string; onPick: (handle: string) => void }) {
  const lit = (current && !leaving) || armed;
  const ref = useRef<HTMLButtonElement | null>(null);
  useHeldPick(ref, lit);
  return (
    <button
      ref={ref}
      type="button"
      data-ledger={rowId}
      data-rail-feeder={feeder.handle}
      onClick={() => onPick(feeder.handle)}
      aria-pressed={current}
      aria-label={label}
      className={cn(ROW, 'h-[60px] gap-[26px] pl-[10px]')}
      style={{ zIndex: stack }}
    >
      <RowTile lit={lit} open={open} reduce={reduce} />
      <RowWash open={open} reduce={reduce} />
      <RowCircle ring={current && !leaving} lit={lit} marked={marked} armed={armed} reduce={reduce} className="h-[52px] w-[52px]">
        {/* decoded ahead by the phone's header, and asked for at once: a face never arrives after its row */}
        <FeederStoryAvatar feeder={{ handle: feeder.handle, profilePicUrl: feeder.profilePicUrl }} loading="eager" className="text-[13px]" />
      </RowCircle>
      <RowName text={feeder.handle} className="text-[15px] tracking-[-0.01em]" />
    </button>
  );
});

/* The one you're on sits on a raised tile (the page's own raised white, or a lift of light on dark): behind its circle
   in the shut column, the whole row once the rail opens (cut like the tab pill). It belongs to its row, so it lands
   where the pick is (coming up there, quickly, as the last one's goes) and travels with its row */
const ROW_TILE = 'pointer-events-none absolute inset-[3px] rounded-[18px] bg-(--fm-raised) shadow-[0_1px_2px_rgba(0,0,0,0.05),0_10px_24px_-10px_rgb(var(--fm-shadow-rgb)/0.3)] dark:bg-white/[0.09] dark:shadow-none';
function RowTile({ lit, open, reduce }: { lit: boolean; open: boolean; reduce: boolean }) {
  return (
    <span
      aria-hidden="true"
      className={ROW_TILE}
      style={{
        opacity: lit ? 1 : 0,
        clipPath: foldClip(open, '18px'),
        transition: reduce ? undefined : `opacity ${lit ? ms(220) : ms(140)}ms ease, clip-path ${foldTime(open)}ms ${CARRY_EASE_CSS}`,
      }}
    />
  );
}

function RailFeeds({ config, open, reduce }: { config: TabHeaderConfig | null; open: boolean; reduce: boolean }) {
  const feeds = config?.feeds ?? null;
  const handle = config?.handle ?? null;
  const { activeFeed, pickKey } = whoOf(feeds, config?.feedId, handle);
  const leaving = Boolean(config?.leaving);
  const armed = config?.armed ?? null;
  // the red story ring: a feeder with a post in the last 24 hours (every tab's rail alike; a tab can say otherwise)
  const fresh = useFreshHandles();
  const isMarked = (feederHandle: string) => (config?.marked ? config.marked(feederHandle) : fresh.has(feederHandle));
  const nameFeeder = (feederHandle: string) => (config?.feederLabel ? config.feederLabel(feederHandle) : `@${feederHandle}`);
  // the feed the list last opened, kept once it closes: closing is the gesture back into it
  const [hubFeed, setHubFeed] = useState<string | null>(activeFeed?.id ?? null);
  if (activeFeed && activeFeed.id !== hubFeed) setHubFeed(activeFeed.id);

  // the tab's handlers, read when a row is tapped: each row keeps one stable callback (a page publishing its header on
  // every render doesn't re-render a row that hasn't changed)
  const handlers = useRef(config);
  useIsoLayoutEffect(() => {
    handlers.current = config;
  });
  const pickAll = useCallback(() => handlers.current?.onAll(), []);
  // the open feed again: back to all of them, or, with one of its feeders picked, all of it
  const pickFeed = useCallback((id: string) => {
    const now = handlers.current;
    if (!now) return;
    if (now.feedId === id && !now.handle) now.onAll();
    else now.onFeed(id);
  }, []);
  const pickFeeder = useCallback((feederHandle: string) => handlers.current?.onFeeder(feederHandle), []);

  // a pick about to be made (the row the pointer comes onto, once a visit, or one pressed or focused): the tab gets it
  // ready
  const intentAt = useRef<Element | null>(null);
  const intent = (event: SyntheticEvent) => {
    const onIntent = handlers.current?.onIntent;
    if (!onIntent) return;
    const item = (event.target as Element | null)?.closest?.('button');
    if (!item) return;
    if (event.type === 'pointerover' && item === intentAt.current) return;
    intentAt.current = item;
    const data = (item as HTMLElement).dataset;
    if (data.railBadge) onIntent({ feedId: data.railBadge, handle: null });
    else if (data.railFeeder) onIntent({ feedId: handlers.current?.feedId ?? null, handle: data.railFeeder });
    else if (data.railAll != null) onIntent({ feedId: null, handle: null });
  };

  // the one you're on, brought into the list's view when the pick moves out of it (from a page, the keys)
  const [list, setList] = useState<HTMLDivElement | null>(null);
  useEffect(() => {
    const scroller = list?.firstElementChild as HTMLElement | null | undefined;
    const item = scroller?.querySelector<HTMLElement>('[aria-pressed="true"]');
    if (!scroller || !item) return;
    const box = scroller.getBoundingClientRect();
    const rect = item.getBoundingClientRect();
    const by = rect.top < box.top + 8 ? rect.top - box.top - 8 : rect.bottom > box.bottom - 8 ? rect.bottom - box.bottom + 8 : 0;
    if (by) scroller.scrollBy({ top: by, behavior: reduce ? 'auto' : 'smooth' });
  }, [list, pickKey, reduce]);

  // the list: all feeds, then every feed, or the open feed and its feeders (while the feeds load, the feeders the tab
  // has)
  const ids: string[] = ['rail:all'];
  const rows: ReactNode[] = [
    <AllRow key="rail:all" count={feeds ? feeds.length : null} back={Boolean(activeFeed)} current={Boolean(feeds) && !activeFeed && !handle} armed={armed === 'all'} leaving={leaving} open={open} reduce={reduce} stack={stackOf(0)} onPick={pickAll} />,
  ];
  const feederRow = (feeder: HeaderFeeder, feedId: string | null) => {
    const rowId = feederId(feedId, feeder.handle);
    rows.push(
      <FeederRow
        key={rowId}
        rowId={rowId}
        feeder={feeder}
        current={handle === feeder.handle}
        marked={isMarked(feeder.handle)}
        armed={armed === `feeder:${feeder.handle}`}
        leaving={leaving}
        open={open}
        reduce={reduce}
        stack={stackOf(ids.length)}
        label={nameFeeder(feeder.handle)}
        onPick={pickFeeder}
      />,
    );
    ids.push(rowId);
  };
  if (feeds) {
    (activeFeed ? [activeFeed] : feeds).forEach((feed) => {
      const expanded = feed.id === activeFeed?.id;
      rows.push(
        <FeedRow
          key={`rail:feed:${feed.id}`}
          feed={feed}
          expanded={expanded}
          current={expanded && !handle}
          marked={feed.feeders.some((feeder) => isMarked(feeder.handle))}
          armed={armed === `feed:${feed.id}`}
          leaving={leaving}
          open={open}
          reduce={reduce}
          stack={stackOf(ids.length)}
          onPick={pickFeed}
        />,
      );
      ids.push(`rail:feed:${feed.id}`);
    });
    if (activeFeed) activeFeed.feeders.forEach((feeder) => feederRow(feeder, activeFeed.id));
  } else {
    (config?.pendingFeeders ?? []).forEach((feeder) => feederRow(feeder, null));
  }
  // opening or closing a feed deals the list again (a feeder picked inside it only moves the rings)
  const deal = !feeds ? 'loading' : activeFeed ? `feed:${activeFeed.id}` : 'all';

  return (
    <div
      ref={setList}
      className="relative flex min-h-0 flex-1 flex-col border-t border-fg/10"
      onPointerOver={intent}
      onPointerDown={intent}
      onFocus={intent}
      onPointerLeave={() => {
        intentAt.current = null;
      }}
    >
      <Ledger
        ids={ids}
        deal={deal}
        axis="y"
        fan
        hub={hubFeed ? `rail:feed:${hubFeed}` : undefined}
        moveMs={FAN_MS}
        startOnDeal
        dealOnMount={false}
        role="group"
        label={activeFeed ? `Feeders in ${titleCase(activeFeed.title)}` : 'Feeds'}
        className="hide-scrollbar min-h-0 flex-1 overflow-y-auto overscroll-contain px-3 py-2"
      >
        {rows}
      </Ledger>
    </div>
  );
}

/* ── the foot: Manage and the tab's setting ───────────────────────────────── */

/* Lead's time range, Feed's zoom: the control the tab gives its header, drawn the rail's way (ControlPlacement): the
   track its width while the rail is open, the value alone while it is shut. Both are there, one showing, so the rail
   opening crosses one over to the other in place. Its place at the foot is always there, held at its height whether a
   tab brings a setting or not (Read's view is on the page), so a tab switch never moves anything around it */
const DOCK_IN_S = sec(0.36);
const DOCK_OUT_S = sec(0.14);
/* A tab's setting comes in from the side the tab is on (below the last, from below) as the last one goes the other
   way, the two crossing in the one place: the same way the page arrives */
const DOCK_ROLL = {
  enter: (dir: number) => ({ opacity: 0, transform: `translateY(${dir * 16}px)` }),
  center: { opacity: 1, transform: 'translateY(0px)', transition: { duration: DOCK_IN_S, delay: DOCK_OUT_S * 0.5, ease: carryCurve } },
  exit: (dir: number) => ({ opacity: 0, transform: `translateY(${-dir * 12}px)`, transition: { duration: DOCK_OUT_S, ease: 'easeIn' as const } }),
};
const DOCK_FADE = {
  enter: { opacity: 0 },
  center: { opacity: 1, transition: REDUCED },
  exit: { opacity: 0, transition: REDUCED },
};

function RailDock({ owner, dir, control, open, reduce, trackRef }: { owner: string | null; dir: number; control: ReactNode; open: boolean; reduce: boolean; trackRef: RefObject<HTMLDivElement | null> }) {
  const swap = (shown: boolean) => ({ opacity: shown ? 1 : 0, transition: reduce ? undefined : shown ? `opacity ${ms(200)}ms ease ${ms(90)}ms` : `opacity ${ms(90)}ms ease` });
  return (
    <div className="grid h-11">
      <AnimatePresence initial={false} custom={dir}>
        {control ? (
          <motion.div
            key={owner}
            custom={dir}
            className="grid [grid-area:1/1]"
            variants={reduce ? DOCK_FADE : DOCK_ROLL}
            initial="enter"
            animate="center"
            exit="exit"
          >
            {/* (two settings cross for a moment: the ref is the newest one's, and one going doesn't take it away) */}
            <div
              ref={(el) => {
                trackRef.current = el;
                return () => {
                  if (trackRef.current === el) trackRef.current = null;
                };
              }}
              className="[grid-area:1/1]"
              style={swap(open)}
              inert={!open}
            >
              <ControlPlacementContext.Provider value="rail">{control}</ControlPlacementContext.Provider>
            </div>
            <div className="pointer-events-none w-[72px] [grid-area:1/1]" style={swap(!open)}>
              <ControlPlacementContext.Provider value="rail-mini">{control}</ControlPlacementContext.Provider>
            </div>
          </motion.div>
        ) : null}
      </AnimatePresence>
    </div>
  );
}

/* ── the rail ─────────────────────────────────────────────────────────────── */

type ManageState = { mounted: boolean; open: boolean };

function SideRailBody({ tab }: { tab: string | null }) {
  const reduce = Boolean(useReducedMotion());
  const shown = useShownHeader(tab);
  const config = shown?.config ?? null;
  // the tab's setting, as the rail carries it (Read keeps its own on the page)
  const control = config ? (config.railControl === undefined ? config.control : config.railControl) ?? null : null;
  const asideRef = useRef<HTMLElement | null>(null);
  const trackRef = useRef<HTMLDivElement | null>(null);
  const manageRef = useRef<HTMLButtonElement | null>(null);
  const [open, setOpen] = useState(false);
  const [manage, setManage] = useState<ManageState>({ mounted: false, open: false });
  // which way the rail moved from the last tab to this one (down the rail, 1), kept beside the tab it was worked out for
  const owner = shown?.tab ?? null;
  const [dock, setDock] = useState({ owner, dir: 1 });
  if (dock.owner !== owner) setDock({ owner, dir: tabOrder(owner) >= tabOrder(dock.owner) ? 1 : -1 });

  /* Open while a mouse rests on it (a moment's rest: passing over on the way to the window's edge doesn't), while the
     keys are in it, or after a tap on its ground (a touch has no hover: a tap on a circle still picks it). It shuts a
     moment after the pointer leaves, when the keys leave it (not under a resting mouse), at a tap outside, at Escape,
     and when Manage's sheet opens */
  const timer = useRef(0);
  const mouseIn = useRef(false);
  const later = (next: boolean, wait: number) => {
    window.clearTimeout(timer.current);
    timer.current = window.setTimeout(() => setOpen(next), wait);
  };
  useEffect(() => () => window.clearTimeout(timer.current), []);
  const onPointerEnter = (event: ReactPointerEvent) => {
    if (event.pointerType !== 'mouse') return;
    mouseIn.current = true;
    later(true, OPEN_AFTER_MS);
  };
  const onPointerLeave = (event: ReactPointerEvent) => {
    if (event.pointerType !== 'mouse') return;
    mouseIn.current = false;
    later(false, SHUT_AFTER_MS);
  };
  const onPointerDown = (event: ReactPointerEvent) => {
    if (event.pointerType === 'mouse') return;
    if ((event.target as Element | null)?.closest('a, button')) return;
    window.clearTimeout(timer.current);
    setOpen((now) => !now);
  };
  const onFocus = (event: ReactFocusEvent) => {
    if ((event.target as Element).matches(':focus-visible')) {
      window.clearTimeout(timer.current);
      setOpen(true);
    }
  };
  const onBlur = (event: ReactFocusEvent) => {
    if (asideRef.current?.contains(event.relatedTarget as Node | null)) return;
    // a click on the rail's own ground moves focus to the page, and a window losing focus takes it away: neither is
    // the keys leaving
    if (mouseIn.current || !document.hasFocus()) return;
    later(false, 0);
  };
  useEffect(() => {
    if (!open) return undefined;
    const outside = (event: PointerEvent) => {
      if (event.pointerType !== 'mouse' && !asideRef.current?.contains(event.target as Node | null)) setOpen(false);
    };
    const keys = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setOpen(false);
    };
    document.addEventListener('pointerdown', outside, true);
    document.addEventListener('keydown', keys);
    return () => {
      document.removeEventListener('pointerdown', outside, true);
      document.removeEventListener('keydown', keys);
    };
  }, [open]);
  // shutting with the keys on the setting's track (it goes inert): they move to Manage, beside it, not to the page
  useIsoLayoutEffect(() => {
    if (open) return;
    if (trackRef.current?.contains(document.activeElement)) manageRef.current?.focus({ preventScroll: true });
  }, [open]);

  const openManage = () => {
    window.clearTimeout(timer.current);
    setOpen(false);
    setManage({ mounted: true, open: true });
  };
  const closeManage = useCallback(() => setManage((current) => ({ ...current, open: false })), []);
  const plateFade = reduce ? undefined : `opacity ${foldTime(open)}ms ${CARRY_EASE_CSS}`;

  return (
    <>
      {/* (Read's engine leaves taps on its rail alone: a tap here doesn't let go of the post you're reading) */}
      <aside
        ref={asideRef}
        id={config?.railId}
        aria-label="Tabs and feeds"
        data-open={open ? '' : undefined}
        className="pointer-events-none fixed bottom-3 left-3 top-[calc(12px+var(--pwa-top-fix,0px))] z-[100] w-[296px]"
      >
        {/* the shadow, on still plates behind the glass (its clip would cut its own): the column's, and the deeper one
            of the rail lying open over the page */}
        <span aria-hidden="true" className="fm-rail-shadow inset-y-0 left-0 w-[96px]" style={{ opacity: open ? 0 : 1, transition: plateFade }} />
        <span aria-hidden="true" className="fm-rail-shadow fm-rail-shadow--open inset-0" style={{ opacity: open ? 1 : 0, transition: plateFade }} />
        <div
          className="fm-depth-chrome fm-depth-chrome--rail pointer-events-auto flex h-full min-h-0 w-full flex-col"
          style={{ clipPath: foldClip(open, '28px'), transition: foldMove(open, reduce) }}
          onPointerEnter={onPointerEnter}
          onPointerLeave={onPointerLeave}
          onPointerDown={onPointerDown}
          onFocus={onFocus}
          onBlur={onBlur}
        >
          <RailTabs tab={tab} open={open} reduce={reduce} />
          <RailFeeds config={config} open={open} reduce={reduce} />
          <div className="grid shrink-0 gap-2 border-t border-fg/10 p-3">
            <button
              ref={manageRef}
              type="button"
              onClick={openManage}
              aria-label="Manage your feeds"
              className="group/row relative flex h-11 items-center gap-6 rounded-[16px] pl-2 text-fg/62 outline-none transition-colors duration-200 hover:text-fg focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]"
            >
              <RowWash open={open} reduce={reduce} />
              <span className={cn(ICON_BOX, 'relative')}>
                <SlidersHorizontal aria-hidden="true" size={20} strokeWidth={2.4} />
              </span>
              <SideName className="text-[15px] font-bold leading-none">
                Manage feeds
              </SideName>
            </button>
            <RailDock owner={owner} dir={dock.dir} control={control} open={open} reduce={reduce} trackRef={trackRef} />
          </div>
        </div>
      </aside>
      {manage.mounted ? <FeedManageSheet open={manage.open} scope={{ feedId: null, handle: null }} onClose={closeManage} /> : null}
    </>
  );
}

export function SideRail({ tab }: { tab: string | null }) {
  return useWideChrome() ? <SideRailBody tab={tab} /> : null;
}

/* ── a title row for the engine that writes into one (Read) ───────────────── */

/* From a laptop up the page has no heading (the rail says where you are), but Read's engine still writes into its title
   row: it fills the readout (#rd-hread) and toggles `reading` on the row (#rd-hdr). So the row is kept, with nothing in
   it to see; the readout floats at the foot of the page's column while a reel is read (readTab.css) */
function DeskRowBody({ tab }: { tab: string | null }) {
  const shown = useShownHeader(tab);
  if (!shown) return null;
  const { className, rowRef, rowId, rowClassName, baseClassName, rowOverlay } = shown.config;
  if (!rowRef && !rowOverlay) return null;
  return (
    <div className="pointer-events-none absolute left-0 top-0 h-0 w-0">
      <div className={className}>
        <div ref={rowRef} id={rowId} className={cn('relative', rowClassName)}>
          <div className={baseClassName} />
          {rowOverlay}
        </div>
      </div>
    </div>
  );
}

export function DeskRow({ tab }: { tab: string | null }) {
  return useWideChrome() ? <DeskRowBody tab={tab} /> : null;
}
