'use client';

/* ─────────────────────────────────────────────────────────────
   SIDE RAIL — the app's chrome from a laptop up. The browser
   already stacks its own bars across the top, and a laptop is
   short of height with width to spare, so the tabs, your feeds
   and the tab's setting stand down the left, and the page is all
   content from its first line.

   It floats, as the phone's dock does: no panel. Shut, it is a
   quiet column of big circles standing on the page: the tabs'
   icons, your feeds (a feed's badge, a feeder's face), Manage and
   the tab's setting on its ink chip. Point at it (or tab into it,
   or tap its ground) and the page beside it frosts over, the
   phone's frost, every name riding in beside its circle; leave it
   and it shuts (sideRail.css). The page never moves for it. Every
   icon and circle sits on one line 48px in from the rail's edge,
   open or shut, and what's near the pointer swells a touch, the
   way the Dock's icons do, in place.

   How it moves, all on the carry curve:
   - a tab: the ink tile glides to its icon at the click (before
     the page has answered), and the icon turns to the page's
     colour exactly where the tile is, as it passes; the page is
     simply there, as you left it (AppTabHost, lib/tabSheet), and
     the setting at the foot rolls over the way the tile went;
   - a feed: the phone's story rail stood on end. Its circle
     rises to the top while the other feeds tuck into its place
     and its feeders fan out of it, from behind it; closing, they
     go back in and the feeds come out again (Ledger, fan). All
     feeds, at the top, is the way back: its wedge turns round to
     point back, as the phone's does;
   - a pick: the one you're on grows a touch and wears the pick's
     ring and a soft shadow;
   - the setting at the foot (Lead's time range, Feed's zoom,
     Read's part) crosses over in a place that is always there, so
     nothing moves around it, and opening spreads it into chips.
   ───────────────────────────────────────────────────────────── */

import dynamic from 'next/dynamic';
import { memo, useCallback, useEffect, useLayoutEffect, useRef, useState, type FocusEvent as ReactFocusEvent, type PointerEvent as ReactPointerEvent, type ReactNode, type RefObject, type SyntheticEvent } from 'react';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { BookOpenText, LayoutGrid, SlidersHorizontal, Trophy, type LucideIcon } from 'lucide-react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { FAN_MS, Ledger } from '@/components/motion/Ledger';
import { carryCurve } from '@/lib/feedStream/motion';
import { feedInitials, titleCase } from '@/lib/feedLabels';
import { ms, sec } from '@/lib/tempo';
import { useFreshHandles } from '@/lib/feedStream/fresh';
import { aModalIsOpen, typingIn } from '@/lib/keys';
import { leaveTab, stayOnTab } from '@/lib/tabSheet';
import { cn } from '@/lib/utils';
import './sideRail.css';
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

/* The rail is laid out open (296px). What takes the pointer is cut to the column (96px) by a clip while it is shut, so
   the page beside a shut rail is the page's own: the clip lets go at once as the rail opens, and comes back as it
   shuts once the names have gone (they fade first: sideRail.css) */
const FOLD_PX = 200;
const NAMES_OUT_MS = ms(140);
const hitClip = (open: boolean) => `inset(0 ${open ? 0 : FOLD_PX}px 0 0 round 28px)`;
const hitMove = (open: boolean, reduce: boolean) => (open || reduce ? undefined : `clip-path 0s linear ${NAMES_OUT_MS}ms`);
// a pointer passing over the rail on its way somewhere doesn't open it: it opens once the pointer rests a moment, and
// shuts a moment after it leaves (coming back in time keeps it open)
const OPEN_AFTER_MS = 110;
const SHUT_AFTER_MS = 200;

/* ── the tabs ─────────────────────────────────────────────────────────────── */

const TABS: ReadonlyArray<{ key: string; label: string; href: string; icon: LucideIcon }> = [
  { key: 'lead', label: 'Lead', href: '/lead', icon: Trophy },
  { key: 'feed', label: 'Feed', href: '/', icon: LayoutGrid },
  { key: 'read', label: 'Read', href: '/read', icon: BookOpenText },
];
// where a tab stands in the rail, top to bottom
const tabOrder = (key: string | null) => TABS.findIndex((item) => item.key === key);
// a tab's row (44px) and the gap under it: what the ink tile glides by, row to row
const TAB_PITCH = 48;
const TAB_TILE_MS = ms(400);
const tileGlide = (reduce: boolean) => (reduce ? undefined : `transform ${TAB_TILE_MS}ms ${PICK_EASE}`);
// an icon in a big circle's box: icons and circles share the rail's one centre line
const ICON_BOX = 'grid h-full w-14 shrink-0 place-items-center';
const TAB_ROW_CLASS = 'flex h-11 items-center gap-4 rounded-[16px] pl-2';
// (the phone's tab words: big, the one you're on in ink and the others a step back)
const TAB_NAME = 'min-w-0 truncate text-[17px] font-bold leading-none tracking-[-0.02em]';

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
      if (typingIn(event.target) || aModalIsOpen()) return;
      const item = TABS.find((entry) => entry.label[0].toLowerCase() === event.key.toLowerCase());
      if (item) latest.current(item.key);
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);
}

// a name beside its icon or circle, past the column's edge: out of sight while the rail is shut, riding in over the
// frost as it opens (sideRail.css)
function SideName({ className, children }: { className: string; children: ReactNode }) {
  return <span className={cn('fm-rail-name relative min-w-0 truncate', className)}>{children}</span>;
}

function RailTabs({ tab, reduce }: { tab: string | null; reduce: boolean }) {
  /* The tile answers the click at once, in a render of its own: the address (and the page) follow in a heavier one a
     moment later, and a tile waiting for that set off late. It wears the tab tapped until the page arrives; the tab
     you're on, chosen again, takes back any tile still waiting */
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
      {/* the tab you're on: an ink tile on its icon alone (the circles below are people; a control isn't one), and in it
          the icons again in the page's colour, held still while it glides (they move back by as much as it moves on):
          an icon turns over exactly where the tile is, as it passes, and both moves are the compositor's. It swells
          with the icon it sits on (useMagnify) */}
      {index >= 0 ? (
        <span
          aria-hidden="true"
          data-rail-tile=""
          className="pointer-events-none absolute left-[26px] top-3 z-[15] h-11 w-11 overflow-hidden rounded-[14px] bg-fg shadow-[0_10px_22px_-10px_rgb(var(--fm-shadow-rgb)/0.55)] dark:shadow-[0_10px_24px_-8px_rgba(0,0,0,0.8)]"
          style={{ transform: `translateY(${y}px)`, transition: tileGlide(reduce) }}
        >
          <span className="absolute inset-x-0 top-0 grid gap-1 text-(--fm-page)" style={{ transform: `translateY(${-y}px)`, transition: tileGlide(reduce) }}>
            {TABS.map((item) => (
              <span key={item.key} className="grid h-11 w-11 place-items-center">
                <item.icon aria-hidden="true" size={20} strokeWidth={2.2} />
              </span>
            ))}
          </span>
        </span>
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
          className={cn('group/row relative z-10 outline-none transition-colors duration-200 hover:text-fg focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]', at === index ? 'text-fg' : 'text-fg/62', TAB_ROW_CLASS)}
        >
          <span data-rail-mag={at === index ? 'tab' : ''} className={cn(ICON_BOX, 'relative')}>
            <item.icon aria-hidden="true" size={20} strokeWidth={2.2} />
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
// the feed the list opens out of and closes back into stacks over every row, the ones before it too: the feeds coming
// back out of it come from behind it
const HUB_STACK = 41;

/* a row's circle: the pick's ring round it (drawn round when it is picked, back when something else is), its lift, the
   faint track while what was tapped waits for its page, and the accent dot for something new */
function RowCircle({ ring, lit, marked, armed, reduce, className, children }: { ring: boolean; lit: boolean; marked: boolean; armed: boolean; reduce: boolean; className: string; children: ReactNode }) {
  const lift = reduce ? undefined : `transform ${PICK_MS}ms ${PICK_EASE}`;
  return (
    <span data-rail-mag="" className={cn('relative z-10 grid shrink-0 place-items-center rounded-full p-[3px]', className)} style={{ transform: `scale(${lit ? PICKED_SCALE : 1})`, transition: lift }}>
      <span
        aria-hidden="true"
        className="pointer-events-none absolute inset-[4px] rounded-full shadow-[0_10px_22px_-8px_rgb(var(--fm-shadow-rgb)/0.55)] dark:shadow-[0_10px_24px_-6px_rgba(0,0,0,0.8)]"
        style={{ opacity: lit ? 1 : 0, transition: pickFade(reduce) }}
      />
      <StoryRing on={ring} reduce={reduce} />
      {armed ? <ArmTrack reduce={reduce} /> : null}
      {/* something new: the red story ring (not round what you're on: that wears the ink ring) */}
      {/* (round the whole circle: its face sits 3px in, a step closer to the edge than the header rail's) */}
      {marked ? <FreshRing className="inset-0" shown={!ring && !lit} reduce={reduce} /> : null}
      <span className="relative z-20 grid h-full w-full place-items-center overflow-hidden rounded-full border-2 border-white dark:border-[var(--fm-ink)]">
        {children}
      </span>
    </span>
  );
}

/* a row's name, in bold ink, past the column's edge (the one you're on is told by its ring and its lift, never by its
   name changing); a feed's count of feeders under it. It rides in as the rail opens (fm-rail-name), and moves on its
   own when its row comes or goes (data-ledger-label), coming in from behind its own left edge */
function RowName({ text, sub, className }: { text: string; sub?: string; className: string }) {
  return (
    <span className="fm-rail-name relative z-10 min-w-0 flex-1 overflow-hidden pr-3">
      <span data-ledger-label="" className="grid gap-1">
        <span className={cn(NAME, 'font-bold text-fg', className)}>{text}</span>
        {sub ? <span className="truncate text-[12px] font-semibold leading-none text-fg/60">{sub}</span> : null}
      </span>
    </span>
  );
}

const plural = (count: number, word: string) => `${count} ${word}${count === 1 ? '' : 's'}`;

/* All feeds: with a feed open it is the way back, and its wedge itself turns round to point back, on the fan's clock,
   the way the phone's does (TabHeader's WedgeMark) */
const AllRow = memo(function AllRow({ count, back, current, armed, leaving, reduce, stack, onPick }: { count: number | null; back: boolean; current: boolean; armed: boolean; leaving: boolean; reduce: boolean; stack: number; onPick: () => void }) {
  const lit = (current && !leaving) || armed;
  const ref = useRef<HTMLButtonElement | null>(null);
  useHeldPick(ref, lit);
  const turn = reduce ? undefined : `transform ${FAN_MS}ms ${PICK_EASE}`;
  return (
    <button ref={ref} type="button" data-ledger="rail:all" data-rail-all="" onClick={onPick} aria-pressed={current} aria-label={back ? 'Back to all feeds' : 'All feeds'} className={cn(ROW, 'h-[62px] gap-[18px] pl-[11px]')} style={{ zIndex: stack }}>
      <RowCircle ring={current && !leaving} lit={lit} marked={false} armed={armed} reduce={reduce} className="h-[50px] w-[50px]">
        <span className="relative grid h-full w-full place-items-center" style={{ background: 'var(--lead-wedge-surface)' }}>
          <svg aria-hidden="true" viewBox="0 0 42 42" className="block h-[24px] w-[24px]" style={{ transform: back ? 'rotate(180deg)' : 'rotate(0deg)', transition: turn }}>
            <path d={WEDGE_PATH} fill="none" stroke="var(--lead-wedge-mark)" strokeWidth="4" strokeLinecap="round" strokeLinejoin="round" />
          </svg>
        </span>
      </RowCircle>
      <RowName text="All feeds" sub={back ? 'Back' : count == null ? undefined : plural(count, 'feed')} className="text-[15px] tracking-[-0.015em]" />
    </button>
  );
});

const FeedRow = memo(function FeedRow({ feed, expanded, current, marked, armed, leaving, reduce, stack, onPick }: { feed: HeaderFeed; expanded: boolean; current: boolean; marked: boolean; armed: boolean; leaving: boolean; reduce: boolean; stack: number; onPick: (id: string) => void }) {
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
      className={cn(ROW, 'h-[62px] gap-[18px] pl-[11px]')}
      style={{ zIndex: stack }}
    >
      <RowCircle ring={current && !leaving} lit={lit} marked={marked} armed={armed} reduce={reduce} className="h-[50px] w-[50px]">
        <span className="grid h-full w-full place-items-center text-[15px] font-black leading-none tracking-[-0.02em]" style={{ color: 'rgb(11 11 15 / 0.82)', background: 'linear-gradient(135deg,#f7f7f8,#cfd3dc)' }}>
          {feedInitials(feed.title)}
        </span>
      </RowCircle>
      <RowName text={name} sub={plural(feed.feeders.length, 'feeder')} className="text-[15px] tracking-[-0.015em]" />
    </button>
  );
});

// a feeder's place in the list carries the feed it sits in (the same handle in two feeds is two rows)
const feederId = (feedId: string | null, handle: string) => `rail:feeder:${feedId ?? ''}:${handle}`;

const FeederRow = memo(function FeederRow({ feeder, rowId, current, marked, armed, leaving, reduce, stack, label, onPick }: { feeder: HeaderFeeder; rowId: string; current: boolean; marked: boolean; armed: boolean; leaving: boolean; reduce: boolean; stack: number; label: string; onPick: (handle: string) => void }) {
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
      className={cn(ROW, 'h-[58px] gap-[20px] pl-[13px]')}
      style={{ zIndex: stack }}
    >
      <RowCircle ring={current && !leaving} lit={lit} marked={marked} armed={armed} reduce={reduce} className="h-[46px] w-[46px]">
        {/* decoded ahead by the phone's header, and asked for at once: a face never arrives after its row */}
        <FeederStoryAvatar feeder={{ handle: feeder.handle, profilePicUrl: feeder.profilePicUrl }} loading="eager" className="text-[13px]" />
      </RowCircle>
      <RowName text={feeder.handle} className="text-[14px] tracking-[-0.01em]" />
    </button>
  );
});

function RailFeeds({ config, reduce }: { config: TabHeaderConfig | null; reduce: boolean }) {
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
    <AllRow key="rail:all" count={feeds ? feeds.length : null} back={Boolean(activeFeed)} current={Boolean(feeds) && !activeFeed && !handle} armed={armed === 'all'} leaving={leaving} reduce={reduce} stack={stackOf(0)} onPick={pickAll} />,
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
          reduce={reduce}
          stack={feed.id === hubFeed ? HUB_STACK : stackOf(ids.length)}
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

  // the list fades out at an edge with more of it to scroll to (sideRail.css): there's no box to cut it against
  useEffect(() => {
    const scroller = list?.firstElementChild as HTMLElement | null | undefined;
    if (!scroller) return undefined;
    const edges = () => {
      const top = scroller.scrollTop > 1;
      const foot = scroller.scrollTop + scroller.clientHeight < scroller.scrollHeight - 1;
      scroller.toggleAttribute('data-cut', top || foot);
      scroller.style.setProperty('--fm-rail-fade-top', top ? '28px' : '0px');
      scroller.style.setProperty('--fm-rail-fade-foot', foot ? '52px' : '0px');
    };
    edges();
    // (a feed opening or closing deals its rows in and out on the fan's clock: read it again once they're in)
    const settled = window.setTimeout(edges, FAN_MS + 80);
    const resize = new ResizeObserver(edges);
    resize.observe(scroller);
    scroller.addEventListener('scroll', edges, { passive: true });
    return () => {
      window.clearTimeout(settled);
      resize.disconnect();
      scroller.removeEventListener('scroll', edges);
    };
  }, [list, deal]);

  return (
    <div
      ref={setList}
      className="relative flex min-h-0 flex-1 flex-col"
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
        className="fm-rail-list hide-scrollbar min-h-0 flex-1 overflow-y-auto overscroll-contain px-3 py-2"
      >
        {rows}
      </Ledger>
    </div>
  );
}

/* ── the foot: Manage and the tab's setting ───────────────────────────────── */

/* Lead's time range, Feed's zoom, Read's part: the control the tab gives its header, drawn the rail's way
   (ControlPlacement), as the phone's chips: shut, the value alone on its ink capsule in the column; open, the others
   spread out beside it (sideRail.css). Its place at the foot is always there, held at its height whether a tab brings a
   setting or not, so a tab switch never moves anything around it */
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

function RailDock({ owner, dir, control, reduce, trackRef }: { owner: string | null; dir: number; control: ReactNode; reduce: boolean; trackRef: RefObject<HTMLDivElement | null> }) {
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
            >
              <ControlPlacementContext.Provider value="rail">{control}</ControlPlacementContext.Provider>
            </div>
          </motion.div>
        ) : null}
      </AnimatePresence>
    </div>
  );
}

/* ── magnify ──────────────────────────────────────────────────────────────── */

/* What's near the pointer swells, the way the Dock's icons do, in place: nothing moves along the column (the rail never
   jumps), and at 1.06× it is a lift under the pointer, never a zoom: a circle keeps its air. Each eases toward the pointer once a frame, a breath behind
   it, on its own `scale` (the pick's lift is its transform), and the ink tile swells with the icon it sits on. A mouse
   only, while it is over the rail */
const MAG_PEAK = 1.06;
const MAG_REACH = 80;
const MAG_LAG = 70;

function useMagnify(panelRef: RefObject<HTMLDivElement | null>, reduce: boolean) {
  useEffect(() => {
    const panel = panelRef.current;
    if (!panel || reduce) return undefined;
    const scales = new WeakMap<HTMLElement, number>();
    let raf = 0;
    let last = 0;
    let y: number | null = null;
    const tick = (now: number) => {
      raf = 0;
      const dt = last ? Math.min(50, now - last) : 16;
      last = now;
      const ease = 1 - Math.exp(-dt / MAG_LAG);
      let moving = false;
      let tile = 1;
      panel.querySelectorAll<HTMLElement>('[data-rail-mag]').forEach((el) => {
        let target = 1;
        if (y !== null) {
          const box = el.getBoundingClientRect();
          const away = Math.abs(y - (box.top + box.height / 2));
          if (away < MAG_REACH) target = 1 + (MAG_PEAK - 1) * (0.5 + 0.5 * Math.cos((Math.PI * away) / MAG_REACH));
        }
        const was = scales.get(el) ?? 1;
        let next = was + (target - was) * ease;
        if (Math.abs(target - next) < 0.002) next = target;
        else moving = true;
        if (next !== was) {
          el.style.scale = next === 1 ? '' : next.toFixed(4);
          scales.set(el, next);
        }
        if (el.dataset.railMag === 'tab') tile = next;
      });
      const tileEl = panel.querySelector<HTMLElement>('[data-rail-tile]');
      if (tileEl) tileEl.style.scale = tile === 1 ? '' : tile.toFixed(4);
      // rows on their way somewhere (a feed opening) carry their circles past a resting pointer: keep reading them
      if (moving || (y !== null && panel.getAnimations({ subtree: true }).length > 0)) raf = requestAnimationFrame(tick);
      else last = 0;
    };
    const kick = () => {
      if (!raf) raf = requestAnimationFrame(tick);
    };
    const onMove = (event: PointerEvent) => {
      if (event.pointerType !== 'mouse') return;
      y = event.clientY;
      kick();
    };
    const onLeave = () => {
      y = null;
      kick();
    };
    panel.addEventListener('pointermove', onMove);
    panel.addEventListener('pointerleave', onLeave);
    return () => {
      window.cancelAnimationFrame(raf);
      panel.removeEventListener('pointermove', onMove);
      panel.removeEventListener('pointerleave', onLeave);
    };
  }, [panelRef, reduce]);
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
  const panelRef = useRef<HTMLDivElement | null>(null);
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
  // shutting with the keys on the setting's chips (shut, they're out of sight): they move to Manage, beside it, not to the page
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
  useMagnify(panelRef, reduce);

  return (
    <>
      {/* (Read's engine leaves taps on its rail alone: a tap here doesn't let go of the post you're reading) */}
      <aside
        ref={asideRef}
        id={config?.railId}
        aria-label="Tabs and feeds"
        data-open={open ? '' : undefined}
        className="fm-rail pointer-events-none fixed bottom-3 left-3 top-[calc(12px+var(--pwa-top-fix,0px))] z-[100] w-[296px]"
      >
        {/* the page beside the open rail, frosted: the phone's frost, out from the window's edge (sideRail.css) */}
        <span aria-hidden="true" className="fm-rail-frost" />
        <div
          ref={panelRef}
          className="pointer-events-auto relative flex h-full min-h-0 w-full flex-col"
          style={{ clipPath: hitClip(open), transition: hitMove(open, reduce) }}
          onPointerEnter={onPointerEnter}
          onPointerLeave={onPointerLeave}
          onPointerDown={onPointerDown}
          onFocus={onFocus}
          onBlur={onBlur}
        >
          <RailTabs tab={tab} reduce={reduce} />
          <RailFeeds config={config} reduce={reduce} />
          <div className="grid shrink-0 gap-2 p-3">
            <button
              ref={manageRef}
              type="button"
              onClick={openManage}
              aria-label="Manage your feeds"
              className="group/row relative flex h-11 items-center gap-6 rounded-[16px] pl-2 text-fg/62 outline-none transition-colors duration-200 hover:text-fg focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-[var(--fm-accent-bright)]"
            >
              <span data-rail-mag="" className={cn(ICON_BOX, 'relative')}>
                <SlidersHorizontal aria-hidden="true" size={18} strokeWidth={2.4} />
              </span>
              <SideName className="text-[14px] font-bold leading-none">
                Manage feeds
              </SideName>
            </button>
            <RailDock owner={owner} dir={dock.dir} control={control} reduce={reduce} trackRef={trackRef} />
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
