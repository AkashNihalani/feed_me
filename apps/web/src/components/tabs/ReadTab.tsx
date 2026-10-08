'use client';

/* ─────────────────────────────────────────────────────────────
   READ — the breakdown behind a feeder: the reader's rules for the
   account up top, then every post as a wall you read by Runs, Repeats
   or Ranks (a run's read opens on its breaker; a post's, its post
   mortem, in the sheet).

   The header is Lead's: your feeds, then a feed's feeders, on the
   story rail (ReadHeader). Which feeder is read is React's: when
   Read has a read for it, the plain-JS engine (components/read/
   readerEngine), ported from the approved prototype, is mounted on
   it and drives the page imperatively; when it doesn't, or the
   rail is on a feed or on all feeds, the page is the index
   (ReadIndex). The engine mounts while the tab is visible and fully
   tears down when it is hidden (Activity) or when the rail moves
   to something it has no read for.

   Reads are sample data for now (two feeders), see
   data/readerPreview.json; the feeds are the app's own.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { createPortal, flushSync } from 'react-dom';
import { useReducedMotion } from 'framer-motion';
import { mountReader } from '@/components/read/readerEngine';
import { leavePage } from '@/components/read/readMotion';
import { LAYER_HTML } from '@/components/read/readMarkup';
import ReadHeader from '@/components/read/ReadHeader';
import ReadIndex from '@/components/read/ReadIndex';
import {
  READ_POSTS,
  cachedFeeds,
  feedOf,
  loadFeeds,
  readFeeders,
  readOf,
  titleCase,
  type RailFeed,
} from '@/components/read/readFeeds';
import { appFont } from '@/lib/fonts';
import { acquireRootPageScroll } from '@/lib/rootScrollMode';
import { useCompressedOnScroll } from '@/lib/useCompressedOnScroll';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import { setTabScope, useTabScope, type TabScope } from '@/lib/tabScope';
import readerData from '@/data/readerPreview.json';
import '@/components/read/readTab.css';

// A stable object: React re-applies dangerouslySetInnerHTML whenever the object changes, which would wipe
// everything the engine has written into the layer.
const LAYER_MARKUP = { __html: LAYER_HTML };

type EngineState = { f: number; lens: string; n: number };
type Reader = { show: (k: number) => void; unmount: () => void };
// where the rail is: a feed (null = all feeds, undefined = not worked out until the feeds load) and a feeder. It is
// the pick every tab shares (tabScope): pick @anuj on Lead and Read opens on Anuj
type Scope = TabScope;

export default function ReadTab() {
  const { appShellStyle, useBrowserPageScroll } = useMobileImmersiveViewport();
  const [engineCompressed, setEngineCompressed] = useState(false);
  // what the engine is showing (which feeder, which view of the wall, how many posts), mirrored into the header
  const [engine, setEngine] = useState<EngineState>({ f: 0, lens: 'runs', n: 0 });
  const [head, setHead] = useState<HTMLDivElement | null>(null);
  const [main, setMain] = useState<HTMLDivElement | null>(null);
  const [layer, setLayer] = useState<HTMLDivElement | null>(null);
  const [feeds, setFeeds] = useState<RailFeed[] | null>(() => cachedFeeds());
  const scope = useTabScope();
  // each time the tab comes back on screen the index arrives afresh, piece by piece (a read does so on its own: the
  // engine mounts again). Effects run again on every reveal, so this counts the visits
  const [visit, setVisit] = useState(0);
  useLayoutEffect(() => {
    setVisit((count) => count + 1);
  }, []);
  // the overlays live at body level so position: fixed is never trapped by a transformed tab container
  const portalTarget = typeof document === 'undefined' ? null : document.body;

  // the index folds the header as Lead does (window scroll); a read folds it from the engine
  const noScroller = useRef<HTMLElement | null>(null);
  const indexCompressed = useCompressedOnScroll(noScroller, true, { collapseDistance: 150, expandDistance: 64, topGuard: 30 });

  const feedId = scope.feedId !== undefined ? scope.feedId : feeds ? feedOf(feeds, scope.handle) : undefined;
  const activeFeed = feeds && feedId ? feeds.find((feed) => feed.id === feedId) || null : null;
  const readIndex = readOf(scope.handle);
  const reading = readIndex != null;
  const compressed = reading ? engineCompressed : indexCompressed;
  const readable = useMemo(() => readFeeders(feeds), [feeds]);

  useEffect(() => {
    const controller = new AbortController();
    loadFeeds(controller.signal).then(setFeeds).catch(() => {});
    return () => controller.abort();
  }, []);

  // the engine measures and scrolls the window at every width, so the page scrolls while Read is visible.
  // The body stays overflow-visible so the root element is the only scroller (otherwise the body becomes
  // a scroll container of its own and the sticky lens on desktop stops sticking).
  // Re-applied whenever the shared viewport hook switches modes, because its own page-scroll lease resets
  // the body's overflow.
  useEffect(() => {
    const release = acquireRootPageScroll();
    const body = document.body;
    const prev = body.style.overflow;
    body.style.overflow = 'visible';
    return () => {
      body.style.overflow = prev;
      release();
    };
  }, [useBrowserPageScroll]);

  // a new page (a read opening, or the index moving) starts from the top; one read to another is the
  // engine's own fade, which scrolls itself
  const page = reading ? `read:${readIndex}` : `index:${feedId ?? 'all'}:${scope.handle ?? ''}`;
  const lastPage = useRef(page);
  useLayoutEffect(() => {
    if (lastPage.current === page) return;
    lastPage.current = page;
    window.scrollTo({ top: 0 });
  }, [page]);

  // mount before paint, on the feeder the rail is on, so a stale wall from the last visit never shows
  const readAt = useRef(readIndex);
  useLayoutEffect(() => {
    readAt.current = readIndex;
  }, [readIndex]);
  const reader = useRef<Reader | null>(null);
  useLayoutEffect(() => {
    if (!head || !main || !layer || !reading) return undefined;
    // the engine's unmount closes and empties every overlay in the layer, so it can stay in the body hidden
    const mounted = mountReader({
      data: readerData,
      layer,
      setCompressed: setEngineCompressed,
      onHeader: setEngine,
      initial: readAt.current ?? 0,
    }) as Reader;
    reader.current = mounted;
    return () => {
      mounted.unmount();
      reader.current = null;
      setEngineCompressed(false);
    };
  }, [head, main, layer, reading]);
  // one read to another: the mounted engine fades over to it
  useLayoutEffect(() => {
    if (readIndex != null) reader.current?.show(readIndex);
  }, [readIndex]);

  /* Moving to another page is one timeline for the whole tab, header and page together, in two beats.
     Leave: the page goes piece by piece (readMotion): every line, row, run box and post on screen lifts away on
     its own, in reading order; picked from a row, the rest dims and parts around it while its runs sweep and it
     lifts away last. In the header, what you tapped arms at once, the ring you were on draws back and the title
     lets go of its name (leaving, armed). Arrive: only then does the new page commit, so the jump back to the
     top happens while nothing is on screen, and the new page, the new title, the new ring and any change to the
     rail all arrive together, on the same curve. One read to another plays it the same way: the engine only
     swaps the read once the tab's leave is done. */
  const reduce = Boolean(useReducedMotion());
  const [leavingTo, setLeavingTo] = useState<Scope | null>(null);
  const indexBox = useRef<HTMLDivElement | null>(null);
  const leaving = useRef<{ next: Scope } | null>(null);
  const feedIdOf = (next: Scope) => (next.feedId !== undefined ? next.feedId : feeds ? feedOf(feeds, next.handle) : undefined);
  const pageOf = (next: Scope) => {
    const read = readOf(next.handle);
    return read != null ? `read:${read}` : `index:${feedIdOf(next) ?? 'all'}:${next.handle ?? ''}`;
  };
  const go = (next: Scope, from: HTMLElement | null = null) => {
    // a pick while the page is leaving: the leave commits the latest one
    if (leaving.current) {
      leaving.current.next = next;
      setLeavingTo(next);
      return;
    }
    const box = reading ? main : indexBox.current;
    if (pageOf(next) === page || reduce || !box) {
      setTabScope(next);
      return;
    }
    setLeavingTo(next);
    leaving.current = { next };
    leavePage(box, from && box.contains(from) ? from : null).then(() => {
      const target = leaving.current?.next ?? next;
      leaving.current = null;
      // commit while the old page's pieces are all gone; what they leave behind goes with the old page
      flushSync(() => {
        setLeavingTo(null);
        setTabScope(target);
      });
    });
  };

  // what the tapped thing is, for the header to arm it while the page leaves
  const armed = leavingTo
    ? leavingTo.handle ? `feeder:${leavingTo.handle}` : feedIdOf(leavingTo) ? `feed:${feedIdOf(leavingTo)}` : 'all'
    : null;
  // the latest place you asked for (a pick toggles against it, even mid-leave)
  const shown = leavingTo ?? scope;
  const shownFeedId = feedIdOf(shown);
  const shownFeed = feeds && shownFeedId ? feeds.find((feed) => feed.id === shownFeedId) || null : null;

  const pickAll = () => go({ feedId: null, handle: null });
  const pickFeed = (id: string) => go({ feedId: id, handle: null });
  // Lead's toggle: the feeder you're on, tapped again, goes back to its feed. A feeder stays in the feed
  // that is open; picked from anywhere else, the rail opens the feed it sits in
  const pickFeeder = (handle: string, from: HTMLElement | null = null) => {
    const home = shownFeed && shownFeed.feeders.some((feeder) => feeder.handle === handle)
      ? shownFeed.id
      : feeds ? feedOf(feeds, handle) : undefined;
    go(shown.handle === handle ? { feedId: home, handle: null } : { feedId: home, handle }, from);
  };

  // the header names where the page is (it changes when the page does)
  const label = scope.handle ? `@${scope.handle}` : activeFeed ? titleCase(activeFeed.title) : 'All feeds';
  const plural = (n: number, word: string) => `${n} ${word}${n === 1 ? '' : 's'}`;
  const eyebrow = scope.handle
    ? readIndex != null ? `${engine.f === readIndex && engine.n ? engine.n : READ_POSTS[readIndex]} of 100 posts` : 'Not read yet'
    : activeFeed ? plural(activeFeed.feeders.length, 'feeder') : feeds ? plural(feeds.length, 'feed') : 'Feeds';

  return (
    <>
      <ReadHeader
        feeds={feeds}
        fallback={readable}
        feedId={feedId}
        handle={scope.handle}
        label={label}
        eyebrow={eyebrow}
        view={engine.lens}
        showView={reading}
        leaving={leavingTo != null}
        armed={armed}
        compressed={compressed}
        topRef={setHead}
        fontClass={appFont.variable}
        onAll={pickAll}
        onFeed={pickFeed}
        onFeeder={pickFeeder}
      />
      {/* min-height only: Read scrolls the page at every width, so it never takes the fixed shell height
          that tabs with their own inner scroller use on desktop */}
      {/* data-tab-arrival: Read plays its own arrival when the tab comes back (the engine's, the index's), so the
          tab host leaves it be */}
      <div className={`rd rd-page ${appFont.variable}`} data-tab-arrival="own" style={{ minHeight: appShellStyle.minHeight }}>
        <div className="wrap" id="rd-main" ref={setMain} hidden={!reading} />
        {reading ? null : (
          <div ref={indexBox}>
            <ReadIndex
              key={`${page}:${visit}`}
              feeds={feeds}
              feed={activeFeed}
              handle={scope.handle}
              readable={readable}
              onFeeder={pickFeeder}
            />
          </div>
        )}
      </div>
      {portalTarget
        ? createPortal(
          <div
            ref={setLayer}
            id="rd-layer"
            className={`rd rd-layer ${appFont.variable}`}
            dangerouslySetInnerHTML={LAYER_MARKUP}
          />,
          portalTarget,
        )
        : null}
    </>
  );
}
