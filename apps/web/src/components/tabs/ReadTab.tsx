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

   Above a read sits its preview (ReadStories: the read as stories, in
   the same order every time), then the wall. The Feeder File
   (FeederFile), the feeder's world as one map, is tucked away: a layer
   over the read, opened from a story, its door, or a rule or thing the
   wall names, flown to what was asked for (Anuj and Lakmé for now).

   Reads are sample data for now (two feeders), see
   data/readerPreview.json; the feeds are the app's own.

   A feeder read in three parts (Read17: Story · Strategy · Split,
   Anuj and Lakmé for now) takes the page instead: none of the above
   is mounted for it, the header folds on the window's scroll as the
   index's does, and its switch is the part switch.
   ───────────────────────────────────────────────────────────── */

import { useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { createPortal, flushSync } from 'react-dom';
import { useReducedMotion } from 'framer-motion';
import { mountReader } from '@/components/read/readerEngine';
import { leavePage } from '@/components/read/readMotion';
import { LAYER_HTML } from '@/components/read/readMarkup';
import ReadHeader from '@/components/read/ReadHeader';
import ReadIndex from '@/components/read/ReadIndex';
import FeederFile, { feederAvatar, hasFeederFile } from '@/components/read/FeederFile';
import ReadStories, { type ReadOpen } from '@/components/read/ReadStories';
import ReadSections, { FileDoor, sectionsFor } from '@/components/read/ReadSections';
import { closeLayer, openLayer, originBox } from '@/components/read/readLayerMotion';
import Read17, { read17Of, type Chapter, type Part } from '@/components/read/read17/Read17';
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
import { isSwitchingTo, wideTabMotion } from '@/lib/tabSheet';
import readerData from '@/data/readerPreview.json';
import '@/components/read/readTab.css';
import { announceFirstScreen } from '@/lib/launch';

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
  /* The title row the engine writes into is the phone header's below 1024px and the side rail's (DeskRow) above: crossing
     that width hands it from one to the other in one commit. The engine finds it by its id, so it stays mounted
     through the hand-over (what you were reading, the view, the pick all kept): only a row going with none to follow
     lets it go */
  const topRef = useCallback((el: HTMLDivElement | null) => {
    if (!el) return undefined;
    setHead(el);
    return () => setHead((current) => (current === el ? null : current));
  }, []);
  const hasHead = head != null;
  const [main, setMain] = useState<HTMLDivElement | null>(null);
  const [layer, setLayer] = useState<HTMLDivElement | null>(null);
  const [feeds, setFeeds] = useState<RailFeed[] | null>(() => cachedFeeds());
  // the index has its feeds: a launch veil goes
  useEffect(() => {
    if (feeds) announceFirstScreen();
  }, [feeds]);
  const scope = useTabScope();
  /* The tab coming back on screen shows the page as it was left, at rest: no entrance plays again (the index keeps its
     pieces; the read's engine, which mounts again on every reveal, draws quietly). Effects run again on every reveal
     (Activity), so this one tells the engine's mount below that this run is a return, not a page change */
  const shownBefore = useRef(false);
  const returning = useRef(false);
  useLayoutEffect(() => {
    returning.current = shownBefore.current;
    shownBefore.current = true;
  }, []);
  // the overlays live at body level so position: fixed is never trapped by a transformed tab container
  const portalTarget = typeof document === 'undefined' ? null : document.body;

  // the index folds the header as Lead does (window scroll); a read folds it from the engine
  const noScroller = useRef<HTMLElement | null>(null);
  const indexCompressed = useCompressedOnScroll(noScroller, true, { collapseDistance: 150, expandDistance: 64, topGuard: 30 });

  const feedId = scope.feedId !== undefined ? scope.feedId : feeds ? feedOf(feeds, scope.handle) : undefined;
  const activeFeed = feeds && feedId ? feeds.find((feed) => feed.id === feedId) || null : null;
  // a read in three parts takes the page: the engine, the stories, the sections and the Feeder File stand down for it
  const r17 = read17Of(scope.handle);
  const [part, setPart] = useState<Part>('story');
  const jump17 = useRef<((target: string) => void) | null>(null);
  // a part picked in the header (or one of the three on the side rail): the page goes there
  const toPart = useCallback((next: Part | Chapter) => {
    setPart(next === 'strategy' ? 'slices' : next);
    jump17.current?.(next);
  }, []);
  const readIndex = r17 ? null : readOf(scope.handle);
  const reading = readIndex != null;
  const compressed = reading ? engineCompressed : indexCompressed;
  const readable = useMemo(() => readFeeders(feeds), [feeds]);
  // the read's sections (This week · Phases · Lanes · Drivers), for the feeders they've been counted for
  const sec = useMemo(() => (r17 ? null : sectionsFor(scope.handle)), [scope.handle, r17]);
  // a read with a Feeder File (preview data: Anuj and Lakmé) opens on its stories, the Feeder File tucked away
  const fileHandle = reading && hasFeederFile(scope.handle) ? scope.handle : null;
  // the read as stories (the reader's preview data carries them as reader.stories)
  const storyFeeder = useMemo(() => {
    if (!fileHandle) return null;
    const f = (readerData as unknown as { feeders: { handle: string; posts: { id: string; thumb?: string; label?: string }[]; reader?: { stories?: unknown[] } }[] }).feeders.find((x) => x.handle === fileHandle);
    return f && f.reader?.stories?.length ? (f as Parameters<typeof ReadStories>[0]['feeder']) : null;
  }, [fileHandle]);
  // the Feeder File's layer: which feeder it's for (anything else reads as closed), whether it's up, and what to fly to
  // (k counts the asks, so asking for the same thing again flies there again). Once opened it stays mounted, hidden,
  // until the feeder changes, so it opens again at once
  const [file, setFile] = useState<{ handle: string | null; open: boolean; id: string | null; k: number }>({ handle: null, open: false, id: null, k: 0 });
  const fileUp = !!fileHandle && file.handle === fileHandle;
  const fileOpen = fileUp && file.open;
  // the layer is a frame (fileLayer) over a layer of its own (fileInner, where the map's full screen and sheet go too);
  // it opens out of what was tapped (from) and closes back into it (readLayerMotion)
  const fileLayer = useRef<HTMLDivElement | null>(null);
  const fileInner = useRef<HTMLDivElement | null>(null);
  const fileFrom = useRef<(() => DOMRect | null) | null>(null);
  const closing = useRef(false);
  // the map ready (in its full screen, on a phone) before the layer opens out of the card: a map still loading would
  // open as an empty window. It's put together ahead, out of sight, once the stories have had a moment
  const fileReady = useRef(false);
  const revealWait = useRef<number | null>(null);
  const reveal = useCallback(() => {
    if (revealWait.current != null) { window.clearTimeout(revealWait.current); revealWait.current = null; }
    const el = fileLayer.current;
    const inner = fileInner.current;
    if (!el || !inner) return;
    el.style.opacity = '';
    openLayer(el, inner, originBox(fileFrom.current));
  }, []);
  const onFileReady = useCallback(() => {
    fileReady.current = true;
    if (revealWait.current != null) reveal();
  }, [reveal]);
  useEffect(() => {
    fileReady.current = false;
    if (!fileHandle) return undefined;
    const park = () => setFile((f) => (f.handle === fileHandle ? f : { handle: fileHandle, open: false, id: null, k: 0 }));
    const w = window as Window & { requestIdleCallback?: (cb: () => void, o?: { timeout: number }) => number; cancelIdleCallback?: (id: number) => void };
    let idle = 0;
    const t = window.setTimeout(() => { if (w.requestIdleCallback) idle = w.requestIdleCallback(park, { timeout: 1500 }); else park(); }, 1400);
    return () => { window.clearTimeout(t); if (idle && w.cancelIdleCallback) w.cancelIdleCallback(idle); };
  }, [fileHandle]);
  const openFile = useCallback((id: string | null, from?: (() => DOMRect | null) | null) => {
    if (!fileHandle) return;
    fileFrom.current = from ?? null;
    closing.current = false;
    setFile((f) => ({ handle: fileHandle, open: true, id: id ?? (f.handle === fileHandle ? f.id : null), k: id ? f.k + 1 : f.k }));
  }, [fileHandle]);
  // the layer shrinks back into what it opened from, then hides
  const closeFile = useCallback(() => {
    const el = fileLayer.current;
    const inner = fileInner.current;
    const done = () => { closing.current = false; setFile((f) => ({ ...f, open: false })); };
    if (!el || !inner) { done(); return; }
    if (closing.current) return;
    closing.current = true;
    closeLayer(el, inner, originBox(fileFrom.current), done);
  }, []);
  const onStories = useCallback((o: ReadOpen) => {
    if (o.posts) { document.getElementById('rd-main')?.scrollIntoView({ behavior: 'smooth', block: 'start' }); return; }
    if (o.file) openFile(o.file === true ? null : o.file.post || o.file.law || o.file.node || null, o.from);
  }, [openFile]);
  // up: it opens out of what was tapped, the page under it holds still, and the read's sheets come up over it
  useLayoutEffect(() => {
    if (!fileOpen) return undefined;
    const el = fileLayer.current;
    if (fileReady.current) reveal();
    else if (el) {
      // (held unseen until the map is ready; if it never says so, it opens anyway)
      el.style.opacity = '0';
      revealWait.current = window.setTimeout(reveal, 2500);
    }
    // (the page under it held still with a mouse; a finger's touches are the layer's own, and on an iPhone locking the
    // page's scroll moves it when it's let go)
    const lock = window.matchMedia('(hover: hover) and (pointer: fine)').matches;
    const prev = document.documentElement.style.overflow;
    if (lock) document.documentElement.style.overflow = 'hidden';
    document.body.classList.add('ff-layer');
    return () => { if (lock) document.documentElement.style.overflow = prev; document.body.classList.remove('ff-layer'); };
  }, [fileOpen, reveal]);
  // a rule or a thing the wall names opens the layer, flown there; a run or a post mortem opened from the map belongs to
  // the read below, so the layer gives way; on a phone, closing the map's own full screen closes the layer with it
  useEffect(() => {
    if (!fileHandle) return undefined;
    const ask = (e: Event) => { const d = (e as CustomEvent).detail; openFile(d && typeof d === 'object' ? d.id : d); };
    // the map's own "Close" (in its full screen on a phone) hands back here: the layer closes into the card
    const leave = () => closeFile();
    const away = () => setFile((f) => ({ ...f, open: false }));
    const full = (e: Event) => { if (!(e as CustomEvent).detail && window.innerWidth < 860) away(); };
    const key = (e: KeyboardEvent) => { if (e.key === 'Escape' && document.body.classList.contains('ff-layer') && !document.body.classList.contains('ff-full')) closeFile(); };
    window.addEventListener('rd:law', ask); window.addEventListener('rd:node', ask);
    ['ff:run', 'ff:rule', 'ff:reopen'].forEach((n) => window.addEventListener(n, away));
    window.addEventListener('ff:full', full); window.addEventListener('keydown', key); window.addEventListener('ff:leave', leave);
    return () => {
      window.removeEventListener('ff:leave', leave);
      window.removeEventListener('rd:law', ask); window.removeEventListener('rd:node', ask);
      ['ff:run', 'ff:rule', 'ff:reopen'].forEach((n) => window.removeEventListener(n, away));
      window.removeEventListener('ff:full', full); window.removeEventListener('keydown', key);
    };
  }, [fileHandle, openFile, closeFile]);

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
  const page = r17 ? `r17:${r17}` : reading ? `read:${readIndex}` : `index:${feedId ?? 'all'}:${scope.handle ?? ''}`;
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
    if (!hasHead || !main || !layer || !reading) return undefined;
    // the engine's unmount closes and empties every overlay in the layer, so it can stay in the body hidden
    // (from a laptop up a tab switch bringing Read in has its own arrival, lib/tabSheet riseIn, and a return shows the
    // page at rest; a phone plays Read's own arrival, as it always has)
    const quiet = wideTabMotion() && (returning.current || isSwitchingTo('read'));
    returning.current = false;
    const mounted = mountReader({
      data: readerData,
      layer,
      setCompressed: setEngineCompressed,
      onHeader: setEngine,
      initial: readAt.current ?? 0,
      quiet,
    }) as Reader;
    reader.current = mounted;
    return () => {
      mounted.unmount();
      reader.current = null;
      setEngineCompressed(false);
    };
  }, [hasHead, main, layer, reading]);
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
    const three = read17Of(next.handle);
    if (three) return `r17:${three}`;
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
    ? r17 ? 'Week of 19–25 Oct' : readIndex != null ? `${engine.f === readIndex && engine.n ? engine.n : READ_POSTS[readIndex]} of 100 posts` : sec ? `${sec.since} posts since April` : 'Not read yet'
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
        chapter={r17 ? { value: part, onChange: toPart } : null}
        leaving={leavingTo != null}
        armed={armed}
        compressed={compressed}
        topRef={topRef}
        fontClass={appFont.variable}
        onAll={pickAll}
        onFeed={pickFeed}
        onFeeder={pickFeeder}
      />
      {storyFeeder && !sec ? <ReadStories key={`stories-${fileHandle}`} feeder={storyFeeder} avatar={feederAvatar(fileHandle)} onOpen={onStories} /> : null}
      {/* min-height only: Read scrolls the page at every width, so it never takes the fixed shell height
          that tabs with their own inner scroller use on desktop */}
      {r17 ? <Read17 key={r17} acct={r17} minHeight={appShellStyle.minHeight} onChapter={setPart} jumpRef={jump17} /> : null}
      {/* data-tab-arrival: Read plays its own arrival when the tab comes back (the engine's, the index's), so the
          tab host leaves it be */}
      {r17 ? null : (
        <div className={`rd rd-page ${appFont.variable}`} data-tab-arrival="own" style={{ minHeight: appShellStyle.minHeight }}>
          {sec ? <ReadSections key={`sections-${scope.handle}`} data={sec} ownWall={!reading} onFile={fileHandle ? () => openFile(null) : null} /> : null}
          <div className="wrap" id="rd-main" ref={setMain} hidden={!reading} />
          {sec && reading && fileHandle ? <div className="rv wrap rv-after"><FileDoor name={sec.name} onFile={() => openFile(null)} /></div> : null}
          {reading || sec ? null : (
            <div ref={indexBox}>
              <ReadIndex
                key={page}
                feeds={feeds}
                feed={activeFeed}
                handle={scope.handle}
                readable={readable}
                onFeeder={pickFeeder}
              />
            </div>
          )}
        </div>
      )}
      {portalTarget && fileUp && fileHandle
        ? createPortal(
          // (parked out of sight until it's opened: laid out, so the map is drawn and ready, but unseen, untouchable and
          // not drawing)
          <div ref={fileLayer} className={`rff ${appFont.variable}`} data-parked={fileOpen ? undefined : ''} inert={!fileOpen} aria-hidden={!fileOpen} role="dialog" aria-modal={fileOpen} aria-label="The Feeder File">
            <div ref={fileInner} className="rff-in">
              <div className="rff-bar">
                <button type="button" onClick={closeFile}><span aria-hidden="true">←</span>Read</button>
                <b>The Feeder File</b><span>@{fileHandle}</span>
              </div>
              <div className="rff-body"><FeederFile key={fileHandle} handle={fileHandle} target={file.id} targetKey={file.k} full shown={fileOpen} layer={fileInner} onReady={onFileReady} /></div>
            </div>
          </div>,
          portalTarget,
        )
        : null}
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
