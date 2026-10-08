'use client';

/* ─────────────────────────────────────────────────────────────
   FEED — the tab: every post your feeders made in the rolling 90
   days, newest first (or by the day their latest result landed),
   at three zooms: DAY (one post at a time), WEEK, MONTH.

   Who: the shared header's story rail (lib/tabScope: the same
   pick as Lead and Read). How close: a pinch, or the header's
   zoom control. The order: the profile's tabs (Newest posts ·
   Latest results).

   The page scrolls itself (the document, as on Lead and Read), so
   content runs under Safari's translucent bars. A pick or a new
   order is made ready while the screen holds still (its posts
   near the day you're on), then the page moves to it at the same
   zoom and day: the posts both hold carry to their new places,
   the rest are replaced in place. The stream
   (components/feed/stream/FeedStream) owns the
   page, the layout and the zoom; this owns the header, the pick,
   the order and the sheets.
   ───────────────────────────────────────────────────────────── */

import { Suspense, startTransition, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import FeedControl from '@/components/feed/stream/FeedControl';
import { FeedEmptyState } from '@/components/feed/stream/FeedEmptyState';
import FeedManageSheet from '@/components/feed/stream/FeedManageSheet';
import FeedStream, { type FeedStreamBlank, type FeedStreamHandle, type StreamProfileView } from '@/components/feed/stream/FeedStream';
import PostSheet from '@/components/feed/stream/PostSheet';
import { usePageReady } from '@/components/shell/AppShell';
import { HEADER_CONTROL, useTabHeader } from '@/components/shell/TabHeader';
import { normalizeHandle, titleCase } from '@/lib/feedLabels';
import { feederLookup, useFeeds, type AppFeed } from '@/lib/feedsStore';
import { relativeTime, scopeKey, type FeedMode, type FeedOrder, type StreamFeeder, type StreamPost, type StreamScope } from '@/lib/feedStream/contract';
import { useStreamIndex } from '@/lib/feedStream/store';
import { readStoredOrder, writeStoredOrder } from '@/lib/feedStream/zoom';
import { useAppHaptics } from '@/lib/haptics';
import { acquireRootPageScroll } from '@/lib/rootScrollMode';
import { getTabScope, setTabScope, useTabScope, useTabScopeTransition, type TabScope } from '@/lib/tabScope';
import { useCompressedOnScroll } from '@/lib/useCompressedOnScroll';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import { cn } from '@/lib/utils';

const useIsoLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

// the rose story ring: feeders with a post in the last 24h, across every feed
const EVERY_FEED: StreamScope = { feedId: null, handle: null };
const FEED_RESELECT_EVENT = 'feedme:feed-tab-reselect';
const FEED_INTENT_GRACE_MS = 3000;
const COUNT = new Intl.NumberFormat('en-US');

type Pick = { feedId: string | null; handle: string | null };
type ManageState = { open: boolean; intent: 'create-feed' | null };

/* the pick as the stream reads it: a feeder counts inside its feed (Lead's rule), a feed that is gone is all feeds.
   While the feeds load, the pick is taken as it is */
function resolvePick(feeds: AppFeed[] | null, pick: TabScope): Pick {
  const handle = pick.handle ? normalizeHandle(pick.handle) || null : null;
  if (!feeds) return { feedId: pick.feedId ?? null, handle };
  const feedId = pick.feedId
    ?? (handle ? feeds.find((feed) => feed.feeders.some((feeder) => normalizeHandle(feeder.handle) === handle))?.id ?? null : null);
  const live = feedId && feeds.some((feed) => feed.id === feedId) ? feedId : null;
  return { feedId: live, handle: live ? handle : null };
}

function counted(value: number, one: string, many: string) {
  return `${COUNT.format(value)} ${value === 1 ? one : many}`;
}

function lastPostLine(iso: string | null | undefined) {
  if (!iso) return null;
  const when = relativeTime(iso);
  if (when === 'now') return 'last post just now';
  return /^\d/.test(when) ? `last post ${when} ago` : `last post ${when}`;
}

// who the feed is about, for its profile: @northside.coffee / Eat / All feeds, and the line under it
function profileViewOf(
  feeds: AppFeed[] | null,
  scope: Pick,
  lookup: ReadonlyMap<string, StreamFeeder>,
  lastPostAt: string | null,
  fresh: boolean,
): StreamProfileView {
  const feed = scope.feedId && feeds ? feeds.find((candidate) => candidate.id === scope.feedId) ?? null : null;
  const badge = (candidate: AppFeed) => ({ id: candidate.id, title: titleCase(candidate.title), feederCount: candidate.feeders.length });
  const last = lastPostLine(lastPostAt);
  if (scope.handle) {
    const feeder = lookup.get(scope.handle) ?? null;
    const home = feed ? titleCase(feed.title) : feeder?.feedTitle ? titleCase(feeder.feedTitle) : null;
    return {
      title: `@${scope.handle}`,
      subtitle: [home, feeder?.isAnchor ? 'anchor' : null, last].filter(Boolean).join(' · '),
      feeder,
      feeds: feed ? [badge(feed)] : [],
      faces: [],
      fresh,
    };
  }
  if (feed) {
    return {
      title: titleCase(feed.title),
      subtitle: [counted(feed.feeders.length, 'feeder', 'feeders'), last].filter(Boolean).join(' · '),
      feeder: null,
      feeds: [badge(feed)],
      faces: feed.feeders.map((feeder) => ({ handle: normalizeHandle(feeder.handle), profilePicUrl: feeder.profilePicUrl ?? null })),
      fresh,
    };
  }
  const handles = new Set<string>();
  feeds?.forEach((candidate) => candidate.feeders.forEach((feeder) => handles.add(normalizeHandle(feeder.handle))));
  return {
    title: 'All feeds',
    subtitle: [feeds ? counted(feeds.length, 'feed', 'feeds') : null, feeds ? counted(handles.size, 'feeder', 'feeders') : null, last]
      .filter(Boolean)
      .join(' · '),
    feeder: null,
    feeds: (feeds ?? []).map(badge),
    faces: [],
    fresh,
  };
}

/* Reopening the app on Feed right after leaving it from Lead takes you back to Lead (as the old Feed did): the
   bottom nav records where you were and what you tapped */
function useReturnToLastTab() {
  const router = useRouter();
  useEffect(() => {
    try {
      const intent = sessionStorage.getItem('feedme:intent');
      const now = Date.now();
      if (intent === '/') {
        sessionStorage.removeItem('feedme:intent');
        sessionStorage.setItem('feedme:feed-intent-ok-ts', String(now));
        sessionStorage.setItem('feedme:last-tab', '/');
        sessionStorage.setItem('feedme:last-tab-ts', String(now));
        return;
      }
      const intentOkAt = Number(sessionStorage.getItem('feedme:feed-intent-ok-ts') || 0);
      if (now - intentOkAt < FEED_INTENT_GRACE_MS) return;
      const lastTab = sessionStorage.getItem('feedme:last-tab');
      const lastAt = Number(sessionStorage.getItem('feedme:last-tab-ts') || 0);
      const navigation = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined;
      let firstPath = window.location.pathname;
      try {
        if (navigation?.name) firstPath = new URL(navigation.name).pathname;
      } catch {}
      if (firstPath !== '/') return;
      const recent = (lastTab === '/lead' || lastTab === '/fire') && now - lastAt < 120000;
      const cameFromLead = ['/lead', '/fire'].some((path) => (document.referrer || '').includes(path));
      if (recent && (navigation?.type === 'reload' || cameFromLead)) router.replace('/lead');
    } catch {}
  }, [router]);
}

function StreamError({ onRetry }: { onRetry: () => void }) {
  return (
    <div className="flex flex-col items-center gap-4 text-center">
      <p className="text-[13px] font-semibold leading-snug text-[var(--st-text-2)]">The feed didn&apos;t load.</p>
      <button
        type="button"
        onClick={onRetry}
        className={cn(HEADER_CONTROL, 'px-5 text-[12px] tracking-[0.14em] transition-transform duration-150 ease-out active:scale-[0.96]')}
      >
        Try again
      </button>
    </div>
  );
}

function FeedTabContent() {
  const searchParams = useSearchParams();
  const linkedFeedId = searchParams.get('id');
  const { feeds } = useFeeds();
  const lookup = feederLookup(feeds);
  const streamRef = useRef<FeedStreamHandle | null>(null);
  const { play } = useAppHaptics();
  const { appShellStyle, isStandaloneMode, useBrowserPageScroll } = useMobileImmersiveViewport();
  // the header folds on the page's own scroll (as on Lead and Read)
  const noScroller = useRef<HTMLElement | null>(null);
  const compressed = useCompressedOnScroll(noScroller, true, { collapseDistance: 150, expandDistance: 64, topGuard: 30 });

  const [order, setOrder] = useState<FeedOrder>(() => readStoredOrder() ?? 'posted');
  const [mode, setMode] = useState<FeedMode | null>(null);
  const [sheetPost, setSheetPost] = useState<StreamPost | null>(null);
  const [manage, setManage] = useState<ManageState>({ open: false, intent: null });

  // the pick, followed in a transition (the rail answers the tap first), with the order
  const pick = useTabScopeTransition();
  const picked = resolvePick(feeds, pick);
  const pickScope = useMemo<StreamScope>(() => ({ feedId: picked.feedId, handle: picked.handle, order }), [picked.feedId, picked.handle, order]);
  const pickKey = scopeKey(pickScope);
  // what the stream shows: the pick, once what was on screen has let go
  const [shownScope, setShownScope] = useState<StreamScope>(pickScope);
  const shownKey = scopeKey(shownScope);

  const shown = useStreamIndex(shownScope);
  // the next scope's index loads while the screen lets go
  useStreamIndex(pickScope);
  const everyFeed = useStreamIndex(EVERY_FEED);

  usePageReady(true);
  useReturnToLastTab();

  // the page scrolls at every width (the stream takes its lease before restoring your place). As on Read, this is
  // re-applied whenever the shared viewport hook switches modes, because its own page-scroll lease resets the body's
  // overflow (an overflowing body becomes a scroll container of its own, and the page's snap stops seeing the cards)
  useEffect(() => {
    const release = acquireRootPageScroll();
    const body = document.body;
    const previous = body.style.overflow;
    body.style.overflow = 'visible';
    return () => {
      body.style.overflow = previous;
      release();
    };
  }, [useBrowserPageScroll]);

  // what the handlers read
  const feedsRef = useRef(feeds);
  const orderRef = useRef(order);
  const shownRef = useRef(shownScope);
  const refreshRef = useRef(shown.refresh);
  const playRef = useRef(play);
  useIsoLayoutEffect(() => {
    feedsRef.current = feeds;
    orderRef.current = order;
    shownRef.current = shownScope;
    refreshRef.current = shown.refresh;
    playRef.current = play;
  });

  // the pick as it is right now (not the page's copy, which may still be catching up)
  const livePickScope = useCallback((): StreamScope => {
    const live = resolvePick(feedsRef.current, getTabScope());
    return { feedId: live.feedId, handle: live.handle, order: orderRef.current };
  }, []);

  // a pick made on another tab: caught up before the first paint, with nothing to let go
  useIsoLayoutEffect(() => {
    const live = livePickScope();
    if (scopeKey(live) !== scopeKey(shownRef.current)) setShownScope(live);
  }, [livePickScope]);

  // a pick (or an order) on this tab: its posts near the day you're on are made ready while the screen holds still,
  // then the stream moves to it (the posts both picks hold carry to their new places, the rest are replaced in place;
  // the profile too, when it is someone else's)
  useEffect(() => {
    const live = livePickScope();
    if (scopeKey(live) === shownKey) return undefined;
    const someoneElse = live.feedId !== shownRef.current.feedId || live.handle !== shownRef.current.handle;
    let cancelled = false;
    const apply = () => setShownScope(livePickScope());
    const stream = streamRef.current;
    if (!stream) {
      startTransition(apply);
      return undefined;
    }
    void stream.prepare(live).then(() => {
      if (!cancelled) stream.commit(apply, { profile: someoneElse });
    });
    return () => {
      cancelled = true;
    };
  }, [livePickScope, pickKey, shownKey]);

  // the old link to a feed (/?id=…): open it on the rail, then drop it from the address
  useIsoLayoutEffect(() => {
    if (!linkedFeedId || window.location.pathname !== '/') return;
    setTabScope({ feedId: linkedFeedId, handle: null });
    const linked = livePickScope();
    if (scopeKey(linked) !== scopeKey(shownRef.current)) setShownScope(linked);
    const url = new URL(window.location.href);
    url.searchParams.delete('id');
    window.history.replaceState(window.history.state, '', `${url.pathname}${url.search}${url.hash}`);
  }, [linkedFeedId, livePickScope]);

  // a re-tap on Feed in the nav: back to the newest post
  useEffect(() => {
    const onReselect = () => streamRef.current?.scrollToNewest();
    window.addEventListener(FEED_RESELECT_EVENT, onReselect);
    return () => window.removeEventListener(FEED_RESELECT_EVENT, onReselect);
  }, []);

  useEffect(() => {
    document.title = 'Feed | FeedMe';
  }, []);

  /* ── the header ── */

  const live = resolvePick(feeds, useTabScope());
  const liveFeed = feeds && live.feedId ? feeds.find((feed) => feed.id === live.feedId) ?? null : null;
  const label = live.handle ? `@${live.handle}` : liveFeed ? titleCase(liveFeed.title) : 'All feeds';
  const railFeeds = useMemo(() => (feeds ?? []).map((feed) => ({
    id: feed.id,
    title: feed.title,
    feeders: feed.feeders.map((feeder) => ({ handle: normalizeHandle(feeder.handle), profilePicUrl: feeder.profilePicUrl ?? null })),
  })), [feeds]);
  const freshHandles = everyFeed.index?.freshHandles;
  const fresh = useMemo(() => new Set((freshHandles ?? []).map((handle) => normalizeHandle(handle))), [freshHandles]);
  const marked = useCallback((handle: string) => fresh.has(handle), [fresh]);

  // Lead's rail rules: the open feed's badge goes back to the whole feed, a picked feeder tapped again lets go
  const pickFeed = useCallback((id: string | null) => {
    if (id === resolvePick(feedsRef.current, getTabScope()).feedId) return;
    setTabScope({ feedId: id, handle: null });
  }, []);
  const pickFeeder = useCallback((handle: string | null) => {
    const current = resolvePick(feedsRef.current, getTabScope());
    setTabScope({ feedId: current.feedId, handle: handle || null });
  }, []);

  const changeMode = useCallback((next: FeedMode) => streamRef.current?.setMode(next), []);
  // a zoom or a pick about to be taken: the stream gets its posts and first pictures ready
  const modeIntent = useCallback((next: FeedMode) => streamRef.current?.warm({ mode: next }), []);
  const pickIntent = useCallback((pick: { feedId: string | null; handle: string | null }) => {
    const current = resolvePick(feedsRef.current, getTabScope());
    // a picked feeder tapped again lets go of it (Lead's rail rules)
    const handle = pick.handle && pick.handle === current.handle ? null : pick.handle;
    const scope: StreamScope = { feedId: pick.feedId, handle, order: orderRef.current };
    if (scopeKey(scope) === scopeKey(shownRef.current)) return;
    streamRef.current?.warm({ scope });
  }, []);

  useTabHeader('feed', {
    title: 'FEED',
    eyebrow: order === 'results' ? 'Feed · latest results' : 'Feed · newest posts',
    label,
    control: <FeedControl mode={mode ?? 'today'} onModeChange={changeMode} onModeIntent={modeIntent} />,
    compressed,
    // none yet means still loading (the header keeps the rail it has)
    feeds: railFeeds.length ? railFeeds : null,
    feedId: liveFeed?.id ?? null,
    handle: live.handle,
    marked,
    onAll: () => pickFeed(null),
    onFeed: (id) => (id === liveFeed?.id ? pickFeeder(null) : pickFeed(id)),
    onFeeder: (handle) => pickFeeder(live.handle === handle ? null : handle),
    onIntent: pickIntent,
  });

  /* ── the order, the sheets, the other tabs ── */

  const changeOrder = useCallback((next: FeedOrder) => {
    if (next === orderRef.current) return;
    orderRef.current = next;
    writeStoredOrder(next);
    setOrder(next);
  }, []);
  const openPost = useCallback((post: StreamPost) => setSheetPost(post), []);
  const closePost = useCallback(() => setSheetPost(null), []);
  const openManage = useCallback(() => setManage({ open: true, intent: null }), []);
  const createFeed = useCallback(() => setManage({ open: true, intent: 'create-feed' }), []);
  const closeManage = useCallback(() => setManage((current) => ({ ...current, open: false })), []);
  const retry = useCallback(() => refreshRef.current(), []);
  // Lead and Read, the way the nav moves between kept-alive tabs: the address changes in place (the pick is shared)
  const goTab = useCallback((href: '/lead' | '/read') => {
    playRef.current('navSwitch');
    try {
      sessionStorage.setItem('feedme:intent', href);
      sessionStorage.setItem('feedme:intent-ts', String(Date.now()));
    } catch {}
    window.history.pushState(null, '', href);
  }, []);
  const openLead = useCallback(() => goTab('/lead'), [goTab]);
  const openRead = useCallback(() => goTab('/read'), [goTab]);

  /* ── the stream ── */

  const index = shown.index;
  const lastPostAt = index?.profile?.lastPostAt ?? null;
  const shownFresh = (index?.freshHandles.length ?? 0) > 0;
  const profile = useMemo(
    () => profileViewOf(feeds, { feedId: shownScope.feedId, handle: shownScope.handle }, lookup, lastPostAt, shownFresh),
    [feeds, lastPostAt, lookup, shownFresh, shownScope.feedId, shownScope.handle],
  );
  const noFeeds = feeds !== null && feeds.length === 0;
  const noPosts = !noFeeds && index?.total === 0;
  const failed = !noFeeds && !index && shown.status === 'error';
  const blank = useMemo<FeedStreamBlank | null>(() => {
    if (noFeeds) return { node: <FeedEmptyState kind="no-feeds" onCreateFeed={createFeed} />, profile: false };
    if (noPosts) return { node: <FeedEmptyState kind="no-posts" onCreateFeed={createFeed} />, profile: true };
    if (failed) return { node: <StreamError onRetry={retry} />, profile: false };
    return null;
  }, [createFeed, failed, noFeeds, noPosts, retry]);

  return (
    <div data-feed-tab="" className="relative w-full">
      <FeedStream
        ref={streamRef}
        scope={shownScope}
        index={index}
        feeders={lookup}
        profile={profile}
        order={order}
        standalone={isStandaloneMode}
        minHeight={String(appShellStyle.minHeight)}
        headerCompressed={compressed}
        blank={blank}
        keyboard={!sheetPost && !manage.open}
        onModeChange={setMode}
        onOrderChange={changeOrder}
        onOpenPost={openPost}
        onManage={openManage}
        onLead={openLead}
        onRead={openRead}
      />
      <PostSheet post={sheetPost} feeder={sheetPost ? lookup.get(sheetPost.handle) ?? null : null} onClose={closePost} />
      <FeedManageSheet open={manage.open} scope={pickScope} intent={manage.intent} onClose={closeManage} />
    </div>
  );
}

export default function FeedTab() {
  return (
    <Suspense fallback={<div className="min-h-[100dvh] w-full bg-[var(--fm-page)]" />}>
      <FeedTabContent />
    </Suspense>
  );
}
