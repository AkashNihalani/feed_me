'use client';

import { startTransition, useCallback, useEffect, useLayoutEffect, useState, useMemo, Suspense, useRef } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import { motion, AnimatePresence } from 'framer-motion';
import { ChevronLeft, Clock, FileText, Plus, Target, Users, X } from 'lucide-react';
import FeedTile from '@/components/feed/FeedTile';
import FeederFileTile from '@/components/feed/FeederFileTile';
import FeederRow from '@/components/feed/FeederRow';
import ScanningCard from '@/components/feed/ScanningCard';
import FeedDetailV2 from '@/components/feed/FeedDetailV2';
import FlipTicker, { TickerItem } from '@/components/feed/FlipTicker';
import { DashboardPayload, TIMEFRAME_TO_DAYS, Timeframe } from '@/components/feed/dashboardTypes';
import FeedBriefDialog from '@/components/feed/FeedBriefDialog';
import { FeedBrief, CREATE_FEED_LAST_STEP, defaultFeedBrief, buildFeedBriefText, normalizeFeedBrief } from '@/components/feed/feedBriefUtils';
import { cn } from '@/lib/utils';
import { useAppHaptics } from '@/lib/haptics';
import { GRID_ITEM_EASE, GRID_LAYOUT_SPRING, HEADER_STAGGER_CONTAINER, HEADER_ROW } from '@/lib/motion';
import { clearCacheByPrefix, getCache, readCache, removeCache, setCache } from '@/lib/pageCache';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import { useCompressedOnScroll } from '@/lib/useCompressedOnScroll';

/* ── Types ── */
type Metrics = { likes: string; comments: string; views: string; postsTracked: string };
type FeedMetrics = Metrics & { percentile?: number | string | null };
type Feeder = {
  handle: string; isAnchor: boolean; profilePicUrl?: string | null;
  followerCount?: number | null; metrics: Metrics;
};
type Feed = {
  id: string;
  title: string;
  feeders: Feeder[];
  metrics: FeedMetrics;
  compositePercentile?: number | string | null;
  feedBrief?: Partial<FeedBrief> | Record<string, unknown>;
  feedBriefText?: string;
  contextBrief?: Partial<FeedBrief> | Record<string, unknown>;
  contextBible?: string;
};
type SortMode = 'recent' | 'name' | 'feeders';
type SlotUsage = {
  used: number;
  handles?: string[];
  limit?: number | null;
  plan?: { price: number; postsCap: number; packPrice: number; packSize: number };
};

const INITIAL_FEEDS: Feed[] = [];
const APPLE_EASE = [0.32, 0.72, 0, 1] as const;
const FEED_HEADER_MORPH_TRANSITION = { duration: 0.62, ease: APPLE_EASE } as const;
const FEED_HEADER_HEIGHT_SETTLE_MS = 740;
const FEED_CACHE_KEY = 'feed:bundle:v7';
const DASHBOARD_CACHE_PREFIX = 'feed:dashboard:v12';
const DASHBOARD_CACHE_TTL = 10 * 60 * 1000;
const dashboardInflight = new Map<string, Promise<DashboardPayload | null>>();
const FEED_TILE_ENTRY_BASE_DELAY = 0.08;
const FEED_TILE_ENTRY_STAGGER = 0.055;
const FEED_TILE_ENTRY_MAX_STAGGER = 0.28;
const FEED_INTENT_GRACE_MS = 3000;
const useIsomorphicLayoutEffect = typeof window === 'undefined' ? useEffect : useLayoutEffect;
const TIMEFRAME_LABELS: Record<Timeframe, string> = {
  '7D': '7D',
  '30D': '30D',
  '60D': '60D',
  '90D': '90D',
};

const pageVariants = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: { duration: 0.42, ease: APPLE_EASE },
  },
};

function istDateKey(date: Date): string {
  const parts = new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).formatToParts(date);
  const values = new Map(parts.map((part) => [part.type, part.value]));
  return `${values.get('year')}-${values.get('month')}-${values.get('day')}`;
}

function utcDateKey(date: Date): string {
  const year = date.getUTCFullYear();
  const month = String(date.getUTCMonth() + 1).padStart(2, '0');
  const day = String(date.getUTCDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function shiftIsoDate(isoDate: string, days: number): string {
  const [year, month, day] = isoDate.split('-').map(Number);
  const date = new Date(Date.UTC(year, month - 1, day));
  date.setUTCDate(date.getUTCDate() + days);
  return utcDateKey(date);
}

function defaultExportRange() {
  const to = istDateKey(new Date());
  return { from: shiftIsoDate(to, -89), to };
}



function dashboardCacheKey(feedId: string, timeframe: Timeframe, handle: string) {
  return `${DASHBOARD_CACHE_PREFIX}:${feedId}:${TIMEFRAME_TO_DAYS[timeframe]}:${handle}`;
}

function isUnauthorizedError(error: unknown): boolean {
  return error instanceof Error && error.message.toLowerCase() === 'unauthorized';
}

function parseMetricNumber(value: string | number | null | undefined): number | null {
  if (value == null) return null;
  if (typeof value === 'number') return Number.isFinite(value) ? value : null;
  const cleaned = value.replace(/[,\s]/g, '').toLowerCase();
  if (!cleaned || cleaned === '--') return null;
  const multiplier = cleaned.endsWith('m') ? 1_000_000 : cleaned.endsWith('k') ? 1_000 : 1;
  const numeric = Number.parseFloat(cleaned.replace(/[km]$/, ''));
  return Number.isFinite(numeric) ? numeric * multiplier : null;
}

function dashboardFallbackFromFeed(feed: Feed | null | undefined, timeframe: Timeframe): DashboardPayload | null {
  if (!feed) return null;
  const windowEnd = istDateKey(new Date());
  const windowStart = shiftIsoDate(windowEnd, -(TIMEFRAME_TO_DAYS[timeframe] - 1));
  const avgPercentileRaw = Number(feed.metrics?.percentile ?? feed.compositePercentile);
  const avgPercentile = Number.isFinite(avgPercentileRaw) && avgPercentileRaw > 0 ? avgPercentileRaw : null;
  const postCount = Math.max(0, Math.round(parseMetricNumber(feed.metrics?.postsTracked) ?? 0));
  const avgLikes = parseMetricNumber(feed.metrics?.likes);
  const avgComments = parseMetricNumber(feed.metrics?.comments);
  const avgViews = parseMetricNumber(feed.metrics?.views);

  return {
    ascent_series: [],
    frequency_series: [],
    heatmap_daily: [],
    killzone_hours: [],
    killzone_days: [],
    apex_mix: [],
    scatter_points: [],
    engagement_averages: [
      {
        media_type: 'all',
        post_count: postCount,
        metric_count: postCount,
        avg_likes: avgLikes,
        avg_comments: avgComments,
        avg_views: avgViews,
      },
    ],
    summary: {
      window_days: TIMEFRAME_TO_DAYS[timeframe],
      window_start_ist: windowStart,
      window_end_ist: windowEnd,
      post_count: postCount,
      posts_with_metrics: postCount,
      avg_percentile_performance: avgPercentile,
      avg_views_percentile: null,
      avg_likes_percentile: null,
      avg_comments_percentile: null,
    },
    pattern_board: [],
  };
}

async function fetchDashboardSnapshot(feedId: string, timeframe: Timeframe, handle: string): Promise<DashboardPayload | null> {
  const cacheKey = dashboardCacheKey(feedId, timeframe, handle);
  const cached = getCache<DashboardPayload>(cacheKey, DASHBOARD_CACHE_TTL);
  if (cached) return cached;

  const existing = dashboardInflight.get(cacheKey);
  if (existing) return existing;

  const params = new URLSearchParams({ feedId, days: String(TIMEFRAME_TO_DAYS[timeframe]) });
  if (handle !== 'all') params.set('handle', handle);

  const request = fetch(`/api/feed/dashboard?${params}`)
    .then(async (res) => {
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || 'Failed');
      const next = (json.dashboard || null) as DashboardPayload | null;
      if (next) setCache(cacheKey, next);
      return next;
    })
    .finally(() => {
      dashboardInflight.delete(cacheKey);
    });

  dashboardInflight.set(cacheKey, request);
  return request;
}

function FeedPageContent() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const { play } = useAppHaptics();
  const { appShellStyle, isStandaloneMode, useBrowserPageScroll, useTranslucentBrowserChrome } = useMobileImmersiveViewport();
  const headerRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);
  const hydratedFeedCacheRef = useRef(false);
  const urlSelectedFeedId = searchParams.get('id');
  const shouldAnimateFeedTilesRef = useRef(true);
  const mobileBottomClearance = useTranslucentBrowserChrome
    ? '0px'
    : 'calc(120px + env(safe-area-inset-bottom))';
  const mobileListBottomClearance = useTranslucentBrowserChrome
    ? '0px'
    : 'calc(190px + env(safe-area-inset-bottom))';

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
      const feedIntentOkTs = Number(sessionStorage.getItem('feedme:feed-intent-ok-ts') || 0);
      if (now - feedIntentOkTs < FEED_INTENT_GRACE_MS) return;
      const lastTab = sessionStorage.getItem('feedme:last-tab');
      const lastTs = Number(sessionStorage.getItem('feedme:last-tab-ts') || 0);
      const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined;
      const initialDocumentPath = (() => {
        try {
          return nav?.name ? new URL(nav.name).pathname : window.location.pathname;
        } catch {
          return window.location.pathname;
        }
      })();
      if (initialDocumentPath !== '/') return;
      const recent = lastTab === '/fire' && now - lastTs < 120000;
      const cameFromFire = (document.referrer || '').includes('/fire');
      if (recent && (nav?.type === 'reload' || cameFromFire)) router.replace('/fire');
    } catch {}
  }, [router]);

  const [selectedFeedId, setSelectedFeedId] = useState<string | null>(urlSelectedFeedId);
  const view = selectedFeedId ? 'detail' : 'list';

  const [feeds, setFeeds] = useState<Feed[]>(INITIAL_FEEDS);
  const [feedDataReady, setFeedDataReady] = useState(false);
  const [isAddingFeeder, setIsAddingFeeder] = useState(false);
  const [addingFeeder, setAddingFeeder] = useState<string | null>(null);
  const [isCreatingFeed, setIsCreatingFeed] = useState(false);
  const [pendingDeleteFeedId, setPendingDeleteFeedId] = useState<string | null>(null);
  const [newFeedName, setNewFeedName] = useState('');
  const [newFeedBrief, setNewFeedBrief] = useState<FeedBrief>(() => defaultFeedBrief());
  const [createFeedStep, setCreateFeedStep] = useState(0);
  const [newFeedBibleDraft, setNewFeedBibleDraft] = useState('');
  const [isEditingFeedContext, setIsEditingFeedContext] = useState(false);
  const [editFeedBrief, setEditFeedBrief] = useState<FeedBrief>(() => defaultFeedBrief());
  const [editFeedStep, setEditFeedStep] = useState(0);
  const [editFeedBibleDraft, setEditFeedBibleDraft] = useState('');
  const [isBusy, setIsBusy] = useState(false);
  const [apiError, setApiError] = useState<string | null>(null);
  const [timeframe, setTimeframe] = useState<Timeframe>('30D');
  const [selectedHandle, setSelectedHandle] = useState<string>('all');
  const [dashboardData, setDashboardData] = useState<DashboardPayload | null>(
    () => (urlSelectedFeedId ? readCache<DashboardPayload>(dashboardCacheKey(urlSelectedFeedId, '30D', 'all'))?.data ?? null : null),
  );
  const [baselineDashboardData, setBaselineDashboardData] = useState<DashboardPayload | null>(
    () => (urlSelectedFeedId ? readCache<DashboardPayload>(dashboardCacheKey(urlSelectedFeedId, '30D', 'all'))?.data ?? null : null),
  );
  const [tickerItems, setTickerItems] = useState<TickerItem[]>([]);
  const [sortMode, setSortMode] = useState<SortMode>('recent');
  const [slotUsage, setSlotUsage] = useState<SlotUsage>({ used: 0 });
  const [exportFrom, setExportFrom] = useState<string>(() => defaultExportRange().from);
  const [exportTo, setExportTo] = useState<string>(() => defaultExportRange().to);
  const [headerHeight, setHeaderHeight] = useState(() => (urlSelectedFeedId ? 204 : 176));
  const headerCompressed = useCompressedOnScroll(
    contentRef,
    view === 'detail' && useBrowserPageScroll,
    { collapseDistance: 220, expandDistance: 120, topGuard: 54 },
  );
  const headerCompressedRef = useRef(headerCompressed);
  useEffect(() => { headerCompressedRef.current = headerCompressed; }, [headerCompressed]);

  const activeFeed = feeds.find(f => f.id === selectedFeedId);
  const dashboardFallback = useMemo(
    () => dashboardFallbackFromFeed(activeFeed, timeframe),
    [activeFeed, timeframe],
  );
  const effectiveDashboardData = dashboardData ?? dashboardFallback;
  const effectiveBaselineDashboardData = baselineDashboardData ?? effectiveDashboardData;
  const useFeedRootSnap = view === 'detail' && useBrowserPageScroll && isStandaloneMode;
  const sortedFeeds = useMemo(() => {
    const list = [...feeds];
    if (sortMode === 'name') return list.sort((a, b) => a.title.localeCompare(b.title));
    if (sortMode === 'feeders') return list.sort((a, b) => b.feeders.length - a.feeders.length || a.title.localeCompare(b.title));
    return list;
  }, [feeds, sortMode]);
  const handles = useMemo<string[]>(() => {
    if (!activeFeed) return [];
    return (activeFeed.feeders || []).map((f) => String(f.handle || '').trim()).filter(Boolean);
  }, [activeFeed]);
  const shortTitle = String(activeFeed?.title || 'Feed').toUpperCase();
  const compactPrimaryLabel = selectedHandle === 'all' ? shortTitle : `@${selectedHandle.toUpperCase()}`;
  const compactSecondaryLabel = selectedHandle === 'all' ? 'FULL FEED' : shortTitle;
  const compactWindowNumber = String(TIMEFRAME_TO_DAYS[timeframe]);
  const compactWindowUnitLabel = TIMEFRAME_TO_DAYS[timeframe] === 1 ? 'DAY' : 'DAYS';
  const mobileDashboardFilterRowHeight = handles.length > 0 ? '76px' : '42px';
  const pendingDeleteFeed = useMemo(
    () => (pendingDeleteFeedId ? feeds.find((feed) => feed.id === pendingDeleteFeedId) ?? null : null),
    [feeds, pendingDeleteFeedId]
  );
  const newFeedBriefPreview = useMemo(
    () => buildFeedBriefText(newFeedBrief, newFeedName.trim() || 'this feed'),
    [newFeedBrief, newFeedName],
  );
  const editFeedBriefPreview = useMemo(
    () => buildFeedBriefText(editFeedBrief, activeFeed?.title || 'this feed'),
    [activeFeed?.title, editFeedBrief],
  );

  const topAveragePercentile = useMemo(() => {
    const raw = Number(effectiveDashboardData?.summary?.avg_percentile_performance);
    if (!Number.isFinite(raw) || raw <= 0) return null;
    return Math.max(1, Math.min(100, Math.round(raw)));
  }, [effectiveDashboardData]);
  const topAverageLabel = topAveragePercentile == null ? '--' : `Top ${topAveragePercentile}%`;
  const topAveragePosts = Math.max(0, Number(effectiveDashboardData?.summary?.posts_with_metrics) || 0);
  const exportScopeLabel = selectedHandle === 'all' ? 'FULL FEED' : `@${selectedHandle.toUpperCase()}`;

  useIsomorphicLayoutEffect(() => {
    if (hydratedFeedCacheRef.current) return;
    hydratedFeedCacheRef.current = true;

    const cachedEntry = readCache<{ feeds: Feed[]; ticker: TickerItem[]; slots?: SlotUsage }>(FEED_CACHE_KEY);
    const cached = cachedEntry?.data ?? null;
    if (!cached) return;

    setFeeds(cached.feeds ?? INITIAL_FEEDS);
    setTickerItems(cached.ticker ?? []);
    setSlotUsage(cached.slots ?? { used: 0 });
    setFeedDataReady(true);
  }, []);

  useEffect(() => {
    setSelectedFeedId(urlSelectedFeedId);
  }, [urlSelectedFeedId]);

  /* ── Data loading ── */
  const loadFeeds = useCallback(async () => {
    const cachedEntry = readCache<{ feeds: Feed[]; ticker: TickerItem[]; slots?: SlotUsage }>(FEED_CACHE_KEY);
    const cached = cachedEntry?.data ?? null;

    if (cached) {
      setFeeds(cached.feeds ?? INITIAL_FEEDS);
      setTickerItems(cached.ticker ?? []);
      setSlotUsage(cached.slots ?? { used: 0 });
      setFeedDataReady(true);
    }

    try {
      const res = await fetch('/api/feed');
      const json = await res.json();
      if (!res.ok) {
        if (res.status === 401) {
          removeCache(FEED_CACHE_KEY);
          clearCacheByPrefix(DASHBOARD_CACHE_PREFIX);
          setFeeds(INITIAL_FEEDS);
          setTickerItems([]);
          setSlotUsage({ used: 0 });
          setFeedDataReady(true);
          router.replace(`/login?next=${encodeURIComponent(`${window.location.pathname}${window.location.search}`)}`);
          throw new Error('Unauthorized');
        }
        throw new Error(json.error || 'Failed to load feeds');
      }
      const nextFeeds = (json.feeds || []) as Feed[];
      const nextTicker = (json.ticker || []) as TickerItem[];
      const nextSlots = (json.slots || null) as SlotUsage | null;
      setFeeds(nextFeeds);
      setTickerItems(nextTicker);
      if (nextSlots) setSlotUsage(nextSlots);
      setCache(FEED_CACHE_KEY, { feeds: nextFeeds, ticker: nextTicker, slots: nextSlots || undefined });
      setFeedDataReady(true);
    } catch (err) {
      setFeedDataReady(true);
      throw err;
    }
  }, [router]);

  useEffect(() => {
    loadFeeds().catch((err) => {
      if (isUnauthorizedError(err)) return;
      setApiError(err instanceof Error ? err.message : 'Failed to load feeds');
    });
  }, [loadFeeds]);
  useEffect(() => { setSelectedHandle('all'); setTimeframe('30D'); }, [selectedFeedId]);

  useEffect(() => {
    const node = headerRef.current;
    if (!node) return undefined;

    let raf = 0;
    let settleTimer = 0;
    const commitHeight = () => {
      if (headerCompressedRef.current) return;
      const nextHeight = Math.ceil(node.getBoundingClientRect().height);
      setHeaderHeight((current) => (Math.abs(current - nextHeight) > 1 ? nextHeight : current));
    };
    const commitInFrame = () => {
      if (raf) window.cancelAnimationFrame(raf);
      raf = window.requestAnimationFrame(() => {
        raf = 0;
        commitHeight();
      });
    };
    const scheduleSettledHeight = () => {
      window.clearTimeout(settleTimer);
      settleTimer = window.setTimeout(commitInFrame, FEED_HEADER_HEIGHT_SETTLE_MS);
    };

    commitInFrame();
    scheduleSettledHeight();
    window.addEventListener('resize', scheduleSettledHeight);

    return () => {
      if (raf) window.cancelAnimationFrame(raf);
      window.clearTimeout(settleTimer);
      window.removeEventListener('resize', scheduleSettledHeight);
    };
  }, [view, activeFeed?.id, handles.length, timeframe, selectedHandle, useBrowserPageScroll]);

  useEffect(() => {
    if (headerCompressed) return undefined;
    const node = headerRef.current;
    if (!node) return undefined;

    const settleTimer = window.setTimeout(() => {
      if (headerCompressedRef.current) return;
      const nextHeight = Math.ceil(node.getBoundingClientRect().height);
      setHeaderHeight((current) => (Math.abs(current - nextHeight) > 1 ? nextHeight : current));
    }, FEED_HEADER_HEIGHT_SETTLE_MS);

    return () => window.clearTimeout(settleTimer);
  }, [headerCompressed]);

  useEffect(() => {
    const html = document.documentElement;
    html.classList.toggle('fm-feed-root-scroll', useFeedRootSnap);
    return () => {
      html.classList.remove('fm-feed-root-scroll');
    };
  }, [useFeedRootSnap]);

  useEffect(() => {
    const html = document.documentElement;
    if (!useFeedRootSnap) {
      html.style.removeProperty('--fm-feed-header-height');
      html.style.removeProperty('--fm-feed-bottom-clearance');
      return undefined;
    }

    html.style.setProperty('--fm-feed-header-height', `${headerHeight}px`);
    html.style.setProperty('--fm-feed-bottom-clearance', isStandaloneMode ? '120px' : '18px');

    return () => {
      html.style.removeProperty('--fm-feed-header-height');
      html.style.removeProperty('--fm-feed-bottom-clearance');
    };
  }, [headerHeight, isStandaloneMode, useFeedRootSnap]);

  useEffect(() => {
    if (!activeFeed) { setDashboardData(null); setBaselineDashboardData(null); return; }
    let cancelled = false;
    const feedId = String(activeFeed.id);
    const cacheKey = dashboardCacheKey(feedId, timeframe, selectedHandle);
    const baselineCacheKey = dashboardCacheKey(feedId, timeframe, 'all');

    const cachedEntry = readCache<DashboardPayload>(cacheKey);
    const hadCache = Boolean(cachedEntry?.data);
    const hasFreshCache = Boolean(cachedEntry && Date.now() - cachedEntry.ts <= DASHBOARD_CACHE_TTL);

    if (cachedEntry?.data) {
      setDashboardData(cachedEntry.data);
      if (selectedHandle === 'all') setBaselineDashboardData(cachedEntry.data);
      setApiError((current) => current && current.toLowerCase().includes('dashboard') ? null : current);
    }

    if (selectedHandle !== 'all') {
      const baselineCached = readCache<DashboardPayload>(baselineCacheKey);
      if (baselineCached?.data) {
        setBaselineDashboardData(baselineCached.data);
      }
      const baselineFresh = Boolean(
        baselineCached && Date.now() - baselineCached.ts <= DASHBOARD_CACHE_TTL,
      );
      if (!baselineFresh) {
        fetchDashboardSnapshot(feedId, timeframe, 'all')
          .then((next) => {
            if (cancelled) return;
            if (next) setBaselineDashboardData(next);
          })
          .catch(() => {});
      }
    }

    if (hasFreshCache) {
      return () => {
        cancelled = true;
      };
    }

    fetchDashboardSnapshot(feedId, timeframe, selectedHandle)
      .then((next) => {
        if (cancelled) return;
        setDashboardData(next);
        if (selectedHandle === 'all' && next) setBaselineDashboardData(next);
        setApiError((current) => current && current.toLowerCase().includes('dashboard') ? null : current);
      })
      .catch((err: unknown) => {
        if (!cancelled && !hadCache) {
          const baselineCached = readCache<DashboardPayload>(baselineCacheKey)?.data ?? null;
          if (baselineCached) setDashboardData(baselineCached);
          setApiError((current) => current && current.toLowerCase().includes('dashboard') ? null : current);
          if (process.env.NODE_ENV !== 'production') {
            console.warn('Dashboard snapshot refresh failed', err);
          }
        }
      });
    return () => {
      cancelled = true;
    };
  }, [activeFeed, timeframe, selectedHandle]);

  const preloadDashboard = useCallback((feedId: string, tf: Timeframe = '30D', handle: string = 'all') => {
    fetchDashboardSnapshot(feedId, tf, handle).catch(() => {});
  }, []);

  /* ── Actions ── */
  const runFeedAction = async (payload: Record<string, unknown>) => {
    setIsBusy(true); setApiError(null);
    try {
      const res = await fetch('/api/feed', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) });
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || 'Action failed');
      const nextFeeds = (json.feeds || []) as Feed[];
      const nextTicker = (json.ticker || []) as TickerItem[];
      const nextSlots = (json.slots || null) as SlotUsage | null;
      setFeeds(nextFeeds);
      setTickerItems(nextTicker);
      if (nextSlots) setSlotUsage(nextSlots);
      setCache(FEED_CACHE_KEY, { feeds: nextFeeds, ticker: nextTicker, slots: nextSlots || undefined });
      return true;
    } catch (err) { setApiError(err instanceof Error ? err.message : 'Action failed'); return false; }
    finally { setIsBusy(false); }
  };

  const handleFeedClick = (id: string) => {
    setApiError(null);
    setSelectedHandle('all');
    setTimeframe('30D');
    setSelectedFeedId(id);
    const cached = readCache<DashboardPayload>(dashboardCacheKey(id, '30D', 'all'))?.data ?? null;
    setDashboardData(cached);
    preloadDashboard(id, '30D', 'all');
    startTransition(() => {
      router.push(`/?id=${id}`, { scroll: false });
    });
  };
  const handleBack = () => {
    setSelectedFeedId(null);
    startTransition(() => {
      router.push('/', { scroll: false });
    });
  };
  const handleDownloadExport = () => {
    if (!activeFeed) return;
    if (exportFrom && exportTo && exportFrom > exportTo) {
      setApiError('Export start date must be before end date');
      return;
    }
    setApiError(null);
    const params = new URLSearchParams();
    if (selectedHandle !== 'all') params.set('handle', selectedHandle);
    if (exportFrom) params.set('from', exportFrom);
    if (exportTo) params.set('to', exportTo);
    const query = params.toString();
    window.location.assign(`/api/download/${activeFeed.id}${query ? `?${query}` : ''}`);
  };
  const handleMakeAnchor = async (feedId: string, handle: string) => runFeedAction({ action: 'make_anchor', feedId: Number(feedId), handle });
  const handleAddFeeder = async (feedId: string, handle: string) => {
    const clean = (handle || '').trim().replace(/^@+/, '').toLowerCase();
    if (!clean) return;
    setAddingFeeder(clean); setApiError(null); setIsBusy(true);
    const ctrl = new AbortController(); const tid = setTimeout(() => ctrl.abort(), 30000);
    try {
      const res = await fetch('/api/feed', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ action: 'add_feeder', feedId: Number(feedId), handle: clean }), signal: ctrl.signal });
      const json = await res.json(); if (!res.ok) throw new Error(json.error || 'Unable to add feeder');
      const nextFeeds = (json.feeds || []) as Feed[];
      const nextTicker = (json.ticker || []) as TickerItem[];
      const nextSlots = (json.slots || null) as SlotUsage | null;
      setFeeds(nextFeeds);
      setTickerItems(nextTicker);
      if (nextSlots) setSlotUsage(nextSlots);
      setCache(FEED_CACHE_KEY, { feeds: nextFeeds, ticker: nextTicker, slots: nextSlots || undefined });
    } catch (err: unknown) {
      setApiError(err instanceof DOMException && err.name === 'AbortError' ? 'Timed out' : (err instanceof Error ? err.message : 'Unable to add feeder'));
    }
    finally { clearTimeout(tid); setAddingFeeder(null); setIsBusy(false); }
  };
  const removeFeeder = async (feedId: string, handle: string) => runFeedAction({ action: 'remove_feeder', feedId: Number(feedId), handle });
  const resetCreateFeedModal = () => {
    setIsCreatingFeed(false);
    setNewFeedName('');
    setNewFeedBrief(defaultFeedBrief());
    setCreateFeedStep(0);
    setNewFeedBibleDraft('');
  };
  const closeCreateFeedModal = () => {
    const dirty = Boolean(
      newFeedName.trim()
      || JSON.stringify(newFeedBrief) !== JSON.stringify(defaultFeedBrief())
      || newFeedBibleDraft.trim(),
    );
    if (dirty && typeof window !== 'undefined' && !window.confirm('Discard this Brief?')) return;
    resetCreateFeedModal();
  };
  const updateNewFeedBrief = (patch: Partial<FeedBrief>) => {
    setNewFeedBrief((current) => ({ ...current, ...patch }));
  };
  const handleCreateFeed = async () => {
    if (!newFeedName.trim()) return;
    if (createFeedStep < CREATE_FEED_LAST_STEP) {
      const nextStep = createFeedStep + 1;
      if (nextStep === CREATE_FEED_LAST_STEP) {
        setNewFeedBibleDraft('');
      }
      setCreateFeedStep(nextStep);
      return;
    }
    const ok = await runFeedAction({
      action: 'create_feed',
      title: newFeedName.trim(),
      feedBrief: newFeedBrief,
      feedBriefText: newFeedBibleDraft.trim() || newFeedBriefPreview,
    });
    if (ok) resetCreateFeedModal();
  };
  const goToPreviousCreateFeedStep = () => {
    setCreateFeedStep((step) => Math.max(0, step - 1));
  };
  const resetCreateFeedContext = () => {
    setNewFeedName('');
    setNewFeedBrief(defaultFeedBrief());
    setCreateFeedStep(0);
    setNewFeedBibleDraft('');
  };
  const openFeedContextEditor = () => {
    if (!activeFeed) return;
    const brief = normalizeFeedBrief(activeFeed.feedBrief || activeFeed.contextBrief);
    setEditFeedBrief(brief);
    setEditFeedStep(0);
    setEditFeedBibleDraft(String(activeFeed.feedBriefText || activeFeed.contextBible || '').trim() || buildFeedBriefText(brief, activeFeed.title || 'this feed'));
    setIsEditingFeedContext(true);
  };
  const closeFeedContextEditor = () => {
    if (!activeFeed) {
      setIsEditingFeedContext(false);
      return;
    }
    const originalBrief = normalizeFeedBrief(activeFeed.feedBrief || activeFeed.contextBrief);
    const originalBible = String(activeFeed.feedBriefText || activeFeed.contextBible || '').trim() || buildFeedBriefText(originalBrief, activeFeed.title || 'this feed');
    const dirty = JSON.stringify(editFeedBrief) !== JSON.stringify(originalBrief)
      || editFeedBibleDraft.trim() !== originalBible.trim();
    if (dirty && typeof window !== 'undefined' && !window.confirm('Discard changes to this Brief?')) return;
    setIsEditingFeedContext(false);
  };
  const updateEditFeedBrief = (patch: Partial<FeedBrief>) => {
    setEditFeedBrief((current) => ({ ...current, ...patch }));
  };
  const handleSaveFeedContext = async () => {
    if (!activeFeed) return;
    if (editFeedStep < CREATE_FEED_LAST_STEP) {
      const nextStep = editFeedStep + 1;
      if (nextStep === CREATE_FEED_LAST_STEP && !editFeedBibleDraft.trim()) {
        setEditFeedBibleDraft('');
      }
      setEditFeedStep(nextStep);
      return;
    }
    const ok = await runFeedAction({
      action: 'update_feed_context',
      feedId: Number(activeFeed.id),
      title: activeFeed.title,
      feedBrief: editFeedBrief,
      feedBriefText: editFeedBibleDraft.trim() || editFeedBriefPreview,
    });
    if (ok) setIsEditingFeedContext(false);
  };
  const handleDeleteFeed = async (feedId: string) => {
    setPendingDeleteFeedId(feedId);
  };
  const confirmDeleteFeed = async () => {
    if (!pendingDeleteFeedId) return;
    const ok = await runFeedAction({ action: 'delete_feed', feedId: Number(pendingDeleteFeedId) });
    if (ok) setPendingDeleteFeedId(null);
  };

  return (
    <motion.div
      variants={pageVariants}
      initial={false}
      animate="visible"
      className={cn(
        'fm-dashboard-mesh relative w-full text-foreground select-none',
        useTranslucentBrowserChrome ? 'bg-transparent' : 'bg-[#f4f7f9] dark:bg-[#030303]',
        useBrowserPageScroll ? 'overflow-visible' : 'overflow-hidden',
      )}
      style={{
        ...appShellStyle,
        ['--fm-feed-header-height' as string]: `${headerHeight}px`,
      }}
    >
      {/* Ambient bg */}
      <div
        className="pointer-events-none fixed inset-0 z-0 bg-[#f4f7f9] dark:bg-[#030303]"
      />

      {/* ═══ LOCKED HEADER ═══ */}
      <motion.div
        ref={headerRef}
        variants={HEADER_STAGGER_CONTAINER}
        initial="initial"
        animate="animate"
        className={cn(
          'pointer-events-auto inset-x-0 top-0 z-[100] flex flex-col items-center px-2 pt-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-4 sm:pt-[calc(18px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] md:pt-[calc(20px+var(--pwa-top-fix,0px))] lg:px-4',
          useBrowserPageScroll ? 'fixed' : 'absolute',
          view === 'detail' && 'fm-feed-header-morph-isolated',
        )}
      >
        <div className="relative fm-tab-header-shell">
          <motion.div
            className={cn(
              'fm-depth-chrome fm-depth-chrome--header w-full',
              view === 'detail' && headerCompressed && 'fm-depth-chrome--header-compressed',
              view === 'detail' && 'fm-feed-header-morph-isolated',
            )}
            initial={false}
            animate={{ scale: view === 'detail' && headerCompressed ? 0.997 : 1 }}
            transition={FEED_HEADER_MORPH_TRANSITION}
          >
            <div className="relative z-10 px-3 py-1.5 sm:px-4 sm:py-2.5 lg:px-4 lg:py-2">
              {/* ═══ MOBILE HEADER (< lg) ═══ */}
              <div className="lg:hidden">
                {view === 'detail' ? (
                  <motion.div
                    className="fm-feed-header-morph-isolated grid overflow-hidden"
                    initial={false}
                    animate={{
                      gridTemplateRows: headerCompressed ? '52px 0px' : `58px ${mobileDashboardFilterRowHeight}`,
                      rowGap: headerCompressed ? 0 : 5,
                    }}
                    transition={FEED_HEADER_MORPH_TRANSITION}
                    style={{ willChange: 'grid-template-rows' }}
                  >
                    <motion.div initial={false} animate={{ opacity: 1 }} className="relative flex min-h-0 items-center">
                      <motion.div
                        className="absolute inset-0 grid grid-cols-[40px_minmax(0,1fr)_auto] items-center gap-2.5"
                        initial={false}
                        animate={{
                          opacity: headerCompressed ? 0 : 1,
                          x: headerCompressed ? -10 : 0,
                          scale: headerCompressed ? 0.985 : 1,
                        }}
                        transition={FEED_HEADER_MORPH_TRANSITION}
                        style={{ pointerEvents: headerCompressed ? 'none' : 'auto' }}
                      >
                        <motion.button
                          key="back-btn"
                          whileTap={{ scale: 0.92 }}
                          onClick={() => { play('snapLock'); handleBack(); }}
                          aria-label="Back to feeds"
                          className="relative flex h-[38px] w-[38px] shrink-0 items-center justify-center rounded-[15px] border border-white/76 bg-white/68 text-black/58 shadow-[0_5px_14px_rgba(15,23,42,0.08),0_1px_0_rgba(255,255,255,0.92)_inset,0_-1px_0_rgba(0,0,0,0.04)_inset] dark:border-white/10 dark:bg-white/[0.06] dark:text-white/45 dark:shadow-[0_7px_18px_rgba(0,0,0,0.42),0_1px_0_rgba(255,255,255,0.06)_inset]"
                        >
                          <ChevronLeft size={21} strokeWidth={2.7} />
                        </motion.button>

                        <div className="flex min-w-0 flex-col justify-center overflow-hidden">
                          <h1 className="min-w-0 truncate text-[28px] font-black leading-[0.9] tracking-[-0.06em] text-black dark:text-white fm-depth-title sm:text-[32px]">
                            {shortTitle}
                          </h1>
                          <span className="mt-1 block truncate text-[9px] font-black uppercase leading-none tracking-[0.22em] text-black/38 dark:text-white/34">
                            {selectedHandle === 'all' ? 'Full feed' : `@${selectedHandle}`}
                          </span>
                        </div>

                        <div className="min-w-[92px] rounded-[17px] border border-white/82 bg-white/76 px-3 py-2 text-right text-black shadow-[inset_0_1px_0_rgba(255,255,255,0.96),0_7px_18px_rgba(15,23,42,0.07)] dark:border-white/10 dark:bg-white/[0.06] dark:shadow-[0_8px_20px_rgba(0,0,0,0.38),0_1px_0_rgba(255,255,255,0.06)_inset]">
                          {topAveragePercentile == null ? (
                            <div className="text-[19px] font-black leading-none tracking-[-0.04em] text-black dark:text-white">--</div>
                          ) : (
                            <div className="flex items-baseline justify-end gap-0.5 whitespace-nowrap">
                              <span className="text-[13px] font-black leading-none tracking-[-0.02em] text-black/72 dark:text-white/72">Top</span>
                              <span className="text-[22px] font-black leading-none tracking-[-0.06em] text-[#E11D48]">{topAveragePercentile}%</span>
                            </div>
                          )}
                          <div className="mt-1 truncate text-[8px] font-black uppercase leading-none tracking-[0.18em] text-black/38 dark:text-white/32">
                            {topAveragePosts > 0 ? `${topAveragePosts} posts` : 'Avg rank'}
                          </div>
                        </div>
                      </motion.div>

                      <motion.div
                        className="absolute inset-0 grid grid-cols-[92px_minmax(0,1fr)] items-center gap-3"
                        initial={false}
                        animate={{
                          opacity: headerCompressed ? 1 : 0,
                          x: headerCompressed ? 0 : 10,
                          scale: headerCompressed ? 1 : 0.985,
                        }}
                        transition={FEED_HEADER_MORPH_TRANSITION}
                        style={{ pointerEvents: headerCompressed ? 'auto' : 'none' }}
                      >
                        <div className="flex min-w-0 items-end gap-1.5 justify-self-start">
                          <span className="text-[48px] font-black leading-[0.78] tracking-[-0.06em] text-black dark:text-white fm-depth-title">
                            {compactWindowNumber}
                          </span>
                          <span className="mb-1.5 text-[10px] font-black uppercase leading-none tracking-[0.2em] text-black/44 dark:text-white/38">
                            {compactWindowUnitLabel}
                          </span>
                        </div>
                        <div className="flex w-full min-w-0 flex-col items-end overflow-hidden text-right leading-none">
                          <span className="block max-w-full truncate text-[16px] font-black uppercase tracking-[0.04em] text-[#E11D48] drop-shadow-[0_8px_18px_rgba(225,29,72,0.16)]">
                            {compactPrimaryLabel}
                          </span>
                          <span className="mt-1 block max-w-full truncate text-[9px] font-black uppercase tracking-[0.24em] text-black/42 dark:text-white/36">
                            {compactSecondaryLabel}
                          </span>
                        </div>
                      </motion.div>
                    </motion.div>

                    <motion.div
                      initial={false}
                      className="min-w-0 overflow-hidden pointer-events-auto"
                      animate={{ opacity: headerCompressed ? 0 : 1, y: headerCompressed ? -8 : 0 }}
                      transition={FEED_HEADER_MORPH_TRANSITION}
                      style={{ pointerEvents: headerCompressed ? 'none' : 'auto' }}
                    >
                      <div className="flex flex-col gap-1">
                        <div className="relative grid h-[40px] grid-cols-4 items-center gap-1 rounded-[18px] border border-white/82 bg-white/70 p-1 shadow-[inset_0_2px_4px_rgba(214,223,235,0.34),inset_0_-1px_0_rgba(255,255,255,0.84),0_4px_10px_rgba(15,23,42,0.04)] dark:border-white/[0.05] dark:bg-white/[0.03] dark:shadow-[inset_0_2px_4px_rgba(0,0,0,0.3),inset_0_-1px_0_rgba(255,255,255,0.03)]">
                          {(['7D', '30D', '60D', '90D'] as Timeframe[]).map(tf => (
                            <motion.button key={tf} type="button" onClick={() => { play('snapLock'); setTimeframe(tf); }} whileTap={{ scale: 0.95 }}
                              className={cn('relative h-full rounded-[14px] text-center font-black',
                                tf === timeframe
                                  ? 'z-10 text-[15px] tracking-[-0.02em] text-white sm:text-[16px]'
                                  : 'z-0 text-[11px] tracking-[0.04em] text-black/42 sm:text-[12px] dark:text-white/38'
                              )} style={{ WebkitTapHighlightColor: 'transparent' }}>
                              {tf === timeframe && (
                                <motion.span
                                  layoutId="timeframe-pill-bg"
                                  className="absolute inset-0 rounded-[14px] border border-[#FB7185] bg-[#E11D48] shadow-[inset_0_1px_0_rgba(255,255,255,0.74),inset_0_-2px_4px_rgba(136,19,55,0.18),0_6px_16px_rgba(225,29,72,0.26)] dark:border-[#FDA4AF]/30 dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.18),0_4px_20px_rgba(225,29,72,0.2),0_12px_28px_rgba(0,0,0,0.5)]"
                                  transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }}
                                />
                              )}
                              <span className="relative z-10">{TIMEFRAME_LABELS[tf]}</span>
                            </motion.button>
                          ))}
                        </div>
                        {handles.length > 0 && (
                          <div className="hide-scrollbar -mx-1 flex min-h-[32px] items-center gap-1.5 overflow-x-auto px-1 py-0.5">
                            <motion.button whileTap={{ scale: 0.97 }} onClick={() => { play('snapLock'); setSelectedHandle('all'); }}
                              className={cn('relative flex h-[30px] shrink-0 items-center whitespace-nowrap rounded-full px-3.5 text-[9px] font-black uppercase leading-none tracking-[0.12em]',
                                selectedHandle === 'all' ? 'z-10 text-white' : 'fm-depth-chip z-0 text-foreground/50 dark:text-foreground/38')}>
                              {selectedHandle === 'all' && (
                                <motion.span layoutId="handle-pill-bg"
                                  className="absolute inset-0 rounded-full border border-[#FB7185] bg-[#E11D48] shadow-[inset_0_1px_0_rgba(255,255,255,0.74),inset_0_-2px_4px_rgba(136,19,55,0.18),0_5px_12px_rgba(225,29,72,0.28)]"
                                  transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }} />
                              )}
                              <span className="relative z-10">Full Feed</span>
                            </motion.button>
                            {handles.map(h => (
                              <motion.button key={h} whileTap={{ scale: 0.97 }} onClick={() => { play('snapLock'); setSelectedHandle(h); }}
                                className={cn('relative flex h-[30px] shrink-0 items-center whitespace-nowrap rounded-full px-3.5 text-[9px] font-black uppercase leading-none tracking-[0.08em]',
                                  selectedHandle === h
                                    ? 'z-10 max-w-[174px] text-white'
                                    : 'fm-depth-chip z-0 max-w-[132px] text-foreground/50 dark:text-foreground/38')}>
                                {selectedHandle === h && (
                                  <motion.span layoutId="handle-pill-bg"
                                    className="absolute inset-0 rounded-full border border-[#FB7185] bg-[#E11D48] shadow-[inset_0_1px_0_rgba(255,255,255,0.74),inset_0_-2px_4px_rgba(136,19,55,0.18),0_5px_12px_rgba(225,29,72,0.28)]"
                                    transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }} />
                                )}
                                <span className="relative z-10 truncate">@{h}</span>
                              </motion.button>
                            ))}
                          </div>
                        )}
                      </div>
                    </motion.div>
                  </motion.div>
                ) : (
                  <div className="flex flex-col gap-1.5 sm:gap-2">
                    <motion.div initial={false} animate={{ opacity: 1 }} className="flex items-center justify-between gap-3">
                      <div className="min-w-0 flex-1">
                        <h1 className="font-extrabold leading-none text-black dark:text-white fm-depth-title transition-[font-size,letter-spacing] duration-300 ease-[cubic-bezier(0.32,0.72,0,1)] text-[22px] tracking-[0.14em] sm:text-[38px] sm:font-black">
                          FEED
                        </h1>
                      </div>
                      <motion.div key="list-actions" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2, ease: [0.4, 0, 0.1, 1] }} className="flex shrink-0 items-center gap-2">
                        <motion.button
                          type="button"
                          whileTap={{ scale: 0.94 }}
                          aria-label={`Sort by ${sortMode}`}
                          onClick={() => { play('snapLock'); setSortMode((current) => current === 'recent' ? 'name' : current === 'name' ? 'feeders' : 'recent'); }}
                          className="relative flex h-[36px] w-[36px] items-center justify-center overflow-hidden rounded-[14px] bg-white/60 border border-white/70 shadow-[0_2px_6px_rgba(0,0,0,0.08),0_1px_0_rgba(255,255,255,0.9)_inset,0_-1px_0_rgba(0,0,0,0.04)_inset] dark:bg-white/[0.06] dark:border-white/10 dark:shadow-[0_2px_8px_rgba(0,0,0,0.4),0_1px_0_rgba(255,255,255,0.06)_inset] text-black/55 dark:text-white/50"
                        >
                          <motion.span
                            key={sortMode}
                            initial={{ y: 8, opacity: 0, scale: 0.9 }}
                            animate={{ y: 0, opacity: 1, scale: 1 }}
                            transition={{ duration: 0.26, ease: [0.4, 0, 0.1, 1] }}
                            className="flex items-center justify-center"
                          >
                            {sortMode === 'recent' ? (
                              <Clock size={16} strokeWidth={2.4} />
                            ) : sortMode === 'name' ? (
                              <span className="text-[12px] font-black leading-none tracking-[-0.02em]">Aa</span>
                            ) : (
                              <Users size={16} strokeWidth={2.4} />
                            )}
                          </motion.span>
                        </motion.button>
                        <motion.button type="button" whileTap={{ scale: 0.94 }} onClick={() => setIsCreatingFeed(true)} aria-label="Add feed"
                          className="flex h-[36px] items-center justify-center gap-1.5 rounded-[14px] bg-[#E11D48] px-2.5 text-[10px] font-black uppercase tracking-[0.14em] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.72),0_2px_4px_rgba(159,18,57,0.22),0_8px_18px_-4px_rgba(225,29,72,0.35)] dark:shadow-[0_2px_4px_rgba(225,29,72,0.15),0_8px_24px_-4px_rgba(225,29,72,0.25),0_1px_0_rgba(255,255,255,0.3)_inset] sm:px-3.5 sm:text-[11px]">
                          <Plus size={16} strokeWidth={3} /> <span className="hidden sm:inline">Add Feed</span>
                        </motion.button>
                      </motion.div>
                    </motion.div>
                    <motion.div
                      initial={false}
                      animate={{ opacity: 1 }}
                      className="min-w-0 pointer-events-auto"
                    >
                      <FlipTicker items={tickerItems} className="w-full" />
                    </motion.div>
                  </div>
                )}

                {apiError && <div className="mt-1.5 text-[10px] font-black uppercase tracking-[0.14em] text-red-600 dark:text-red-400">{apiError}</div>}
              </div>

              {/* ═══ DESKTOP HEADER (≥ lg) — Mirrors Fire tab ═══ */}
              <motion.div variants={HEADER_ROW} className="hidden flex-col gap-1.5 lg:flex">
                <AnimatePresence mode="popLayout" initial={false}>
                  {view === 'detail' ? (
                    <motion.div key="desktop-detail-header" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.3, ease: APPLE_EASE }} className="flex flex-col gap-1.5">
                      {/* Row 1: Back + Name | Timeframe pills | performance summary */}
                      <div className="grid grid-cols-[minmax(132px,auto)_minmax(0,1fr)_auto] items-center gap-2">
                        <div className="flex items-center gap-2">
                          <motion.button
                            whileTap={{ scale: 0.92 }}
                            onClick={() => { play('snapLock'); handleBack(); }}
                            className="relative flex shrink-0 items-center justify-center rounded-[14px] p-2 text-black/58 dark:text-white/45 bg-white/60 border border-white/70 shadow-[0_2px_6px_rgba(0,0,0,0.08),0_1px_0_rgba(255,255,255,0.9)_inset,0_-1px_0_rgba(0,0,0,0.04)_inset] dark:bg-white/[0.06] dark:border-white/10 dark:shadow-[0_2px_8px_rgba(0,0,0,0.4),0_1px_0_rgba(255,255,255,0.06)_inset]"
                          >
                            <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M15 18l-6-6 6-6" /></svg>
                          </motion.button>
                          <h1 className="truncate text-[26px] font-black tracking-[-0.04em] text-black dark:text-white fm-depth-title">
                            {shortTitle}
                          </h1>
                        </div>
                        <div className="flex justify-center px-0.5">
                          <div className="relative flex items-center gap-1.5 rounded-[18px] border border-white/82 bg-white/74 p-1 shadow-[inset_0_2px_4px_rgba(214,223,235,0.34),inset_0_-1px_0_rgba(255,255,255,0.84),0_4px_10px_rgba(15,23,42,0.04)] dark:bg-white/[0.03] dark:border-white/[0.05] dark:shadow-[inset_0_2px_4px_rgba(0,0,0,0.3),inset_0_-1px_0_rgba(255,255,255,0.03)]">
                            {(['7D', '30D', '60D', '90D'] as Timeframe[]).map(tf => (
                              <motion.button key={tf} type="button" onClick={() => { play('snapLock'); setTimeframe(tf); }} whileTap={{ scale: 0.95 }}
                                className={cn('relative rounded-[14px] px-4 py-1.75 text-center font-black',
                                  tf === timeframe
                                    ? 'z-10 text-[17px] tracking-[-0.02em] text-white'
                                    : 'z-0 text-[13px] tracking-[0.04em] text-black/45 dark:text-white/40'
                                )} style={{ WebkitTapHighlightColor: 'transparent' }}>
                                {tf === timeframe && (
                                  <motion.span
                                    layoutId="timeframe-pill-bg-desk"
                                    className="absolute inset-0 rounded-[14px] border border-[#FB7185] bg-[#E11D48] shadow-[inset_0_1px_0_rgba(255,255,255,0.74),inset_0_-2px_4px_rgba(136,19,55,0.18),0_6px_16px_rgba(225,29,72,0.26)] dark:border-[#FDA4AF]/30 dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.18),0_4px_20px_rgba(225,29,72,0.2),0_12px_28px_rgba(0,0,0,0.5)]"
                                    transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }}
                                  />
                                )}
                                <span className="relative z-10">{TIMEFRAME_LABELS[tf]}</span>
                              </motion.button>
                            ))}
                          </div>
                        </div>
                        <div className="flex items-center justify-end">
                          <div className="rounded-[14px] border border-black/6 bg-white/68 px-3 py-1.5 text-right shadow-[0_6px_14px_rgba(0,0,0,0.06),inset_0_1px_0_rgba(255,255,255,0.8)] dark:border-white/10 dark:bg-white/[0.07] dark:shadow-[0_8px_18px_rgba(0,0,0,0.3),inset_0_1px_0_rgba(255,255,255,0.06)]">
                            <div className="text-[7px] font-black uppercase tracking-[0.18em] text-black/36 dark:text-white/30">Avg perf</div>
                            <div className="flex items-baseline justify-end gap-1.5">
                              <span className="text-[22px] font-black leading-none tracking-[-0.04em] text-black dark:text-white">{topAverageLabel}</span>
                              <span className="text-[8px] font-black uppercase tracking-[0.1em] text-black/36 dark:text-white/30">
                                {topAveragePosts > 0 ? `${topAveragePosts}p` : ''}
                              </span>
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Row 2: Recessed filter tray with handle pills */}
                      {handles.length > 0 && (
                        <div className="flex flex-wrap items-center gap-1.5 overflow-hidden rounded-[18px] border border-black/5 bg-black/[0.035] px-2 py-1.5 shadow-[inset_0_2px_8px_rgba(0,0,0,0.04)] dark:border-white/8 dark:bg-white/[0.03] dark:shadow-[inset_0_2px_8px_rgba(0,0,0,0.3)]">
                          <motion.button whileTap={{ scale: 0.97 }} onClick={() => { play('snapLock'); setSelectedHandle('all'); }}
                            className={cn('relative whitespace-nowrap rounded-full px-3.5 py-1.25 text-[9px] font-black uppercase tracking-[0.1em]',
                              selectedHandle === 'all' ? 'z-10 text-white' : 'text-foreground/52 dark:text-foreground/40 z-0')}>
                            {selectedHandle === 'all' && (
                              <motion.span layoutId="handle-pill-bg-desk"
                                className="absolute inset-0 rounded-full border border-[#FB7185] bg-[#E11D48] shadow-[inset_0_1px_0_rgba(255,255,255,0.74),inset_0_-2px_4px_rgba(136,19,55,0.18),0_5px_12px_rgba(225,29,72,0.28)]"
                                transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }} />
                            )}
                            <span className="relative z-10">Full Feed</span>
                          </motion.button>
                          {handles.map(h => (
                            <motion.button key={h} whileTap={{ scale: 0.97 }} onClick={() => { play('snapLock'); setSelectedHandle(h); }}
                              className={cn('relative whitespace-nowrap rounded-full px-3.5 py-1.25 text-[9px] font-black uppercase tracking-[0.08em]',
                                selectedHandle === h ? 'z-10 text-white' : 'text-foreground/52 dark:text-foreground/40 z-0')}>
                              {selectedHandle === h && (
                                <motion.span layoutId="handle-pill-bg-desk"
                                  className="absolute inset-0 rounded-full border border-[#FB7185] bg-[#E11D48] shadow-[inset_0_1px_0_rgba(255,255,255,0.74),inset_0_-2px_4px_rgba(136,19,55,0.18),0_5px_12px_rgba(225,29,72,0.28)]"
                                  transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }} />
                              )}
                              <span className="relative z-10">@{h}</span>
                            </motion.button>
                          ))}
                        </div>
                      )}
                    </motion.div>
                  ) : (
                    <motion.div key="desktop-list-header" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.3, ease: APPLE_EASE }} className="flex flex-col gap-2">
                      {/* Row 1: FEED title | FlipTicker | + Add Feed */}
                      <div className="grid grid-cols-[minmax(148px,auto)_minmax(0,1fr)_auto] items-center gap-2.5">
                        <div className="text-[28px] font-black uppercase tracking-[0.18em] text-black dark:text-white fm-depth-title">
                          FEED
                        </div>
                        <div className="flex justify-center px-0.5 min-w-0">
                          <FlipTicker items={tickerItems} className="w-full" />
                        </div>
                        <div className="flex items-center justify-end gap-2">
                          <motion.button type="button" whileTap={{ scale: 0.94 }} onClick={() => setIsCreatingFeed(true)}
                            className="flex items-center justify-center gap-1.5 rounded-[14px] bg-[#E11D48] px-3.5 py-2 text-[11px] font-black uppercase tracking-[0.14em] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.72),0_2px_4px_rgba(159,18,57,0.22),0_8px_18px_-4px_rgba(225,29,72,0.35)] dark:shadow-[0_2px_4px_rgba(225,29,72,0.15),0_8px_24px_-4px_rgba(225,29,72,0.25),0_1px_0_rgba(255,255,255,0.3)_inset]">
                            <Plus size={15} strokeWidth={3} /> Add Feed
                          </motion.button>
                        </div>
                      </div>

                      {/* Row 2: Recessed filter tray with sort + feed count */}
                      <div className="flex flex-wrap items-center gap-2 overflow-hidden rounded-[18px] border border-black/5 bg-black/[0.035] px-2.5 py-2 shadow-[inset_0_2px_8px_rgba(0,0,0,0.04)] dark:border-white/8 dark:bg-white/[0.03] dark:shadow-[inset_0_2px_8px_rgba(0,0,0,0.3)]">
                        <div className="flex items-center gap-1 rounded-[14px] border border-black/5 bg-white/58 p-1 shadow-[0_4px_12px_rgba(0,0,0,0.06),inset_0_1px_0_rgba(255,255,255,0.75)] dark:border-white/8 dark:bg-white/[0.05] dark:shadow-[0_8px_18px_rgba(0,0,0,0.35),inset_0_1px_0_rgba(255,255,255,0.06)]">
                          {(['recent', 'name', 'feeders'] as SortMode[]).map(mode => {
                            const isActive = sortMode === mode;
                            return (
                              <motion.button
                                key={mode}
                                type="button"
                                whileTap={{ scale: 0.95 }}
                                onClick={() => { play('snapLock'); setSortMode(mode); }}
                                className={cn(
                                  'rounded-[11px] px-2.5 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] transition-colors duration-200',
                                  isActive
                                    ? 'bg-[#E11D48] text-white shadow-[0_6px_14px_rgba(225,29,72,0.22),inset_0_1px_0_rgba(255,255,255,0.75)]'
                                    : 'text-black/55 dark:text-white/45',
                                )}
                              >
                                {mode === 'recent' ? 'RECENT' : mode === 'name' ? 'NAME' : 'FEEDERS'}
                              </motion.button>
                            );
                          })}
                        </div>

                        <div className="rounded-[12px] border border-black/6 bg-white/60 px-2.5 py-1.5 text-[9px] font-black uppercase tracking-[0.14em] text-black shadow-[0_4px_10px_rgba(0,0,0,0.05),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/8 dark:bg-white/[0.05] dark:text-white/78 dark:shadow-[0_8px_18px_rgba(0,0,0,0.32),inset_0_1px_0_rgba(255,255,255,0.06)]">
                          {feeds.length} {feeds.length === 1 ? 'FEED' : 'FEEDS'}
                        </div>
                      </div>
                    </motion.div>
                  )}
                </AnimatePresence>

                {apiError && <div className="text-[10px] font-black uppercase tracking-[0.14em] text-red-600 dark:text-red-400">{apiError}</div>}
              </motion.div>
            </div>

          </motion.div>
        </div>
      </motion.div>

      {/* ═══ CONTENT ═══ */}
      <AnimatePresence mode="wait" initial={false}>
        {view === 'list' ? (
          <motion.div key="list"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.32, ease: [0.22, 1, 0.36, 1] }}
            className={cn(
              'z-10',
              useBrowserPageScroll
                ? cn('relative', useTranslucentBrowserChrome ? 'min-h-[100lvh]' : 'min-h-[var(--fm-app-height,100dvh)]')
                : 'relative h-full',
            )}>
            <div
              className={cn(
                'w-full overflow-x-hidden pt-[calc(178px+env(safe-area-inset-top))] sm:pt-[calc(184px+env(safe-area-inset-top))] md:pt-[214px]',
                useBrowserPageScroll
                  ? cn(useTranslucentBrowserChrome ? 'min-h-[100lvh]' : 'min-h-[var(--fm-app-height,100dvh)]', 'overflow-visible')
                  : 'hide-scrollbar h-full overflow-y-auto',
              )}
              style={{
                WebkitOverflowScrolling: useBrowserPageScroll ? undefined : 'touch',
                paddingBottom: isStandaloneMode ? 'calc(190px + env(safe-area-inset-bottom))' : mobileListBottomClearance,
              }}
            >
              <div className="w-full px-2 sm:px-3 lg:px-4">
                <motion.div layout className="fm-tab-canvas-shell mx-auto grid grid-cols-1 gap-4 min-[720px]:grid-cols-2 lg:gap-5 xl:grid-cols-3">
                  <AnimatePresence mode="popLayout">
                    {sortedFeeds.map((feed, i) => (
                      <motion.div
                        key={feed.id}
                        layout
                        initial={shouldAnimateFeedTilesRef.current ? { opacity: 0, y: 22, scale: 0.985 } : false}
                        animate={{ opacity: 1, y: 0, scale: 1 }}
                        exit={{ opacity: 0, y: 10, scale: 0.985 }}
                        transition={{
                          layout: GRID_LAYOUT_SPRING,
                          opacity: {
                            duration: 0.22,
                            delay: FEED_TILE_ENTRY_BASE_DELAY + Math.min(i * FEED_TILE_ENTRY_STAGGER, FEED_TILE_ENTRY_MAX_STAGGER),
                            ease: GRID_ITEM_EASE,
                          },
                          y: {
                            type: 'spring',
                            stiffness: 270,
                            damping: 30,
                            mass: 0.9,
                            delay: FEED_TILE_ENTRY_BASE_DELAY + Math.min(i * FEED_TILE_ENTRY_STAGGER, FEED_TILE_ENTRY_MAX_STAGGER),
                          },
                          scale: {
                            duration: 0.34,
                            delay: FEED_TILE_ENTRY_BASE_DELAY + Math.min(i * FEED_TILE_ENTRY_STAGGER, FEED_TILE_ENTRY_MAX_STAGGER),
                            ease: GRID_ITEM_EASE,
                          },
                        }}
                      >
                        <FeedTile title={feed.title} count={feed.feeders.length} anchor={feed.feeders.find(f => f.isAnchor)?.handle} feeders={feed.feeders}
                          metrics={feed.metrics} onClick={() => handleFeedClick(feed.id)} onPreview={() => preloadDashboard(feed.id)} onDelete={() => handleDeleteFeed(feed.id)} index={i} enableEntranceAnimation={false} />
                      </motion.div>
                    ))}
                  </AnimatePresence>
                  {feedDataReady && feeds.length === 0 && (
                    <div className="col-span-full fm-depth-glass rounded-[28px] px-6 py-10 text-center">
                      <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-[18px] fm-depth-chip">
                        <Target size={26} className="text-foreground/48 dark:text-white/42" />
                      </div>
                      <div className="mt-4 text-[18px] font-black uppercase tracking-[0.14em] text-foreground/72 dark:text-white/66">No feeds yet</div>
                      <p className="mt-2 text-[12px] font-black uppercase tracking-[0.1em] text-foreground/42 dark:text-white/34">Create a feed to start tracking feeder-level momentum</p>
                    </div>
                  )}
                </motion.div>
              </div>
            </div>
          </motion.div>
        ) : (
          <motion.div key={`detail-${selectedFeedId}`}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.32, ease: [0.22, 1, 0.36, 1] }}
            className={cn(
              'z-10',
              useBrowserPageScroll
                ? cn('relative', useTranslucentBrowserChrome ? 'min-h-[100lvh]' : 'min-h-[var(--fm-app-height,100dvh)]')
                : 'relative h-full',
            )}>
            <FeedDetailV2
              activeFeed={activeFeed}
              timeframe={timeframe}
              dashboardData={effectiveDashboardData}
              baselineDashboardData={effectiveBaselineDashboardData}
              usePageScroll={useBrowserPageScroll}
              mobileSnapSections={isStandaloneMode}
              bottomClearance={isStandaloneMode ? 'calc(120px + env(safe-area-inset-bottom))' : mobileBottomClearance}
              headerOffset={useBrowserPageScroll ? undefined : `${headerHeight + 16}px`}
              immersiveBrowserMode={useTranslucentBrowserChrome}
              exportScopeLabel={exportScopeLabel}
              exportFrom={exportFrom}
              exportTo={exportTo}
              onExportFromChange={setExportFrom}
              onExportToChange={setExportTo}
              onExport={() => { play('snapLock'); handleDownloadExport(); }}
            >
              {activeFeed && (
                <FeederFileTile
                  key="feeder-file"
                  feedTitle={activeFeed.title}
                  feederCount={activeFeed.feeders.length}
                  trackedPosts={activeFeed.metrics?.postsTracked || '0'}
                  onOpen={() => {
                    const params = new URLSearchParams();
                    if (selectedHandle !== 'all') params.set('handle', selectedHandle);
                    const query = params.toString();
                    play('snapLock');
                    router.push(`/feed/${activeFeed.id}/feeder-file${query ? `?${query}` : ''}`, { scroll: false });
                  }}
                />
              )}
              {activeFeed && (
                <motion.button
                  key="brief"
                  layout
                  initial={{ scale: 0.95 }}
                  animate={{ scale: 1 }}
                  whileTap={{ scale: 0.98 }}
                  type="button"
                  onClick={openFeedContextEditor}
                  className="fm-depth-glass flex min-h-[180px] flex-col justify-between rounded-[22px] p-4 text-left"
                >
                  <div className="flex items-start justify-between gap-3">
                    <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-[14px] border border-black/6 bg-white/68 text-foreground/56 shadow-[inset_0_1px_0_rgba(255,255,255,0.8)] dark:border-white/8 dark:bg-white/[0.05] dark:text-white/48">
                      <FileText size={19} strokeWidth={2.5} />
                    </div>
                    <span className={cn(
                      'rounded-full px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em]',
                      activeFeed.feedBriefText || activeFeed.contextBible
                        ? 'bg-[#E11D48] text-white'
                        : 'bg-black/6 text-foreground/42 dark:bg-white/8 dark:text-white/36',
                    )}>
                      {activeFeed.feedBriefText || activeFeed.contextBible ? 'Saved' : 'Add'}
                    </span>
                  </div>
                  <div>
                    <div className="text-[18px] font-black tracking-normal text-foreground dark:text-white">Brief</div>
                    <div className="mt-1 line-clamp-2 text-[11px] font-semibold leading-relaxed text-foreground/48 dark:text-white/38">
                      {activeFeed.feedBriefText || activeFeed.contextBible || 'Add the niche, audience, and tracking priorities before intelligence cards run.'}
                    </div>
                  </div>
                </motion.button>
              )}
              {addingFeeder && <ScanningCard key="scanning" handle={addingFeeder} />}
              {activeFeed?.feeders.length === 0 && !addingFeeder ? (
                <div className="col-span-full fm-depth-glass flex flex-col items-center justify-center py-20">
                  <Target size={48} className="mb-4 text-foreground/40" />
                  <span className="font-black uppercase text-xl text-foreground/50 tracking-widest">NO TARGETS ACQUIRED</span>
                </div>
              ) : (
                activeFeed?.feeders.map((feeder, i) => (
                  <FeederRow key={feeder.handle} index={i} handle={feeder.handle} isAnchor={feeder.isAnchor} profilePicUrl={feeder.profilePicUrl}
                    metrics={feeder.metrics} onMakeAnchor={() => activeFeed && handleMakeAnchor(activeFeed.id, feeder.handle)}
                    onRemove={() => activeFeed && removeFeeder(activeFeed.id, feeder.handle)}
                    onOpenFeed={() => {
                      if (!activeFeed) return;
                      play('snapLock');
                      router.push(`/feed/${activeFeed.id}/feeder/${encodeURIComponent(feeder.handle)}`, { scroll: false });
                    }} />
                ))
              )}
              <motion.div layout initial={{ scale: 0.95 }} animate={{ scale: 1 }} whileTap={{ scale: 0.98 }}
                className={cn("fm-depth-glass rounded-[22px] p-4 min-h-[180px] flex flex-col justify-center items-center group cursor-pointer", isAddingFeeder ? "ring-1 ring-black/10 dark:ring-[#E11D48]/45" : "")}
                onClick={() => !isAddingFeeder && setIsAddingFeeder(true)}>
                {!isAddingFeeder ? (
                  <div className="flex flex-col items-center gap-2">
                    <Plus size={48} strokeWidth={2} className="text-foreground/30" />
                    <span className="font-black uppercase tracking-[0.2em] text-[10px] text-foreground/40">ADD FEEDER</span>
                    <span className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/30">Uses 1 slot · {slotUsage.used ?? 0} used</span>
                  </div>
                ) : (
                    <div className="w-full h-full flex flex-col justify-between" onClick={e => e.stopPropagation()}>
                      <div className="flex items-center gap-3 w-full">
                      <div className="flex h-12 w-12 shrink-0 items-center justify-center rounded-[12px] border border-[#FB7185] bg-[#E11D48] text-2xl font-black text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.72),0_8px_18px_-8px_rgba(225,29,72,0.34)]">@</div>
                      <input autoFocus placeholder="HANDLE" className="bg-transparent text-2xl font-black uppercase w-full focus:outline-none placeholder:text-foreground/20 text-foreground h-12"
                        onKeyDown={e => { if (e.key === 'Enter') { handleAddFeeder(activeFeed!.id, e.currentTarget.value); setIsAddingFeeder(false); } if (e.key === 'Escape') setIsAddingFeeder(false); }} />
                    </div>
                    <div className="flex items-center justify-between mt-auto pt-3 border-t border-black/6 dark:border-white/8">
                      <span className="text-[9px] font-black uppercase tracking-widest text-foreground/40">PRESS ENTER</span>
                      <button onClick={() => setIsAddingFeeder(false)} className="text-[9px] font-black uppercase tracking-widest text-foreground/60">CANCEL</button>
                    </div>
                  </div>
                )}
              </motion.div>
            </FeedDetailV2>
          </motion.div>
        )}
      </AnimatePresence>

      <FeedBriefDialog
        open={isCreatingFeed}
        mode="create"
        title={newFeedName || 'New feed'}
        feedName={newFeedName}
        brief={newFeedBrief}
        step={createFeedStep}
        bibleDraft={newFeedBibleDraft}
        isBusy={isBusy}
        onFeedNameChange={setNewFeedName}
        onBriefChange={updateNewFeedBrief}
        onStepChange={setCreateFeedStep}
        onBibleDraftChange={setNewFeedBibleDraft}
        onClose={closeCreateFeedModal}
        onPrimary={handleCreateFeed}
        onBack={goToPreviousCreateFeedStep}
        onStartOver={resetCreateFeedContext}
      />

      <FeedBriefDialog
        open={isEditingFeedContext && Boolean(activeFeed)}
        mode="edit"
        title={activeFeed?.title || 'Feed'}
        brief={editFeedBrief}
        step={editFeedStep}
        bibleDraft={editFeedBibleDraft}
        isBusy={isBusy}
        onBriefChange={updateEditFeedBrief}
        onStepChange={setEditFeedStep}
        onBibleDraftChange={setEditFeedBibleDraft}
        onClose={closeFeedContextEditor}
        onPrimary={handleSaveFeedContext}
        onBack={() => setEditFeedStep((current) => Math.max(0, current - 1))}
        onStartOver={() => {
          if (!activeFeed) return;
          const brief = normalizeFeedBrief(activeFeed.feedBrief || activeFeed.contextBrief);
          setEditFeedBrief(brief);
          setEditFeedStep(0);
          setEditFeedBibleDraft(String(activeFeed.feedBriefText || activeFeed.contextBible || '').trim() || buildFeedBriefText(brief, activeFeed.title || 'this feed'));
        }}
      />
      <AnimatePresence>
        {pendingDeleteFeed && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.22, ease: APPLE_EASE }}
            className="fixed inset-0 z-[125] flex items-center justify-center bg-[rgba(244,247,249,0.54)] px-4 dark:bg-[rgba(3,3,3,0.68)]"
          >
            <motion.div
              initial={{ y: 18, opacity: 0, scale: 0.985 }}
              animate={{ y: 0, opacity: 1, scale: 1 }}
              exit={{ y: 14, opacity: 0, scale: 0.985 }}
              transition={{ duration: 0.26, ease: APPLE_EASE }}
              className="fm-depth-glass relative w-full max-w-[500px] overflow-hidden rounded-[30px] border border-white/85 p-6 dark:border-white/10"
            >
              <div className="pointer-events-none absolute inset-0 rounded-[30px] dark:opacity-0"
                style={{ background: 'linear-gradient(180deg, rgba(255,255,255,0.42) 0%, rgba(255,255,255,0) 34%, rgba(0,0,0,0.02) 100%)' }}
              />
              <div className="relative z-10">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <div className="text-[11px] font-black uppercase tracking-[0.18em] text-red-500 dark:text-red-400">Delete Feed</div>
                    <div className="mt-2 text-[26px] font-black uppercase tracking-[-0.05em] text-foreground dark:text-white">
                      Remove {pendingDeleteFeed.title}?
                    </div>
                    <p className="mt-2 max-w-[360px] text-[11px] font-black uppercase tracking-[0.1em] text-foreground/38 dark:text-white/32">
                      This permanently removes the bundle and its tracked backend data.
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={() => setPendingDeleteFeedId(null)}
                    className="fm-depth-chip flex h-10 w-10 shrink-0 items-center justify-center rounded-[14px] text-foreground/56 dark:text-white/46"
                  >
                    <X size={16} strokeWidth={2.6} />
                  </button>
                </div>
                <div className="mt-6 flex items-center justify-end gap-3">
                  <button
                    type="button"
                    onClick={() => setPendingDeleteFeedId(null)}
                    className="fm-depth-chip rounded-[16px] px-4 py-2.5 text-[11px] font-black uppercase tracking-[0.16em] text-foreground/70 dark:text-white/60"
                  >
                    Cancel
                  </button>
                  <motion.button
                    type="button"
                    whileTap={{ scale: 0.95 }}
                    onClick={confirmDeleteFeed}
                    disabled={isBusy}
                    className="rounded-[16px] bg-[#ff5b66] px-4 py-2.5 text-[11px] font-black uppercase tracking-[0.16em] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.28),0_12px_28px_-12px_rgba(255,91,102,0.6)] disabled:opacity-40"
                  >
                    {isBusy ? 'Deleting' : 'Delete Feed'}
                  </motion.button>
                </div>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}

export default function FeedPage() {
  return (
    <Suspense fallback={<div className="h-screen w-full bg-background" />}>
      <FeedPageContent />
    </Suspense>
  );
}
