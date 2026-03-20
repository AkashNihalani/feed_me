'use client';

import { useCallback, useEffect, useState, useMemo, Suspense } from 'react';
import { useSearchParams, useRouter } from 'next/navigation';
import { motion, AnimatePresence, LayoutGroup } from 'framer-motion';
import { Plus, Target, X } from 'lucide-react';
import FeedTile from '@/components/feed/FeedTile';
import FeederRow from '@/components/feed/FeederRow';
import ScanningCard from '@/components/feed/ScanningCard';
import FeedDetailV2 from '@/components/feed/FeedDetailV2';
import TickerTape, { TickerItem } from '@/components/feed/TickerTape';
import { DashboardPayload, TIMEFRAME_TO_WEEKS, Timeframe } from '@/components/feed/dashboardTypes';
import { cn } from '@/lib/utils';
import { useAppHaptics } from '@/lib/haptics';
import { getCache, readCache, setCache } from '@/lib/pageCache';

/* ── Types ── */
type Metrics = { likes: string; comments: string; views: string; postsTracked: string };
type FeedMetrics = Metrics & { percentile?: number | string | null };
type Feeder = {
  handle: string; isAnchor: boolean; profilePicUrl?: string | null;
  followerCount?: number | null; verificationStatus?: 'pending' | 'verified' | 'failed'; metrics: Metrics;
};
type Feed = { id: string; title: string; feeders: Feeder[]; metrics: FeedMetrics; compositePercentile?: number | string | null };
type SortMode = 'recent' | 'name' | 'feeders';
type SlotUsage = {
  used: number;
  handles?: string[];
  limit?: number | null;
  plan?: { price: number; postsCap: number; packPrice: number; packSize: number };
};

const INITIAL_FEEDS: Feed[] = [];
const APPLE_EASE = [0.32, 0.72, 0, 1] as const;
const FEED_CACHE_KEY = 'feed:bundle:v3';
const DASHBOARD_CACHE_PREFIX = 'feed:dashboard:v2';
const FEED_CACHE_TTL = 2 * 60 * 1000;
const DASHBOARD_CACHE_TTL = 2 * 60 * 1000;
const TIMEFRAME_LABELS: Record<Timeframe, string> = {
  '4W': '1M',
  '12W': '3M',
  '26W': '6M',
  '52W': '12M',
};

function parseCompactNumber(value: unknown): number {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value !== 'string') return 0;
  const src = value.trim().toUpperCase().replace(/,/g, '');
  const m = src.match(/^(\d+(?:\.\d+)?)([KMB])?$/);
  if (!m) { const n = Number(src); return Number.isFinite(n) ? n : 0; }
  const base = parseFloat(m[1]);
  if (m[2] === 'K') return base * 1_000;
  if (m[2] === 'M') return base * 1_000_000;
  if (m[2] === 'B') return base * 1_000_000_000;
  return base;
}

function computeCompositePercentile(activeFeed: Feed | undefined): number {
  if (!activeFeed) return 50;
  const fromData = Number(activeFeed.compositePercentile ?? activeFeed.metrics?.percentile);
  if (Number.isFinite(fromData) && fromData >= 1 && fromData <= 100) return Math.round(fromData);
  const views = parseCompactNumber(activeFeed?.metrics?.views);
  const likes = parseCompactNumber(activeFeed?.metrics?.likes);
  const comments = parseCompactNumber(activeFeed?.metrics?.comments);
  const strength = Math.log10(views + 1) * 0.5 + Math.log10(likes + 1) * 0.3 + Math.log10(comments + 1) * 0.2;
  const normalized = Math.max(0, Math.min(1, strength / 6));
  return Math.max(1, Math.min(100, Math.round(100 - normalized * 90) || 32));
}

/* ── Slide-up entrance ── */
const pageVariants = {
  hidden: { opacity: 0, y: 14, scale: 0.992 },
  visible: {
    opacity: 1,
    y: 0,
    scale: 1,
    transition: { duration: 0.42, ease: APPLE_EASE },
  },
};

function FeedPageContent() {
  const searchParams = useSearchParams();
  const router = useRouter();
  const { play } = useAppHaptics();

  useEffect(() => {
    try {
      const intent = sessionStorage.getItem('feedme:intent');
      if (intent === '/') { sessionStorage.removeItem('feedme:intent'); return; }
      const lastTab = sessionStorage.getItem('feedme:last-tab');
      const lastTs = Number(sessionStorage.getItem('feedme:last-tab-ts') || 0);
      const nav = performance.getEntriesByType('navigation')[0] as PerformanceNavigationTiming | undefined;
      const recent = lastTab === '/fire' && Date.now() - lastTs < 120000;
      const cameFromFire = (document.referrer || '').includes('/fire');
      if (recent && (nav?.type === 'reload' || cameFromFire)) router.replace('/fire');
    } catch {}
  }, [router]);

  const selectedFeedId = searchParams.get('id');
  const view = selectedFeedId ? 'detail' : 'list';

  const [feeds, setFeeds] = useState<Feed[]>(INITIAL_FEEDS);
  const [isAddingFeeder, setIsAddingFeeder] = useState(false);
  const [addingFeeder, setAddingFeeder] = useState<string | null>(null);
  const [isCreatingFeed, setIsCreatingFeed] = useState(false);
  const [pendingDeleteFeedId, setPendingDeleteFeedId] = useState<string | null>(null);
  const [newFeedName, setNewFeedName] = useState('');
  const [isBusy, setIsBusy] = useState(false);
  const [apiError, setApiError] = useState<string | null>(null);
  const [timeframe, setTimeframe] = useState<Timeframe>('4W');
  const [selectedHandle, setSelectedHandle] = useState<string>('all');
  const [dashboardData, setDashboardData] = useState<DashboardPayload | null>(null);
  const [tickerItems, setTickerItems] = useState<TickerItem[]>([]);
  const [sortMode, setSortMode] = useState<SortMode>('recent');
  const [slotUsage, setSlotUsage] = useState<SlotUsage>({ used: 0 });

  const activeFeed = feeds.find(f => f.id === selectedFeedId);
  const sortedFeeds = useMemo(() => {
    const list = [...feeds];
    if (sortMode === 'name') return list.sort((a, b) => a.title.localeCompare(b.title));
    if (sortMode === 'feeders') return list.sort((a, b) => b.feeders.length - a.feeders.length || a.title.localeCompare(b.title));
    return list;
  }, [feeds, sortMode]);
  const composite = useMemo(() => computeCompositePercentile(activeFeed), [activeFeed]);
  const shortTitle = String(activeFeed?.title || 'Feed').toUpperCase();
  const pendingDeleteFeed = useMemo(
    () => (pendingDeleteFeedId ? feeds.find((feed) => feed.id === pendingDeleteFeedId) ?? null : null),
    [feeds, pendingDeleteFeedId]
  );
  const handles = useMemo<string[]>(() => {
    if (!activeFeed) return [];
    return (activeFeed.feeders || []).map((f) => String(f.handle || '').trim()).filter(Boolean);
  }, [activeFeed]);

  /* ── Data loading ── */
  const loadFeeds = useCallback(async () => {
    const cached = getCache<{ feeds: Feed[]; ticker: TickerItem[]; slots?: SlotUsage }>(FEED_CACHE_KEY, FEED_CACHE_TTL);
    if (cached) {
      setFeeds(cached.feeds);
      setTickerItems(cached.ticker);
      if (cached.slots) setSlotUsage(cached.slots);
      return;
    }
    try {
      const res = await fetch('/api/feed');
      const json = await res.json();
      if (!res.ok) throw new Error(json.error || 'Failed to load feeds');
      const nextFeeds = (json.feeds || []) as Feed[];
      const nextTicker = (json.ticker || []) as TickerItem[];
      const nextSlots = (json.slots || null) as SlotUsage | null;
      setFeeds(nextFeeds);
      setTickerItems(nextTicker);
      if (nextSlots) setSlotUsage(nextSlots);
      setCache(FEED_CACHE_KEY, { feeds: nextFeeds, ticker: nextTicker, slots: nextSlots || undefined });
    } catch (err) {
      throw err;
    }
  }, []);

  useEffect(() => { loadFeeds().catch(err => setApiError(err instanceof Error ? err.message : 'Failed to load feeds')); }, [loadFeeds]);
  useEffect(() => { setSelectedHandle('all'); setTimeframe('4W'); }, [selectedFeedId]);

  useEffect(() => {
    if (!activeFeed) { setDashboardData(null); return; }
    const controller = new AbortController();
    const params = new URLSearchParams({ feedId: String(activeFeed.id), weeks: String(TIMEFRAME_TO_WEEKS[timeframe]) });
    if (selectedHandle !== 'all') params.set('handle', selectedHandle);
    const cacheKey = `${DASHBOARD_CACHE_PREFIX}:${activeFeed.id}:${TIMEFRAME_TO_WEEKS[timeframe]}:${selectedHandle}`;
    const cached = readCache<DashboardPayload>(cacheKey);
    const hadCache = Boolean(cached);
    if (cached && Date.now() - cached.ts <= DASHBOARD_CACHE_TTL) {
      setDashboardData(cached.data);
      return () => controller.abort();
    }
    fetch(`/api/feed/dashboard?${params}`, { signal: controller.signal })
      .then(async res => {
        const json = await res.json();
        if (!res.ok) throw new Error(json.error || 'Failed');
        const next = (json.dashboard || null) as DashboardPayload | null;
        setDashboardData(next);
        if (next) setCache(cacheKey, next);
      })
      .catch((err: unknown) => {
        if (!(err instanceof DOMException && err.name === 'AbortError') && !hadCache) {
          setApiError(err instanceof Error ? err.message : 'Failed');
        }
      });
    return () => controller.abort();
  }, [activeFeed, timeframe, selectedHandle]);

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

  const handleFeedClick = (id: string) => router.push(`/?id=${id}`);
  const handleBack = () => router.push('/');
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
  const handleCreateFeed = async () => {
    if (!newFeedName.trim()) return;
    const ok = await runFeedAction({ action: 'create_feed', title: newFeedName.trim() });
    if (ok) { setNewFeedName(''); setIsCreatingFeed(false); }
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
      initial="hidden"
      animate="visible"
      className="relative h-[100svh] w-full overflow-hidden bg-background text-foreground select-none md:h-[100dvh]"
    >
      {/* Ambient bg */}
      <div className="pointer-events-none fixed inset-0 z-0 bg-white dark:bg-[#030303]" />

      {/* ═══ LOCKED HEADER ═══ */}
      <div className="pointer-events-auto absolute inset-x-0 top-0 z-[100] flex flex-col items-center px-2 pt-[calc(10px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-4 sm:pt-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] md:pt-[calc(20px+var(--pwa-top-fix,0px))] lg:px-4">
        <div className="relative fm-tab-header-shell">
          <div
            className={cn(
              'w-full overflow-hidden rounded-[32px] relative transition-all duration-500 ease-[cubic-bezier(0.4,0,0.1,1)]',
              'bg-white/65 backdrop-blur-[48px] backdrop-saturate-[200%]',
              'border border-white/80 border-t-white/90',
              'shadow-[0_1px_0_rgba(255,255,255,0.95)_inset,0_-1px_0_rgba(0,0,0,0.03)_inset,0_4px_8px_rgba(0,0,0,0.03),0_12px_28px_-4px_rgba(0,0,0,0.08),0_32px_64px_-12px_rgba(0,0,0,0.1),0_48px_96px_-16px_rgba(0,0,0,0.06)]',
              'dark:bg-[rgba(6,6,6,0.65)] dark:border-white/[0.07] dark:border-t-white/[0.12]',
              'dark:shadow-[0_1px_0_rgba(255,255,255,0.06)_inset,0_-1px_0_rgba(0,0,0,0.5)_inset,0_8px_16px_rgba(0,0,0,0.4),0_24px_48px_-8px_rgba(0,0,0,0.6),0_48px_96px_-16px_rgba(0,0,0,0.5)]',
            )}
          >
            {/* Inner bevel highlight */}
            <div className="pointer-events-none absolute inset-0 rounded-[32px] z-0 dark:opacity-0 transition-opacity"
              style={{ background: 'linear-gradient(180deg, rgba(255,255,255,0.45) 0%, rgba(255,255,255,0) 30%, rgba(0,0,0,0.015) 100%)' }}
            />
            <div className="pointer-events-none absolute inset-[1px] rounded-[31px] z-0 dark:hidden"
              style={{ boxShadow: 'inset 0 2px 4px rgba(255,255,255,0.7), inset 0 -2px 6px rgba(0,0,0,0.04)' }}
            />

            <div className="relative z-10 px-3.5 py-3 sm:px-5 sm:py-3.5">
              <div className="flex flex-col gap-2 sm:gap-2.5">
                {/* Row 1: Title + actions */}
                <div className="flex items-center justify-between gap-3">
                  <div className="flex min-w-0 items-center gap-2.5">
                    <AnimatePresence mode="popLayout">
                      {view === 'detail' && (
                        <motion.button
                          key="back-btn"
                          initial={{ opacity: 0, scale: 0.8 }}
                          animate={{ opacity: 1, scale: 1 }}
                          exit={{ opacity: 0, scale: 0.8 }}
                          transition={{ duration: 0.25, ease: [0.4, 0, 0.1, 1] }}
                          whileTap={{ scale: 0.92 }}
                          onClick={() => { play('snapLock'); handleBack(); }}
                          className="relative flex shrink-0 items-center justify-center rounded-[14px] p-2 text-black/58 dark:text-white/45 bg-white/60 border border-white/70 shadow-[0_2px_6px_rgba(0,0,0,0.08),0_1px_0_rgba(255,255,255,0.9)_inset,0_-1px_0_rgba(0,0,0,0.04)_inset] dark:bg-white/[0.06] dark:border-white/10 dark:shadow-[0_2px_8px_rgba(0,0,0,0.4),0_1px_0_rgba(255,255,255,0.06)_inset]"
                        >
                          <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round"><path d="M15 18l-6-6 6-6" /></svg>
                        </motion.button>
                      )}
                    </AnimatePresence>
                    <AnimatePresence mode="popLayout" initial={false}>
                      <motion.div
                        key={view === 'list' ? 'feed-title' : `detail-${selectedFeedId}`}
                        initial={{ opacity: 0, y: 8, scale: 0.99 }}
                        animate={{ opacity: 1, y: 0, scale: 1 }}
                        exit={{ opacity: 0, y: -6, scale: 0.99 }}
                        transition={{ duration: 0.28, ease: APPLE_EASE }}
                      >
                        <h1 className={cn(
                          'font-black leading-none text-black dark:text-white fm-depth-title',
                          view === 'list'
                            ? 'text-[30px] tracking-[0.14em] sm:text-[38px]'
                            : 'truncate text-[28px] tracking-[-0.04em] sm:text-[34px]'
                        )}>
                          {view === 'list' ? 'FEED' : shortTitle}
                        </h1>
                      </motion.div>
                    </AnimatePresence>
                  </div>

                  {/* Right side */}
                  <AnimatePresence mode="popLayout" initial={false}>
                    {view === 'list' ? (
                      <motion.div key="list-actions" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.2, ease: [0.4, 0, 0.1, 1] }} className="flex shrink-0 items-center gap-2">
                        <motion.button
                          type="button"
                          whileTap={{ scale: 0.94 }}
                          onClick={() => { play('snapLock'); setSortMode((current) => current === 'recent' ? 'name' : current === 'name' ? 'feeders' : 'recent'); }}
                          className="relative flex items-center gap-1.5 overflow-hidden rounded-[14px] px-3 py-2 bg-white/60 border border-white/70 shadow-[0_2px_6px_rgba(0,0,0,0.08),0_1px_0_rgba(255,255,255,0.9)_inset,0_-1px_0_rgba(0,0,0,0.04)_inset] dark:bg-white/[0.06] dark:border-white/10 dark:shadow-[0_2px_8px_rgba(0,0,0,0.4),0_1px_0_rgba(255,255,255,0.06)_inset]"
                        >
                          <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round" className="text-black/50 dark:text-white/50">
                            <line x1="4" x2="20" y1="8" y2="8" /><line x1="4" x2="16" y1="14" y2="14" /><line x1="4" x2="12" y1="20" y2="20" />
                          </svg>
                          <AnimatePresence mode="popLayout" initial={false}>
                            <motion.span
                              key={sortMode}
                              initial={{ y: 10, opacity: 0 }}
                              animate={{ y: 0, opacity: 1 }}
                              exit={{ y: -10, opacity: 0 }}
                              transition={{ duration: 0.2, ease: [0.4, 0, 0.1, 1] }}
                              className="text-[9px] font-black uppercase tracking-[0.1em] text-foreground/70 dark:text-white/60 sm:text-[10px]"
                            >
                              Sort {sortMode}
                            </motion.span>
                          </AnimatePresence>
                        </motion.button>
                        <motion.button type="button" whileTap={{ scale: 0.94 }} onClick={() => setIsCreatingFeed(true)}
                          className="flex items-center justify-center gap-1.5 rounded-[14px] bg-[#CCFF00] px-3 py-2 text-[10px] font-black uppercase tracking-[0.14em] text-black shadow-[inset_0_1px_0_rgba(255,255,255,0.72),0_2px_4px_rgba(167,208,0,0.22),0_8px_18px_-4px_rgba(204,255,0,0.35)] dark:shadow-[0_2px_4px_rgba(204,255,0,0.15),0_8px_24px_-4px_rgba(204,255,0,0.25),0_1px_0_rgba(255,255,255,0.3)_inset] sm:px-3.5 sm:text-[11px]">
                          <Plus size={15} strokeWidth={3} /> Add Feed
                        </motion.button>
                      </motion.div>
                    ) : (
                      <motion.div key="detail-badge" initial={{ opacity: 0, y: 8, scale: 0.96 }} animate={{ opacity: 1, y: 0, scale: 1 }} exit={{ opacity: 0, y: -6, scale: 0.96 }} transition={{ duration: 0.26, ease: APPLE_EASE }}
                        className="shrink-0 rounded-[12px] border border-white/82 bg-white/74 px-2.5 py-1.5 text-[clamp(18px,5vw,28px)] font-black leading-none text-black shadow-[inset_0_1px_0_rgba(255,255,255,0.98),0_4px_10px_rgba(15,23,42,0.06)] dark:text-[#CCFF00] dark:bg-white/[0.05] dark:border-white/10 dark:shadow-[0_3px_12px_rgba(0,0,0,0.4),0_0_22px_rgba(204,255,0,0.15),0_1px_0_rgba(255,255,255,0.06)_inset]">
                        P{composite}
                      </motion.div>
                    )}
                  </AnimatePresence>
                </div>

                {/* Row 2 */}
                <AnimatePresence mode="popLayout" initial={false}>
                  {view === 'detail' ? (
                    <motion.div key="detail-filters" initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.4, ease: [0.4, 0, 0.1, 1] }} className="flex flex-col gap-1.5">
                      <div className="relative flex items-center gap-1 rounded-[14px] border border-white/82 bg-white/74 p-0.5 shadow-[inset_0_2px_4px_rgba(214,223,235,0.34),inset_0_-1px_0_rgba(255,255,255,0.84),0_4px_10px_rgba(15,23,42,0.04)] dark:bg-white/[0.03] dark:border-white/[0.05] dark:shadow-[inset_0_2px_4px_rgba(0,0,0,0.3),inset_0_-1px_0_rgba(255,255,255,0.03)]">
                        {(['4W', '12W', '26W', '52W'] as Timeframe[]).map(tf => (
                          <motion.button key={tf} type="button" onClick={() => { play('snapLock'); setTimeframe(tf); }} whileTap={{ scale: 0.95 }}
                            className={cn('relative flex-1 rounded-[10px] py-1 text-center font-black',
                              tf === timeframe
                                ? 'text-[16px] tracking-[-0.02em] text-black sm:text-[18px] z-10'
                                : 'text-[12px] tracking-[0.04em] text-black/45 sm:text-[13px] dark:text-white/40 z-0'
                            )} style={{ WebkitTapHighlightColor: 'transparent' }}>
                            {tf === timeframe && (
                              <motion.span
                                layoutId="timeframe-pill-bg"
                                className="absolute inset-0 rounded-[10px] border border-[#d7ff57] bg-[#CCFF00] shadow-[inset_0_1px_0_rgba(255,255,255,0.74),inset_0_-2px_4px_rgba(130,156,0,0.18),0_4px_10px_rgba(204,255,0,0.25)] dark:border-[#dfff7a]/30 dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.18),0_4px_20px_rgba(204,255,0,0.2),0_12px_28px_rgba(0,0,0,0.5)]"
                                transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }}
                              />
                            )}
                            <span className="relative z-10">{TIMEFRAME_LABELS[tf]}</span>
                          </motion.button>
                        ))}
                      </div>
                      {handles.length > 0 && (
                        <div className="hide-scrollbar flex items-center gap-2 overflow-x-auto pt-1">
                          <motion.button whileTap={{ scale: 0.97 }} onClick={() => { play('snapLock'); setSelectedHandle('all'); }}
                            className={cn('relative whitespace-nowrap rounded-full px-4 py-1.5 text-[10px] font-black uppercase tracking-[0.1em]',
                              selectedHandle === 'all' ? 'text-black z-10' : 'fm-depth-chip text-foreground/52 dark:text-foreground/40 z-0')}>
                            {selectedHandle === 'all' && (
                              <motion.span layoutId="handle-pill-bg"
                                className="absolute inset-0 rounded-full border border-[#d7ff57] bg-[#CCFF00] shadow-[inset_0_1px_0_rgba(255,255,255,0.74),inset_0_-2px_4px_rgba(130,156,0,0.18),0_5px_12px_rgba(204,255,0,0.28)]"
                                transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }} />
                            )}
                            <span className="relative z-10">All Targets</span>
                          </motion.button>
                          {handles.map(h => (
                            <motion.button key={h} whileTap={{ scale: 0.97 }} onClick={() => { play('snapLock'); setSelectedHandle(h); }}
                              className={cn('relative whitespace-nowrap rounded-full px-4 py-1.5 text-[10px] font-black uppercase tracking-[0.08em]',
                                selectedHandle === h ? 'text-black z-10' : 'fm-depth-chip text-foreground/52 dark:text-foreground/40 z-0')}>
                              {selectedHandle === h && (
                                <motion.span layoutId="handle-pill-bg"
                                  className="absolute inset-0 rounded-full border border-[#d7ff57] bg-[#CCFF00] shadow-[inset_0_1px_0_rgba(255,255,255,0.74),inset_0_-2px_4px_rgba(130,156,0,0.18),0_5px_12px_rgba(204,255,0,0.28)]"
                                  transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }} />
                              )}
                              <span className="relative z-10">@{h}</span>
                            </motion.button>
                          ))}
                        </div>
                      )}
                    </motion.div>
                  ) : (
                    <motion.div
                      key="list-ticker"
                      initial={{ opacity: 0 }}
                      animate={{ opacity: 1 }}
                      exit={{ opacity: 0 }}
                      transition={{ duration: 0.4, ease: [0.4, 0, 0.1, 1] }}
                      className="min-w-0 pointer-events-auto"
                    >
                      <TickerTape items={tickerItems} className="w-full" />
                    </motion.div>
                  )}
                </AnimatePresence>

                {apiError && <div className="text-[10px] font-black uppercase tracking-[0.14em] text-red-600 dark:text-red-400">{apiError}</div>}
              </div>
            </div>

          </div>
        </div>
      </div>

      {/* ═══ CONTENT ═══ */}
      <AnimatePresence mode="popLayout" initial={false}>
        {view === 'list' ? (
          <motion.div key="list"
            initial={{ opacity: 0, y: 10, scale: 0.995 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -8, scale: 0.995 }}
            transition={{ duration: 0.34, ease: APPLE_EASE }}
            className="absolute inset-0 z-10 will-change-[opacity] transform-gpu">
            <div className="hide-scrollbar h-full w-full overflow-y-auto overflow-x-hidden pb-[calc(190px+env(safe-area-inset-bottom))] pt-[calc(168px+env(safe-area-inset-top))] sm:pt-[calc(174px+env(safe-area-inset-top))] md:pt-[208px]"
              style={{ WebkitOverflowScrolling: 'touch' }}>
              <div className="w-full px-3 sm:px-4 lg:px-4">
                <LayoutGroup>
                <div className="fm-tab-content-shell mx-auto grid grid-cols-1 gap-4 min-[720px]:grid-cols-2 lg:gap-5">
                  {sortedFeeds.map((feed, i) => (
                    <motion.div
                      key={feed.id}
                      layout
                      layoutId={`feed-cell-${feed.id}`}
                      transition={{ layout: { type: 'spring', stiffness: 320, damping: 30, mass: 0.8 } }}
                    >
                      <FeedTile title={feed.title} count={feed.feeders.length} anchor={feed.feeders.find(f => f.isAnchor)?.handle}
                        metrics={feed.metrics} onClick={() => handleFeedClick(feed.id)} onDelete={() => handleDeleteFeed(feed.id)} index={i} layoutId={`feed-${feed.id}`} />
                    </motion.div>
                  ))}
                  {feeds.length === 0 && (
                    <div className="col-span-full fm-depth-glass rounded-[28px] px-6 py-10 text-center">
                      <div className="mx-auto flex h-14 w-14 items-center justify-center rounded-[18px] fm-depth-chip">
                        <Target size={26} className="text-foreground/48 dark:text-white/42" />
                      </div>
                      <div className="mt-4 text-[18px] font-black uppercase tracking-[0.14em] text-foreground/72 dark:text-white/66">No feeds yet</div>
                      <p className="mt-2 text-[12px] font-black uppercase tracking-[0.1em] text-foreground/42 dark:text-white/34">Create a feed to start tracking feeder-level momentum</p>
                    </div>
                  )}
                </div>
                </LayoutGroup>
              </div>
            </div>
          </motion.div>
        ) : (
          <motion.div key={`detail-${selectedFeedId}`}
            initial={{ opacity: 0, y: 10, scale: 0.995 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -8, scale: 0.995 }}
            transition={{ duration: 0.34, ease: APPLE_EASE }}
            className="absolute inset-0 z-10 will-change-[opacity] transform-gpu">
            <FeedDetailV2 activeFeed={activeFeed} timeframe={timeframe} dashboardData={dashboardData}>
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
                    onRemove={() => activeFeed && removeFeeder(activeFeed.id, feeder.handle)} />
                ))
              )}
              <motion.div layout initial={{ scale: 0.95 }} animate={{ scale: 1 }} whileTap={{ scale: 0.98 }}
                className={cn("fm-depth-glass rounded-[22px] p-4 min-h-[180px] flex flex-col justify-center items-center group cursor-pointer", isAddingFeeder ? "ring-1 ring-black/10 dark:ring-[#CCFF00]/45" : "")}
                style={{ willChange: 'transform' }} onClick={() => !isAddingFeeder && setIsAddingFeeder(true)}>
                {!isAddingFeeder ? (
                  <div className="flex flex-col items-center gap-2">
                    <Plus size={48} strokeWidth={2} className="text-foreground/30" />
                    <span className="font-black uppercase tracking-[0.2em] text-[10px] text-foreground/40">ADD FEEDER</span>
                    <span className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/30">Uses 1 slot · {slotUsage.used ?? 0} used</span>
                  </div>
                ) : (
                  <div className="w-full h-full flex flex-col justify-between" onClick={e => e.stopPropagation()}>
                    <div className="flex items-center gap-3 w-full">
                      <div className="fm-depth-chip w-12 h-12 rounded-[12px] flex items-center justify-center text-black dark:text-[#CCFF00] font-black text-2xl shrink-0">@</div>
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

      {/* Create Feed Modal */}
      <AnimatePresence>
        {isCreatingFeed && (
          <motion.div initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }} transition={{ duration: 0.22, ease: APPLE_EASE }}
            className="absolute inset-0 z-[120] flex items-center justify-center bg-[rgba(244,247,249,0.5)] px-4 dark:bg-[rgba(3,3,3,0.64)]">
            <motion.div initial={{ y: 20, opacity: 0, scale: 0.985 }} animate={{ y: 0, opacity: 1, scale: 1 }} exit={{ y: 16, opacity: 0, scale: 0.985 }}
              transition={{ duration: 0.28, ease: APPLE_EASE }}
              className="fm-depth-glass relative w-full max-w-[560px] overflow-hidden rounded-[32px] border border-white/85 p-6 shadow-[0_20px_60px_-20px_rgba(15,23,42,0.28)] dark:border-white/10 dark:shadow-[0_30px_80px_-20px_rgba(0,0,0,0.6)]">
              <div className="pointer-events-none absolute inset-0 rounded-[32px] dark:opacity-0"
                style={{ background: 'linear-gradient(180deg, rgba(255,255,255,0.4) 0%, rgba(255,255,255,0) 32%, rgba(0,0,0,0.02) 100%)' }}
              />
              <button type="button" onClick={() => { setIsCreatingFeed(false); setNewFeedName(''); }}
                className="fm-depth-chip absolute right-4 top-4 z-10 flex h-10 w-10 items-center justify-center rounded-[14px] text-foreground/56 dark:text-white/46">
                <X size={16} strokeWidth={2.6} />
              </button>
              <div className="relative z-10 pr-12">
                <div className="text-[11px] font-black uppercase tracking-[0.18em] text-foreground/46 dark:text-white/36">Create Feed</div>
                <div className="mt-2 text-[28px] font-black uppercase tracking-[-0.05em] text-foreground dark:text-white">What are you tracking?</div>
                <p className="mt-2 max-w-[360px] text-[11px] font-black uppercase tracking-[0.1em] text-foreground/38 dark:text-white/32">
                  Create a fresh bundle and start routing feeders into it.
                </p>
              </div>
              <div className="relative z-10 mt-8 rounded-[24px] border border-white/85 bg-white/62 px-5 py-4 shadow-[inset_0_1px_0_rgba(255,255,255,0.92),0_10px_24px_-16px_rgba(15,23,42,0.16)] dark:border-white/8 dark:bg-white/[0.03] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.05),0_16px_32px_-18px_rgba(0,0,0,0.45)]">
                <div className="text-[9px] font-black uppercase tracking-[0.16em] text-foreground/36 dark:text-white/30">Feed Name</div>
                <input autoFocus maxLength={15} value={newFeedName} onChange={e => setNewFeedName(e.target.value.toUpperCase())}
                onKeyDown={e => { if (e.key === 'Enter') handleCreateFeed(); if (e.key === 'Escape') { setIsCreatingFeed(false); setNewFeedName(''); } }}
                placeholder="FEED NAME" className="mt-2 w-full bg-transparent text-[36px] font-black uppercase tracking-[-0.06em] text-foreground outline-none placeholder:text-foreground/18 dark:text-white" />
              </div>
              <div className="relative z-10 mt-5 flex items-center justify-between">
                <span className="text-[10px] font-black uppercase tracking-[0.14em] text-foreground/42 dark:text-white/34">{newFeedName.length} / 15</span>
                <motion.button type="button" whileTap={{ scale: 0.95 }} onClick={handleCreateFeed} disabled={!newFeedName.trim() || isBusy}
                  className="rounded-[16px] bg-black px-4 py-2.5 text-[11px] font-black uppercase tracking-[0.16em] text-white shadow-[0_10px_22px_-10px_rgba(0,0,0,0.5)] disabled:opacity-40 dark:bg-[#CCFF00] dark:text-black dark:shadow-[0_14px_30px_-12px_rgba(204,255,0,0.3)]">
                  {isBusy ? 'Creating' : 'Create Feed'}
                </motion.button>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
      <AnimatePresence>
        {pendingDeleteFeed && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.22, ease: APPLE_EASE }}
            className="absolute inset-0 z-[125] flex items-center justify-center bg-[rgba(244,247,249,0.54)] px-4 dark:bg-[rgba(3,3,3,0.68)]"
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
                      This archives the bundle and removes it from the active Feed view.
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
