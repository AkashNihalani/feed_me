'use client';

import { useCallback, useEffect, useMemo, useState, useRef } from 'react';
import { Loader2 } from 'lucide-react';
import { AnimatePresence, motion } from 'framer-motion';
import ChronoTabs from '@/components/fire/ChronoTabs';
import FluidDeck from '@/components/fire/FluidDeck';
import ZSpaceFilter, { FilterScope, FilterThreshold } from '@/components/fire/ZSpaceFilter';
import { AlertUrgency, FireAlertItem, FireLayers } from '@/components/fire/types';
import { useAppHaptics } from '@/lib/haptics';
import { cn } from '@/lib/utils';
import { getCache, setCache } from '@/lib/pageCache';

type AlertRow = Record<string, unknown>;

const TERMINAL_STATUSES = new Set(['dropped', 'error', 'archived']);

function asRecord(v: unknown): Record<string, unknown> { return v && typeof v === 'object' && !Array.isArray(v) ? v as Record<string, unknown> : {}; }
function asString(v: unknown): string { if (typeof v === 'string') return v; if (typeof v === 'number') return String(v); return ''; }
function asNumber(v: unknown): number | null {
  if (typeof v === 'number' && Number.isFinite(v)) return v;
  if (typeof v === 'string') { const n = Number(v.replace('%', '').trim()); return Number.isFinite(n) ? n : null; }
  return null;
}
function pickBestMetric(metrics: Record<string, unknown>, preferred: string): string {
  const candidates = ['views', 'likes', 'comments'];
  const normalized = preferred.trim().toLowerCase();
  const order = normalized && candidates.includes(normalized)
    ? [normalized, ...candidates.filter((metric) => metric !== normalized)]
    : candidates;

  for (const metric of order) {
    const metricData = asRecord(metrics[metric]);
    if (asNumber(metricData.value) != null) return metric;
  }
  for (const metric of order) {
    const metricData = asRecord(metrics[metric]);
    if (asNumber(metricData.percentile) != null) return metric;
  }
  return order[0] ?? 'views';
}
function compactNumber(v: number | null): string {
  if (v == null || !Number.isFinite(v)) return '?';
  if (Math.abs(v) >= 1e6) return `${(v / 1e6).toFixed(1)}M`;
  if (Math.abs(v) >= 1e3) return `${(v / 1e3).toFixed(1)}K`;
  return String(Math.round(v));
}
function toMediaProxyUrl(url: string): string { const u = url.trim(); return u ? `/api/media?url=${encodeURIComponent(u)}` : ''; }
function timeAgoText(iso: string): string {
  const ts = new Date(iso).getTime(); if (Number.isNaN(ts)) return '';
  const h = Math.floor((Date.now() - ts) / 3600000);
  if (h < 1) return 'just now'; if (h < 24) return `${h}h ago`; return `${Math.floor(h / 24)}d ago`;
}
function percentileToTag(p: number | null): string { if (p == null) return '😴'; if (p <= 5) return '🚀'; if (p <= 20) return '🔥'; if (p <= 35) return '✅'; return '😴'; }
function normalizeMediaLabel(raw: string): string {
  const s = raw.trim().toLowerCase(); if (!s) return 'POST';
  if (s.includes('reel')) return 'REEL'; if (s.includes('sidecar') || s.includes('carousel')) return 'CAROUSEL';
  if (s.includes('image') || s.includes('photo')) return 'IMAGE'; return s.toUpperCase();
}
function toIstDayKey(d: string | Date): string {
  const date = typeof d === 'string' ? new Date(d) : d;
  const fmt = (dt: Date) => new Intl.DateTimeFormat('en-CA', { timeZone: 'Asia/Kolkata', year: 'numeric', month: '2-digit', day: '2-digit' }).format(dt);
  return Number.isNaN(date.getTime()) ? fmt(new Date()) : fmt(date);
}
function inferUrgency(alertType: string, p: number | null): AlertUrgency {
  const t = alertType.toLowerCase();
  if (t === 'blaze') return 'now'; if (t === 'burn') return 'today'; if (t === 'spark') return 'watch';
  if (p != null && p <= 10) return 'now'; if (p != null && p <= 25) return 'today'; return 'watch';
}
function formatDelta(n: number | null, cp: string): string | undefined {
  if (cp.toLowerCase() === 'd1' || n == null) return undefined;
  const x = Math.round(n); return x > 0 ? `+${x}` : x < 0 ? `${x}` : '0';
}

function normalizeAlertRow(row: AlertRow): FireAlertItem | null {
  const payload = asRecord(row.payload);
  const explicit = asRecord(row.intelligence_layers);
  const layers = Object.keys(explicit).length > 0 ? explicit : asRecord(payload.layers);
  const status = asString(row.status).trim().toLowerCase();
  if (status && TERMINAL_STATUSES.has(status)) return null;
  const checkpoint = asString(row.checkpoint) || asString(row.surface_checkpoint) || asString(asRecord(payload.meta).checkpoint) || 'd1';
  const surfaceHandle = asString(row.surface_handle) || asString(row.surface_feeder) || asString(asRecord(payload.meta).handle);
  const surfaceMediaType = normalizeMediaLabel(asString(row.surface_media_type) || asString(row.surface_mediaType) || asString(asRecord(payload.meta).media_type));
  const metrics = asRecord(payload.metrics);
  const bestMetric = pickBestMetric(metrics, asString(payload.best_metric) || asString(row.metric_key) || 'views');
  const bmd = asRecord(metrics[bestMetric]);
  const position = asRecord(payload.position);
  const surfacePercentile = asNumber(position.overall_percentile) ?? asNumber(position.percentile) ?? asNumber(row.surface_percentile) ?? asNumber(bmd.percentile);
  const metricValue = asNumber(bmd.value) ?? asNumber(row.surface_metric_value) ?? asNumber(row.metric_value);
  const surfaceDelta = asNumber(row.surface_delta) ?? asNumber(row.surface_shift);
  const meta = asRecord(payload.meta);
  const businessDateKey = asString(row.captured_business_date_ist) || asString(row.business_date_ist) || asString(meta.captured_business_date_ist) || asString(meta.business_date_ist) || asString(meta.anchor_business_date_ist) || toIstDayKey(asString(row.created_at));
  const title = `@${surfaceHandle ? surfaceHandle.toUpperCase() : 'FEEDER'} · ${surfaceMediaType} · ${checkpoint.toUpperCase()} · ${compactNumber(metricValue)} ${bestMetric.toUpperCase()}`;
  return {
    id: `alert-${asString(row.id) || asString(row.dedupe_key) || Math.random().toString(36).slice(2)}`, postKey: asString(row.post_key), family: 'insight',
    urgency: inferUrgency(asString(row.alert_type), surfacePercentile), color: '#FF6B00', handle: `@${surfaceHandle || 'feed'}`, title,
    whyNow: asString(row.body) || '', action: '', percentileTag: percentileToTag(surfacePercentile), mediaType: surfaceMediaType,
    stage: checkpoint.toUpperCase(), percentile: surfacePercentile == null ? undefined : String(Math.round(surfacePercentile)),
    delta: formatDelta(surfaceDelta, checkpoint), evidence: [], timeAgo: timeAgoText(asString(row.created_at)),
    createdAt: asString(row.created_at), postUrl: asString(meta.post_url ?? payload.post_url) || 'https://instagram.com',
    thumbnailUrl: toMediaProxyUrl(asString(meta.thumbnail_url ?? payload.thumbnail_url)), businessDateKey, businessDateIst: businessDateKey, status,
    surfacePercentile, surfaceDelta, trajectoryDeltaPercentile: surfaceDelta, surfaceHandle, surfaceMediaType,
    checkpoint: checkpoint.toUpperCase(), metricValue, metricKey: bestMetric, payload, layers: layers as FireLayers,
    stamp: { handle: surfaceHandle, mediaType: surfaceMediaType, checkpoint: checkpoint.toUpperCase(), metricLabel: bestMetric.toUpperCase(), metricValue },
  };
}

/* ── API helpers ── */

async function fetchMeta(): Promise<{ days: string[]; scopes: string[] }> {
  const res = await fetch('/api/fire?mode=meta');
  if (!res.ok) throw new Error(`Meta fetch failed: ${res.status}`);
  const data = await res.json();
  return { days: data.days ?? [], scopes: data.scopes ?? [] };
}

async function fetchPage(day: string, scope: string, threshold: string, cursor: number): Promise<{ items: FireAlertItem[]; hasMore: boolean; total: number }> {
  const params = new URLSearchParams({ day, scope, threshold, cursor: String(cursor) });
  const res = await fetch(`/api/fire?${params}`);
  if (!res.ok) { const b = await res.json().catch(() => ({ error: res.statusText })); throw new Error(b.error || `HTTP ${res.status}`); }
  const { rows, hasMore, total } = await res.json();
  const items = (rows as AlertRow[]).map(normalizeAlertRow).filter((r): r is FireAlertItem => r !== null);
  return { items, hasMore: hasMore ?? false, total: total ?? 0 };
}

/* Apple-style deceleration ease */
const APPLE_EASE = [0.32, 0.72, 0, 1] as const;
const FIRE_META_CACHE_KEY = 'fire:meta:v1';
const FIRE_STATE_CACHE_KEY = 'fire:state:v1';
const FIRE_PAGE_CACHE_PREFIX = 'fire:page:v1';
const FIRE_CACHE_TTL = 2 * 60 * 1000;

export default function FirePage() {
  const { play } = useAppHaptics();

  // Meta state
  const [pickerDays, setPickerDays] = useState<string[]>([]);
  const [availableScopes, setAvailableScopes] = useState<string[]>([]);

  // Page state
  const [cards, setCards] = useState<FireAlertItem[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadingMore, setLoadingMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasMore, setHasMore] = useState(false);
  const [total, setTotal] = useState(0);

  // Filter state
  const [selectedDay, setSelectedDay] = useState('');
  const [isZSpaceOpen, setIsZSpaceOpen] = useState(false);
  const [activeThreshold, setActiveThreshold] = useState<FilterThreshold>('ALL');
  const [activeScope, setActiveScope] = useState<FilterScope>('ALL');

  // Cursor for pagination
  const cursorRef = useRef(0);
  // Track current fetch params to prevent stale appends
  const fetchKeyRef = useRef('');

  useEffect(() => {
    const cachedState = getCache<{
      pickerDays: string[];
      availableScopes: string[];
      selectedDay: string;
      activeThreshold: FilterThreshold;
      activeScope: FilterScope;
      cards: FireAlertItem[];
      hasMore: boolean;
      total: number;
      cursor: number;
    }>(FIRE_STATE_CACHE_KEY, FIRE_CACHE_TTL);
    if (!cachedState) return;
    setPickerDays(cachedState.pickerDays || []);
    setAvailableScopes(cachedState.availableScopes || []);
    setSelectedDay(cachedState.selectedDay || '');
    setActiveThreshold(cachedState.activeThreshold || 'ALL');
    setActiveScope(cachedState.activeScope || 'ALL');
    setCards(cachedState.cards || []);
    setHasMore(Boolean(cachedState.hasMore));
    setTotal(cachedState.total || 0);
    cursorRef.current = cachedState.cursor || (cachedState.cards || []).length;
    setLoading(false);
  }, []);

  useEffect(() => {
    if (!selectedDay) return;
    setCache(FIRE_STATE_CACHE_KEY, {
      pickerDays,
      availableScopes,
      selectedDay,
      activeThreshold,
      activeScope,
      cards,
      hasMore,
      total,
      cursor: cursorRef.current,
    });
  }, [pickerDays, availableScopes, selectedDay, activeThreshold, activeScope, cards, hasMore, total]);

  useEffect(() => { document.title = 'Fire'; }, []);

  // PWA padding
  useEffect(() => {
    const updatePwaPad = () => {
      const isStandalone = window.matchMedia('(display-mode: standalone)').matches || (window.navigator as any).standalone;
      const vv = window.visualViewport;
      const offset = Math.max(0, Math.round(vv?.offsetTop ?? 0));
      const pad = offset + 8;
      document.documentElement.style.setProperty('--pwa-top-pad', isStandalone ? `${pad}px` : '0px');
      document.documentElement.style.setProperty('--pwa-top-fix', isStandalone ? `${-(offset + 6)}px` : '0px');
      document.documentElement.style.setProperty('--pwa-bottom-pad', isStandalone ? '-24px' : '0px');
    };
    updatePwaPad();
    const mql = window.matchMedia('(display-mode: standalone)');
    const handler = () => updatePwaPad();
    mql.addEventListener?.('change', handler);
    window.addEventListener('resize', handler);
    return () => {
      mql.removeEventListener?.('change', handler);
      window.removeEventListener('resize', handler);
    };
  }, []);

  // ─── Fetch metadata on mount + periodic refresh ────────────
  const refreshMeta = useCallback(async () => {
    const cached = getCache<{ days: string[]; scopes: string[] }>(FIRE_META_CACHE_KEY, FIRE_CACHE_TTL);
    if (cached) {
      setAvailableScopes(cached.scopes ?? []);
      if ((cached.days ?? []).length > 0) {
        setPickerDays((prevDays) => {
          const prevTop = prevDays[0] || '';
          setSelectedDay((current) => {
            if (!current) return cached.days[0];
            if (!cached.days.includes(current)) return cached.days[0];
            if (current === prevTop) return cached.days[0];
            return current;
          });
          return cached.days;
        });
      }
      return;
    }
    const meta = await fetchMeta();
    setCache(FIRE_META_CACHE_KEY, meta);
    setAvailableScopes(meta.scopes);
    if (meta.days.length > 0) {
      setPickerDays((prevDays) => {
        const prevTop = prevDays[0] || '';
        setSelectedDay((current) => {
          if (!current) return meta.days[0];
          if (!meta.days.includes(current)) return meta.days[0];
          if (current === prevTop) return meta.days[0];
          return current;
        });
        return meta.days;
      });
    }
  }, []);

  useEffect(() => {
    let mounted = true;
    const run = async () => {
      try {
        await refreshMeta();
      } catch (err) {
        if (!mounted) return;
        console.error('[FirePage] Meta fetch error:', err);
        setError(err instanceof Error ? err.message : 'Failed to load');
        setLoading(false);
      }
    };

    run();

    const intervalId = window.setInterval(() => {
      run();
    }, 60_000);

    const onVisibility = () => {
      if (document.visibilityState === 'visible') run();
    };
    window.addEventListener('focus', run);
    document.addEventListener('visibilitychange', onVisibility);

    return () => {
      mounted = false;
      window.clearInterval(intervalId);
      window.removeEventListener('focus', run);
      document.removeEventListener('visibilitychange', onVisibility);
    };
  }, [refreshMeta]);

  // ─── Fetch first page when day/filters change ─────────────
  useEffect(() => {
    if (!selectedDay) return;
    let mounted = true;
    const key = `${selectedDay}:${activeScope}:${activeThreshold}`;
    const cacheKey = `${FIRE_PAGE_CACHE_PREFIX}:${key}`;
    fetchKeyRef.current = key;
    const cached = getCache<{ items: FireAlertItem[]; hasMore: boolean; total: number; cursor: number }>(cacheKey, FIRE_CACHE_TTL);
    if (cached) {
      setCards(cached.items);
      setHasMore(cached.hasMore);
      setTotal(cached.total);
      cursorRef.current = cached.cursor;
      setLoading(false);
      setError(null);
      return () => { mounted = false; };
    }

    cursorRef.current = 0;
    setLoading(true);
    setCards([]);
    setHasMore(false);
    setError(null);

    (async () => {
      try {
        const result = await fetchPage(selectedDay, activeScope, activeThreshold, 0);
        if (!mounted || fetchKeyRef.current !== key) return;
        setCards(result.items);
        setHasMore(result.hasMore);
        setTotal(result.total);
        cursorRef.current = result.items.length;
        setCache(cacheKey, {
          items: result.items,
          hasMore: result.hasMore,
          total: result.total,
          cursor: result.items.length,
        });
      } catch (err) {
        if (!mounted || fetchKeyRef.current !== key) return;
        setError(err instanceof Error ? err.message : 'Failed to load alerts');
      } finally {
        if (mounted && fetchKeyRef.current === key) setLoading(false);
      }
    })();

    return () => { mounted = false; };
  }, [selectedDay, activeScope, activeThreshold]);

  // ─── Load next page (called by FluidDeck sentinel) ────────
  const handleLoadMore = useCallback(async () => {
    if (loadingMore || !hasMore) return;
    setLoadingMore(true);
    const key = fetchKeyRef.current;
    try {
      const result = await fetchPage(selectedDay, activeScope, activeThreshold, cursorRef.current);
      if (fetchKeyRef.current !== key) return; // filters changed while loading
      setCards(prev => {
        const nextCards = [...prev, ...result.items];
        setCache(`${FIRE_PAGE_CACHE_PREFIX}:${key}`, {
          items: nextCards,
          hasMore: result.hasMore,
          total: result.total,
          cursor: nextCards.length,
        });
        return nextCards;
      });
      setHasMore(result.hasMore);
      setTotal(result.total);
      cursorRef.current += result.items.length;
    } catch (err) {
      console.error('[FirePage] Load more error:', err);
    } finally {
      setLoadingMore(false);
    }
  }, [loadingMore, hasMore, selectedDay, activeScope, activeThreshold]);

  return (
    <motion.div
      initial={{ opacity: 0, y: 14, scale: 0.992 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ duration: 0.42, ease: APPLE_EASE }}
      className="relative h-[100svh] w-full overflow-hidden bg-background text-foreground select-none md:h-[100dvh]">
      {/* Ambient bg */}
      <div className="pointer-events-none fixed inset-0 z-0 bg-white dark:bg-[#030303]" />

      <ZSpaceFilter isOpen={isZSpaceOpen} onClose={() => setIsZSpaceOpen(false)} activeThreshold={activeThreshold}
        activeScope={activeScope} availableScopes={availableScopes} onChange={(t, s) => { setActiveThreshold(t); setActiveScope(s); }} />

      <div className="h-full w-full contain-paint">
        {/* ═══ HEADER ═══ */}
        <div className="pointer-events-auto absolute inset-x-0 top-0 z-[100] flex flex-col items-center px-2 pt-[calc(10px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-4 sm:pt-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] md:pt-[calc(20px+var(--pwa-top-fix,0px))] lg:px-4">
          <div className="relative fm-app-shell">
            <div className={cn(
              'w-full pointer-events-auto overflow-hidden rounded-[32px] relative',
              'bg-white/65 backdrop-blur-[40px] backdrop-saturate-[180%]',
              'border border-white/80 border-t-white/90',
              'shadow-[0_1px_0_rgba(255,255,255,0.95)_inset,0_-1px_0_rgba(0,0,0,0.03)_inset,0_4px_8px_rgba(0,0,0,0.03),0_12px_28px_-4px_rgba(0,0,0,0.08),0_32px_64px_-12px_rgba(0,0,0,0.1),0_48px_96px_-16px_rgba(0,0,0,0.06)]',
              'dark:bg-[rgba(6,6,6,0.65)] dark:border-white/[0.07] dark:border-t-white/[0.12]',
              'dark:shadow-[0_1px_0_rgba(255,255,255,0.06)_inset,0_-1px_0_rgba(0,0,0,0.5)_inset,0_8px_16px_rgba(0,0,0,0.4),0_24px_48px_-8px_rgba(0,0,0,0.6),0_48px_96px_-16px_rgba(0,0,0,0.5)]',
            )}>
              {/* Inner bevel highlight for neumorphic depth */}
              <div className="pointer-events-none absolute inset-0 rounded-[32px] z-0 dark:opacity-0 transition-opacity"
                style={{
                  background: 'linear-gradient(180deg, rgba(255,255,255,0.45) 0%, rgba(255,255,255,0) 30%, rgba(0,0,0,0.015) 100%)',
                }}
              />
              <div className="pointer-events-none absolute inset-[1px] rounded-[31px] z-0 dark:hidden"
                style={{
                  boxShadow: 'inset 0 2px 4px rgba(255,255,255,0.7), inset 0 -2px 6px rgba(0,0,0,0.04)',
                }}
              />
              <div className="relative z-10 px-3.5 py-3 sm:px-5 sm:py-3.5">
                <div className="flex flex-col gap-2 sm:gap-2.5">
                  <div className="flex items-center justify-between gap-3">
                    <h1 className="shrink-0 text-[30px] font-black leading-none tracking-[0.14em] text-black drop-shadow-none sm:text-[38px] dark:text-white fm-depth-title">
                      FIRE
                    </h1>
                    <motion.button whileTap={{ scale: 0.92 }} onClick={() => { play('snapLock'); setIsZSpaceOpen(true); }}
                      className={cn(
                        'relative flex shrink-0 items-center justify-center rounded-[14px] p-2 transition-colors',
                        'bg-white/60 border border-white/70 shadow-[0_2px_6px_rgba(0,0,0,0.08),0_1px_0_rgba(255,255,255,0.9)_inset,0_-1px_0_rgba(0,0,0,0.04)_inset]',
                        'dark:bg-white/[0.06] dark:border-white/10 dark:shadow-[0_2px_8px_rgba(0,0,0,0.4),0_1px_0_rgba(255,255,255,0.06)_inset]',
                        activeThreshold !== 'ALL' || activeScope !== 'ALL'
                          ? 'text-lime dark:text-lime dark:drop-shadow-[0_0_8px_rgba(204,255,0,0.3)]'
                          : 'text-black/58 dark:text-white/45',
                      )}>
                      <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.5" strokeLinecap="round" strokeLinejoin="round">
                        <line x1="4" x2="20" y1="21" y2="21" /><line x1="4" x2="20" y1="14" y2="14" /><line x1="4" x2="20" y1="7" y2="7" />
                      </svg>
                    </motion.button>
                  </div>
                  <div className="min-w-0 pointer-events-auto">
                    <ChronoTabs days={pickerDays} activeDay={selectedDay} onChange={setSelectedDay} />
                  </div>
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* ═══ CONTENT ═══ */}
        <div className="h-full w-full">
          {loading ? (
            <div className="flex h-full w-full items-center justify-center"><Loader2 className="h-12 w-12 animate-spin text-lime" /></div>
          ) : error ? (
            <div className="flex h-full w-full items-center justify-center px-6 text-center">
              <div className="rounded-2xl border border-red-400/40 bg-red-500/10 px-6 py-5 text-sm font-semibold tracking-wide text-red-600 dark:text-red-300">FIRE DATA UNAVAILABLE: {error}</div>
            </div>
          ) : (
            <div className="mx-auto h-full w-full">
              <AnimatePresence mode="popLayout" initial={false}>
                <motion.div key={`${selectedDay}-${activeScope}-${activeThreshold}`}
                  initial={{ opacity: 0, y: 10, scale: 0.996 }} animate={{ opacity: 1, y: 0, scale: 1 }} exit={{ opacity: 0, y: -8, scale: 0.996 }}
                  transition={{ duration: 0.3, ease: APPLE_EASE }} className="h-full will-change-transform transform-gpu">
                  {cards.length === 0 ? (
                    <div className="flex h-full w-full items-center justify-center px-6 text-center">
                      <div className="rounded-2xl border border-white/30 bg-white/20 px-6 py-4 text-xs font-black uppercase tracking-[0.2em] text-foreground/70 dark:border-white/14 dark:bg-black/24">
                        No alerts for this day
                      </div>
                    </div>
                  ) : (
                    <FluidDeck
                      cards={cards}
                      hasMore={hasMore}
                      loadingMore={loadingMore}
                      onLoadMore={handleLoadMore}
                    />
                  )}
                </motion.div>
              </AnimatePresence>
            </div>
          )}
        </div>
      </div>

      {process.env.NODE_ENV !== 'production' && (
        <div className="pointer-events-none absolute bottom-3 left-3 z-[140] rounded-lg border border-white/30 bg-black/55 px-2.5 py-2 text-[10px] font-mono leading-tight text-lime/95 dark:border-white/20">
          <div>cards: {cards.length} / {total}</div>
          <div>day: {selectedDay || '--'}</div>
          <div>has more: {hasMore ? 'yes' : 'no'}</div>
        </div>
      )}
    </motion.div>
  );
}
