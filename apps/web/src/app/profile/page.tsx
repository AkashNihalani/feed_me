'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { getSupabase, User } from '@/lib/supabase';
import { cn } from '@/lib/utils';
import { getCache, setCache } from '@/lib/pageCache';
import { useAppHaptics } from '@/lib/haptics';
import { HEADER_STAGGER_CONTAINER, HEADER_ROW } from '@/lib/motion';
import {
  FUND_ALERT_THRESHOLD_KEY,
  PWA_NOTIFICATION_ENABLED_KEY,
  clampAlertThreshold,
  readBooleanFlag,
  readFundAlertThreshold,
} from '@/lib/fireAlertSettings';
import {
  ensureServiceWorkerRegistration,
  getCurrentPushSubscription,
  subscribeToWebPush,
  unsubscribeFromWebPush,
} from '@/lib/webPush';
import { useRouter } from 'next/navigation';
import {
  ArrowUpRight,
  Bell,
  Bug,
  Check,
  CreditCard,
  FileText,
  LifeBuoy,
  Lock,
  X,
  Moon,
  Sun,
  Target,
  Unlock,
} from 'lucide-react';
import FeedPassCard from '@/components/profile/FeedPassCard';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';

type Metrics = { likes: string; comments: string; views: string; postsTracked: string };

type Feeder = {
  handle: string;
  isAnchor: boolean;
  profilePicUrl?: string | null;
  followerCount?: number | null;
  verificationStatus?: 'pending' | 'verified' | 'failed';
  metrics: Metrics;
};

type Feed = {
  id: string;
  title: string;
  feeders: Feeder[];
  metrics: Metrics;
};

type SlotUsage = {
  used: number;
  handles?: string[];
  limit?: number | null;
  plan?: { price: number; postsCap: number; packPrice: number; packSize: number };
};

type EngineJob = {
  id: number;
  handle: string;
  jobType: string;
  status: string;
  attempt: number;
  createdAt: string;
  updatedAt: string;
  lastError: string | null;
};

type EngineRunBatchBreakdown = {
  label: string;
  count: number;
};

type EngineRunBatch = {
  id: string;
  headline: string;
  windowStart: string;
  windowEnd: string;
  totalRuns: number;
  creatorCount: number;
  systemCount: number;
  businessStart: string | null;
  businessEnd: string | null;
  types: EngineRunBatchBreakdown[];
  statuses: EngineRunBatchBreakdown[];
};

type EngineStats = {
  recentJobs: EngineJob[];
  totalFeeders: number;
  totalPosts: number;
  jobStats: { done: number; failed: number; pending: number; running: number };
  queuedBatches: EngineRunBatch[];
  completedBatches: EngineRunBatch[];
  queuedRuns: EngineRun[];
  completedRuns: EngineRun[];
};

type EngineRun = {
  id: string;
  kind: string;
  label: string;
  checkpoint?: string;
  businessDay?: string;
  mediaType?: string;
  handle?: string;
  status: string;
  scheduledAt: string;
  completedAt?: string;
};

type FundFireRow = {
  id: string | number;
  checkpoint?: string | null;
  surface_percentile?: number | null;
  surface_percentile_exact?: number | null;
  feed_id?: number | string | null;
  payload?: Record<string, unknown> | null;
};

type FundFireSnapshot = {
  day: string;
  total: number;
  rows: FundFireRow[];
};

const emptyStats: EngineStats = {
  recentJobs: [],
  totalFeeders: 0,
  totalPosts: 0,
  jobStats: { done: 0, failed: 0, pending: 0, running: 0 },
  queuedBatches: [],
  completedBatches: [],
  queuedRuns: [],
  completedRuns: [],
};

const emptyFireSnapshot: FundFireSnapshot = {
  day: '',
  total: 0,
  rows: [],
};

const APPLE_EASE = [0.32, 0.72, 0, 1] as const;
const FUND_CACHE_KEY = 'fund:bundle:v1';
const FUND_CACHE_TTL = 2 * 60 * 1000;

const staggerContainer = {
  hidden: { opacity: 0 },
  visible: {
    opacity: 1,
    transition: { staggerChildren: 0.06, delayChildren: 0.06 },
  },
};

const tileVariant = {
  hidden: { y: 16, opacity: 0, scale: 0.98 },
  visible: {
    y: 0,
    opacity: 1,
    scale: 1,
    transition: { duration: 0.42, ease: [0.32, 0.72, 0, 1] as [number, number, number, number] },
  },
};

const FIRE_RING_WINDOW_MS = 60 * 60 * 1000;
const TRACKING_CHECKPOINT_ORDER = ['D1', 'D3', 'D7', 'D21'] as const;

function formatCountdownDuration(ms: number | null) {
  if (ms == null) return '--';
  if (ms <= 0) return 'now';
  if (ms < 60_000) {
    const seconds = Math.max(0, Math.min(59, Math.ceil(ms / 1000)));
    return `0:${String(seconds).padStart(2, '0')}`;
  }
  const totalMinutes = Math.ceil(ms / 60000);
  if (totalMinutes < 60) return `${totalMinutes}m`;

  const hours = Math.floor(totalMinutes / 60);
  const minutes = totalMinutes % 60;
  return minutes > 0 ? `${hours}h ${minutes}m` : `${hours}h`;
}

function decomposeCountdown(ms: number | null) {
  if (ms == null) return null;
  const total = Math.max(0, Math.floor(ms / 1000));
  const h = Math.floor(total / 3600);
  const m = Math.floor((total % 3600) / 60);
  const s = total % 60;
  return { h, m, s, total };
}

function istHourOf(value: string | null | undefined): number | null {
  if (!value) return null;
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return null;
  const hourStr = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Kolkata',
    hour: 'numeric',
    hour12: false,
  }).format(date);
  const hour = parseInt(hourStr, 10);
  return Number.isFinite(hour) ? hour % 24 : null;
}

function istDayProgress(now: number): number {
  const parts = new Intl.DateTimeFormat('en-US', {
    timeZone: 'Asia/Kolkata',
    hour: 'numeric',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  }).formatToParts(new Date(now));
  let h = 0;
  let m = 0;
  let s = 0;
  for (const part of parts) {
    if (part.type === 'hour') h = parseInt(part.value, 10) % 24;
    else if (part.type === 'minute') m = parseInt(part.value, 10);
    else if (part.type === 'second') s = parseInt(part.value, 10);
  }
  return Math.min(1, Math.max(0, (h * 3600 + m * 60 + s) / 86400));
}

function istClockLabel(now: number): string {
  return new Intl.DateTimeFormat('en-GB', {
    timeZone: 'Asia/Kolkata',
    hour: '2-digit',
    minute: '2-digit',
    hour12: false,
  }).format(new Date(now));
}

function formatIstDayKey(value: Date | string) {
  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) return '';
  return new Intl.DateTimeFormat('en-CA', {
    timeZone: 'Asia/Kolkata',
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
  }).format(date);
}

function parseTimestampMs(value: string | null | undefined): number | null {
  if (!value) return null;
  const stamp = new Date(value).getTime();
  return Number.isFinite(stamp) ? stamp : null;
}

function summarizeBatchTypes(types: EngineRunBatchBreakdown[] | undefined, limit = 4) {
  if (!types?.length) return '';

  const counts = new Map<string, number>();
  for (const entry of types) {
    const label = String(entry.label || '').toUpperCase();
    const count = Number(entry.count || 0);
    if (!label || count <= 0) continue;
    counts.set(label, (counts.get(label) || 0) + count);
  }

  return TRACKING_CHECKPOINT_ORDER
    .map((label) => ({ label, count: counts.get(label) || 0 }))
    .filter((entry) => entry.count > 0)
    .slice(0, limit)
    .map((entry) => `${entry.count} ${entry.label}`)
    .join(' · ');
}

function parseMetric(value: string | number | undefined) {
  if (typeof value === 'number') return value;
  if (!value) return 0;
  const raw = String(value).replace(/,/g, '');
  const n = Number(raw);
  return Number.isFinite(n) ? n : 0;
}

function nullableNumber(value: unknown): number | null {
  if (typeof value === 'number' && Number.isFinite(value)) return value;
  if (typeof value === 'string' && value.trim()) {
    const parsed = Number(value);
    return Number.isFinite(parsed) ? parsed : null;
  }
  return null;
}

function firePercentile(row: FundFireRow): number | null {
  return nullableNumber(row.surface_percentile_exact) ?? nullableNumber(row.surface_percentile);
}

async function fetchFundFireSnapshot(day: string): Promise<FundFireSnapshot> {
  const rows: FundFireRow[] = [];
  let total = 0;
  let cursor = 0;

  for (let page = 0; page < 12; page += 1) {
    const params = new URLSearchParams({
      mode: 'page',
      day,
      threshold: 'ALL',
      sort: 'best',
      cursor: String(cursor),
      pageSize: '30',
    });
    const response = await fetch(`/api/fire?${params.toString()}`);
    if (!response.ok) {
      throw new Error(`Fire snapshot fetch failed: ${response.status}`);
    }

    const data = await response.json() as {
      rows?: FundFireRow[];
      hasMore?: boolean;
      total?: number;
    };

    const batch = Array.isArray(data.rows) ? data.rows : [];
    rows.push(...batch);
    total = Number.isFinite(Number(data.total)) ? Number(data.total) : total;

    if (!data.hasMore || batch.length === 0) break;
    cursor += batch.length;
  }

  return {
    day,
    total: total || rows.length,
    rows,
  };
}

type BrowserNotificationPermission = NotificationPermission | 'unsupported';
type NotificationSettingsResponse = {
  error?: string;
  fireAlertThreshold?: number;
  hasActiveSubscription?: boolean;
  pwaPushEnabled?: boolean;
};

type BillingLineItem = {
  handle: string;
  posts: number;
  excess: number;
  cost: number;
};

// Deep Hardware Toggle (Signature Deep Neumorphism - Solid Neon)
function HardwareToggle({ active }: { active: boolean }) {
  return (
    <div
      className={cn(
        'relative flex h-[32px] w-[56px] shrink-0 items-center rounded-full p-[3px] transition-all duration-300',
        active
          ? [
              'bg-[#E11D48]',
              'shadow-[inset_0_3px_6px_rgba(136,19,55,0.55),inset_0_-1px_2px_rgba(255,255,255,0.35),0_2px_6px_rgba(225,29,72,0.15)]',
              'dark:shadow-[inset_0_3px_8px_rgba(0,0,0,0.5),inset_0_-1px_2px_rgba(255,255,255,0.15),0_0_16px_rgba(225,29,72,0.2),0_4px_12px_rgba(0,0,0,0.4)]',
            ]
          : [
              'bg-black/[0.08] dark:bg-black/90',
              'border border-black/[0.08] dark:border-white/[0.06]',
              'shadow-[inset_0_3px_8px_rgba(0,0,0,0.12),inset_0_-1px_1px_rgba(255,255,255,0.5)]',
              'dark:shadow-[inset_0_4px_10px_rgba(0,0,0,0.8),inset_0_-1px_1px_rgba(255,255,255,0.04),0_2px_6px_rgba(0,0,0,0.5)]',
            ]
      )}
    >
      <motion.div
        layout
        transition={{ type: 'spring', stiffness: 500, damping: 30, mass: 0.7 }}
        className={cn(
          'relative z-10 h-[22px] w-[22px] rounded-full',
          active
            ? [
                'bg-gradient-to-b from-[#111] to-black ml-[24px]',
                'shadow-[0_3px_10px_rgba(0,0,0,0.4),inset_0_1px_2px_rgba(255,255,255,0.2),inset_0_-2px_3px_rgba(0,0,0,0.5)]',
                'dark:shadow-[0_3px_12px_rgba(0,0,0,0.7),inset_0_1px_2px_rgba(255,255,255,0.15),inset_0_-2px_4px_rgba(0,0,0,0.6)]',
              ]
            : [
                'bg-gradient-to-b from-[#f5f5f5] to-[#ddd] dark:from-[#3a3a3a] dark:to-[#222] ml-0',
                'shadow-[0_2px_8px_rgba(0,0,0,0.2),inset_0_1px_2px_rgba(255,255,255,0.8),inset_0_-2px_3px_rgba(0,0,0,0.1)]',
                'dark:shadow-[0_3px_10px_rgba(0,0,0,0.7),inset_0_1px_2px_rgba(255,255,255,0.1),inset_0_-2px_4px_rgba(0,0,0,0.5)]',
              ]
        )}
      />
    </div>
  );
}

export default function FundPage() {
  const router = useRouter();
  const { play } = useAppHaptics();
  const { appShellStyle, isStandaloneMode, useBrowserPageScroll, useTranslucentBrowserChrome } = useMobileImmersiveViewport();
  const mobileBottomClearance = useTranslucentBrowserChrome
    ? 'calc(18px + env(safe-area-inset-bottom))'
    : 'calc(170px + env(safe-area-inset-bottom))';

  const [isDarkMode, setIsDarkMode] = useState(true);
  const [alertThreshold, setAlertThreshold] = useState(25);
  const [isDraggingSlider, setIsDraggingSlider] = useState(false);
  const [thresholdLocked, setThresholdLocked] = useState(true);
  const lockRafRef = useRef<number | null>(null);
  const lockStartRef = useRef<number>(0);
  const [themeRipple, setThemeRipple] = useState<{ active: boolean; x: number; y: number; toDark: boolean } | null>(null);
  const themeToggleRef = useRef<HTMLDivElement | null>(null);
  const [primedBundleId, setPrimedBundleId] = useState<string | null>(null);
  const [feeds, setFeeds] = useState<Feed[]>([]);
  const [slots, setSlots] = useState<SlotUsage>({ used: 0 });
  const [engineStats, setEngineStats] = useState<EngineStats>(emptyStats);
  const [fireSnapshot, setFireSnapshot] = useState<FundFireSnapshot>(emptyFireSnapshot);
  const [fireSignalsLoading, setFireSignalsLoading] = useState(true);
  const [activityNow, setActivityNow] = useState(() => Date.now());
  const [pwaNotificationsEnabled, setPwaNotificationsEnabled] = useState(false);
  const [notificationBusy, setNotificationBusy] = useState(false);
  const [notificationPermission, setNotificationPermission] = useState<BrowserNotificationPermission>('default');
  const [notificationPrefsReady, setNotificationPrefsReady] = useState(false);
  const [notificationTestBusy, setNotificationTestBusy] = useState(false);
  const [notificationTestNotice, setNotificationTestNotice] = useState<string | null>(null);
  const [showManageSubscriptionModal, setShowManageSubscriptionModal] = useState(false);

  // Launch sequence states removed

  const syncPwaNotificationState = useCallback(() => {
    if (typeof window === 'undefined') return;
    if (!('Notification' in window) || !('serviceWorker' in navigator)) {
      setNotificationPermission('unsupported');
      setPwaNotificationsEnabled(false);
      return;
    }

    setNotificationPermission(Notification.permission);
    setPwaNotificationsEnabled(
      Notification.permission === 'granted' &&
      readBooleanFlag(window.localStorage, PWA_NOTIFICATION_ENABLED_KEY)
    );
  }, []);

  const readApiError = useCallback(async (response: Response, fallback: string) => {
    try {
      const data = await response.json() as { error?: string };
      return data.error || fallback;
    } catch {
      return fallback;
    }
  }, []);

  useEffect(() => {
    const localThreshold = readFundAlertThreshold(window.localStorage);
    setAlertThreshold(localThreshold);
    syncPwaNotificationState();

    let cancelled = false;
    const bootstrapNotificationSettings = async () => {
      try {
        const localSubscription = ('Notification' in window && Notification.permission === 'granted')
          ? await getCurrentPushSubscription().catch(() => null)
          : null;
        const response = await fetch('/api/profile/notifications', { cache: 'no-store' });
        if (!response.ok) return;

        const data = await response.json() as NotificationSettingsResponse;
        const nextThreshold = clampAlertThreshold(
          Number(data.fireAlertThreshold ?? localThreshold),
          localThreshold
        );
        const nextEnabled = Boolean(data.pwaPushEnabled) && Boolean(localSubscription) && Notification.permission === 'granted';

        window.localStorage.setItem(FUND_ALERT_THRESHOLD_KEY, String(nextThreshold));
        window.localStorage.setItem(PWA_NOTIFICATION_ENABLED_KEY, nextEnabled ? 'true' : 'false');

        if (!cancelled) {
          setAlertThreshold(nextThreshold);
          setPwaNotificationsEnabled(nextEnabled);
        }
      } catch (error) {
        console.error('[fund] Failed to bootstrap notification settings', error);
      } finally {
        if (!cancelled) {
          setNotificationPrefsReady(true);
        }
      }
    };

    bootstrapNotificationSettings().catch(() => {
      if (!cancelled) setNotificationPrefsReady(true);
    });

    const handleStorage = (event: StorageEvent) => {
      if (!event.key || event.key === FUND_ALERT_THRESHOLD_KEY) {
        setAlertThreshold(readFundAlertThreshold(window.localStorage));
      }
      if (!event.key || event.key === PWA_NOTIFICATION_ENABLED_KEY) {
        syncPwaNotificationState();
      }
    };
    const handleVisibility = () => {
      if (document.visibilityState === 'visible') syncPwaNotificationState();
    };

    window.addEventListener('storage', handleStorage);
    window.addEventListener('focus', syncPwaNotificationState);
    document.addEventListener('visibilitychange', handleVisibility);
    return () => {
      cancelled = true;
      window.removeEventListener('storage', handleStorage);
      window.removeEventListener('focus', syncPwaNotificationState);
      document.removeEventListener('visibilitychange', handleVisibility);
    };
  }, [syncPwaNotificationState]);

  useEffect(() => {
    if (!notificationPrefsReady || typeof window === 'undefined') return;
    window.localStorage.setItem(FUND_ALERT_THRESHOLD_KEY, String(alertThreshold));

    const timer = window.setTimeout(async () => {
      try {
        const response = await fetch('/api/profile/notifications', {
          body: JSON.stringify({ fireAlertThreshold: alertThreshold }),
          headers: { 'Content-Type': 'application/json' },
          method: 'PUT',
        });

        if (!response.ok) {
          throw new Error(await readApiError(response, 'Failed to save fire alert threshold'));
        }
      } catch (error) {
        console.error('[fund] Failed to persist fire alert threshold', error);
      }
    }, 350);

    return () => window.clearTimeout(timer);
  }, [alertThreshold, notificationPrefsReady, readApiError]);

  useEffect(() => {
    let frame: number | null = null;
    let last = 0;
    const tick = (now: number) => {
      if (now - last >= 1000) {
        last = now;
        setActivityNow(Date.now());
      }
      frame = window.requestAnimationFrame(tick);
    };
    frame = window.requestAnimationFrame(tick);
    const onVisibility = () => {
      if (document.hidden) {
        if (frame != null) window.cancelAnimationFrame(frame);
        frame = null;
      } else if (frame == null) {
        last = 0;
        frame = window.requestAnimationFrame(tick);
      }
    };
    document.addEventListener('visibilitychange', onVisibility);
    return () => {
      if (frame != null) window.cancelAnimationFrame(frame);
      document.removeEventListener('visibilitychange', onVisibility);
    };
  }, []);

  const currentFireDay = useMemo(() => formatIstDayKey(new Date(activityNow)), [activityNow]);
  const dayProgress = useMemo(() => istDayProgress(activityNow), [activityNow]);
  const nowClockLabel = useMemo(() => istClockLabel(activityNow), [activityNow]);

  useEffect(() => {
    let cancelled = false;

    const loadFireSnapshot = async (quiet = false) => {
      if (!quiet) setFireSignalsLoading(true);
      try {
        const snapshot = await fetchFundFireSnapshot(currentFireDay);
        if (!cancelled) {
          setFireSnapshot(snapshot);
        }
      } catch (error) {
        console.error('[fund] Failed to fetch fire snapshot', error);
      } finally {
        if (!cancelled) {
          setFireSignalsLoading(false);
        }
      }
    };

    loadFireSnapshot().catch(() => {});
    const refreshSnapshot = () => {
      loadFireSnapshot(true).catch(() => {});
    };
    const handleFocus = () => refreshSnapshot();
    const handleVisibility = () => {
      if (!document.hidden) refreshSnapshot();
    };

    const refreshTimer = window.setInterval(refreshSnapshot, 60 * 1000);
    window.addEventListener('focus', handleFocus);
    document.addEventListener('visibilitychange', handleVisibility);

    return () => {
      cancelled = true;
      window.clearInterval(refreshTimer);
      window.removeEventListener('focus', handleFocus);
      document.removeEventListener('visibilitychange', handleVisibility);
    };
  }, [currentFireDay]);

  useEffect(() => {
    let cancelled = false;

    const loadEngineStats = async () => {
      try {
        const res = await fetch('/api/profile/engine', { cache: 'no-store' });
        if (!res.ok) return;
        const data = await res.json() as EngineStats;
        if (cancelled) return;
        setEngineStats(data);

        const cached = getCache<{
          feeds: Feed[];
          slots: SlotUsage;
          engineStats: EngineStats;
          user: Partial<User> | null;
        }>(FUND_CACHE_KEY, FUND_CACHE_TTL);

        setCache(FUND_CACHE_KEY, {
          feeds: cached?.feeds || feeds,
          slots: cached?.slots || slots,
          engineStats: data,
          user: cached?.user || null,
        });
      } catch (error) {
        console.error('[fund] Failed to refresh engine stats', error);
      }
    };

    const refreshEngineStats = () => {
      loadEngineStats().catch(() => {});
    };
    const handleFocus = () => refreshEngineStats();
    const handleVisibility = () => {
      if (!document.hidden) refreshEngineStats();
    };

    refreshEngineStats();
    const refreshTimer = window.setInterval(refreshEngineStats, 60 * 1000);
    window.addEventListener('focus', handleFocus);
    document.addEventListener('visibilitychange', handleVisibility);

    return () => {
      cancelled = true;
      window.clearInterval(refreshTimer);
      window.removeEventListener('focus', handleFocus);
      document.removeEventListener('visibilitychange', handleVisibility);
    };
  }, [feeds, slots]);

  useEffect(() => {
    async function fetchData() {
      const { data: { user: authUser }, error: authError } = await getSupabase().auth.getUser();
      if (authError || !authUser) {
        router.replace('/login');
        return;
      }

      const userId = authUser.id;

      const { data: fetchedUserData, error: userFetchError } = await getSupabase()
        .from('users')
        .select('*')
        .eq('id', userId)
        .single();
      let userData = fetchedUserData;

      if (!userData && userFetchError?.code === 'PGRST116') {
        const { data: newUser, error: createError } = await getSupabase()
          .from('users')
          .insert({
            id: userId,
            email: authUser.email || '',
            name: authUser.user_metadata?.full_name || authUser.email?.split('@')[0] || 'User',
            balance: 1000,
            email_notifications: true,
          })
          .select()
          .single();

        if (createError) {
          alert(`Failed to initialize account: ${createError.message}`);
          return;
        }
        userData = newUser as User;
      }

      let userSnapshot: Partial<User> | null = null;
      if (userData) {
        userSnapshot = {
          id: userData.id,
          name: userData.name,
          email: userData.email,
          email_notifications: userData.email_notifications ?? true,
        };
      }

      let nextFeeds: Feed[] = [];
      let nextSlots: SlotUsage = { used: 0 };
      let nextStats: EngineStats = emptyStats;

      try {
        const res = await fetch('/api/feed');
        if (res.ok) {
          const data = await res.json();
          nextFeeds = data.feeds || [];
          nextSlots = data.slots || { used: 0 };
          setFeeds(nextFeeds);
          setSlots(nextSlots);
        }
      } catch (e) {
        console.error('Failed to fetch feed bundle', e);
      }

      try {
        const res = await fetch('/api/profile/engine');
        if (res.ok) {
          const data = await res.json();
          nextStats = data;
          setEngineStats(nextStats);
        }
      } catch (e) {
        console.error('Failed to fetch engine stats', e);
      }

      setCache(FUND_CACHE_KEY, {
        feeds: nextFeeds,
        slots: nextSlots,
        engineStats: nextStats,
        user: userSnapshot,
      });
    }

    const cached = getCache<{
      feeds: Feed[];
      slots: SlotUsage;
      engineStats: EngineStats;
      user: Partial<User> | null;
    }>(FUND_CACHE_KEY, FUND_CACHE_TTL);

    if (cached) {
      setFeeds(cached.feeds || []);
      setSlots(cached.slots || { used: 0 });
      setEngineStats(cached.engineStats || emptyStats);
      if (cached.user) {
      }
    }

    if (!cached) {
      fetchData().catch(() => {});
    }

    // Default to dark mode unless user explicitly set 'light'
    const savedTheme = localStorage.getItem('theme');
    const shouldBeDark = savedTheme !== 'light';
    setIsDarkMode(shouldBeDark);
    if (shouldBeDark) {
      document.documentElement.classList.add('dark');
      document.documentElement.classList.remove('light');
      document.documentElement.style.colorScheme = 'dark';
    } else {
      document.documentElement.classList.remove('dark');
      document.documentElement.classList.add('light');
      document.documentElement.style.colorScheme = 'light';
    }
  }, [router]);

  useEffect(() => {
    let payment: string | null = null;
    try {
      payment = new URLSearchParams(window.location.search).get('payment');
    } catch {}
    if (payment !== 'success') return;
    (async () => {
      try {
        const { data: { user: authUser } } = await getSupabase().auth.getUser();
        if (!authUser) return;
        await getSupabase().from('users').select('*').eq('id', authUser.id).single();
      } finally {
        router.replace('/profile');
      }
    })().catch(() => {});
  }, [router]);

  const slotPlanPrice = slots.plan?.price ?? 499;
  const slotPostsCap = slots.plan?.postsCap ?? 30;
  const billingSummary = useMemo(() => {
    const allFeeders = feeds.flatMap((feed) => feed.feeders || []);
    const feederCount = allFeeders.length;
    const baseCost = feederCount * slotPlanPrice;
    const overages: BillingLineItem[] = allFeeders
      .map((feeder) => {
        const posts = parseMetric(feeder.metrics?.postsTracked);
        const excess = Math.max(0, posts - slotPostsCap);
        return { handle: feeder.handle, posts, excess, cost: excess * 15 };
      })
      .filter((item) => item.excess > 0)
      .sort((a, b) => b.excess - a.excess);
    const overageCost = overages.reduce((sum, item) => sum + item.cost, 0);
    return { feederCount, baseCost, overages, overageCost, total: baseCost + overageCost };
  }, [feeds, slotPlanPrice, slotPostsCap]);
  const sortedQueuedBatches = useMemo(
    () =>
      [...engineStats.queuedBatches]
        .sort((a, b) => new Date(a.windowStart).getTime() - new Date(b.windowStart).getTime())
        .slice(0, 8),
    [engineStats.queuedBatches],
  );
  const liveQueuedBatches = useMemo(
    () => sortedQueuedBatches.filter((batch) => {
      const startMs = parseTimestampMs(batch.windowStart);
      return startMs != null && startMs <= activityNow;
    }),
    [activityNow, sortedQueuedBatches],
  );
  const nextFireBatch = useMemo(
    () => sortedQueuedBatches.find((batch) => {
      const startMs = parseTimestampMs(batch.windowStart);
      return startMs != null && startMs > activityNow;
    }) || null,
    [activityNow, sortedQueuedBatches],
  );
  const nextFireAtMs = useMemo(
    () => parseTimestampMs(nextFireBatch?.windowStart),
    [nextFireBatch],
  );
  const nextFireCountdownMs = nextFireAtMs == null ? null : Math.max(0, nextFireAtMs - activityNow);
  const nextFireCountdown = formatCountdownDuration(nextFireCountdownMs);
  const liveNowCount = useMemo(
    () => liveQueuedBatches.reduce((sum, batch) => sum + Math.max(0, Number(batch.totalRuns || 0)), 0),
    [liveQueuedBatches],
  );
  const liveNowMix = useMemo(
    () => summarizeBatchTypes(liveQueuedBatches.flatMap((batch) => batch.types || [])),
    [liveQueuedBatches],
  );
  const nextFireMix = useMemo(
    () => summarizeBatchTypes(nextFireBatch?.types),
    [nextFireBatch],
  );
  const queuePulse = useMemo(() => {
    const stageCounts = { D1: 0, D3: 0, D7: 0, D21: 0 };
    const creators = new Set<string>();
    let totalChecks = 0;

    for (const run of engineStats.queuedRuns) {
      const runDay = run.businessDay || formatIstDayKey(run.scheduledAt);
      if (runDay !== currentFireDay) continue;

      totalChecks += 1;
      if (run.handle) creators.add(run.handle);

      const checkpoint = String(run.checkpoint || run.label || '').toUpperCase();
      if (checkpoint === 'D1' || checkpoint === 'D3' || checkpoint === 'D7' || checkpoint === 'D21') {
        stageCounts[checkpoint] += 1;
      }
    }

    return {
      totalChecks,
      creatorsDue: creators.size,
      stageCounts,
    };
  }, [currentFireDay, engineStats.queuedRuns]);
  const firePulse = useMemo(() => {
    const stageCounts = { D1: 0, D3: 0, D7: 0 };
    const feedIds = new Set<number>();
    let bestPercentile: number | null = null;
    let lineSignals = 0;

    const rows = fireSnapshot.day === currentFireDay ? fireSnapshot.rows : [];
    for (const row of rows) {
      const checkpoint = String(row.checkpoint || '').toUpperCase();
      if (checkpoint === 'D1' || checkpoint === 'D3' || checkpoint === 'D7') {
        stageCounts[checkpoint] += 1;
      }

      const feedId = Number(row.feed_id);
      if (Number.isFinite(feedId)) {
        feedIds.add(feedId);
      }

      const percentile = firePercentile(row);
      if (percentile != null) {
        if (bestPercentile == null || percentile < bestPercentile) {
          bestPercentile = percentile;
        }
        if (percentile <= alertThreshold) {
          lineSignals += 1;
        }
      }
    }

    return {
      totalSignals: fireSnapshot.day === currentFireDay ? fireSnapshot.total : 0,
      lineSignals,
      bestPercentile,
      feedsLit: feedIds.size,
      stageCounts,
    };
  }, [alertThreshold, currentFireDay, fireSnapshot]);
  const todayFlowTotal = firePulse.totalSignals + queuePulse.totalChecks;
  const inFinalMinute = nextFireCountdownMs != null && nextFireCountdownMs > 0 && nextFireCountdownMs <= 90_000;
  const countdownParts = useMemo(() => decomposeCountdown(nextFireCountdownMs), [nextFireCountdownMs]);

  const ringProgress = useMemo(() => {
    if (liveNowCount > 0) return 1;
    if (nextFireAtMs == null) return 0;

    let prevAnchor: number | null = null;
    for (const batch of engineStats.completedBatches) {
      const t = parseTimestampMs(batch.windowEnd) ?? parseTimestampMs(batch.windowStart);
      if (t != null && t <= activityNow && (prevAnchor == null || t > prevAnchor)) {
        prevAnchor = t;
      }
    }

    const fourHours = 4 * 60 * 60 * 1000;
    const tenMinutes = 10 * 60 * 1000;
    if (prevAnchor == null || nextFireAtMs - prevAnchor > fourHours || nextFireAtMs - prevAnchor < tenMinutes) {
      prevAnchor = nextFireAtMs - Math.min(fourHours, Math.max(tenMinutes, FIRE_RING_WINDOW_MS));
    }

    const span = Math.max(tenMinutes, nextFireAtMs - prevAnchor);
    const elapsed = Math.max(0, activityNow - prevAnchor);
    return Math.min(1, Math.max(0.015, elapsed / span));
  }, [activityNow, liveNowCount, nextFireAtMs, engineStats.completedBatches]);

  const dayShape = useMemo(() => {
    type Bin = {
      scheduled: number;
      completed: number;
      types: { D1: number; D3: number; D7: number; D21: number; OTHER: number };
    };
    const bins: Bin[] = Array.from({ length: 24 }, () => ({
      scheduled: 0,
      completed: 0,
      types: { D1: 0, D3: 0, D7: 0, D21: 0, OTHER: 0 },
    }));
    const tagType = (run: EngineRun): keyof Bin['types'] => {
      const cp = String(run.checkpoint || run.label || '').toUpperCase();
      if (cp === 'D1' || cp === 'D3' || cp === 'D7' || cp === 'D21') return cp;
      return 'OTHER';
    };
    for (const run of engineStats.queuedRuns) {
      const day = run.businessDay || formatIstDayKey(run.scheduledAt);
      if (day !== currentFireDay) continue;
      const hour = istHourOf(run.scheduledAt);
      if (hour == null) continue;
      bins[hour].scheduled += 1;
      bins[hour].types[tagType(run)] += 1;
    }
    for (const run of engineStats.completedRuns) {
      const stamp = run.completedAt || run.scheduledAt;
      const day = run.businessDay || formatIstDayKey(stamp);
      if (day !== currentFireDay) continue;
      const hour = istHourOf(stamp);
      if (hour == null) continue;
      bins[hour].completed += 1;
      bins[hour].types[tagType(run)] += 1;
    }
    let peak = 0;
    for (const bin of bins) peak = Math.max(peak, bin.scheduled + bin.completed);
    return { bins, peak };
  }, [currentFireDay, engineStats.queuedRuns, engineStats.completedRuns]);

  const upcomingDropMarkers = useMemo(
    () =>
      sortedQueuedBatches
        .map((batch) => {
          const startMs = parseTimestampMs(batch.windowStart);
          if (startMs == null) return null;
          const offsetMs = startMs - activityNow;
          if (offsetMs <= 0 || offsetMs > FIRE_RING_WINDOW_MS) return null;
          return { id: batch.id, progress: 1 - offsetMs / FIRE_RING_WINDOW_MS };
        })
        .filter((m): m is { id: string; progress: number } => m != null)
        .slice(0, 4),
    [activityNow, sortedQueuedBatches],
  );
  const pulsePillText = fireSignalsLoading
    ? 'Syncing'
    : liveNowCount > 0
      ? 'Live now'
      : nextFireBatch
        ? 'Next drop'
        : todayFlowTotal > 0
          ? 'Today live'
          : 'Armed';
  const pulseHeadline = nextFireBatch
    ? nextFireCountdown
    : liveNowCount > 0
      ? 'live'
      : '--';
  const pulseLabel = nextFireBatch
    ? liveNowCount > 0
      ? 'Then'
      : 'Next drop'
    : liveNowCount > 0
      ? 'Current drop'
      : 'Awaiting queue';
  const alertsArmed = pwaNotificationsEnabled;
  const pwaStatusText = notificationBusy
    ? 'Requesting browser permission...'
    : notificationPermission === 'unsupported'
      ? 'Notifications are not available in this browser.'
      : notificationPermission === 'denied'
        ? 'Browser permission is blocked. Enable it in site settings.'
        : notificationTestNotice
          ? notificationTestNotice
        : pwaNotificationsEnabled
          ? `Live as native push on the same fire line below ${alertThreshold}%.`
          : `Turn this on to arm native Fire pushes below ${alertThreshold}% for this device.`;
  const pwaBadgeText = notificationPermission === 'granted'
    ? 'live'
    : notificationPermission === 'denied'
      ? 'blocked'
      : notificationPermission === 'unsupported'
        ? 'na'
        : 'ready';

  // Group active feeders feed-wise
  const feedWiseUsage = useMemo(() => {
    return feeds
      .map(feed => {
        const feeders = (feed.feeders || []).map(f => {
          const posts = parseMetric(f.metrics?.postsTracked);
          return { handle: f.handle, count: posts, pct: Math.min(100, (posts / slotPostsCap) * 100) };
        });
        return {
          id: feed.id,
          title: feed.title,
          feeders: feeders.sort((a, b) => b.count - a.count),
          totalPosts: feeders.reduce((sum, f) => sum + f.count, 0)
        };
      })
      .filter(f => f.feeders.length > 0)
      .sort((a, b) => b.totalPosts - a.totalPosts);
  }, [feeds, slotPostsCap]);

  // Auto-lock: after slider release, fill ring over 1.2s then lock
  const LOCK_DURATION = 1200;
  function startAutoLock() {
    lockStartRef.current = performance.now();
    const tick = () => {
      const elapsed = performance.now() - lockStartRef.current;
      const pct = Math.min(1, elapsed / LOCK_DURATION);
      if (pct < 1) {
        lockRafRef.current = requestAnimationFrame(tick);
      } else {
        setThresholdLocked(true);
        play('snapLock');
        lockRafRef.current = null;
      }
    };
    lockRafRef.current = requestAnimationFrame(tick);
  }

  const [manageSubscriptionBusy, setManageSubscriptionBusy] = useState(false);
  const supportEmail = 'support@feedmemore.com';
  const siteUrl = 'https://feedmemore.vercel.app';

  const openManageSubscription = useCallback(() => {
    setShowManageSubscriptionModal(true);
  }, []);

  const closeManageSubscription = useCallback(() => {
    setShowManageSubscriptionModal(false);
  }, []);

  const startManageSubscriptionCheckout = useCallback(async () => {
    if (manageSubscriptionBusy || typeof window === 'undefined') return;
    setManageSubscriptionBusy(true);
    try {
      const response = await fetch('/api/payments/create-link', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ amount: slotPlanPrice }),
      });
      if (!response.ok) throw new Error(await readApiError(response, 'Unable to open subscription checkout'));
      const data = await response.json() as { paymentLinkUrl?: string };
      if (!data.paymentLinkUrl) throw new Error('Payment link unavailable');
      window.location.href = data.paymentLinkUrl;
    } catch (error) {
      console.error('[fund] subscription link error', error);
      window.alert(error instanceof Error ? error.message : 'Unable to open subscription checkout');
    } finally {
      setManageSubscriptionBusy(false);
    }
  }, [manageSubscriptionBusy, readApiError, slotPlanPrice]);

  const openSupportEmail = useCallback((subject: string) => {
    if (typeof window === 'undefined') return;
    const href = `mailto:${supportEmail}?subject=${encodeURIComponent(subject)}`;
    window.location.href = href;
  }, []);

  const handlePwaNotificationToggle = useCallback(async () => {
    if (notificationBusy || typeof window === 'undefined') return;
    if (!('Notification' in window) || !('serviceWorker' in navigator)) {
      setNotificationPermission('unsupported');
      return;
    }

    setNotificationBusy(true);

    if (pwaNotificationsEnabled) {
      try {
        await unsubscribeFromWebPush();
        window.localStorage.setItem(PWA_NOTIFICATION_ENABLED_KEY, 'false');
        setPwaNotificationsEnabled(false);
        setNotificationTestNotice('Fire alerts paused on this device.');
      } catch (error) {
        console.error('[fund] Failed to disable PWA notifications', error);
        setNotificationTestNotice(error instanceof Error ? error.message : 'Failed to disable Fire Alerts.');
      } finally {
        setNotificationBusy(false);
      }
      return;
    }

    let subscriptionArmed = false;
    try {
      await ensureServiceWorkerRegistration();
      let permission = Notification.permission;
      if (permission === 'default') {
        permission = await Notification.requestPermission();
      }

      setNotificationPermission(permission);
      if (permission !== 'granted') {
        window.localStorage.setItem(PWA_NOTIFICATION_ENABLED_KEY, 'false');
        setPwaNotificationsEnabled(false);
        setNotificationTestNotice('Browser permission is blocked. Enable it in site settings.');
        return;
      }

      await subscribeToWebPush();
      subscriptionArmed = true;
      const response = await fetch('/api/profile/notifications', {
        body: JSON.stringify({
          fireAlertThreshold: alertThreshold,
        }),
        headers: { 'Content-Type': 'application/json' },
        method: 'PUT',
      });

      if (!response.ok) {
        throw new Error(await readApiError(response, 'Failed to arm Fire Alerts'));
      }

      window.localStorage.setItem(PWA_NOTIFICATION_ENABLED_KEY, 'true');
      setPwaNotificationsEnabled(true);
      setNotificationTestNotice('Fire alerts armed. Send a real test push to verify delivery.');
    } catch (error) {
      if (subscriptionArmed) {
        try {
          await unsubscribeFromWebPush();
        } catch (cleanupError) {
          console.error('[fund] Failed to roll back PWA subscription', cleanupError);
        }
      }
      console.error('[fund] Failed to enable PWA notifications', error);
      window.localStorage.setItem(PWA_NOTIFICATION_ENABLED_KEY, 'false');
      setPwaNotificationsEnabled(false);
      setNotificationTestNotice(error instanceof Error ? error.message : 'Failed to arm Fire Alerts.');
    } finally {
      setNotificationBusy(false);
    }
  }, [alertThreshold, notificationBusy, pwaNotificationsEnabled, readApiError]);

  const handleTestNotification = useCallback(async () => {
    if (notificationTestBusy || typeof window === 'undefined') return;
    if (!('Notification' in window) || !('serviceWorker' in navigator)) {
      setNotificationPermission('unsupported');
      return;
    }

    if (Notification.permission !== 'granted' || !pwaNotificationsEnabled) {
      setNotificationTestNotice('Enable Fire Alerts first, then send a test notification.');
      return;
    }

    setNotificationTestBusy(true);
    setNotificationTestNotice(null);

    try {
      const response = await fetch('/api/push/test', { method: 'POST' });
      if (!response.ok) {
        throw new Error(await readApiError(response, 'Failed to queue test push'));
      }
      play('snapLock');
      setNotificationTestNotice('Real push queued. Background delivery now depends on the worker and your browser push permission.');
    } catch (error) {
      console.error('[fund] Failed to send test notification', error);
      setNotificationTestNotice(error instanceof Error ? error.message : 'Test failed. Check notification permissions and Web Push setup.');
    } finally {
      setNotificationTestBusy(false);
    }
  }, [notificationTestBusy, play, pwaNotificationsEnabled, readApiError]);

  return (
    <motion.div
      initial={false}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.42, ease: APPLE_EASE }}
      className={cn(
        'relative w-full text-foreground select-none',
        useTranslucentBrowserChrome ? 'bg-transparent' : 'bg-[#f4f7f9] dark:bg-[#030303]',
        useBrowserPageScroll ? 'overflow-visible' : 'overflow-hidden',
      )}
      style={appShellStyle}
    >
      <div
        className="pointer-events-none fixed inset-0 z-0 bg-[#f4f7f9] dark:bg-[#030303]"
      />

      <AnimatePresence>
        {showManageSubscriptionModal && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="fixed inset-0 z-[260] flex items-end justify-center sm:items-center sm:px-4 sm:py-6"
            style={{ background: 'rgba(0,0,0,0.72)', backdropFilter: 'blur(8px)', WebkitBackdropFilter: 'blur(8px)' }}
            onClick={closeManageSubscription}
          >
            <motion.div
              initial={{ opacity: 0, y: 40 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: 30 }}
              transition={{ duration: 0.32, ease: APPLE_EASE }}
              onClick={(event) => event.stopPropagation()}
              className="relative w-full max-h-[92vh] overflow-y-auto overflow-x-hidden sm:max-w-[520px] sm:rounded-[28px]"
              style={{
                background: '#080808',
                borderRadius: 'clamp(24px, 4vw, 28px)',
                boxShadow: '0 0 80px rgba(225,29,72,0.06), 0 40px 100px rgba(0,0,0,0.7)',
              }}
            >
              {/* Top accent line */}
              <div className="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-[#E11D48]/30 to-transparent" />

              {/* ── Header ── */}
              <div className="relative px-5 pb-0 pt-5 sm:px-7 sm:pt-7">
                {/* Close */}
                <button
                  type="button"
                  onClick={closeManageSubscription}
                  className="absolute right-4 top-4 flex h-8 w-8 items-center justify-center rounded-full bg-white/[0.06] text-white/40 transition-colors hover:bg-white/[0.1] hover:text-white/60 sm:right-6 sm:top-6"
                >
                  <X size={15} strokeWidth={2.5} />
                </button>

                <div className="text-[9px] font-black uppercase tracking-[0.2em] text-[#E11D48]/50 sm:text-[10px]">Feed Pass</div>
                <div className="mt-2 text-[26px] font-black leading-none tracking-[-0.04em] text-white sm:text-[32px]">
                  Subscription
                </div>
              </div>

              {/* ── Vibrant total pill ── */}
              <div className="px-5 pt-5 sm:px-7 sm:pt-6">
                <div
                  className="relative overflow-hidden rounded-[18px] bg-[#E11D48] px-5 py-4 sm:rounded-[20px] sm:px-6 sm:py-5"
                  style={{ boxShadow: '0 12px 40px rgba(225,29,72,0.16), 0 0 80px rgba(225,29,72,0.06)' }}
                >
                  {/* Subtle inner glow */}
                  <div className="pointer-events-none absolute inset-0" style={{ background: 'linear-gradient(180deg, rgba(255,255,255,0.12) 0%, transparent 40%, rgba(0,0,0,0.04) 100%)' }} />
                  <div className="relative flex items-end justify-between gap-4">
                    <div>
                      <div className="text-[8px] font-black uppercase tracking-[0.16em] text-white/64 sm:text-[9px]">Monthly total</div>
                      <div className="mt-1 text-[38px] font-black leading-none tracking-[-0.05em] text-white sm:text-[46px]">
                        ₹{billingSummary.total.toLocaleString('en-IN')}
                      </div>
                    </div>
                    <div className="mb-1 rounded-full bg-white/12 px-3 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-white/70 sm:text-[9px]">
                      {billingSummary.feederCount} feeder{billingSummary.feederCount === 1 ? '' : 's'}
                    </div>
                  </div>
                </div>
              </div>

              {/* ── Cost breakdown ── */}
              <div className="px-5 pt-5 sm:px-7 sm:pt-6">
                <div className="space-y-0">
                  {/* Base row */}
                  <div className="flex items-center justify-between border-b border-white/[0.06] py-3.5 first:pt-0 sm:py-4">
                    <div>
                      <div className="text-[11px] font-black uppercase tracking-[0.06em] text-white/80 sm:text-[12px]">Base passes</div>
                      <div className="mt-0.5 text-[9px] font-bold uppercase tracking-[0.1em] text-white/25 sm:text-[10px]">
                        {billingSummary.feederCount} × ₹{slotPlanPrice} per feeder
                      </div>
                    </div>
                    <div className="text-[16px] font-black tracking-[-0.02em] text-white sm:text-[18px]">
                      ₹{billingSummary.baseCost.toLocaleString('en-IN')}
                    </div>
                  </div>

                  {/* Overage row */}
                  <div className="flex items-center justify-between border-b border-white/[0.06] py-3.5 sm:py-4">
                    <div>
                      <div className="text-[11px] font-black uppercase tracking-[0.06em] text-white/80 sm:text-[12px]">Overage charges</div>
                      <div className="mt-0.5 text-[9px] font-bold uppercase tracking-[0.1em] text-white/25 sm:text-[10px]">
                        {billingSummary.overages.length > 0
                          ? `${billingSummary.overages.length} feeder${billingSummary.overages.length === 1 ? '' : 's'} over ${slotPostsCap}-post cap`
                          : `All within ${slotPostsCap}-post cap`}
                      </div>
                    </div>
                    <div className={cn(
                      'text-[16px] font-black tracking-[-0.02em] sm:text-[18px]',
                      billingSummary.overageCost > 0 ? 'text-[#E11D48]' : 'text-white/30'
                    )}>
                      {billingSummary.overageCost > 0 ? `₹${billingSummary.overageCost.toLocaleString('en-IN')}` : '₹0'}
                    </div>
                  </div>
                </div>
              </div>

              {/* ── Feeder ledger (overages only) ── */}
              {billingSummary.overages.length > 0 && (
                <div className="px-5 pt-5 sm:px-7 sm:pt-6">
                  <div className="mb-3 text-[9px] font-black uppercase tracking-[0.16em] text-white/22 sm:text-[10px]">
                    Overage detail
                  </div>
                  <div className="space-y-1.5 sm:space-y-2">
                    {billingSummary.overages.map((item) => (
                      <div
                        key={item.handle}
                        className="flex items-center justify-between rounded-[14px] bg-white/[0.03] px-4 py-3 sm:rounded-[16px] sm:py-3.5"
                      >
                        <div className="min-w-0 flex-1">
                        <div className="truncate text-[11px] font-black tracking-[-0.01em] text-white/75 sm:text-[12px]">
                          @{item.handle}
                        </div>
                        <div className="mt-0.5 text-[8px] font-bold uppercase tracking-[0.1em] text-white/22 sm:text-[9px]">
                          {item.posts} posts · +{item.excess} over cap
                        </div>
                      </div>
                        <div className="ml-3 text-[13px] font-black text-white/78 sm:text-[14px]">
                          +₹{item.cost}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}

              {/* ── Add feeder CTA ── */}
              <div className="px-5 pb-6 pt-5 sm:px-7 sm:pb-7 sm:pt-6">
                <button
                  type="button"
                  onClick={startManageSubscriptionCheckout}
                  disabled={manageSubscriptionBusy}
                  className="group flex w-full items-center justify-between rounded-[16px] border border-white/[0.06] bg-white/[0.03] px-4 py-3.5 text-left transition-all duration-200 hover:border-[#E11D48]/16 hover:bg-[#E11D48]/[0.04] active:scale-[0.98] disabled:cursor-not-allowed disabled:opacity-50 sm:rounded-[18px] sm:px-5 sm:py-4"
                >
                  <div className="flex items-center gap-3">
                    <div
                      className="flex h-9 w-9 items-center justify-center rounded-full bg-[#E11D48] text-white sm:h-10 sm:w-10"
                      style={{ boxShadow: '0 6px 20px rgba(225,29,72,0.18)' }}
                    >
                      <CreditCard size={15} strokeWidth={2.5} />
                    </div>
                    <div>
                      <div className="text-[10px] font-black uppercase tracking-[0.1em] text-white/70 sm:text-[11px]">
                        Add feeder pass
                      </div>
                      <div className="mt-0.5 text-[8px] font-bold uppercase tracking-[0.1em] text-white/25 sm:text-[9px]">
                        ₹{slotPlanPrice} per slot · {slotPostsCap} posts included
                      </div>
                    </div>
                  </div>
                  <ArrowUpRight size={16} className="text-white/20 transition-colors group-hover:text-[#E11D48]/60" />
                </button>
              </div>

              {/* Bottom safe area for mobile */}
              <div className="h-[env(safe-area-inset-bottom)]" />
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>

      {/* ═══ PREMIUM THEME RIPPLE TRANSITION ═══ */}
      {themeRipple && (
        <div
          className="pointer-events-none fixed inset-0 z-[9999] will-change-[clip-path,opacity]"
          style={{
            background: themeRipple.toDark
              ? `radial-gradient(circle at ${themeRipple.x}px ${themeRipple.y}px, rgba(225,29,72,0.16) 0%, rgba(8,8,8,0.22) 28%, rgba(8,8,8,0.12) 62%, rgba(8,8,8,0.04) 100%)`
              : `radial-gradient(circle at ${themeRipple.x}px ${themeRipple.y}px, rgba(255,255,255,0.58) 0%, rgba(244,247,249,0.34) 30%, rgba(255,255,255,0.18) 62%, rgba(255,255,255,0.06) 100%)`,
            backdropFilter: themeRipple.toDark
              ? 'blur(10px) saturate(115%) brightness(0.88)'
              : 'blur(10px) saturate(112%) brightness(1.08)',
            WebkitBackdropFilter: themeRipple.toDark
              ? 'blur(10px) saturate(115%) brightness(0.88)'
              : 'blur(10px) saturate(112%) brightness(1.08)',
            clipPath: themeRipple.active
              ? 'circle(150vmax at ' + themeRipple.x + 'px ' + themeRipple.y + 'px)'
              : 'circle(0px at ' + themeRipple.x + 'px ' + themeRipple.y + 'px)',
            opacity: themeRipple.active ? 1 : 0.35,
            boxShadow: themeRipple.toDark
              ? 'inset 0 0 0 1px rgba(255,255,255,0.05)'
              : 'inset 0 0 0 1px rgba(255,255,255,0.24)',
            transition: 'clip-path 0.72s cubic-bezier(0.4, 0, 0.2, 1), opacity 0.2s ease',
          }}
          onTransitionEnd={() => {
            if (themeRipple.active) setThemeRipple(null);
          }}
        />
      )}

      {/* ═══ MINIMAL LOCKED HEADER ═══ */}
      <motion.div
        variants={HEADER_STAGGER_CONTAINER}
        initial="initial"
        animate="animate"
        className={cn(
          'pointer-events-auto inset-x-0 top-0 z-[100] flex flex-col items-center px-2 pt-[calc(10px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-4 sm:pt-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] md:pt-[calc(20px+var(--pwa-top-fix,0px))] lg:px-4',
          useBrowserPageScroll ? 'fixed' : 'absolute',
        )}
      >
        <div className="relative fm-tab-header-shell">
          <div className="fm-depth-chrome fm-depth-chrome--header w-full">
            <div className="relative z-10 px-3.5 py-3 sm:px-5 sm:py-3.5">
              <motion.div variants={HEADER_ROW} className="flex items-center justify-between gap-3">
                <h1 className="text-[30px] font-black leading-none tracking-[0.14em] text-black sm:text-[38px] dark:text-white fm-depth-title">FUND</h1>
                <div className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/50">Slot Control Room</div>
              </motion.div>
            </div>
          </div>
        </div>
      </motion.div>

      {/* ═══ CONTENT STAGGER GRID ═══ */}
      <div className={cn(
        'z-10 pointer-events-none',
        useBrowserPageScroll ? 'relative min-h-[var(--fm-app-height,100dvh)]' : 'relative h-full',
      )}>
        <div
          className={cn(
            'w-full overflow-x-hidden pt-[calc(96px+env(safe-area-inset-top))] sm:pt-[calc(108px+env(safe-area-inset-top))] md:pt-[118px] pointer-events-auto',
            useBrowserPageScroll ? 'min-h-[var(--fm-app-height,100dvh)] overflow-visible' : 'hide-scrollbar h-full overflow-y-auto',
          )}
          style={{
            WebkitOverflowScrolling: useBrowserPageScroll ? undefined : 'touch',
            paddingBottom: isStandaloneMode ? 'calc(170px + env(safe-area-inset-bottom))' : mobileBottomClearance,
          }}
        >
          <motion.div
            variants={staggerContainer}
            initial="hidden"
            animate="visible"
            className="fm-tab-canvas-shell mx-auto space-y-4 px-2 sm:space-y-5 sm:px-0 lg:space-y-6 xl:space-y-7 transform-gpu will-change-transform"
          >
            {/* Middle Section: Management & Control Center */}
            <div className="grid items-stretch gap-4 xl:grid-cols-[0.92fr_1.08fr] xl:gap-5 2xl:grid-cols-[0.88fr_1.12fr] 2xl:gap-6">
              <motion.div variants={tileVariant} className="flex h-full flex-col gap-4 lg:gap-5">
                <FeedPassCard
                  feeds={feeds}
                  slotPlanPrice={slotPlanPrice}
                  slotPostsCap={slotPostsCap}
                  onManageSubscription={openManageSubscription}
                  manageBusy={manageSubscriptionBusy}
                />

                {/* Compact Support & Policy Row */}
                <div className={cn(
                  'fm-depth-chip rounded-[20px] px-3.5 py-3 sm:px-5 sm:py-3.5',
                  'flex items-center gap-2 sm:gap-3',
                )}>
                  <button
                    type="button"
                    onClick={() => openSupportEmail('FeedMe bug report')}
                    className="inline-flex items-center gap-1.5 rounded-full border border-black/6 bg-black/[0.03] px-2.5 py-1.5 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/60 transition-colors active:scale-[0.97] active:bg-black/[0.06] dark:border-white/[0.06] dark:bg-white/[0.04] dark:text-white/55 dark:active:bg-white/[0.08] sm:px-3 sm:text-[9px]"
                  >
                    <Bug size={11} />
                    Bug
                  </button>
                  <button
                    type="button"
                    onClick={() => openSupportEmail('FeedMe refund or cancellation help')}
                    className="inline-flex items-center gap-1.5 rounded-full border border-black/6 bg-black/[0.03] px-2.5 py-1.5 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/60 transition-colors active:scale-[0.97] active:bg-black/[0.06] dark:border-white/[0.06] dark:bg-white/[0.04] dark:text-white/55 dark:active:bg-white/[0.08] sm:px-3 sm:text-[9px]"
                  >
                    <LifeBuoy size={11} />
                    Billing
                  </button>
                  <div className="ml-auto flex items-center gap-2.5 sm:gap-3">
                    <a href={`${siteUrl}/privacy`} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/42 transition-colors active:text-foreground dark:text-white/38 dark:active:text-white/70 sm:text-[9px]">
                      <FileText size={10} />Privacy
                    </a>
                    <a href={`${siteUrl}/terms`} target="_blank" rel="noreferrer" className="inline-flex items-center gap-1 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/42 transition-colors active:text-foreground dark:text-white/38 dark:active:text-white/70 sm:text-[9px]">
                      <FileText size={10} />Terms
                    </a>
                  </div>
                </div>

                {/* Spacer to push left column flush with right column bottom */}
                <div className="hidden xl:block xl:flex-1" />
              </motion.div>

              {/* Right Column: Unified Control Center */}
              <motion.div variants={tileVariant} className={cn(
                'fm-depth-glass rounded-[32px] p-5 relative overflow-hidden flex flex-col lg:p-6 xl:p-7',
                'bg-white/70 border border-white/80',
                'shadow-[inset_0_1px_0_rgba(255,255,255,0.9),inset_0_-1px_0_rgba(0,0,0,0.04),0_12px_32px_-4px_rgba(15,23,42,0.07)]',
                'dark:bg-[rgba(10,10,10,0.65)] dark:border-white/[0.06] dark:border-t-white/[0.1]',
                'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),inset_0_-1px_0_rgba(0,0,0,0.5),0_24px_48px_rgba(0,0,0,0.5)]',
              )}>
                {/* ── Countdown Timer Box ── */}
                <div className={cn(
                  'relative w-full overflow-hidden rounded-[22px] p-[1.5px]',
                )}>
                  {/* Animated border — the box border IS the progress indicator */}
                  <svg className="pointer-events-none absolute inset-0 z-10 h-full w-full overflow-visible" aria-hidden="true">
                    <defs>
                      <linearGradient id="fire-arc-gradient" x1="0" y1="0" x2="1" y2="1">
                        <stop offset="0%" stopColor="#9F1239" />
                        <stop offset="55%" stopColor="#E11D48" />
                        <stop offset="100%" stopColor="#FB7185" />
                      </linearGradient>
                      <linearGradient id="fire-tip-gradient" x1="0" y1="0" x2="1" y2="1">
                        <stop offset="0%" stopColor="#FB7185" />
                        <stop offset="100%" stopColor="#FFFFFF" />
                      </linearGradient>
                    </defs>
                    <rect
                      x="1" y="1"
                      width="99%" height="99%"
                      rx="21" ry="21"
                      fill="none"
                      stroke="currentColor"
                      strokeWidth="1.5"
                      className="text-foreground/[0.06] dark:text-white/[0.06]"
                    />
                    {upcomingDropMarkers.slice(1).map((marker) => (
                      <rect
                        key={`drop-${marker.id}`}
                        x="1" y="1"
                        width="99%" height="99%"
                        rx="21" ry="21"
                        fill="none"
                        stroke="#FB7185"
                        strokeWidth="2"
                        strokeLinecap="round"
                        pathLength={1}
                        strokeDasharray="0.014 0.986"
                        strokeDashoffset={-marker.progress}
                        style={{ opacity: 0.5, filter: 'drop-shadow(0 0 4px rgba(251,113,133,0.55))' }}
                      />
                    ))}
                    <motion.rect
                      x="1" y="1"
                      width="99%" height="99%"
                      rx="21" ry="21"
                      fill="none"
                      stroke="url(#fire-arc-gradient)"
                      strokeWidth="2.25"
                      strokeLinecap="round"
                      pathLength={1}
                      strokeDasharray="1"
                      initial={false}
                      animate={{
                        strokeDashoffset: 1 - ringProgress,
                        opacity: inFinalMinute ? [1, 0.6, 1] : [0.92, 1, 0.92],
                      }}
                      transition={{
                        strokeDashoffset: { duration: 0.9, ease: APPLE_EASE },
                        opacity: inFinalMinute
                          ? { duration: 1.2, repeat: Infinity, ease: 'easeInOut' }
                          : { duration: 3.6, repeat: Infinity, ease: 'easeInOut' },
                      }}
                      style={{ filter: 'drop-shadow(0 0 8px rgba(225,29,72,0.55))' }}
                    />
                    {/* Leading dot at the tip of the arc */}
                    {nextFireBatch && (
                      <motion.rect
                        x="1" y="1"
                        width="99%" height="99%"
                        rx="21" ry="21"
                        fill="none"
                        stroke="url(#fire-tip-gradient)"
                        strokeWidth="4.5"
                        strokeLinecap="round"
                        pathLength={1}
                        strokeDasharray="0.006 0.994"
                        initial={false}
                        animate={{
                          strokeDashoffset: -ringProgress,
                          opacity: inFinalMinute ? [1, 0.55, 1] : [0.85, 1, 0.85],
                        }}
                        transition={{
                          strokeDashoffset: { duration: 0.9, ease: APPLE_EASE },
                          opacity: { duration: inFinalMinute ? 0.9 : 2.4, repeat: Infinity, ease: 'easeInOut' },
                        }}
                        style={{ filter: 'drop-shadow(0 0 12px rgba(255,255,255,0.85)) drop-shadow(0 0 20px rgba(225,29,72,0.85))' }}
                      />
                    )}
                  </svg>

                  {/* Inner content */}
                  <div className={cn(
                    'relative z-0 rounded-[21px] px-5 py-4 sm:px-6 sm:py-5',
                    'bg-gradient-to-b from-white/80 to-white/50',
                    'dark:from-white/[0.04] dark:to-white/[0.015]',
                  )}>
                    <div className="flex items-start justify-between gap-4">
                      <div className="min-w-0 flex-1">
                        <div className="flex items-center gap-2">
                          <div className="text-[8px] font-black uppercase tracking-[0.18em] text-foreground/38 dark:text-white/32">Today&apos;s Fire flow</div>
                          <div className="text-[7px] font-black uppercase tracking-[0.16em] text-foreground/30 dark:text-white/22 tabular-nums">{nowClockLabel} IST</div>
                        </div>
                        <div className="mt-1.5 flex items-end gap-2.5">
                          <motion.div
                            key={`signals-${currentFireDay}-${firePulse.totalSignals}`}
                            initial={{ y: 6, opacity: 0, scale: 0.96 }}
                            animate={{ y: 0, opacity: 1, scale: 1 }}
                            transition={{ duration: 0.32, ease: APPLE_EASE }}
                            className="bg-gradient-to-b from-foreground to-foreground/70 bg-clip-text text-[44px] font-black leading-[0.85] tracking-[-0.07em] text-transparent tabular-nums dark:from-white dark:to-white/70 sm:text-[52px]"
                            style={{ filter: 'drop-shadow(0 0 18px rgba(225,29,72,0.18))' }}
                          >
                            {fireSignalsLoading && fireSnapshot.day !== currentFireDay ? '--' : firePulse.totalSignals}
                          </motion.div>
                          <div className="pb-1 text-[8px] font-black uppercase tracking-[0.16em] text-foreground/45 dark:text-white/38">
                            {firePulse.totalSignals === 1 ? 'signal fired' : 'signals fired'}
                          </div>
                        </div>
                        <div className="mt-2 flex flex-wrap items-center gap-x-2 gap-y-1 text-[8px] font-black uppercase tracking-[0.13em]">
                          {firePulse.lineSignals > 0 && (
                            <span className="inline-flex items-center gap-1 rounded-full border border-[#E11D48]/22 bg-[#E11D48]/10 px-2 py-[3px] text-[#BE123C] dark:border-[#E11D48]/24 dark:bg-[#E11D48]/12 dark:text-[#FB7185]">
                              <span className="h-1 w-1 rounded-full bg-[#E11D48]" />
                              <span className="tabular-nums">{firePulse.lineSignals}</span>
                              <span>on your {alertThreshold}% line</span>
                            </span>
                          )}
                          {queuePulse.creatorsDue > 0 && (
                            <span className="text-foreground/45 dark:text-white/38">
                              <span className="tabular-nums">{queuePulse.creatorsDue}</span> creator{queuePulse.creatorsDue === 1 ? '' : 's'} still scheduled
                            </span>
                          )}
                          {firePulse.totalSignals === 0 && queuePulse.creatorsDue === 0 && !fireSignalsLoading && (
                            <span className="text-foreground/40 dark:text-white/30">Awaiting today&apos;s drops</span>
                          )}
                        </div>
                      </div>

                      <div className="shrink-0 text-right">
                        <motion.div
                          key={pulsePillText}
                          initial={{ y: -4, opacity: 0 }}
                          animate={{ y: 0, opacity: 1 }}
                          transition={{ duration: 0.24, ease: APPLE_EASE }}
                          className={cn(
                            'inline-flex min-w-[86px] items-center justify-center gap-1.5 rounded-full px-3.5 py-1.5 text-[9px] font-black uppercase leading-none tracking-[0.16em]',
                            liveNowCount > 0
                              ? 'border border-[#E11D48]/35 bg-[#E11D48] text-white shadow-[0_6px_18px_rgba(225,29,72,0.45)]'
                              : 'border border-[#E11D48]/18 bg-[#E11D48]/10 text-[#BE123C] dark:border-[#E11D48]/22 dark:bg-[#E11D48]/12 dark:text-[#FB7185]',
                          )}
                        >
                          {liveNowCount > 0 && (
                            <motion.span
                              animate={{ scale: [1, 1.6, 1], opacity: [1, 0.4, 1] }}
                              transition={{ duration: 1.1, repeat: Infinity, ease: 'easeInOut' }}
                              className="h-1.5 w-1.5 rounded-full bg-white shadow-[0_0_8px_rgba(255,255,255,0.85)]"
                            />
                          )}
                          {pulsePillText}
                        </motion.div>
                        {countdownParts ? (
                          <motion.div
                            animate={{ scale: inFinalMinute ? [1, 1.025, 1] : [1, 1.005, 1] }}
                            transition={{ duration: inFinalMinute ? 1 : 3.4, repeat: Infinity, ease: 'easeInOut' }}
                            className="mt-3.5 flex items-baseline justify-end gap-[2px] font-black leading-none tabular-nums"
                          >
                            {countdownParts.h > 0 && (
                              <>
                                <motion.span
                                  key={`h-${countdownParts.h}`}
                                  initial={{ y: -6, opacity: 0 }}
                                  animate={{ y: 0, opacity: 1 }}
                                  transition={{ duration: 0.22, ease: APPLE_EASE }}
                                  className={cn(
                                    'text-[34px] tracking-[-0.07em] sm:text-[40px]',
                                    inFinalMinute ? 'text-[#E11D48]' : 'text-foreground dark:text-white',
                                  )}
                                  style={{ filter: 'drop-shadow(0 0 14px rgba(225,29,72,0.35))' }}
                                >
                                  {countdownParts.h}
                                </motion.span>
                                <span className="ml-[1px] mr-[3px] text-[14px] tracking-[0.04em] text-foreground/35 dark:text-white/26">h</span>
                              </>
                            )}
                            {(countdownParts.h > 0 || countdownParts.m > 0) && (
                              <>
                                <motion.span
                                  key={`m-${countdownParts.h}-${countdownParts.m}`}
                                  initial={{ y: -6, opacity: 0 }}
                                  animate={{ y: 0, opacity: 1 }}
                                  transition={{ duration: 0.22, ease: APPLE_EASE }}
                                  className={cn(
                                    'text-[34px] tracking-[-0.07em] sm:text-[40px]',
                                    inFinalMinute ? 'text-[#E11D48]' : 'text-foreground dark:text-white',
                                  )}
                                  style={{ filter: 'drop-shadow(0 0 14px rgba(225,29,72,0.35))' }}
                                >
                                  {countdownParts.h > 0 ? String(countdownParts.m).padStart(2, '0') : countdownParts.m}
                                </motion.span>
                                <span className="ml-[1px] mr-[3px] text-[14px] tracking-[0.04em] text-foreground/35 dark:text-white/26">m</span>
                              </>
                            )}
                            <motion.span
                              key={`s-${countdownParts.s}`}
                              initial={{ y: -4, opacity: 0 }}
                              animate={{ y: 0, opacity: 1 }}
                              transition={{ duration: 0.18, ease: APPLE_EASE }}
                              className={cn(
                                'text-[20px] tracking-[-0.04em] sm:text-[22px]',
                                inFinalMinute ? 'text-[#E11D48]' : 'text-foreground/55 dark:text-white/45',
                              )}
                            >
                              {String(countdownParts.s).padStart(2, '0')}
                            </motion.span>
                            <span className="ml-[1px] text-[11px] tracking-[0.04em] text-foreground/30 dark:text-white/22">s</span>
                          </motion.div>
                        ) : (
                          <motion.div
                            key={pulseHeadline}
                            initial={{ y: 4, opacity: 0 }}
                            animate={{ y: 0, opacity: 1 }}
                            transition={{ duration: 0.24, ease: APPLE_EASE }}
                            className="mt-3.5 text-[26px] font-black leading-none tracking-[-0.06em] text-foreground dark:text-white sm:text-[30px]"
                          >
                            {pulseHeadline}
                          </motion.div>
                        )}
                        <div className="mt-1.5 text-[7px] font-black uppercase tracking-[0.14em] text-foreground/38 dark:text-white/30">
                          {pulseLabel}
                        </div>
                        {nextFireBatch && (
                          <div className="mt-1 text-[8px] font-black uppercase tracking-[0.11em] text-foreground/55 dark:text-white/45">
                            <span className="tabular-nums">{nextFireBatch.totalRuns}</span> incoming{nextFireMix ? ` · ${nextFireMix}` : ''}
                          </div>
                        )}
                        {!nextFireBatch && liveNowCount > 0 && (
                          <div className="mt-1 text-[8px] font-black uppercase tracking-[0.11em] text-foreground/55 dark:text-white/45">
                            <span className="tabular-nums">{liveNowCount}</span> processing{liveNowMix ? ` · ${liveNowMix}` : ''}
                          </div>
                        )}
                      </div>
                    </div>

                    {/* ── 24h day-shape timeline ── */}
                    <div className="mt-5">
                      <div className="mb-1.5 flex items-center justify-between text-[7px] font-black uppercase tracking-[0.16em] text-foreground/30 dark:text-white/22 tabular-nums">
                        <span>00</span>
                        <span>06</span>
                        <span>12</span>
                        <span>18</span>
                        <span>24 IST</span>
                      </div>
                      <div className="relative h-[34px]">
                        <div
                          className="absolute inset-0 grid items-end"
                          style={{ gridTemplateColumns: 'repeat(24, minmax(0, 1fr))', gap: '2px' }}
                        >
                          {dayShape.bins.map((bin, hour) => {
                            const total = bin.scheduled + bin.completed;
                            const peak = dayShape.peak || 1;
                            const heightPct = total > 0 ? Math.max(20, (total / peak) * 100) : 0;
                            const completedPct = total > 0 ? (bin.completed / total) * 100 : 0;
                            const isPast = hour < Math.floor(dayProgress * 24);
                            const isCurrent = hour === Math.floor(dayProgress * 24);
                            return (
                              <div key={hour} className="relative h-full">
                                {total === 0 ? (
                                  <div
                                    className={cn(
                                      'absolute bottom-0 left-0 right-0 rounded-[2px]',
                                      isCurrent
                                        ? 'h-[6px] bg-foreground/[0.14] dark:bg-white/[0.10]'
                                        : 'h-[3px] bg-foreground/[0.05] dark:bg-white/[0.04]',
                                    )}
                                  />
                                ) : (
                                  <motion.div
                                    initial={false}
                                    animate={{ height: `${heightPct}%` }}
                                    transition={{ duration: 0.5, ease: APPLE_EASE }}
                                    className={cn(
                                      'absolute bottom-0 left-0 right-0 flex flex-col-reverse overflow-hidden rounded-[3px]',
                                      isCurrent ? 'shadow-[0_0_10px_rgba(225,29,72,0.5)]' : '',
                                    )}
                                  >
                                    <div
                                      className={cn(
                                        'w-full',
                                        isPast || isCurrent ? 'bg-[#E11D48]' : 'bg-[#E11D48]/55',
                                      )}
                                      style={{ height: `${completedPct}%` }}
                                    />
                                    <div
                                      className={cn(
                                        'w-full',
                                        isCurrent
                                          ? 'bg-[#E11D48]/30'
                                          : isPast
                                            ? 'bg-foreground/[0.12] dark:bg-white/[0.10]'
                                            : 'bg-foreground/[0.16] dark:bg-white/[0.14]',
                                      )}
                                      style={{ height: `${100 - completedPct}%` }}
                                    />
                                  </motion.div>
                                )}
                              </div>
                            );
                          })}
                        </div>
                        <motion.div
                          className="pointer-events-none absolute top-0 bottom-0 w-px bg-[#FB7185]"
                          animate={{ left: `${dayProgress * 100}%` }}
                          transition={{ duration: 0.6, ease: APPLE_EASE }}
                          style={{ filter: 'drop-shadow(0 0 4px rgba(251,113,133,0.7))' }}
                        >
                          <div className="absolute -top-[3px] left-1/2 h-[5px] w-[5px] -translate-x-1/2 rounded-full bg-[#FB7185] shadow-[0_0_6px_rgba(251,113,133,0.9)]" />
                        </motion.div>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Divider between activity and settings */}
                <div className="mt-5 mb-5 h-px w-full bg-foreground/[0.06] dark:bg-white/[0.06]" />

                <div className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/50 mb-4">Settings & Alerts</div>

                <div className="space-y-4 flex-1 flex flex-col">

                  {/* ── Lights Out (Dark Mode Toggle) ── */}
                  <div className={cn(
                    'w-full flex items-center justify-between rounded-[20px] px-5 py-[18px]',
                    'bg-gradient-to-b from-white/90 to-white/60',
                    'border border-white/90 border-t-white',
                    'shadow-[inset_0_2px_4px_rgba(255,255,255,1),inset_0_-1px_2px_rgba(0,0,0,0.04),0_6px_16px_-4px_rgba(15,23,42,0.08),0_2px_4px_rgba(0,0,0,0.03)]',
                    'dark:bg-gradient-to-b dark:from-white/[0.06] dark:to-white/[0.015]',
                    'dark:border-white/[0.06] dark:border-t-white/[0.1]',
                    'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.06),inset_0_-1px_0_rgba(0,0,0,0.5),0_8px_20px_-4px_rgba(0,0,0,0.5)]',
                    'transition-all duration-500'
                  )}>
                    <div className="flex items-center gap-3.5">
                      <div className={cn(
                        'flex items-center justify-center w-8 h-8 rounded-[10px]',
                        'bg-white/70 border border-white/90',
                        'shadow-[inset_0_1px_2px_rgba(255,255,255,1),0_2px_6px_rgba(0,0,0,0.06)]',
                        'dark:bg-black/50 dark:border-white/[0.05]',
                        'dark:shadow-[inset_0_2px_4px_rgba(0,0,0,0.5),0_1px_0_rgba(255,255,255,0.04)]',
                        'transition-all duration-300'
                      )}>
                        <AnimatePresence mode="wait">
                          {isDarkMode ? (
                            <motion.div key="moon" initial={{ rotate: -90, opacity: 0 }} animate={{ rotate: 0, opacity: 1 }} exit={{ rotate: 90, opacity: 0 }} transition={{ duration: 0.3 }}>
                              <Moon size={15} className="text-foreground/50 dark:text-white/60" />
                            </motion.div>
                          ) : (
                            <motion.div key="sun" initial={{ rotate: 90, opacity: 0 }} animate={{ rotate: 0, opacity: 1 }} exit={{ rotate: -90, opacity: 0 }} transition={{ duration: 0.3 }}>
                              <Sun size={15} className="text-[#E11D48]" />
                            </motion.div>
                          )}
                        </AnimatePresence>
                      </div>
                      <span className="text-[11px] font-black uppercase tracking-[0.14em] text-foreground/70 dark:text-white/70">Lights Out</span>
                    </div>
                    <div ref={themeToggleRef} className="cursor-pointer group" onClick={() => {
                      const newIsDarkMode = !isDarkMode;
                      // Get toggle position for ripple origin
                      const rect = themeToggleRef.current?.getBoundingClientRect();
                      const x = rect ? rect.left + rect.width / 2 : window.innerWidth / 2;
                      const y = rect ? rect.top + rect.height / 2 : window.innerHeight / 2;
                      // Create ripple at zero radius
                      setThemeRipple({ active: false, x, y, toDark: newIsDarkMode });
                      // Trigger expansion in next frame
                      requestAnimationFrame(() => {
                        requestAnimationFrame(() => {
                          setThemeRipple({ active: true, x, y, toDark: newIsDarkMode });
                        });
                      });
                      // Switch theme classes after ripple covers enough area
                      setTimeout(() => {
                        setIsDarkMode(newIsDarkMode);
                        if (newIsDarkMode) {
                          document.documentElement.classList.add('dark');
                          document.documentElement.classList.remove('light');
                          document.documentElement.style.colorScheme = 'dark';
                          localStorage.setItem('theme', 'dark');
                        } else {
                          document.documentElement.classList.remove('dark');
                          document.documentElement.classList.add('light');
                          document.documentElement.style.colorScheme = 'light';
                          localStorage.setItem('theme', 'light');
                        }
                      }, 260);
                    }}>
                      <div className="group-active:scale-90 transition-transform duration-200"><HardwareToggle active={isDarkMode} /></div>
                    </div>
                  </div>

                  {/* ── Device Fire Alerts ── */}
                  <div className={cn(
                    'w-full flex items-center justify-between gap-3 rounded-[20px] px-5 py-[18px]',
                    'transition-all duration-500',
                    pwaNotificationsEnabled
                      ? [
                          'bg-[#111]',
                          'border border-black',
                          'shadow-[inset_0_1px_1px_rgba(255,255,255,0.08),0_10px_24px_-10px_rgba(0,0,0,0.45),0_0_22px_rgba(225,29,72,0.12)]',
                          'dark:shadow-[inset_0_1px_1px_rgba(255,255,255,0.08),0_18px_32px_-14px_rgba(0,0,0,0.65),0_0_28px_rgba(225,29,72,0.15)]',
                        ]
                      : notificationPermission === 'denied'
                        ? [
                            'bg-[linear-gradient(180deg,rgba(255,122,0,0.14),rgba(255,255,255,0.85))]',
                            'border border-[#ff9d4d]/45',
                            'shadow-[inset_0_1px_2px_rgba(255,255,255,0.8),0_8px_20px_-10px_rgba(255,122,0,0.3)]',
                            'dark:bg-[linear-gradient(180deg,rgba(255,107,0,0.16),rgba(255,255,255,0.03))]',
                            'dark:border-[#FF6B00]/30',
                          ]
                        : [
                            'bg-gradient-to-b from-white/90 to-white/60',
                            'border border-white/90 border-t-white',
                            'shadow-[inset_0_2px_4px_rgba(255,255,255,1),inset_0_-1px_2px_rgba(0,0,0,0.04),0_6px_16px_-4px_rgba(15,23,42,0.08)]',
                            'dark:bg-gradient-to-b dark:from-white/[0.06] dark:to-white/[0.015]',
                            'dark:border-white/[0.06] dark:border-t-white/[0.1]',
                            'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.06),inset_0_-1px_0_rgba(0,0,0,0.5),0_8px_20px_-4px_rgba(0,0,0,0.5)]',
                          ]
                  )}>
                    <div className="flex min-w-0 flex-1 items-center gap-3.5 pr-3">
                      <div className={cn(
                        'flex items-center justify-center w-8 h-8 rounded-[10px] transition-all duration-300',
                        'shrink-0',
                        pwaNotificationsEnabled
                          ? 'bg-[#E11D48] shadow-[0_6px_14px_rgba(225,29,72,0.22),inset_0_1px_1px_rgba(255,255,255,0.6)]'
                          : notificationPermission === 'denied'
                            ? 'bg-[#FF6B00]/15 border border-[#FF6B00]/25'
                            : [
                                'bg-white/70 border border-white/90',
                                'shadow-[inset_0_1px_2px_rgba(255,255,255,1),0_2px_6px_rgba(0,0,0,0.06)]',
                                'dark:bg-black/50 dark:border-white/[0.05]',
                                'dark:shadow-[inset_0_2px_4px_rgba(0,0,0,0.5),0_1px_0_rgba(255,255,255,0.04)]',
                              ]
                      )}>
                        <Bell size={15} className={cn(
                          pwaNotificationsEnabled
                            ? 'text-black'
                            : notificationPermission === 'denied'
                              ? 'text-[#FF6B00]'
                              : 'text-foreground/35 dark:text-white/30',
                          'transition-colors duration-300'
                        )} />
                      </div>
                      <div className="min-w-0 flex-1">
                        <div className={cn(
                          'text-[11px] font-black uppercase tracking-[0.14em] transition-colors duration-300',
                          pwaNotificationsEnabled ? 'text-white' : 'text-foreground/70 dark:text-white/70'
                        )}>
                          Fire Alerts
                        </div>
                        <div className={cn(
                          'mt-1 text-[9px] font-bold uppercase tracking-[0.12em] leading-relaxed',
                          pwaNotificationsEnabled
                            ? 'text-[#E11D48]/70'
                            : notificationPermission === 'denied'
                              ? 'text-[#9f4b00] dark:text-[#ffb278]'
                              : 'text-foreground/45 dark:text-white/38'
                        )}>
                          {pwaStatusText}
                        </div>
                        <div className={cn(
                          'mt-1 text-[8px] font-bold uppercase tracking-[0.12em]',
                          pwaNotificationsEnabled
                            ? 'text-white/42'
                            : 'text-foreground/38 dark:text-white/28'
                        )}>
                          iPhone: install FeedMe to Home Screen for alerts.
                        </div>
                      </div>
                    </div>
                    <div className="flex shrink-0 items-center gap-2.5 sm:gap-3">
                      {notificationPermission === 'granted' && (
                        <button
                          type="button"
                          disabled={notificationBusy || notificationTestBusy || !pwaNotificationsEnabled}
                          onClick={handleTestNotification}
                          className={cn(
                            'rounded-full px-3 py-1.5 text-[8px] font-black uppercase tracking-[0.16em] transition-all duration-200',
                            'border border-white/85 bg-white/72 text-foreground/78 shadow-[inset_0_1px_0_rgba(255,255,255,0.92),0_4px_12px_rgba(15,23,42,0.08)]',
                            'dark:border-white/10 dark:bg-white/[0.06] dark:text-white/72 dark:shadow-[0_8px_20px_rgba(0,0,0,0.35),inset_0_1px_0_rgba(255,255,255,0.06)]',
                            pwaNotificationsEnabled && !notificationTestBusy && 'hover:-translate-y-0.5 hover:shadow-[inset_0_1px_0_rgba(255,255,255,0.92),0_6px_16px_rgba(15,23,42,0.1)] dark:hover:shadow-[0_10px_24px_rgba(0,0,0,0.45),inset_0_1px_0_rgba(255,255,255,0.06)]',
                            (!pwaNotificationsEnabled || notificationBusy || notificationTestBusy) && 'cursor-not-allowed opacity-55'
                          )}
                        >
                          {notificationTestBusy ? 'Sending...' : 'Send test'}
                        </button>
                      )}
                      <div className={cn(
                        'hidden rounded-full px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.18em] sm:flex items-center gap-1.5',
                        pwaNotificationsEnabled
                          ? 'bg-[#E11D48]/14 text-[#E11D48] border border-[#E11D48]/18'
                          : notificationPermission === 'denied'
                            ? 'bg-[#FF6B00]/10 text-[#FF6B00] border border-[#FF6B00]/20'
                            : 'bg-black/5 text-foreground/45 border border-black/5 dark:bg-white/5 dark:text-white/40 dark:border-white/[0.06]'
                      )}>
                        {notificationPermission === 'granted' ? <Unlock size={10} strokeWidth={2.4} /> : <Lock size={10} strokeWidth={2.4} />}
                        <span>{pwaBadgeText}</span>
                      </div>
                      <button
                        type="button"
                        disabled={notificationBusy || notificationPermission === 'unsupported'}
                        className={cn(
                          'group cursor-pointer disabled:cursor-not-allowed',
                          notificationPermission === 'unsupported' && 'opacity-40'
                        )}
                        onClick={handlePwaNotificationToggle}
                      >
                        <div className="group-active:scale-90 transition-transform duration-200">
                          <HardwareToggle active={pwaNotificationsEnabled} />
                        </div>
                      </button>
                    </div>
                  </div>

                  {/* ── Alert Threshold — Auto-Lock on Release ── */}
                  <div className={cn(
                    'w-full flex-1 flex flex-col rounded-[20px] px-5 pt-5 pb-4 relative overflow-hidden',
                    'bg-gradient-to-b from-white/95 to-white/65',
                    'border border-white/90 border-t-white',
                    'shadow-[inset_0_2px_4px_rgba(255,255,255,1),inset_0_-1px_2px_rgba(0,0,0,0.04),0_8px_24px_-6px_rgba(15,23,42,0.09),0_2px_4px_rgba(0,0,0,0.03)]',
                    'dark:bg-gradient-to-b dark:from-white/[0.06] dark:to-white/[0.015]',
                    'dark:border-white/[0.06] dark:border-t-white/[0.1]',
                    'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.06),inset_0_-1px_0_rgba(0,0,0,0.5),0_12px_32px_-6px_rgba(0,0,0,0.5)]',
                    'transition-all duration-500',
                    !alertsArmed && 'opacity-35 pointer-events-none'
                  )}>
                    {/* Header row */}
                    <div className="flex items-center justify-between w-full mb-1">
                      <div className="flex items-center gap-3">
                        <div className={cn(
                          'flex items-center justify-center w-8 h-8 rounded-[10px]',
                          'bg-white/70 border border-white/90',
                          'shadow-[inset_0_1px_2px_rgba(255,255,255,1),0_2px_6px_rgba(0,0,0,0.06)]',
                          'dark:bg-black/50 dark:border-white/[0.05]',
                          'dark:shadow-[inset_0_2px_4px_rgba(0,0,0,0.5),0_1px_0_rgba(255,255,255,0.04)]',
                        )}>
                          <Target size={15} className="text-foreground/50 dark:text-white/50" />
                        </div>
                        <div className="text-[11px] font-black uppercase tracking-[0.14em] text-foreground/70 dark:text-white/70">Threshold</div>
                      </div>

                      {/* The hero number display with lock state */}
                      <div className="flex items-center gap-2">
                        <motion.div
                          key={`${alertThreshold}-${thresholdLocked}`}
                          initial={{ scale: 0.85, opacity: 0.5 }}
                          animate={{ scale: 1, opacity: 1 }}
                          transition={{ duration: 0.3, ease: [0.25, 0.1, 0.25, 1] }}
                          className={cn(
                            'relative flex items-center justify-center min-w-[56px] h-[34px] rounded-[10px] px-2.5',
                            thresholdLocked
                              ? 'bg-[#E11D48]/10 border border-[#E11D48]/20 dark:bg-[#E11D48]/[0.06] dark:border-[#E11D48]/15'
                              : 'bg-black/[0.04] border border-black/[0.06] dark:bg-white/[0.04] dark:border-white/[0.06]'
                          )}
                        >
                          <span className={cn(
                            'text-[18px] font-black tabular-nums tracking-tight',
                            thresholdLocked
                              ? 'text-[#BE123C] dark:text-[#E11D48] dark:drop-shadow-[0_0_10px_rgba(225,29,72,0.3)]'
                              : 'text-foreground/80 dark:text-white/80',
                            'transition-colors duration-300'
                          )}>
                            {alertThreshold}%
                          </span>
                          {/* Lock seal overlay */}
                          <AnimatePresence>
                            {thresholdLocked && (
                              <motion.div
                                initial={{ scale: 0, opacity: 0 }}
                                animate={{ scale: 1, opacity: 1 }}
                                exit={{ scale: 0.5, opacity: 0 }}
                                transition={{ type: 'spring', stiffness: 400, damping: 20 }}
                                className="absolute -top-1.5 -right-1.5 flex items-center justify-center w-4 h-4 rounded-full bg-[#E11D48] shadow-[0_2px_6px_rgba(225,29,72,0.4),inset_0_1px_1px_rgba(255,255,255,0.6)]"
                              >
                                <Check size={9} strokeWidth={3.5} className="text-black" />
                              </motion.div>
                            )}
                          </AnimatePresence>
                        </motion.div>
                      </div>
                    </div>

                    {/* Slider Track — Redesigned Deep Channel */}
                    <div className="relative mt-4 mb-2 flex items-center">
                      {/* Recessed track channel */}
                      <div className={cn(
                        'relative w-full h-[10px] rounded-full overflow-visible',
                        'bg-black/[0.06] border border-black/[0.04]',
                        'shadow-[inset_0_2px_6px_rgba(0,0,0,0.08),inset_0_-1px_1px_rgba(255,255,255,0.6)]',
                        'dark:bg-black/70 dark:border-white/[0.03]',
                        'dark:shadow-[inset_0_3px_8px_rgba(0,0,0,0.8),inset_0_-1px_1px_rgba(255,255,255,0.04)]',
                      )}>
                        {/* Invisible native range input — touch-friendly */}
                        <input
                          type="range"
                          min={1}
                          max={100}
                          value={alertThreshold}
                          onMouseDown={() => {
                            if (lockRafRef.current) { cancelAnimationFrame(lockRafRef.current); lockRafRef.current = null; }
                            setThresholdLocked(false); setIsDraggingSlider(true);
                          }}
                          onMouseUp={() => { setIsDraggingSlider(false); startAutoLock(); }}
                          onTouchStart={() => {
                            if (lockRafRef.current) { cancelAnimationFrame(lockRafRef.current); lockRafRef.current = null; }
                            setThresholdLocked(false); setIsDraggingSlider(true);
                          }}
                          onTouchEnd={() => { setIsDraggingSlider(false); startAutoLock(); }}
                          onChange={(e) => {
                            const next = clampAlertThreshold(Number(e.target.value));
                            setAlertThreshold(next);
                            localStorage.setItem(FUND_ALERT_THRESHOLD_KEY, String(next));
                          }}
                          className="absolute inset-0 z-20 w-full h-full opacity-0 cursor-pointer"
                          style={{ WebkitAppearance: 'none', margin: 0, padding: '12px 0', height: '34px', top: '-12px' }}
                        />

                        {/* Filled portion */}
                        <motion.div
                          initial={{ width: 0 }}
                          animate={{ width: `${alertThreshold}%` }}
                          transition={{ duration: 0.15, ease: 'easeOut' }}
                          className={cn(
                            'absolute inset-y-[2px] left-[2px] rounded-full',
                            'bg-[#E11D48] shadow-[0_0_14px_rgba(225,29,72,0.45),inset_0_1px_2px_rgba(255,255,255,0.8),inset_0_-1px_2px_rgba(0,0,0,0.15)]',
                            isDraggingSlider && 'shadow-[0_0_22px_rgba(225,29,72,0.65),inset_0_1px_2px_rgba(255,255,255,1),inset_0_-1px_3px_rgba(0,0,0,0.2)]',
                            'transition-shadow duration-200'
                          )}
                        />

                        {/* Thumb knob */}
                        <motion.div
                          animate={{
                            left: `calc(${alertThreshold}% - 10px)`,
                            scale: isDraggingSlider ? 1.25 : 1
                          }}
                          transition={{ duration: 0.12, ease: 'easeOut' }}
                          className={cn(
                            'absolute top-1/2 -translate-y-1/2 h-[22px] w-[22px] rounded-full pointer-events-none z-10',
                            'bg-gradient-to-b from-white to-[#f0f0f0]',
                            'border border-white/80',
                            'shadow-[0_3px_10px_rgba(0,0,0,0.25),0_1px_3px_rgba(0,0,0,0.15),inset_0_2px_3px_rgba(255,255,255,1),inset_0_-2px_3px_rgba(0,0,0,0.08)]',
                            'dark:from-[#2a2a2a] dark:to-[#1a1a1a] dark:border-white/10',
                            'dark:shadow-[0_3px_12px_rgba(0,0,0,0.7),0_1px_3px_rgba(0,0,0,0.5),inset_0_2px_3px_rgba(255,255,255,0.12),inset_0_-2px_3px_rgba(0,0,0,0.3)]',
                            isDraggingSlider && 'dark:shadow-[0_6px_20px_rgba(225,29,72,0.25),0_3px_8px_rgba(0,0,0,0.6),inset_0_2px_3px_rgba(255,255,255,0.15)]',
                            'transition-opacity duration-300'
                          )}
                        >
                          {/* Inner dot indicator */}
                          <div className={cn(
                            'absolute inset-0 m-auto w-[6px] h-[6px] rounded-full',
                            thresholdLocked ? 'bg-black/10 dark:bg-white/10' : 'bg-[#E11D48]/80 shadow-[0_0_6px_rgba(225,29,72,0.4)]',
                            'transition-all duration-300'
                          )} />
                        </motion.div>
                      </div>
                    </div>

                    {/* Status line */}
                    <div className="text-[8px] font-bold uppercase tracking-[0.14em] text-foreground/35 dark:text-white/30 leading-relaxed mt-2">
                      {thresholdLocked
                        ? alertsArmed
                          ? `Active — fire alerts below ${alertThreshold}%`
                          : `Turn on Fire Alerts to use the ${alertThreshold}% fire line`
                        : isDraggingSlider ? 'Adjusting…' : 'Locking in…'}
                    </div>
                  </div>

                </div>
              </motion.div>
            </div>

            {/* Bottom Section: Unified Feed Coverage & Feed-Wise Usage (The Crown Jewel) */}
            <motion.div variants={tileVariant}>
              <div className="text-[12px] font-black uppercase tracking-[0.16em] text-foreground/50 mb-4 px-2 select-none">Feed Bundles & Usage</div>
              <div className="grid gap-5 xl:grid-cols-2 xl:gap-6">
                {feeds.length === 0 && (
                    <div className="col-span-full py-12 text-center text-[11px] font-black uppercase tracking-[0.16em] text-foreground/30">
                      No structured feeds
                    </div>
                )}
                {feedWiseUsage.map((feed) => {
                  const isPrimed = primedBundleId === feed.id;
                  
                  return (
                    <div 
                      key={feed.id} 
                      className="block group cursor-pointer"
	                      onClick={(e) => {
	                        // If it's already primed, let the navigation happen (we'll implement programmatic push or just use a Link wrapper around the content)
	                        if (isPrimed) {
	                          router.push(`/?id=${feed.id}`);
	                        } else {
                          // Prime it on first tap
                          e.preventDefault();
                          setPrimedBundleId(feed.id);
                          
                          // Unprime after 3 seconds if no second tap
                          setTimeout(() => {
                            setPrimedBundleId(current => current === feed.id ? null : current);
                          }, 3000);
                        }
                      }}
                    >
                     {/* massive premium Liquid Glass card for each Feed Bundle */}
                     <motion.div whileTap={{ scale: 0.98 }} className={cn(
                      'fm-depth-glass relative overflow-hidden rounded-[32px] p-6 h-full flex flex-col lg:p-7',
                      'bg-white/70 border border-white/80',
                      'shadow-[inset_0_1px_0_rgba(255,255,255,0.9),inset_0_-1px_0_rgba(0,0,0,0.04),0_12px_40px_-4px_rgba(15,23,42,0.06)]',
                      'dark:bg-[rgba(10,10,10,0.65)] dark:border-white/[0.06] dark:border-t-white/[0.1]',
                      'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),inset_0_-1px_0_rgba(0,0,0,0.5),0_24px_56px_rgba(0,0,0,0.5)]',
                      'transition-colors duration-500',
                      isPrimed && 'dark:bg-[rgba(15,15,15,0.85)] bg-white/90 ring-1 ring-[#E11D48]/40'
                    )}>
                      {/* Card Header (Bundle Info & Link Arrow) */}
                      <div className="flex items-start justify-between mb-6">
                        <div>
                           <div className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/50 mb-1">Bundle</div>
                           <h3 className="text-[24px] font-black uppercase tracking-[-0.04em] text-foreground dark:text-white/95 leading-none">{feed.title}</h3>
                        </div>
                        <div className={cn(
                          'flex items-center gap-4',
                        )}>
                          <div className="text-right">
                             <div className="text-[9px] font-black uppercase tracking-[0.12em] text-foreground/40 mb-1">Total Posts</div>
                             <div className="text-[16px] font-black text-foreground/80 dark:text-white/80 leading-none">{feed.totalPosts}</div>
                          </div>
                          
                          {/* Elevated Linking Arrow Button with 2-Tap Fill Logic */}
                          <div className="flex flex-col items-center">
                            <div className={cn(
                              'relative flex h-10 w-10 items-center justify-center rounded-full overflow-hidden',
                              'bg-white/90 border border-white/90 shadow-[0_4px_12px_rgba(0,0,0,0.05),inset_0_2px_4px_rgba(255,255,255,1)]',
                              'dark:bg-black dark:border-white/5 dark:shadow-[inset_0_4px_8px_rgba(0,0,0,0.8),inset_0_-1px_1px_rgba(255,255,255,0.06),0_1px_1px_rgba(255,255,255,0.05)]',
                              'transition-all duration-300',
                              isPrimed && 'dark:shadow-[0_8px_24px_rgba(225,29,72,0.4)]'
                            )}>
                              {/* The Neon Fill Animation */}
                              <motion.div 
                                initial={{ y: '100%' }}
                                animate={{ y: isPrimed ? '0%' : '100%' }}
                                transition={{ duration: 0.3, ease: 'easeOut' }}
                                className="absolute inset-0 bg-[#E11D48] shadow-[inset_0_2px_4px_rgba(255,255,255,0.8)]"
                              />
                              <ArrowUpRight 
                                size={18} 
                                strokeWidth={2.5} 
                                className={cn(
                                  "relative z-10 transition-colors duration-300",
                                  isPrimed ? "text-black drop-shadow-sm" : "text-foreground/40 dark:text-white/40"
                                )} 
                              />
                            </div>
                            <AnimatePresence>
                              {isPrimed && (
                                <motion.div 
                                  initial={{ opacity: 0, y: 5 }}
                                  animate={{ opacity: 1, y: 0 }}
                                  exit={{ opacity: 0, y: 3 }}
                                  className="absolute -bottom-5 right-6 text-[8px] font-black uppercase tracking-[0.14em] text-[#E11D48]"
                                >
                                  Tap To Enter
                                </motion.div>
                              )}
                            </AnimatePresence>
                          </div>
                        </div>
                      </div>

                      {/* Hardware Battery Gauges (Feeders Usage) */}
                      <div className="space-y-4">
                         {feed.feeders.map(f => (
                           <div key={f.handle} className="grid grid-cols-[100px_1fr_40px] items-center gap-4">
                             <span className="text-[11px] font-black uppercase tracking-[0.1em] text-foreground/80 dark:text-white/80 truncate">@{f.handle}</span>
                             
                             {/* Cylindrical Recessed Hardware Meter */}
                             <div className="relative h-3.5 w-full rounded-full bg-black/5 border border-black/5 dark:bg-black/80 dark:border-transparent dark:shadow-[inset_0_3px_6px_rgba(0,0,0,0.8),inset_0_-1px_1px_rgba(255,255,255,0.06),0_1px_1px_rgba(255,255,255,0.05)] overflow-hidden">
                               <motion.div 
                                 initial={{ width: 0 }}
                                 animate={{ width: `${f.pct}%` }}
                                 transition={{ duration: 1, ease: 'easeOut', delay: 0.1 }}
                                 className="absolute inset-[1.5px] rounded-full bg-[#E11D48] shadow-[0_0_12px_rgba(225,29,72,0.4),inset_0_1px_2px_rgba(255,255,255,0.7),inset_0_-1px_2px_rgba(0,0,0,0.15)]"
                               />
                             </div>

                             <span className="text-[11px] font-black text-right text-foreground/60 dark:text-white/60">{f.count}<span className="text-foreground/30 dark:text-white/30 text-[9px] font-bold">/{slotPostsCap}</span></span>
                           </div>
                         ))}
                      </div>
                     </motion.div>
                  </div>
                );
              })}
              </div>
            </motion.div>

          </motion.div>
          <div className="h-16 w-full" />
        </div>
      </div>
    </motion.div>
  );
}
