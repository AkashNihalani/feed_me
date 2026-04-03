'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { getSupabase, User } from '@/lib/supabase';
import { cn, formatShortIST } from '@/lib/utils';
import { getCache, setCache } from '@/lib/pageCache';
import { useAppHaptics } from '@/lib/haptics';
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
  mediaType?: string;
  handle?: string;
  status: string;
  scheduledAt: string;
  completedAt?: string;
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

function formatRunBatchMoment(start?: string, end?: string) {
  if (!start) return '--';
  try {
    const startDate = new Date(start);
    const endDate = end ? new Date(end) : startDate;
    const day = new Intl.DateTimeFormat('en-IN', {
      month: 'short',
      day: 'numeric',
      timeZone: 'Asia/Kolkata',
    }).format(startDate);
    const timeFormatter = new Intl.DateTimeFormat('en-IN', {
      hour: 'numeric',
      minute: '2-digit',
      hour12: true,
      timeZone: 'Asia/Kolkata',
    });
    const startTime = timeFormatter.format(startDate);
    const endTime = timeFormatter.format(endDate);
    if (!end || startDate.getTime() === endDate.getTime()) {
      return `${day} · ${startTime}`;
    }
    return `${day} · ${startTime} to ${endTime}`;
  } catch {
    return '--';
  }
}

function formatBusinessDayRange(start?: string | null, end?: string | null) {
  if (!start) return null;
  try {
    const formatDay = (value: string) =>
      new Intl.DateTimeFormat('en-IN', {
        month: 'short',
        day: 'numeric',
        timeZone: 'Asia/Kolkata',
      }).format(new Date(`${value}T00:00:00+05:30`));

    if (!end || end === start) {
      return `Tracking posts from ${formatDay(start)}`;
    }
    return `Tracking posts from ${formatDay(start)} to ${formatDay(end)}`;
  } catch {
    return start === end || !end ? `Tracking posts from ${start}` : `Tracking posts from ${start} to ${end}`;
  }
}

function formatBatchSummary(batch: EngineRunBatch) {
  const parts: string[] = [];
  if (batch.creatorCount > 0) {
    parts.push(`${batch.creatorCount} account${batch.creatorCount === 1 ? '' : 's'}`);
  }
  if (batch.systemCount > 0) {
    parts.push(`${batch.systemCount} system`);
  }
  return parts.length > 0 ? parts.join(' · ') : `${batch.totalRuns} check${batch.totalRuns === 1 ? '' : 's'}`;
}

function formatBatchProgress(statuses: EngineRunBatchBreakdown[]) {
  if (!statuses.length) return '--';
  return statuses
    .slice(0, 3)
    .map((item) => {
      const label = item.label.toLowerCase();
      const icon = label === 'done' || label === 'live' ? '✓' : label === 'failed' ? '✕' : '◷';
      return `${icon} ${item.count}`;
    })
    .join(' · ');
}

const RUN_TYPE_LABELS: Record<string, string> = {
  D1: 'Day 1',
  D3: 'Day 3',
  D7: 'Day 7',
  D21: 'Day 21',
  DAILY: 'Discovery',
  POLL: 'Discovery',
  FOLLOWERS: 'Followers',
  REPAIR: 'Repair',
};

function humanRunTypeLabel(raw: string) {
  return RUN_TYPE_LABELS[raw.toUpperCase()] || raw;
}

function parseMetric(value: string | number | undefined) {
  if (typeof value === 'number') return value;
  if (!value) return 0;
  const raw = String(value).replace(/,/g, '');
  const n = Number(raw);
  return Number.isFinite(n) ? n : 0;
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
              'bg-[#CCFF00]',
              'shadow-[inset_0_3px_6px_rgba(130,156,0,0.55),inset_0_-1px_2px_rgba(255,255,255,0.35),0_2px_6px_rgba(204,255,0,0.15)]',
              'dark:shadow-[inset_0_3px_8px_rgba(0,0,0,0.5),inset_0_-1px_2px_rgba(255,255,255,0.15),0_0_16px_rgba(204,255,0,0.2),0_4px_12px_rgba(0,0,0,0.4)]',
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
    async function fetchData() {
      const { data: { user: authUser }, error: authError } = await getSupabase().auth.getUser();
      if (authError || !authUser) {
        window.location.href = '/login';
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
            total_runs: 0,
            data_points: 0,
            success_rate: 0,
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
  }, []);

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

  const totalFeeds = feeds.length;
  const totalFeeders = feeds.reduce((sum, feed) => sum + (feed.feeders?.length || 0), 0);
  const slotsUsed = slots.used ?? 0;
  const slotPlanPrice = slots.plan?.price ?? 499;
  const slotPostsCap = slots.plan?.postsCap ?? 30;
  const monthlySpend = slotsUsed * slotPlanPrice;
  const lastScrape = engineStats.recentJobs[0]?.updatedAt || engineStats.recentJobs[0]?.createdAt || '';
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
  const upcomingRuns = useMemo(
    () =>
      [...engineStats.queuedBatches]
        .sort((a, b) => new Date(a.windowStart).getTime() - new Date(b.windowStart).getTime())
        .slice(0, 5),
    [engineStats.queuedBatches],
  );
  const recentRuns = useMemo(
    () =>
      [...engineStats.completedBatches]
        .sort((a, b) => new Date(b.windowStart).getTime() - new Date(a.windowStart).getTime())
        .slice(0, 5),
    [engineStats.completedBatches],
  );
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
      initial={{ opacity: 0, y: 14, scale: 0.992 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ duration: 0.42, ease: APPLE_EASE }}
      className={cn(
        'relative w-full text-foreground select-none',
        useTranslucentBrowserChrome ? 'bg-transparent' : 'bg-background',
        useBrowserPageScroll ? 'overflow-visible' : 'overflow-hidden',
      )}
      style={appShellStyle}
    >
      <div
        className={cn(
          'pointer-events-none fixed inset-0 z-0',
          useTranslucentBrowserChrome
            ? 'bg-[radial-gradient(circle_at_top,_rgba(28,28,28,0.96)_0%,_rgba(8,8,8,0.92)_42%,_rgba(0,0,0,0.84)_100%)]'
            : 'bg-[#f4f7f9] dark:bg-[#030303]',
        )}
      />

      <AnimatePresence>
        {showManageSubscriptionModal && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="fixed inset-0 z-[180] flex items-end justify-center sm:items-center sm:px-4 sm:py-6"
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
                boxShadow: '0 0 80px rgba(204,255,0,0.06), 0 40px 100px rgba(0,0,0,0.7)',
              }}
            >
              {/* Top accent line */}
              <div className="absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-[#CCFF00]/30 to-transparent" />

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

                <div className="text-[9px] font-black uppercase tracking-[0.2em] text-[#CCFF00]/50 sm:text-[10px]">Feed Pass</div>
                <div className="mt-2 text-[26px] font-black leading-none tracking-[-0.04em] text-white sm:text-[32px]">
                  Subscription
                </div>
              </div>

              {/* ── Vibrant total pill ── */}
              <div className="px-5 pt-5 sm:px-7 sm:pt-6">
                <div
                  className="relative overflow-hidden rounded-[18px] bg-[#CCFF00] px-5 py-4 sm:rounded-[20px] sm:px-6 sm:py-5"
                  style={{ boxShadow: '0 12px 40px rgba(204,255,0,0.16), 0 0 80px rgba(204,255,0,0.06)' }}
                >
                  {/* Subtle inner glow */}
                  <div className="pointer-events-none absolute inset-0" style={{ background: 'linear-gradient(180deg, rgba(255,255,255,0.12) 0%, transparent 40%, rgba(0,0,0,0.04) 100%)' }} />
                  <div className="relative flex items-end justify-between gap-4">
                    <div>
                      <div className="text-[8px] font-black uppercase tracking-[0.16em] text-black/40 sm:text-[9px]">Monthly total</div>
                      <div className="mt-1 text-[38px] font-black leading-none tracking-[-0.05em] text-black sm:text-[46px]">
                        ₹{billingSummary.total.toLocaleString('en-IN')}
                      </div>
                    </div>
                    <div className="mb-1 rounded-full bg-black/10 px-3 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-black/55 sm:text-[9px]">
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
                      billingSummary.overageCost > 0 ? 'text-[#FF55A3]' : 'text-white/30'
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
                        <div className="ml-3 text-[13px] font-black text-[#FF55A3] sm:text-[14px]">
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
                  className="group flex w-full items-center justify-between rounded-[16px] border border-white/[0.06] bg-white/[0.03] px-4 py-3.5 text-left transition-all duration-200 hover:border-[#CCFF00]/16 hover:bg-[#CCFF00]/[0.04] active:scale-[0.98] disabled:cursor-not-allowed disabled:opacity-50 sm:rounded-[18px] sm:px-5 sm:py-4"
                >
                  <div className="flex items-center gap-3">
                    <div
                      className="flex h-9 w-9 items-center justify-center rounded-full bg-[#CCFF00] text-black sm:h-10 sm:w-10"
                      style={{ boxShadow: '0 6px 20px rgba(204,255,0,0.18)' }}
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
                  <ArrowUpRight size={16} className="text-white/20 transition-colors group-hover:text-[#CCFF00]/60" />
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
          className="fixed inset-0 z-[9999] pointer-events-none"
          style={{
            background: themeRipple.toDark ? '#030303' : '#f4f7f9',
            clipPath: themeRipple.active
              ? 'circle(150vmax at ' + themeRipple.x + 'px ' + themeRipple.y + 'px)'
              : 'circle(0px at ' + themeRipple.x + 'px ' + themeRipple.y + 'px)',
            transition: 'clip-path 0.7s cubic-bezier(0.4, 0, 0.2, 1)',
          }}
          onTransitionEnd={() => {
            if (themeRipple.active) setThemeRipple(null);
          }}
        />
      )}

      {/* ═══ MINIMAL LOCKED HEADER ═══ */}
      <div className={cn(
        'pointer-events-auto inset-x-0 top-0 z-[100] flex flex-col items-center px-2 pt-[calc(10px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-4 sm:pt-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] md:pt-[calc(20px+var(--pwa-top-fix,0px))] lg:px-4',
        useBrowserPageScroll ? 'fixed' : 'absolute',
      )}>
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
            {/* Glass Highlights */}
            <div className="pointer-events-none absolute inset-0 rounded-[32px] z-0 dark:opacity-0"
              style={{ background: 'linear-gradient(180deg, rgba(255,255,255,0.45) 0%, rgba(255,255,255,0) 30%, rgba(0,0,0,0.015) 100%)' }}
            />
            <div className="pointer-events-none absolute inset-[1px] rounded-[31px] z-0 dark:hidden"
              style={{ boxShadow: 'inset 0 2px 4px rgba(255,255,255,0.7), inset 0 -2px 6px rgba(0,0,0,0.04)' }}
            />

            <div className="relative z-10 px-3.5 py-3 sm:px-5 sm:py-3.5">
              <div className="flex items-center justify-between gap-3">
                <h1 className="text-[30px] font-black leading-none tracking-[0.14em] text-black sm:text-[38px] dark:text-white fm-depth-title">FUND</h1>
                <div className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/50">Slot Control Room</div>
              </div>
            </div>
          </div>
        </div>
      </div>

      {/* ═══ CONTENT STAGGER GRID ═══ */}
      <div className={cn(
        'z-10 pointer-events-none',
        useBrowserPageScroll ? 'relative min-h-[var(--fm-app-height,100dvh)]' : 'absolute inset-0',
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
            {/* Account Overview Bar */}
            <motion.div variants={tileVariant} className="grid grid-cols-2 gap-3 sm:grid-cols-4 lg:gap-4 xl:gap-5">
              {[
                { label: 'Active Feeds', value: totalFeeds },
                { label: 'Total Feeders', value: totalFeeders },
                { label: 'Monthly Spend', value: `₹${monthlySpend}` },
                { label: 'Last Scrape', value: lastScrape ? formatShortIST(lastScrape) : '--' },
              ].map((item) => (
                <div key={item.label} className={cn(
                  'fm-depth-chip rounded-[20px] px-4 py-3.5 text-center transition-all duration-300 xl:px-5 xl:py-4.5',
                  'bg-white/70 border border-white/80',
                  'shadow-[inset_0_1px_0_rgba(255,255,255,0.9),inset_0_-1px_0_rgba(0,0,0,0.04),0_6px_16px_rgba(15,23,42,0.06)]',
                  'dark:bg-[rgba(10,10,10,0.65)] dark:border-white/[0.06] dark:border-t-white/[0.1]',
                  'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),inset_0_-1px_0_rgba(0,0,0,0.5),0_12px_24px_rgba(0,0,0,0.4)]',
                )}>
                  <div className="text-[9px] font-black uppercase tracking-[0.16em] text-foreground/45 dark:text-white/40 xl:text-[11px]">{item.label}</div>
                  <div className="mt-1 text-[16px] font-black tracking-[-0.02em] text-foreground dark:text-white/90 drop-shadow-sm xl:text-[20px] xl:mt-2">{item.value}</div>
                </div>
              ))}
            </motion.div>

            {/* Launch Sequence Removed per User Request */}

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

                <div className={cn(
                  'fm-depth-glass rounded-[30px] p-4 relative overflow-hidden flex flex-col gap-4 lg:min-h-[236px] lg:p-5 xl:p-5 2xl:p-6',
                  'bg-white/72 border border-white/82',
                  'shadow-[inset_0_1px_0_rgba(255,255,255,0.9),inset_0_-1px_0_rgba(0,0,0,0.04),0_12px_32px_-4px_rgba(15,23,42,0.07)]',
                  'dark:bg-[rgba(10,10,10,0.72)] dark:border-white/[0.06] dark:border-t-white/[0.1]',
                  'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.05),inset_0_-1px_0_rgba(0,0,0,0.5),0_24px_48px_rgba(0,0,0,0.52)]'
                )}>
                  <div className="flex items-start justify-between gap-4">
                    <div>
                      <div className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/46 dark:text-white/42">Support</div>
                      <div className="mt-2 text-[24px] font-black tracking-[-0.04em] text-foreground dark:text-white">Help and policy</div>
                      <div className="mt-2 max-w-[30rem] text-[10px] font-bold uppercase tracking-[0.1em] leading-relaxed text-foreground/42 dark:text-white/34">
                        Quick support, billing help, and policy access in the same premium language as the pass.
                      </div>
                    </div>
                    <div className="rounded-full border border-black/8 bg-black/[0.04] px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.13em] text-foreground/50 dark:border-white/[0.08] dark:bg-white/[0.04] dark:text-white/48">
                      Launch ready
                    </div>
                  </div>

                  <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                    <button
                      type="button"
                      onClick={() => openSupportEmail('FeedMe bug report')}
                      className={cn(
                        'flex items-center justify-between rounded-[20px] px-4 py-4 text-left transition-colors duration-200',
                        'bg-white/72 border border-white/86 shadow-[inset_0_1px_0_rgba(255,255,255,0.95),0_10px_20px_rgba(15,23,42,0.05)] hover:bg-white/88',
                        'dark:bg-white/[0.04] dark:border-white/[0.06] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),0_12px_24px_rgba(0,0,0,0.28)] dark:hover:bg-white/[0.06]'
                      )}
                    >
                      <div className="flex items-center gap-2.5">
                        <Bug size={15} className="text-foreground/52 dark:text-white/58" />
                        <div>
                          <div className="text-[10px] font-black uppercase tracking-[0.12em] text-foreground/72 dark:text-white/72">Report a bug</div>
                          <div className="mt-1 text-[9px] font-bold uppercase tracking-[0.08em] text-foreground/34 dark:text-white/28">Flag UI or product issues fast</div>
                        </div>
                      </div>
                      <ArrowUpRight size={14} className="text-foreground/40 dark:text-white/40" />
                    </button>
                    <button
                      type="button"
                      onClick={() => openSupportEmail('FeedMe refund or cancellation help')}
                      className={cn(
                        'flex items-center justify-between rounded-[20px] px-4 py-4 text-left transition-colors duration-200',
                        'bg-white/72 border border-white/86 shadow-[inset_0_1px_0_rgba(255,255,255,0.95),0_10px_20px_rgba(15,23,42,0.05)] hover:bg-white/88',
                        'dark:bg-white/[0.04] dark:border-white/[0.06] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),0_12px_24px_rgba(0,0,0,0.28)] dark:hover:bg-white/[0.06]'
                      )}
                    >
                      <div className="flex items-center gap-2.5">
                        <LifeBuoy size={15} className="text-foreground/52 dark:text-white/58" />
                        <div>
                          <div className="text-[10px] font-black uppercase tracking-[0.12em] text-foreground/72 dark:text-white/72">Refund / cancel help</div>
                          <div className="mt-1 text-[9px] font-bold uppercase tracking-[0.08em] text-foreground/34 dark:text-white/28">Resolve billing support directly</div>
                        </div>
                      </div>
                      <ArrowUpRight size={14} className="text-foreground/40 dark:text-white/40" />
                    </button>
                  </div>

                  <div className="mt-auto rounded-[20px] border border-black/6 bg-black/[0.025] px-4 py-3.5 dark:border-white/[0.06] dark:bg-white/[0.03]">
                    <div className="mb-2 text-[9px] font-bold uppercase tracking-[0.1em] text-foreground/34 dark:text-white/28">
                      Support and policy access for billing and app review.
                    </div>
                    <div className="flex flex-wrap items-center gap-3">
                      <a
                        href={`${siteUrl}/privacy`}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1 text-[9px] font-black uppercase tracking-[0.12em] text-foreground/54 transition-colors hover:text-foreground dark:text-white/52 dark:hover:text-white/76"
                      >
                        <FileText size={12} />
                        Privacy
                      </a>
                      <a
                        href={`${siteUrl}/terms`}
                        target="_blank"
                        rel="noreferrer"
                        className="inline-flex items-center gap-1 text-[9px] font-black uppercase tracking-[0.12em] text-foreground/54 transition-colors hover:text-foreground dark:text-white/52 dark:hover:text-white/76"
                      >
                        <FileText size={12} />
                        Tos
                      </a>
                    </div>
                  </div>
                </div>
              </motion.div>

              {/* Right Column: Deep Settings & Alerts Panel */}
              <div className="flex h-full flex-col gap-4 lg:gap-5">
              <motion.div variants={tileVariant} className={cn(
                'fm-depth-glass rounded-[32px] p-5 relative overflow-hidden flex flex-col xl:min-h-[380px] lg:p-5 xl:p-6',
                'bg-white/70 border border-white/80',
                'shadow-[inset_0_1px_0_rgba(255,255,255,0.9),inset_0_-1px_0_rgba(0,0,0,0.04),0_12px_32px_-4px_rgba(15,23,42,0.07)]',
                'dark:bg-[rgba(10,10,10,0.65)] dark:border-white/[0.06] dark:border-t-white/[0.1]',
                'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),inset_0_-1px_0_rgba(0,0,0,0.5),0_24px_48px_rgba(0,0,0,0.5)]',
              )}>
                <div className="mb-4 flex items-center justify-between gap-3">
                  <div className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/50 dark:text-white/40">Feed Activity</div>
                  <div className="text-[9px] font-bold uppercase tracking-[0.12em] text-foreground/34 dark:text-white/28">
                    What&apos;s happening across your feeds
                  </div>
                </div>
                <div className="grid gap-3 xl:grid-cols-2">
                  {[
                    {
                      title: 'Coming Up',
                      items: upcomingRuns,
                      badgeLabel: 'Scheduled',
                      badgeClass: 'bg-[#CCFF00] text-black border-transparent',
                      badgeStyle: { boxShadow: '0 4px 12px rgba(204,255,0,0.18)' } as React.CSSProperties,
                      mode: 'upcoming' as const,
                      emptyText: 'Nothing scheduled yet',
                    },
                    {
                      title: 'Recently Completed',
                      items: recentRuns,
                      badgeLabel: 'Done',
                      badgeClass: 'bg-black/[0.06] text-foreground/60 border-black/8 dark:bg-white/[0.08] dark:text-white/60 dark:border-white/[0.08]',
                      badgeStyle: {} as React.CSSProperties,
                      mode: 'recent' as const,
                      emptyText: 'No recent activity',
                    },
                  ].map((group) => (
                    <div key={group.title} className="rounded-[24px] border border-black/6 bg-black/[0.025] p-3.5 dark:border-white/[0.06] dark:bg-white/[0.03]">
                      <div className="mb-2 flex items-center justify-between gap-3">
                        <div className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/42 dark:text-white/36">{group.title}</div>
                        <div className={cn('rounded-full border px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.12em]', group.badgeClass)} style={group.badgeStyle}>
                          {group.badgeLabel}
                        </div>
                      </div>
                      <div className="space-y-2">
                        {group.items.length === 0 ? (
                          <div className="rounded-[16px] border border-dashed border-black/8 bg-white/55 px-3 py-4 text-center text-[9px] font-bold uppercase tracking-[0.1em] text-foreground/34 dark:border-white/[0.08] dark:bg-black/20 dark:text-white/28">
                            {group.emptyText}
                          </div>
                        ) : (
                          group.items.map((item) => (
                            <div key={item.id} className="rounded-[16px] border border-black/6 bg-white/75 px-3 py-3 dark:border-white/[0.06] dark:bg-black/24">
                              <div className="min-w-0">
                                <div className="text-[11px] font-black tracking-[-0.01em] text-foreground/80 dark:text-white/80">
                                  {formatBatchSummary(item)}
                                </div>
                                <div className="mt-1 text-[9px] font-bold uppercase tracking-[0.08em] text-foreground/34 dark:text-white/28">
                                  {group.mode === 'recent'
                                    ? `Completed ${formatRunBatchMoment(item.windowStart, item.windowEnd)}`
                                    : `Runs at ${formatRunBatchMoment(item.windowStart, item.windowEnd)}`}
                                </div>
                                {item.businessStart ? (
                                  <div className="mt-1 text-[8px] font-bold uppercase tracking-[0.1em] text-foreground/26 dark:text-white/24">
                                    {formatBusinessDayRange(item.businessStart, item.businessEnd)}
                                  </div>
                                ) : null}
                              </div>

                              <div
                                className="mt-3 flex items-center justify-between rounded-[12px] bg-[#CCFF00] px-3 py-2"
                                style={{ boxShadow: '0 4px 14px rgba(204,255,0,0.14)' }}
                              >
                                <div className="text-[13px] font-black tracking-[-0.02em] text-black">
                                  {item.totalRuns} <span className="text-[9px] font-black uppercase tracking-[0.1em] text-black/50">checks</span>
                                </div>
                                <div className="text-[8px] font-black uppercase tracking-[0.12em] text-black/40">
                                  {formatBatchProgress(item.statuses)}
                                </div>
                              </div>

                              <div className="mt-2.5 flex flex-wrap gap-1.5">
                                {item.types.slice(0, 4).map((typeItem) => (
                                  <div
                                    key={`${item.id}-${typeItem.label}`}
                                    className="rounded-full border border-black/6 bg-black/[0.04] px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/50 dark:border-white/[0.08] dark:bg-white/[0.05] dark:text-white/54"
                                  >
                                    {humanRunTypeLabel(typeItem.label)} · {typeItem.count}
                                  </div>
                                ))}
                                {item.types.length > 4 ? (
                                  <div className="rounded-full border border-dashed border-black/8 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.12em] text-foreground/34 dark:border-white/[0.08] dark:text-white/34">
                                    +{item.types.length - 4} more
                                  </div>
                                ) : null}
                              </div>
                            </div>
                          ))
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </motion.div>

              <motion.div variants={tileVariant} className={cn(
                'fm-depth-glass rounded-[32px] p-5 relative overflow-hidden flex flex-col lg:flex-1 lg:p-6 xl:p-7',
                'bg-white/70 border border-white/80',
                'shadow-[inset_0_1px_0_rgba(255,255,255,0.9),inset_0_-1px_0_rgba(0,0,0,0.04),0_12px_32px_-4px_rgba(15,23,42,0.07)]',
                'dark:bg-[rgba(10,10,10,0.65)] dark:border-white/[0.06] dark:border-t-white/[0.1]',
                'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04),inset_0_-1px_0_rgba(0,0,0,0.5),0_24px_48px_rgba(0,0,0,0.5)]',
              )}>
                <div className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/50 mb-5">Settings & Alerts</div>

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
                              <Sun size={15} className="text-[#a7d000]" />
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
                      }, 350);
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
                          'shadow-[inset_0_1px_1px_rgba(255,255,255,0.08),0_10px_24px_-10px_rgba(0,0,0,0.45),0_0_22px_rgba(204,255,0,0.12)]',
                          'dark:shadow-[inset_0_1px_1px_rgba(255,255,255,0.08),0_18px_32px_-14px_rgba(0,0,0,0.65),0_0_28px_rgba(204,255,0,0.15)]',
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
                          ? 'bg-[#CCFF00] shadow-[0_6px_14px_rgba(204,255,0,0.22),inset_0_1px_1px_rgba(255,255,255,0.6)]'
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
                            ? 'text-[#CCFF00]/70'
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
                          ? 'bg-[#CCFF00]/14 text-[#CCFF00] border border-[#CCFF00]/18'
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
                              ? 'bg-[#CCFF00]/10 border border-[#CCFF00]/20 dark:bg-[#CCFF00]/[0.06] dark:border-[#CCFF00]/15'
                              : 'bg-black/[0.04] border border-black/[0.06] dark:bg-white/[0.04] dark:border-white/[0.06]'
                          )}
                        >
                          <span className={cn(
                            'text-[18px] font-black tabular-nums tracking-tight',
                            thresholdLocked
                              ? 'text-[#7a9900] dark:text-[#CCFF00] dark:drop-shadow-[0_0_10px_rgba(204,255,0,0.3)]'
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
                                className="absolute -top-1.5 -right-1.5 flex items-center justify-center w-4 h-4 rounded-full bg-[#CCFF00] shadow-[0_2px_6px_rgba(204,255,0,0.4),inset_0_1px_1px_rgba(255,255,255,0.6)]"
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
                            'bg-[#CCFF00] shadow-[0_0_14px_rgba(204,255,0,0.45),inset_0_1px_2px_rgba(255,255,255,0.8),inset_0_-1px_2px_rgba(0,0,0,0.15)]',
                            isDraggingSlider && 'shadow-[0_0_22px_rgba(204,255,0,0.65),inset_0_1px_2px_rgba(255,255,255,1),inset_0_-1px_3px_rgba(0,0,0,0.2)]',
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
                            isDraggingSlider && 'dark:shadow-[0_6px_20px_rgba(204,255,0,0.25),0_3px_8px_rgba(0,0,0,0.6),inset_0_2px_3px_rgba(255,255,255,0.15)]',
                            'transition-opacity duration-300'
                          )}
                        >
                          {/* Inner dot indicator */}
                          <div className={cn(
                            'absolute inset-0 m-auto w-[6px] h-[6px] rounded-full',
                            thresholdLocked ? 'bg-black/10 dark:bg-white/10' : 'bg-[#CCFF00]/80 shadow-[0_0_6px_rgba(204,255,0,0.4)]',
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
                          window.location.href = `/?id=${feed.id}`;
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
                      isPrimed ? 'dark:bg-[rgba(15,15,15,0.85)] bg-white/90 ring-1 ring-[#CCFF00]/40' : 'hover:dark:bg-[rgba(15,15,15,0.7)] hover:bg-white/80'
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
                              isPrimed && 'dark:shadow-[0_8px_24px_rgba(204,255,0,0.4)]'
                            )}>
                              {/* The Neon Fill Animation */}
                              <motion.div 
                                initial={{ y: '100%' }}
                                animate={{ y: isPrimed ? '0%' : '100%' }}
                                transition={{ duration: 0.3, ease: 'easeOut' }}
                                className="absolute inset-0 bg-[#CCFF00] shadow-[inset_0_2px_4px_rgba(255,255,255,0.8)]"
                              />
                              <ArrowUpRight 
                                size={18} 
                                strokeWidth={2.5} 
                                className={cn(
                                  "relative z-10 transition-colors duration-300",
                                  isPrimed ? "text-black drop-shadow-sm" : "text-foreground/40 dark:text-white/40 group-hover:dark:text-[#CCFF00]"
                                )} 
                              />
                            </div>
                            <AnimatePresence>
                              {isPrimed && (
                                <motion.div 
                                  initial={{ opacity: 0, y: 5 }}
                                  animate={{ opacity: 1, y: 0 }}
                                  exit={{ opacity: 0, y: 3 }}
                                  className="absolute -bottom-5 right-6 text-[8px] font-black uppercase tracking-[0.14em] text-[#CCFF00]"
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
                                 className="absolute inset-[1.5px] rounded-full bg-[#CCFF00] shadow-[0_0_12px_rgba(204,255,0,0.4),inset_0_1px_2px_rgba(255,255,255,0.7),inset_0_-1px_2px_rgba(0,0,0,0.15)]"
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
