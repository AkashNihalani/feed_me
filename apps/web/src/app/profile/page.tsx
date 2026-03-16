'use client';

import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { getSupabase, User } from '@/lib/supabase';
import { cn, formatShortIST } from '@/lib/utils';
import { getCache, setCache } from '@/lib/pageCache';
import { useAppHaptics } from '@/lib/haptics';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import {
  ArrowUpRight,
  Bell,
  Check,
  CreditCard,
  Crown,
  Lock,
  Moon,
  Sun,
  Package,
  Plus,
  Target,
  Unlock,
} from 'lucide-react';

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

type EngineStats = {
  recentJobs: EngineJob[];
  totalFeeders: number;
  totalPosts: number;
  jobStats: { done: number; failed: number; pending: number; running: number };
};

const emptyStats: EngineStats = {
  recentJobs: [],
  totalFeeders: 0,
  totalPosts: 0,
  jobStats: { done: 0, failed: 0, pending: 0, running: 0 },
};

const APPLE_EASE = [0.32, 0.72, 0, 1] as const;

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

function parseMetric(value: string | number | undefined) {
  if (typeof value === 'number') return value;
  if (!value) return 0;
  const raw = String(value).replace(/,/g, '');
  const n = Number(raw);
  return Number.isFinite(n) ? n : 0;
}

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
  const launchRef = useRef<HTMLDivElement | null>(null);

  const [isLoading, setIsLoading] = useState(true);
  const [isDarkMode, setIsDarkMode] = useState(true);
  const [user, setUser] = useState<User | null>(null);
  const [emailNotifications, setEmailNotifications] = useState(true);
  const [alertThreshold, setAlertThreshold] = useState(25);
  const [isDraggingSlider, setIsDraggingSlider] = useState(false);
  const [thresholdLocked, setThresholdLocked] = useState(true);
  const [lockProgress, setLockProgress] = useState(0);
  const lockTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const lockRafRef = useRef<number | null>(null);
  const lockStartRef = useRef<number>(0);
  const [themeRipple, setThemeRipple] = useState<{ active: boolean; x: number; y: number; toDark: boolean } | null>(null);
  const themeToggleRef = useRef<HTMLDivElement | null>(null);
  const [primedBundleId, setPrimedBundleId] = useState<string | null>(null);
  const [feeds, setFeeds] = useState<Feed[]>([]);
  const [slots, setSlots] = useState<SlotUsage>({ used: 0 });
  const [engineStats, setEngineStats] = useState<EngineStats>(emptyStats);
  const [flowBusy, setFlowBusy] = useState(false);
  const [flowError, setFlowError] = useState<string | null>(null);
  const [flowNotice, setFlowNotice] = useState<string | null>(null);

  // Launch sequence states removed

  const FUND_CACHE_KEY = 'fund:bundle:v1';
  const FUND_CACHE_TTL = 2 * 60 * 1000;

  useEffect(() => {
    const stored = localStorage.getItem('fund:alert-threshold');
    if (stored) {
      const next = Number(stored);
      if (Number.isFinite(next)) setAlertThreshold(Math.max(1, Math.min(100, next)));
    }
  }, []);

  useEffect(() => {
    async function fetchData(hadCache: boolean) {
      if (!hadCache) setIsLoading(true);

      const { data: { user: authUser }, error: authError } = await getSupabase().auth.getUser();
      if (authError || !authUser) {
        window.location.href = '/login';
        return;
      }

      const userId = authUser.id;

      let { data: userData, error: userFetchError } = await getSupabase()
        .from('users')
        .select('*')
        .eq('id', userId)
        .single();

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
          setIsLoading(false);
          return;
        }
        userData = newUser as User;
      }

      let userSnapshot: Partial<User> | null = null;
      if (userData) {
        setUser(userData as User);
        setEmailNotifications(userData.email_notifications ?? true);
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

      setIsLoading(false);

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

    const hadCache = Boolean(cached);
    if (cached) {
      setFeeds(cached.feeds || []);
      setSlots(cached.slots || { used: 0 });
      setEngineStats(cached.engineStats || emptyStats);
      if (cached.user) {
        setUser((prev) => ({ ...(prev || {}), ...cached.user } as User));
        setEmailNotifications(cached.user.email_notifications ?? true);
      }
      setIsLoading(false);
    }

    if (!cached) {
      fetchData(hadCache).catch(() => {});
    }

    // Default to dark mode unless user explicitly set 'light'
    const savedTheme = localStorage.getItem('theme');
    const shouldBeDark = savedTheme !== 'light';
    setIsDarkMode(shouldBeDark);
    if (shouldBeDark) {
      document.documentElement.classList.add('dark');
      document.documentElement.classList.remove('light');
    } else {
      document.documentElement.classList.remove('dark');
      document.documentElement.classList.add('light');
    }
  }, []);

  useEffect(() => {
    let payment: string | null = null;
    try {
      payment = new URLSearchParams(window.location.search).get('payment');
    } catch {}
    if (payment !== 'success') return;
    setFlowNotice('Payment received. Refreshing wallet balance…');
    (async () => {
      try {
        const { data: { user: authUser } } = await getSupabase().auth.getUser();
        if (!authUser) return;
        const { data: freshUser } = await getSupabase().from('users').select('*').eq('id', authUser.id).single();
        if (freshUser) setUser(freshUser as User);
      } finally {
        router.replace('/profile');
      }
    })().catch(() => {});
  }, [router]);

  const totalFeeds = feeds.length;
  const totalFeeders = feeds.reduce((sum, feed) => sum + (feed.feeders?.length || 0), 0);
  const slotsUsed = slots.used ?? 0;
  const slotPlanPrice = slots.plan?.price ?? 499;
  const slotPostsCap = slots.plan?.postsCap ?? 35;
  const packPrice = slots.plan?.packPrice ?? 99;
  const packSize = slots.plan?.packSize ?? 15;
  const monthlySpend = slotsUsed * slotPlanPrice;
  const lastScrape = engineStats.recentJobs[0]?.updatedAt || engineStats.recentJobs[0]?.createdAt || '';

  const totalTrackedPosts = feeds.reduce((sum, feed) => sum + parseMetric(feed.metrics?.postsTracked), 0);

  async function runFeedAction(payload: Record<string, unknown>) {
    const res = await fetch('/api/feed', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    const json = await res.json();
    if (!res.ok) throw new Error(json.error || 'Action failed');
    if (json.feeds) setFeeds((json.feeds || []) as Feed[]);
    if (json.slots) setSlots((json.slots || { used: 0 }) as SlotUsage);
    return json;
  }

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
    setLockProgress(0);
    const tick = () => {
      const elapsed = performance.now() - lockStartRef.current;
      const pct = Math.min(1, elapsed / LOCK_DURATION);
      setLockProgress(pct);
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

  return (
    <motion.div
      initial={{ opacity: 0, y: 14, scale: 0.992 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ duration: 0.42, ease: APPLE_EASE }}
      className="relative h-[100svh] w-full overflow-hidden bg-background text-foreground select-none md:h-[100dvh]"
    >
      <div className="pointer-events-none fixed inset-0 z-0 bg-[#f4f7f9] dark:bg-[#030303]" />

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
      <div className="pointer-events-auto absolute inset-x-0 top-0 z-[100] flex flex-col items-center px-4 pt-[calc(10px+env(safe-area-inset-top))] sm:px-6 sm:pt-[calc(14px+env(safe-area-inset-top))] md:px-8 md:pt-[24px]">
        <div className="relative w-full">
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
      <div className="absolute inset-0 z-10 pointer-events-none">
        <div
          className="hide-scrollbar h-full w-full overflow-y-auto overflow-x-hidden pb-[calc(170px+env(safe-area-inset-bottom))] pt-[calc(96px+env(safe-area-inset-top))] sm:pt-[calc(108px+env(safe-area-inset-top))] md:pt-[118px] pointer-events-auto"
          style={{ WebkitOverflowScrolling: 'touch' }}
        >
          <motion.div
            variants={staggerContainer}
            initial="hidden"
            animate="visible"
            className="fm-app-shell mx-auto max-w-[1480px] space-y-4 px-4 sm:space-y-5 sm:px-0 lg:space-y-6 xl:space-y-7 transform-gpu will-change-transform"
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
            <div className="grid gap-4 xl:grid-cols-[1.05fr_0.95fr] xl:gap-5 2xl:gap-6">
              {/* Left Column: Slot Plan & Actions */}
              <motion.div variants={tileVariant} className={cn(
                'fm-depth-glass rounded-[32px] p-5 relative overflow-hidden flex flex-col justify-between lg:p-6 xl:p-7',
                'bg-gradient-to-br from-white/90 to-white/60 border border-white/90',
                'shadow-[inset_0_2px_4px_rgba(255,255,255,1),inset_0_-2px_4px_rgba(0,0,0,0.03),0_12px_32px_-4px_rgba(15,23,42,0.08),0_24px_64px_-16px_rgba(15,23,42,0.06)]',
                'dark:from-white/[0.05] dark:to-white/[0.01] dark:border-white/[0.08] dark:border-t-white/[0.12]',
                'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.08),inset_0_-1px_0_rgba(0,0,0,0.6),0_24px_48px_rgba(0,0,0,0.5),0_32px_80px_rgba(0,0,0,0.4)]',
              )}>
                <div className="flex items-start justify-between">
                  <div>
                    <div className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/50">Base Plan</div>
                    <div className="mt-1 text-[28px] font-black uppercase tracking-[-0.04em] text-foreground sm:text-[36px] drop-shadow-sm dark:drop-shadow-[0_2px_8px_rgba(0,0,0,0.6)]">
                      ₹{slotPlanPrice} <span className="text-[14px] text-foreground/40 sm:text-[18px]">/ Slot / Month</span>
                    </div>
                  </div>
                  <div className={cn(
                    'rounded-[20px] px-4 py-2.5 text-center',
                    'bg-white/80 border border-white shadow-[inset_0_1px_4px_rgba(0,0,0,0.04)]',
                    'dark:bg-black/40 dark:border-white/5 dark:shadow-[inset_0_2px_8px_rgba(0,0,0,0.7),0_4px_12px_rgba(0,0,0,0.4)]',
                  )}>
                    <div className="text-[9px] font-black uppercase tracking-[0.16em] text-foreground/50 dark:text-white/40">Slots Used</div>
                    <div className="text-[24px] font-black text-foreground drop-shadow-sm dark:text-white/90">{slotsUsed}</div>
                  </div>
                </div>

                <div className="mt-8 grid gap-4 lg:grid-cols-2 xl:mt-10 xl:gap-5 relative z-10">
                  {/* Neon on Black / Black on Neon Action Tile */}
                  <motion.button
                    type="button"
                    whileTap={{ scale: 0.96 }}
                    className={cn(
                    'flex items-center justify-between rounded-[24px] px-5 py-4 text-left group',
                    'bg-[#CCFF00]',
                    'shadow-[inset_0_2px_6px_rgba(255,255,255,0.6),inset_0_-2px_6px_rgba(130,156,0,0.6),0_8px_24px_rgba(204,255,0,0.25)]',
                  )}
                  >
                    <div>
                      <div className="text-[10px] font-black uppercase tracking-[0.14em] text-black/60">New Connection</div>
                      <div className="mt-1 text-[22px] font-black tracking-tight text-black drop-shadow-sm">Add Slot</div>
                      <div className="mt-0.5 text-[9px] font-bold uppercase tracking-[0.12em] text-black/50 leading-tight max-w-[120px]">Deploy a new feeder into any bundle</div>
                    </div>
                    <div className="flex flex-col items-center justify-center gap-1.5 lg:gap-2">
                      <div className="flex h-[42px] w-[42px] lg:h-[60px] lg:w-[60px] xl:h-[72px] xl:w-[72px] items-center justify-center rounded-full bg-black shadow-[0_4px_16px_rgba(0,0,0,0.4),inset_0_1px_2px_rgba(255,255,255,0.2)] group-hover:scale-105 group-active:scale-90 transition-transform duration-300">
                        <Plus size={22} strokeWidth={3} className="text-[#CCFF00] lg:w-8 lg:h-8 xl:w-10 xl:h-10" />
                      </div>
                      <div className="text-[12px] lg:text-[18px] xl:text-[22px] font-black uppercase text-black">₹{slotPlanPrice}</div>
                    </div>
                  </motion.button>

                  <motion.button
                    type="button"
                    whileTap={{ scale: 0.96 }}
                    className={cn(
                    'flex items-center justify-between rounded-[24px] px-5 py-4 text-left group',
                    'bg-[#111] dark:bg-black',
                    'border border-black dark:border-white/5',
                    'shadow-[inset_0_1px_1px_rgba(255,255,255,0.1),0_8px_24px_rgba(0,0,0,0.3)]',
                  )}
                  >
                    <div>
                      <div className="text-[10px] font-black uppercase tracking-[0.14em] text-[#CCFF00]/60 dark:text-white/40">Expansion</div>
                      <div className="mt-1 text-[22px] font-black tracking-tight text-[#CCFF00] drop-shadow-[0_0_12px_rgba(204,255,0,0.2)]">Buy Sub</div>
                      <div className="mt-0.5 text-[9px] font-bold uppercase tracking-[0.12em] text-[#CCFF00]/40 leading-tight max-w-[140px]">Fund wallet and activate tracking</div>
                    </div>
                    <div className="flex flex-col items-center justify-center gap-1.5 lg:gap-2">
                      <div className="flex h-[42px] w-[42px] lg:h-[60px] lg:w-[60px] xl:h-[72px] xl:w-[72px] items-center justify-center rounded-full bg-black shadow-[inset_0_4px_8px_rgba(0,0,0,0.8),inset_0_-1px_2px_rgba(255,255,255,0.08),0_1px_1px_rgba(255,255,255,0.1)] group-hover:scale-105 group-active:scale-90 transition-transform duration-300">
                        <Package size={20} strokeWidth={2.5} className="text-[#CCFF00] drop-shadow-[0_0_8px_rgba(204,255,0,0.5)] lg:w-7 lg:h-7 xl:w-9 xl:h-9" />
                      </div>
                      <div className="text-[12px] lg:text-[18px] xl:text-[22px] font-black uppercase text-[#CCFF00]">₹{Math.max(100, slotPlanPrice)}</div>
                    </div>
                  </motion.button>
                </div>
              </motion.div>

              {/* Right Column: Deep Settings & Alerts Panel */}
              <motion.div variants={tileVariant} className={cn(
                'fm-depth-glass rounded-[32px] p-5 relative overflow-hidden flex flex-col lg:p-6 xl:p-7',
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
                          localStorage.setItem('theme', 'dark');
                        } else {
                          document.documentElement.classList.remove('dark');
                          document.documentElement.classList.add('light');
                          localStorage.setItem('theme', 'light');
                        }
                      }, 350);
                    }}>
                      <div className="group-active:scale-90 transition-transform duration-200"><HardwareToggle active={isDarkMode} /></div>
                    </div>
                  </div>

                  {/* ── Fire Alert Notifications ── */}
                  <div className={cn(
                    'w-full flex items-center justify-between rounded-[20px] px-5 py-[18px]',
                    'transition-all duration-500',
                    emailNotifications
                      ? [
                          'bg-[#CCFF00]',
                          'border border-[#bde600]',
                          'shadow-[inset_0_2px_6px_rgba(255,255,255,0.5),inset_0_-2px_4px_rgba(130,156,0,0.45),0_6px_18px_-4px_rgba(204,255,0,0.25),0_2px_4px_rgba(0,0,0,0.05)]',
                          'dark:border-[#CCFF00]/30',
                          'dark:shadow-[inset_0_2px_6px_rgba(255,255,255,0.15),inset_0_-2px_4px_rgba(0,0,0,0.35),0_0_20px_rgba(204,255,0,0.15),0_8px_24px_-4px_rgba(0,0,0,0.4)]',
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
                    <div className="flex items-center gap-3.5">
                      <div className={cn(
                        'flex items-center justify-center w-8 h-8 rounded-[10px] transition-all duration-300',
                        emailNotifications
                          ? 'bg-black/90 shadow-[0_3px_8px_rgba(0,0,0,0.3),inset_0_1px_1px_rgba(255,255,255,0.1)]'
                          : [
                              'bg-white/70 border border-white/90',
                              'shadow-[inset_0_1px_2px_rgba(255,255,255,1),0_2px_6px_rgba(0,0,0,0.06)]',
                              'dark:bg-black/50 dark:border-white/[0.05]',
                              'dark:shadow-[inset_0_2px_4px_rgba(0,0,0,0.5),0_1px_0_rgba(255,255,255,0.04)]',
                            ]
                      )}>
                        <Bell size={15} className={cn(
                          emailNotifications ? 'text-[#CCFF00]' : 'text-foreground/35 dark:text-white/30',
                          'transition-colors duration-300'
                        )} />
                      </div>
                      <span className={cn(
                        'text-[11px] font-black uppercase tracking-[0.14em] transition-colors duration-300',
                        emailNotifications ? 'text-black' : 'text-foreground/70 dark:text-white/70'
                      )}>Fire Alerts</span>
                    </div>
                    <div className="cursor-pointer group" onClick={async () => {
                      const newValue = !emailNotifications;
                      setEmailNotifications(newValue);
                      await getSupabase().from('users').update({ email_notifications: newValue }).eq('id', user?.id);
                    }}>
                      <div className="group-active:scale-90 transition-transform duration-200"><HardwareToggle active={emailNotifications} /></div>
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
                    !emailNotifications && 'opacity-35 pointer-events-none'
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
                            setThresholdLocked(false); setLockProgress(0); setIsDraggingSlider(true);
                          }}
                          onMouseUp={() => { setIsDraggingSlider(false); startAutoLock(); }}
                          onTouchStart={() => {
                            if (lockRafRef.current) { cancelAnimationFrame(lockRafRef.current); lockRafRef.current = null; }
                            setThresholdLocked(false); setLockProgress(0); setIsDraggingSlider(true);
                          }}
                          onTouchEnd={() => { setIsDraggingSlider(false); startAutoLock(); }}
                          onChange={(e) => {
                            const next = Number(e.target.value);
                            setAlertThreshold(next);
                            localStorage.setItem('fund:alert-threshold', String(next));
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
                        ? `Active — fire alerts below ${alertThreshold}%`
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
