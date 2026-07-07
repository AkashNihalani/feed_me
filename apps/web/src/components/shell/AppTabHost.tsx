'use client';

import dynamic from 'next/dynamic';
import { Activity, useEffect, useLayoutEffect, useRef, useState, type ComponentType } from 'react';
import { SWITCH_CLOCK_CSS_EASE, SWITCH_CLOCK_MS } from '@/lib/motion';

const tabLoaders = {
  feed: () => import('@/components/tabs/FeedTab'),
  fire: () => import('@/components/tabs/FireTab'),
  fund: () => import('@/components/tabs/FundTab'),
} as const;

const FeedTab = dynamic(tabLoaders.feed, { loading: () => <TabFallback /> });
const FireTab = dynamic(tabLoaders.fire, { loading: () => <TabFallback /> });
const FundTab = dynamic(tabLoaders.fund, { loading: () => <TabFallback /> });

type TabKey = keyof typeof tabLoaders;

const TAB_ORDER: TabKey[] = ['feed', 'fire', 'fund'];
const TAB_COMPONENTS: Record<TabKey, ComponentType> = {
  feed: FeedTab,
  fire: FireTab,
  fund: FundTab,
};

// useLayoutEffect warns during SSR; fall back to useEffect on the server.
const useIsomorphicLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

function tabKeyForPathname(pathname: string): TabKey {
  if (pathname === '/fire') return 'fire';
  if (pathname === '/profile') return 'fund';
  return 'feed';
}

function scheduleIdle(callback: () => void) {
  if (typeof window === 'undefined') return () => {};
  const idleWindow = window as Window & {
    requestIdleCallback?: (cb: IdleRequestCallback, options?: IdleRequestOptions) => number;
    cancelIdleCallback?: (id: number) => void;
  };

  if (idleWindow.requestIdleCallback) {
    const id = idleWindow.requestIdleCallback(callback, { timeout: 1800 });
    return () => idleWindow.cancelIdleCallback?.(id);
  }

  const id = window.setTimeout(callback, 700);
  return () => window.clearTimeout(id);
}

function TabFallback() {
  return <div className="min-h-[100dvh] w-full bg-[var(--fm-page)]" />;
}

export default function AppTabHost({ pathname }: { pathname: string }) {
  const activeTab = tabKeyForPathname(pathname);

  // Each tab is mounted once and kept alive. Switching reveals an already-built
  // tab instead of tearing the old one down and rebuilding the new one from
  // scratch — that rebuild (1.8k–2.6k lines + every effect re-running) is what
  // dropped switches to ~30fps and made them feel like a fresh page load.
  //
  // <Activity mode="hidden"> preserves a hidden tab's state and DOM while
  // suspending its effects, so background tabs stop running timers and scroll
  // listeners (e.g. the Fund live clock) instead of draining the main thread.
  const [mountedTabs, setMountedTabs] = useState<Set<TabKey>>(() => new Set([activeTab]));
  const prevTabRef = useRef<TabKey>(activeTab);
  const tabElsRef = useRef<Partial<Record<TabKey, HTMLDivElement | null>>>({});

  // Ensure the active tab is mounted — covers a direct deep-link to a tab that
  // idle pre-warm hasn't reached yet.
  useEffect(() => {
    if (mountedTabs.has(activeTab)) return;
    const timer = window.setTimeout(() => {
      setMountedTabs((current) => {
        if (current.has(activeTab)) return current;
        const next = new Set(current);
        next.add(activeTab);
        return next;
      });
    }, 0);
    return () => window.clearTimeout(timer);
  }, [activeTab, mountedTabs]);

  const renderedTabs = mountedTabs.has(activeTab) ? mountedTabs : new Set([...mountedTabs, activeTab]);

  // Once the first paint settles, build the remaining tabs in the background as
  // hidden Activities, so even the first switch to them is instant. Hidden
  // Activities don't run effects, so this is a one-time low-priority render with
  // nothing left running afterward.
  useEffect(() => {
    return scheduleIdle(() => {
      setMountedTabs((current) => (current.size === TAB_ORDER.length ? current : new Set(TAB_ORDER)));
    });
  }, []);

  // A newly revealed tab starts at the top, matching prior behavior (every
  // switch used to rebuild at scroll 0) so a tab never inherits the previous
  // tab's page-scroll offset. Runs before paint to avoid a visible jump.
  //
  // The revealed tab then settles in with a transform-only rise on the shared
  // switch clock (same duration/ease as the header entrance). Deliberately NOT
  // opacity (grouping the shadowed subtree makes iOS rasterize the whole tab
  // per switch) and deliberately NOT framer-motion (animations inside a hidden
  // <Activity> are suspended and replay unreliably on reveal — this host is
  // always mounted, so a plain CSS transition it drives always fires). The
  // transform is cleared once settled so position:fixed descendants (e.g. the
  // fire PWA deck band) don't keep a transformed containing block.
  useIsomorphicLayoutEffect(() => {
    if (prevTabRef.current === activeTab) return;
    prevTabRef.current = activeTab;
    window.scrollTo(0, 0);

    const el = tabElsRef.current[activeTab];
    if (!el) return;
    if (typeof window.matchMedia === 'function' && window.matchMedia('(prefers-reduced-motion: reduce)').matches) {
      return;
    }

    el.style.transition = 'none';
    el.style.transform = 'translateY(10px)';

    let raf2 = 0;
    let settleTimer = 0;
    const finish = () => {
      el.style.transition = '';
      el.style.transform = '';
    };
    const raf1 = window.requestAnimationFrame(() => {
      raf2 = window.requestAnimationFrame(() => {
        el.style.transition = `transform ${SWITCH_CLOCK_MS}ms ${SWITCH_CLOCK_CSS_EASE}`;
        el.style.transform = 'translateY(0px)';
        settleTimer = window.setTimeout(finish, SWITCH_CLOCK_MS + 40);
      });
    });

    return () => {
      window.cancelAnimationFrame(raf1);
      window.cancelAnimationFrame(raf2);
      window.clearTimeout(settleTimer);
      finish();
    };
  }, [activeTab]);

  return (
    <div data-active-tab={activeTab} className="min-h-[100dvh] w-full">
      {TAB_ORDER.map((key) => {
        if (!renderedTabs.has(key)) return null;
        const TabComponent = TAB_COMPONENTS[key];
        const isActive = key === activeTab;
        return (
          <Activity key={key} name={`tab-${key}`} mode={isActive ? 'visible' : 'hidden'}>
            {/* Tabs are already built (kept alive); arrival is the transform-only
                settle driven by the layout effect above, on the shared switch
                clock. No whole-tab opacity dissolve (grouping a shadowed subtree
                forces iOS to rasterize it on every switch). */}
            <div
              data-tab={key}
              ref={(node) => {
                tabElsRef.current[key] = node;
              }}
              className="min-h-[100dvh] w-full"
            >
              <TabComponent />
            </div>
          </Activity>
        );
      })}
    </div>
  );
}
