'use client';

import dynamic from 'next/dynamic';
import { Activity, memo, useEffect, useLayoutEffect, useRef, useState, type ComponentType } from 'react';
import { SWITCH_CLOCK_CSS_EASE, SWITCH_CLOCK_MS } from '@/lib/motion';
import { arrivePieces } from '@/lib/pageArrival';
import { noteTab, riseIn, settleTab, wideTabMotion } from '@/lib/tabSheet';

const tabLoaders = {
  feed: () => import('@/components/tabs/FeedTab'),
  lead: () => import('@/app/lead/page'),
  read: () => import('@/components/tabs/ReadTab'),
  fire: () => import('@/components/tabs/FireTab'),
  fund: () => import('@/components/tabs/FundTab'),
} as const;

const FeedTab = dynamic(tabLoaders.feed, { loading: () => <TabFallback /> });
const LeadTab = dynamic(tabLoaders.lead, { loading: () => <TabFallback /> });
const ReadTab = dynamic(tabLoaders.read, { loading: () => <TabFallback /> });
const FireTab = dynamic(tabLoaders.fire, { loading: () => <TabFallback /> });
const FundTab = dynamic(tabLoaders.fund, { loading: () => <TabFallback /> });

type TabKey = keyof typeof tabLoaders;

// Read replaced Fund in the bottom nav; Fund stays reachable (Feed's Fund panel links to /profile) but is no
// longer pre-warmed.
const PRIMARY_TAB_ORDER: TabKey[] = ['feed', 'lead', 'read'];
const TAB_RENDER_ORDER: TabKey[] = [...PRIMARY_TAB_ORDER, 'fire', 'fund'];
const TAB_COMPONENTS: Record<TabKey, ComponentType> = {
  feed: FeedTab,
  lead: LeadTab,
  read: ReadTab,
  fire: FireTab,
  fund: FundTab,
};

// useLayoutEffect warns during SSR; fall back to useEffect on the server.
const useIsomorphicLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

function tabKeyForPathname(pathname: string): TabKey {
  if (pathname === '/lead') return 'lead';
  if (pathname === '/read') return 'read';
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

// A tab's subtree never re-renders because another tab was picked: a switch only flips which Activity is visible.
// (A tab still re-renders on its own state, and on the router hooks it reads itself.)
const TabBody = memo(function TabBody({ component: TabComponent }: { component: ComponentType }) {
  return <TabComponent />;
});

/* the CSS entrances of a tab shown again restart from the top (display: none ended them): finished at once, so the
   tab is drawn as it was left. An endless one (a loader, an ambient loop) plays on, and so does one that follows the
   scroll (a scroll-driven one has no end: finished, it would stop following it) */
function settleEntrances(root: HTMLElement) {
  root.getAnimations({ subtree: true }).forEach((animation) => {
    if (!(animation instanceof CSSAnimation)) return;
    if (animation.effect?.getTiming().iterations === Infinity) return;
    if (animation.timeline && !(animation.timeline instanceof DocumentTimeline)) return;
    try {
      animation.finish();
    } catch {
      // one that cannot finish (no end)
    }
  });
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

  // Once the first paint settles, build the remaining primary tabs in the
  // background. Fire is an auxiliary Lead surface, so it stays out of the
  // initial bundle/render path until Post Mortem opens it.
  useEffect(() => {
    return scheduleIdle(() => {
      setMountedTabs((current) => (
        PRIMARY_TAB_ORDER.every((key) => current.has(key))
          ? current
          : new Set([...current, ...PRIMARY_TAB_ORDER])
      ));
    });
  }, []);

  /* A tab switch has one arrival, the same every time, cached or not: the tab you pick is there as you left it, and
     its page comes in as its own pieces rising into place, row by row (lib/tabSheet riseIn), once the frame that shows
     it is on screen. Nothing else on the page plays an entrance: the CSS entrances a hidden tab's pieces restart on
     being shown again (display: none ends them) are finished before the first frame, and a page with an entrance of
     its own (Read's engine) mounts quietly when a switch brings it (noteTab). Other motions can be tried on a
     preview (?tabmotion=).

     A tab that keeps its own place in the page (Feed) has already put its scroll back before this runs; any other
     starts at the top, as a fresh tab would. */
  noteTab(activeTab);
  useIsomorphicLayoutEffect(() => {
    settleTab(activeTab);
    if (prevTabRef.current === activeTab) return;
    prevTabRef.current = activeTab;

    const el = tabElsRef.current[activeTab];
    // a tab that keeps its own place in the page (Feed) has already put the scroll back, before this runs
    if (!el?.querySelector('[data-tab-scroll="own"]')) window.scrollTo(0, 0);
    if (!el) return;
    settleEntrances(el);
    if (typeof window.matchMedia === 'function' && window.matchMedia('(prefers-reduced-motion: reduce)').matches) return;
    // a laptop up: the page's blocks rise into place, row by row (lib/tabSheet: transform alone)
    if (wideTabMotion()) return riseIn(el) ?? undefined;

    /* A phone: never the tab as one block where it can be helped. A tab that plays its own arrival (Read) is left to
       it, and a tab whose pieces are marked [data-arrive] (Lead) has them rise one after another on the page beat,
       under the one header adapting above. Only tabs with neither settle in whole: one transform on the tab, never
       its opacity (iOS draws a faded subtree with shadows afresh, every frame) */
    if (el.querySelector('[data-tab-arrival="own"]')) return;
    if (arrivePieces(el) > 0) return;

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
      {TAB_RENDER_ORDER.map((key) => {
        if (!renderedTabs.has(key)) return null;
        const isActive = key === activeTab;
        return (
          <Activity key={key} name={`tab-${key}`} mode={isActive ? 'visible' : 'hidden'}>
            {/* Tabs are already built (kept alive): a switch shows this one as it
                was left (the layout effect above) */}
            <div
              data-tab={key}
              ref={(node) => {
                tabElsRef.current[key] = node;
              }}
              className="min-h-[100dvh] w-full"
            >
              <TabBody component={TAB_COMPONENTS[key]} />
            </div>
          </Activity>
        );
      })}
    </div>
  );
}
