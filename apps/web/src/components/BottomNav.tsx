'use client';

import Link from 'next/link';
import { useEffect, useLayoutEffect, useMemo, useRef, useState, type CSSProperties } from 'react';
import { usePathname, useRouter } from 'next/navigation';
import { BookOpenText, LayoutGrid, Trophy } from 'lucide-react';
import { motion } from 'framer-motion';
import { useAppHaptics } from '@/lib/haptics';
import { PILL_SPRING } from '@/lib/motion';
import { watchScrollFold, type ScrollFold } from '@/lib/useCompressedOnScroll';
import { cn } from '@/lib/utils';

const NAV_ITEMS = [
  { label: 'Feed', href: '/', icon: LayoutGrid },
  { label: 'Lead', href: '/lead', icon: Trophy },
  { label: 'Read', href: '/read', icon: BookOpenText },
];

// the routes AppTabHost keeps alive: between these a switch is just which tab shows
function isKeptAliveTab(pathname: string | null) {
  return pathname === '/' || pathname === '/lead' || pathname === '/read' || pathname === '/fire' || pathname === '/profile';
}

function primaryTabHref(pathname: string | null) {
  return pathname === '/fire' ? '/lead' : pathname;
}

/* Below the laptop width the bar gets out of the way of the posts: scrolling on, it folds down to the icon of the tab
   you're on, in one small glass circle in the middle (iOS's tab bar, minimised: the header already names the tab);
   scrolling back up, or a tap on it, opens the bar again. It folds on the header's thresholds
   (lib/useCompressedOnScroll), so the two bars go together.

   One piece carries the fold: the tab's ink pill lifts off the bar and closes into an ink disc on its way to the middle,
   round at both ends the whole way (two ink circles and the band between them, the band closing as the pair shrinks:
   .fm-nav-ink), with the icon riding in it, never scaled, and the label tucked away as it sets off. Around it the rest
   steps aside: the other two glide in toward the middle and dissolve, the bar's glass draws in and melts, and the
   circle's glass settles in round the disc as it lands. Opening plays it back, what comes back last arriving last.
   Transform and opacity alone (the compositor's: no clip, no width animated, no blur redrawn for it), on places measured
   on a tab change or a resize, never on a scroll; the fold is written straight onto the nav as the scroll crosses a
   threshold (data-mini), so scrolling never re-renders the bar. A tab switch shows the new tab's bar as it is at once
   (no fold or unfold played after the switch). The timings live in globals.css (.fm-nav-*) */
const MINI_QUERY = '(max-width: 1023px)';
const FOLD = { collapseDistance: 150, expandDistance: 64, topGuard: 30 } as const;
// the circle: its size and the ink disc in it (globals.css, .fm-nav-ring)
const RING = 56;
const RING_INK = 46;
// after a tab switch, how long whatever the bar does (the new tab's scroll put back) happens at once
const SNAP_MS = 450;

export default function BottomNav() {
  const pathname = usePathname();
  const router = useRouter();
  const { play } = useAppHaptics();
  const trackRef = useRef<HTMLDivElement>(null);
  const itemRefs = useRef<(HTMLAnchorElement | null)[]>([]);
  const pendingResetTimerRef = useRef<number | null>(null);
  const navRef = useRef<HTMLDivElement>(null);
  const barRef = useRef<HTMLDivElement>(null);
  const glassRef = useRef<HTMLDivElement>(null);
  const miniRef = useRef<HTMLButtonElement>(null);
  const foldRef = useRef<ScrollFold | null>(null);
  const [pillBounds, setPillBounds] = useState({ x: 0, width: 86 });
  // Optimistic target: the pill + tint move the instant a tab is tapped,
  // instead of waiting for the route commit (which can be many frames later).
  const [pendingHref, setPendingHref] = useState<{ href: string; fromPathname: string } | null>(null);
  const activeIndex = useMemo(() => {
    const target = pendingHref?.fromPathname === pathname ? pendingHref.href : primaryTabHref(pathname);
    return NAV_ITEMS.findIndex((item) => target === item.href);
  }, [pendingHref, pathname]);
  // the routes that show no bar at all, and whether the bar holds the tab you're on (only then does it fold: what it
  // folds down to is that tab's icon)
  const hidden = pathname === '/login' || !!pathname?.startsWith('/command') || !!pathname?.startsWith('/drop') || !!pathname?.startsWith('/visit') || !!pathname?.startsWith('/read/') || !!pathname?.startsWith('/living-world');
  const onTab = activeIndex >= 0;

  useLayoutEffect(() => {
    if (activeIndex < 0) return;
    const track = trackRef.current;
    const item = itemRefs.current[activeIndex];
    if (!track || !item) return;

    let raf = 0;
    const measure = () => {
      if (raf) window.cancelAnimationFrame(raf);
      raf = window.requestAnimationFrame(() => {
        raf = 0;
        // where the fold goes: from the tab's place in the bar to the middle (laid-out places, never the transformed
        // ones: the bar may be folded as it is measured), written onto the nav for globals.css (.fm-nav-*)
        const nav = navRef.current;
        const bar = barRef.current;
        const glass = glassRef.current;
        const icon = item.querySelector<HTMLElement>('[data-nav-icon]');
        if (nav && bar && glass && icon) {
          // the tab, in the nav (the bar is centred in it: from the nav's own width, which keeps the half pixel
          // offsetLeft would round away)
          const navWidth = nav.getBoundingClientRect().width;
          const barLeft = (navWidth - bar.offsetWidth) / 2;
          const x = barLeft + glass.offsetLeft + glass.clientLeft + track.offsetLeft + item.offsetLeft;
          const y = bar.offsetTop + glass.offsetTop + glass.clientTop + track.offsetTop + item.offsetTop;
          const w = item.offsetWidth;
          const h = item.offsetHeight;
          // its icon, in the tab
          let iconX = 0;
          let iconY = 0;
          for (let el: HTMLElement | null = icon; el && el !== item; el = el.offsetParent as HTMLElement | null) {
            iconX += el.offsetLeft;
            iconY += el.offsetTop;
          }
          // the circle: in the middle on a whole pixel, standing on the bar's foot
          const ringX = Math.round(navWidth / 2 - RING / 2);
          const cx = ringX + RING / 2;
          const cy = nav.offsetHeight - RING / 2;
          const px = (value: number) => `${value.toFixed(2)}px`;
          const vars: Record<string, string> = {
            '--fm-ring-x': px(ringX),
            '--fm-hero-x': px(x),
            '--fm-hero-y': px(y),
            '--fm-hero-w': px(w),
            '--fm-hero-h': px(h),
            '--fm-hero-mid': px(Math.max(0, w - h)),
            // the ink: its left end's middle onto the circle's, the pair shrunk to the circle's disc
            '--fm-hero-tx': px(cx - x - h / 2),
            '--fm-hero-ty': px(cy - y - h / 2),
            '--fm-hero-s': (RING_INK / h).toFixed(4),
            // the icon: onto the circle's middle, landing on whole pixels (sharp once still)
            '--fm-hero-ix': px(Math.round(cx - icon.offsetWidth / 2) - x - iconX),
            '--fm-hero-iy': px(Math.round(cy - icon.offsetHeight / 2) - y - iconY),
          };
          for (const name in vars) nav.style.setProperty(name, vars[name]);
        }
        // (laid-out places too: a measure while the bar is folded would take its shrink for the tab's size)
        setPillBounds((current) => {
          const next = { x: item.offsetLeft, width: item.offsetWidth };
          return Math.abs(current.x - next.x) < 0.5 && Math.abs(current.width - next.width) < 0.5
            ? current
            : next;
        });
      });
    };

    measure();
    const observer = typeof ResizeObserver === 'undefined' ? null : new ResizeObserver(measure);
    observer?.observe(track);
    observer?.observe(item);
    // (a window resize only when its width changes: on an iPhone the toolbar folding mid-scroll resizes it too)
    let width = window.innerWidth;
    const onResize = () => {
      if (window.innerWidth === width) return;
      width = window.innerWidth;
      measure();
    };
    window.addEventListener('resize', onResize);

    return () => {
      if (raf) window.cancelAnimationFrame(raf);
      observer?.disconnect();
      window.removeEventListener('resize', onResize);
    };
  }, [activeIndex, hidden]);

  useEffect(() => {
    try {
      if (!pathname || pathname === '/login') return;
      const tabHref = primaryTabHref(pathname);
      const isTab = tabHref === '/' || tabHref === '/lead' || tabHref === '/read' || tabHref === '/profile';
      if (!isTab) return;
      const existing = sessionStorage.getItem('feedme:last-tab');
      const intent = sessionStorage.getItem('feedme:intent');
      if (tabHref === '/' && existing && existing !== '/' && intent !== '/') return;
      sessionStorage.setItem('feedme:last-tab', tabHref);
      sessionStorage.setItem('feedme:last-tab-ts', String(Date.now()));
    } catch {}
  }, [pathname]);

  useEffect(() => {
    router.prefetch('/');
    router.prefetch('/lead');
    router.prefetch('/read');
  }, [router]);

  useEffect(() => () => {
    if (pendingResetTimerRef.current != null) window.clearTimeout(pendingResetTimerRef.current);
  }, []);

  // the fold: followed below the laptop width while the bar holds the tab you're on, written onto the bar (and which of
  // the bar and the circle takes taps; the circle, there only on a tab, is set up here once it is)
  useEffect(() => {
    const nav = navRef.current;
    if (!nav || !onTab) return undefined;
    const query = window.matchMedia(MINI_QUERY);
    const apply = (folded: boolean) => {
      if (folded) nav.dataset.mini = '';
      else delete nav.dataset.mini;
      if (barRef.current) barRef.current.inert = folded;
      if (miniRef.current) miniRef.current.inert = !folded;
    };
    const follow = () => {
      foldRef.current?.stop();
      foldRef.current = null;
      apply(false);
      if (query.matches) foldRef.current = watchScrollFold(window, FOLD, apply);
    };
    follow();
    query.addEventListener('change', follow);
    return () => {
      query.removeEventListener('change', follow);
      foldRef.current?.stop();
      foldRef.current = null;
      apply(false);
    };
  }, [hidden, onTab]);

  /* a tab switch: the bar open, at once, and whatever the new tab's page does to it for a moment after happens at once.
     From the tap itself (an unfold still under way lands: the folded icon never shows the new tab) and again as the
     new path commits */
  const snapTimer = useRef(0);
  const snap = () => {
    const nav = navRef.current;
    if (!nav) return;
    nav.dataset.snap = '';
    foldRef.current?.reset(false);
    window.clearTimeout(snapTimer.current);
    snapTimer.current = window.setTimeout(() => {
      delete nav.dataset.snap;
    }, SNAP_MS);
  };
  const firstPath = useRef(true);
  useLayoutEffect(() => {
    if (firstPath.current) {
      firstPath.current = false;
      return;
    }
    snap();
  }, [pathname]);
  useEffect(() => () => window.clearTimeout(snapTimer.current), []);

  if (hidden) return null;

  return (
    // (from a laptop up Feed, Lead and Read are reached from the side rail: SideRail)
    <div ref={navRef} data-fm-bottom-nav="true" className={cn('fixed bottom-[calc(12px+env(safe-area-inset-bottom))] left-0 right-0 z-[180] flex justify-center pointer-events-none md:bottom-5', (pathname === '/' || pathname === '/lead' || pathname === '/read') && 'lg:hidden')}>
      {/* the bar's shadow, on a still plate behind it rather than on the glass itself (globals.css, .fm-chrome-shadow) */}
      <div ref={barRef} className="fm-nav-bar relative pointer-events-auto">
      <span aria-hidden="true" className="fm-nav-fade fm-chrome-shadow rounded-full" />
      <div ref={glassRef} className="fm-nav-fade fm-depth-chrome fm-depth-chrome--nav flex items-center gap-0.5 px-1 py-1">
        <div ref={trackRef} className="relative grid grid-cols-3 gap-0.5">
          {activeIndex >= 0 && (
            <motion.span
              aria-hidden="true"
              /* what is selected is ink (black on light, near-white on dark): red is kept for what is new. (Folding, the
                 ink you see is the hero's, from the very first frame: .fm-nav-held) */
              className="fm-nav-held absolute inset-y-0 left-0 rounded-full bg-fg shadow-[0_4px_14px_rgb(var(--fm-shadow-rgb)/0.22),0_1px_2px_rgba(0,0,0,0.08)] dark:shadow-[0_6px_20px_rgba(0,0,0,0.45)]"
              initial={false}
              animate={{ x: pillBounds.x, width: pillBounds.width }}
              transition={PILL_SPRING}
            />
          )}
          {NAV_ITEMS.map((item, index) => {
            const isActive = pathname === item.href;
            const isHighlighted = index === activeIndex;
            // folding, the other tabs glide in toward the middle (where the one you're on ends up) and go, the one
            // farthest from yours first; opening, the nearest first
            const away = Math.abs(index - activeIndex);
            const farthest = activeIndex === 1 ? 1 : 2;
            const gather = isHighlighted || activeIndex < 0 ? undefined : ({
              '--fm-nav-gather': `${Math.sign(1 - index) * 18}px`,
              '--fm-nav-out': `${(farthest - away) * 50}ms`,
              '--fm-nav-in': `${220 + (away - 1) * 60}ms`,
            } as CSSProperties);
            return (
              <Link
                key={item.label}
                ref={(node) => {
                  itemRefs.current[index] = node;
                }}
                href={item.href}
                prefetch
                scroll={false}
                aria-label={item.label}
                aria-current={isHighlighted ? 'page' : undefined}
                className={cn('group relative z-10', isHighlighted ? 'fm-nav-held' : 'fm-nav-tab')}
                style={gather}
                onClick={(event) => {
                  play(isActive ? 'navReselect' : 'navSwitch');
                  const isModifiedClick = event.metaKey || event.ctrlKey || event.shiftKey || event.altKey;
                  if (isActive && item.href === '/') {
                    event.preventDefault();
                    window.dispatchEvent(new CustomEvent('feedme:feed-tab-reselect'));
                  }
                  try {
                    sessionStorage.setItem('feedme:intent', item.href);
                    sessionStorage.setItem('feedme:intent-ts', String(Date.now()));
                  } catch {}
                  if (!isActive && !isModifiedClick && isKeptAliveTab(pathname)) {
                    // Every tab is already built and kept alive (AppTabHost), so moving between them needs no trip to
                    // the server: the URL changes in place (Next keeps usePathname in step with pushState) and the switch
                    // starts on the tap, instead of after the route's payload (and the auth proxy) answer, a beat
                    // later on wifi and longer on a phone's network
                    event.preventDefault();
                    window.history.pushState(null, '', item.href);
                  }
                  if (!isActive && !isModifiedClick) {
                    snap();
                    setPendingHref({ href: item.href, fromPathname: pathname });
                    if (pendingResetTimerRef.current != null) window.clearTimeout(pendingResetTimerRef.current);
                    pendingResetTimerRef.current = window.setTimeout(() => {
                      pendingResetTimerRef.current = null;
                      setPendingHref(null);
                    }, 1200);
                  }
                }}
              >
                <motion.div whileTap={{ scale: 0.95 }} transition={{ type: 'spring', stiffness: 380, damping: 30, mass: 0.82 }}
                  className={cn(
                    'relative flex min-w-[86px] flex-col items-center justify-center rounded-full px-3 py-2.5 transition-colors duration-300 lg:min-w-[78px] lg:px-3 lg:py-2',
                    isHighlighted ? 'text-(--fm-page)' : 'text-fg/60')}>
                  <span data-nav-icon="" className="relative z-10">
                    <item.icon size={20} strokeWidth={2.75} />
                  </span>
                  <span className="relative z-10 mt-1 text-[10px] font-black uppercase tracking-[0.14em]">
                    {item.label}
                  </span>
                </motion.div>
              </Link>
            );
          })}
        </div>
      </div>
      </div>
      {/* folded: the circle's glass in the middle (the ink in it is the tab's, carried there: .fm-nav-hero below); a tap
          opens the bar */}
      {activeIndex >= 0 ? (
        <button
          ref={miniRef}
          type="button"
          aria-label={`${NAV_ITEMS[activeIndex].label}. Show the tabs`}
          onClick={(event) => {
            // from the keys, the focus goes on to the tab you're on (the circle it was on is gone)
            const fromKeys = event.currentTarget.matches(':focus-visible');
            foldRef.current?.reset(false);
            if (fromKeys) itemRefs.current[activeIndex]?.focus({ preventScroll: true });
          }}
          className="fm-nav-ring rounded-full border-0 bg-transparent p-0 outline-none [-webkit-tap-highlight-color:transparent] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)]"
        >
          <span aria-hidden="true" className="fm-nav-ring-glass fm-chrome-shadow rounded-full" />
          <span aria-hidden="true" className="fm-nav-ring-glass fm-depth-chrome fm-depth-chrome--nav block h-full w-full rounded-full" />
        </button>
      ) : null}
      {/* the fold's one moving piece: the tab you're on, its ink and its icon, laid exactly over the bar's pill and carried
          into the circle (globals.css, .fm-nav-hero). Seen only while folded or on its way */}
      {activeIndex >= 0 ? (
        <div aria-hidden="true" className="fm-nav-hero">
          <div className="fm-nav-ink fm-nav-hero-move">
            {/* its shadow, from pieces side by side (shadows of pieces that touch add up to the whole's)... */}
            <span className="fm-nav-shade fm-nav-ink-cap fm-nav-shade-cap" />
            <span className="fm-nav-shade fm-nav-ink-band fm-nav-hero-move" />
            <span className="fm-nav-shade fm-nav-ink-end fm-nav-shade-end fm-nav-hero-move" />
            {/* ...and its ink, from pieces that overlap (no seam): two circles and the band between them */}
            <span className="fm-nav-ink-cap" />
            <span className="fm-nav-ink-band fm-nav-hero-move" />
            <span className="fm-nav-ink-end fm-nav-hero-move" />
          </div>
          {/* (the face stays put and what is on it moves: a box carried down with the icon would reach the strip at the
              screen's foot Safari colours its toolbar from, and turn the toolbar solid) */}
          <div className="absolute inset-0 flex flex-col items-center justify-center text-(--fm-page)">
            {(() => {
              const Icon = NAV_ITEMS[activeIndex].icon;
              return (
                <span className="fm-nav-hero-rider fm-nav-hero-move relative">
                  <Icon size={20} strokeWidth={2.75} />
                </span>
              );
            })()}
            <span className="fm-nav-hero-rider fm-nav-hero-label mt-1 text-[10px] font-black uppercase tracking-[0.14em]">
              {NAV_ITEMS[activeIndex].label}
            </span>
          </div>
        </div>
      ) : null}
    </div>
  );
}
