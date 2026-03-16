'use client';

import { usePathname } from 'next/navigation';
import { AnimatePresence, motion } from 'framer-motion';
import { useRef, useCallback, useMemo } from 'react';

/* ── Tab order for direction hint ── */
const TAB_ORDER: Record<string, number> = { '/': 0, '/fire': 1, '/profile': 2 };

function getTabIndex(path: string): number {
  return TAB_ORDER[path] ?? -1;
}

export default function PageTransitionProvider({ children }: { children: React.ReactNode }) {
  const pathname = usePathname();
  const prevPathRef = useRef(pathname);
  const scrollPositions = useRef<Map<string, number>>(new Map());

  /* Compute direction: 1 = forward, -1 = backward */
  const direction = useMemo(() => {
    const prevIdx = getTabIndex(prevPathRef.current);
    const nextIdx = getTabIndex(pathname);
    if (prevIdx < 0 || nextIdx < 0) return 1;
    if (nextIdx > prevIdx) return 1;
    if (nextIdx < prevIdx) return -1;
    return 0;
  }, [pathname]);

  if (pathname !== prevPathRef.current) {
    prevPathRef.current = pathname;
  }

  /* Save scroll position before exit */
  const handleExitStart = useCallback(() => {
    const scrollContainer = document.querySelector('[data-page-scroll]');
    if (scrollContainer && prevPathRef.current) {
      scrollPositions.current.set(prevPathRef.current, scrollContainer.scrollTop);
    }
  }, []);

  /* Restore scroll position after enter */
  const handleAnimationComplete = useCallback(() => {
    const scrollContainer = document.querySelector('[data-page-scroll]');
    const saved = scrollPositions.current.get(pathname);
    if (scrollContainer && saved != null) {
      scrollContainer.scrollTop = saved;
    }
  }, [pathname]);

  /*
   * Ultra-fast cross-fade with a subtle directional hint.
   * Minimal x-offset (6%) so the header barely shifts.
   * Total duration ~160ms — instant-feeling on both desktop and mobile.
   */
  const variants = {
    enter: (dir: number) => ({
      x: dir === 0 ? 0 : dir > 0 ? '6%' : '-6%',
      opacity: 0,
    }),
    center: {
      x: 0,
      opacity: 1,
    },
    exit: (dir: number) => ({
      x: dir === 0 ? 0 : dir > 0 ? '-4%' : '4%',
      opacity: 0,
    }),
  };

  return (
    <AnimatePresence
      mode="popLayout"
      initial={false}
      custom={direction}
      onExitComplete={handleExitStart}
    >
      <motion.div
        key={pathname}
        custom={direction}
        variants={variants}
        initial="enter"
        animate="center"
        exit="exit"
        transition={{
          x: { duration: 0.16, ease: [0.4, 0, 0.1, 1] },
          opacity: { duration: 0.12, ease: 'easeOut' },
        }}
        onAnimationComplete={handleAnimationComplete}
        className="h-full w-full will-change-transform transform-gpu"
        style={{ position: 'absolute', inset: 0 }}
      >
        {children}
      </motion.div>
    </AnimatePresence>
  );
}
