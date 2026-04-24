'use client';

import { ReactNode } from 'react';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { usePathname } from 'next/navigation';

const APPLE_EASE = [0.32, 0.72, 0, 1] as const;

function tabRoot(pathname: string): string {
  if (pathname === '/fire' || pathname.startsWith('/fire/')) return '/fire';
  if (pathname === '/profile' || pathname.startsWith('/profile/')) return '/profile';
  if (pathname === '/login' || pathname.startsWith('/login/')) return '/login';
  return '/';
}

export default function AppRouteTransition({ children }: { children: ReactNode }) {
  const pathname = usePathname() || '/';
  const reduceMotion = useReducedMotion();
  const routeRoot = tabRoot(pathname);

  const variants = reduceMotion
    ? {
        enter: { opacity: 1 },
        center: { opacity: 1 },
        exit: { opacity: 1 },
      }
    : {
        enter: {
          opacity: 0,
          x: 16,
          y: 6,
          scale: 0.994,
        },
        center: {
          opacity: 1,
          x: 0,
          y: 0,
          scale: 1,
        },
        exit: {
          opacity: 0,
          x: -12,
          y: -4,
          scale: 0.996,
        },
      };

  return (
    <AnimatePresence initial={false} mode="wait">
      <motion.div
        key={routeRoot}
        variants={variants}
        initial="enter"
        animate="center"
        exit="exit"
        transition={{
          x: { type: 'spring', stiffness: 360, damping: 38, mass: 0.78 },
          y: { type: 'spring', stiffness: 360, damping: 38, mass: 0.78 },
          scale: { duration: 0.22, ease: APPLE_EASE },
          opacity: { duration: 0.18, ease: APPLE_EASE },
        }}
        className="h-full min-h-[100dvh] w-full overflow-hidden"
        style={{ willChange: reduceMotion ? undefined : 'transform, opacity' }}
      >
        {children}
      </motion.div>
    </AnimatePresence>
  );
}
