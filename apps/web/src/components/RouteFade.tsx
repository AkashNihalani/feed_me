'use client';

import { ReactNode } from 'react';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { usePathname } from 'next/navigation';

// Collapse nested routes into their tab root so /fire/[id] -> /fire doesn't
// trigger a fade. Only true tab-to-tab navigation animates.
function tabRoot(pathname: string): string {
  if (pathname.startsWith('/fire')) return '/fire';
  if (pathname.startsWith('/profile')) return '/profile';
  if (pathname.startsWith('/feed')) return '/feed';
  if (pathname.startsWith('/login')) return '/login';
  return '/';
}

export default function RouteFade({ children }: { children: ReactNode }) {
  const pathname = usePathname() || '/';
  const reduceMotion = useReducedMotion();
  const routeKey = tabRoot(pathname);

  if (reduceMotion) {
    return <>{children}</>;
  }

  return (
    <AnimatePresence initial={false} mode="wait">
      <motion.div
        key={routeKey}
        initial={{ opacity: 0 }}
        animate={{ opacity: 1, transition: { duration: 0.16, ease: [0.22, 1, 0.36, 1] } }}
        exit={{ opacity: 0, transition: { duration: 0.12, ease: [0.22, 1, 0.36, 1] } }}
        className="h-full min-h-[100dvh] w-full"
      >
        {children}
      </motion.div>
    </AnimatePresence>
  );
}
