'use client';

import {
  ReactNode,
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from 'react';
import { createPortal } from 'react-dom';
import { usePathname } from 'next/navigation';
import { motion, useReducedMotion } from 'framer-motion';
import { PAGE_DISSOLVE } from '@/lib/motion';
import { cn } from '@/lib/utils';

type PageReadyContextValue = {
  isRoutePresent: boolean;
  isRouteReady: boolean;
  reportReady: (ready?: boolean) => void;
};

type AppHeaderProps = {
  id?: string;
  compressed?: boolean;
  children: ReactNode;
};

const PageReadyContext = createContext<PageReadyContextValue | null>(null);
const HeaderLayerContext = createContext<{
  element: HTMLDivElement | null;
  setCompressed: (compressed: boolean) => void;
} | null>(null);
function isTabRoute(pathname: string) {
  return pathname === '/' || pathname === '/fire' || pathname === '/profile';
}

function usePageReadyContext() {
  const context = useContext(PageReadyContext);
  if (!context) {
    throw new Error('usePageReady must be used inside AppShell');
  }
  return context;
}

export function usePageReady(ready: boolean) {
  const { reportReady } = usePageReadyContext();

  useEffect(() => {
    reportReady(ready);
  }, [ready, reportReady]);
}

export function AppHeader({ children, compressed }: AppHeaderProps) {
  const headerLayer = useContext(HeaderLayerContext);
  const { isRoutePresent, isRouteReady } = usePageReadyContext();
  const reduceMotion = Boolean(useReducedMotion());
  const visible = isRoutePresent && isRouteReady;
  const headerLayerElement = headerLayer?.element ?? null;

  useEffect(() => {
    if (!headerLayer) return;
    if (visible) headerLayer.setCompressed(Boolean(compressed));
  }, [compressed, headerLayer, visible]);

  return headerLayerElement
    ? createPortal(
      <motion.div
        initial={false}
        animate={{ opacity: visible ? 1 : 0 }}
        transition={reduceMotion ? { duration: 0.01 } : PAGE_DISSOLVE.animate.transition}
        className={cn(
          'w-full overflow-hidden',
          isRoutePresent ? 'relative' : 'absolute inset-x-0 top-0',
        )}
        style={{
          pointerEvents: visible ? 'auto' : 'none',
          willChange: reduceMotion ? undefined : 'opacity',
        }}
      >
        {children}
      </motion.div>,
      headerLayerElement,
    )
    : null;
}

function RouteTransitionLayer({
  children,
}: {
  autoReady: boolean;
  children: ReactNode;
  routeKey: string;
}) {
  const reportReady = useCallback(() => {}, []);

  const pageReadyValue = useMemo<PageReadyContextValue>(() => ({
    isRoutePresent: true,
    isRouteReady: true,
    reportReady,
  }), [reportReady]);

  return (
    <PageReadyContext.Provider value={pageReadyValue}>
      <div className="col-start-1 row-start-1 h-full min-h-[100dvh] w-full">
        {children}
      </div>
    </PageReadyContext.Provider>
  );
}

export default function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const hasTabChrome = isTabRoute(pathname);
  const [headerLayerElement, setHeaderLayerElement] = useState<HTMLDivElement | null>(null);
  const [headerCompression, setHeaderCompression] = useState({ pathname: '', compressed: false });
  const setHeaderCompressed = useCallback((compressed: boolean) => {
    setHeaderCompression({ pathname, compressed });
  }, [pathname]);
  const headerCompressed = headerCompression.pathname === pathname && headerCompression.compressed;
  const headerLayerValue = useMemo(() => ({
    element: headerLayerElement,
    setCompressed: setHeaderCompressed,
  }), [headerLayerElement, setHeaderCompressed]);

  return (
    <HeaderLayerContext.Provider value={headerLayerValue}>
      <div className="fm-app-shell-root relative h-full min-h-[100dvh] w-full bg-[#f4f7f9] dark:bg-[#030303]">
        <div
          className={cn(
            'pointer-events-none fixed inset-x-0 top-0 z-[100] flex flex-col items-center px-2 pt-[calc(10px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-4 sm:pt-[calc(14px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] md:pt-[calc(20px+var(--pwa-top-fix,0px))] lg:px-4',
            !hasTabChrome && 'hidden',
          )}
        >
          <div className="relative fm-tab-header-shell">
            <div
              className={cn(
                'fm-depth-chrome fm-depth-chrome--header pointer-events-auto w-full',
                headerCompressed && 'fm-depth-chrome--header-compressed',
              )}
            >
              <div ref={setHeaderLayerElement} className="relative overflow-hidden" />
            </div>
          </div>
        </div>
        <div className="grid h-full min-h-[100dvh] w-full">
          <RouteTransitionLayer key={pathname} routeKey={pathname} autoReady={true}>
            {children}
          </RouteTransitionLayer>
        </div>
      </div>
    </HeaderLayerContext.Provider>
  );
}
