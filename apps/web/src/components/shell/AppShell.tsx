'use client';

import {
  CSSProperties,
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
import { HEADER_ROUTE_MORPH, PAGE_SURFACE_MOTION, ROUTE_CONTENT_SETTLE } from '@/lib/motion';
import { cn } from '@/lib/utils';
import AppTabHost from './AppTabHost';

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
  currentId: string | null;
  element: HTMLDivElement | null;
  setCompressed: (compressed: boolean) => void;
} | null>(null);
function isTabRoute(pathname: string) {
  return pathname === '/' || pathname === '/lead' || pathname === '/fire' || pathname === '/profile';
}

function headerIdForPathname(pathname: string) {
  if (pathname === '/') return 'feed';
  if (pathname === '/lead') return 'lead';
  if (pathname === '/fire') return 'fire';
  if (pathname === '/profile') return 'fund';
  return null;
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

export function AppHeader({ id, children, compressed }: AppHeaderProps) {
  const headerLayer = useContext(HeaderLayerContext);
  const { isRoutePresent, isRouteReady } = usePageReadyContext();
  const reduceMotion = Boolean(useReducedMotion());
  const isCurrentHeader = !id || headerLayer?.currentId === id;
  const visible = isCurrentHeader && isRoutePresent && isRouteReady;
  const headerLayerElement = headerLayer?.element ?? null;

  useEffect(() => {
    if (!headerLayer) return;
    if (visible) headerLayer.setCompressed(Boolean(compressed));
  }, [compressed, headerLayer, visible]);

  if (id && !isCurrentHeader) return null;

  return headerLayerElement
    ? createPortal(
      <motion.div
        initial={reduceMotion ? false : HEADER_ROUTE_MORPH.initial}
        animate={reduceMotion
          ? { opacity: visible ? 1 : 0, transition: { duration: 0.01 } }
          : visible
            ? HEADER_ROUTE_MORPH.animate
            : HEADER_ROUTE_MORPH.exit}
        exit={reduceMotion ? { opacity: 0, transition: { duration: 0.01 } } : HEADER_ROUTE_MORPH.exit}
        className={cn(
          'h-full w-full overflow-hidden',
          isRoutePresent ? 'relative' : 'absolute inset-x-0 top-0',
        )}
        style={{
          pointerEvents: visible ? 'auto' : 'none',
          transformOrigin: '50% 0%',
          willChange: reduceMotion ? undefined : 'transform, opacity',
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
  children: ReactNode;
}) {
  const reduceMotion = Boolean(useReducedMotion());
  const reportReady = useCallback(() => {}, []);

  const pageReadyValue = useMemo<PageReadyContextValue>(() => ({
    isRoutePresent: true,
    isRouteReady: true,
    reportReady,
  }), [reportReady]);

  return (
    <PageReadyContext.Provider value={pageReadyValue}>
      <motion.div
        data-route-surface="true"
        initial={reduceMotion ? false : PAGE_SURFACE_MOTION.initial}
        animate={reduceMotion
          ? { opacity: 1, transition: { duration: 0.01 } }
          : PAGE_SURFACE_MOTION.animate}
        className="col-start-1 row-start-1 h-full min-h-[100dvh] w-full bg-[var(--fm-page)]"
        style={{
          willChange: reduceMotion ? undefined : 'opacity',
          pointerEvents: 'auto',
        }}
      >
        <motion.div
          data-route-content="true"
          initial={reduceMotion ? false : ROUTE_CONTENT_SETTLE.initial}
          animate={reduceMotion ? { opacity: 1, y: 0, transition: { duration: 0.01 } } : ROUTE_CONTENT_SETTLE.animate}
          className="h-full min-h-[100dvh] w-full"
          style={{ willChange: reduceMotion ? undefined : 'opacity, transform' }}
        >
          {children}
        </motion.div>
      </motion.div>
    </PageReadyContext.Provider>
  );
}

export default function AppShell({ children }: { children: ReactNode }) {
  const pathname = usePathname();
  const hasTabChrome = isTabRoute(pathname);
  const currentHeaderId = headerIdForPathname(pathname);
  const routeLayerKey = hasTabChrome ? 'tabs' : pathname;
  const [headerLayerElement, setHeaderLayerElement] = useState<HTMLDivElement | null>(null);
  const [headerCompression, setHeaderCompression] = useState({ pathname: '', compressed: false });
  const setHeaderCompressed = useCallback((compressed: boolean) => {
    setHeaderCompression({ pathname, compressed });
  }, [pathname]);
  const headerCompressed = headerCompression.pathname === pathname && headerCompression.compressed;
  const shellStyle = currentHeaderId === 'lead'
    ? {
        '--fm-mobile-header-chrome-height': '152px',
        '--fm-desktop-header-chrome-height': '168px',
        '--fm-mobile-header-chrome-compressed-height': '68px',
      } as CSSProperties
    : undefined;
  const headerLayerValue = useMemo(() => ({
    currentId: currentHeaderId,
    element: headerLayerElement,
    setCompressed: setHeaderCompressed,
  }), [currentHeaderId, headerLayerElement, setHeaderCompressed]);

  return (
    <HeaderLayerContext.Provider value={headerLayerValue}>
      <div
        data-header-id={currentHeaderId ?? undefined}
        className="fm-app-shell-root relative h-full min-h-[100dvh] w-full bg-[var(--fm-page)]"
        style={shellStyle}
      >
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
              <div ref={setHeaderLayerElement} className="relative h-full overflow-hidden" />
            </div>
          </div>
        </div>
        <div className="grid h-full min-h-[100dvh] w-full">
          <RouteTransitionLayer key={routeLayerKey}>
            {hasTabChrome ? <AppTabHost pathname={pathname} /> : children}
          </RouteTransitionLayer>
        </div>
      </div>
    </HeaderLayerContext.Provider>
  );
}
