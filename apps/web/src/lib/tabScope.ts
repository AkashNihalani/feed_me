/* ─────────────────────────────────────────────────────────────
   TAB SCOPE — the one pick every tab shares: which feed is open
   on the story rail (null: all feeds) and which feeder is picked.
   Pick @anuj on Lead, tap Read, and Read opens on Anuj; switching
   tabs never moves the rail.

   feedId is undefined only while a feeder was picked before the
   feeds had loaded (the tab works out its feed from the handle).
   A small external store, so a pick re-renders only who reads it.
   ───────────────────────────────────────────────────────────── */

import { startTransition, useEffect, useLayoutEffect, useState, useSyncExternalStore } from 'react';
import { afterDrawn } from '@/lib/webkitMotion';

const useIsomorphicLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

export type TabScope = { feedId: string | null | undefined; handle: string | null };

let scope: TabScope = { feedId: null, handle: null };
const listeners = new Set<() => void>();

export function getTabScope() {
  return scope;
}

export function setTabScope(next: TabScope) {
  if (next.feedId === scope.feedId && next.handle === scope.handle) return;
  scope = { feedId: next.feedId, handle: next.handle };
  listeners.forEach((listener) => listener());
}

export function subscribeTabScope(listener: () => void) {
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

// for what must answer a pick at once (the header's rail)
export function useTabScope() {
  return useSyncExternalStore(subscribeTabScope, getTabScope, getTabScope);
}

/* for a page that is heavy to re-render: it follows the pick once the header's answer to the tap is on screen (the
   rail's circles and rings already moving, on the compositor, where a heavy page can't hold them up), and then in a
   transition, so its re-render yields to the frames the header is animating. A heavy commit landing on the rail's
   first frames was what made the circles start late and jump. Coming back on screen it catches up before the first
   paint */
export function useTabScopeTransition() {
  const [current, setCurrent] = useState(getTabScope);
  useIsomorphicLayoutEffect(() => {
    setCurrent(getTabScope());
    let cancel = () => {};
    const unsubscribe = subscribeTabScope(() => {
      cancel();
      cancel = afterDrawn(() => startTransition(() => setCurrent(getTabScope())));
    });
    return () => {
      cancel();
      unsubscribe();
    };
  }, []);
  return current;
}
