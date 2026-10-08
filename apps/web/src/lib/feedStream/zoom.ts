/* ─────────────────────────────────────────────────────────────
   FEED ZOOM — the Feed's three zooms (DAY, WEEK, MONTH): which
   one comes next, the one remembered per breakpoint, the order
   remembered, and the live pinch's spring back. How the page moves
   between them is lib/feedStream/motion.
   ───────────────────────────────────────────────────────────── */

import { ZOOM_ORDER, type FeedMode, type FeedOrder } from './contract';
import { reducedMotion } from './motion';

/* ── zooms ─────────────────────────────────────────────────── */

export type ViewBreakpoint = 'phone' | 'desktop';

export const DESKTOP_MIN_WIDTH = 1024;

export function breakpointOf(width: number): ViewBreakpoint {
  return width >= DESKTOP_MIN_WIDTH ? 'desktop' : 'phone';
}

// today on a phone, the week from 1024px
export function defaultModeFor(breakpoint: ViewBreakpoint): FeedMode {
  return breakpoint === 'desktop' ? 'week' : 'today';
}

// one step along ZOOM_ORDER: 1 is together (more posts a screen), -1 is apart (closer)
export function stepMode(mode: FeedMode, direction: 1 | -1): FeedMode | null {
  return ZOOM_ORDER[ZOOM_ORDER.indexOf(mode) + direction] ?? null;
}

// the toggle (G): the Day view ⇄ the week
export function toggledMode(mode: FeedMode): FeedMode {
  return mode === 'today' ? 'week' : 'today';
}

/* ── memory ────────────────────────────────────────────────── */

const VIEW_KEY = 'feedme:feed-view:v2';
const MODES: readonly string[] = ZOOM_ORDER;

type StoredView = { modes?: Partial<Record<ViewBreakpoint, FeedMode>>; order?: FeedOrder };

function readView(): StoredView {
  try {
    if (typeof window === 'undefined') return {};
    const raw = window.localStorage.getItem(VIEW_KEY);
    const parsed = raw ? (JSON.parse(raw) as unknown) : null;
    return parsed && typeof parsed === 'object' ? (parsed as StoredView) : {};
  } catch {
    return {};
  }
}

function writeView(patch: (view: StoredView) => StoredView) {
  try {
    if (typeof window === 'undefined') return;
    window.localStorage.setItem(VIEW_KEY, JSON.stringify(patch(readView())));
  } catch {
    // private mode, quota, blocked storage: the default is fine
  }
}

export function readStoredMode(breakpoint: ViewBreakpoint): FeedMode | null {
  const mode = readView().modes?.[breakpoint];
  return typeof mode === 'string' && MODES.includes(mode) ? mode : null;
}

export function writeStoredMode(breakpoint: ViewBreakpoint, mode: FeedMode) {
  writeView((view) => ({ ...view, modes: { ...(view.modes ?? {}), [breakpoint]: mode } }));
}

export function readStoredOrder(): FeedOrder | null {
  const order = readView().order;
  return order === 'posted' || order === 'results' ? order : null;
}

export function writeStoredOrder(order: FeedOrder) {
  writeView((view) => ({ ...view, order }));
}

/* ── the live pinch ────────────────────────────────────────── */

const SPRING_BACK_MS = 360;
const SPRING_BACK_EASE = 'cubic-bezier(0.22, 1.1, 0.36, 1)';

/* A live pinch let go short of its threshold: the page springs back from where the fingers left it */
export function springBack(el: HTMLElement, from: number): Promise<void> {
  el.style.transform = '';
  const clean = () => {
    el.style.transformOrigin = '';
    el.style.willChange = '';
  };
  if (reducedMotion() || Math.abs(from - 1) < 0.002 || typeof el.animate !== 'function') {
    clean();
    return Promise.resolve();
  }
  const animation = el.animate([{ transform: `scale(${from})` }, { transform: 'none' }], { duration: SPRING_BACK_MS, easing: SPRING_BACK_EASE });
  return animation.finished.then(clean, clean);
}
