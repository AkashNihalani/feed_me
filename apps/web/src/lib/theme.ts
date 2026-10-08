/* ─────────────────────────────────────────────────────────────
   THEME — light, dark, or auto (the device's own setting).
   Light is the default for anyone who hasn't picked.

   The page is themed by two classes on <html> (.light / .dark),
   set before first paint by layout.tsx's bootstrap script, which
   reads the same key and resolves auto the same way. Everything
   that colours itself reads CSS tokens (globals.css, stream.css,
   readTab.css), so a switch is one class flip: no React render.
   What paints from JS (an SVG fill, a canvas) reads the resolved
   theme through useResolvedTheme.

   A switch made from a control can be drawn as a circle growing
   out of that control (View Transitions), where the browser has
   them and motion isn't reduced; otherwise it flips at once.
   ───────────────────────────────────────────────────────────── */

import { useSyncExternalStore } from 'react';
import { DARK_QUERY, DEFAULT_THEME, THEME_COLORS, THEME_STORAGE_KEY, type ResolvedTheme, type ThemePreference } from './themeBootstrap';

export { DEFAULT_THEME, THEME_COLORS, THEME_STORAGE_KEY, type ResolvedTheme, type ThemePreference };

const REVEAL_MS = 560;

const listeners = new Set<() => void>();
let preference: ThemePreference = DEFAULT_THEME;
let resolved: ResolvedTheme = 'light';
let started = false;

function readPreference(): ThemePreference {
  try {
    const saved = window.localStorage.getItem(THEME_STORAGE_KEY);
    if (saved === 'light' || saved === 'dark' || saved === 'auto') return saved;
  } catch {
    // storage blocked: the default
  }
  return DEFAULT_THEME;
}

function systemTheme(): ResolvedTheme {
  return window.matchMedia?.(DARK_QUERY).matches ? 'dark' : 'light';
}

function resolve(next: ThemePreference): ResolvedTheme {
  return next === 'auto' ? systemTheme() : next;
}

function paint(theme: ResolvedTheme) {
  const root = document.documentElement;
  root.classList.toggle('dark', theme === 'dark');
  root.classList.toggle('light', theme === 'light');
  root.style.colorScheme = theme;
  document.querySelectorAll<HTMLMetaElement>('meta[name="theme-color"]').forEach((meta) => {
    meta.setAttribute('content', THEME_COLORS[theme]);
  });
}

function emit() {
  listeners.forEach((listener) => listener());
}

function settle(next: ThemePreference) {
  preference = next;
  const theme = resolve(next);
  if (theme !== resolved || !document.documentElement.classList.contains(theme)) paint(theme);
  resolved = theme;
  emit();
}

// once, on the client: what the bootstrap script already applied, then the device and the other tabs
function start() {
  if (started || typeof window === 'undefined') return;
  started = true;
  preference = readPreference();
  resolved = resolve(preference);
  paint(resolved);
  window.matchMedia?.(DARK_QUERY).addEventListener?.('change', () => {
    if (preference === 'auto') settle('auto');
  });
  window.addEventListener('storage', (event) => {
    if (event.key === THEME_STORAGE_KEY) settle(readPreference());
  });
}

function subscribe(listener: () => void) {
  start();
  listeners.add(listener);
  return () => {
    listeners.delete(listener);
  };
}

export function getThemePreference() {
  start();
  return preference;
}

export function getResolvedTheme() {
  start();
  return resolved;
}

type ViewTransitionDocument = Document & {
  startViewTransition?: (update: () => void) => { ready: Promise<void>; finished: Promise<void> };
};

/* Set the preference. `from` (a point on screen, usually the control's centre) draws the change as a circle growing
   out of it; without one, or without View Transitions, or with reduced motion, the theme flips at once. */
export function setThemePreference(next: ThemePreference, from?: { x: number; y: number }) {
  start();
  try {
    window.localStorage.setItem(THEME_STORAGE_KEY, next);
  } catch {
    // storage blocked: the choice lasts this visit
  }
  const changes = resolve(next) !== resolved;
  const doc = document as ViewTransitionDocument;
  const reduce = window.matchMedia?.('(prefers-reduced-motion: reduce)').matches;
  if (!changes || !from || reduce || typeof doc.startViewTransition !== 'function') {
    settle(next);
    return;
  }
  const root = document.documentElement;
  const radius = Math.hypot(Math.max(from.x, window.innerWidth - from.x), Math.max(from.y, window.innerHeight - from.y));
  root.dataset.themeReveal = '';
  const transition = doc.startViewTransition(() => settle(next));
  transition.ready
    .then(() => {
      root.animate(
        { clipPath: [`circle(0px at ${from.x}px ${from.y}px)`, `circle(${radius}px at ${from.x}px ${from.y}px)`] },
        { duration: REVEAL_MS, easing: 'cubic-bezier(0.32, 0.72, 0, 1)', pseudoElement: '::view-transition-new(root)' },
      );
    })
    .catch(() => undefined);
  transition.finished
    .catch(() => undefined)
    .finally(() => {
      delete root.dataset.themeReveal;
    });
}

const serverPreference = () => DEFAULT_THEME;
const serverResolved = (): ResolvedTheme => 'light';

export function useThemePreference() {
  return useSyncExternalStore(subscribe, getThemePreference, serverPreference);
}

// for what paints its colours from JS (SVG fills, canvases): re-renders when the page's theme changes
export function useResolvedTheme() {
  return useSyncExternalStore(subscribe, getResolvedTheme, serverResolved);
}
