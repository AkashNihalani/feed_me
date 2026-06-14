// Shared framer-motion tokens. Use these so animations feel identical across
// surfaces (feeder grid, fire desktop grid, etc.) instead of drifting per-page.

export const GRID_LAYOUT_SPRING = {
  type: 'spring',
  stiffness: 300,
  damping: 28,
  mass: 0.86,
} as const;

export const PILL_SPRING = {
  type: 'spring',
  stiffness: 340,
  damping: 36,
  mass: 0.9,
} as const;

export const GRID_ITEM_EASE = [0.22, 1, 0.36, 1] as const;
export const PAGE_EXIT_EASE = [0.42, 0, 1, 1] as const;

export const PAGE_DISSOLVE = {
  initial: { opacity: 0 },
  animate: {
    opacity: 1,
    transition: { duration: 0.24, ease: GRID_ITEM_EASE },
  },
  exit: {
    opacity: 0,
    transition: { duration: 0.22, ease: PAGE_EXIT_EASE },
  },
} as const;

export const PAGE_SURFACE_MOTION = {
  initial: {
    opacity: 1,
  },
  animate: {
    opacity: 1,
    transition: { duration: 0.01, ease: GRID_ITEM_EASE },
  },
  exit: {
    opacity: 1,
    transition: { duration: 0.01, ease: GRID_ITEM_EASE },
  },
} as const;

export const ROUTE_CONTENT_SETTLE = {
  initial: {
    opacity: 0.985,
    y: 2,
  },
  animate: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.16, ease: GRID_ITEM_EASE },
  },
} as const;

export const TAB_CONTENT_SETTLE = {
  initial: {
    opacity: 1,
    x: 0,
    y: 0,
  },
  animate: {
    opacity: 1,
    x: 0,
    y: 0,
    transition: { duration: 0.34, ease: GRID_ITEM_EASE },
  },
  exit: {
    opacity: 1,
    x: 0,
    y: 0,
    transition: { duration: 0.34, ease: GRID_ITEM_EASE },
  },
} as const;

export const HEADER_ROUTE_MORPH = {
  initial: {
    opacity: 0.985,
    y: 2,
    scale: 1,
  },
  animate: {
    opacity: 1,
    y: 0,
    scale: 1,
    transition: { duration: 0.16, ease: GRID_ITEM_EASE },
  },
  exit: {
    opacity: 1,
    y: 0,
    scale: 1,
    transition: { duration: 0.01, ease: GRID_ITEM_EASE },
  },
} as const;

export const HEADER_COLLAPSE_SPRING = {
  type: 'spring',
  stiffness: 380,
  damping: 38,
  mass: 0.86,
} as const;

export const GRID_ITEM_TRANSITION = {
  layout: GRID_LAYOUT_SPRING,
  opacity: { duration: 0.18, ease: GRID_ITEM_EASE },
  y: { duration: 0.24, ease: GRID_ITEM_EASE },
  scale: { duration: 0.24, ease: GRID_ITEM_EASE },
} as const;

// ── Canonical "slot" element motion ─────────────────────────────────────────
// ONE token for element entrance/exit across the app so the feed grid, feed
// dashboard panels, fund panels and the fire deck all read identically:
// children slot in one-by-one and leave the same way ("comes in, goes out
// same"). Wrap a list/grid in SLOT_CONTAINER (initial="hidden", animate bound to
// the surface's active state so it re-fires every time you arrive), and give
// each direct child SLOT_ITEM.
// Stagger cadence shared by the container variant AND surfaces that animate each
// element on its own mount (per-tile delay), so the slot rhythm reads the same
// whether a list is driven by a container or by per-item entrances.
export const SLOT_STAGGER_STEP = 0.05;
export const SLOT_STAGGER_MAX = 0.28;

export const SLOT_CONTAINER = {
  hidden: {},
  visible: {
    transition: { staggerChildren: SLOT_STAGGER_STEP, delayChildren: 0.03 },
  },
  exit: {
    transition: { staggerChildren: 0.022, staggerDirection: -1 },
  },
} as const;

export const SLOT_ITEM = {
  hidden: { y: 20, opacity: 0, scale: 0.985 },
  visible: {
    y: 0,
    opacity: 1,
    scale: 1,
    transition: {
      opacity: { duration: 0.22, ease: GRID_ITEM_EASE },
      y: { type: 'spring', stiffness: 320, damping: 32, mass: 0.88 },
      scale: { duration: 0.26, ease: GRID_ITEM_EASE },
    },
  },
  exit: {
    y: 12,
    opacity: 0,
    scale: 0.985,
    transition: { duration: 0.18, ease: PAGE_EXIT_EASE },
  },
} as const;

// Back-compat aliases — existing imports keep working, now with a shared exit.
export const PANEL_STAGGER_CONTAINER = SLOT_CONTAINER;
export const PANEL_TILE_VARIANT = SLOT_ITEM;

// Header micro-cascade — opacity only, no positional motion. Used by every
// tab's header so rows don't add a second physical settle on route change.
// Wrap the header container with HEADER_STAGGER_CONTAINER, and each row with
// HEADER_ROW.
export const HEADER_STAGGER_CONTAINER = {
  initial: {},
  animate: {
    transition: { staggerChildren: 0.05, delayChildren: 0.02 },
  },
} as const;

export const HEADER_ROW = {
  initial: { opacity: 0 },
  animate: {
    opacity: 1,
    transition: { duration: 0.22, ease: GRID_ITEM_EASE },
  },
} as const;
