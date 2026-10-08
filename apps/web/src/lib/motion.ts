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

// ── The switch clock ─────────────────────────────────────────────────────────
// ONE duration + ease for everything that moves when the user changes surface:
// the tab content settle (AppTabHost), the header content entrance (AppShell),
// and in-tab state swaps (PAGE_DISSOLVE). A switch should read as a single
// coordinated motion, not three elements on private timers. The bottom-nav
// pill keeps its spring — it's the gesture's initiator, and its settle time
// already lands within this window.
export const SWITCH_CLOCK_MS = 260;
export const SWITCH_CLOCK_S = SWITCH_CLOCK_MS / 1000;
export const SWITCH_CLOCK_CSS_EASE = 'cubic-bezier(0.22, 1, 0.36, 1)';

// ── The page beat ────────────────────────────────────────────────────────────
// ONE clock for everything that arrives or leaves when a page changes what it shows (a pick on the story rail, a
// board re-dealt): the header's circles, the page's pieces and the rail's ring all move on it, so the circles and
// the rows they summon land together. Quick and light, the way Feed's tiles settle: pieces arrive a BEAT_STEP apart
// (at most BEAT_MAX_STEPS steps, so a long list never trails), each settling on BEAT_ARRIVE; they leave faster, on
// BEAT_LEAVE. Animate it as opacity plus a transform STRING (framer runs those on the compositor).
export const BEAT_EASE = [0.16, 0.9, 0.2, 1] as const;
export const BEAT_LEAVE_EASE = [0.45, 0, 0.2, 1] as const;
export const BEAT_EASE_CSS = 'cubic-bezier(0.16, 0.9, 0.2, 1)';
export const BEAT_ARRIVE_S = 0.48;
export const BEAT_LEAVE_S = 0.26;
export const BEAT_START_S = 0.04;
export const BEAT_STEP_S = 0.04;
export const BEAT_MAX_STEPS = 8;
export const beatDelay = (step: number) => BEAT_START_S + Math.min(Math.max(step, 0), BEAT_MAX_STEPS) * BEAT_STEP_S;
// a piece arriving at its step / leaving at its step (leaves are half as far apart)
export const beatArrive = (step: number) => ({ duration: BEAT_ARRIVE_S, ease: BEAT_EASE, delay: beatDelay(step) });
export const beatLeave = (step: number) => ({ duration: BEAT_LEAVE_S, ease: BEAT_LEAVE_EASE, delay: Math.min(Math.max(step, 0), 6) * (BEAT_STEP_S / 2) });
// a list whose items arrive on the beat (wrap in BEAT_CONTAINER, give each child BEAT_ITEM)
export const BEAT_CONTAINER = {
  hidden: {},
  visible: { transition: { staggerChildren: BEAT_STEP_S, delayChildren: BEAT_START_S } },
} as const;
export const BEAT_ITEM = {
  hidden: { opacity: 0, transform: 'translateY(12px)' },
  visible: { opacity: 1, transform: 'translateY(0px)', transition: { duration: BEAT_ARRIVE_S, ease: BEAT_EASE } },
} as const;
// one piece settling in on its own at a step (a row, a card): the same move as BEAT_ITEM
export const BEAT_HIDDEN = { opacity: 0, transform: 'translateY(12px)' } as const;
export const BEAT_SHOWN = { opacity: 1, transform: 'translateY(0px)' } as const;

export const PAGE_DISSOLVE = {
  initial: { opacity: 0 },
  animate: {
    opacity: 1,
    transition: { duration: SWITCH_CLOCK_S, ease: GRID_ITEM_EASE },
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

// Cross-route arrival: a real dissolve + rise on the switch clock. Cross-surface
// changes have no shared elements, so a dissolve (not physics) is the grammar.
export const ROUTE_CONTENT_SETTLE = {
  initial: {
    opacity: 0,
    y: 8,
  },
  animate: {
    opacity: 1,
    y: 0,
    transition: { duration: SWITCH_CLOCK_S, ease: GRID_ITEM_EASE },
  },
} as const;

// Header content entrance on route/tab change. The outgoing header unmounts
// hard (its portal returns null the moment the route flips), so the incoming
// side carries the whole transition: rise + fade on the switch clock, against
// the glass capsule that never moves. exit doubles as the "mounted but not
// ready" resting state, so it must be invisible.
export const HEADER_ROUTE_MORPH = {
  initial: {
    opacity: 0,
    y: 6,
    scale: 1,
  },
  animate: {
    opacity: 1,
    y: 0,
    scale: 1,
    transition: { duration: SWITCH_CLOCK_S, ease: GRID_ITEM_EASE },
  },
  exit: {
    opacity: 0,
    y: 0,
    scale: 1,
    transition: { duration: 0.12, ease: GRID_ITEM_EASE },
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
