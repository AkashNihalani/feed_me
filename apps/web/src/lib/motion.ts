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
  stiffness: 420,
  damping: 34,
  mass: 0.78,
} as const;

export const GRID_ITEM_EASE = [0.22, 1, 0.36, 1] as const;

export const GRID_ITEM_TRANSITION = {
  layout: GRID_LAYOUT_SPRING,
  opacity: { duration: 0.18, ease: GRID_ITEM_EASE },
  y: { duration: 0.24, ease: GRID_ITEM_EASE },
  scale: { duration: 0.24, ease: GRID_ITEM_EASE },
} as const;
