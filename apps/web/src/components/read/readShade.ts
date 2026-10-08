/* The shade of a run, from its typical post (top %): above the middle (top 50%) it reddens continuously toward
   the Feed card's crimson (top 1%), below it it recedes into the page (darkens on dark, pales on light). A copy of
   the engine's shade() (readerEngine.js), so React drawn runs wear exactly the colours of the read's trajectory.

   The colours are CSS mixes of the theme's ramp (--fm-shade-*, globals.css), not fixed values: a theme switch
   recolours every run at once, with nothing to re-render. On dark they mix to the same values the ramp always had. */

import type { CSSProperties } from 'react';

const pct = (t: number) => `${(t * 100).toFixed(1)}%`;
// a toward b by t, in sRGB (what the old per-channel mix did)
const mix = (a: string, b: string, t: number) => `color-mix(in srgb, ${a}, ${b} ${pct(t)})`;

export function shade(v: number) {
  const t = Math.min(100, Math.max(1, v));
  if (t <= 50) {
    const k = Math.pow((50 - t) / 49, 0.9);
    const g = Math.max(0, (k - 0.35) / 0.65);
    return { c: mix('var(--fm-shade-mid)', 'var(--fm-shade-hi)', k), tx: 'var(--fm-shade-tx-hi)', gc: g ? `rgba(247,24,82,${(g * 0.6).toFixed(2)})` : 'transparent', gi: (0.1 + g * 0.24).toFixed(2) };
  }
  const w = Math.min(1, (t - 50) / 35);
  return { c: mix('var(--fm-shade-mid)', 'var(--fm-shade-lo)', w), tx: mix('var(--fm-shade-tx-mid)', 'var(--fm-shade-tx-lo)', w), gc: 'transparent', gi: '0.08' };
}

// a read that landed without a rank (rare): the ramp's far end
export const UNRANKED_SHADE = { c: 'var(--fm-shade-lo)', tx: 'var(--fm-shade-tx-mid)', gc: 'transparent', gi: '0.08' };

// the same custom properties the read's run boxes take (--c fill, --tx text, --gc glow, --gi inner light)
export function shadeStyle(v: number): CSSProperties {
  const x = shade(v);
  return { '--c': x.c, '--tx': x.tx, '--gc': x.gc, '--gi': x.gi } as CSSProperties;
}

export const RAMP = `linear-gradient(90deg, ${[1, 8, 15, 22, 29, 36, 43, 50, 60, 70, 85, 100].map((v) => `${shade(v).c} ${(((v - 1) / 99) * 100).toFixed(1)}%`).join(', ')})`;
