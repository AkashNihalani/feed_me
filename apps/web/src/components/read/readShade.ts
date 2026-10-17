/* The shade of a run, from its typical post (Top %): seven fixed steps of the theme's ramp (--fm-shade-0…6,
   globals.css). Below the median a run takes one of three greys that recede into the page; top 26–50% and 11–25% are
   soft tints of the accent, top 4–10% and 1–3% are solid, and the top step carries a little light. Fixed steps, not a
   continuous mix: a mix of grey into the accent passes through muddy middles and through tones where neither ink nor
   white reads. A copy of the engine's shade() (readerEngine.js), so React-drawn runs wear exactly the read's colours.

   The colours are tokens, not values: a theme switch recolours every run at once, with nothing to re-render. */

import type { CSSProperties } from 'react';

// which of the seven steps a Top % falls on (0 = bottom 15%, 6 = top 1–3%)
export function shadeStep(v: number) {
  const t = Math.min(100, Math.max(1, v));
  return t <= 3 ? 6 : t <= 10 ? 5 : t <= 25 ? 4 : t <= 50 ? 3 : t <= 65 ? 2 : t <= 85 ? 1 : 0;
}

// the inner light on a box, stronger toward the top
const INNER = ['0.08', '0.08', '0.08', '0.1', '0.16', '0.24', '0.34'];

export function shade(v: number) {
  const step = shadeStep(v);
  return {
    c: `var(--fm-shade-${step})`,
    // the solid steps always carry white; the greys and tints take the theme's number colour (ink on light)
    tx: step >= 5 ? '#ffffff' : 'var(--fm-shade-tx)',
    gc: step === 6 ? 'var(--fm-shade-glow)' : 'transparent',
    gi: INNER[step],
  };
}

// a read that landed without a rank (rare): the ramp's far end
export const UNRANKED_SHADE = { c: 'var(--fm-shade-0)', tx: 'var(--fm-shade-tx)', gc: 'transparent', gi: '0.08' };

// the same custom properties the read's run boxes take (--c fill, --tx text, --gc glow, --gi inner light)
export function shadeStyle(v: number): CSSProperties {
  const x = shade(v);
  return { '--c': x.c, '--tx': x.tx, '--gc': x.gc, '--gi': x.gi } as CSSProperties;
}

// the ramp as a legend, top 1% on the left: one hard band per step, each as wide as the Top % range it covers
const EDGES = [1, 3.5, 10.5, 25.5, 50.5, 65.5, 85.5, 100];
const at = (v: number) => `${(((v - 1) / 99) * 100).toFixed(1)}%`;
export const RAMP = `linear-gradient(90deg, ${EDGES.slice(0, -1).map((from, i) => `${shade(Math.ceil(from)).c} ${at(from)} ${at(EDGES[i + 1])}`).join(', ')})`;
