/* The shade of a run, from its typical post (top %): above the middle (top 50%) it reddens continuously toward
   the Feed card's crimson (top 1%), below it it darkens. A copy of the engine's shade() (readerEngine.js), so
   React drawn runs wear exactly the colours of the read's trajectory. */

import type { CSSProperties } from 'react';

const SH_HI = [247, 24, 82];
const SH_MID = [84, 84, 92];
const SH_LO = [30, 30, 35];
const mix = (a: number[], b: number[], t: number) => a.map((x, i) => Math.round(x + (b[i] - x) * t));

export function shade(v: number) {
  const t = Math.min(100, Math.max(1, v));
  if (t <= 50) {
    const k = Math.pow((50 - t) / 49, 0.9);
    const g = Math.max(0, (k - 0.35) / 0.65);
    return { c: `rgb(${mix(SH_MID, SH_HI, k)})`, tx: '#fff', gc: g ? `rgba(247,24,82,${(g * 0.6).toFixed(2)})` : 'transparent', gi: (0.1 + g * 0.24).toFixed(2) };
  }
  const w = Math.min(1, (t - 50) / 35);
  return { c: `rgb(${mix(SH_MID, SH_LO, w)})`, tx: `rgba(255,255,255,${(0.9 - w * 0.36).toFixed(2)})`, gc: 'transparent', gi: '0.08' };
}

// the same custom properties the read's run boxes take (--c fill, --tx text, --gc glow, --gi inner light)
export function shadeStyle(v: number): CSSProperties {
  const x = shade(v);
  return { '--c': x.c, '--tx': x.tx, '--gc': x.gc, '--gi': x.gi } as CSSProperties;
}

export const RAMP = `linear-gradient(90deg, ${[1, 8, 15, 22, 29, 36, 43, 50, 60, 70, 85, 100].map((v) => `${shade(v).c} ${(((v - 1) / 99) * 100).toFixed(1)}%`).join(', ')})`;
