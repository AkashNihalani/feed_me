'use client';

/* ─────────────────────────────────────────────────────────────
   LEAD CANVAS — how Lead's charts draw and move.

   A chart is one canvas: one layer for the browser to keep, no
   matter how many bars, cells or bubbles it holds. A change of
   window or scope never re-renders the chart's elements; the
   canvas tweens from the scene on screen to the next one on the
   Feed's carry curve (lib/feedStream/motion), drawing each frame
   itself. Nothing on the page is restyled, nothing is laid out,
   no layer is made or dropped while it moves. A pick (a hover, a
   finger, a tap) is drawn at once, without a tween.

   Colours come from the theme's own tokens, resolved once per
   theme (a light/dark switch redraws with the new ones), and the
   canvas is drawn at the screen's pixel density.
   ───────────────────────────────────────────────────────────── */

import { useCallback, useEffect, useRef } from 'react';
import { shade } from '@/components/read/readShade';
import { carryCurve } from '@/lib/feedStream/motion';
import { ms } from '@/lib/tempo';
import { afterDrawn, holdsFlights } from '@/lib/webkitMotion';
import { LEDGER_DEAL_MS } from '@/components/motion/Ledger';

export const TWEEN_MS = ms(500);

export type Palette = {
  fg: [number, number, number];
  panel: string;
  raised: string;
  accent: string;
  accentText: string;
  // the Top % ramp (index 1–100), resolved to plain colours
  ramp: string[];
  font: string;
};

let cached: { key: string; palette: Palette } | null = null;

let pixel: CanvasRenderingContext2D | null = null;

/* any css colour (a token, a color-mix) as a plain rgba(), read back through a one-pixel canvas */
function resolveColor(probe: HTMLElement, value: string) {
  probe.style.color = '';
  probe.style.color = value;
  const computed = getComputedStyle(probe).color || value;
  if (!pixel) pixel = document.createElement('canvas').getContext('2d', { willReadFrequently: true });
  if (!pixel) return computed;
  pixel.clearRect(0, 0, 1, 1);
  pixel.fillStyle = '#000';
  pixel.fillStyle = computed;
  pixel.fillRect(0, 0, 1, 1);
  const [r, g, b, a] = pixel.getImageData(0, 0, 1, 1).data;
  return `rgba(${r}, ${g}, ${b}, ${(a / 255).toFixed(3)})`;
}

/* the theme's colours as plain values a canvas can use, read from where the chart sits */
export function paletteFor(el: HTMLElement): Palette {
  const root = document.documentElement;
  const key = root.classList.contains('dark') ? 'dark' : 'light';
  if (cached && cached.key === key) return cached.palette;
  const style = getComputedStyle(el);
  const fgRgb = (style.getPropertyValue('--fm-fg-rgb') || '15 23 42').trim().split(/[\s,]+/).map(Number) as [number, number, number];
  const probe = document.createElement('span');
  probe.style.position = 'absolute';
  probe.style.visibility = 'hidden';
  (el.parentElement || document.body).appendChild(probe);
  const ramp = Array.from({ length: 101 }, (_, value) => resolveColor(probe, shade(Math.max(1, value)).c));
  const palette: Palette = {
    fg: fgRgb.length === 3 && fgRgb.every(Number.isFinite) ? fgRgb : [15, 23, 42],
    panel: resolveColor(probe, 'var(--ld-panel)'),
    raised: resolveColor(probe, 'var(--ld-raised)'),
    accent: resolveColor(probe, 'var(--fm-accent)'),
    accentText: resolveColor(probe, 'var(--fm-accent-text)'),
    ramp,
    font: style.fontFamily || 'system-ui, sans-serif',
  };
  probe.remove();
  cached = { key, palette };
  return palette;
}

export const ink = (palette: Palette, alpha: number) => `rgba(${palette.fg[0]}, ${palette.fg[1]}, ${palette.fg[2]}, ${alpha})`;

/* a chart's words and numbers: never fainter than the small type's floor (leadKit DIM: 58% ink on the light page, 48%
   on the dark one), and never set under 10px (labelFont) */
export const labelInk = (palette: Palette, alpha: number) => ink(palette, Math.max(alpha, palette.fg[0] > 128 ? 0.48 : 0.58));
export const labelFont = (palette: Palette, size: number) => `800 ${Math.max(10, size)}px ${palette.font}`;

export function rampColor(palette: Palette, topPercent: number) {
  return palette.ramp[Math.max(1, Math.min(100, Math.round(topPercent)))];
}

/* mix two css rgb()/rgba() colours (as resolved by the browser) */
export function mixColor(a: string, b: string, t: number) {
  const pa = parseRgb(a);
  const pb = parseRgb(b);
  if (!pa || !pb) return t < 0.5 ? a : b;
  const c = pa.map((v, i) => v + (pb[i] - v) * t);
  return `rgba(${Math.round(c[0])}, ${Math.round(c[1])}, ${Math.round(c[2])}, ${c[3].toFixed(3)})`;
}

function parseRgb(value: string): [number, number, number, number] | null {
  const m = /rgba?\(([^)]+)\)/.exec(value);
  if (!m) return null;
  const parts = m[1].split(/[\s,/]+/).filter(Boolean).map(Number);
  if (parts.length < 3 || parts.some((n) => !Number.isFinite(n))) return null;
  return [parts[0], parts[1], parts[2], parts.length > 3 ? parts[3] : 1];
}

export const lerp = (a: number, b: number, t: number) => a + (b - a) * t;

export function roundRect(ctx: CanvasRenderingContext2D, x: number, y: number, w: number, h: number, r: number) {
  const radius = Math.max(0, Math.min(r, w / 2, h / 2));
  ctx.beginPath();
  ctx.moveTo(x + radius, y);
  ctx.arcTo(x + w, y, x + w, y + h, radius);
  ctx.arcTo(x + w, y + h, x, y + h, radius);
  ctx.arcTo(x, y + h, x, y, radius);
  ctx.arcTo(x, y, x + w, y, radius);
  ctx.closePath();
}

const reduced = () => typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

export type Frame = { ctx: CanvasRenderingContext2D; width: number; height: number; palette: Palette };

/* A canvas that draws `draw(frame, from, to, t)`: `to` is the scene to show, `from` the one it is leaving (null on
   the first draw), t the carry curve's progress 0..1. A new scene (by identity) tweens from wherever the last one
   was, when the chart is on screen. Off screen there is no move to see: the new scene is drawn as it is once the chart
   comes within a screen of view (or the page has been idle a while), not in the middle of the switch. `redraw()`
   draws again at once (a pick). Sizes, density and theme are handled here. */
export function useCanvasScene<S>(scene: S, draw: (frame: Frame, from: S | null, to: S, t: number) => void, options: { animate?: boolean } = {}) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  const state = useRef<{ from: S | null; to: S; start: number; raf: number; size: { w: number; h: number; dpr: number }; stale: boolean; later: number }>({ from: null, to: scene, start: 0, raf: 0, size: { w: 0, h: 0, dpr: 1 }, stale: false, later: 0 });
  const drawRef = useRef(draw);
  const seen = useRef(false);
  const near = useRef(false);
  const animate = options.animate !== false;

  useEffect(() => {
    drawRef.current = draw;
  });

  const paint = useCallback((t: number) => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const { size } = state.current;
    if (!size.w || !size.h) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    ctx.setTransform(size.dpr, 0, 0, size.dpr, 0, 0);
    ctx.clearRect(0, 0, size.w, size.h);
    drawRef.current({ ctx, width: size.w, height: size.h, palette: paletteFor(canvas) }, state.current.from, state.current.to, t);
  }, []);

  const redraw = useCallback(() => {
    const s = state.current;
    if (s.raf) return; // a tween in flight draws the latest pick itself
    paint(1);
  }, [paint]);

  // size and density
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return undefined;
    const fit = () => {
      const rect = canvas.getBoundingClientRect();
      const dpr = Math.min(3, window.devicePixelRatio || 1);
      const w = Math.round(rect.width);
      const h = Math.round(rect.height);
      if (!w || !h) return;
      const s = state.current;
      if (s.size.w === w && s.size.h === h && s.size.dpr === dpr) return;
      s.size = { w, h, dpr };
      canvas.width = Math.round(w * dpr);
      canvas.height = Math.round(h * dpr);
      paint(s.raf ? Math.min(1, (performance.now() - s.start) / TWEEN_MS) : 1);
    };
    fit();
    const observer = new ResizeObserver(fit);
    observer.observe(canvas);
    // a theme switch (the root's class) redraws with the new colours; other writes to the root are not ours to answer
    const themeOf = () => (document.documentElement.classList.contains('dark') ? 'dark' : 'light');
    let theme = themeOf();
    const themeWatch = new MutationObserver(() => {
      const next = themeOf();
      if (next === theme) return;
      theme = next;
      paint(1);
    });
    themeWatch.observe(document.documentElement, { attributes: true, attributeFilter: ['class'] });
    // whether any of it is on screen (a tween only plays where it can be seen), and whether it is within a screen of
    // it (a scene that changed while it was away is drawn then)
    const sight = new IntersectionObserver((entries) => {
      seen.current = entries.some((entry) => entry.isIntersecting);
    });
    sight.observe(canvas);
    const approach = new IntersectionObserver((entries) => {
      near.current = entries.some((entry) => entry.isIntersecting);
      const s = state.current;
      if (near.current && s.stale) {
        s.stale = false;
        paint(1);
      }
    }, { rootMargin: '100% 0px' });
    approach.observe(canvas);
    const held = state.current;
    return () => {
      observer.disconnect();
      themeWatch.disconnect();
      sight.disconnect();
      approach.disconnect();
      window.clearTimeout(held.later);
    };
  }, [paint]);

  // a new scene: tween to it
  useEffect(() => {
    const s = state.current;
    if (s.to === scene && s.start) return undefined;
    const first = !s.start;
    s.from = first || !animate || !seen.current || reduced() ? null : s.to;
    s.to = scene;
    s.start = performance.now();
    if (s.raf) cancelAnimationFrame(s.raf);
    s.raf = 0;
    if (!s.from) {
      // away from the screen: drawn when it comes near, or once the page has been idle a while
      if (!first && !seen.current && !near.current) {
        s.stale = true;
        window.clearTimeout(s.later);
        // once the switch's motion on screen has played (the Feederboard's deal is the longest)
        s.later = window.setTimeout(() => {
          if (!s.stale) return;
          s.stale = false;
          paint(1);
        }, LEDGER_DEAL_MS + 100);
        return undefined;
      }
      s.stale = false;
      paint(1);
      return undefined;
    }
    // the tween's clock starts at its first drawn frame, not when it was asked for (a switch's frame can be slow to
    // arrive: the tween would arrive half done); on WebKit, once that frame is on screen (lib/webkitMotion)
    let clock = 0;
    const step = (now: number) => {
      if (!clock) clock = now;
      s.start = clock;
      const progress = Math.min(1, (now - clock) / TWEEN_MS);
      paint(carryCurve(progress));
      if (progress < 1) s.raf = requestAnimationFrame(step);
      else {
        s.raf = 0;
        s.from = null;
      }
    };
    let held = () => {};
    if (holdsFlights()) {
      s.raf = -1;
      held = afterDrawn(() => {
        s.raf = requestAnimationFrame(step);
      });
    } else s.raf = requestAnimationFrame(step);
    return () => {
      held();
      if (s.raf > 0) cancelAnimationFrame(s.raf);
      s.raf = 0;
    };
  }, [animate, paint, scene]);

  return { canvasRef, redraw };
}

/* a series resampled to a fixed count, so any curve can bend into any other */
export function resample(values: Array<number | null>, count = 96) {
  const known: Array<[number, number]> = [];
  values.forEach((value, index) => {
    if (value != null) known.push([index, value]);
  });
  if (!known.length) return Array(count).fill(1) as number[];
  const out: number[] = [];
  let k = 0;
  for (let sample = 0; sample < count; sample += 1) {
    const x = (sample / (count - 1)) * Math.max(1, values.length - 1);
    while (k < known.length - 2 && known[k + 1][0] < x) k += 1;
    const [x0, y0] = known[k];
    const [x1, y1] = known[Math.min(known.length - 1, k + 1)];
    if (x <= x0 || x1 === x0) out.push(x <= x0 ? y0 : y1);
    else if (x >= x1) out.push(y1);
    else out.push(y0 + ((y1 - y0) * (x - x0)) / (x1 - x0));
  }
  return out;
}

/* a smooth line through points that never overshoots them (monotone cubic) */
export function monotonePath(ctx: CanvasRenderingContext2D, points: Array<[number, number]>, move = true) {
  const n = points.length;
  if (!n) return;
  if (move) ctx.moveTo(points[0][0], points[0][1]);
  else ctx.lineTo(points[0][0], points[0][1]);
  if (n < 3) {
    for (let i = 1; i < n; i += 1) ctx.lineTo(points[i][0], points[i][1]);
    return;
  }
  const slope: number[] = [];
  for (let i = 0; i < n - 1; i += 1) slope.push((points[i + 1][1] - points[i][1]) / ((points[i + 1][0] - points[i][0]) || 1));
  const tangent = points.map((_, i) => (i === 0 ? slope[0] : i === n - 1 ? slope[n - 2] : slope[i - 1] * slope[i] <= 0 ? 0 : (slope[i - 1] + slope[i]) / 2));
  for (let i = 0; i < n - 1; i += 1) {
    const [x0, y0] = points[i];
    const [x1, y1] = points[i + 1];
    const h = (x1 - x0) / 3;
    ctx.bezierCurveTo(x0 + h, y0 + tangent[i] * h, x1 - h, y1 - tangent[i + 1] * h, x1, y1);
  }
}
