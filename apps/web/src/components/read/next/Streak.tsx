'use client';

/* The Ask box's streak: one line of the crimson travelling the box's own outline.

   It is drawn on the outline itself, so its length is a real length along the edge. A turning wedge of light could
   not do that on a wide box: it flooded the short ends and thinned to a sliver along the top.

   Pace: the head has a speed of its own that never settles. A quick surge and a long ease down, with two slower
   swells laid over it, none in step with the lap, so no two laps run alike. The tail follows the head on a soft lag:
   when the head surges the streak stretches out behind it, and when the head eases the tail catches up and the streak
   gathers, and by more than the pace alone would give: about a fourteenth of the outline at its shortest, well over a
   third at its longest.

   Colour: one continuous gradient along its length, oxblood feathering in at the tail, the crimson in the body, a
   pale lit tip near the head, feathering out in front. It is painted, not stacked: the outline is eight pieces (four
   sides, four corners) and each piece the streak lies on is stroked with a true gradient along it, so there is no
   step anywhere in it. One small canvas repaints per frame, only while the box is on screen. */
import { useEffect, useRef, type CSSProperties } from 'react';

export type StreakPace = 'rest' | 'focus' | 'busy';

const LAP = 9.2; // seconds round the box at its mean pace
const TRAIL = 0.7; // seconds the tail lags the head: the stretch
const BODY = 0.045; // the streak's length at a standstill, as a share of the outline
// the stretch is not in step with the lag: a surge draws it out far longer, an ease gathers it far shorter
const REACH = 1.7;
const GAIN = 1.9;
const LONGEST = 0.5; // never more of the outline than this
const SAMPLES = 32; // gradient stops along the streak (the paint between them is the browser's own blend)

/* the head's speed against its mean (0 = mean): a surge with a long ease down, and two slower swells */
function drift(t: number) {
  const x = (Math.PI * 2 * t) / 5.2;
  return 0.58 * Math.sin(x + 0.7 * Math.sin(x)) + 0.24 * Math.sin((Math.PI * 2 * t) / 3.1 + 1.7) + 0.12 * Math.sin((Math.PI * 2 * t) / 11.3 + 4);
}

/* the streak along its length, u from its tail (0) to its front (1) */
type RGB = [number, number, number];
const soft = (x: number) => x * x * (3 - 2 * x);
const PEAK = 0.6;
// how bright: a long soft rise from the tail to the lit tip, a shorter soft fall in front of it
const alphaAt = (u: number) => (u <= 0 || u >= 1 ? 0 : u < PEAK ? Math.pow(soft(u / PEAK), 1.25) : Math.pow(soft((1 - u) / (1 - PEAK)), 0.85));
// which colour: oxblood, deep crimson, the crimson, the pale tip, the crimson again (indices into the palette)
const TINT: [number, number][] = [
  [0, 0],
  [0.3, 1],
  [0.48, 2],
  [0.6, 3],
  [0.74, 2],
  [1, 2],
];
function colourAt(u: number, pal: RGB[]): RGB {
  const v = Math.max(0, Math.min(1, u));
  for (let i = 1; i < TINT.length; i += 1) {
    if (v <= TINT[i][0]) {
      const k = (v - TINT[i - 1][0]) / (TINT[i][0] - TINT[i - 1][0]);
      const a = pal[TINT[i - 1][1]];
      const b = pal[TINT[i][1]];
      return [a[0] + (b[0] - a[0]) * k, a[1] + (b[1] - a[1]) * k, a[2] + (b[2] - a[2]) * k];
    }
  }
  return pal[2];
}
const parse = (s: string, fallback: RGB): RGB => {
  const t = s.trim();
  const hex = /^#([0-9a-f]{6})$/i.exec(t);
  if (hex) return [parseInt(hex[1].slice(0, 2), 16), parseInt(hex[1].slice(2, 4), 16), parseInt(hex[1].slice(4, 6), 16)];
  const n = t.match(/[\d.]+/g);
  return n && n.length >= 3 ? [Number(n[0]), Number(n[1]), Number(n[2])] : fallback;
};

/* the outline as pieces, clockwise from the top left: a side is a line, a corner a quarter turn. a is where a piece
   starts along the outline */
type Piece = { a: number; len: number; x0: number; y0: number; x1: number; y1: number; arc: { cx: number; cy: number; r: number; from: number } | null };
function outline(w: number, h: number, edge: number, radius: number): { pieces: Piece[]; P: number } {
  const i = edge / 2;
  const r = Math.max(0, Math.min(radius, w / 2, h / 2) - i);
  const q = (Math.PI * r) / 2;
  const L = i + r;
  const R = w - i - r;
  const T = i + r;
  const B = h - i - r;
  const pieces: Piece[] = [];
  let a = 0;
  const line = (x0: number, y0: number, x1: number, y1: number) => {
    const len = Math.hypot(x1 - x0, y1 - y0);
    if (len > 0.01) pieces.push({ a, len, x0, y0, x1, y1, arc: null });
    a += len;
  };
  const turn = (cx: number, cy: number, from: number) => {
    if (q > 0.01) pieces.push({ a, len: q, x0: cx + r * Math.cos(from), y0: cy + r * Math.sin(from), x1: cx + r * Math.cos(from + Math.PI / 2), y1: cy + r * Math.sin(from + Math.PI / 2), arc: { cx, cy, r, from } });
    a += q;
  };
  line(L, i, R, i);
  turn(R, T, -Math.PI / 2);
  line(w - i, T, w - i, B);
  turn(R, B, 0);
  line(R, h - i, L, h - i);
  turn(L, B, Math.PI / 2);
  line(i, B, i, T);
  turn(L, T, Math.PI);
  return { pieces, P: a };
}

export default function Streak({ radius }: { radius: number; pace?: StreakPace; reduce?: boolean }) {
  const ref = useRef<HTMLDivElement | null>(null);
  // (a little heavier than a hairline: the box's face covers everything inside it)
  const edge = radius < 28 ? 3.5 : 4;
  useEffect(() => {
    const box = ref.current;
    const canvas = box?.querySelector('canvas');
    const ctx = canvas?.getContext('2d');
    if (!box || !canvas || !ctx) return undefined;
    const still = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    let pieces: Piece[] = [];
    let P = 0; // the outline's length
    let W = 0;
    let H = 0;
    let dpr = 1;
    let pal: RGB[] = [
      [121, 27, 38],
      [171, 3, 44],
      [218, 46, 70],
      [251, 161, 162],
    ];
    let head = 0;
    let tail = 0;
    let t = 0;
    let last = 0;
    let raf = 0;
    let seen = true;

    const tones = () => {
      const cs = getComputedStyle(box);
      pal = [parse(cs.getPropertyValue('--fm-accent-deeper'), pal[0]), parse(cs.getPropertyValue('--fm-accent-deep'), pal[1]), parse(cs.getPropertyValue('--fm-accent-rgb'), pal[2]), parse(cs.getPropertyValue('--rn-streak-tip'), pal[3])];
    };

    const draw = () => {
      if (!P) return;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      ctx.clearRect(0, 0, W, H);
      const mean = (P / LAP) * TRAIL;
      const lag = Math.max(0, head - P * BODY - tail);
      const len = Math.min(P * BODY + mean * Math.pow(lag / mean, REACH) * GAIN, P * LONGEST);
      const from = (((head - len) % P) + P) % P;
      // (a touch wider than the edge: the box's face covers the inside, the box's own corner clips the outside)
      // the light the streak casts under the box follows its bright part: where it is along the outline, and how much it
      // lies along the foot (strongest there, faintest along the top)
      const host = box.parentElement;
      if (host) {
        const at = ((((head - len * 0.4) % P) + P) % P);
        let cx = W / 2;
        let cy = 0;
        for (let k = 0; k < pieces.length; k += 1) {
          const pc = pieces[k];
          if (at < pc.a || at > pc.a + pc.len) continue;
          const f = (at - pc.a) / pc.len;
          if (pc.arc) {
            const ang = pc.arc.from + (f * Math.PI) / 2;
            cx = pc.arc.cx + pc.arc.r * Math.cos(ang);
            cy = pc.arc.cy + pc.arc.r * Math.sin(ang);
          } else {
            cx = pc.x0 + (pc.x1 - pc.x0) * f;
            cy = pc.y0 + (pc.y1 - pc.y0) * f;
          }
          break;
        }
        const foot = H ? cy / H : 0;
        // (written straight onto the light itself each frame: a pseudo-element fed by custom properties is not
        // repainted per frame in Safari)
        const light = host.querySelector<HTMLElement>('.rn-ask__castl');
        if (light) {
          // (subtle: only the streak's run along the foot casts, it rises and falls with how near the foot it is, and it
          // stays well inside the box's width, never past its sides)
          const w = Math.round(Math.min(W * 0.4, 120 + len * 0.45));
          const x = Math.max(w / 2 + 28, Math.min(W - w / 2 - 28, cx));
          const near = Math.max(0, (foot - 0.6) / 0.4);
          light.style.width = `${w}px`;
          light.style.transform = `translate3d(${(x - w / 2).toFixed(1)}px, 0, 0)`;
          light.style.opacity = (0.45 * near * near).toFixed(3);
        }
      }
      ctx.lineWidth = edge + 2;
      ctx.lineCap = 'butt';
      for (let k = 0; k < pieces.length; k += 1) {
        const pc = pieces[k];
        // where the streak starts, on the lap that reaches this piece
        let T = from;
        if (T + len <= pc.a) T += P;
        else if (T >= pc.a + pc.len) T -= P;
        if (T + len <= pc.a || T >= pc.a + pc.len) continue;
        const g = ctx.createLinearGradient(pc.x0, pc.y0, pc.x1, pc.y1);
        const stop = (s: number) => {
          const u = (pc.a + s * pc.len - T) / len;
          const c = colourAt(u, pal);
          // (a corner's paint runs along its chord: where a point of the turn falls on it)
          const at = pc.arc ? (1 - Math.cos((s * Math.PI) / 2) + Math.sin((s * Math.PI) / 2)) / 2 : s;
          g.addColorStop(Math.max(0, Math.min(1, at)), `rgba(${Math.round(c[0])},${Math.round(c[1])},${Math.round(c[2])},${alphaAt(u).toFixed(3)})`);
        };
        stop(0);
        for (let j = 0; j <= SAMPLES; j += 1) {
          const s = (T + (j / SAMPLES) * len - pc.a) / pc.len;
          if (s > 0 && s < 1) stop(s);
        }
        stop(1);
        ctx.beginPath();
        if (pc.arc) ctx.arc(pc.arc.cx, pc.arc.cy, pc.arc.r, pc.arc.from, pc.arc.from + Math.PI / 2);
        else {
          ctx.moveTo(pc.x0, pc.y0);
          ctx.lineTo(pc.x1, pc.y1);
        }
        ctx.strokeStyle = g;
        ctx.stroke();
      }
    };

    /* the outline, fitted to the box (again whenever the box changes size: the streak keeps its place on it) */
    const fit = () => {
      const w = box.clientWidth;
      const h = box.clientHeight;
      if (!w || !h) return;
      dpr = Math.min(3, window.devicePixelRatio || 1);
      if (canvas.width !== Math.round(w * dpr) || canvas.height !== Math.round(h * dpr)) {
        canvas.width = Math.round(w * dpr);
        canvas.height = Math.round(h * dpr);
      }
      W = w;
      H = h;
      const was = P;
      const o = outline(w, h, edge, radius);
      pieces = o.pieces;
      P = o.P;
      tones();
      if (was) {
        head *= P / was;
        tail *= P / was;
      } else {
        head = P * 0.12;
        tail = head - P * BODY - (P / LAP) * TRAIL;
      }
      draw();
    };

    const frame = (now: number) => {
      raf = requestAnimationFrame(frame);
      const dt = Math.min(0.064, (now - last) / 1000);
      last = now;
      if (!P || dt <= 0) return;
      t += dt;
      head += (P / LAP) * Math.max(0.06, 1 + drift(t)) * dt;
      tail += (head - P * BODY - tail) * (1 - Math.exp(-dt / TRAIL));
      if (head > P * 2) {
        head -= P;
        tail -= P;
      }
      draw();
    };
    const run = () => {
      cancelAnimationFrame(raf);
      if (still || !seen) return;
      last = performance.now();
      raf = requestAnimationFrame(frame);
    };

    fit();
    run();
    const ro = new ResizeObserver(fit);
    ro.observe(box);
    const io = new IntersectionObserver(([en]) => {
      seen = en.isIntersecting;
      run();
    });
    io.observe(box);
    // the theme turning (light, dark) changes the tones
    const mo = new MutationObserver(() => {
      tones();
      draw();
    });
    mo.observe(document.documentElement, { attributes: true, attributeFilter: ['class', 'data-theme'] });
    return () => {
      cancelAnimationFrame(raf);
      ro.disconnect();
      io.disconnect();
      mo.disconnect();
    };
  }, [radius, edge]);
  return (
    <div className="rn-sweep" ref={ref} aria-hidden="true" style={{ '--edge': `${edge}px` } as CSSProperties}>
      <canvas className="rn-sweep__svg" />
    </div>
  );
}
