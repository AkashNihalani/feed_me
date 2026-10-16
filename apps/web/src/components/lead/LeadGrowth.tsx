'use client';

/* Follow-through — which posts turned into followers: the audience over the window, read day by day. The stage is one canvas: the followers curve, each day's
   gain or loss as a solid bar (the three biggest rises in rose, days without a reading hatched), a dot for the
   posts that went out, the numbered moments. Drag across it (or hover) and the readout reads that day, the strip
   under it shows the day's posts. Beside it, the moments: the window's three biggest rises (numbered left to
   right) and its worst day, each with the strongest post that went out that day or the two before ("after", never
   "because").

   Motion: a new window or scope tweens the canvas on the Feed's carry curve (the curve bends into its new shape, the
   bars grow and fall); the readout's numbers roll; the strip's posts change in place. */

import { CSSProperties, memo, useCallback, useEffect, useMemo, useRef } from 'react';
import { Users } from 'lucide-react';
import SlotText from '@/components/SlotText';
import { cn } from '@/lib/utils';
import type { LeadActivity } from '@/app/api/lead/activity/route';
import { ink, lerp, monotonePath, resample, roundRect, useCanvasScene, type Frame, type Palette } from './leadCanvas';
import { CrossfadeImage, Delta, Eyebrow, PostTile, Swap, compact, dayLabel, shortDay, signed, timeLabel, topLabel, useHeldNear, usePick, useReveal, useScrub } from './leadKit';
import { growthFor, type GrowthDay, type GrowthModel, type LeadFeed, type Moment } from './leadModels';

const INK = [0.86, 0.52, 0.3, 0.18];
const SAMPLES = 96;
const PAD = 14;

type Props = { feeds: LeadFeed[]; activity: LeadActivity | null; feedId: string; feeder: string; days: number };

function LeadGrowth(props: Props) {
  const live = useMemo(() => ({ feeds: props.feeds, activity: props.activity, feedId: props.feedId, feeder: props.feeder, days: props.days }), [props.feeds, props.activity, props.feedId, props.feeder, props.days]);
  const [nearRef, scope] = useHeldNear(live);
  const model = useMemo(() => growthFor(scope), [scope]);
  const dealKey = `${scope.feedId}|${scope.feeder}|${scope.days}`;
  const [revealRef, reveal] = useReveal<HTMLDivElement>();
  const { hover, pinned, setHover, setPinned } = usePick<number>(dealKey);
  const count = model.days.length;
  const index = Math.min(count - 1, hover ?? pinned ?? model.lastRead);
  const day = model.days[index];
  const picked = hover != null || pinned != null;
  const riseKeys = useMemo(() => new Set(model.moments.filter((moment) => moment.rise).map((moment) => moment.key)), [model.moments]);

  return (
    <section ref={nearRef} aria-labelledby="lead-growth-title" className="lead-band relative isolate overflow-hidden border-t border-fg/[0.09] bg-(--ld-panel) px-4 py-5 sm:px-6 lg:px-7 lg:py-6">
      <div ref={revealRef} data-reveal={reveal}>
        <GrowthHead model={model} days={scope.days} />

        <div className="mt-4 grid gap-4 lg:grid-cols-[minmax(0,1fr)_minmax(280px,0.4fr)] lg:gap-6">
          <div className="lead-rise min-w-0" style={{ '--rise': 2 } as CSSProperties}>
            <div className="overflow-hidden rounded-[22px] border border-fg/[0.08] bg-(--ld-raised)">
              <Readout day={day} picked={picked} rise={riseKeys.has(day.key)} />
              <GrowthStage model={model} index={index} picked={picked} onHover={setHover} onPin={setPinned} />
            </div>
            <DayPosts day={day} />
          </div>
          <div className="lead-rise min-w-0" style={{ '--rise': 3 } as CSSProperties}>
            <Moments model={model} activeIndex={picked ? index : null} onPick={setPinned} />
          </div>
        </div>
      </div>
    </section>
  );
}

export default memo(LeadGrowth);

/* how this window's gain sits against the one before, said plainly ("up from +12.4K the 30D before") */
function againstBefore(net: number | null, prior: number | null, days: number) {
  if (net == null || prior == null) return null;
  const word = Math.round(net) === Math.round(prior) ? 'level with' : net > prior ? 'up from' : 'down from';
  return `${word} ${signed(prior)} the ${days}D before`;
}

const GrowthHead = memo(function GrowthHead({ model, days }: { model: GrowthModel; days: number }) {
  const priorUp = model.net != null && model.priorNet != null ? model.net >= model.priorNet : null;
  const drivers = model.drivers.filter((driver) => driver.net > 0);
  const shown = drivers.slice(0, 4);
  const driverTotal = drivers.reduce((sum, driver) => sum + driver.net, 0);
  // who brought most of it, when one did
  const mostly = drivers.length > 1 && driverTotal > 0 && drivers[0].net / driverTotal >= 0.5 ? drivers[0].name : null;
  const before = againstBefore(model.net, model.priorNet, days);
  return (
    <div className="lead-rise" style={{ '--rise': 0 } as CSSProperties}>
      <div className="flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between sm:gap-6">
        <div className="min-w-0">
          <Eyebrow icon={<Users className="h-4 w-4" />}>Follow-through · {days}D</Eyebrow>
          <h2 id="lead-growth-title" className="mt-2 text-[20px] font-black leading-none tracking-[-0.035em] text-fg sm:text-[22px]">Which posts turned into followers.</h2>
        </div>
        <div className="sm:text-right">
          <SlotText value={signed(model.net)} className="fm-depth-title text-[48px] font-black tabular-nums leading-[0.8] tracking-[-0.055em] text-fg lg:text-[56px] laptop:text-[44px]" />
          {/* two lines on a phone (both always fit), one from sm up; the same height whatever they say */}
          <div className="mt-2 flex h-[34px] flex-col items-start justify-center gap-1.5 overflow-x-clip whitespace-nowrap sm:h-[16px] sm:flex-row sm:items-center sm:justify-end sm:gap-3">
            <span className="text-[9px] font-black uppercase tracking-[0.16em] text-fg/44 light:text-fg/54"><Swap value={mostly ? `followers · mostly ${mostly}` : 'followers gained'} /></span>
            {before ? <Delta up={priorUp}><Swap value={before} /></Delta> : null}
          </div>
        </div>
      </div>
      {/* who it came from: each one's share of the gain (always one line) */}
      <div className="mt-4 h-[30px]">
        <div className="relative h-[6px] w-full overflow-hidden rounded-full bg-fg/[0.06]">
          {shown.map((driver, order) => {
            const left = drivers.slice(0, order).reduce((sum, one) => sum + one.net, 0) / Math.max(1, driverTotal);
            return (
              <span
                key={driver.id}
                className="lead-glide absolute inset-y-0 left-0 w-full origin-left"
                style={{ transform: `translateX(${left * 100}%) scaleX(${driver.net / Math.max(1, driverTotal)})`, background: `rgb(var(--fm-fg-rgb) / ${INK[order]})` }}
              />
            );
          })}
        </div>
        <div className="mt-2 flex h-[16px] gap-x-5 overflow-x-clip whitespace-nowrap">
          {shown.map((driver, order) => (
            <span key={driver.id} className="flex shrink-0 items-center gap-1.5 text-[10px] font-black text-fg/76">
              <span className="h-2 w-2 rounded-full" style={{ background: `rgb(var(--fm-fg-rgb) / ${INK[order]})` }} />
              {driver.name}
              <span className="tabular-nums text-fg/44">{signed(driver.net)}</span>
            </span>
          ))}
          {drivers.length > shown.length ? <span className="shrink-0 text-[10px] font-black text-fg/40">+{drivers.length - shown.length} more</span> : null}
        </div>
      </div>
    </div>
  );
});

function Readout({ day, picked, rise }: { day: GrowthDay; picked: boolean; rise: boolean }) {
  return (
    <div className="flex h-[66px] items-start justify-between gap-4 px-4 pt-3.5 sm:px-5">
      <div className="min-w-0">
        <div className="h-[13px] overflow-x-clip whitespace-nowrap text-[8px] font-black uppercase tracking-[0.18em] text-fg/40 light:text-fg/52">
          <Swap value={`${picked ? 'Picked' : 'Latest reading'} · ${dayLabel(day.key)}`} />
        </div>
        <div className="mt-1.5 flex items-baseline gap-2 whitespace-nowrap">
          <SlotText value={day.followers == null ? '—' : compact(day.followers, 2)} className="fm-depth-title text-[28px] font-black tabular-nums leading-none tracking-[-0.045em] text-fg sm:text-[32px]" />
          <span className="text-[9px] font-black uppercase tracking-[0.14em] text-fg/40 light:text-fg/52">followers</span>
        </div>
      </div>
      <div className="shrink-0 text-right">
        <div className="text-[8px] font-black uppercase tracking-[0.18em] text-fg/40 light:text-fg/52">{day.estimated ? 'Estimated' : 'That day'}</div>
        <SlotText
          value={signed(day.delta)}
          className={cn('fm-depth-title mt-1.5 block text-[28px] font-black tabular-nums leading-none tracking-[-0.045em] sm:text-[32px]', rise ? 'text-[var(--fm-accent-text)]' : 'text-fg')}
        />
      </div>
    </div>
  );
}

/* ── the stage: one canvas ─────────────────────────────────────────────── */

type Scene = {
  n: number;
  // the curve, resampled, as fractions of the curve's height (0 top), from day `start` to day `end`
  curve: number[];
  start: number;
  end: number;
  // each day: its reading (fraction, or null), its move (-1..1 of the window's biggest), how to draw it
  points: Array<number | null>;
  moves: number[];
  kinds: Array<'gain' | 'rise' | 'loss' | 'estimated' | 'none'>;
  posts: number[];
  marks: Array<{ at: number; y: number; label: string; rise: boolean; big: boolean }>;
  top: string;
  bottom: string;
  // the scale's top label goes on the side the curve leaves free at the top (a rising curve ends up there, on its
  // last reading), the bottom label on the other
  topLeft: boolean;
  axis: Array<{ at: number; label: string }>;
};

function scaleFor(model: GrowthModel) {
  const values = model.days.map((day) => day.followers).filter((value): value is number => value != null);
  if (model.first != null) values.push(model.first);
  if (!values.length) return { min: 0, max: 1 };
  const min = Math.min(...values);
  const max = Math.max(...values);
  const pad = Math.max((max - min) * 0.12, max * 0.0012, 1);
  return { min: min - pad, max: max + pad * 0.6 };
}

function sceneOf(model: GrowthModel): Scene {
  const n = model.days.length;
  const { min, max } = scaleFor(model);
  const y = (value: number | null) => (value == null ? null : 1 - (value - min) / (max - min));
  const points = model.days.map((one, at) => (at <= model.lastRead ? y(one.followers) : null));
  const first = points.findIndex((value) => value != null);
  const known = first < 0 ? [] : points.slice(first, model.lastRead + 1);
  const deltas = model.days.map((one) => one.delta ?? 0);
  const gainMax = Math.max(1, ...deltas);
  const lossMax = Math.max(1, ...deltas.map((value) => -value));
  const rises = new Set(model.moments.filter((moment) => moment.rise).map((moment) => moment.index));
  const biggest = Math.max(0, ...model.moments.filter((moment) => moment.rise).map((moment) => moment.delta));
  return {
    n,
    curve: known.length ? resample(known, SAMPLES) : Array(SAMPLES).fill(1),
    start: first < 0 ? 0 : first,
    end: first < 0 ? 0 : model.lastRead,
    points,
    moves: deltas.map((delta) => (delta > 0 ? delta / gainMax : delta < 0 ? delta / lossMax : 0)),
    kinds: model.days.map((one, at) => (one.estimated ? 'estimated' : one.delta == null ? 'none' : (one.delta ?? 0) < 0 ? 'loss' : rises.has(at) ? 'rise' : 'gain')),
    posts: model.days.map((one) => one.shots.length),
    marks: model.moments
      .map((moment, order) => ({ at: moment.index, y: points[moment.index] ?? 1, label: moment.rise ? String(order + 1) : '↓', rise: moment.rise, big: moment.rise && moment.delta === biggest }))
      .filter((mark) => points[mark.at] != null),
    top: compact(max, 2),
    bottom: compact(min, 2),
    topLeft: known.length < 2 || (known[0] ?? 1) >= (known[known.length - 1] ?? 1),
    axis: [0, 1, 2, 3].map((step) => {
      const at = Math.round((step * (n - 1)) / 3);
      return { at, label: at === n - 1 ? 'TODAY' : shortDay(model.days[at].key).toUpperCase() };
    }),
  };
}

let hatchFor: { key: string; pattern: CanvasPattern | null } | null = null;

function hatch(ctx: CanvasRenderingContext2D, palette: Palette) {
  const key = palette.fg.join(',');
  if (hatchFor && hatchFor.key === key) return hatchFor.pattern;
  const tile = document.createElement('canvas');
  tile.width = 8;
  tile.height = 8;
  const t = tile.getContext('2d');
  if (t) {
    t.strokeStyle = ink(palette, 0.42);
    t.lineWidth = 2;
    t.beginPath();
    t.moveTo(-2, 10);
    t.lineTo(10, -2);
    t.moveTo(-6, 6);
    t.lineTo(6, -6);
    t.moveTo(2, 14);
    t.lineTo(14, 2);
    t.stroke();
  }
  hatchFor = { key, pattern: ctx.createPattern(tile, 'repeat') };
  return hatchFor.pattern;
}

/* a bar with softened corners where it is wide enough to show them */
function bar(path: Path2D, x: number, y: number, w: number, h: number) {
  if (w >= 4 && h >= 4 && 'roundRect' in path) path.roundRect(x, y, w, h, 2);
  else path.rect(x, y, w, h);
}

function layoutOf(width: number, height: number, n: number) {
  const axisH = 18;
  const dotsH = 14;
  const barsH = Math.round(height * 0.24);
  const curveTop = 10;
  const curveH = height - axisH - dotsH - barsH - curveTop - 12;
  const barsTop = curveTop + curveH + 8;
  const colW = (width - PAD * 2) / Math.max(1, n);
  return { axisH, dotsH, barsH, curveTop, curveH, barsTop, base: barsTop + barsH * 0.72, colW, xOf: (at: number) => PAD + (at + 0.5) * colW };
}

function drawScene({ ctx, width, height, palette }: Frame, scene: Scene, alpha: number, grow: number) {
  const { dotsH, barsH, curveTop, curveH, barsTop, base, colW, xOf } = layoutOf(width, height, scene.n);
  const font = (size: number) => `800 ${size}px ${palette.font}`;
  ctx.globalAlpha = alpha;

  // gridlines and the scale's ends
  ctx.strokeStyle = ink(palette, 0.07);
  ctx.lineWidth = 1;
  ctx.setLineDash([3, 4]);
  for (const line of [0.25, 0.5, 0.75]) {
    ctx.beginPath();
    ctx.moveTo(PAD, Math.round(curveTop + curveH * line) + 0.5);
    ctx.lineTo(width - PAD, Math.round(curveTop + curveH * line) + 0.5);
    ctx.stroke();
  }
  ctx.setLineDash([]);
  ctx.font = font(9);
  ctx.fillStyle = ink(palette, 0.4);
  ctx.textAlign = scene.topLeft ? 'left' : 'right';
  ctx.textBaseline = 'top';
  ctx.fillText(scene.top, scene.topLeft ? PAD : width - PAD, curveTop);
  ctx.textAlign = scene.topLeft ? 'right' : 'left';
  ctx.textBaseline = 'bottom';
  ctx.fillText(scene.bottom, scene.topLeft ? width - PAD : PAD, curveTop + curveH);

  // the curve and its area
  if (scene.curve.length) {
    const x0 = xOf(scene.start);
    const x1 = xOf(scene.end);
    const pts: Array<[number, number]> = scene.curve.map((value, k) => [x0 + ((x1 - x0) * k) / (SAMPLES - 1), curveTop + value * curveH]);
    const fill = ctx.createLinearGradient(0, curveTop, 0, curveTop + curveH);
    fill.addColorStop(0, ink(palette, 0.2 * grow));
    fill.addColorStop(1, ink(palette, 0));
    ctx.beginPath();
    monotonePath(ctx, pts);
    ctx.lineTo(x1, curveTop + curveH);
    ctx.lineTo(x0, curveTop + curveH);
    ctx.closePath();
    ctx.fillStyle = fill;
    ctx.fill();
    ctx.beginPath();
    monotonePath(ctx, pts);
    ctx.strokeStyle = ink(palette, 0.92);
    ctx.lineWidth = 2.75;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.stroke();
    // the last reading
    const last = pts[pts.length - 1];
    ctx.fillStyle = ink(palette, 0.14);
    ctx.beginPath();
    ctx.arc(last[0], last[1], 9, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = ink(palette, 0.95);
    ctx.beginPath();
    ctx.arc(last[0], last[1], 5, 0, Math.PI * 2);
    ctx.fill();
  }

  // each day's move
  const gap = colW > 14 ? 3 : colW > 6 ? 2 : 1;
  ctx.strokeStyle = ink(palette, 0.14);
  ctx.beginPath();
  ctx.moveTo(PAD, Math.round(base) + 0.5);
  ctx.lineTo(width - PAD, Math.round(base) + 0.5);
  ctx.stroke();
  // the bars of one kind are one shape, filled once (a frame of a tween draws every bar)
  const bars = { estimated: new Path2D(), rise: new Path2D(), gain: new Path2D(), loss: new Path2D() };
  scene.moves.forEach((move, at) => {
    const kind = scene.kinds[at];
    const x = PAD + at * colW + gap / 2;
    const w = Math.max(1, colW - gap);
    if (kind === 'estimated') {
      const h = Math.max(3, Math.abs(move) * barsH * 0.72) * grow;
      bar(bars.estimated, x, base - h, w, h);
    } else if (move > 0) {
      const h = Math.max(2, move * barsH * 0.72) * grow;
      bar(kind === 'rise' ? bars.rise : bars.gain, x, base - h, w, h);
    } else if (move < 0) {
      const h = Math.max(2, -move * barsH * 0.28) * grow;
      bar(bars.loss, x, base, w, h);
    }
  });
  ctx.fillStyle = hatch(ctx, palette) ?? ink(palette, 0.2);
  ctx.fill(bars.estimated);
  ctx.fillStyle = palette.accent;
  ctx.fill(bars.rise);
  ctx.fillStyle = ink(palette, 0.8);
  ctx.fill(bars.gain);
  ctx.fillStyle = ink(palette, 0.36);
  ctx.fill(bars.loss);

  // the posts that went out (one shape)
  const dots = new Path2D();
  scene.posts.forEach((count, at) => {
    if (!count) return;
    const r = (count >= 6 ? 4.5 : count >= 3 ? 3.5 : 2.5) * grow;
    const x = xOf(at);
    const y = barsTop + barsH + dotsH / 2 + 2;
    dots.moveTo(x + r, y);
    dots.arc(x, y, r, 0, Math.PI * 2);
  });
  ctx.fillStyle = ink(palette, 0.45);
  ctx.fill(dots);

  // the moments, numbered on the curve
  for (const mark of scene.marks) {
    const r = (mark.big ? 13 : 11) * (0.4 + 0.6 * grow);
    const x = xOf(mark.at);
    const yy = curveTop + mark.y * curveH;
    ctx.fillStyle = palette.raised;
    ctx.beginPath();
    ctx.arc(x, yy, r + 2.5, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = mark.rise ? palette.accent : ink(palette, 0.92);
    ctx.beginPath();
    ctx.arc(x, yy, r, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = mark.rise ? '#fff' : palette.raised;
    ctx.font = font(mark.big ? 12 : 11);
    ctx.textAlign = 'center';
    ctx.textBaseline = 'middle';
    ctx.fillText(mark.label, x, yy + 0.5);
  }

  // the dates
  ctx.font = font(8);
  ctx.fillStyle = ink(palette, 0.4);
  ctx.textBaseline = 'bottom';
  scene.axis.forEach((tick, step) => {
    const lastTick = step === scene.axis.length - 1;
    ctx.textAlign = step === 0 ? 'left' : lastTick ? 'right' : 'center';
    ctx.fillText(tick.label, step === 0 ? PAD : lastTick ? width - PAD : xOf(tick.at), height - 2);
  });
  ctx.globalAlpha = 1;
}

/* the pick: a soft column behind the day, a rule down the curve and a dot on it (drawn at once, no tween) */
function drawPick({ ctx, width, height, palette }: Frame, scene: Scene, index: number) {
  const { axisH, curveTop, curveH, colW, xOf } = layoutOf(width, height, scene.n);
  const x = xOf(index);
  ctx.fillStyle = ink(palette, 0.05);
  roundRect(ctx, PAD + index * colW, curveTop, colW, height - axisH - curveTop, 5);
  ctx.fill();
  ctx.strokeStyle = ink(palette, 0.38);
  ctx.lineWidth = 1;
  ctx.beginPath();
  ctx.moveTo(Math.round(x) + 0.5, curveTop);
  ctx.lineTo(Math.round(x) + 0.5, curveTop + curveH);
  ctx.stroke();
  const y = scene.points[index];
  if (y != null) {
    ctx.fillStyle = palette.raised;
    ctx.beginPath();
    ctx.arc(x, curveTop + y * curveH, 8, 0, Math.PI * 2);
    ctx.fill();
    ctx.fillStyle = ink(palette, 0.95);
    ctx.beginPath();
    ctx.arc(x, curveTop + y * curveH, 5, 0, Math.PI * 2);
    ctx.fill();
  }
}

function blend(from: Scene, to: Scene, t: number): Scene {
  return {
    ...to,
    curve: to.curve.map((value, k) => lerp(from.curve[k] ?? value, value, t)),
    start: lerp(from.start, to.start, t),
    end: lerp(from.end, to.end, t),
    moves: to.moves.map((value, at) => lerp(from.moves[at] ?? 0, value, t)),
  };
}

const GrowthStage = memo(function GrowthStage({ model, index, picked, onHover, onPin }: { model: GrowthModel; index: number; picked: boolean; onHover: (index: number | null) => void; onPin: (index: number | null) => void }) {
  const scene = useMemo(() => sceneOf(model), [model]);
  const pick = useRef({ index, picked });
  const draw = useCallback((frame: Frame, from: Scene | null, to: Scene, t: number) => {
    if (!from || t >= 1) drawScene(frame, to, 1, 1);
    else if (from.n === to.n) drawScene(frame, blend(from, to, t), 1, 1);
    else {
      // a different number of days: the old chart gives way while the new one grows in, its curve bending from the old
      drawScene(frame, from, Math.max(0, 1 - t * 1.6), 1);
      drawScene(frame, { ...to, curve: to.curve.map((value, k) => lerp(from.curve[k] ?? value, value, t)) }, Math.min(1, t * 1.4), 0.25 + 0.75 * t);
    }
    if (t >= 1 && pick.current.picked) drawPick(frame, to, Math.min(to.n - 1, pick.current.index));
  }, []);
  const { canvasRef, redraw } = useCanvasScene(scene, draw);
  const scrub = useScrub(scene.n, (next, mode) => (mode === 'hover' ? onHover(next) : onPin(next)), PAD);
  // the pick is drawn by the canvas alone, at once
  useEffect(() => {
    pick.current = { index, picked };
    redraw();
  }, [index, picked, redraw]);
  return (
    <div className="relative cursor-crosshair select-none pb-2" {...scrub}>
      <div className="relative h-[214px] w-full sm:h-[236px] lg:h-[248px] laptop:h-[204px]">
        <canvas ref={canvasRef} role="img" aria-label={`Followers over ${scene.n} days`} className="absolute inset-0 block h-full w-full" />
      </div>
    </div>
  );
});

function DayPosts({ day }: { day: GrowthDay }) {
  const shown = day.shots.slice(0, 14);
  return (
    <div className="mt-4">
      <div className="flex h-[16px] items-baseline justify-between gap-3 overflow-x-clip whitespace-nowrap">
        <div className="min-w-0 truncate text-[12px] font-black tracking-[-0.01em] text-fg">
          <Swap value={day.shots.length ? `${day.shots.length} ${day.shots.length === 1 ? 'post' : 'posts'} went out on ${dayLabel(day.key)}` : `Nothing went out on ${dayLabel(day.key)}`} />
        </div>
        <span className="shrink-0 text-[9px] font-black uppercase tracking-[0.14em] text-[var(--fm-accent-text)]">{day.best != null ? `best top ${topLabel(day.best)}%` : ''}</span>
      </div>
      <div className="-mx-4 mt-2.5 flex h-[88px] gap-2 overflow-x-auto px-4 [scrollbar-width:none] sm:mx-0 sm:h-[96px] sm:px-0 [&::-webkit-scrollbar]:hidden">
        {shown.map((shot) => (
          <PostTile key={shot.key} shot={shot} when={timeLabel(shot.ms)} className="h-full flex-none" />
        ))}
        {!shown.length ? <span className="grid h-full flex-1 place-items-center rounded-[14px] border border-dashed border-fg/[0.12] text-[10px] font-black uppercase tracking-[0.14em] text-fg/36">A quiet day</span> : null}
      </div>
    </div>
  );
}

/* the moments: four rows, always (a short window fills the rest with what else it says) */
const Moments = memo(function Moments({ model, activeIndex, onPick }: { model: GrowthModel; activeIndex: number | null; onPick: (index: number) => void }) {
  const fillers = useMemo(() => {
    const out: Array<{ label: string; value: string; note: string }> = [];
    const estimated = model.days.filter((one) => one.estimated);
    if (estimated.length) out.push({ label: 'Missing readings', value: `${estimated.length}d`, note: `${shortDay(estimated[0].key)}${estimated.length > 1 ? ` – ${shortDay(estimated[estimated.length - 1].key)}` : ''}, drawn as an estimate` });
    const measured = model.days.filter((one) => !one.estimated && one.delta != null);
    if (measured.length) {
      const average = measured.reduce((sum, one) => sum + (one.delta ?? 0), 0) / measured.length;
      out.push({ label: 'Typical day', value: signed(average), note: `across ${measured.length} measured days` });
      out.push({ label: 'Down days', value: String(measured.filter((one) => (one.delta ?? 0) < 0).length), note: `of ${measured.length} measured days lost followers` });
    }
    return out;
  }, [model.days]);
  const biggest = Math.max(0, ...model.moments.filter((moment) => moment.rise).map((moment) => moment.delta));
  const extra = fillers.slice(0, Math.max(0, 4 - model.moments.length));
  return (
    <div className="flex h-full flex-col">
      <div className="mb-2.5 flex h-[14px] items-center justify-between text-[8px] font-black uppercase tracking-[0.18em] text-fg/44 light:text-fg/54">
        <span>Spikes</span>
        <span className="truncate pl-3 text-fg/32 light:text-fg/46">Biggest jumps, and what posted just before</span>
      </div>
      <div className="grid flex-1 grid-rows-4 gap-2">
        {model.moments.map((moment, order) => (
          <MomentRow
            key={moment.key}
            moment={moment}
            rank={moment.rise ? order + 1 : null}
            biggest={moment.rise && moment.delta === biggest}
            active={activeIndex === moment.index}
            onPick={() => onPick(moment.index)}
          />
        ))}
        {extra.map((fact) => (
          <div key={fact.label} className="flex min-h-[78px] items-center gap-3 rounded-[16px] border border-dashed border-fg/[0.1] px-3">
            <div className="grid h-[58px] w-[46px] shrink-0 place-items-center rounded-[10px] bg-fg/[0.04] text-[13px] font-black tabular-nums text-fg/60">{fact.value}</div>
            <div className="min-w-0">
              <div className="text-[8px] font-black uppercase tracking-[0.16em] text-fg/44 light:text-fg/54">{fact.label}</div>
              <div className="mt-1 text-[11px] font-bold leading-[1.3] text-fg/56">{fact.note}</div>
            </div>
          </div>
        ))}
      </div>
    </div>
  );
});

function MomentRow({ moment, rank, biggest, active, onPick }: { moment: Moment; rank: number | null; biggest: boolean; active: boolean; onPick: () => void }) {
  const percentile = moment.after?.ranked?.latestPercentile ?? null;
  const when = moment.daysBefore === 0 ? 'same day' : moment.daysBefore === 1 ? 'day before' : `${moment.daysBefore} days before`;
  return (
    <button
      type="button"
      onClick={onPick}
      className={cn(
        'group flex min-h-[78px] items-center gap-3 rounded-[16px] border bg-(--ld-raised) px-2.5 text-left transition-[border-color,box-shadow] duration-200',
        active ? 'border-[var(--fm-accent)] shadow-[0_0_0_1px_var(--fm-accent)]' : 'border-fg/[0.08] hover:border-fg/[0.16]',
      )}
    >
      <span className="relative block h-[60px] w-[48px] shrink-0 overflow-hidden rounded-[10px] bg-(--fm-media)">
        <CrossfadeImage src={moment.after?.thumb || null} />
        <span className={cn('absolute left-1 top-1 grid h-[18px] min-w-[18px] place-items-center rounded-full px-1 text-[9px] font-black tabular-nums', rank ? 'bg-[var(--fm-accent)] text-white' : 'bg-white text-black')}>{rank ?? '↓'}</span>
      </span>
      <span className="min-w-0 flex-1">
        <span className="flex items-baseline justify-between gap-2">
          <span className={cn('text-[20px] font-black tabular-nums leading-none tracking-[-0.04em]', rank ? 'text-fg' : 'text-fg/80')}>{signed(moment.delta)}</span>
          <span className="shrink-0 text-[8px] font-black uppercase tracking-[0.12em] text-fg/40 light:text-fg/52">{dayLabel(moment.key)}</span>
        </span>
        <span className="mt-1.5 block truncate text-[11px] font-black text-fg/80">
          {rank ? (biggest ? 'Biggest jump' : 'Jump') : 'Biggest drop'}{moment.after ? ` · after @${moment.after.handle}` : ''}
        </span>
        <span className="mt-0.5 block truncate text-[10px] font-bold text-fg/48 light:text-fg/56">
          {moment.after ? [`${moment.after.format || 'post'}`, percentile != null ? `top ${topLabel(percentile)}%` : 'not ranked yet', when].join(' · ') : 'no post in the two days before'}
        </span>
      </span>
    </button>
  );
}
