'use client';

/* Prime time — when the scope posts and when that lands, by weekday and three-hour block of the IST day (times come
   from the posts' own shortcodes: see lib/postMoment). Each bubble is a slot: its size is how many posts went out
   then, its shade their typical Top % on the app's ramp; a slot needs a few ranked posts before it is shaded
   (hollow until then), so one lucky post can't call a time. Beside the grid: the slot that lands best, the one
   used most, and the posts behind whichever is picked, best first, each with its date and time.

   Motion: the grid is one canvas; a new window or scope tweens every bubble's size and shade together. */

import { CSSProperties, memo, useCallback, useEffect, useMemo, useRef } from 'react';
import { Clock3 } from 'lucide-react';
import { RAMP } from '@/components/read/readShade';
import { cn } from '@/lib/utils';
import type { LeadActivity } from '@/app/api/lead/activity/route';
import { ink, lerp, mixColor, rampColor, useCanvasScene, type Frame } from './leadCanvas';
import { Eyebrow, PostTile, Swap, blockLabel, momentLabel, topLabel, useHeldNear, usePick, useReveal } from './leadKit';
import { BLOCKS, WEEKDAYS, WEEKDAYS_LONG, timingFor, type LeadFeed, type Slot, type TimingModel } from './leadModels';

type Props = { feeds: LeadFeed[]; activity: LeadActivity | null; feedId: string; feeder: string; days: number };

function slotName(slot: Slot, long = true) {
  return `${(long ? WEEKDAYS_LONG : WEEKDAYS)[slot.day]} ${blockLabel(slot.block, true)}`;
}

function LeadTiming(props: Props) {
  const live = useMemo(() => ({ feeds: props.feeds, activity: props.activity, feedId: props.feedId, feeder: props.feeder, days: props.days }), [props.feeds, props.activity, props.feedId, props.feeder, props.days]);
  const [nearRef, scope] = useHeldNear(live);
  const model = useMemo(() => timingFor(scope), [scope]);
  const dealKey = `${scope.feedId}|${scope.feeder}|${scope.days}`;
  const [revealRef, reveal] = useReveal<HTMLDivElement>();
  const { hover, pinned, setHover, setPinned } = usePick<string>(dealKey);
  const best = model.best;
  const busiest = model.busiest && model.busiest.id !== best?.id ? model.busiest : null;
  const picked = model.slots.find((slot) => slot.id === (hover ?? pinned)) || best || model.busiest;
  const mode = hover || (pinned && pinned !== best?.id && pinned !== busiest?.id) ? 'picked' : picked && picked.id === busiest?.id ? 'busiest' : 'best';
  const lift = best?.typical != null && model.typical != null ? Math.round(model.typical - best.typical) : null;

  return (
    <section ref={nearRef} aria-labelledby="lead-timing-title" className="lead-band relative isolate min-w-0 overflow-hidden border-t border-fg/[0.09] bg-(--ld-panel) px-4 py-5 sm:px-6 lg:px-7 lg:py-6">
      <div ref={revealRef} data-reveal={reveal}>
        {/* phones read headline, grid, posts; wide screens put the grid beside the other two */}
        <div className="grid gap-5 lg:grid-cols-[minmax(0,1fr)_minmax(0,1fr)] lg:grid-rows-[auto_1fr] lg:gap-x-8 lg:gap-y-0">
          <div className="lead-rise min-w-0 lg:col-start-2 lg:row-start-1" style={{ '--rise': 0 } as CSSProperties}>
            <Eyebrow icon={<Clock3 className="h-4 w-4" />}>Prime time · IST · {scope.days}D</Eyebrow>
            <h2 id="lead-timing-title" className="sr-only">Prime time: when posts land best</h2>
            <div className="mt-3 flex h-[40px] items-baseline gap-3 overflow-x-clip whitespace-nowrap">
              {best ? (
                <>
                  <span className="fm-depth-title text-[34px] font-black leading-none tracking-[-0.05em] text-fg lg:text-[38px] laptop:text-[32px]">
                    {/* a phone's band can't hold 'Wednesday 9 PM–12 AM' at this size: the day goes short there */}
                    <span className="sm:hidden"><Swap value={WEEKDAYS[best.day]} /></span>
                    <span className="hidden sm:inline"><Swap value={WEEKDAYS_LONG[best.day]} /></span>
                  </span>
                  <span className="fm-depth-title text-[34px] font-black leading-none tracking-[-0.05em] text-[var(--fm-accent-text)] lg:text-[38px] laptop:text-[32px]"><Swap value={blockLabel(best.block, true)} /></span>
                </>
              ) : (
                <span className="text-[20px] font-black leading-none tracking-[-0.03em] text-fg/70">Too early to call it</span>
              )}
            </div>
            <dl className="mt-3 grid grid-cols-3 border-y border-fg/[0.08]">
              {[
                { label: 'Lands best', value: best?.typical != null ? `top ${topLabel(best.typical)}%` : '—', note: lift != null && lift > 0 ? `${lift} above usual` : `${model.ranked} ranked` },
                { label: 'From', value: best ? String(best.ranked) : '—', note: best ? 'ranked posts' : `needs ${model.floor} a slot` },
                { label: 'Busiest', value: busiest ? `${WEEKDAYS[busiest.day]} ${blockLabel(busiest.block)}` : '—', note: busiest ? `${busiest.count} posts` : 'same slot' },
              ].map((stat) => (
                <div key={stat.label} className="min-w-0 border-r border-fg/[0.08] py-2.5 pr-3 last:border-r-0 [&:not(:first-child)]:pl-3">
                  <dt className="truncate text-[8px] font-black uppercase tracking-[0.16em] text-fg/40 light:text-fg/52">{stat.label}</dt>
                  <dd className="mt-1.5 truncate text-[17px] font-black tabular-nums leading-none tracking-[-0.03em] text-fg"><Swap value={stat.value} /></dd>
                  <div className="mt-1 truncate text-[10px] font-bold text-fg/44 light:text-fg/54"><Swap value={stat.note} /></div>
                </div>
              ))}
            </dl>
          </div>

          {/* the grid */}
          <div className="lead-rise flex min-w-0 flex-col lg:col-start-1 lg:row-span-2 lg:row-start-1" style={{ '--rise': 1 } as CSSProperties}>
            <GridCanvas model={model} picked={mode === 'best' ? null : picked?.id ?? null} onHover={setHover} onPin={setPinned} />
            <div className="mt-2.5 flex h-[14px] items-center justify-between gap-4 overflow-x-clip whitespace-nowrap text-[8px] font-black uppercase tracking-[0.12em] text-fg/40 light:text-fg/52">
              <span className="flex items-center gap-2">
                <span className="flex items-end gap-0.5"><i className="block h-[6px] w-[6px] rounded-full bg-fg/40" /><i className="block h-[10px] w-[10px] rounded-full bg-fg/40" /></span>
                more posts
              </span>
              <span className="flex items-center gap-2">top 1%<i className="block h-[6px] w-[56px] rounded-full" style={{ background: RAMP }} />100%</span>
              <span className="hidden items-center gap-1.5 sm:flex"><i className="block h-[10px] w-[10px] rounded-full border-[1.5px] border-fg/30" />under {model.floor} ranked</span>
            </div>
          </div>

          {/* the picked slot's posts */}
          <div className="lead-rise flex min-w-0 flex-col lg:col-start-2 lg:row-start-2 lg:pt-4" style={{ '--rise': 2 } as CSSProperties}>
            <div className="grid h-10 grid-cols-3 rounded-[13px] border border-fg/[0.07] bg-(--fm-well) p-[3px] shadow-(--fm-well-shade)">
              {[
                { id: 'best', label: 'Lands best', slot: best, onPick: () => setPinned(best?.id ?? null) },
                { id: 'busiest', label: 'Busiest', slot: busiest, onPick: () => setPinned(busiest?.id ?? null) },
                { id: 'picked', label: 'Picked', slot: mode === 'picked' ? picked : null, onPick: () => undefined },
              ].map((option) => (
                <button
                  key={option.id}
                  type="button"
                  disabled={!option.slot}
                  onClick={option.onPick}
                  aria-pressed={mode === option.id}
                  className={cn(
                    'relative rounded-[10px] text-[10px] font-black uppercase tracking-[0.08em] transition-colors duration-200 disabled:opacity-40',
                    mode === option.id ? 'bg-fg text-(--ld-panel)' : 'text-fg/50 hover:text-fg/80',
                  )}
                >
                  {option.label}
                </button>
              ))}
            </div>
            <div className="mt-3 flex h-[16px] items-baseline justify-between gap-3 overflow-x-clip whitespace-nowrap">
              <div className="min-w-0 truncate text-[13px] font-black tracking-[-0.01em] text-fg"><Swap value={picked ? slotName(picked) : 'No posts yet'} /></div>
              <div className="shrink-0 text-[10px] font-bold text-fg/50 light:text-fg/58">
                <Swap value={picked ? [`${picked.count} posts`, picked.typical != null ? `top ${topLabel(picked.typical)}%` : null].filter(Boolean).join(' · ') : ''} />
              </div>
            </div>
            <div className="-mx-4 mt-2.5 flex h-[104px] gap-2 overflow-x-auto px-4 [scrollbar-width:none] sm:mx-0 sm:h-[118px] sm:px-0 [&::-webkit-scrollbar]:hidden">
              {(picked?.shots || []).slice(0, 12).map((shot) => (
                <PostTile key={shot.key} shot={shot} when={momentLabel(shot.ms).replace(/^\w+ /, '')} className="h-full flex-none" />
              ))}
              {picked && !picked.shots.length ? <span className="grid h-full flex-1 place-items-center rounded-[14px] border border-dashed border-fg/[0.12] text-[10px] font-black uppercase tracking-[0.14em] text-fg/36">No posts in this slot</span> : null}
            </div>
          </div>
        </div>
      </div>
    </section>
  );
}

export default memo(LeadTiming);

/* ── the grid: one canvas ──────────────────────────────────────────────── */

// a bubble's shade is its typical Top % (resolved against the theme at draw time); null: unshaded
type Bubble = { id: string; size: number; top: number | null; hollow: boolean; best: boolean };
type Scene = { bubbles: Bubble[]; days: string[]; blocks: string[] };

function sceneOf(model: TimingModel): Scene {
  const most = Math.max(1, ...model.slots.map((slot) => slot.count));
  return {
    bubbles: model.slots.map((slot) => {
      const callable = slot.ranked >= model.floor && slot.typical != null;
      return {
        id: slot.id,
        size: slot.count ? 0.34 + 0.66 * Math.sqrt(slot.count / most) : 0.14,
        top: callable ? slot.typical : null,
        hollow: !callable && slot.count > 0,
        best: model.best?.id === slot.id,
      };
    }),
    days: model.dayTypical.map((value) => (value == null ? '—' : `${topLabel(value)}%`)),
    blocks: model.blockTypical.map((value) => (value == null ? '—' : `${topLabel(value)}%`)),
  };
}

const TOP = 18;
const LEFT = 32;
const RIGHT = 36;
const BOTTOM = 18;

function geometry(width: number, height: number) {
  const cellW = (width - LEFT - RIGHT) / BLOCKS.length;
  const cellH = (height - TOP - BOTTOM) / 7;
  return { cellW, cellH, cx: (block: number) => LEFT + (block + 0.5) * cellW, cy: (day: number) => TOP + (day + 0.5) * cellH, r: Math.min(cellW, cellH) / 2 - 3 };
}

function drawGrid({ ctx, width, height, palette }: Frame, from: Scene | null, to: Scene, t: number, picked: string | null) {
  const g = geometry(width, height);
  const font = (size: number) => `800 ${size}px ${palette.font}`;
  const maxR = Math.min(g.r, 20);
  const empty = ink(palette, 0.12);
  ctx.textBaseline = 'middle';
  // block heads, the day names, the margins (each day's and each block's typical)
  ctx.font = font(8);
  ctx.textAlign = 'center';
  ctx.fillStyle = ink(palette, 0.45);
  BLOCKS.forEach((_, block) => ctx.fillText(blockLabel(block).toUpperCase(), g.cx(block), TOP / 2 - 1));
  ctx.textAlign = 'right';
  ctx.fillStyle = ink(palette, 0.34);
  ctx.fillText('TOP', width, TOP / 2 - 1);
  ctx.textAlign = 'left';
  ctx.font = font(9);
  WEEKDAYS.forEach((name, day) => {
    ctx.fillStyle = ink(palette, day >= 5 ? 0.46 : 0.62);
    ctx.fillText(name.toUpperCase(), 0, g.cy(day));
  });
  ctx.font = font(10);
  ctx.textAlign = 'right';
  to.days.forEach((label, day) => {
    ctx.fillStyle = ink(palette, label === '—' ? 0.22 : 0.62);
    ctx.fillText(label, width, g.cy(day));
  });
  ctx.textAlign = 'center';
  to.blocks.forEach((label, block) => {
    ctx.fillStyle = ink(palette, label === '—' ? 0.22 : 0.62);
    ctx.fillText(label, g.cx(block), height - BOTTOM / 2 + 1);
  });
  ctx.textAlign = 'left';
  ctx.fillStyle = ink(palette, 0.34);
  ctx.font = font(8);
  ctx.fillText('TOP', 0, height - BOTTOM / 2 + 1);

  const fillOf = (bubble: Bubble) => (bubble.top != null ? rampColor(palette, bubble.top) : bubble.hollow ? null : empty);
  to.bubbles.forEach((bubble, index) => {
    const old = from?.bubbles[index] ?? null;
    const x = g.cx(index % BLOCKS.length);
    const y = g.cy(Math.floor(index / BLOCKS.length));
    const r = Math.max(2, maxR * (old ? lerp(old.size, bubble.size, t) : bubble.size));
    const toFill = fillOf(bubble);
    const fromFill = old ? fillOf(old) : toFill;
    if (toFill && fromFill) {
      ctx.fillStyle = mixColor(fromFill, toFill, t);
      ctx.beginPath();
      ctx.arc(x, y, r, 0, Math.PI * 2);
      ctx.fill();
    } else if (toFill || fromFill) {
      // a shade arriving or leaving fades with the hollow ring rather than snapping
      ctx.globalAlpha = toFill ? t : 1 - t;
      ctx.fillStyle = (toFill ?? fromFill) as string;
      ctx.beginPath();
      ctx.arc(x, y, r, 0, Math.PI * 2);
      ctx.fill();
      ctx.globalAlpha = 1;
    }
    const hollow = old ? lerp(old.hollow ? 1 : 0, bubble.hollow ? 1 : 0, t) : bubble.hollow ? 1 : 0;
    if (hollow > 0.01) {
      ctx.strokeStyle = ink(palette, 0.32 * hollow);
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.arc(x, y, r - 0.75, 0, Math.PI * 2);
      ctx.stroke();
    }
    const ring = old ? lerp(old.best ? 1 : 0, bubble.best ? 1 : 0, t) : bubble.best ? 1 : 0;
    if (ring > 0.01) {
      ctx.globalAlpha = ring;
      ctx.strokeStyle = palette.accent;
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(x, y, r + 4, 0, Math.PI * 2);
      ctx.stroke();
      ctx.globalAlpha = 1;
    }
    if (picked === bubble.id) {
      ctx.strokeStyle = ink(palette, 0.85);
      ctx.lineWidth = 2;
      ctx.beginPath();
      ctx.arc(x, y, r + (bubble.best ? 8 : 4), 0, Math.PI * 2);
      ctx.stroke();
    }
  });
}

const GridCanvas = memo(function GridCanvas({ model, picked, onHover, onPin }: { model: TimingModel; picked: string | null; onHover: (id: string | null) => void; onPin: (id: string | null) => void }) {
  const scene = useMemo(() => sceneOf(model), [model]);
  const pick = useRef(picked);
  const draw = useCallback((frame: Frame, from: Scene | null, to: Scene, t: number) => drawGrid(frame, from, to, t, pick.current), []);
  const { canvasRef, redraw } = useCanvasScene(scene, draw);
  useEffect(() => {
    pick.current = picked;
    redraw();
  }, [picked, redraw]);
  const slotAt = (clientX: number, clientY: number, el: HTMLElement) => {
    const rect = el.getBoundingClientRect();
    const g = geometry(rect.width, rect.height);
    const block = Math.floor((clientX - rect.left - LEFT) / g.cellW);
    const day = Math.floor((clientY - rect.top - TOP) / g.cellH);
    if (block < 0 || block >= BLOCKS.length || day < 0 || day > 6) return null;
    return `${day}:${block}`;
  };
  // the canvas fills a box sized by the layout (never by its own pixels, which would feed back into its size)
  return (
    <div className="relative min-h-[300px] w-full flex-1 lg:min-h-[330px] laptop:min-h-[276px]">
      <canvas
        ref={canvasRef}
        role="img"
        aria-label="Posts and how they landed by weekday and time of day"
        className="absolute inset-0 block h-full w-full cursor-pointer select-none"
        onPointerMove={(event) => { if (event.pointerType === 'mouse') onHover(slotAt(event.clientX, event.clientY, event.currentTarget)); }}
        onPointerLeave={(event) => { if (event.pointerType === 'mouse') onHover(null); }}
        onClick={(event) => {
          const id = slotAt(event.clientX, event.clientY, event.currentTarget);
          if (id) onPin(id);
        }}
      />
    </div>
  );
});
