'use client';

/* Cadence — how often the scope posts, and who keeps it going (the pace setters). The pace leads (posts a day, against the window
   before), then the calendar: the last thirteen weeks, one row a week, one cell a day, the day's count written in
   it and its shade the volume, the selected window lit and the weeks before it dimmed, today ringed. Tap or hover a
   day for its posts. Beside it, who sets the pace: each feed (or feeder, or one feeder's formats) with its posts,
   its part of the total and how that moved against the window before.

   Motion: the calendar is one canvas; a new window tweens which part is lit, a new scope tweens the cells' shades.
   The lanes' bars glide (transform). */

import { CSSProperties, memo, useCallback, useEffect, useMemo, useRef } from 'react';
import { Activity, Clapperboard, Image as ImageIcon, Layers } from 'lucide-react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import SlotText from '@/components/SlotText';
import { istDayKey } from '@/lib/postMoment';
import type { LeadActivity } from '@/app/api/lead/activity/route';
import { ink, lerp, roundRect, useCanvasScene, type Frame } from './leadCanvas';
import { Delta, Eyebrow, PostTile, Swap, dayLabel, hoursLabel, shortDay, timeLabel, useHeldNear, usePick, useReveal } from './leadKit';
import { WEEKDAYS, rhythmFor, type CalendarDay, type LaneRhythm, type LeadFeed, type RhythmModel } from './leadModels';

type Props = { feeds: LeadFeed[]; activity: LeadActivity | null; feedId: string; feeder: string; days: number };

function pace(perDay: number) {
  if (perDay >= 1) return { value: perDay.toFixed(perDay >= 10 ? 0 : 1).replace(/\.0$/, ''), unit: perDay < 1.05 ? 'post a day' : 'posts a day' };
  const perWeek = perDay * 7;
  return { value: perWeek.toFixed(perWeek >= 10 ? 0 : 1).replace(/\.0$/, ''), unit: perWeek === 1 ? 'post a week' : 'posts a week' };
}

function LeadRhythm(props: Props) {
  const live = useMemo(() => ({ feeds: props.feeds, activity: props.activity, feedId: props.feedId, feeder: props.feeder, days: props.days }), [props.feeds, props.activity, props.feedId, props.feeder, props.days]);
  const [nearRef, scope] = useHeldNear(live);
  const model = useMemo(() => rhythmFor(scope), [scope]);
  const dealKey = `${scope.feedId}|${scope.feeder}|${scope.days}`;
  const [revealRef, reveal] = useReveal<HTMLDivElement>();
  const { hover, pinned, setHover, setPinned } = usePick<string>(dealKey);
  const cells = useMemo(() => model.weeks.flatMap((week) => week.days.filter((day): day is CalendarDay => day != null)), [model.weeks]);
  const latest = useMemo(() => [...cells].reverse().find((day) => day.inWindow && day.count > 0) || cells[cells.length - 1], [cells]);
  const picked = cells.find((day) => day.key === (hover ?? pinned)) || latest;
  const now = pace(model.perDay);
  const change = model.priorPerDay ? (model.perDay - model.priorPerDay) / model.priorPerDay : null;
  const active = model.lanes.filter((lane) => lane.count || lane.prior);
  const lanes = active.slice(0, 6);
  const laneTop = Math.max(0.0001, ...lanes.map((lane) => lane.share));

  return (
    <section ref={nearRef} aria-labelledby="lead-rhythm-title" className="lead-band relative isolate min-w-0 overflow-hidden border-t border-fg/[0.09] bg-(--ld-panel) px-4 py-5 sm:px-6 lg:px-7 lg:py-6">
      <div ref={revealRef} data-reveal={reveal}>
        <div className="grid gap-5 lg:grid-cols-[minmax(0,0.9fr)_minmax(0,1.1fr)] lg:gap-8">
          {/* the pace, and who sets it */}
          <div className="lead-rise flex min-w-0 flex-col" style={{ '--rise': 0 } as CSSProperties}>
            <Eyebrow icon={<Activity className="h-4 w-4" />}>Cadence · {scope.days}D</Eyebrow>
            <h2 id="lead-rhythm-title" className="sr-only">Cadence: how often posts go out</h2>
            <div className="mt-3 flex h-[56px] items-end gap-4">
              <SlotText value={now.value} className="fm-depth-title text-[56px] font-black tabular-nums leading-[0.8] tracking-[-0.06em] text-fg lg:text-[64px] laptop:text-[50px]" />
              <div className="min-w-0 pb-0.5">
                <div className="whitespace-nowrap text-[15px] font-black leading-none tracking-[-0.02em] text-fg"><Swap value={now.unit} /></div>
                <div className="mt-2 h-[15px] overflow-x-clip whitespace-nowrap">
                  {change == null ? (
                    <Delta up={null}>none the {scope.days}D before</Delta>
                  ) : (
                    <Delta up={Math.abs(change) < 0.05 ? null : change > 0}>
                      <Swap value={Math.abs(change) < 0.05 ? `same pace as the ${scope.days}D before` : `${Math.round(Math.abs(change) * 100)}% ${change > 0 ? 'busier' : 'slower'} than the ${scope.days}D before`} />
                    </Delta>
                  )}
                </div>
              </div>
            </div>

            <dl className="mt-4 grid grid-cols-3 border-y border-fg/[0.08]">
              {[
                { label: 'Posted on', value: `${model.activeDays}/${model.days}`, note: 'days' },
                { label: 'Usual gap', value: hoursLabel(model.gapHours), note: 'between posts' },
                { label: 'Longest quiet', value: hoursLabel(model.silenceHours), note: model.silenceFrom ? `from ${shortDay(istDayKey(model.silenceFrom))}` : '—' },
              ].map((stat) => (
                <div key={stat.label} className="min-w-0 border-r border-fg/[0.08] py-2.5 pr-3 last:border-r-0 [&:not(:first-child)]:pl-3">
                  <dt className="truncate text-[8px] font-black uppercase tracking-[0.16em] text-fg/40 light:text-fg/52">{stat.label}</dt>
                  <dd className="fm-depth-title mt-1.5 text-[21px] font-black tabular-nums leading-none tracking-[-0.04em] text-fg"><SlotText value={stat.value} /></dd>
                  <div className="mt-1 truncate text-[10px] font-bold text-fg/44 light:text-fg/54">{stat.note}</div>
                </div>
              ))}
            </dl>

            <div className="mt-4 flex h-[14px] items-center justify-between gap-3 overflow-x-clip whitespace-nowrap">
              <span className="text-[8px] font-black uppercase tracking-[0.2em] text-fg/44 light:text-fg/54">Pace setters</span>
              <span className="text-[8px] font-black uppercase tracking-[0.14em] text-fg/34 light:text-fg/48">share · vs before</span>
            </div>
            <ul className="mt-1 flex flex-1 flex-col justify-start">
              {lanes.map((lane) => (
                <LaneRow key={lane.id} lane={lane} top={laneTop} perDayScope={model.perDay >= 1} />
              ))}
              {active.length > lanes.length ? <li className="pt-2 text-[9px] font-black uppercase tracking-[0.14em] text-fg/36 light:text-fg/48">+{active.length - lanes.length} more posting less</li> : null}
            </ul>
          </div>

          {/* the calendar */}
          <div className="lead-rise flex min-h-[300px] min-w-0 flex-col lg:min-h-[360px] laptop:min-h-[290px]" style={{ '--rise': 1 } as CSSProperties}>
            <CalendarCanvas model={model} picked={picked?.key ?? null} pinned={hover != null || pinned != null} onHover={setHover} onPin={setPinned} />
          </div>
        </div>

        {/* the picked day's posts */}
        <div className="lead-rise mt-5" style={{ '--rise': 2 } as CSSProperties}>
          <div className="flex h-[16px] items-baseline justify-between gap-3 overflow-x-clip whitespace-nowrap">
            <div className="min-w-0 truncate text-[12px] font-black tracking-[-0.01em] text-fg">
              <Swap value={picked ? `${picked.count ? `${picked.count} ${picked.count === 1 ? 'post' : 'posts'}` : 'Nothing'} on ${dayLabel(picked.key)}` : ''} />
            </div>
            <span className="shrink-0 text-[9px] font-black uppercase tracking-[0.14em] text-fg/40 light:text-fg/52">{hover || pinned ? 'picked day' : 'latest day with posts'}</span>
          </div>
          <div className="-mx-4 mt-2.5 flex h-[88px] gap-2 overflow-x-auto px-4 [scrollbar-width:none] sm:mx-0 sm:h-[96px] sm:px-0 [&::-webkit-scrollbar]:hidden">
            {(picked?.shots || []).slice(0, 14).map((shot) => <PostTile key={shot.key} shot={shot} when={timeLabel(shot.ms)} className="h-full flex-none" />)}
            {picked && !picked.count ? <span className="grid h-full flex-1 place-items-center rounded-[14px] border border-dashed border-fg/[0.12] text-[10px] font-black uppercase tracking-[0.14em] text-fg/36">A quiet day</span> : null}
          </div>
        </div>
      </div>
    </section>
  );
}

export default memo(LeadRhythm);

/* ── the calendar: one canvas ──────────────────────────────────────────── */

type Cell = { key: string; count: number; level: number; lit: number; today: boolean } | null;
type Scene = { rows: Array<{ label: string; total: number; lit: number; cells: Cell[] }> };

function sceneOf(model: RhythmModel): Scene {
  return {
    rows: model.weeks.map((week) => ({
      label: shortDay(week.key).toUpperCase(),
      total: week.total,
      lit: week.days.some((day) => day?.inWindow) ? 1 : 0,
      cells: week.days.map((day) => (day
        ? { key: day.key, count: day.count, level: day.count ? 0.08 + 0.72 * Math.pow(day.count / Math.max(1, model.most), 1.6) : 0, lit: day.inWindow ? 1 : 0, today: day.today }
        : null)),
    })),
  };
}

const HEAD = 20;
const GAP = 4;

function geometry(width: number, height: number, rows: number) {
  const left = width < 420 ? 36 : 44;
  const right = width < 420 ? 30 : 38;
  const cellW = (width - left - right - GAP * 6) / 7;
  const cellH = (height - HEAD - GAP * (rows - 1)) / rows;
  return { left, right, cellW, cellH, x: (col: number) => left + col * (cellW + GAP), y: (row: number) => HEAD + row * (cellH + GAP) };
}

function drawCalendar({ ctx, width, height, palette }: Frame, from: Scene | null, to: Scene, t: number, pick: { key: string | null; pinned: boolean }) {
  const g = geometry(width, height, to.rows.length);
  const font = (size: number) => `800 ${size}px ${palette.font}`;
  // weekday heads
  ctx.font = font(8);
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  WEEKDAYS.forEach((name, col) => {
    ctx.fillStyle = ink(palette, col >= 5 ? 0.4 : 0.55);
    ctx.fillText(width < 420 ? name[0] : name.toUpperCase(), g.x(col) + g.cellW / 2, HEAD / 2 - 2);
  });
  ctx.textAlign = 'left';
  ctx.fillStyle = ink(palette, 0.34);
  ctx.fillText('WEEK', 0, HEAD / 2 - 2);
  ctx.textAlign = 'right';
  ctx.fillText('POSTS', width, HEAD / 2 - 2);

  const radius = Math.min(8, g.cellH / 3);
  to.rows.forEach((row, r) => {
    const old = from?.rows[r];
    const lit = old ? lerp(old.lit, row.lit, t) : row.lit;
    const y = g.y(r);
    // the week's label, and its total in the window
    ctx.font = font(8);
    ctx.textAlign = 'left';
    ctx.textBaseline = 'middle';
    ctx.fillStyle = ink(palette, 0.3 + 0.22 * lit);
    ctx.fillText(row.label, 0, y + g.cellH / 2);
    if (lit > 0.05) {
      ctx.textAlign = 'right';
      ctx.font = font(10);
      ctx.fillStyle = ink(palette, (0.25 + 0.4 * lit));
      ctx.fillText(String(old && t < 1 ? Math.round(lerp(old.total, row.total, t)) : row.total), width, y + g.cellH / 2);
    }

    row.cells.forEach((cell, c) => {
      if (!cell) return;
      const before = old?.cells[c];
      const level = before ? lerp(before.level, cell.level, t) : cell.level;
      const on = before ? lerp(before.lit, cell.lit, t) : cell.lit;
      const x = g.x(c);
      const dim = 0.32 + 0.68 * on;
      if (level > 0.001) {
        ctx.fillStyle = ink(palette, level * dim);
        roundRect(ctx, x, y, g.cellW, g.cellH, radius);
        ctx.fill();
      } else {
        ctx.setLineDash([3, 3]);
        ctx.strokeStyle = ink(palette, 0.12 * dim);
        ctx.lineWidth = 1;
        roundRect(ctx, x + 0.5, y + 0.5, g.cellW - 1, g.cellH - 1, radius);
        ctx.stroke();
        ctx.setLineDash([]);
      }
      if (cell.count && g.cellH >= 15) {
        ctx.font = font(g.cellH >= 24 ? 11 : 9);
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillStyle = level * dim > 0.5 ? palette.panel : ink(palette, 0.35 + 0.35 * on);
        ctx.fillText(String(cell.count), x + g.cellW / 2, y + g.cellH / 2 + 0.5);
      }
      const ring = cell.today ? palette.accent : pick.pinned && pick.key === cell.key ? ink(palette, 0.85) : null;
      if (ring) {
        ctx.strokeStyle = ring;
        ctx.lineWidth = 2;
        roundRect(ctx, x - 2, y - 2, g.cellW + 4, g.cellH + 4, radius + 2);
        ctx.stroke();
      }
      if (cell.today && pick.pinned && pick.key === cell.key) {
        ctx.strokeStyle = ink(palette, 0.85);
        ctx.lineWidth = 2;
        roundRect(ctx, x - 5, y - 5, g.cellW + 10, g.cellH + 10, radius + 4);
        ctx.stroke();
      }
    });
  });
}

const CalendarCanvas = memo(function CalendarCanvas({ model, picked, pinned, onHover, onPin }: { model: RhythmModel; picked: string | null; pinned: boolean; onHover: (key: string | null) => void; onPin: (key: string | null) => void }) {
  const scene = useMemo(() => sceneOf(model), [model]);
  const pick = useRef({ key: picked, pinned });
  const draw = useCallback((frame: Frame, from: Scene | null, to: Scene, t: number) => drawCalendar(frame, from, to, t, pick.current), []);
  const { canvasRef, redraw } = useCanvasScene(scene, draw);
  useEffect(() => {
    pick.current = { key: picked, pinned };
    redraw();
  }, [picked, pinned, redraw]);
  const cellAt = (clientX: number, clientY: number, el: HTMLElement) => {
    const rect = el.getBoundingClientRect();
    const g = geometry(rect.width, rect.height, scene.rows.length);
    const col = Math.floor((clientX - rect.left - g.left) / (g.cellW + GAP));
    const row = Math.floor((clientY - rect.top - HEAD) / (g.cellH + GAP));
    return scene.rows[row]?.cells[col]?.key ?? null;
  };
  // the canvas fills a box sized by the layout (never by its own pixels, which would feed back into its size)
  return (
    <div className="relative min-h-[300px] w-full flex-1 lg:min-h-[360px] laptop:min-h-[290px]">
      <canvas
        ref={canvasRef}
        role="img"
        aria-label={`Posts per day over the last ${scene.rows.length} weeks`}
        className="absolute inset-0 block h-full w-full cursor-pointer select-none"
        onPointerMove={(event) => { if (event.pointerType === 'mouse') onHover(cellAt(event.clientX, event.clientY, event.currentTarget)); }}
        onPointerLeave={(event) => { if (event.pointerType === 'mouse') onHover(null); }}
        onClick={(event) => {
          const key = cellAt(event.clientX, event.clientY, event.currentTarget);
          if (key) onPin(key);
        }}
      />
    </div>
  );
});

/* ── who sets the pace ─────────────────────────────────────────────────── */

function LaneIcon({ lane }: { lane: LaneRhythm }) {
  if (lane.format) {
    const Icon = lane.format === 'reel' ? Clapperboard : lane.format === 'carousel' ? Layers : ImageIcon;
    return <span className="grid h-8 w-8 shrink-0 place-items-center rounded-full border border-fg/[0.1] bg-fg/[0.04]"><Icon className="h-4 w-4 text-fg/60" /></span>;
  }
  return (
    <span className="block h-8 w-8 shrink-0">
      <FeederStoryAvatar feeder={{ handle: lane.name.replace(/^@/, ''), profilePicUrl: lane.avatar }} className="text-[10px]" />
    </span>
  );
}

function LaneRow({ lane, top, perDayScope }: { lane: LaneRhythm; top: number; perDayScope: boolean }) {
  const change = lane.change;
  const rate = perDayScope ? `${lane.perDay.toFixed(lane.perDay >= 10 ? 0 : 1).replace(/\.0$/, '')}/day` : `${(lane.perDay * 7).toFixed(1).replace(/\.0$/, '')}/wk`;
  return (
    <li className="flex h-[48px] shrink-0 items-center gap-3 border-b border-fg/[0.06] last:border-b-0">
      <LaneIcon lane={lane} />
      <div className="min-w-0 flex-1">
        <div className="flex items-baseline justify-between gap-3">
          <span className="truncate text-[12px] font-black leading-none tracking-[-0.01em] text-fg">{lane.name}</span>
          <span className="shrink-0 text-[12px] font-black tabular-nums leading-none text-fg">{lane.count}<i className="ml-1 not-italic text-[8px] font-black uppercase tracking-[0.1em] text-fg/40">posts</i></span>
        </div>
        <div className="mt-1.5 flex items-center gap-2.5">
          <span className="relative block h-[5px] min-w-0 flex-1 overflow-hidden rounded-full bg-fg/[0.07]">
            <span className="lead-glide absolute inset-y-0 left-0 w-full origin-left rounded-full bg-fg/75" style={{ transform: `scaleX(${lane.share / top})` }} />
          </span>
          <span className="w-[74px] shrink-0 text-right text-[9px] font-black tabular-nums text-fg/50">{Math.round(lane.share * 100)}% · {rate}</span>
          <span className="w-[52px] shrink-0 text-right">
            {change == null ? <Delta up={true}>new</Delta> : Math.abs(change) < 0.1 ? <Delta up={null}>steady</Delta> : <Delta up={change > 0}>{Math.round(Math.abs(change) * 100)}%</Delta>}
          </span>
        </div>
      </div>
    </li>
  );
}
