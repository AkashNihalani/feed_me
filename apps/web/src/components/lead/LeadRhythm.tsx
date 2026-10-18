'use client';

/* Cadence — how often the scope posts, and who keeps it going (the pace setters). The pace leads (posts a day, against the window
   before), then the calendar: the window itself, one row a week, one cell a day, the day's count written in it and its
   shade the volume, today ringed. It zooms with the window (7D is a row or two of tall days, 90D thirteen or fourteen
   rows of small ones); a day outside the window is never drawn, never dimmed. Tap or hover a day for its posts.
   Beside it, who sets the pace: each feed (or feeder, or one feeder's formats) with its posts, its part of the total
   and how that moved against the window before.

   Motion: the calendar is one canvas; a new window zooms it (the weeks glide to their new rows, anchored at this
   week), a new scope tweens the cells' shades. The lanes' bars glide (transform). */

import { CSSProperties, memo, useCallback, useEffect, useMemo, useRef } from 'react';
import { Activity, Clapperboard, Image as ImageIcon, Layers } from 'lucide-react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import SlotText from '@/components/SlotText';
import { istDayKey } from '@/lib/postMoment';
import type { LeadActivity } from '@/app/api/lead/activity/route';
import { ink, labelFont, labelInk, lerp, roundRect, useCanvasScene, type Frame } from './leadCanvas';
import { DIM, Delta, Eyebrow, MARK, NOTE, PostTile, SOFT, Swap, dayLabel, hoursLabel, shortDay, timeLabel, useHeldNear, usePick, useReveal } from './leadKit';
import { cn } from '@/lib/utils';
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
  // with nothing picked the strip shows the window's busiest day (on a tie, the later one): Growth above opens on the
  // latest day's posts, so the two strips only match when the latest day is also the busiest
  const busiest = useMemo(() => cells.reduce<CalendarDay | null>((best, day) => (day.inWindow && day.count > 0 && (!best || day.count >= best.count) ? day : best), null) || cells[cells.length - 1], [cells]);
  const picked = cells.find((day) => day.key === (hover ?? pinned)) || busiest;
  // a day tapped again lets go (back to the busiest)
  const pinDay = useCallback((key: string | null) => setPinned((current) => (key && current === key ? null : key)), [setPinned]);
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
                <div className="mt-2 h-4 overflow-x-clip whitespace-nowrap">
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
                  <dt className={cn('truncate', MARK, 'tracking-[0.1em]', DIM)}>{stat.label}</dt>
                  <dd className="fm-depth-title mt-1.5 text-[21px] font-black tabular-nums leading-none tracking-[-0.04em] text-fg"><SlotText value={stat.value} /></dd>
                  <div className={cn('mt-1 truncate', NOTE, DIM)}>{stat.note}</div>
                </div>
              ))}
            </dl>

            <div className="mt-4 flex h-[14px] items-center justify-between gap-3 overflow-x-clip whitespace-nowrap">
              <span className={cn(MARK, 'tracking-[0.18em]', DIM)}>Pace setters</span>
              <span className={cn(MARK, DIM)}>share · vs before</span>
            </div>
            <ul className="mt-1 flex flex-1 flex-col justify-start">
              {lanes.map((lane) => (
                <LaneRow key={lane.id} lane={lane} top={laneTop} perDayScope={model.perDay >= 1} />
              ))}
              {active.length > lanes.length ? <li className={cn('pt-2', NOTE, DIM)}>+{active.length - lanes.length} more posting less</li> : null}
            </ul>
          </div>

          {/* the calendar */}
          <div className="lead-rise flex min-h-[300px] min-w-0 flex-col lg:min-h-[360px] laptop:min-h-[290px]" style={{ '--rise': 1 } as CSSProperties}>
            <CalendarCanvas model={model} picked={picked?.key ?? null} pinned={hover != null || pinned != null} onHover={setHover} onPin={pinDay} />
          </div>
        </div>

        {/* the picked day's posts */}
        <div className="lead-rise mt-5" style={{ '--rise': 2 } as CSSProperties}>
          <div className="flex h-[16px] items-baseline justify-between gap-3 overflow-x-clip whitespace-nowrap">
            <div className="min-w-0 truncate text-[12px] font-black tracking-[-0.01em] text-fg">
              <Swap value={picked ? `${picked.count ? `${picked.count} ${picked.count === 1 ? 'post' : 'posts'}` : 'Nothing'} on ${dayLabel(picked.key)}` : ''} />
            </div>
            <span className={cn('shrink-0', MARK, DIM)}>{hover || pinned ? 'picked day' : 'busiest day'}</span>
          </div>
          <div className="-mx-4 mt-2.5 flex h-[128px] gap-2 overflow-x-auto px-4 [scrollbar-width:none] sm:mx-0 sm:h-[136px] sm:px-0 [&::-webkit-scrollbar]:hidden">
            {(picked?.shots || []).slice(0, 14).map((shot) => <PostTile key={shot.key} shot={shot} when={timeLabel(shot.ms)} className="h-full flex-none" />)}
            {picked && !picked.count ? <span className={cn('grid h-full flex-1 place-items-center rounded-[14px] border border-dashed border-fg/[0.12]', MARK, DIM)}>A quiet day</span> : null}
          </div>
        </div>
      </div>
    </section>
  );
}

export default memo(LeadRhythm);

/* ── the calendar: one canvas ──────────────────────────────────────────── */

/* The calendar is the window, zoomed to it: only the weeks the window touches are drawn, and only its days (a day
   outside it is left out, the way a month calendar leaves its edges blank, never drawn dimmed). Fewer weeks, bigger
   days: the rows always fill the box, and a day tall enough wears its date. A new window zooms there: the weeks both
   windows hold glide to their new rows and heights, anchored at this week (the foot), while a week only one of them
   holds slides on past the top as it fades, all on the carry curve (useCanvasScene). A new scope keeps the rows and
   tweens the shades. */

type Cell = { key: string; col: number; count: number; level: number; today: boolean };
type Row = { key: string; label: string; total: number; cells: Cell[] };
type Scene = { rows: Row[]; latest: string };

function sceneOf(model: RhythmModel): Scene {
  const rows = model.weeks
    .filter((week) => week.days.some((day) => day?.inWindow))
    .map((week) => {
      const cells = week.days.flatMap((day, col) => (day && day.inWindow
        ? [{ key: day.key, col, count: day.count, level: day.count ? 0.08 + 0.72 * Math.pow(day.count / Math.max(1, model.most), 1.6) : 0, today: day.today }]
        : []));
      // a week is named by its first day in the window
      return { key: week.key, label: shortDay(cells[0]?.key ?? week.key).toUpperCase(), total: week.total, cells };
    });
  return { rows, latest: model.weeks[model.weeks.length - 1]?.key ?? '' };
}

const HEAD = 20;
const GAP = 4;
// room under the last row for today's ring (and the picked one round it)
const FOOT = 6;
const WEEK_MS = 7 * 86_400_000;

// how many weeks before this week a week (its Monday) is: its place counted from the foot, in any window
function weeksBack(key: string, latest: string) {
  return Math.round((Date.parse(latest) - Date.parse(key)) / WEEK_MS);
}

function geometry(width: number, height: number, rows: number) {
  const count = Math.max(1, rows);
  const left = width < 420 ? 44 : 52;
  const right = width < 420 ? 30 : 38;
  const cellW = (width - left - right - GAP * 6) / 7;
  const cellH = (height - HEAD - FOOT - GAP * (count - 1)) / count;
  // a row's top from its place counted from the foot (0: this week's): a week the window doesn't hold still has one
  return { left, right, cellW, cellH, x: (col: number) => left + col * (cellW + GAP), y: (back: number) => height - FOOT - (back + 1) * cellH - back * GAP };
}

function drawCalendar({ ctx, width, height, palette }: Frame, from: Scene | null, to: Scene, t: number, pick: { key: string | null; pinned: boolean }) {
  const next = geometry(width, height, to.rows.length);
  const prev = from ? geometry(width, height, from.rows.length) : next;
  const { cellW } = next;
  // weekday heads
  ctx.font = labelFont(palette, 10);
  ctx.textAlign = 'center';
  ctx.textBaseline = 'middle';
  WEEKDAYS.forEach((name, col) => {
    ctx.fillStyle = labelInk(palette, col >= 5 ? 0.4 : 0.66);
    ctx.fillText(width < 420 ? name[0] : name.toUpperCase(), next.x(col) + cellW / 2, HEAD / 2 - 2);
  });
  ctx.textAlign = 'left';
  ctx.fillStyle = labelInk(palette, 0.34);
  ctx.fillText('WEEK', 0, HEAD / 2 - 2);
  ctx.textAlign = 'right';
  ctx.fillText('POSTS', width, HEAD / 2 - 2);

  // the weeks of both windows, each between its place in the old layout and the new; a week leaving slides up under
  // the heads (clipped there) as it fades
  const before = new Map((from?.rows ?? []).map((row) => [row.key, row]));
  const after = new Map(to.rows.map((row) => [row.key, row]));
  const keys = [...new Set([...before.keys(), ...after.keys()])];
  ctx.save();
  ctx.beginPath();
  ctx.rect(0, HEAD - 2, width, height - HEAD + 2);
  ctx.clip();
  for (const key of keys) {
    const old = before.get(key) ?? null;
    const row = after.get(key) ?? null;
    const shown = row ?? old;
    if (!shown) continue;
    const back = weeksBack(key, to.latest);
    const y = lerp(prev.y(back), next.y(back), t);
    const h = lerp(prev.cellH, next.cellH, t);
    const rowAlpha = row && old ? 1 : row ? t : 1 - t;
    if (rowAlpha <= 0.001) continue;
    ctx.globalAlpha = rowAlpha;

    // the week's name, and its total in the window
    ctx.font = labelFont(palette, 10);
    ctx.textBaseline = 'middle';
    ctx.textAlign = 'left';
    ctx.fillStyle = labelInk(palette, 0.6);
    ctx.fillText(shown.label, 0, y + h / 2);
    ctx.textAlign = 'right';
    ctx.fillStyle = labelInk(palette, 0.65);
    ctx.fillText(String(row && old && t < 1 ? Math.round(lerp(old.total, row.total, t)) : shown.total), width, y + h / 2);

    // its days: a day both windows hold stays, one coming in or going out fades with the move
    const days = new Map<string, { was: Cell | null; now: Cell | null }>();
    old?.cells.forEach((cell) => days.set(cell.key, { was: cell, now: null }));
    row?.cells.forEach((cell) => days.set(cell.key, { was: days.get(cell.key)?.was ?? null, now: cell }));
    const radius = Math.min(8, h / 3);
    const side = Math.min(cellW, h);
    const countSize = side >= 52 ? 16 : side >= 30 ? 12 : 10;
    const dated = h >= 40 && cellW >= 30;
    for (const { was, now } of days.values()) {
      const cell = now ?? was;
      if (!cell) continue;
      const alpha = rowAlpha * (now && was ? 1 : now ? t : 1 - t);
      if (alpha <= 0.001) continue;
      ctx.globalAlpha = alpha;
      const level = now && was ? lerp(was.level, now.level, t) : cell.level;
      const x = next.x(cell.col);
      if (level > 0.001) {
        ctx.fillStyle = ink(palette, level);
        roundRect(ctx, x, y, cellW, h, radius);
        ctx.fill();
      } else {
        ctx.setLineDash([3, 3]);
        ctx.strokeStyle = ink(palette, 0.12);
        ctx.lineWidth = 1;
        roundRect(ctx, x + 0.5, y + 0.5, cellW - 1, h - 1, radius);
        ctx.stroke();
        ctx.setLineDash([]);
      }
      const onDark = level > 0.5;
      // zoomed in far enough, a day wears its date in its corner
      if (dated) {
        ctx.globalAlpha = alpha * 0.72;
        ctx.font = labelFont(palette, 10);
        ctx.textAlign = 'left';
        ctx.textBaseline = 'top';
        ctx.fillStyle = onDark ? palette.panel : labelInk(palette, 0.6);
        ctx.fillText(String(Number(cell.key.slice(8))), x + 5, y + 5);
        ctx.globalAlpha = alpha;
      }
      if (cell.count && h >= 15) {
        ctx.font = labelFont(palette, countSize);
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillStyle = onDark ? palette.panel : labelInk(palette, 0.7);
        ctx.fillText(String(cell.count), x + cellW / 2, y + h / 2 + (dated ? 3 : 0.5));
      }
      if (!now) continue;
      const ring = now.today ? palette.accent : pick.pinned && pick.key === now.key ? ink(palette, 0.85) : null;
      if (ring) {
        ctx.strokeStyle = ring;
        ctx.lineWidth = 2;
        roundRect(ctx, x - 2, y - 2, cellW + 4, h + 4, radius + 2);
        ctx.stroke();
      }
      if (now.today && pick.pinned && pick.key === now.key) {
        ctx.strokeStyle = ink(palette, 0.85);
        ctx.lineWidth = 2;
        roundRect(ctx, x - 5, y - 5, cellW + 10, h + 10, radius + 4);
        ctx.stroke();
      }
    }
  }
  ctx.restore();
  ctx.globalAlpha = 1;
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
  // the day under a point: its week by place from the foot, its day by column (only the window's days answer)
  const cellAt = (clientX: number, clientY: number, el: HTMLElement) => {
    const rect = el.getBoundingClientRect();
    const g = geometry(rect.width, rect.height, scene.rows.length);
    const col = Math.floor((clientX - rect.left - g.left) / (g.cellW + GAP));
    const back = Math.floor((rect.bottom - FOOT - clientY) / (g.cellH + GAP));
    if (clientY - rect.top < HEAD || back < 0) return null;
    const row = scene.rows.find((week) => weeksBack(week.key, scene.latest) === back);
    return row?.cells.find((cell) => cell.col === col)?.key ?? null;
  };
  // the canvas fills a box sized by the layout (never by its own pixels, which would feed back into its size)
  return (
    <div className="relative min-h-[300px] w-full flex-1 lg:min-h-[360px] laptop:min-h-[290px]">
      <canvas
        ref={canvasRef}
        role="img"
        aria-label={`Posts per day over the last ${model.days} days`}
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
          <span className="shrink-0 text-[12px] font-black tabular-nums leading-none text-fg">{lane.count}<i className={cn('ml-1 not-italic', MARK, 'tracking-[0.08em]', DIM)}>posts</i></span>
        </div>
        <div className="mt-1.5 flex items-center gap-2.5">
          <span className="relative block h-[5px] min-w-0 flex-1 overflow-hidden rounded-full bg-fg/[0.07]">
            <span className="lead-glide absolute inset-y-0 left-0 w-full origin-left rounded-full bg-fg/75" style={{ transform: `scaleX(${lane.share / top})` }} />
          </span>
          <span className={cn('w-[84px] shrink-0 text-right text-[12px] font-black tabular-nums leading-4', SOFT)}>{Math.round(lane.share * 100)}% · {rate}</span>
          <span className="w-[52px] shrink-0 text-right">
            {change == null ? <Delta up={true}>new</Delta> : Math.abs(change) < 0.1 ? <Delta up={null}>steady</Delta> : <Delta up={change > 0}>{Math.round(Math.abs(change) * 100)}%</Delta>}
          </span>
        </div>
      </div>
    </li>
  );
}
