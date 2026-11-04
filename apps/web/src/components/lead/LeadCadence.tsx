'use client';

/* Cadence — how often posts go out, every post at once: a column a day, a brick a post, the best at the foot, each
   brick in its Top % shade (hatched when it has no read yet). One form for every window: 7D zooms in until the bricks
   show their covers, 90D zooms out to ninety slim columns. Weekends sit on a faint band; today is ringed in rose; the
   dashed rose line is the usual pace (posts a day over the window before, at 7D and 30D). Beside it, the pace, the
   days posted on, the busiest day and the posts that reached the top 10%. Hover a column or a brick to read it (a
   tap keeps it; a tap again lets go).

   Motion: a new window or scope tweens the canvas on the Feed's carry curve: the days on both screens glide to their
   new place and width, the new ones come in from the left, the ones leaving slide away; the numbers roll. */

import { CSSProperties, memo, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Activity } from 'lucide-react';
import SlotText from '@/components/SlotText';
import { cn } from '@/lib/utils';
import type { LeadActivity } from '@/app/api/lead/activity/route';
import { RAMP } from '@/components/read/readShade';
import { ink, labelFont, labelInk, lerp, rampColor, roundRect, useCanvasScene, type Frame, type Palette } from './leadCanvas';
import { DIM, Delta, Eyebrow, MARK, NOTE, PostTile, SOFT, ScrollStrip, Swap, compact, dayLabel, shortDay, timeLabel, topLabel, useHeldNear, useReveal } from './leadKit';
import { viewsLabel } from './leadPlays';
import { skylineFor, type LeadFeed, type SkyDay, type SkylineModel } from './leadModels';

type Props = { feeds: LeadFeed[]; activity: LeadActivity | null; feedId: string; feeder: string; days: number };

function pace(perDay: number) {
  if (perDay >= 1) return { value: perDay >= 10 ? String(Math.round(perDay)) : String(Math.round(perDay * 10) / 10), unit: 'posts a day' };
  return { value: String(Math.round(perDay * 70) / 10), unit: 'posts a week' };
}

function LeadCadence(props: Props) {
  const live = useMemo(() => ({ feeds: props.feeds, activity: props.activity, feedId: props.feedId, feeder: props.feeder, days: props.days }), [props.feeds, props.activity, props.feedId, props.feeder, props.days]);
  const [nearRef, scope] = useHeldNear(live);
  const model = useMemo(() => skylineFor(scope), [scope]);
  const dealKey = `${scope.feedId}|${scope.feeder}|${scope.days}`;
  const [revealRef, reveal] = useReveal<HTMLDivElement>();
  // the day (and the brick) under the pointer, and the one tapped
  const [pick, setPick] = useState<{ deal: string; hover: Hit | null; pinned: Hit | null }>({ deal: dealKey, hover: null, pinned: null });
  const live2 = pick.deal === dealKey ? pick : { deal: dealKey, hover: null, pinned: null };
  const shown = live2.hover ?? live2.pinned;

  const now = pace(model.perDay);
  const change = model.priorPerDay ? (model.perDay - model.priorPerDay) / model.priorPerDay : null;
  const stats = [
    { label: 'Posted on', value: `${model.activeDays}/${model.days.length}`, note: 'days' },
    { label: 'Busiest', value: model.busiest ? String(model.busiest.shots.length) : '—', note: model.busiest ? dayLabel(model.busiest.key) : 'no posts' },
    { label: 'Top 10%', value: String(model.hits), note: `of ${model.count} posts` },
  ];

  return (
    <section ref={nearRef} aria-labelledby="lead-cadence-title" className="lead-band relative isolate min-w-0 overflow-hidden border-t border-fg/[0.09] bg-(--ld-panel) px-4 py-5 sm:px-6 lg:px-7 lg:py-6">
      <div ref={revealRef} data-reveal={reveal}>
        {/* one balanced block: on a phone the pace, the skyline, the day and the key stack; on a laptop the pace, the day
            and the key stand in a column beside the skyline, which grows to their height (no empty space under either) */}
        <div className="grid gap-5 lg:grid-cols-[minmax(0,0.85fr)_minmax(0,1.15fr)] lg:grid-rows-[auto_1fr_auto] lg:gap-x-9 lg:gap-y-4 lg:[grid-template-areas:'head_sky'_'day_sky'_'ramp_sky']">
          <div className="lead-rise flex min-w-0 flex-col lg:[grid-area:head]" style={{ '--rise': 0 } as CSSProperties}>
            <Eyebrow icon={<Activity className="h-4 w-4" />}>Cadence · {scope.days}D</Eyebrow>
            <h2 id="lead-cadence-title" className="sr-only">Cadence: every post in the window, a column a day</h2>
            <div className="mt-3 flex h-[56px] items-end gap-4">
              <SlotText value={now.value} className="fm-depth-title text-[56px] font-black tabular-nums leading-[0.8] tracking-[-0.06em] text-fg lg:text-[64px] laptop:text-[50px]" />
              <div className="min-w-0 pb-0.5">
                <div className="whitespace-nowrap text-[15px] font-black leading-none tracking-[-0.02em] text-fg"><Swap value={now.unit} /></div>
                <div className="mt-2 h-4 overflow-x-clip whitespace-nowrap">
                  {/* 90D has nothing before it (see readsBefore): the line stays, empty, so nothing shifts */}
                  {model.priorPerDay == null ? null : change == null ? (
                    <Delta up={null}>none the {scope.days}D before</Delta>
                  ) : (
                    <Delta up={Math.abs(change) < 0.05 ? null : change > 0}>
                      <Swap value={Math.abs(change) < 0.05 ? `same pace as the ${scope.days}D before` : `${Math.round(Math.abs(change) * 100)}% ${change > 0 ? 'busier' : 'slower'} than the ${scope.days}D before`} />
                    </Delta>
                  )}
                </div>
              </div>
            </div>
            <dl className="mt-5 grid grid-cols-3 border-y border-fg/[0.08]">
              {stats.map((stat) => (
                <div key={stat.label} className="min-w-0 border-r border-fg/[0.08] py-2.5 pr-3 last:border-r-0 [&:not(:first-child)]:pl-3">
                  <dt className={cn('truncate', MARK, 'tracking-[0.1em]', DIM)}>{stat.label}</dt>
                  <dd className="fm-depth-title mt-1.5 text-[21px] font-black tabular-nums leading-none tracking-[-0.04em] text-fg"><SlotText value={stat.value} /></dd>
                  <div className={cn('mt-1 truncate', NOTE, DIM)}><Swap value={stat.note} /></div>
                </div>
              ))}
            </dl>
          </div>

          <div className="lead-rise flex min-w-0 flex-col lg:[grid-area:sky]" style={{ '--rise': 1 } as CSSProperties}>
            <Skyline
              model={model}
              hover={live2.hover}
              pinned={live2.pinned}
              onHover={(hit) => setPick((current) => ({ ...(current.deal === dealKey ? current : { deal: dealKey, pinned: null }), deal: dealKey, hover: hit }))}
              onPin={(hit) => setPick((current) => {
                const base = current.deal === dealKey ? current : { deal: dealKey, hover: null, pinned: null };
                const same = base.pinned && hit && base.pinned.key === hit.key && base.pinned.brick === hit.brick;
                return { ...base, pinned: same ? null : hit };
              })}
            />
          </div>

          <div className="lead-rise min-w-0 lg:[grid-area:day]" style={{ '--rise': 2 } as CSSProperties}>
            <DayRead model={model} hit={shown} picked={Boolean(live2.pinned) && !live2.hover} />
          </div>
          <Ramp usual={model.priorPerDay != null} days={scope.days} className="lead-rise flex lg:[grid-area:ramp]" />
        </div>
      </div>
    </section>
  );
}

export default memo(LeadCadence);

/* What the skyline holds for the day picked (the busiest, when nothing is): how many went out, how many reached the
   top 10%, its best post, the day against the usual pace, and its posts best first. A brick picked is its post: the
   picture, its place, its multiple of the usual, its likes, comments and views. One height whatever it shows, so the
   page under it never moves. */
function DayRead({ model, hit, picked }: { model: SkylineModel; hit: Hit | null; picked: boolean }) {
  const day = (hit && model.days.find((one) => one.key === hit.key)) || model.busiest;
  const shot = day && hit?.brick != null ? day.shots[hit.brick] ?? null : null;
  const label = hit ? (picked ? 'Picked' : 'This day') : 'Busiest day';
  const frame = 'h-[184px] overflow-hidden lg:h-full lg:min-h-[184px] rounded-[18px] border border-fg/[0.08] bg-(--ld-raised) p-3.5 sm:p-4';
  if (!day) return <div className={cn(frame, 'grid place-items-center', NOTE, DIM)}>Nothing went out in this window</div>;

  if (shot) {
    const post = shot.ranked;
    const views = post ? viewsLabel(post) : null;
    const facts = [
      { label: 'Placed', value: post?.latestPercentile != null ? `Top ${topLabel(post.latestPercentile)}%` : 'not yet', win: (post?.latestPercentile ?? 101) <= 10 },
      { label: 'Its usual', value: post?.rankingMultiple != null ? `${post.rankingMultiple >= 10 ? Math.round(post.rankingMultiple) : Math.round(post.rankingMultiple * 10) / 10}×` : '—', win: false },
      { label: 'Likes', value: post?.likes != null ? compact(post.likes) : '—', win: false },
      { label: 'Comments', value: post?.comments != null ? compact(post.comments) : '—', win: false },
      ...(views ? [{ label: 'Views', value: views.replace(' views', ''), win: false }] : []),
    ];
    return (
      <div className={frame}>
        <div className="flex h-full gap-4">
          <a href={post?.postUrl || '#'} target={post?.postUrl ? '_blank' : undefined} rel="noreferrer" onClick={(event) => { if (!post?.postUrl) event.preventDefault(); }} className="h-full shrink-0" aria-label={post?.postUrl ? 'Open the post on Instagram' : 'The post'}>
            <PostTile shot={shot} className="h-full w-auto aspect-[4/5]" />
          </a>
          <div className="flex min-w-0 flex-1 flex-col">
            <div className={cn('truncate', MARK, DIM)}>{picked ? 'Picked post' : 'This post'} · {dayLabel(day.key)}, {timeLabel(shot.ms)}</div>
            <div className="mt-1.5 truncate text-[17px] font-black tracking-[-0.02em] text-fg">@{shot.handle} <span className={cn('font-bold', DIM)}>· {shot.format ?? 'post'}</span></div>
            <dl className="mt-auto grid grid-cols-2 gap-x-4 gap-y-3 sm:grid-cols-3">
              {facts.map((fact) => (
                <div key={fact.label} className="min-w-0">
                  <dt className={cn('truncate', MARK, DIM)}>{fact.label}</dt>
                  <dd className={cn('fm-depth-title mt-1 truncate text-[20px] font-black tabular-nums leading-none tracking-[-0.03em]', fact.win ? 'text-[var(--fm-accent-text)]' : 'text-fg')}><SlotText value={fact.value} /></dd>
                </div>
              ))}
            </dl>
          </div>
        </div>
      </div>
    );
  }

  const count = day.shots.length;
  const hits = day.shots.filter((one) => (one.ranked?.latestPercentile ?? 101) <= 10).length;
  const best = day.shots.find((one) => one.ranked?.latestPercentile != null)?.ranked?.latestPercentile ?? null;
  const usual = model.priorPerDay ?? model.perDay;
  const against = count - Math.round(usual);
  return (
    <div className={frame}>
      <div className="flex h-full gap-4">
        <div className="flex w-[132px] shrink-0 flex-col sm:w-[150px]">
          <div className={cn('truncate', MARK, DIM)}>{label}</div>
          <div className={cn('mt-0.5 truncate', NOTE, SOFT)}><Swap value={`${dayLabel(day.key)}${day.today ? ' · so far' : ''}`} /></div>
          <div className="mt-2 flex items-baseline gap-1.5">
            <SlotText value={String(count)} className="fm-depth-title text-[40px] font-black tabular-nums leading-none tracking-[-0.05em] text-fg" />
            <span className={cn(MARK, DIM)}>{count === 1 ? 'post' : 'posts'}</span>
          </div>
          <div className="mt-auto space-y-1">
            <div className={cn('truncate', NOTE, hits ? 'text-[var(--fm-accent-text)]' : DIM)}><b className="font-black">{hits}</b> in the top 10%</div>
            <div className={cn('truncate', NOTE, SOFT)}>best <b className="font-black text-fg">{best != null ? `Top ${topLabel(best)}%` : '—'}</b></div>
            <div className={cn('truncate', NOTE, SOFT)}>{against === 0 ? 'right on the usual pace' : `${Math.abs(against)} ${against > 0 ? 'over' : 'under'} the usual pace`}</div>
          </div>
        </div>
        <ScrollStrip resetKey={day.key} outer="-mr-3.5 flex flex-1 sm:-mr-4" className="flex h-full w-full gap-2 overflow-x-auto pr-3.5 [scrollbar-width:none] sm:pr-4 [&::-webkit-scrollbar]:hidden">
          {day.shots.slice(0, 12).map((one) => <PostTile key={one.key} shot={one} when={timeLabel(one.ms)} className="h-full w-auto flex-none aspect-[4/5]" />)}
          {!count ? <span className={cn('grid flex-1 place-items-center rounded-[14px] border border-dashed border-fg/[0.12]', MARK, DIM)}>A quiet day</span> : null}
        </ScrollStrip>
      </div>
    </div>
  );
}

function Ramp({ usual, days, className }: { usual: boolean; days: number; className?: string }) {
  return (
    <div className={cn('flex-wrap items-center gap-x-4 gap-y-1.5', NOTE, DIM, className)}>
      <span>A brick per post, the best at the foot</span>
      <span className="flex items-center gap-1.5"><i className="block h-2 w-16 rounded-full" style={{ background: RAMP }} />Top 1% → 100%</span>
      <span className="flex items-center gap-1.5"><i className="block h-2.5 w-3.5 rounded-[3px] bg-[repeating-linear-gradient(135deg,transparent_0_3px,rgb(var(--fm-fg-rgb)/0.24)_3px_4px)] shadow-[inset_0_0_0_1px_rgb(var(--fm-fg-rgb)/0.14)]" />not ranked yet</span>
      {/* kept in place (unseen) at 90D, so the key wraps the same in every range */}
      <span className={cn('flex items-center gap-1.5', !usual && 'invisible')} aria-hidden={!usual}><i className="block h-0 w-4 border-t-2 border-dashed border-[var(--fm-accent)]" />usual pace, the {days}D before</span>
    </div>
  );
}

/* ── the skyline: one canvas ───────────────────────────────────────────── */

type Hit = { key: string; brick: number | null };
type Scene = {
  days: Array<{ key: string; weekend: boolean; today: boolean; bricks: Array<{ percentile: number | null; thumb: string | null }> }>;
  at: Map<string, number>;
  most: number;
  usual: number | null;
  ticks: Array<{ key: string; label: string }>;
};

function sceneOf(model: SkylineModel): Scene {
  const n = model.days.length;
  const window = model.days;
  let ticks: Array<{ key: string; label: string }>;
  if (n <= 8) ticks = window.map((day) => ({ key: day.key, label: shortDay(day.key).toUpperCase() }));
  else {
    const mondays = window.filter((day) => day.weekday === 0);
    const every = n <= 31 ? 1 : 2;
    ticks = mondays.filter((_, order) => (mondays.length - 1 - order) % every === 0).map((day) => ({ key: day.key, label: shortDay(day.key).toUpperCase() }));
  }
  return {
    days: model.days.map((day: SkyDay) => ({
      key: day.key,
      weekend: day.weekday >= 5,
      today: day.today,
      bricks: day.shots.map((shot) => ({ percentile: shot.ranked?.latestPercentile ?? null, thumb: n <= 8 ? shot.thumb : null })),
    })),
    at: new Map(model.days.map((day, index) => [day.key, index])),
    most: Math.max(1, model.most, Math.ceil(model.priorPerDay ?? 0)),
    usual: model.priorPerDay,
    ticks,
  };
}

function geometry(width: number, height: number, scene: Scene) {
  const top = 10;
  const bottom = 24;
  const right = 28;
  const n = Math.max(1, scene.days.length);
  const plotW = width - right;
  const plotH = height - top - bottom;
  const gap = n <= 8 ? (width < 480 ? 8 : 14) : n <= 31 ? (width < 480 ? 2 : 4) : width < 480 ? 1 : 2;
  const col = (plotW - gap * (n - 1)) / n;
  const unit = plotH / (scene.most * 1.06);
  return { top, plotH, right, gap, col, unit, base: top + plotH, x: (index: number) => index * (col + gap) };
}

// covers drawn into the bricks at 7D, decoded once and kept; a cover arriving draws the chart again
const covers = new Map<string, HTMLImageElement>();
const coverListeners = new Set<() => void>();
function coverOf(url: string) {
  let image = covers.get(url);
  if (!image) {
    image = new Image();
    image.decoding = 'async';
    image.onload = () => coverListeners.forEach((listener) => listener());
    image.src = url;
    covers.set(url, image);
  }
  return image.complete && image.naturalWidth ? image : null;
}

let hatchFor: { key: string; pattern: CanvasPattern | null } | null = null;
function hatchOf(ctx: CanvasRenderingContext2D, palette: Palette) {
  const key = palette.fg.join(',');
  if (hatchFor && hatchFor.key === key) return hatchFor.pattern;
  const tile = document.createElement('canvas');
  tile.width = 6;
  tile.height = 6;
  const t = tile.getContext('2d');
  if (t) {
    t.strokeStyle = ink(palette, 0.24);
    t.lineWidth = 1;
    t.beginPath();
    t.moveTo(-1, 7);
    t.lineTo(7, -1);
    t.stroke();
  }
  hatchFor = { key, pattern: ctx.createPattern(tile, 'repeat') };
  return hatchFor.pattern;
}

function Skyline({ model, hover, pinned, onHover, onPin }: { model: SkylineModel; hover: Hit | null; pinned: Hit | null; onHover: (hit: Hit | null) => void; onPin: (hit: Hit | null) => void }) {
  const scene = useMemo(() => sceneOf(model), [model]);
  const boxRef = useRef<HTMLDivElement | null>(null);

  const draw = useCallback(({ ctx, width, height, palette }: Frame, from: Scene | null, to: Scene, t: number) => {
    const gTo = geometry(width, height, to);
    const gFrom = from ? geometry(width, height, from) : gTo;
    const hatch = hatchOf(ctx, palette);
    const unitNow = lerp(gFrom.unit, gTo.unit, t);
    const keys = new Set([...(from ? from.days.map((day) => day.key) : []), ...to.days.map((day) => day.key)]);
    type Column = { key: string; x: number; w: number; unit: number; alpha: number; day: Scene['days'][number]; live: boolean };
    const columns: Column[] = [];
    for (const key of keys) {
      const a = from?.at.get(key);
      const b = to.at.get(key);
      if (a != null && b != null) columns.push({ key, x: lerp(gFrom.x(a), gTo.x(b), t), w: lerp(gFrom.col, gTo.col, t), unit: unitNow, alpha: 1, day: to.days[b], live: true });
      else if (b != null) columns.push({ key, x: gTo.x(b) - (1 - t) * 40, w: gTo.col, unit: gTo.unit, alpha: from ? t : 1, day: to.days[b], live: true });
      else if (a != null && from) columns.push({ key, x: gFrom.x(a) - t * 40, w: gFrom.col, unit: gFrom.unit, alpha: 1 - t, day: from.days[a], live: false });
    }
    const { base, top, plotH } = gTo;

    // weekends, a faint band behind their days
    for (const column of columns) {
      if (!column.day.weekend || column.alpha <= 0.01) continue;
      ctx.globalAlpha = column.alpha;
      ctx.fillStyle = ink(palette, 0.035);
      const pad = Math.min(3, gTo.gap / 2);
      ctx.fillRect(column.x - pad, top - 4, column.w + pad * 2, plotH + 4);
    }
    ctx.globalAlpha = 1;
    // the count scale, right
    const most = plotH / unitNow;
    const step = most > 40 ? 20 : most > 16 ? 10 : most > 8 ? 4 : 2;
    ctx.font = labelFont(palette, 10);
    ctx.textAlign = 'right';
    ctx.textBaseline = 'middle';
    ctx.lineWidth = 1;
    for (let count = 0; count <= most; count += step) {
      const y = Math.round(base - count * unitNow) + 0.5;
      ctx.strokeStyle = ink(palette, count ? 0.06 : 0.16);
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(width - gTo.right + 6, y);
      ctx.stroke();
      ctx.fillStyle = labelInk(palette, 0.42);
      ctx.fillText(String(count), width, y);
    }
    // the days: a brick a post, the best at the foot
    const hoverHit = hover;
    const pinnedHit = pinned;
    for (const column of columns) {
      if (column.alpha <= 0.01) continue;
      ctx.globalAlpha = column.alpha;
      const bricks = column.day.bricks;
      const gapY = column.unit >= 5 ? 1.5 : column.unit >= 3 ? 1 : 0;
      const radius = Math.min(4, column.w / 3, column.unit / 3);
      const withCovers = column.w >= 34 && column.unit >= 30;
      bricks.forEach((brick, k) => {
        const y = base - (k + 1) * column.unit;
        const h = column.unit - gapY;
        roundRect(ctx, column.x, y + gapY, column.w, h, radius);
        if (brick.percentile != null) {
          ctx.fillStyle = rampColor(palette, brick.percentile);
          ctx.fill();
        } else {
          ctx.fillStyle = hatch ?? ink(palette, 0.1);
          ctx.fill();
          ctx.strokeStyle = ink(palette, 0.18);
          ctx.lineWidth = 1;
          ctx.stroke();
        }
        const image = withCovers && brick.thumb ? coverOf(brick.thumb) : null;
        if (image) {
          ctx.save();
          const ix = column.x + 3;
          const iy = y + gapY + 3;
          const iw = column.w - 6;
          const ih = h - 6;
          roundRect(ctx, ix, iy, iw, ih, Math.max(1, radius - 2));
          ctx.clip();
          const s = Math.max(iw / image.naturalWidth, ih / image.naturalHeight);
          const dw = image.naturalWidth * s;
          const dh = image.naturalHeight * s;
          ctx.drawImage(image, ix + (iw - dw) / 2, iy + (ih - dh) / 2, dw, dh);
          ctx.restore();
        }
        const lit = (one: Hit | null) => one && one.key === column.key && one.brick === k;
        if (column.live && t >= 1 && (lit(hoverHit) || lit(pinnedHit))) {
          roundRect(ctx, column.x - 1, y + gapY - 1, column.w + 2, h + 2, radius + 1);
          ctx.strokeStyle = ink(palette, 0.95);
          ctx.lineWidth = 2;
          ctx.stroke();
        }
      });
      if (!bricks.length && !column.day.today) {
        ctx.fillStyle = ink(palette, 0.2);
        ctx.fillRect(column.x + column.w / 2 - Math.min(5, column.w / 2), base - 1.5, Math.min(10, column.w), 1.5);
      }
      // today, ringed in rose; the day picked or under the pointer, in ink
      const ring = (color: string, out: number) => {
        const tall = Math.max(bricks.length * column.unit, Math.min(plotH, 24));
        roundRect(ctx, column.x - out, base - tall - out, column.w + out * 2, tall + out * 2, Math.min(6, column.w / 2 + out));
        ctx.strokeStyle = color;
        ctx.lineWidth = column.w < 6 ? 1.25 : 2;
        ctx.stroke();
      };
      if (column.day.today) ring(palette.accent, 3);
      const dayLit = (one: Hit | null) => one && one.key === column.key && one.brick == null;
      if (column.live && t >= 1 && (dayLit(hoverHit) || dayLit(pinnedHit))) ring(ink(palette, 0.9), column.day.today ? 6 : 3);
    }
    ctx.globalAlpha = 1;
    // the usual pace: posts a day over the window before (7D and 30D)
    const usual = from ? lerp(from.usual ?? 0, to.usual ?? 0, t) : to.usual ?? 0;
    if (to.usual != null || (from?.usual != null && t < 1)) {
      ctx.globalAlpha = to.usual != null ? 1 : 1 - t;
      const y = Math.round(base - usual * unitNow) + 0.5;
      ctx.setLineDash([5, 4]);
      ctx.strokeStyle = palette.accent;
      ctx.lineWidth = 1.5;
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(width - gTo.right + 6, y);
      ctx.stroke();
      ctx.setLineDash([]);
      ctx.font = labelFont(palette, 10);
      ctx.textAlign = 'right';
      ctx.textBaseline = 'bottom';
      ctx.lineWidth = 4;
      ctx.lineJoin = 'round';
      ctx.strokeStyle = palette.panel;
      const label = `USUAL ${Math.round(usual)}`;
      ctx.strokeText(label, width - gTo.right + 4, y - 4);
      ctx.fillStyle = palette.accentText;
      ctx.fillText(label, width - gTo.right + 4, y - 4);
      ctx.globalAlpha = 1;
    }
    // the dates
    ctx.font = labelFont(palette, 10);
    ctx.textBaseline = 'alphabetic';
    ctx.fillStyle = labelInk(palette, 0.5);
    const small = to.days.length <= 8;
    for (const tick of to.ticks) {
      const column = columns.find((one) => one.key === tick.key && one.live);
      if (!column) continue;
      ctx.globalAlpha = column.alpha;
      ctx.textAlign = small ? 'center' : 'left';
      ctx.fillText(tick.label, Math.max(0, small ? column.x + column.w / 2 : column.x), height - 7);
    }
    ctx.globalAlpha = 1;
  }, [hover, pinned]);

  const { canvasRef, redraw } = useCanvasScene(scene, draw);
  useEffect(() => {
    redraw();
  }, [hover, pinned, redraw]);
  useEffect(() => {
    coverListeners.add(redraw);
    return () => {
      coverListeners.delete(redraw);
    };
  }, [redraw]);

  const hitAt = useCallback((clientX: number, clientY: number): Hit | null => {
    const box = boxRef.current;
    if (!box) return null;
    const rect = box.getBoundingClientRect();
    const g = geometry(rect.width, rect.height, scene);
    const px = clientX - rect.left;
    const py = clientY - rect.top;
    const index = Math.floor((px + g.gap / 2) / (g.col + g.gap));
    if (index < 0 || index >= scene.days.length) return null;
    const day = scene.days[index];
    const k = Math.floor((g.base - py) / g.unit);
    return { key: day.key, brick: k >= 0 && k < day.bricks.length && g.unit >= 4 ? k : null };
  }, [scene]);

  return (
    <div
      ref={boxRef}
      className="relative h-[260px] cursor-pointer select-none sm:h-[300px] lg:h-auto lg:min-h-[320px] lg:flex-1"
      style={{ touchAction: 'pan-y' }}
      onPointerMove={(event) => {
        if (event.pointerType === 'mouse') onHover(hitAt(event.clientX, event.clientY));
      }}
      onPointerLeave={(event) => {
        if (event.pointerType === 'mouse') onHover(null);
      }}
      onClick={(event) => onPin(hitAt(event.clientX, event.clientY))}
    >
      <canvas ref={canvasRef} role="img" aria-label="Every post in the window, a column a day, a brick a post, shaded by its Top %" className="absolute inset-0 h-full w-full" />
    </div>
  );
}
