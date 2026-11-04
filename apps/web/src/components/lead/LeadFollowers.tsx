'use client';

/* Followers — who's growing. Every feeder's follower growth since the window began is a line, in percent of its own
   size (so a 56M account can't drown a 40K one), faint behind the average feeder's (the thick ink line, its area down
   to the start). The fastest grower is drawn in rose; each feeder's best day in the window (2× its usual day or more,
   the three biggest) is a rose dot with its multiple. Drag across it (or hover) and the readout reads that day; a
   feeder in the lists beside it lights its own line. The lists: the gainers, and the slowest (or the losers).

   One feeder picked is read against its feed: the chart keeps every feeder of the feed and the feed's average
   feeder, and the picked one is the line in rose; beside it, in place of the leaderboard, its five biggest days
   (gains or losses, numbered on the chart too, each with its own post from that day or the day before). The layout
   never changes between a feed and its feeder: the rose passes from line to line on the chart's own tween.

   Motion: a new window or scope tweens the canvas on the Feed's carry curve (each line bends into its new shape, the
   scale eases, lines that come or go fade on the same clock); the headline rolls; the lists re-deal on the ledger's
   beat, as the Feederboard's rows do. */

import { CSSProperties, memo, useCallback, useEffect, useMemo, useState } from 'react';
import { Users } from 'lucide-react';
import SlotText from '@/components/SlotText';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { Ledger, ledgerDelay } from '@/components/motion/Ledger';
import { cn } from '@/lib/utils';
import type { LeadActivity } from '@/app/api/lead/activity/route';
import { ink, labelFont, labelInk, lerp, monotonePath, resample, useCanvasScene, type Frame, type Palette } from './leadCanvas';
import { CrossfadeImage, DIM, Eyebrow, MARK, NOTE, SOFT, Swap, compact, dayLabel, shortDay, signed, topLabel, useHeldNear, usePick, useReveal, useScrub } from './leadKit';
import { titleCase } from '@/lib/feedLabels';
import { followersFor, type FollowerLine, type FollowerSpike, type FollowersModel, type LeadFeed } from './leadModels';

type Props = { feeds: LeadFeed[]; activity: LeadActivity | null; feedId: string; feeder: string; days: number };

const SAMPLES = 72;
const PAD = 12;

/* "+1.5%", "−0.04%", "0%" */
function percent(value: number | null, digits?: number) {
  if (value == null) return '—';
  const size = Math.abs(value);
  if (size < 0.005) return '0%';
  const places = digits ?? (size >= 10 ? 0 : size < 0.1 ? 2 : 1);
  return `${value > 0 ? '+' : '−'}${size.toFixed(places)}%`;
}

const times = (value: number) => `${value >= 10 ? Math.round(value) : Math.round(value * 10) / 10}×`;

function LeadFollowers(props: Props) {
  const live = useMemo(() => ({ feeds: props.feeds, activity: props.activity, feedId: props.feedId, feeder: props.feeder, days: props.days }), [props.feeds, props.activity, props.feedId, props.feeder, props.days]);
  const [nearRef, scope] = useHeldNear(live);
  // a feeder is read against its feed: the model is the feed's, the feeder is the focus
  const feedScope = useMemo(() => (scope.feeder ? { ...scope, feeder: '' } : scope), [scope]);
  const model = useMemo(() => followersFor(feedScope), [feedScope]);
  const focus = scope.feeder ? model.lines.find((line) => line.handle === scope.feeder) ?? null : null;
  const feedName = scope.feedId === 'all' ? 'All feeds' : titleCase(scope.feeds.find((feed) => feed.id === scope.feedId)?.title || 'the feed');
  const rank = focus ? model.lines.indexOf(focus) + 1 : 0;
  const dealKey = `${scope.feedId}|${scope.feeder}|${scope.days}`;
  const [revealRef, reveal] = useReveal<HTMLDivElement>();
  const { hover, pinned, setHover, setPinned } = usePick<number>(dealKey);
  // a feeder lit from the lists (hover on a laptop, a tap on a phone)
  const [lit, setLit] = useState<{ deal: string; handle: string | null; pinned: string | null }>({ deal: dealKey, handle: null, pinned: null });
  const hot = lit.deal === dealKey ? lit.handle ?? lit.pinned : null;
  const index = hover ?? pinned;
  const solo = model.lines.length === 1;
  const lead = focus ?? (solo ? model.lines[0] : null);
  const headline = lead ? lead.pct : model.pct;
  const sub = !model.lines.length
    ? 'No follower readings in this window yet'
    : lead
      ? `${signed(Math.round(lead.delta))} followers${focus && !solo ? ` · ${rank} of ${model.lines.length} in ${feedName} · its average feeder ${percent(model.pct)}` : ''}`
      : `${signed(Math.round(model.net))} followers in all · ${model.grew} of ${model.lines.length} grew${model.shrank ? `, ${model.shrank} shrank` : ''}`;

  return (
    <section ref={nearRef} aria-labelledby="lead-growth-title" className="lead-band relative isolate overflow-hidden border-t border-fg/[0.09] bg-(--ld-panel) px-4 py-5 sm:px-6 lg:px-7 lg:py-6">
      <div ref={revealRef} data-reveal={reveal}>
        <div className="lead-rise flex flex-col gap-2 sm:flex-row sm:items-end sm:justify-between sm:gap-6" style={{ '--rise': 0 } as CSSProperties}>
          <div className="min-w-0">
            <Eyebrow icon={<Users className="h-4 w-4" />}>Followers · {scope.days}D</Eyebrow>
            <h2 id="lead-growth-title" className="mt-2 overflow-hidden whitespace-nowrap text-[20px] font-black leading-[1.1] tracking-[-0.035em] text-fg sm:text-[22px]"><Swap value={lead ? `How @${lead.handle} is growing.` : 'Who’s growing.'} className="truncate align-top" /></h2>
          </div>
          <div className="sm:text-right">
            <div className="flex items-baseline gap-2 sm:justify-end">
              <SlotText value={percent(headline)} className={cn('fm-depth-title text-[48px] font-black tabular-nums leading-[0.8] tracking-[-0.055em] lg:text-[56px] laptop:text-[44px]', (headline ?? 0) >= 2 ? 'text-[var(--fm-accent-text)]' : 'text-fg')} />
              <span className={cn(MARK, DIM)}><Swap value={lead ? `@${lead.handle}` : 'the average feeder'} /></span>
            </div>
            <div className={cn('mt-2 h-4 overflow-x-clip whitespace-nowrap', NOTE, SOFT)}>
              <Swap value={sub} />
            </div>
          </div>
        </div>

        {/* on a laptop the chart keeps a chart's proportions: the lists stand beside it instead of under it */}
        {/* one height whatever it shows (a feed, a feeder, any range): the page under it never moves on a switch */}
        <div className={cn('mt-4 grid gap-4', !solo && model.lines.length ? 'lg:h-[540px] lg:grid-cols-[minmax(0,1.65fr)_minmax(300px,1fr)] lg:items-stretch lg:gap-6' : '')}>
        <div className="lead-rise flex min-w-0 flex-col overflow-hidden rounded-[22px] border border-fg/[0.08] bg-(--ld-raised)" style={{ '--rise': 1 } as CSSProperties}>
          <Readout model={model} focus={focus} feedName={feedName} index={index} hot={hot} />
          <FollowersStage model={model} focus={focus} index={index} hot={hot} onHover={setHover} onPin={setPinned} />
          <Legend solo={solo} focus={focus} feedName={feedName} events={focus ? focus.spikes.length > 0 : model.events.length > 0} />
        </div>

        {!solo && model.lines.length ? (
          <div className="lead-rise flex h-[540px] min-h-0 min-w-0 flex-col sm:h-[310px] lg:h-full" style={{ '--rise': 2 } as CSSProperties}>
            {focus ? (
              <Spikes
                line={focus}
                keys={model.keys}
                deal={dealKey}
                days={scope.days}
                index={index}
                onHover={setHover}
                onPin={(at) => setPinned((current) => (current === at ? null : at))}
              />
            ) : <Movers
              model={model}
              deal={dealKey}
              days={scope.days}
              all={scope.feedId === 'all'}
              hot={hot}
              onHover={(handle) => setLit((current) => ({ deal: dealKey, handle, pinned: current.deal === dealKey ? current.pinned : null }))}
              onPin={(handle) => setLit((current) => ({ deal: dealKey, handle: null, pinned: current.deal === dealKey && current.pinned === handle ? null : handle }))}
            />}
          </div>
        ) : null}
        </div>
      </div>
    </section>
  );
}

export default memo(LeadFollowers);

/* the followers a day added across the lines (each line's move that day, in followers of its own size) */
function dayChange(model: FollowersModel, at: number, only: FollowerLine | null) {
  if (at < 1) return null;
  let total = 0;
  let any = false;
  for (const line of only ? [only] : model.lines) {
    const now = line.growth[at];
    const was = line.growth[at - 1];
    if (now == null || was == null) continue;
    total += ((now - was) / 100) * line.base;
    any = true;
  }
  return any ? Math.round(total) : null;
}

/* The day picked (or the latest reading): its follower change leads, big; the growth so far beside it; under them,
   who moved that day (a feeder's best day says so, with its own post) */
function Readout({ model, focus, feedName, index, hot }: { model: FollowersModel; focus: FollowerLine | null; feedName: string; index: number | null; hot: string | null }) {
  const at = index ?? model.lastIndex;
  const key = model.keys[at];
  const change = dayChange(model, at, focus);
  const event = model.events.find((one) => one.index === at && (!focus || one.handle === focus.handle)) ?? null;
  const hotLine = model.lines.find((line) => line.handle === hot) ?? null;
  let who = '';
  const spike = focus?.spikes.find((one) => one.index === at) ?? null;
  if (focus && spike) {
    who = `@${focus.handle} ${signed(Math.round(spike.delta))}${spike.multiple != null ? `, ${times(spike.multiple)} its usual day` : spike.delta < 0 ? ', one of its biggest drops' : ''}${spike.after ? `, ${spikeNote(spike)}` : ''}`;
  } else if (event) {
    const post = event.after?.ranked;
    who = `@${event.handle} ${signed(Math.round(event.delta))}, ${times(event.multiple)} its usual day${event.after ? `, after its ${event.after.format ?? 'post'} ${event.sameDay ? 'that day' : 'the day before'}${post?.latestPercentile != null ? ` (Top ${topLabel(post.latestPercentile)}%)` : ''}` : ''}`;
  } else if (at > 0 && model.lines.length > 1) {
    let best: { line: FollowerLine; gain: number } | null = null;
    for (const line of model.lines) {
      const now = line.growth[at];
      const was = line.growth[at - 1];
      if (now == null || was == null) continue;
      const gain = ((now - was) / 100) * line.base;
      if (!best || gain > best.gain) best = { line, gain };
    }
    if (best && best.gain >= 1) who = `${focus ? `${feedName}’s biggest move` : 'Biggest move'}: @${best.line.handle} ${signed(Math.round(best.gain))}`;
  }
  if (hotLine && hotLine.growth[at] != null) who = `@${hotLine.handle} ${percent(hotLine.growth[at])} since the window began${who ? ` · ${who}` : ''}`;
  if (!model.lines.length) {
    return <div className={cn('flex h-[42px] items-center px-4 sm:px-5', NOTE, DIM)}>No follower readings yet</div>;
  }
  return (
    <div className="px-4 pb-1 pt-3.5 sm:px-5">
      <div className="flex items-start justify-between gap-4">
        <div className="min-w-0">
          <div className={cn('h-4 overflow-hidden whitespace-nowrap', NOTE, DIM)}>
            <Swap value={`${index == null ? 'Latest reading' : 'Picked'} · ${dayLabel(key)}`} />
          </div>
          <div className="mt-1.5 flex items-baseline gap-2 whitespace-nowrap">
            <SlotText
              value={change == null ? '—' : signed(change)}
              className={cn('fm-depth-title text-[34px] font-black tabular-nums leading-none tracking-[-0.05em] sm:text-[40px]', event || (change != null && change > 0 && change >= Math.abs(model.net) / Math.max(4, model.keys.length) * 2) ? 'text-[var(--fm-accent-text)]' : change != null && change < 0 ? 'text-fg/60' : 'text-fg')}
            />
            <span className={cn(MARK, DIM)}>{focus ? `@${focus.handle} that day` : 'followers that day'}</span>
          </div>
        </div>
        <div className="shrink-0 text-right">
          <div className={cn('h-4', MARK, 'leading-4', DIM)}>{focus ? 'So far' : model.lines.length > 1 ? 'Average so far' : 'So far'}</div>
          <SlotText value={percent(focus ? focus.growth[at] : model.lines.length > 1 ? model.average[at] : model.lines[0].growth[at])} className={cn('fm-depth-title mt-1.5 block text-[22px] font-black tabular-nums leading-none tracking-[-0.04em] sm:text-[24px]', focus ? 'text-[var(--fm-accent-text)]' : 'text-fg')} />
          <div className={cn('mt-1 h-4 whitespace-nowrap', NOTE, DIM)}>
            {focus && model.lines.length > 1 ? <>average feeder <b className="font-black text-fg">{percent(model.average[at])}</b></> : `since ${shortDay(model.keys[0])}`}
          </div>
        </div>
      </div>
      <div className={cn('mt-2 h-4 overflow-hidden whitespace-nowrap', NOTE, SOFT)}>
        <Swap value={who || (index == null ? 'Drag across the chart for any day' : 'No single big move that day')} className="truncate" />
      </div>
    </div>
  );
}

function Legend({ solo, focus, feedName, events }: { solo: boolean; focus: FollowerLine | null; feedName: string; events: boolean }) {
  return (
    <div className={cn('flex h-[42px] shrink-0 items-center gap-x-4 overflow-x-auto whitespace-nowrap border-t border-fg/[0.07] px-4 [scrollbar-width:none] sm:px-5 [&::-webkit-scrollbar]:hidden', NOTE, DIM)}>
      {focus && !solo ? <span className="flex items-center gap-1.5"><i className="block h-0 w-4 rounded-full border-t-[2.5px] border-[var(--fm-accent)]" />@{focus.handle}</span> : null}
      <span className="flex items-center gap-1.5"><i className="block h-0 w-4 rounded-full border-t-[2.5px] border-fg/90" />{solo ? 'Followers' : focus ? `${feedName}’s average feeder` : 'Average feeder'}</span>
      {solo || focus ? null : <span className="flex items-center gap-1.5"><i className="block h-0 w-4 rounded-full border-t-2 border-[var(--fm-accent)]" />Fastest grower</span>}
      {solo ? null : <span className="flex items-center gap-1.5"><i className="block h-0 w-4 rounded-full border-t-[1.5px] border-fg/25" />{focus ? `Each feeder in ${feedName}` : 'Each feeder'}</span>}
      {events ? <span className="flex items-center gap-1.5"><i className="block h-2 w-2 rounded-full bg-[var(--fm-accent)]" />{focus ? 'Its biggest days, numbered as in the list' : 'A feeder’s best day, × its usual'}</span> : null}
    </div>
  );
}

/* ── the gainers and the slowest (or the losers), side by side ─────────── */

function Movers({ model, deal, days, all, hot, onHover, onPin }: { model: FollowersModel; deal: string; days: number; all: boolean; hot: string | null; onHover: (handle: string | null) => void; onPin: (handle: string) => void }) {
  const n = model.lines.length;
  const k = Math.min(4, Math.max(1, Math.floor(n / 2)));
  const ups = model.lines.slice(0, k);
  const downs = model.lines.slice(-k).reverse();
  const lost = downs.every((line) => line.delta < 0);
  const column = (title: string, note: string, lines: FollowerLine[], side: string) => (
    <div className="flex min-h-0 min-w-0 flex-col">
      <div className="flex items-baseline justify-between gap-3 px-1 pb-2">
        <h3 className="text-[15px] font-black tracking-[-0.02em] text-fg">{title}</h3>
        <span className={cn(NOTE, DIM)}>{note}</span>
      </div>
      <Ledger ids={lines.map((line) => `${side}:${line.feedId}:${line.handle}`)} deal={`${side}|${deal}`} className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-[18px] border border-fg/[0.07]">
        {lines.map((line, order) => (
          <button
            key={`${side}:${line.feedId}:${line.handle}`}
            data-ledger={`${side}:${line.feedId}:${line.handle}`}
            type="button"
            onPointerEnter={(event) => { if (event.pointerType === 'mouse') onHover(line.handle); }}
            onPointerLeave={(event) => { if (event.pointerType === 'mouse') onHover(null); }}
            onClick={() => onPin(line.handle)}
            aria-pressed={hot === line.handle}
            className={cn(
              'relative grid min-h-0 w-full flex-1 grid-cols-[18px_32px_minmax(0,1fr)_auto] items-center gap-3 border-b border-fg/[0.07] px-3.5 py-1.5 text-left transition-colors duration-[calc(240ms*var(--fm-tempo))] last:border-b-0 sm:px-4',
              hot === line.handle ? 'bg-(--ld-raised)' : 'bg-(--ld-panel) hover:bg-(--ld-raised)',
            )}
          >
            <span className={cn('text-[12px] font-black tabular-nums', DIM)}>{order + 1}</span>
            <span className="relative isolate block h-8 w-8 rounded-full"><FeederStoryAvatar feeder={{ handle: line.handle, profilePicUrl: line.pic }} className="text-[10px]" /></span>
            <span className="min-w-0">
              <span className="block truncate text-[14px] font-black tracking-[-0.02em] text-fg">@{line.handle}</span>
              <span className={cn('mt-0.5 block truncate', NOTE, DIM)}>{all ? `${line.feedName} · ` : ''}{compact(line.end, 1)} followers</span>
            </span>
            <span className="text-right">
              <span className={cn('block text-[17px] font-black tabular-nums tracking-[-0.03em]', line.pct >= 2 ? 'text-[var(--fm-accent-text)]' : line.delta < 0 ? DIM : 'text-fg')}>
                <SlotText value={percent(line.pct)} delay={ledgerDelay(order)} />
              </span>
              <span className={cn('mt-0.5 block tabular-nums', NOTE, DIM)}>{signed(Math.round(line.delta))}</span>
            </span>
          </button>
        ))}
      </Ledger>
    </div>
  );
  return (
    <div className="grid h-full min-h-0 grid-rows-2 gap-4 sm:grid-cols-2 sm:grid-rows-1 sm:gap-6 lg:grid-cols-1 lg:grid-rows-2 lg:gap-5">
      {column('Gainers', `growth in ${days}D`, ups, 'up')}
      {lost ? column('Losers', 'fewer followers', downs, 'down') : column('Slowest', 'least growth', downs, 'slow')}
    </div>
  );
}

/* ── a feeder's biggest days, numbered as the chart marks them ─────────── */

function spikeNote(spike: FollowerSpike) {
  if (!spike.after) return 'no post that day or the day before';
  const percentile = spike.after.ranked?.latestPercentile;
  return `after its ${spike.after.format ?? 'post'} ${spike.sameDay ? 'that day' : 'the day before'}${percentile != null ? ` · Top ${topLabel(percentile)}%` : ''}`;
}

function Spikes({ line, keys, deal, days, index, onHover, onPin }: { line: FollowerLine; keys: string[]; deal: string; days: number; index: number | null; onHover: (index: number | null) => void; onPin: (index: number) => void }) {
  return (
    <div className="flex h-full min-h-0 min-w-0 flex-col">
      <div className="flex items-baseline justify-between gap-3 px-1 pb-2">
        <h3 className="text-[15px] font-black tracking-[-0.02em] text-fg">Biggest days</h3>
        <span className={cn('truncate', NOTE, DIM)}>@{line.handle} · {days}D</span>
      </div>
      {line.spikes.length ? (
        <Ledger ids={line.spikes.map((spike) => `spike:${spike.index}`)} deal={`spike|${deal}`} className="flex min-h-0 flex-1 flex-col overflow-hidden rounded-[18px] border border-fg/[0.07]">
          {line.spikes.map((spike, order) => {
            const up = spike.delta > 0;
            const picked = index === spike.index;
            return (
              <button
                key={`spike:${spike.index}`}
                data-ledger={`spike:${spike.index}`}
                type="button"
                onPointerEnter={(event) => { if (event.pointerType === 'mouse') onHover(spike.index); }}
                onPointerLeave={(event) => { if (event.pointerType === 'mouse') onHover(null); }}
                onClick={() => onPin(spike.index)}
                aria-pressed={picked}
                className={cn(
                  'grid min-h-0 w-full flex-1 grid-cols-[22px_40px_minmax(0,1fr)_auto] items-center gap-3 border-b border-fg/[0.07] px-3.5 py-1.5 text-left transition-colors duration-[calc(240ms*var(--fm-tempo))] last:border-b-0 sm:px-4',
                  picked ? 'bg-(--ld-raised)' : 'bg-(--ld-panel) hover:bg-(--ld-raised)',
                )}
              >
                <span className={cn('grid h-[22px] w-[22px] place-items-center rounded-full text-[11px] font-black tabular-nums', up ? 'bg-[var(--fm-accent)] text-white' : 'bg-fg/75 text-(--ld-panel)')}>{order + 1}</span>
                <span className="relative block h-[50px] w-10 overflow-hidden rounded-[10px] bg-(--fm-media) shadow-(--ld-shadow-thumb)">
                  {spike.after ? <CrossfadeImage src={spike.after.thumb} /> : null}
                </span>
                <span className="min-w-0">
                  <span className="block truncate text-[14px] font-black tracking-[-0.02em] text-fg">{dayLabel(keys[spike.index])}</span>
                  <span className={cn('mt-0.5 block truncate', NOTE, DIM)}>{spikeNote(spike)}</span>
                </span>
                <span className="text-right">
                  <span className={cn('block text-[18px] font-black tabular-nums tracking-[-0.03em]', up ? 'text-[var(--fm-accent-text)]' : 'text-fg/60')}>
                    <SlotText value={signed(Math.round(spike.delta))} delay={ledgerDelay(order)} />
                  </span>
                  <span className={cn('mt-0.5 block whitespace-nowrap', NOTE, DIM)}>{spike.multiple != null ? `${times(spike.multiple)} its usual day` : up ? 'a gain' : 'a loss'}</span>
                </span>
              </button>
            );
          })}
        </Ledger>
      ) : (
        <div className={cn('grid flex-1 place-items-center rounded-[18px] border border-dashed border-fg/[0.12] p-6 text-center', NOTE, DIM)}>Not enough follower readings in this window yet</div>
      )}
    </div>
  );
}

/* ── the chart: one canvas ─────────────────────────────────────────────── */

type Curve = { x0: number; x1: number; ys: number[] };
type Scene = {
  n: number;
  lo: number;
  hi: number;
  lines: Array<{ id: string; handle: string; curve: Curve }>;
  // the line in rose: the fastest grower, or the feeder picked
  mark: string | null;
  markTag: string | null;
  focused: boolean;
  average: Curve | null;
  ticks: Array<{ at: number; label: string }>;
  // a feed's best days (× usual), or a feeder's biggest days as numbered badges (rose up, ink down)
  events: Array<{ handle: string; at: number; value: number; label: string; badge: boolean; up: boolean }>;
  averageTag: string | null;
};

/* a series with gaps as a curve from its first value to its last, resampled so any curve can bend into any other */
function curveOf(values: Array<number | null>): Curve | null {
  const first = values.findIndex((value) => value != null);
  if (first < 0) return null;
  let last = values.length - 1;
  while (last > first && values[last] == null) last -= 1;
  const span = Math.max(1, values.length - 1);
  return { x0: first / span, x1: last / span, ys: resample(values.slice(first, last + 1), SAMPLES) };
}

/* the dates under the chart: every day at 7D, Mondays at 30D, every other Monday beyond */
function ticksOf(keys: string[]) {
  const window = keys.slice(1);
  const n = window.length;
  const span = Math.max(1, keys.length - 1);
  if (n <= 8) return window.map((key, index) => ({ at: (index + 1) / span, label: shortDay(key).toUpperCase() }));
  const mondays = window.map((key, index) => ({ key, index })).filter(({ key }) => new Date(`${key}T12:00:00Z`).getUTCDay() === 1);
  const every = n <= 31 ? 1 : 2;
  return mondays.filter((_, order) => (mondays.length - 1 - order) % every === 0).map(({ key, index }) => ({ at: (index + 1) / span, label: shortDay(key).toUpperCase() }));
}

function sceneOf(model: FollowersModel, focus: FollowerLine | null): Scene {
  const values = [0, ...model.lines.flatMap((line) => line.growth.filter((value): value is number => value != null))];
  let lo = Math.min(...values);
  let hi = Math.max(...values);
  const range = Math.max(0.2, hi - lo);
  lo -= range * 0.06;
  hi += range * 0.12;
  const span = Math.max(1, model.keys.length - 1);
  const marked = model.lines.length > 1 ? focus ?? model.lines[0] : null;
  const lines = model.lines
    .map((line) => ({ id: `${line.feedId}:${line.handle}`, handle: line.handle, curve: curveOf(line.growth) }))
    .filter((line): line is { id: string; handle: string; curve: Curve } => line.curve != null);
  return {
    n: model.keys.length,
    lo,
    hi,
    lines,
    mark: marked ? `${marked.feedId}:${marked.handle}` : null,
    markTag: marked ? `@${marked.handle} ${percent(marked.pct)}` : null,
    focused: Boolean(focus),
    average: model.lines.length > 1 ? curveOf(model.average) : null,
    ticks: ticksOf(model.keys),
    events: focus
      ? focus.spikes.flatMap((spike, order) => {
        const value = focus.growth[spike.index];
        return value == null ? [] : [{ handle: focus.handle, at: spike.index / span, value, label: String(order + 1), badge: true, up: spike.delta > 0 }];
      })
      : model.events.flatMap((event) => {
        const line = model.lines.find((one) => one.handle === event.handle);
        const value = line?.growth[event.index];
        return value == null ? [] : [{ handle: event.handle, at: event.index / span, value, label: times(event.multiple), badge: false, up: true }];
      }),
    averageTag: model.lines.length > 1 ? `average ${percent(model.pct)}` : model.lines.length ? `@${model.lines[0].handle} ${percent(model.pct)}` : null,
  };
}

function layoutOf(width: number, height: number, scene: Pick<Scene, 'lo' | 'hi'>) {
  const top = 30;
  const bottom = 24;
  const plotW = width - PAD * 2;
  const plotH = height - top - bottom;
  return {
    top,
    plotH,
    x: (at: number) => PAD + at * plotW,
    y: (value: number) => top + ((scene.hi - value) / (scene.hi - scene.lo || 1)) * plotH,
  };
}

function niceStep(range: number, count: number) {
  const raw = range / count;
  const magnitude = Math.pow(10, Math.floor(Math.log10(raw)));
  const f = raw / magnitude;
  return (f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10) * magnitude;
}

function blend(from: Curve | null, to: Curve, t: number): Curve {
  if (!from) return to;
  return { x0: lerp(from.x0, to.x0, t), x1: lerp(from.x1, to.x1, t), ys: to.ys.map((value, k) => lerp(from.ys[k], value, t)) };
}

function strokeCurve(ctx: CanvasRenderingContext2D, curve: Curve, layout: ReturnType<typeof layoutOf>) {
  const points: Array<[number, number]> = curve.ys.map((value, k) => [layout.x(curve.x0 + ((curve.x1 - curve.x0) * k) / (SAMPLES - 1)), layout.y(value)]);
  ctx.beginPath();
  monotonePath(ctx, points);
  return points;
}

/* a label with a halo of the chart's ground, so a line under it never cuts it */
function tag(ctx: CanvasRenderingContext2D, palette: Palette, text: string, x: number, y: number, color: string, align: CanvasTextAlign, baseline: CanvasTextBaseline) {
  ctx.font = labelFont(palette, 11);
  ctx.textAlign = align;
  ctx.textBaseline = baseline;
  ctx.lineWidth = 4;
  ctx.lineJoin = 'round';
  ctx.strokeStyle = palette.raised;
  ctx.strokeText(text, x, y);
  ctx.fillStyle = color;
  ctx.fillText(text, x, y);
}

function FollowersStage({ model, focus, index, hot, onHover, onPin }: { model: FollowersModel; focus: FollowerLine | null; index: number | null; hot: string | null; onHover: (index: number | null) => void; onPin: (index: number | null) => void }) {
  const scene = useMemo(() => sceneOf(model, focus), [model, focus]);
  const count = model.keys.length;

  const draw = useCallback((frame: Frame, from: Scene | null, to: Scene, t: number) => {
    const { ctx, width, height, palette } = frame;
    const scale = { lo: from ? lerp(from.lo, to.lo, t) : to.lo, hi: from ? lerp(from.hi, to.hi, t) : to.hi };
    const layout = layoutOf(width, height, scale);
    const settled = !from || t >= 1;

    // the scale: a few round steps across, 0% (the window's start) dashed, labels at the right edge
    const step = niceStep(scale.hi - scale.lo, 4);
    ctx.lineWidth = 1;
    for (let value = Math.ceil(scale.lo / step) * step; value <= scale.hi; value += step) {
      const y = Math.round(layout.y(value)) + 0.5;
      const zero = Math.abs(value) < step / 1000;
      ctx.strokeStyle = ink(palette, zero ? 0.28 : 0.07);
      ctx.setLineDash(zero ? [4, 4] : []);
      ctx.beginPath();
      ctx.moveTo(PAD, y);
      ctx.lineTo(width - PAD, y);
      ctx.stroke();
      ctx.setLineDash([]);
      tag(ctx, palette, zero ? '0%' : percent(value, step < 1 ? 1 : 0), width - PAD, y - 3, labelInk(palette, zero ? 0.6 : 0.42), 'right', 'bottom');
    }
    // the dates
    ctx.font = labelFont(palette, 10);
    ctx.fillStyle = labelInk(palette, 0.48);
    ctx.textBaseline = 'alphabetic';
    for (const tick of to.ticks) {
      const x = layout.x(tick.at);
      ctx.textAlign = tick.at < 0.04 ? 'left' : tick.at > 0.96 ? 'right' : 'center';
      ctx.fillText(tick.label, x, height - 7);
    }

    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    const lit = hot ? to.lines.find((line) => line.handle === hot) ?? null : null;
    const fromLines = new Map((from?.lines ?? []).map((line) => [line.id, line]));
    const toIds = new Set(to.lines.map((line) => line.id));
    const curveNow = (line: Scene['lines'][number]) => blend(fromLines.get(line.id)?.curve ?? null, line.curve, t);
    const faint = lit ? 0.08 : to.focused ? 0.11 : 0.15;
    // every feeder, faint (lines that come or go fade on the tween's clock)
    for (const line of to.lines) {
      if (line === lit) continue;
      ctx.globalAlpha = from && !fromLines.has(line.id) ? t : 1;
      strokeCurve(ctx, curveNow(line), layout);
      ctx.strokeStyle = ink(palette, to.lines.length > 1 ? faint : 0.9);
      ctx.lineWidth = to.lines.length > 1 ? 1.25 : 2.6;
      ctx.stroke();
    }
    for (const line of from?.lines ?? []) {
      if (toIds.has(line.id)) continue;
      ctx.globalAlpha = 1 - t;
      strokeCurve(ctx, line.curve, layout);
      ctx.strokeStyle = ink(palette, 0.15);
      ctx.lineWidth = 1.25;
      ctx.stroke();
    }
    ctx.globalAlpha = 1;
    // the average feeder: its area down to the start
    if (to.average) {
      const points = strokeCurve(ctx, blend(from?.average ?? null, to.average, t), layout);
      const zeroY = layout.y(0);
      ctx.lineTo(points[points.length - 1][0], zeroY);
      ctx.lineTo(points[0][0], zeroY);
      ctx.closePath();
      const fill = ctx.createLinearGradient(0, layout.top, 0, layout.top + layout.plotH);
      fill.addColorStop(0, ink(palette, 0.1));
      fill.addColorStop(1, ink(palette, 0));
      ctx.fillStyle = fill;
      ctx.fill();
    }
    // the rose line passes from the one marked before to the one marked now on the tween's clock
    const markFrom = from ? from.mark : to.mark;
    const marking = (id: string | null, alpha: number, width: number) => {
      const line = id ? to.lines.find((one) => one.id === id) : null;
      if (!line || alpha <= 0.01 || line === lit) return;
      ctx.globalAlpha = alpha;
      strokeCurve(ctx, curveNow(line), layout);
      ctx.strokeStyle = palette.accent;
      ctx.lineWidth = width;
      ctx.stroke();
      ctx.globalAlpha = 1;
    };
    const markWidth = to.focused ? 2.75 : 2;
    if (markFrom !== to.mark) marking(markFrom, 1 - t, markWidth);
    marking(to.mark, markFrom !== to.mark ? t : 1, markWidth);
    if (to.average) {
      strokeCurve(ctx, blend(from?.average ?? null, to.average, t), layout);
      ctx.strokeStyle = ink(palette, lit ? 0.5 : 0.92);
      ctx.lineWidth = 2.6;
      ctx.stroke();
    }
    if (lit) {
      strokeCurve(ctx, curveNow(lit), layout);
      ctx.strokeStyle = lit.id === to.mark ? palette.accent : ink(palette, 0.95);
      ctx.lineWidth = 2.6;
      ctx.stroke();
    }

    if (settled) {
      // the ends named: the rose line above, the average below
      const endOf = (curve: Curve) => [layout.x(curve.x1), layout.y(curve.ys[curve.ys.length - 1])] as const;
      const marked = to.mark ? to.lines.find((line) => line.id === to.mark) : null;
      if (marked && to.markTag) {
        const [x, y] = endOf(marked.curve);
        tag(ctx, palette, to.markTag, x - 6, y < layout.top + 16 ? y + 8 : y - 8, palette.accent, 'right', y < layout.top + 16 ? 'top' : 'bottom');
        ctx.beginPath();
        ctx.arc(x, y, 3.5, 0, Math.PI * 2);
        ctx.fillStyle = palette.accent;
        ctx.fill();
      }
      const main = to.average ?? to.lines[0]?.curve ?? null;
      if (main && to.averageTag) {
        const [x, y] = endOf(main);
        tag(ctx, palette, to.averageTag, x - 6, y + 8, ink(palette, 0.9), 'right', 'top');
        ctx.beginPath();
        ctx.arc(x, y, 3.5, 0, Math.PI * 2);
        ctx.fillStyle = ink(palette, 0.92);
        ctx.fill();
      }
      // the feeders' best days
      for (const event of to.events) {
        const x = layout.x(event.at);
        const y = layout.y(event.value);
        if (event.badge) {
          ctx.beginPath();
          ctx.arc(x, y, 8.5, 0, Math.PI * 2);
          ctx.fillStyle = event.up ? palette.accent : ink(palette, 0.8);
          ctx.fill();
          ctx.lineWidth = 2;
          ctx.strokeStyle = palette.raised;
          ctx.stroke();
          ctx.font = labelFont(palette, 10);
          ctx.textAlign = 'center';
          ctx.textBaseline = 'middle';
          ctx.fillStyle = event.up ? '#ffffff' : palette.raised;
          ctx.fillText(event.label, x, y + 0.5);
          continue;
        }
        ctx.beginPath();
        ctx.arc(x, y, 5.5, 0, Math.PI * 2);
        ctx.fillStyle = palette.accent;
        ctx.fill();
        ctx.lineWidth = 2;
        ctx.strokeStyle = palette.raised;
        ctx.stroke();
        tag(ctx, palette, event.label, x, y - 9, palette.accent, 'center', 'bottom');
      }
      // the day picked
      if (index != null) {
        const span = Math.max(1, count - 1);
        const x = Math.round(layout.x(index / span)) + 0.5;
        ctx.strokeStyle = ink(palette, 0.3);
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.moveTo(x, layout.top - 6);
        ctx.lineTo(x, layout.top + layout.plotH);
        ctx.stroke();
        const dot = (value: number | null | undefined, color: string) => {
          if (value == null) return;
          ctx.beginPath();
          ctx.arc(x, layout.y(value), 4.5, 0, Math.PI * 2);
          ctx.fillStyle = color;
          ctx.fill();
          ctx.lineWidth = 2;
          ctx.strokeStyle = palette.raised;
          ctx.stroke();
        };
        dot(model.lines.length > 1 ? model.average[index] : model.lines[0]?.growth[index], ink(palette, 0.92));
        if (focus && model.lines.length > 1) dot(focus.growth[index], palette.accent);
        const hotLine = model.lines.find((line) => line.handle === hot);
        if (hotLine && hotLine !== focus) dot(hotLine.growth[index], !focus && hotLine === model.lines[0] && model.lines.length > 1 ? palette.accent : ink(palette, 0.95));
      }
    }
  }, [count, focus, hot, index, model]);

  const { canvasRef, redraw } = useCanvasScene(scene, draw);
  useEffect(() => {
    redraw();
  }, [index, hot, redraw]);

  // the columns sit PAD in from each side; a day is the column whose centre is nearest
  const scrub = useScrub(count, useCallback((picked: number | null, mode: 'hover' | 'pin') => (mode === 'hover' ? onHover(picked) : onPin(picked)), [onHover, onPin]), PAD);

  return (
    <div className="relative h-[250px] cursor-crosshair select-none sm:h-[290px] lg:h-auto lg:min-h-[300px] lg:flex-1" {...scrub}>
      <canvas ref={canvasRef} role="img" aria-label="Each feeder's follower growth across the window, and the average feeder's" className="absolute inset-0 h-full w-full" />
    </div>
  );
}
