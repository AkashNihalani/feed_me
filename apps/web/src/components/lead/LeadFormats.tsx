'use client';

/* On the menu — what the scope posts against what lands. One row a format (reels, carousels, images; always all three,
   always in that order, so a switch never moves a row). Each row holds two bars on one 0–100% scale: its share of
   the posts, and its share of the hits (the posts that cleared the bar: the top 10% when there are enough of those,
   else the top 25% or half). A format whose hits outrun its posts punches above its weight: its verdict (how many
   times its share it lands) and its hits bar go rose. Every number sits just past the end of its bar, in a gutter
   kept for it, so no share is ever too small to be labelled (the old split hid any segment under ~44px: most of them
   on a phone).

   Motion: the bars glide to their new lengths and their numbers ride their ends (transform, on the Feed's carry
   curve), row after row a beat apart; the numbers roll. */

import { CSSProperties, memo, useMemo } from 'react';
import { Clapperboard, Image as ImageIcon, Layers, Shapes } from 'lucide-react';
import SlotText from '@/components/SlotText';
import { ms } from '@/lib/tempo';
import { cn } from '@/lib/utils';
import type { LeadActivity } from '@/app/api/lead/activity/route';
import { CrossfadeImage, Delta, Eyebrow, Swap, topLabel, useHeldNear, useReveal } from './leadKit';
import { FORMAT_LABEL, formatsFor, type Format, type FormatRead, type LeadFeed } from './leadModels';

const ICON: Record<Format, typeof Clapperboard> = { reel: Clapperboard, carousel: Layers, image: ImageIcon };
// the formats told apart by ink strength (the app is mono: rose only for what lands above its share)
const INK: Record<Format, number> = { reel: 0.86, carousel: 0.56, image: 0.32 };
// a row's beat after the one above it
const ROW_BEAT_MS = ms(45);

type Props = { feeds: LeadFeed[]; activity: LeadActivity | null; feedId: string; feeder: string; days: number };

const pct = (value: number) => `${Math.round(value * 100)}%`;
const punch = (read: FormatRead) => (read.share > 0 && read.hits > 0 ? read.hitShare / read.share : null);

function formatEr(value: number | null) {
  if (value == null || !Number.isFinite(value)) return '—';
  const percent = Math.abs(value) <= 1 ? value * 100 : value;
  return `${percent.toFixed(percent >= 10 ? 1 : 2).replace(/\.0+$/, '').replace(/(\.\d*[1-9])0+$/, '$1')}%`;
}

function LeadFormats(props: Props) {
  const live = useMemo(() => ({ feeds: props.feeds, activity: props.activity, feedId: props.feedId, feeder: props.feeder, days: props.days }), [props.feeds, props.activity, props.feedId, props.feeder, props.days]);
  const [nearRef, scope] = useHeldNear(live);
  const model = useMemo(() => formatsFor(scope), [scope]);
  const [revealRef, reveal] = useReveal<HTMLDivElement>();
  // the format that lands furthest past its share leads the sentence
  const star = useMemo(() => [...model.reads].filter((read) => read.count >= 3).sort((a, b) => (punch(b) ?? 0) - (punch(a) ?? 0))[0] || null, [model.reads]);
  const barName = model.bar === 10 ? 'the top 10%' : model.bar === 25 ? 'the top 25%' : 'the top half';
  const hitsName = model.bar === 10 ? 'Top 10%' : model.bar === 25 ? 'Top 25%' : 'Top half';

  return (
    <section ref={nearRef} aria-labelledby="lead-formats-title" className="lead-band relative isolate overflow-hidden border-t border-fg/[0.09] bg-(--ld-panel) px-4 py-5 sm:px-6 lg:px-7 lg:py-6">
      <div ref={revealRef} data-reveal={reveal}>
        <div className="lead-rise" style={{ '--rise': 0 } as CSSProperties}>
          <Eyebrow icon={<Shapes className="h-4 w-4" />}>On the menu · {scope.days}D</Eyebrow>
          <h2 id="lead-formats-title" className="mt-2 text-[20px] font-black leading-none tracking-[-0.035em] text-fg sm:text-[22px]">What they post vs what lands.</h2>
          <p className="mt-2.5 h-[40px] max-w-[46rem] overflow-x-clip text-[13px] font-bold leading-[1.45] text-fg/60 light:text-fg/66 sm:h-[20px]">
            {star && model.hits ? (
              <Swap value={`${FORMAT_LABEL[star.format]}: ${pct(star.share)} of posts, ${pct(star.hitShare)} of ${barName}.`} />
            ) : (
              <Swap value={`Nothing has reached ${barName} in this window yet.`} />
            )}
          </p>
        </div>

        {/* what the bars are shares of */}
        <div className="lead-rise mt-4 flex h-[14px] items-center gap-4 overflow-x-clip whitespace-nowrap text-[9px] font-black uppercase tracking-[0.14em] text-fg/44 light:text-fg/54" style={{ '--rise': 1 } as CSSProperties}>
          <span><Swap value={`Posts · ${model.count}`} /></span>
          <span className="text-fg/20">·</span>
          <span><Swap value={`${hitsName} · ${model.hits}`} /></span>
        </div>

        <ul className="lead-rise mt-3 border-t border-fg/[0.08]" style={{ '--rise': 2 } as CSSProperties}>
          {model.reads.map((read, index) => (
            <MenuRow key={read.format} read={read} lead={read.format === model.lead} hitsName={hitsName} beat={index * ROW_BEAT_MS} />
          ))}
        </ul>
      </div>
    </section>
  );
}

export default memo(LeadFormats);

/* A share as a bar on the row's 0–100% scale, its number just past its end (with its move in points against the
   window before, when it has one). The bar is a full-width fill scaled to its share and the number rides a full-width
   carrier moved by the same share, both transforms; the track keeps a gutter on its right, so a share near 100% still
   has room for its number. */
function ShareBar({ label, share, fill, beat, strong = false, move = null }: { label: string; share: number; fill: string; beat: number; strong?: boolean; move?: number | null }) {
  const at = Math.max(0, Math.min(1, share));
  const glide = { transitionDelay: `${beat}ms` } as CSSProperties;
  return (
    <div className="flex h-[20px] items-center gap-3">
      <span className="w-[54px] shrink-0 truncate text-[8px] font-black uppercase tracking-[0.12em] text-fg/44 light:text-fg/54">{label}</span>
      <div className="relative mr-[64px] h-[10px] min-w-0 flex-1">
        <span aria-hidden="true" className="absolute inset-0 rounded-[3px] bg-fg/[0.06]" />
        <span aria-hidden="true" className="lead-glide absolute inset-0 origin-left rounded-[3px]" style={{ ...glide, background: fill, transform: `scaleX(${at.toFixed(4)})` }} />
        <span aria-hidden="true" className="lead-glide pointer-events-none absolute inset-0" style={{ ...glide, transform: `translateX(${(at * 100).toFixed(2)}%)` }}>
          <span className="absolute left-0 top-1/2 flex -translate-y-1/2 items-baseline gap-1 whitespace-nowrap pl-2 leading-none">
            <span className={cn('text-[12px] font-black tabular-nums', strong ? 'text-[var(--fm-accent-text)]' : 'text-fg/80')}><SlotText value={pct(at)} delay={beat} /></span>
            {move ? <span className="text-[9px] font-black tabular-nums text-fg/40 light:text-fg/50">{move > 0 ? '▲' : '▼'}{Math.abs(move)}</span> : null}
          </span>
        </span>
      </div>
    </div>
  );
}

function MenuRow({ read, lead, hitsName, beat }: { read: FormatRead; lead: boolean; hitsName: string; beat: number }) {
  const Icon = ICON[read.format];
  // judged on the number the row prints, so a 1.1× never says it matches
  const raw = punch(read);
  const times = raw == null ? null : Math.round(raw * 10) / 10;
  const above = times != null && times > 1;
  const below = times != null && times < 1;
  const shareMove = read.prior ? Math.round((read.share - read.priorShare) * 100) : null;
  const typicalMove = read.typical != null && read.priorTypical != null ? Math.round(read.priorTypical - read.typical) : null;
  const bestPercentile = read.best?.ranked?.latestPercentile ?? null;
  const verdict = times == null ? 'no hits yet' : above ? 'above its share' : below ? 'below its share' : 'matches its share';
  const ink = `rgb(var(--fm-fg-rgb) / ${INK[read.format]})`;

  return (
    <li className="grid grid-cols-[minmax(0,1fr)_auto] gap-x-4 gap-y-3 border-b border-fg/[0.08] py-4 last:border-b-0 lg:grid-cols-[230px_minmax(0,1fr)_150px] lg:items-center lg:gap-x-8">
      {/* the format, its best post and its count */}
      <div className="flex min-w-0 items-center gap-3">
        <span className="relative block h-[60px] w-[48px] shrink-0 overflow-hidden rounded-[10px] bg-(--fm-media)">
          <span className="absolute inset-0 grid place-items-center"><Icon className="h-4 w-4 text-white/24" /></span>
          <CrossfadeImage src={read.best?.thumb || null} />
          {bestPercentile != null ? <span className="absolute inset-x-0 bottom-0 whitespace-nowrap bg-[linear-gradient(180deg,transparent,rgba(0,0,0,.8))] px-1 pb-0.5 pt-2 text-center text-[8px] font-black text-white">Top {topLabel(bestPercentile)}%</span> : null}
        </span>
        <div className="min-w-0">
          <div className="flex h-[22px] items-center gap-2 overflow-x-clip whitespace-nowrap">
            <span className="hidden h-5 w-5 shrink-0 place-items-center rounded-full sm:grid" style={{ background: ink }}>
              <Icon className={cn('h-3 w-3', INK[read.format] > 0.45 ? 'text-(--ld-panel)' : 'text-fg/80')} />
            </span>
            <span className="text-[15px] font-black tracking-[-0.02em] text-fg">{FORMAT_LABEL[read.format]}</span>
            {lead ? <span className="rounded-full bg-[var(--fm-accent)] px-2 py-0.5 text-[7px] font-black uppercase tracking-[0.14em] text-white">lands best</span> : null}
          </div>
          <div className="mt-1 h-[14px] overflow-x-clip whitespace-nowrap text-[10px] font-bold text-fg/48 light:text-fg/56">
            <Swap value={`${read.count} ${read.count === 1 ? 'post' : 'posts'} · ${read.hits} ${read.hits === 1 ? 'hit' : 'hits'}`} />
          </div>
        </div>
      </div>

      {/* the verdict: how many times its share of the posts it lands in the hits */}
      <div className="text-right lg:col-start-3 lg:row-start-1">
        <div className={cn('fm-depth-title text-[26px] font-black tabular-nums leading-none tracking-[-0.04em]', above ? 'text-[var(--fm-accent-text)]' : 'text-fg')}>
          <SlotText value={times == null ? '—' : `${times.toFixed(1)}×`} delay={beat} />
        </div>
        <div className="mt-1.5 h-[12px] overflow-x-clip whitespace-nowrap text-[9px] font-black uppercase tracking-[0.1em] text-fg/44 light:text-fg/54"><Swap value={verdict} /></div>
        {/* where its usual post ranks, and which way that went against the window before */}
        <div className="mt-1 flex h-[12px] items-center justify-end gap-1.5 overflow-x-clip whitespace-nowrap text-[10px] font-bold text-fg/48 light:text-fg/56">
          <Swap value={read.typical == null ? `ER ${formatEr(read.er)}` : `usually top ${topLabel(read.typical)}%`} />
          {typicalMove ? <Delta up={typicalMove > 0}><Swap value={String(Math.abs(typicalMove))} /></Delta> : null}
        </div>
      </div>

      {/* its share of the posts and of the hits, on one scale */}
      <div className="col-span-2 flex flex-col gap-1 lg:col-span-1 lg:col-start-2 lg:row-start-1">
        <ShareBar label="Posts" share={read.share} move={shareMove} fill={ink} beat={beat} />
        <ShareBar label={hitsName} share={read.hitShare} fill={above ? 'var(--fm-accent)' : ink} beat={beat} strong={above} />
      </div>
    </li>
  );
}
