'use client';

/* ─────────────────────────────────────────────────────────────
   FEEDER READER — the run hero.
   Ultra big, ultra minimal. The number dominates; crimson is a punch,
   never a wash. Light + dark via foreground/background tokens.

   "Alive" = continuous but never interaction-triggered: numbers roll in
   with the login Odometer, the round dot pulses. No hover/tap motion.
   Mock data for now — wired to the run-report API once the prompt ships.
   ───────────────────────────────────────────────────────────── */

import { cn } from '@/lib/utils';
import Odometer from '@/components/login/Odometer';

type Trend = 'up' | 'down' | 'flat';

type FeederEntry = { handle: string; avgRank: number; trend: Trend };

const ANCHOR = {
  handle: 'anuj',
  round: 6,
  roundTotal: 10,
  avgRank: 31,
  followerDelta: 1200,
  trend: 'down' as Trend,
  memoryNote: 'softest run in 6 weeks',
  read: 'Posting a lot, landing nothing — the comedy is fine, the aim is off.',
  door: 'status comedy × the character format',
};

const FIELD: FeederEntry[] = [
  { handle: 'kushacomedy', avgRank: 19, trend: 'up' },
  { handle: 'realhasan', avgRank: 28, trend: 'flat' },
  { handle: 'dostcalling', avgRank: 41, trend: 'down' },
];

const TREND_GLYPH: Record<Trend, string> = { up: '▲', down: '▼', flat: '—' };

function Label({ children }: { children: React.ReactNode }) {
  return (
    <span className="text-[10px] font-black uppercase tracking-[0.22em] text-foreground/38">{children}</span>
  );
}

/* a single ultra-big number, e.g. AVG 31%. The size class sits on the wrapper so
   the Odometer inherits it and the % stays proportional (em is relative here). */
function BigStat({ value, size }: { value: number; size: string }) {
  return (
    <span className={cn('inline-flex items-baseline font-black leading-none text-foreground', size)}>
      <Odometer value={value} animateOnMount className="inline-flex" />
      <span className="ml-[0.06em] text-[0.36em] text-[#E11D48]">%</span>
    </span>
  );
}

export default function FeederReader() {
  return (
    <section className="w-full text-foreground" data-feeder-reader="true">
      {/* header */}
      <div className="mb-8 flex items-center justify-between">
        <div className="flex items-center gap-2.5">
          <span className="h-2 w-2 rounded-full bg-[#E11D48] fm-live-dot" />
          <span className="text-[11px] font-black uppercase tracking-[0.28em] text-foreground/50">FeederReader</span>
        </div>
        <span className="text-[11px] font-black uppercase tracking-[0.2em] text-foreground/40">
          Round {ANCHOR.round}<span className="text-foreground/25">/{ANCHOR.roundTotal}</span>
        </span>
      </div>

      {/* anchor identity */}
      <div className="flex items-baseline justify-between gap-4">
        <h2 className="text-[28px] font-black leading-none tracking-tight sm:text-[34px]">@{ANCHOR.handle}</h2>
        <span className="text-[11px] font-black uppercase tracking-[0.18em] text-[#E11D48]">Anchor</span>
      </div>

      {/* the number — ultra big */}
      <div className="mt-8">
        <div className="flex items-baseline gap-3">
          <span className="text-[13px] font-black uppercase tracking-[0.3em] text-foreground/40">Avg</span>
          <span className={cn('text-[12px] font-black uppercase tracking-[0.18em]', ANCHOR.trend === 'down' ? 'text-[#E11D48]' : 'text-foreground/45')}>
            {TREND_GLYPH[ANCHOR.trend]} {ANCHOR.trend}
          </span>
        </div>
        <BigStat value={ANCHOR.avgRank} size="text-[clamp(76px,20vw,148px)]" />
        <div className="mt-3 flex flex-wrap items-center gap-x-4 gap-y-1">
          <Label>Average this round</Label>
          <span className="text-[11px] font-black uppercase tracking-[0.16em] text-foreground/40">{ANCHOR.memoryNote}</span>
          <span className="text-[11px] font-black uppercase tracking-[0.16em] text-foreground/40">
            <span className="text-foreground/65">+{(ANCHOR.followerDelta / 1000).toFixed(1)}k</span> followers
          </span>
        </div>
      </div>

      {/* the read */}
      <p className="mt-10 max-w-[680px] text-[19px] font-black leading-[1.28] tracking-tight sm:text-[23px]">
        {ANCHOR.read}
      </p>

      {/* the door — the one highlight */}
      <div className="mt-6 flex items-baseline gap-3">
        <span className="text-[22px] font-black leading-none text-[#E11D48]">↳</span>
        <div>
          <Label>Open door</Label>
          <div className="mt-0.5 text-[17px] font-black leading-snug tracking-tight sm:text-[19px]">{ANCHOR.door}</div>
        </div>
      </div>

      {/* the field — ultra minimal standings */}
      <div className="mt-14">
        <div className="mb-1 border-t border-foreground/12 pt-5">
          <Label>The field · chasing @{ANCHOR.handle}</Label>
        </div>
        <div className="divide-y divide-foreground/8">
          {FIELD.map((entry) => (
            <div key={entry.handle} className="flex items-center justify-between py-5">
              <span className="text-[16px] font-black tracking-tight sm:text-[18px]">@{entry.handle}</span>
              <div className="flex items-baseline gap-3">
                <span className={cn('text-[12px] font-black', entry.trend === 'down' ? 'text-[#E11D48]' : entry.trend === 'up' ? 'text-foreground' : 'text-foreground/35')}>
                  {TREND_GLYPH[entry.trend]}
                </span>
                <span className="text-[11px] font-black uppercase tracking-[0.18em] text-foreground/35">Avg</span>
                <BigStat value={entry.avgRank} size="text-[34px] sm:text-[40px]" />
              </div>
            </div>
          ))}
        </div>
      </div>
    </section>
  );
}
