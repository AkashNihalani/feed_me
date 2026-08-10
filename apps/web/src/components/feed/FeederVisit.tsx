'use client';

/* ─────────────────────────────────────────────────────────────
   FEEDER VISIT — prototype of the weekly Feed Day drop.

   One controlled full-screen "visit" from the Feeder Reader: cold open →
   callback (settles a hunch placed on record) → one case beat (Cold Run
   Broken, with its staged performance) → the quiet stuff → gameplan +
   a new hunch → sign-off. Dummy data shaped like the future payload.

   Grammar:
   - each beat is one full screen; a beat never scrolls internally
   - performances play once on entry (transform/opacity only), then rest
   - the tier is invisible infrastructure: it sizes the cover wedge and
     sets the language temperature — it is never printed
   - the wedge is identity, not a gauge: fixed aperture everywhere except
     the cover composition
   ───────────────────────────────────────────────────────────── */

import Link from 'next/link';
import { createContext, useCallback, useContext, useEffect, useRef, useState } from 'react';
import {
  AnimatePresence,
  animate as animateMotion,
  motion,
  type MotionValue,
  useInView,
  useMotionValue,
  useReducedMotion,
  useTransform,
} from 'framer-motion';
import { cn } from '@/lib/utils';
import Odometer from '@/components/login/Odometer';

export const ACCENT = '#E11D48';

/* One clock for the whole visit — every fade/rise shares this ease, every
   pop shares this spring. Consistency beats variety. */
export const VISIT_EASE = [0.22, 1, 0.36, 1] as const;
export const VISIT_POP = { type: 'spring', stiffness: 420, damping: 22, mass: 0.9 } as const;

/* ── dummy payload (shape mirrors the future visit payload) ────────────── */

type Tier = 0 | 1 | 2 | 3; // didn't bite → bit hard. Drives composition + language temp only.

export type VisitCaseBeat = {
  kicker: string;
  hook: readonly string[];
  misses: number;
  rank: number;
  pool: number;
  rankNote: string;
  verdict: string;
  evidence: string;
  receipt: string;
  receiptUrl?: string;
  fullRead: readonly string[];
  packet: readonly { tag: string; rank: string; url?: string; breaker?: boolean }[];
  artifact?: 'ember' | 'pool';
  reader?: VisitReaderCase;
};

export type VisitReaderPost = {
  title: string;
  recentRank?: string;
  overallRank?: string;
};

export type VisitReaderHistory = {
  week: number;
  movement: string;
  title: string;
};

export type VisitReaderArtifact =
  | { kind: 'rank_span'; posts: readonly VisitReaderPost[] }
  | {
      kind: 'evidence_turnover';
      carried: readonly VisitReaderPost[];
      added: readonly VisitReaderPost[];
      dropped: readonly VisitReaderPost[];
    }
  | { kind: 'tenure'; history: readonly VisitReaderHistory[] }
  | { kind: 'anomaly_pair'; posts: readonly [VisitReaderPost, VisitReaderPost] }
  | { kind: 'evidence_stack'; posts: readonly VisitReaderPost[] };

export type VisitReaderCase = {
  biteId: string;
  movement: string;
  previousTitle?: string;
  history: readonly VisitReaderHistory[];
  currentRead: string;
  changedBecause: string;
  whyItMattersNow: string;
  evidenceRefs: readonly string[];
  counterevidenceRefs: readonly string[];
  reinterpretation?: {
    oldRead: string;
    newRead: string;
    evidenceRefs: readonly string[];
  };
  artifact: VisitReaderArtifact;
};

export type FeederVisitData = {
  handle: string;
  week: number;
  tier: Tier;
  cover: { headline: readonly string[]; sub: string; tagline?: string; summary?: string };
  runAverage?: number;
  runLabel?: string;
  callback: { placed: string; hunch: string; verdict: string; settle: string };
  caseBeat: VisitCaseBeat;
  caseBeats?: readonly VisitCaseBeat[];
  quiet: readonly { line: string; note?: string; postRefs?: readonly string[] }[];
  gameplan: readonly string[];
  worldWatch?: {
    rankPulse: string;
    signals: readonly string[];
    triggers: readonly { label: string; note: string; postRef: string }[];
  };
  newHunch: string;
  laneLabel?: string;
  closingState?: string;
  plate: { filled: number; total: number };
  shelf: readonly { week: number; line: string; when: string }[];
};

export const VISIT: FeederVisitData = {
  handle: 'anuj.mp4',
  week: 12,
  tier: 3 as Tier,
  cover: {
    headline: ['The dry', 'spell is', 'over.'],
    sub: 'Three things this week. One of them settles a bet.',
  },
  callback: {
    placed: 'two weeks back',
    hunch:
      'The demo era is done here. If anything breaks this slump, it will be a post that picks a fight — not one that shows a product.',
    verdict: 'Called it.',
    settle: 'The breaker was an argument. Not a demo in sight.',
  },
  caseBeat: {
    kicker: 'the break',
    hook: ['Seven misses.', 'Then this.'],
    misses: 7,
    rank: 4,
    pool: 62,
    rankNote: 'best debut in 6 weeks',
    verdict:
      'You finally gave them something to take sides on — and they argued about it in your comments instead of scrolling past.',
    evidence:
      'The courtroom bit pulled twice your usual comment share. The seven before it never left the floor.',
    receipt: 'the courtroom bit',
    // The case file: the full read + the contrast packet. This is where a
    // longer Sonnet read lives — the beat face never grows to fit it.
    fullRead: [
      'Seven posts, one shape: something gets built, something gets shown, and nobody is asked to pick a side. The floor never moved. Not one of them cracked the top half of the window, and three sat in the bottom quartile — quiet in the exact way polished things go quiet.',
      'Then the courtroom bit. Same office, same faces, different job: it staged a fight and made the audience the jury. The comments stopped being congratulations and started being verdicts — twice your usual comment share, and the likes followed the argument in.',
      'The lesson isn’t “do courtroom skits.” It’s that this audience shows up to adjudicate, not to admire. The demo posts asked for applause. The breaker asked for a ruling. That’s the switch that ended the run — and it’s repeatable.',
    ],
    packet: [
      { tag: 'gadget unbox #3', rank: '41/62' },
      { tag: 'desk setup tour', rank: '48/62' },
      { tag: 'app speedrun', rank: '39/62' },
      { tag: 'launch teaser', rank: '55/62' },
      { tag: 'macro monday', rank: '44/62' },
      { tag: 'studio reveal', rank: '51/62' },
      { tag: 'q&a stitch', rank: '37/62' },
      { tag: 'the courtroom bit', rank: '4/62', breaker: true },
    ] as { tag: string; rank: string; breaker?: boolean }[],
  },
  quiet: [
    { line: 'The office-tour reel is still holding its place in the account’s recent memory.', note: 'observation' },
    { line: 'The account’s longer pause did not erase its social pressure.', note: 'observation' },
  ],
  gameplan: [
    'Argue more. The demo era is done.',
    'The courtroom bit gets a sequel this week — not next month.',
  ],
  newHunch:
    'If the carousel lane gets three more tries and none crack the top quartile, I’m calling it dead.',
  plate: { filled: 3, total: 5 },
  shelf: [
    { week: 11, line: 'Quiet week. Floor held.', when: 'jun 29' },
    { week: 10, line: 'Collab carried the week.', when: 'jun 22' },
    { week: 9, line: 'Comments found a lane.', when: 'jun 15' },
    { week: 8, line: 'Duty posts, duty numbers.', when: 'jun 8' },
  ],
};

const VisitContext = createContext<FeederVisitData>(VISIT);
const useVisit = () => useContext(VisitContext);

/* Cover composition per tier — the only place the wedge carries meaning.
   The wedge always faces the headline (mouth toward the text), so the home
   cover's small wedge can morph straight into this one with no rotation. */
export const READER_WEDGE_LAYOUT_ID = 'reader-wedge';
const COVER_WEDGE: Record<Tier, { size: string; aperture: number; opacity: number }> = {
  3: { size: 'clamp(300px, 74vw, 660px)', aperture: 64, opacity: 1 },
  2: { size: 'clamp(220px, 52vw, 460px)', aperture: 44, opacity: 0.94 },
  1: { size: 'clamp(150px, 36vw, 300px)', aperture: 26, opacity: 0.62 },
  0: { size: 'clamp(110px, 26vw, 200px)', aperture: 12, opacity: 0.4 },
};

/* ── primitives ────────────────────────────────────────────────────────── */

export function wedgeGradient(aperture: number, color: string = ACCENT) {
  const from = 90 - aperture / 2;
  const to = 90 + aperture / 2;
  return `conic-gradient(from 0deg, ${color} 0deg ${from}deg, transparent ${from}deg ${to}deg, ${color} ${to}deg 360deg)`;
}

export function Wedge({
  size,
  aperture = 40,
  className,
  style,
}: {
  size: number | string;
  aperture?: number;
  className?: string;
  style?: React.CSSProperties;
}) {
  return (
    <span
      aria-hidden
      className={cn('inline-block rounded-full', className)}
      style={{ width: size, height: size, background: wedgeGradient(aperture), ...style }}
    />
  );
}

/* Post thumbnail — real media when a src exists, a quiet typographic
   placeholder when it doesn't (dummy data has none). Always 4:5. */
export function Thumb({
  tag,
  src,
  size = 34,
  breaker,
  className,
}: {
  tag: string;
  src?: string;
  size?: number;
  breaker?: boolean;
  className?: string;
}) {
  const w = size;
  const h = Math.round((size * 5) / 4);
  return (
    <span
      className={cn(
        'relative inline-flex shrink-0 items-center justify-center overflow-hidden rounded-[7px] border',
        breaker ? 'border-[#E11D48]/60 bg-[#E11D48]/12' : 'border-foreground/12 bg-foreground/[0.06]',
        className,
      )}
      style={{ width: w, height: h }}
    >
      {src ? (
        // eslint-disable-next-line @next/next/no-img-element -- prototype dummy media
        <img src={src} alt="" className="h-full w-full object-cover" />
      ) : (
        <span className={cn('text-[11px] font-black uppercase', breaker ? 'text-[#E11D48]' : 'text-foreground/35')}>
          {tag.replace(/^the\s+/i, '').charAt(0)}
        </span>
      )}
    </span>
  );
}

export function Eyebrow({ children, accent, className }: { children: React.ReactNode; accent?: boolean; className?: string }) {
  return (
    <span
      className={cn(
        'block text-[10px] font-black uppercase tracking-[0.22em]',
        accent ? 'text-[#E11D48]' : 'text-foreground/40',
        className,
      )}
    >
      {children}
    </span>
  );
}

/* One full-screen beat. Children get the reveal variants via a container. */
const BEAT_CONTAINER = {
  hidden: {},
  visible: { transition: { staggerChildren: 0.14, delayChildren: 0.1 } },
} as const;

const BEAT_LINE = {
  hidden: { opacity: 0, transform: 'translate3d(0, 18px, 0)' },
  visible: {
    opacity: 1,
    transform: 'translate3d(0, 0, 0)',
    transition: {
      opacity: { duration: 0.4, ease: VISIT_EASE },
      transform: { duration: 0.52, ease: VISIT_EASE },
    },
  },
} as const;

/* ── THE ONE SHELL ────────────────────────────────────────────────────────
   Every beat renders into the same skeleton: language column left, artifact
   stage right. Mobile: stacked full-screen story beat. Desktop: a composed
   two-zone spread with FLUID width (84vw capped) so laptops and big
   monitors get composition, not margin. Vertical rhythm is clamped, never
   a full viewport. Any artifact drops into the stage unchanged. */
const SHELL_SECTION = 'relative flex min-h-[100dvh] w-screen max-w-[100vw] items-center lg:min-h-0';
const SHELL_WRAP =
  'relative mx-auto box-border w-full max-w-[608px] px-6 pb-24 pt-14 lg:w-[min(1080px,84vw)] lg:max-w-none lg:px-0 lg:py-[clamp(56px,9vh,110px)] xl:w-[min(1080px,calc(100vw-360px))] 2xl:w-[min(1280px,calc(100vw-360px))]';
const SHELL_COLS = 'lg:grid lg:grid-cols-[minmax(0,1fr)_minmax(0,0.92fr)] lg:items-center lg:gap-x-[clamp(48px,6vw,120px)]';

function BeatShell({
  index,
  eyebrow,
  accent,
  left,
  stage,
}: {
  index: number;
  eyebrow: React.ReactNode;
  accent?: boolean;
  left: React.ReactNode;
  stage?: React.ReactNode;
}) {
  const ref = useRef<HTMLElement>(null);
  const inView = useInView(ref, { once: true, amount: 0.4 });
  const reduce = Boolean(useReducedMotion());
  return (
    <section ref={ref} data-beat={index} className={SHELL_SECTION}>
      <motion.div
        className={cn(SHELL_WRAP, stage && SHELL_COLS)}
        variants={BEAT_CONTAINER}
        initial={reduce ? false : 'hidden'}
        animate={reduce || inView ? 'visible' : 'hidden'}
      >
        <div className="min-w-0">
          <motion.div variants={BEAT_LINE}>
            <Eyebrow accent={accent}>{eyebrow}</Eyebrow>
          </motion.div>
          {left}
        </div>
        {stage && (
          <div className="mt-10 min-w-0 lg:mt-0">
            {stage}
          </div>
        )}
      </motion.div>
    </section>
  );
}

/* ── beat 1 · cold open ────────────────────────────────────────────────── */

function CoverBeat() {
  const visit = useVisit();
  const reduce = Boolean(useReducedMotion());
  const wedge = COVER_WEDGE[visit.tier];
  return (
    <section
      data-beat={0}
      className="relative flex min-h-[100dvh] w-full snap-start items-end overflow-hidden [scroll-snap-stop:always]"
    >
      {/* the composition: one big cropped wedge, sized by the (invisible) tier.
          Shares a layoutId with the home cover's small wedge — entering the
          visit blows that one up into this one. Mouth faces the headline. */}
      <motion.span
        aria-hidden
        layoutId={READER_WEDGE_LAYOUT_ID}
        className="absolute rounded-full will-change-transform"
        style={{
          width: wedge.size,
          height: wedge.size,
          right: 'calc(-0.24 * ' + wedge.size + ')',
          top: 'calc(-0.2 * ' + wedge.size + ')',
          background: wedgeGradient(wedge.aperture),
          opacity: wedge.opacity,
          rotate: 180,
        }}
        transition={reduce ? { duration: 0 } : { type: 'spring', duration: 0.48, bounce: 0 }}
      />

      <div className="relative mx-auto box-border w-full max-w-[608px] px-6 pb-24 pt-32 lg:w-[min(1080px,84vw)] lg:max-w-none lg:px-0 xl:w-[min(1080px,calc(100vw-360px))] 2xl:w-[min(1280px,calc(100vw-360px))]">
        <motion.div
          variants={BEAT_CONTAINER}
          initial={reduce ? false : 'hidden'}
          animate="visible"
        >
          <motion.div variants={BEAT_LINE}>
            <Eyebrow accent>Feeder Reader · week {visit.week}</Eyebrow>
            <Eyebrow className="mt-1.5">@{visit.handle}</Eyebrow>
          </motion.div>
          <motion.h1
            variants={BEAT_LINE}
            className="mt-6 text-[clamp(52px,13vw,84px)] font-black leading-[0.94] tracking-tight text-foreground lg:text-[110px]"
          >
            {visit.cover.headline.map((line) => (
              <span key={line} className="block">
                {line}
              </span>
            ))}
          </motion.h1>
          <motion.p variants={BEAT_LINE} className="mt-6 max-w-[300px] text-[14px] font-black leading-snug text-foreground/45 lg:max-w-[400px] lg:text-[17px]">
            {visit.cover.tagline || visit.cover.sub}
          </motion.p>
        </motion.div>
      </div>

      <motion.span
        aria-hidden
        className="absolute inset-x-0 bottom-6 text-center text-[11px] font-black uppercase tracking-[0.18em] text-foreground/28"
        initial={reduce ? false : { opacity: 0 }}
        animate={{ opacity: 1 }}
        transition={reduce ? { duration: 0 } : { delay: 0.42, duration: 0.2, ease: [0.23, 1, 0.32, 1] }}
      >
        scroll to read
      </motion.span>
    </section>
  );
}

/* ── the artifact kit ─────────────────────────────────────────────────────
   Small deterministic performances, self-playing on entry. Same kit
   everywhere: wedge (identity), tiles (runs), ribbon (hunch lifecycle),
   ticks (tenure), dots (rhythm). Data drives the choreography. */

/* THE LEDGER — a hunch's life at full weight: placed ●━━━━━▶ the wedge
   arrives (settled), or the track runs out at 60% with a hollow ring
   waiting (open). Used wherever a bet is on record. */
export function HunchRibbon({
  fromLabel,
  toLabel,
  settled,
  className,
}: {
  fromLabel: string;
  toLabel: string;
  settled: boolean;
  className?: string;
}) {
  const ref = useRef<HTMLDivElement>(null);
  const play = useInView(ref, { once: true, amount: 0.7 });
  const reduce = Boolean(useReducedMotion());
  return (
    <div ref={ref} aria-hidden className={cn('w-full max-w-[520px]', className)}>
      <div className="flex items-center gap-3.5">
        <span className="h-[15px] w-[15px] shrink-0 rounded-full bg-foreground/35" />
        <span className="relative block h-[8px] flex-1 overflow-hidden rounded-full bg-foreground/[0.08]">
          <motion.span
            className="absolute inset-y-0 left-0 w-full origin-left rounded-full"
            style={{ background: settled ? ACCENT : 'rgba(127,127,127,0.45)' }}
            initial={reduce ? false : { scaleX: 0 }}
            animate={reduce || play ? { scaleX: settled ? 1 : 0.58 } : { scaleX: 0 }}
            transition={reduce ? { duration: 0 } : { delay: 0.05, duration: 0.28, ease: READER_SLOT_EASE }}
          />
        </span>
        {settled ? (
          <motion.span
            className="h-[26px] w-[26px] shrink-0 rounded-full"
            style={{ background: wedgeGradient(70), rotate: 180 }}
            initial={reduce ? false : { opacity: 0, transform: 'scale(0.94)' }}
            animate={reduce || play ? { opacity: 1, transform: 'scale(1)' } : {}}
            transition={reduce ? { duration: 0 } : { delay: 0.12, duration: 0.2, ease: READER_SLOT_EASE }}
          />
        ) : (
          <span className="h-[15px] w-[15px] shrink-0 rounded-full border-[3px] border-foreground/25" />
        )}
      </div>
      <div className="mt-2.5 flex items-baseline justify-between">
        <span className="text-[10px] font-black uppercase tracking-[0.14em] text-foreground/40">{fromLabel}</span>
        <span className={cn('text-[10px] font-black uppercase tracking-[0.14em]', settled ? 'text-[#E11D48]' : 'text-foreground/40')}>
          {toLabel}
        </span>
      </div>
    </div>
  );
}

/* THE GATES — lifecycle checkpoints + one thick path. This variant is the
   evergreen read: the path holds high and runs past the last gate. */
export function Gates({ className }: { className?: string }) {
  const ref = useRef<HTMLDivElement>(null);
  const play = useInView(ref, { once: true, amount: 0.7 });
  const reduce = Boolean(useReducedMotion());
  const gateX = [16, 104, 192, 280];
  return (
    <div ref={ref} aria-hidden className={cn('w-full max-w-[420px]', className)}>
      <svg viewBox="0 0 320 96" className="block h-[84px] w-full lg:h-[104px]">
        {gateX.map((x) => (
          <line key={x} x1={x} y1={6} x2={x} y2={78} stroke="currentColor" strokeWidth={4} strokeLinecap="round" className="text-foreground/[0.12]" />
        ))}
        <motion.path
          d="M16 58 C 50 50, 80 38, 104 30 C 140 20, 170 17, 192 16 C 235 14, 285 14, 316 14"
          fill="none"
          stroke={ACCENT}
          strokeWidth={7}
          strokeLinecap="round"
          initial={reduce ? false : { pathLength: 0 }}
          animate={reduce || play ? { pathLength: 1 } : { pathLength: 0 }}
          transition={reduce ? { duration: 0 } : { delay: 0.3, duration: 1.1, ease: VISIT_EASE }}
        />
        <motion.circle
          cx={280}
          cy={14}
          r={8}
          fill={ACCENT}
          initial={reduce ? false : { scale: 0, opacity: 0 }}
          animate={reduce || play ? { scale: 1, opacity: 1 } : {}}
          transition={reduce ? { duration: 0 } : { delay: 1.3, ...VISIT_POP }}
          style={{ transformOrigin: '280px 14px' }}
        />
      </svg>
      <div className="mt-1 grid grid-cols-4 text-[10px] font-black uppercase tracking-[0.14em] text-foreground/35">
        <span>d1</span>
        <span className="text-center">d3</span>
        <span className="text-center">d7</span>
        <span className="text-right text-[#E11D48]">d21 →</span>
      </div>
    </div>
  );
}

/* THE STRIP, rhythm variant — same tile language as the ember row: tiles
   are posts in time, the silence is real empty width, the returns land red. */
export function RhythmStrip({ className }: { className?: string }) {
  const ref = useRef<HTMLDivElement>(null);
  const play = useInView(ref, { once: true, amount: 0.7 });
  const reduce = Boolean(useReducedMotion());
  const cells: ('post' | 'gap' | 'return')[] = ['post', 'post', 'post', 'post', 'gap', 'return', 'return'];
  let delayIndex = 0;
  return (
    <div ref={ref} aria-hidden className={cn('flex items-end gap-[7px] lg:gap-[10px]', className)}>
      {cells.map((cell, i) => {
        if (cell === 'gap') {
          return (
            <span key={i} className="relative block h-[26px] w-[64px] lg:h-[40px] lg:w-[96px]">
              <span className="absolute inset-x-1 bottom-0 border-b-2 border-dashed border-foreground/15" />
            </span>
          );
        }
        const d = delayIndex++;
        return (
          <motion.span
            key={i}
            className={cn(
              'origin-bottom rounded-[6px] lg:rounded-[8px]',
              cell === 'post' ? 'h-[26px] w-[20px] bg-foreground/[0.12] lg:h-[40px] lg:w-[30px]' : 'h-[36px] w-[24px] bg-[#E11D48] lg:h-[54px] lg:w-[36px]',
            )}
            initial={reduce ? false : { opacity: 0, scaleY: 0.3 }}
            animate={reduce || play ? { opacity: 1, scaleY: 1 } : {}}
            transition={
              reduce
                ? { duration: 0 }
                : cell === 'return'
                  ? { delay: 0.75 + d * 0.09, ...VISIT_POP }
                  : { delay: 0.25 + d * 0.09, duration: 0.3, ease: VISIT_EASE }
            }
          />
        );
      })}
    </div>
  );
}

/* ── beat 2 · the callback ─────────────────────────────────────────────── */

function CallbackBeat({ play }: { play: boolean }) {
  const visit = useVisit();
  return (
    <BeatShell
      index={1}
      eyebrow={<>the run · {visit.runLabel || 'current read'}</>}
      left={
        <motion.blockquote
          variants={BEAT_LINE}
          className="mt-6 border-l-2 border-foreground/15 pl-5 text-[19px] font-black leading-snug tracking-tight text-foreground/55 sm:text-[21px] lg:pl-7 lg:text-[26px] 2xl:text-[30px]"
        >
          {visit.cover.summary || visit.callback.hunch}
        </motion.blockquote>
      }
      stage={
        <>
          {visit.runAverage ? (
            <motion.div variants={BEAT_LINE}>
              <RunLandingWedge
                value={visit.runAverage}
                biteCount={visit.caseBeats?.length || 1}
                play={play}
              />
            </motion.div>
          ) : (
            <>
              <motion.p variants={BEAT_LINE} className="text-[clamp(40px,9vw,58px)] font-black leading-none tracking-tight text-[#E11D48] lg:text-[72px] 2xl:text-[84px]">
                {visit.callback.verdict}
              </motion.p>
              <motion.p variants={BEAT_LINE} className="mt-4 text-[15px] font-black leading-snug text-foreground/60 lg:text-[17px] 2xl:text-[19px]">
                {visit.runLabel?.replaceAll('_', ' ') || 'current read'}
              </motion.p>
            </>
          )}
        </>
      }
    />
  );
}

const RUN_DOT_COUNT = 17;
const RUN_EATER_SIZE = 48;
const RUN_EATER_RADIUS = RUN_EATER_SIZE / 2;
const RUN_FEED_DELAY = 0.16;
const RUN_FEED_DURATION = 1.5;
const RUN_FEED_EASE = [0.32, 0.72, 0, 1] as const;

function runEaterPath(aperture: number) {
  const halfAngle = (aperture / 2) * (Math.PI / 180);
  const x = 50 + Math.cos(halfAngle) * 50;
  const rise = Math.sin(halfAngle) * 50;
  return `M 50 50 L ${x.toFixed(2)} ${(50 - rise).toFixed(2)} A 50 50 0 1 0 ${x.toFixed(2)} ${(50 + rise).toFixed(2)} Z`;
}

const RUN_MOUTH_OPEN = runEaterPath(52);
const RUN_MOUTH_CLOSED = runEaterPath(8);
const RUN_MOUTH_FRAMES = Array.from(
  { length: 15 },
  (_, index) => (index % 2 === 0 ? RUN_MOUTH_OPEN : RUN_MOUTH_CLOSED),
);

function RunRankDot({ index, trackWidth, eaterX }: { index: number; trackWidth: number; eaterX: MotionValue<number> }) {
  const position = index / (RUN_DOT_COUNT - 1);
  const swallowAt = position * trackWidth - RUN_EATER_RADIUS;
  const opacity = useTransform(eaterX, [swallowAt - 3, swallowAt], [1, 0]);
  const transform = useTransform(
    eaterX,
    [swallowAt - 7, swallowAt - 3, swallowAt],
    [
      'translate3d(-50%, -50%, 0) scale(1)',
      'translate3d(-50%, -50%, 0) scale(1.22)',
      'translate3d(-50%, -50%, 0) scale(0.68)',
    ],
  );

  return (
    <motion.span
      data-rank-dot={index}
      className="absolute top-1/2 size-[5px] rounded-full bg-foreground/28"
      style={{ left: `${position * 100}%`, opacity, transform }}
    />
  );
}

function RunLandingWedge({ value, biteCount, play }: { value: number; biteCount: number; play: boolean }) {
  const reduce = Boolean(useReducedMotion());
  const trackRef = useRef<HTMLDivElement>(null);
  const [trackWidth, setTrackWidth] = useState(0);
  const [didLand, setDidLand] = useState(false);
  const stop = Math.max(1, Math.min(100, value));
  const progress = ((100 - stop) / 99) * 100;
  const progressFraction = progress / 100;
  const startX = -RUN_EATER_RADIUS - 4;
  const targetX = trackWidth * progressFraction - RUN_EATER_RADIUS;
  const landed = reduce || (play && didLand);
  const eaterX = useMotionValue(startX);
  const eaterTransform = useTransform(
    eaterX,
    (current) => `translate3d(calc(-50% + ${current}px), -50%, 0)`,
  );

  useEffect(() => {
    const track = trackRef.current;
    if (!track) return;
    const update = () => setTrackWidth(track.clientWidth);
    update();
    const observer = new ResizeObserver(update);
    observer.observe(track);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (!trackWidth) return;

    if (reduce) {
      eaterX.set(targetX);
      return;
    }

    if (!play) {
      eaterX.set(startX);
      return;
    }

    eaterX.set(startX);
    const controls = animateMotion(eaterX, targetX, {
      delay: RUN_FEED_DELAY,
      duration: RUN_FEED_DURATION,
      ease: RUN_FEED_EASE,
      onComplete: () => setDidLand(true),
    });

    return () => controls.stop();
  }, [eaterX, play, reduce, startX, targetX, trackWidth]);

  return (
    <div
      data-run-landing-wedge
      className="w-full max-w-[520px]"
      aria-label={`Average post position top ${Math.round(stop)} percent. ${biteCount} active Bites.`}
    >
      <div className="flex items-end justify-between gap-6">
        <div>
          <Eyebrow>run average</Eyebrow>
          <motion.p
            className="mt-2 whitespace-nowrap text-[clamp(34px,9vw,52px)] font-black leading-none tracking-[-0.055em] text-foreground"
            initial={false}
            animate={{
              opacity: landed ? 1 : 0,
              transform: landed ? 'translate3d(0, 0, 0)' : 'translate3d(0, 7px, 0)',
            }}
            transition={reduce ? { duration: 0 } : { duration: 0.24, ease: VISIT_EASE }}
          >
            Top{' '}
            <span className="text-[#E11D48]">
              {Math.round(stop)}%
            </span>
          </motion.p>
        </div>
        <motion.div
          className="shrink-0 text-right"
          initial={false}
          animate={{
            opacity: landed ? 1 : 0,
            transform: landed ? 'translate3d(0, 0, 0)' : 'translate3d(0, 7px, 0)',
          }}
          transition={reduce ? { duration: 0 } : { delay: landed ? 0.06 : 0, duration: 0.24, ease: VISIT_EASE }}
        >
          <span className="block text-[clamp(34px,9vw,52px)] font-black leading-[0.8] tracking-[-0.06em] tabular-nums text-[#E11D48]">
            {String(biteCount).padStart(2, '0')}
          </span>
          <span className="mt-3 block text-[9px] font-black uppercase tracking-[0.16em] text-foreground/38">active Bites</span>
        </motion.div>
      </div>

      <div aria-hidden className="mt-9">
        <div ref={trackRef} className="relative mx-7 h-14">
          {trackWidth
            ? Array.from({ length: RUN_DOT_COUNT }).map((_, index) => (
                <RunRankDot key={index} index={index} trackWidth={trackWidth} eaterX={eaterX} />
              ))
            : null}
          <motion.span
            data-rank-eater
            className="absolute left-0 top-1/2 z-10 size-12 will-change-transform"
            style={{ transform: eaterTransform }}
          >
            <motion.span
              data-rank-eater-glyph
              className="block size-full"
              style={{ backfaceVisibility: 'hidden' }}
              initial={false}
              animate={landed && !reduce
                ? { transform: ['scale(1)', 'scale(1.045)', 'scale(1)'] }
                : { transform: 'scale(1)' }}
              transition={reduce
                ? { duration: 0 }
                : { duration: 0.26, times: [0, 0.42, 1], ease: VISIT_EASE }}
            >
              <svg viewBox="0 0 100 100" className="block size-full overflow-visible">
                <motion.path
                  data-rank-eater-mouth
                  fill={ACCENT}
                  initial={false}
                  animate={{ d: play && trackWidth && !reduce ? RUN_MOUTH_FRAMES : RUN_MOUTH_OPEN }}
                  transition={reduce
                    ? { duration: 0 }
                    : { delay: RUN_FEED_DELAY, duration: RUN_FEED_DURATION, ease: 'linear' }}
                />
              </svg>
            </motion.span>
            <motion.span
              data-rank-impact
              className="pointer-events-none absolute inset-0 rounded-full border border-[#E11D48]/70"
              initial={false}
              animate={landed && !reduce
                ? { opacity: [0, 0.42, 0], transform: ['scale(0.96)', 'scale(1.25)', 'scale(1.42)'] }
                : { opacity: 0, transform: 'scale(0.96)' }}
              transition={reduce
                ? { duration: 0 }
                : { duration: 0.38, times: [0, 0.34, 1], ease: VISIT_EASE }}
            />
          </motion.span>
        </div>
        <div className="mt-2 flex justify-between text-[9px] font-black uppercase tracking-[0.16em] text-foreground/32">
          <span>100%</span>
          <span>1%</span>
        </div>
      </div>
    </div>
  );
}

/* ── beat 3 · the case: cold run broken ───────────────────────────────────
   The performance: seven ember tiles build the drought, the breaker drops
   in and lights up, then the number lands. Facts drive the choreography. */

function EmberPerformance({ play, reduce, caseBeat }: { play: boolean; reduce: boolean; caseBeat: VisitCaseBeat }) {
  const misses = caseBeat.misses;
  const emberDelay = (i: number) => 0.2 + i * 0.09;
  const breakerDelay = 0.2 + misses * 0.09 + 0.45;

  return (
    <div className="flex items-end gap-[6px] lg:gap-[9px]">
      {Array.from({ length: misses }).map((_, i) => (
        <motion.span
          key={i}
          aria-hidden
          className="h-[26px] w-[18px] origin-bottom rounded-[5px] bg-foreground/[0.09] sm:w-[22px] lg:h-[52px] lg:w-[40px] lg:rounded-[9px]"
          initial={reduce ? false : { opacity: 0, scaleY: 0.3 }}
          animate={reduce || play ? { opacity: 1, scaleY: 1 } : {}}
          transition={
            reduce
              ? { duration: 0 }
              : { delay: emberDelay(i), duration: 0.3, ease: [0.22, 1, 0.36, 1] }
          }
        />
      ))}
      <motion.span
        aria-hidden
        className="h-[38px] w-[26px] rounded-[6px] will-change-transform sm:w-[30px] lg:h-[76px] lg:w-[58px] lg:rounded-[12px]"
        style={{ background: ACCENT }}
        initial={reduce ? false : { opacity: 0, y: -44, scale: 0.9 }}
        animate={
          reduce || play
            ? {
                opacity: 1,
                y: 0,
                scale: 1,
                boxShadow: reduce
                  ? 'none'
                  : ['0 0 0px rgba(225,29,72,0)', '0 0 26px rgba(225,29,72,0.55)', '0 0 12px rgba(225,29,72,0.28)'],
              }
            : {}
        }
        transition={
          reduce
            ? { duration: 0 }
            : {
                delay: breakerDelay,
                y: { type: 'spring', stiffness: 420, damping: 22, mass: 0.9 },
                opacity: { duration: 0.16 },
                scale: { type: 'spring', stiffness: 420, damping: 22 },
                boxShadow: { duration: 1.1, times: [0, 0.4, 1], delay: breakerDelay + 0.1 },
              }
        }
      />
    </div>
  );
}

function PoolPerformance({ play, reduce, caseBeat }: { play: boolean; reduce: boolean; caseBeat: VisitCaseBeat }) {
  const position = Math.max(4, Math.min(96, (caseBeat.rank / caseBeat.pool) * 100));
  return (
    <div aria-hidden className="relative h-[92px] w-full max-w-[420px]">
      {Array.from({ length: 22 }).map((_, i) => (
        <motion.span
          key={i}
          className="absolute h-[5px] w-[5px] rounded-full bg-foreground/[0.14]"
          style={{ left: `${5 + ((i * 37) % 90)}%`, top: 14 + ((i * 29) % 54) }}
          initial={reduce ? false : { opacity: 0, scale: 0.4 }}
          animate={reduce || play ? { opacity: 1, scale: 1 } : {}}
          transition={reduce ? { duration: 0 } : { delay: 0.08 + (i % 7) * 0.05, duration: 0.35 }}
        />
      ))}
      <motion.span
        className="absolute top-[38px] h-[20px] w-[20px] rounded-full bg-[#E11D48]"
        style={{ left: `${position}%`, marginLeft: -10 }}
        initial={reduce ? false : { opacity: 0, y: -48, scale: 0.5 }}
        animate={reduce || play ? { opacity: 1, y: 0, scale: 1 } : {}}
        transition={reduce ? { duration: 0 } : { delay: 0.75, ...VISIT_POP }}
      />
      <span className="absolute inset-x-0 bottom-1 border-b border-foreground/10" />
    </div>
  );
}

/* ── Account Reader Bite beats ───────────────────────────────────────────
   The original Reader's slot grammar, split into two beats: first the title
   changes and explains itself; then the evidence and its most useful current
   artifact arrive. */

const READER_SLOT_EASE = [0.23, 1, 0.32, 1] as const;

function ReaderSlot({
  play,
  reduce,
  delay = 0,
  className,
  children,
}: {
  play: boolean;
  reduce: boolean;
  delay?: number;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <div className={cn('overflow-hidden', className)}>
      <motion.div
        initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 12px, 0)' }}
        animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
        transition={reduce ? { duration: 0 } : { duration: 0.26, ease: READER_SLOT_EASE, delay: Math.min(delay, 0.12) }}
      >
        {children}
      </motion.div>
    </div>
  );
}

function EvolutionTape({ reader, play, reduce, currentTitle }: {
  reader: VisitReaderCase;
  play: boolean;
  reduce: boolean;
  currentTitle: string;
}) {
  const changed = Boolean(reader.previousTitle && reader.previousTitle !== currentTitle);

  return (
    <div className="w-full max-w-[560px]">
      <div className="flex items-center justify-between gap-5">
        <Eyebrow>{reader.history.length > 1 ? 'portrait history' : 'first portrait'}</Eyebrow>
        <motion.span
          className="shrink-0 text-[10px] font-black uppercase tracking-[0.16em] text-[#E11D48]"
          initial={reduce ? false : { opacity: 0, transform: 'translate3d(-8px, 0, 0)' }}
          animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
          transition={reduce ? { duration: 0 } : { delay: 0.1, duration: 0.22, ease: READER_SLOT_EASE }}
        >
          {changed ? `${reader.movement} →` : reader.movement === 'held' ? 'still holds' : `${reader.movement} →`}
        </motion.span>
      </div>

      <div className="relative mt-4 border-t border-foreground/16">
        <motion.span
          aria-hidden
          className="absolute left-0 top-[-1px] h-[2px] w-[34%] origin-left bg-[#E11D48]"
          initial={reduce ? false : { scaleX: 0 }}
          animate={reduce || play ? { scaleX: 1 } : {}}
          transition={reduce ? { duration: 0 } : { delay: 0.08, duration: 0.26, ease: READER_SLOT_EASE }}
        />
        <div className="grid" style={{ gridTemplateColumns: `repeat(${reader.history.length}, minmax(0, 1fr))` }}>
          {reader.history.map((item, index) => {
            const current = index === reader.history.length - 1;
            return (
              <motion.div
                key={`${item.week}:${item.movement}`}
                className="min-w-0 pb-1 pr-2 pt-4 last:pr-0"
                initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 8px, 0)' }}
                animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
                transition={reduce ? { duration: 0 } : { delay: 0.1 + index * 0.045, duration: 0.22, ease: READER_SLOT_EASE }}
              >
                <span className={cn('block text-[9px] font-black uppercase tracking-[0.15em]', current ? 'text-[#E11D48]' : 'text-foreground/30')}>
                  {String(item.week).padStart(2, '0')}
                </span>
                <span className={cn('mt-1.5 block truncate text-[10px] font-black uppercase tracking-[0.1em] sm:text-[11px]', current ? 'text-foreground' : 'text-foreground/42')}>
                  {item.movement}
                </span>
              </motion.div>
            );
          })}
        </div>
      </div>
    </div>
  );
}

function MovementEvent({ reader, currentTitle, play, reduce }: {
  reader: VisitReaderCase;
  currentTitle: string;
  play: boolean;
  reduce: boolean;
}) {
  return (
    <motion.div
      data-reader-movement-event={reader.movement}
      initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 10px, 0)' }}
      animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
      transition={reduce ? { duration: 0 } : { duration: 0.24, ease: READER_SLOT_EASE }}
      className="w-full max-w-[1040px]"
    >
      <Eyebrow accent>the reading moved</Eyebrow>
      <p className="mt-4 text-[clamp(68px,22vw,108px)] font-black uppercase leading-[0.78] tracking-[-0.075em] text-[#E11D48] lg:text-[148px]">
        {reader.movement}
      </p>
      <motion.span
        aria-hidden
        className="mt-7 block h-[3px] w-full origin-left bg-[#E11D48]"
        initial={reduce ? false : { transform: 'scaleX(0)' }}
        animate={reduce || play ? { transform: 'scaleX(1)' } : {}}
        transition={reduce ? { duration: 0 } : { delay: 0.06, duration: 0.3, ease: READER_SLOT_EASE }}
      />
      <p className="mt-6 max-w-[820px] text-[17px] font-black leading-[1.28] tracking-tight text-foreground/78 sm:text-[20px] lg:text-[26px]">
        {reader.changedBecause}
      </p>
      <div className="mt-7 flex items-baseline justify-between gap-6 border-t border-foreground/12 pt-4">
        <p className="min-w-0 truncate text-[10px] font-black uppercase tracking-[0.12em] text-foreground/30">
          {reader.previousTitle}
        </p>
        <span className="shrink-0 text-[13px] font-black text-[#E11D48]" aria-hidden>→</span>
        <p className="min-w-0 truncate text-right text-[10px] font-black uppercase tracking-[0.12em] text-foreground/74">
          {currentTitle}
        </p>
      </div>
    </motion.div>
  );
}

type BiteTitleStage = 'previous' | 'shift' | 'current';

function BiteTitleStageBeat({ caseBeat, index, stage }: { caseBeat: VisitCaseBeat; index: number; stage: BiteTitleStage }) {
  const reader = caseBeat.reader!;
  const ref = useRef<HTMLElement>(null);
  const play = useInView(ref, { once: true, amount: 0.45 });
  const reduce = Boolean(useReducedMotion());
  const currentTitle = caseBeat.hook.join(' ');
  const titleChanges = Boolean(reader.previousTitle && reader.previousTitle !== currentTitle && reader.movement !== 'held');
  const visibleTitle = stage === 'previous' && titleChanges ? reader.previousTitle! : currentTitle;
  const longTitle = visibleTitle.length > 26;

  return (
    <section ref={ref} data-beat={index} data-title-stage={stage} className={SHELL_SECTION}>
      <div className={SHELL_WRAP}>
        {stage !== 'shift' ? (
          <ReaderSlot play={play} reduce={reduce}>
            <div className="flex items-center justify-between gap-4">
              <Eyebrow accent>{reader.movement} Bite</Eyebrow>
              <Eyebrow>{stage === 'previous' ? 'previous portrait' : reader.movement === 'held' ? 'the reading holds' : 'current portrait'}</Eyebrow>
            </div>
          </ReaderSlot>
        ) : null}

        {stage === 'shift' ? (
          <ReaderSlot play={play} reduce={reduce}>
            <MovementEvent reader={reader} currentTitle={currentTitle} play={play} reduce={reduce} />
          </ReaderSlot>
        ) : (
          <>
            <ReaderSlot play={play} reduce={reduce} delay={0.08} className="mt-6">
              <h2
                className={cn(
                  'max-w-[1040px] font-black leading-[0.91] tracking-[-0.055em] text-foreground',
                  stage === 'previous' && 'text-foreground/32',
                  longTitle
                    ? 'text-[clamp(42px,11vw,62px)] lg:text-[70px] 2xl:text-[78px]'
                    : 'text-[clamp(48px,13vw,72px)] lg:text-[84px] 2xl:text-[96px]',
                )}
              >
                {visibleTitle}
              </h2>
            </ReaderSlot>

            {stage === 'previous' ? (
              <ReaderSlot play={play} reduce={reduce} delay={0.18} className="mt-10 border-t border-foreground/10 pt-6 text-right">
                <p className="text-[10px] font-black uppercase tracking-[0.16em] text-foreground/30">scroll · next: {reader.movement}</p>
              </ReaderSlot>
            ) : (
              <div className="mt-7 grid gap-7 border-t border-foreground/12 pt-6 lg:grid-cols-[minmax(0,1.04fr)_minmax(340px,0.96fr)] lg:gap-16">
                <div>
                  {reader.movement === 'held' ? (
                    <ReaderSlot play={play} reduce={reduce} delay={0.16}>
                      <Eyebrow accent>why it still holds</Eyebrow>
                      <p className="mt-3 max-w-[690px] text-[16px] font-black leading-[1.28] tracking-tight text-foreground/78 sm:text-[18px] lg:text-[22px]">
                        {reader.changedBecause}
                      </p>
                    </ReaderSlot>
                  ) : null}
                  <ReaderSlot play={play} reduce={reduce} delay={0.24} className={cn(reader.movement === 'held' ? 'mt-5' : '', 'border-l-2 border-[#E11D48] pl-4 lg:pl-5')}>
                    <Eyebrow>{reader.movement === 'held' ? 'why it matters now' : 'what the new title unlocks'}</Eyebrow>
                    <p className="mt-2 max-w-[620px] text-[13px] font-black leading-[1.55] text-foreground/50 lg:text-[15px]">
                      {reader.whyItMattersNow}
                    </p>
                  </ReaderSlot>
                </div>
                <ReaderSlot play={play} reduce={reduce} delay={0.22}>
                  <EvolutionTape reader={reader} play={play} reduce={reduce} currentTitle={currentTitle} />
                </ReaderSlot>
              </div>
            )}
          </>
        )}
      </div>
    </section>
  );
}

function BiteTitleBeat({ caseBeat, index, stage }: { caseBeat: VisitCaseBeat; index: number; stage: BiteTitleStage }) {
  return <BiteTitleStageBeat caseBeat={caseBeat} index={index} stage={stage} />;
}

function parseReaderRank(rank?: string) {
  const match = rank?.match(/^(\d+)\/(\d+)/);
  if (!match) return null;
  return { value: Number(match[1]), pool: Number(match[2]), position: Number(match[1]) / Number(match[2]) };
}

function RankSpanArtifact({ artifact, play, reduce }: { artifact: Extract<VisitReaderArtifact, { kind: 'rank_span' }>; play: boolean; reduce: boolean }) {
  const ranked = artifact.posts
    .map((post) => ({ post, rank: parseReaderRank(post.recentRank || post.overallRank) }))
    .filter((item): item is { post: VisitReaderPost; rank: NonNullable<ReturnType<typeof parseReaderRank>> } => Boolean(item.rank))
    .slice(0, 7);
  const ordered = [...ranked].sort((a, b) => a.rank.value - b.rank.value);
  const best = ordered[0];
  const floor = ordered.at(-1);

  return (
    <div className="w-full max-w-[680px]">
      <Eyebrow>the supporting span</Eyebrow>
      {best && floor ? (
        <div className="mt-7 grid grid-cols-[1fr_auto_1fr] items-end gap-4 sm:gap-7">
          {[best, floor].map((item, i) => (
            <motion.div
              key={item.post.title}
              className={cn('min-w-0', i === 0 ? 'order-1' : 'order-3 text-right')}
              initial={reduce ? false : { opacity: 0, transform: `translate3d(${i === 0 ? '-10px' : '10px'}, 0, 0)` }}
              animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
              transition={reduce ? { duration: 0 } : { delay: 0.06 + i * 0.07, duration: 0.36, ease: READER_SLOT_EASE }}
            >
              <span className={cn('block text-[clamp(68px,20vw,104px)] font-black leading-[0.78] tracking-[-0.08em] tabular-nums', i === 0 ? 'text-[#E11D48]' : 'text-foreground/62')}>
                {item.rank.value}
              </span>
              <span className="mt-4 block text-[9px] font-black uppercase tracking-[0.14em] text-foreground/32">
                of {item.rank.pool} · {i === 0 ? 'highest' : 'lowest'}
              </span>
              <span className="mt-2 block text-[12px] font-black leading-tight text-foreground/62 sm:text-[14px]">{item.post.title}</span>
            </motion.div>
          ))}
          <motion.span
            aria-hidden
            className="order-2 mb-[52px] block h-px w-[34px] bg-foreground/24 sm:w-[70px]"
            initial={reduce ? false : { opacity: 0, transform: 'scaleX(0)' }}
            animate={reduce || play ? { opacity: 1, transform: 'scaleX(1)' } : {}}
            transition={reduce ? { duration: 0 } : { delay: 0.12, duration: 0.3, ease: READER_SLOT_EASE }}
          />
        </div>
      ) : null}
    </div>
  );
}

function EvidenceTurnoverArtifact({ artifact, play, reduce }: { artifact: Extract<VisitReaderArtifact, { kind: 'evidence_turnover' }>; play: boolean; reduce: boolean }) {
  const groups = [
    { label: 'carried', posts: artifact.carried, accent: false },
    { label: 'added now', posts: artifact.added, accent: true },
    { label: 'left the read', posts: artifact.dropped, accent: false },
  ];
  return (
    <div className="w-full max-w-[680px]">
      <Eyebrow>the evidence moved</Eyebrow>
      <div className="mt-7 grid grid-cols-3 gap-3 sm:gap-7">
        {groups.map((group, i) => (
          <motion.div
            key={group.label}
            className={cn('min-w-0', group.accent ? 'text-[#E11D48]' : 'text-foreground/72')}
            initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 8px, 0)' }}
            animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
            transition={reduce ? { duration: 0 } : { delay: 0.05 + i * 0.06, duration: 0.34, ease: READER_SLOT_EASE }}
          >
            <span className="block text-[clamp(64px,18vw,102px)] font-black leading-[0.78] tracking-[-0.08em] tabular-nums">
              {i === 1 ? '+' : i === 2 ? '−' : ''}{group.posts.length}
            </span>
            <span className="mt-4 block border-t border-current/20 pt-3 text-[9px] font-black uppercase tracking-[0.14em] opacity-60">{group.label}</span>
          </motion.div>
        ))}
      </div>
    </div>
  );
}

function TenureArtifact({ artifact, play, reduce }: { artifact: Extract<VisitReaderArtifact, { kind: 'tenure' }>; play: boolean; reduce: boolean }) {
  const heldStreak = [...artifact.history].reverse().findIndex((item) => item.movement !== 'held');
  const held = heldStreak === -1 ? artifact.history.length : heldStreak;
  const recasts = artifact.history.filter((item) => item.movement === 'recast').length;
  return (
    <div className="w-full max-w-[680px]">
      <Eyebrow>the reading held</Eyebrow>
      <div className="mt-6 flex items-end gap-5">
        <motion.span
          className="text-[clamp(100px,30vw,164px)] font-black leading-[0.72] tracking-[-0.09em] tabular-nums text-[#E11D48]"
          initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 10px, 0)' }}
          animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
          transition={reduce ? { duration: 0 } : { duration: 0.38, ease: READER_SLOT_EASE }}
        >
          {String(artifact.history.length).padStart(2, '0')}
        </motion.span>
        <span className="pb-2 text-[10px] font-black uppercase leading-relaxed tracking-[0.16em] text-foreground/36">runs<br />active</span>
      </div>
      <div className="mt-7 grid grid-cols-3 border-y border-foreground/12">
        {artifact.history.map((item, i) => (
          <motion.div
            key={`${item.week}:${item.movement}`}
            className={cn('min-w-0 py-3', i > 0 && 'border-l border-foreground/12 pl-4')}
            initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 7px, 0)' }}
            animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
            transition={reduce ? { duration: 0 } : { delay: 0.08 + i * 0.05, duration: 0.28, ease: READER_SLOT_EASE }}
          >
            <span className="block text-[9px] font-black uppercase tracking-[0.12em] text-foreground/28">week {item.week}</span>
            <span className={cn('mt-1 block text-[10px] font-black uppercase tracking-[0.08em]', i === artifact.history.length - 1 ? 'text-[#E11D48]' : 'text-foreground/55')}>{item.movement}</span>
          </motion.div>
        ))}
      </div>
      <p className="mt-5 text-[10px] font-black uppercase tracking-[0.14em] text-foreground/34">
        {held ? `${held} held in a row` : 'meaning moved this run'} · {recasts} recast{recasts === 1 ? '' : 's'}
      </p>
    </div>
  );
}

function AnomalyPairArtifact({ artifact, play, reduce }: { artifact: Extract<VisitReaderArtifact, { kind: 'anomaly_pair' }>; play: boolean; reduce: boolean }) {
  return (
    <div className="w-full max-w-[680px]">
      <Eyebrow>same Bite · different landing</Eyebrow>
      <div className="mt-7 grid grid-cols-[1fr_auto_1fr] items-end gap-4 sm:gap-7">
        {artifact.posts.map((post, i) => {
          const rank = parseReaderRank(post.recentRank || post.overallRank);
          return (
            <motion.div
              key={post.title}
              className={cn('min-w-0', i === 0 ? 'order-1' : 'order-3 text-right')}
              initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 8px, 0)' }}
              animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
              transition={reduce ? { duration: 0 } : { delay: 0.05 + i * 0.07, duration: 0.34, ease: READER_SLOT_EASE }}
            >
              <span className={cn('inline-flex items-baseline font-black leading-none', i === 0 ? 'text-[#E11D48]' : 'text-foreground/65')}>
                <span className="text-[clamp(68px,20vw,104px)] tracking-[-0.08em] tabular-nums">{rank?.value ?? '—'}</span>
                <span className="ml-1 text-[15px] text-foreground/30 lg:text-[18px]">/{rank?.pool ?? '—'}</span>
              </span>
              <p className="mt-3 text-[12px] font-black leading-tight text-foreground/62 sm:text-[14px]">{post.title}</p>
              {post.overallRank ? <p className="mt-1 text-[9px] font-black uppercase tracking-[0.12em] text-foreground/30">{post.overallRank} overall</p> : null}
            </motion.div>
          );
        })}
        <motion.span
          aria-hidden
          className="order-2 mb-[52px] block h-px w-[34px] bg-foreground/24 sm:w-[70px]"
          initial={reduce ? false : { opacity: 0, transform: 'scaleX(0)' }}
          animate={reduce || play ? { opacity: 1, transform: 'scaleX(1)' } : {}}
          transition={reduce ? { duration: 0 } : { delay: 0.12, duration: 0.3, ease: READER_SLOT_EASE }}
        />
      </div>
    </div>
  );
}

function EvidenceStackArtifact({ artifact, play, reduce }: { artifact: Extract<VisitReaderArtifact, { kind: 'evidence_stack' }>; play: boolean; reduce: boolean }) {
  return (
    <div className="w-full max-w-[680px]">
      <Eyebrow>the proof holds</Eyebrow>
      <div className="mt-6 flex items-end gap-5">
        <motion.span
          className="text-[clamp(100px,30vw,164px)] font-black leading-[0.72] tracking-[-0.09em] tabular-nums text-[#E11D48]"
          initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 10px, 0)' }}
          animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
          transition={reduce ? { duration: 0 } : { duration: 0.38, ease: READER_SLOT_EASE }}
        >
          {String(artifact.posts.length).padStart(2, '0')}
        </motion.span>
        <span className="pb-2 text-[10px] font-black uppercase leading-relaxed tracking-[0.16em] text-foreground/36">posts<br />supporting</span>
      </div>
      <div className="mt-7 grid grid-cols-1 gap-px border-y border-foreground/12 sm:grid-cols-3">
        {artifact.posts.slice(0, 3).map((post, i) => (
          <motion.div
            key={post.title}
            className={cn('min-w-0 py-3 sm:pr-4', i > 0 && 'border-t border-foreground/12 sm:border-l sm:border-t-0 sm:pl-4')}
            initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 7px, 0)' }}
            animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
            transition={reduce ? { duration: 0 } : { delay: 0.05 + i * 0.055, duration: 0.3, ease: READER_SLOT_EASE }}
          >
            <span className="block text-[9px] font-black uppercase tracking-[0.12em] text-foreground/28">{String(i + 1).padStart(2, '0')}</span>
            <span className="mt-1.5 block text-[12px] font-black leading-tight text-foreground/62 lg:text-[14px]">{post.title}</span>
          </motion.div>
        ))}
      </div>
    </div>
  );
}

function ReaderArtifactStage({ artifact, play, reduce }: { artifact: VisitReaderArtifact; play: boolean; reduce: boolean }) {
  const content = artifact.kind === 'rank_span'
    ? <RankSpanArtifact artifact={artifact} play={play} reduce={reduce} />
    : artifact.kind === 'evidence_turnover'
      ? <EvidenceTurnoverArtifact artifact={artifact} play={play} reduce={reduce} />
      : artifact.kind === 'tenure'
        ? <TenureArtifact artifact={artifact} play={play} reduce={reduce} />
        : artifact.kind === 'anomaly_pair'
          ? <AnomalyPairArtifact artifact={artifact} play={play} reduce={reduce} />
          : <EvidenceStackArtifact artifact={artifact} play={play} reduce={reduce} />;

  return (
    <motion.div
      data-reader-artifact={artifact.kind}
      className="relative w-full max-w-[680px]"
      initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 10px, 0)' }}
      animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
      transition={reduce ? { duration: 0 } : { duration: 0.36, ease: READER_SLOT_EASE }}
    >
      {content}
    </motion.div>
  );
}

function BiteReadingBeat({ caseBeat, index }: { caseBeat: VisitCaseBeat; index: number }) {
  const reader = caseBeat.reader!;
  const ref = useRef<HTMLElement>(null);
  const play = useInView(ref, { once: true, amount: 0.45 });
  const reduce = Boolean(useReducedMotion());
  const boundaryCount = reader.counterevidenceRefs.length;

  return (
    <section ref={ref} data-beat={index} className={SHELL_SECTION}>
      <div className={cn(SHELL_WRAP, 'lg:grid lg:grid-cols-[minmax(220px,0.58fr)_minmax(0,1.42fr)] lg:items-center lg:gap-x-[clamp(64px,8vw,144px)]')}>
        <div className="min-w-0 lg:border-r lg:border-foreground/12 lg:pr-[clamp(40px,5vw,80px)]">
          <ReaderSlot play={play} reduce={reduce}>
            <Eyebrow accent>the current read</Eyebrow>
          </ReaderSlot>
          <ReaderSlot play={play} reduce={reduce} delay={0.08} className="mt-3">
            <h2 className="text-[clamp(30px,8vw,48px)] font-black leading-[0.94] tracking-[-0.045em] text-foreground lg:text-[54px]">{caseBeat.hook.join(' ')}</h2>
          </ReaderSlot>
          <ReaderSlot play={play} reduce={reduce} delay={0.18} className="mt-9 flex items-end gap-3">
            <span className="text-[clamp(72px,22vw,112px)] font-black leading-[0.74] tracking-[-0.09em] tabular-nums text-[#E11D48]">
              {String(caseBeat.packet.length).padStart(2, '0')}
            </span>
            <span className="pb-1 text-[9px] font-black uppercase leading-relaxed tracking-[0.14em] text-foreground/32">posts<br />hold this read</span>
          </ReaderSlot>
        </div>

        <div className="mt-10 min-w-0 lg:mt-0">
          <ReaderSlot play={play} reduce={reduce} delay={0.14}>
            <p className="max-w-[760px] text-[clamp(20px,5.6vw,28px)] font-black leading-[1.22] tracking-[-0.025em] text-foreground/82 lg:text-[34px]">
              {reader.currentRead}
            </p>
          </ReaderSlot>
          <ReaderSlot play={play} reduce={reduce} delay={0.26} className="mt-8 flex flex-wrap gap-x-8 gap-y-2 border-t border-foreground/12 pt-4">
            <span className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/32">current window · {caseBeat.packet.length} supporting</span>
            <span className={cn('text-[9px] font-black uppercase tracking-[0.14em]', boundaryCount ? 'text-[#E11D48]' : 'text-foreground/28')}>
              {boundaryCount ? `${boundaryCount} ${boundaryCount === 1 ? 'boundary' : 'boundaries'}` : 'no boundary found'}
            </span>
          </ReaderSlot>
        </div>
      </div>
    </section>
  );
}

function BiteProofBeat({ caseBeat, index, onOpenFile }: { caseBeat: VisitCaseBeat; index: number; onOpenFile: () => void }) {
  const reader = caseBeat.reader!;
  const ref = useRef<HTMLElement>(null);
  const play = useInView(ref, { once: true, amount: 0.4 });
  const reduce = Boolean(useReducedMotion());
  const proofHeadline = {
    rank_span: 'The span.',
    evidence_turnover: 'The proof moved.',
    tenure: 'Still standing.',
    anomaly_pair: 'Different landing.',
    evidence_stack: 'The proof holds.',
  }[reader.artifact.kind];

  return (
    <section ref={ref} data-beat={index} className={SHELL_SECTION}>
      <div className={cn(SHELL_WRAP, 'lg:grid lg:grid-cols-[minmax(0,0.72fr)_minmax(0,1.28fr)] lg:items-center lg:gap-x-[clamp(56px,6vw,112px)]')}>
        <div className="min-w-0">
          <ReaderSlot play={play} reduce={reduce}>
            <Eyebrow accent>the evidence</Eyebrow>
          </ReaderSlot>
          <ReaderSlot play={play} reduce={reduce} delay={0.08} className="mt-4">
            <h2 className="max-w-[570px] text-[clamp(48px,13vw,74px)] font-black leading-[0.88] tracking-[-0.06em] text-foreground lg:text-[82px]">
              {proofHeadline}
            </h2>
          </ReaderSlot>
          <ReaderSlot play={play} reduce={reduce} delay={0.18} className="mt-6 border-l-2 border-[#E11D48] pl-4 lg:pl-5">
            <Eyebrow>the pressure now</Eyebrow>
            <p className="mt-2 max-w-[560px] text-[14px] font-black leading-relaxed text-foreground/56 lg:text-[16px]">
              {reader.whyItMattersNow}
            </p>
          </ReaderSlot>
          <ReaderSlot play={play} reduce={reduce} delay={0.3} className="mt-7">
            <button
              type="button"
              onClick={onOpenFile}
              className="inline-flex items-center gap-1.5 rounded-full border border-foreground/12 px-3.5 py-2 text-[10px] font-black uppercase tracking-[0.12em] text-foreground/45 transition-colors hover:border-[#E11D48]/40 hover:text-[#E11D48]"
            >
              open the case file <span aria-hidden>→</span>
            </button>
          </ReaderSlot>
        </div>

        <div className="mt-10 min-w-0 lg:mt-0 lg:justify-self-stretch">
          <ReaderArtifactStage artifact={reader.artifact} play={play} reduce={reduce} />
        </div>
      </div>
    </section>
  );
}

/* The case file — where the long read lives. The beat face never grows to
   fit prose; anything past two sentences opens here. Bottom sheet on mobile,
   right panel on desktop. */
export function CaseFileSheet({ open, onClose, caseIndex = 0 }: { open: boolean; onClose: () => void; caseIndex?: number }) {
  const visit = useVisit();
  const reduce = Boolean(useReducedMotion());
  const c = (visit.caseBeats || [visit.caseBeat])[caseIndex] || visit.caseBeat;

  useEffect(() => {
    if (!open) return;
    const onKey = (e: KeyboardEvent) => {
      if (e.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [open, onClose]);

  return (
    <AnimatePresence>
      {open && (
        <div className="fixed inset-0 z-40">
          <motion.button
            type="button"
            aria-label="Close the case file"
            className="absolute inset-0 h-full w-full cursor-default bg-black/45"
            onClick={onClose}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: reduce ? 0 : 0.24 }}
          />
          <motion.div
            role="dialog"
            aria-label="Case file"
            className={cn(
              'absolute flex flex-col bg-background text-foreground',
              'inset-x-0 bottom-0 max-h-[82dvh] rounded-t-[26px] border-t border-foreground/10',
              'sm:inset-x-auto sm:inset-y-0 sm:right-0 sm:max-h-none sm:w-[460px] sm:rounded-none sm:border-l sm:border-t-0',
            )}
            initial={reduce ? { opacity: 0 } : { opacity: 0, y: 48 }}
            animate={{ opacity: 1, y: 0 }}
            exit={reduce ? { opacity: 0 } : { opacity: 0, y: 32 }}
            transition={reduce ? { duration: 0 } : { duration: 0.34, ease: [0.22, 1, 0.36, 1] }}
          >
            <div className="flex items-start justify-between gap-4 border-b border-foreground/10 px-6 pb-4 pt-5 sm:pt-7">
              <div>
                <Eyebrow accent>case file · {c.kicker}</Eyebrow>
                <p className="mt-2 text-[22px] font-black leading-tight tracking-tight">
                  {c.hook.join(' ')}
                </p>
              </div>
              <button
                type="button"
                onClick={onClose}
                aria-label="Close"
                className="inline-flex h-8 w-8 shrink-0 items-center justify-center rounded-full border border-foreground/12 text-[12px] font-black text-foreground/55 transition-colors hover:text-foreground"
              >
                ✕
              </button>
            </div>

            <div className="hide-scrollbar min-h-0 flex-1 overflow-y-auto px-6 pb-10 pt-6">
              <div className="space-y-5">
                {c.fullRead.map((para) => (
                  <p key={para.slice(0, 24)} className="text-[14.5px] font-black leading-relaxed text-foreground/72">
                    {para}
                  </p>
                ))}
              </div>

              <Eyebrow className="mb-4 mt-9">the packet · {c.packet.length} posts</Eyebrow>
              <div className="grid grid-cols-4 gap-3">
                {c.packet.map((post) => (
                  <a
                    key={post.tag}
                    href={post.url}
                    target="_blank"
                    rel="noreferrer"
                    className="min-w-0 no-underline"
                    aria-label={`Open ${post.tag} on Instagram`}
                  >
                    <Thumb tag={post.tag} size={64} breaker={post.breaker} className="w-full" />
                    <p
                      className={cn(
                        'mt-1.5 truncate text-[9px] font-black uppercase tracking-[0.08em]',
                        post.breaker ? 'text-[#E11D48]' : 'text-foreground/45',
                      )}
                    >
                      {post.tag}
                    </p>
                    <p className="text-[9px] font-black tabular-nums text-foreground/30">{post.rank}</p>
                  </a>
                ))}
              </div>
            </div>
          </motion.div>
        </div>
      )}
    </AnimatePresence>
  );
}

function CaseBeat({ caseBeat, index, onOpenFile }: { caseBeat: VisitCaseBeat; index: number; onOpenFile: () => void }) {
  const ref = useRef<HTMLDivElement>(null);
  const play = useInView(ref, { once: true, amount: 0.55 });
  const reduce = Boolean(useReducedMotion());
  const misses = caseBeat.misses;
  const afterBreaker = 0.2 + misses * 0.09 + 0.45 + 0.5;

  const late = (extra: number) =>
    reduce
      ? { duration: 0 }
      : { delay: afterBreaker + extra, duration: 0.45, ease: [0.22, 1, 0.36, 1] as const };

  return (
    <section data-beat={index} className={SHELL_SECTION}>
      <div ref={ref} className={cn(SHELL_WRAP, SHELL_COLS)}
      >
        <motion.div
          className="lg:col-start-1"
          initial={reduce ? false : { opacity: 0, y: 14 }}
          animate={reduce || play ? { opacity: 1, y: 0 } : {}}
          transition={reduce ? { duration: 0 } : { duration: 0.4, ease: [0.22, 1, 0.36, 1] }}
        >
          <Eyebrow>{caseBeat.kicker}</Eyebrow>
          <h2 className="mt-4 text-[clamp(34px,8vw,52px)] font-black leading-[1.02] tracking-tight text-foreground lg:text-[64px]">
            {caseBeat.hook.map((line) => (
              <span key={line} className="block">
                {line}
              </span>
            ))}
          </h2>
        </motion.div>

        {/* the performance zone — right column on desktop, in-flow on mobile */}
        <div className="mt-10 lg:col-start-2 lg:row-span-2 lg:row-start-1 lg:mt-0 lg:justify-self-center">
          {caseBeat.artifact === 'pool' ? (
            <PoolPerformance play={play} reduce={reduce} caseBeat={caseBeat} />
          ) : (
            <EmberPerformance play={play} reduce={reduce} caseBeat={caseBeat} />
          )}

          <motion.div
            className="mt-8 flex items-baseline gap-2"
            initial={reduce ? false : { opacity: 0, y: 10 }}
            animate={reduce || play ? { opacity: 1, y: 0 } : {}}
            transition={late(0)}
          >
          <span className="inline-flex items-baseline font-black leading-none text-[#E11D48]">
            {reduce || play ? (
              <Odometer
                value={caseBeat.rank}
                animateOnMount
                revealDelayMs={reduce ? 0 : Math.round((afterBreaker + 0.1) * 1000)}
                className="inline-flex text-[56px] tabular-nums sm:text-[64px] lg:text-[88px]"
              />
            ) : (
              <span className="inline-flex text-[56px] tabular-nums opacity-0 sm:text-[64px] lg:text-[88px]">{caseBeat.rank}</span>
            )}
            <span className="ml-1 text-[18px] text-foreground/35 lg:text-[24px]">/{caseBeat.pool}</span>
          </span>
          <span className="text-[12px] font-black uppercase tracking-[0.14em] text-foreground/40">
            {caseBeat.rankNote}
          </span>
          </motion.div>
        </div>

        <div className="lg:col-start-1 lg:row-start-2">
        <motion.p
          className="mt-7 max-w-[440px] text-[16px] font-black leading-snug tracking-tight text-foreground sm:text-[17px] lg:max-w-[460px] lg:text-[21px]"
          initial={reduce ? false : { opacity: 0, y: 10 }}
          animate={reduce || play ? { opacity: 1, y: 0 } : {}}
          transition={late(0.16)}
        >
          {caseBeat.verdict}
        </motion.p>
        <motion.p
          className="mt-3 max-w-[420px] text-[13.5px] font-black leading-snug text-foreground/50 lg:max-w-[440px] lg:text-[16px]"
          initial={reduce ? false : { opacity: 0, y: 10 }}
          animate={reduce || play ? { opacity: 1, y: 0 } : {}}
          transition={late(0.28)}
        >
          {caseBeat.evidence}
        </motion.p>

        <motion.div
          className="mt-7 flex flex-wrap items-center gap-2.5"
          initial={reduce ? false : { opacity: 0 }}
          animate={reduce || play ? { opacity: 1 } : {}}
          transition={late(0.4)}
        >
          <a
            href={caseBeat.receiptUrl}
            target="_blank"
            rel="noreferrer"
            aria-label={`Open ${caseBeat.receipt} on Instagram`}
            className="inline-flex items-center gap-2 rounded-full border border-foreground/12 bg-foreground/[0.03] py-1 pl-1 pr-3.5 text-[10px] font-black uppercase tracking-[0.12em] text-foreground/60 no-underline transition-colors hover:border-[#E11D48]/40 hover:text-[#E11D48]"
          >
            <Thumb tag={caseBeat.receipt} size={22} breaker />
            {caseBeat.receipt}
          </a>
          <button
            type="button"
            onClick={onOpenFile}
            className="inline-flex items-center gap-1.5 rounded-full border border-foreground/12 px-3.5 py-2 text-[10px] font-black uppercase tracking-[0.12em] text-foreground/45 transition-colors hover:border-[#E11D48]/40 hover:text-[#E11D48]"
          >
            open the case file <span aria-hidden>→</span>
          </button>
        </motion.div>
        </div>
      </div>
    </section>
  );
}

/* ── beat 4 · observations ───────────────────────────────────────────── */

function ObservationProof({ item, index, play, reduce }: {
  item: FeederVisitData['quiet'][number];
  index: number;
  play: boolean;
  reduce: boolean;
}) {
  const refs = item.postRefs || [];

  return (
    <div className="w-full max-w-[520px]">
      <div className="flex items-center justify-between gap-4">
        <Eyebrow>{refs.length ? `${refs.length} posts in view` : 'current window'}</Eyebrow>
        {item.note ? <span className="text-[9px] font-black uppercase tracking-[0.14em] text-[#E11D48]">{item.note}</span> : null}
      </div>
      <div className="mt-3 border-y border-foreground/10">
        {refs.slice(0, 3).map((title, refIndex) => (
          <motion.div
            key={title}
            className="grid grid-cols-[28px_minmax(0,1fr)] items-baseline gap-3 border-t border-foreground/8 py-2.5 first:border-t-0"
            initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 7px, 0)' }}
            animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
            transition={reduce ? { duration: 0 } : { delay: 0.04 + index * 0.035 + refIndex * 0.035, duration: 0.22, ease: READER_SLOT_EASE }}
          >
            <span className="text-[10px] font-black tabular-nums text-[#E11D48]">0{refIndex + 1}</span>
            <span className="truncate text-[11px] font-black text-foreground/58">{title}</span>
          </motion.div>
        ))}
      </div>
      {refs.length > 3 ? (
        <p className="mt-2 text-[9px] font-black uppercase tracking-[0.14em] text-foreground/28">+{refs.length - 3} more returns</p>
      ) : null}
    </div>
  );
}

function QuietBeat({ index = 3, itemIndex = 0 }: { index?: number; itemIndex?: number }) {
  const visit = useVisit();
  const item = visit.quiet[itemIndex];
  const ref = useRef<HTMLElement>(null);
  const inView = useInView(ref, { once: true, amount: 0.35 });
  const reduce = Boolean(useReducedMotion());
  if (!item) return null;

  return (
    <section ref={ref} data-beat={index} className={SHELL_SECTION}>
      <motion.div
        className={cn(SHELL_WRAP, SHELL_COLS)}
        variants={BEAT_CONTAINER}
        initial={reduce ? false : 'hidden'}
        animate={reduce || inView ? 'visible' : 'hidden'}
      >
        <motion.div variants={BEAT_LINE} className="min-w-0">
          <div className="flex items-center justify-between gap-5">
            <Eyebrow>also — quietly —</Eyebrow>
            <span className="text-[10px] font-black tabular-nums text-[#E11D48]">
              {String(itemIndex + 1).padStart(2, '0')} / {String(visit.quiet.length).padStart(2, '0')}
            </span>
          </div>
          <p className="mt-7 max-w-[620px] text-[clamp(26px,7vw,38px)] font-black leading-[1.06] tracking-[-0.035em] text-foreground/82 lg:text-[48px]">
            {item.line}
          </p>
        </motion.div>
        <motion.div variants={BEAT_LINE} className="mt-9 min-w-0 lg:mt-0 lg:justify-self-center">
          <ObservationProof item={item} index={itemIndex} play={inView} reduce={reduce} />
        </motion.div>
      </motion.div>
    </section>
  );
}

/* ── beat 5 · feederverse watch ───────────────────────────────────────── */

function WorldWatchStage({ visit }: { visit: FeederVisitData }) {
  const watch = visit.worldWatch;
  const signals = watch?.signals || visit.gameplan.slice(1);
  const triggers = watch?.triggers || [];

  return (
    <div className="w-full max-w-[560px]">
      <div className="flex items-baseline justify-between border-b border-foreground/10 pb-3">
        <Eyebrow>signals in the window</Eyebrow>
        <span className="text-[26px] font-black tabular-nums text-[#E11D48]">{signals.length}</span>
      </div>
      <div>
        {signals.slice(0, 3).map((signal, index) => (
          <motion.div key={signal} variants={BEAT_LINE} className="grid grid-cols-[34px_minmax(0,1fr)] gap-4 border-b border-foreground/10 py-4">
            <span className="text-[10px] font-black tabular-nums text-foreground/28">0{index + 1}</span>
            <p className="text-[12px] font-black leading-snug text-foreground/58 lg:text-[14px]">{signal}</p>
          </motion.div>
        ))}
      </div>
      {triggers.length ? (
        <motion.div variants={BEAT_LINE} className="mt-5 border-l-2 border-[#E11D48] pl-4">
          <div className="flex items-baseline justify-between gap-4">
            <Eyebrow accent>triggered this run</Eyebrow>
            <span className="text-[22px] font-black text-[#E11D48]">{triggers.length}</span>
          </div>
          <div className="mt-3 flex flex-wrap gap-x-4 gap-y-2">
            {triggers.map((trigger) => (
              <span key={trigger.label} className="text-[10px] font-black leading-tight text-foreground/52">
                <span className="text-foreground/78">{trigger.label}</span> · {trigger.postRef}
              </span>
            ))}
          </div>
        </motion.div>
      ) : null}
    </div>
  );
}

function GameplanBeat({ index = 4 }: { index?: number }) {
  const visit = useVisit();
  const rankPulse = visit.worldWatch?.rankPulse || visit.gameplan[0] || '';
  return (
    <BeatShell
      index={index}
      eyebrow="this week"
      left={
        <motion.div variants={BEAT_LINE} className="mt-7">
          <Eyebrow accent>rank pulse</Eyebrow>
          <p className="mt-4 max-w-[650px] text-[24px] font-black leading-[1.12] tracking-tight text-foreground sm:text-[29px] lg:text-[38px] 2xl:text-[44px]">{rankPulse}</p>
        </motion.div>
      }
      stage={<WorldWatchStage visit={visit} />}
    />
  );
}

/* ── beat 6 · next watch ───────────────────────────────────────────────── */

function NextWatchBeat({ index = 5 }: { index?: number }) {
  const visit = useVisit();

  return (
    <BeatShell
      index={index}
      eyebrow="next watch"
      accent
      left={
        <motion.p variants={BEAT_LINE} className="mt-6 max-w-[760px] text-[30px] font-black leading-[1.05] tracking-[-0.035em] text-foreground sm:text-[38px] lg:text-[50px] 2xl:text-[58px]">
          {visit.newHunch}
        </motion.p>
      }
      stage={
        <motion.div variants={BEAT_LINE} className="lg:border-l lg:border-foreground/10 lg:pl-[clamp(28px,3vw,56px)]">
          <Eyebrow>open tension</Eyebrow>
          <HunchRibbon fromLabel={`week ${visit.week} · current portrait`} toLabel="next window · unresolved" settled={false} className="mt-6" />
          <p className="mt-5 max-w-[420px] text-[12px] font-black leading-relaxed text-foreground/38">
            This is not a forecast. It is the pressure the next posts will answer.
          </p>
        </motion.div>
      }
    />
  );
}

/* ── beat 7 · sign-off: the dip ────────────────────────────────────────── */

function SignOffBeat({ index = 5 }: { index?: number }) {
  const visit = useVisit();
  const ref = useRef<HTMLDivElement>(null);
  const play = useInView(ref, { once: true, amount: 0.55 });
  const reduce = Boolean(useReducedMotion());

  return (
    <section
      data-beat={index}
      className="relative flex min-h-[100dvh] w-full snap-start items-center [scroll-snap-stop:always] lg:min-h-[72vh]"
    >
      <div ref={ref} className="relative mx-auto box-border w-full max-w-[808px] px-6 py-20 text-center">
        <motion.span
          aria-hidden
          className="mx-auto block h-[3px] w-24 origin-left bg-[#E11D48]"
          initial={reduce ? false : { transform: 'scaleX(0)' }}
          animate={reduce || play ? { transform: 'scaleX(1)' } : {}}
          transition={reduce ? { duration: 0 } : { duration: 0.24, ease: READER_SLOT_EASE }}
        />
        <motion.div
          initial={reduce ? false : { opacity: 0, transform: 'translate3d(0, 10px, 0)' }}
          animate={reduce || play ? { opacity: 1, transform: 'translate3d(0, 0, 0)' } : {}}
          transition={reduce ? { duration: 0 } : { delay: 0.04, duration: 0.26, ease: READER_SLOT_EASE }}
        >
          <Eyebrow className="mt-6">portrait closed · week {visit.week}</Eyebrow>
          <p className="mt-5 text-[clamp(42px,10vw,72px)] font-black leading-[0.92] tracking-[-0.055em] text-foreground">
            {visit.closingState || 'That’s the picture for now.'}
          </p>
          <p className="mt-5 text-[11px] font-black uppercase tracking-[0.16em] text-foreground/35">
            @{visit.handle} · {visit.plate.filled} active Bites
          </p>

          <div className="mt-9 inline-flex items-center gap-2.5 border-y border-foreground/12 px-4 py-3">
          <span className="inline-flex items-center gap-[5px]">
            {Array.from({ length: visit.plate.total }).map((_, i) => (
              <span
                key={i}
                aria-hidden
                className={cn(
                  'h-[7px] w-[7px] rounded-full',
                  i < visit.plate.filled ? 'bg-[#E11D48]' : 'bg-foreground/15',
                )}
              />
            ))}
          </span>
          <span className="text-[10px] font-black uppercase tracking-[0.14em] text-foreground/50">
            plate {visit.plate.filled}/{visit.plate.total} · next visit building
          </span>
          </div>
        </motion.div>
      </div>
    </section>
  );
}

/* ── progress rail — the wedge eats a dot per beat ─────────────────────── */

const RAIL_STEP = 22;

function ProgressRail({ active, chomp, count, onSelect }: { active: number; chomp: boolean; count: number; onSelect?: (index: number) => void }) {
  const reduce = Boolean(useReducedMotion());
  return (
    <div
      aria-hidden={onSelect ? undefined : true}
      aria-label={onSelect ? 'Visit beats' : undefined}
      className="pointer-events-none absolute top-1/2 z-20 block -translate-y-1/2"
      style={{ height: (count - 1) * RAIL_STEP + 14, right: 'max(12px, env(safe-area-inset-right))', left: 'auto' }}
    >
      {Array.from({ length: count }).map((_, i) => {
        const dot = (
          <span
            className={cn(
              'block h-[5px] w-[5px] rounded-full transition-opacity duration-300',
              i <= active ? 'opacity-0' : 'bg-foreground/25 opacity-100',
            )}
          />
        );
        return onSelect ? (
          <button
            key={i}
            type="button"
            aria-label={`Go to beat ${i + 1}`}
            onClick={() => onSelect(i)}
            className="pointer-events-auto absolute left-1/2 grid h-[16px] w-[16px] -translate-x-1/2 place-items-center"
            style={{ top: i * RAIL_STEP }}
          >
            {dot}
          </button>
        ) : (
          <span key={i} className="absolute left-1/2 -translate-x-1/2" style={{ top: i * RAIL_STEP + 5 }}>
            {dot}
          </span>
        );
      })}
      <motion.span
        className="absolute left-1/2 h-[14px] w-[14px] -translate-x-1/2 rounded-full"
        style={{ background: wedgeGradient(70), rotate: 90 }}
        animate={{ top: active * RAIL_STEP }}
        transition={
          reduce
            ? { duration: 0 }
            : { top: { type: 'spring', duration: 0.3, bounce: 0 } }
        }
      >
        {/* the chomp: a nearly-shut mouth crossfades over the open one */}
        <motion.span
          className="absolute inset-0 rounded-full"
          style={{ background: wedgeGradient(8) }}
          animate={!reduce && chomp ? { opacity: [0, 1, 0] } : { opacity: 0 }}
          transition={{ duration: reduce ? 0 : 0.18, times: [0, 0.5, 1], ease: READER_SLOT_EASE }}
        />
      </motion.span>
    </div>
  );
}

/* ── desktop shelf rail — his past visits, one line each ───────────────── */

function ShelfRail() {
  const visit = useVisit();
  return (
    <aside
      aria-label="Past visits"
      className="pointer-events-none fixed left-5 top-1/2 z-20 hidden w-[150px] -translate-y-1/2 xl:block 2xl:left-12 2xl:w-[200px]"
    >
      <Eyebrow>past visits</Eyebrow>
      <div className="mt-4 border-l border-foreground/10">
        <div className="border-l-2 border-[#E11D48] py-2 pl-4 -ml-px">
          <p className="text-[9px] font-black uppercase tracking-[0.16em] text-[#E11D48]">week {visit.week} · now</p>
          <p className="mt-1 text-[13px] font-black leading-tight tracking-tight text-foreground">
            {visit.cover.headline.join(' ')}
          </p>
        </div>
        {visit.shelf.map((v) => (
          <div key={v.week} className="py-2 pl-4">
            <p className="text-[9px] font-black uppercase tracking-[0.16em] text-foreground/30">
              week {v.week} · {v.when}
            </p>
            <p className="mt-1 text-[13px] font-black leading-tight tracking-tight text-foreground/45">{v.line}</p>
          </div>
        ))}
      </div>
    </aside>
  );
}

/* ── the visit ─────────────────────────────────────────────────────────── */

function useIsDesktop() {
  const [desktop, setDesktop] = useState(false);

  useEffect(() => {
    const query = window.matchMedia('(min-width: 1024px)');
    const update = () => setDesktop(query.matches);
    update();
    query.addEventListener('change', update);
    return () => query.removeEventListener('change', update);
  }, []);

  return desktop;
}

const DECK_VARIANTS = {
  enter: (direction: number) => ({
    opacity: 0,
    transform: `translate3d(0, ${direction > 0 ? '16vh' : '-16vh'}, 0)`,
  }),
  center: {
    opacity: 1,
    transform: 'translate3d(0, 0, 0)',
    transition: {
      opacity: { duration: 0.5, ease: VISIT_EASE },
      transform: { duration: 0.65, ease: VISIT_EASE },
    },
  },
  exit: (direction: number) => ({
    opacity: 0,
    transform: `translate3d(0, ${direction > 0 ? '-12vh' : '12vh'}, 0)`,
    transition: { duration: 0.4, ease: VISIT_EASE },
  }),
} as const;

const DECK_WHEEL_COOLDOWN_MS = 760;
const DECK_WHEEL_MIN_DELTA = 24;

export default function FeederVisit({ onClose, visit = VISIT }: { onClose?: () => void; visit?: FeederVisitData }) {
  const desktop = useIsDesktop();
  const reduce = Boolean(useReducedMotion());
  const [activeStep, setActiveStep] = useState(0);
  const [direction, setDirection] = useState(1);
  const [stepSettled, setStepSettled] = useState(false);
  const [chomp, setChomp] = useState(false);
  const [fileOpen, setFileOpen] = useState(false);
  const [openCaseIndex, setOpenCaseIndex] = useState(0);
  const activeStepRef = useRef(0);
  const wheelLockRef = useRef(0);
  const gestureStartRef = useRef<{ x: number; y: number } | null>(null);
  const caseBeats = visit.caseBeats?.length ? visit.caseBeats : [visit.caseBeat];
  const caseBeatCount = caseBeats.reduce((count, caseBeat) => count + (caseBeat.reader ? 3 : 1), 0);
  const storySteps: { key: string; beat: number; node: React.ReactNode }[] = [
    { key: 'cover', beat: 0, node: <CoverBeat /> },
    { key: 'callback', beat: 1, node: <CallbackBeat play={activeStep === 1 && stepSettled} /> },
  ];

  let biteIndex = 2;
  caseBeats.forEach((caseBeat, caseIndex) => {
    const openFile = () => {
      setOpenCaseIndex(caseIndex);
      setFileOpen(true);
    };
    if (caseBeat.reader) {
      const titleIndex = biteIndex++;
      const currentTitle = caseBeat.hook.join(' ');
      const titleChanges = Boolean(
        caseBeat.reader.previousTitle
        && caseBeat.reader.previousTitle !== currentTitle
        && caseBeat.reader.movement !== 'held',
      );
      const titleStages: BiteTitleStage[] = titleChanges ? ['previous', 'shift', 'current'] : ['current'];
      titleStages.forEach((stage) => storySteps.push({
        key: `${caseBeat.reader!.biteId}-title-${stage}`,
        beat: titleIndex,
        node: <BiteTitleBeat caseBeat={caseBeat} index={titleIndex} stage={stage} />,
      }));
      storySteps.push(
        {
          key: `${caseBeat.reader.biteId}-reading`,
          beat: biteIndex,
          node: <BiteReadingBeat caseBeat={caseBeat} index={biteIndex++} />,
        },
        {
          key: `${caseBeat.reader.biteId}-proof`,
          beat: biteIndex,
          node: <BiteProofBeat caseBeat={caseBeat} index={biteIndex++} onOpenFile={openFile} />,
        },
      );
      return;
    }
    storySteps.push({
      key: `${caseBeat.kicker}-${caseIndex}`,
      beat: biteIndex,
      node: <CaseBeat caseBeat={caseBeat} index={biteIndex++} onOpenFile={openFile} />,
    });
  });

  let postCaseBeat = 2 + caseBeatCount;
  visit.quiet.forEach((item, itemIndex) => {
    storySteps.push({
      key: `quiet-${itemIndex}-${item.line}`,
      beat: postCaseBeat,
      node: <QuietBeat index={postCaseBeat++} itemIndex={itemIndex} />,
    });
  });
  storySteps.push(
    { key: 'gameplan', beat: postCaseBeat, node: <GameplanBeat index={postCaseBeat++} /> },
    { key: 'next-watch', beat: postCaseBeat, node: <NextWatchBeat index={postCaseBeat++} /> },
    { key: 'signoff', beat: postCaseBeat, node: <SignOffBeat index={postCaseBeat} /> },
  );
  const beatCount = postCaseBeat + 1;

  const activeBeat = storySteps[activeStep]?.beat ?? 0;

  useEffect(() => {
    activeStepRef.current = activeStep;
  }, [activeStep]);

  useEffect(() => {
    activeStepRef.current = 0;
    setActiveStep(0);
    setDirection(1);
    setStepSettled(false);
  }, [visit.handle, visit.week]);

  useEffect(() => {
    if (activeBeat === 0) return;
    const frame = window.requestAnimationFrame(() => setChomp(true));
    const timer = window.setTimeout(() => setChomp(false), 360);
    return () => {
      window.cancelAnimationFrame(frame);
      window.clearTimeout(timer);
    };
  }, [activeBeat]);

  const goToStep = useCallback((index: number) => {
    const clamped = Math.max(0, Math.min(storySteps.length - 1, index));
    if (clamped === activeStepRef.current) return;
    setStepSettled(false);
    setDirection(clamped > activeStepRef.current ? 1 : -1);
    activeStepRef.current = clamped;
    setActiveStep(clamped);
  }, [storySteps.length]);

  const step = useCallback((delta: -1 | 1) => {
    goToStep(activeStepRef.current + delta);
  }, [goToStep]);

  const goToBeat = (beat: number) => {
    const index = storySteps.findIndex((storyStep) => storyStep.beat === beat);
    if (index >= 0) goToStep(index);
  };

  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.defaultPrevented || fileOpen) return;
      const target = event.target as HTMLElement | null;
      if (target && /^(input|textarea|select)$/i.test(target.tagName)) return;
      const forward = event.key === 'ArrowDown' || event.key === 'PageDown' || event.key === ' ';
      const backward = event.key === 'ArrowUp' || event.key === 'PageUp';
      if (!forward && !backward) return;
      event.preventDefault();
      goToStep(activeStepRef.current + (forward ? 1 : -1));
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [fileOpen, goToStep]);

  const onDeckWheel = (event: React.WheelEvent<HTMLDivElement>) => {
    if (fileOpen || Math.abs(event.deltaY) < DECK_WHEEL_MIN_DELTA) return;
    const now = performance.now();
    if (now - wheelLockRef.current < DECK_WHEEL_COOLDOWN_MS) return;
    wheelLockRef.current = now;
    step(event.deltaY > 0 ? 1 : -1);
  };

  const onDeckPointerDown = (event: React.PointerEvent<HTMLDivElement>) => {
    if (event.pointerType === 'mouse' && event.button !== 0) return;
    event.currentTarget.setPointerCapture(event.pointerId);
    gestureStartRef.current = { x: event.clientX, y: event.clientY };
  };

  const onDeckPointerUp = (event: React.PointerEvent<HTMLDivElement>) => {
    const start = gestureStartRef.current;
    gestureStartRef.current = null;
    if (event.currentTarget.hasPointerCapture(event.pointerId)) event.currentTarget.releasePointerCapture(event.pointerId);
    if (!start || fileOpen) return;
    const deltaY = start.y - event.clientY;
    const deltaX = start.x - event.clientX;
    if (Math.abs(deltaY) < 46 || Math.abs(deltaY) < Math.abs(deltaX) * 1.2) return;
    const now = performance.now();
    if (now - wheelLockRef.current < DECK_WHEEL_COOLDOWN_MS) return;
    wheelLockRef.current = now;
    step(deltaY > 0 ? 1 : -1);
  };

  return (
    <VisitContext.Provider value={visit}>
      <div
        data-active-beat={activeBeat}
        data-active-step={activeStep}
        className="relative h-[100dvh] overflow-hidden bg-background text-foreground"
        style={{ width: '100vw', maxWidth: '100vw' }}
      >
        {!fileOpen &&
          (onClose ? (
            <button
              type="button"
              onClick={onClose}
              aria-label="Close the visit"
              className="fixed right-4 top-4 z-30 inline-flex h-9 w-9 items-center justify-center rounded-full border border-foreground/12 bg-background/70 text-[14px] font-black text-foreground/55 backdrop-blur-sm transition-colors hover:text-foreground sm:right-6 sm:top-6"
            >
              ✕
            </button>
          ) : (
            <Link
              href="/read"
              aria-label="Close the visit"
              className="fixed right-4 top-4 z-30 inline-flex h-9 w-9 items-center justify-center rounded-full border border-foreground/12 bg-background/70 text-[14px] font-black text-foreground/55 no-underline backdrop-blur-sm transition-colors hover:text-foreground sm:right-6 sm:top-6"
            >
              ✕
            </Link>
          ))}

        <ProgressRail active={activeBeat} chomp={chomp} count={beatCount} onSelect={desktop ? goToBeat : undefined} />
        {activeBeat < beatCount - 1 ? <ShelfRail /> : null}

        <div
          className="relative h-[100dvh] w-screen max-w-[100vw] touch-none overflow-hidden"
          onWheel={onDeckWheel}
          onPointerDown={onDeckPointerDown}
          onPointerUp={onDeckPointerUp}
          onPointerCancel={() => { gestureStartRef.current = null; }}
        >
          <AnimatePresence initial={false} custom={direction} mode="popLayout">
            <motion.div
              key={`${visit.handle}:${visit.week}:${storySteps[activeStep]?.key}`}
              data-story-step={activeStep}
              custom={direction}
              variants={DECK_VARIANTS}
              initial={reduce ? { opacity: 0 } : 'enter'}
              animate={reduce ? { opacity: 1 } : 'center'}
              exit={reduce ? { opacity: 0, transition: { duration: 0.16 } } : 'exit'}
              onAnimationComplete={() => {
                if (activeStepRef.current === activeStep) setStepSettled(true);
              }}
              className="absolute inset-0 flex overflow-hidden"
            >
              <div className="m-auto w-screen max-w-[100vw] min-w-0">{storySteps[activeStep]?.node}</div>
            </motion.div>
          </AnimatePresence>
        </div>

        <CaseFileSheet open={fileOpen} onClose={() => setFileOpen(false)} caseIndex={openCaseIndex} />
      </div>
    </VisitContext.Provider>
  );
}
