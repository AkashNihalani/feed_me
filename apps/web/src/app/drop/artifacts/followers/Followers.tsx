import Link from 'next/link';
import type { CSSProperties } from 'react';

export type FollowerRunWindow = {
  id: string;
  label: string;
  range: string;
  startFollowers: number;
  endFollowers: number;
  baselineDelta: number | null;
};

type RunView = FollowerRunWindow & {
  delta: number;
  percent: number;
};

const NUMBER = new Intl.NumberFormat('en-US');
const REVEAL_START_MS = 220;
const REVEAL_STEP_MS = 340;

function signedNumber(value: number): string {
  if (!Number.isFinite(value) || Math.abs(value) < 0.5) return '0';
  return `${value > 0 ? '+' : '−'}${NUMBER.format(Math.abs(Math.round(value)))}`;
}

function signedPercent(value: number): string {
  if (!Number.isFinite(value) || Math.abs(value) < 0.005) return '0.00%';
  return `${value > 0 ? '+' : '−'}${Math.abs(value).toFixed(2)}%`;
}

function baselineRead(delta: number, baseline: number | null): string {
  if (baseline == null || !Number.isFinite(baseline) || Math.abs(baseline) < 0.5) return 'baseline unavailable';
  if (Math.abs(delta) < 0.5) return 'flat vs usual';
  if (Math.sign(delta) !== Math.sign(baseline)) return 'opposite usual';
  const multiple = Math.abs(delta / baseline);
  return Math.abs(multiple - 1) < 0.05 ? 'near usual' : `${multiple.toFixed(1)}× usual`;
}

function RunTile({ run, index, count }: { run: RunView; index: number; count: number }) {
  const isCurrent = index === 0;
  const baseline = baselineRead(run.delta, run.baselineDelta);
  const style = {
    '--tile-fill': isCurrent ? '#E11D48' : index === 1 ? '#34343A' : '#26262B',
    '--reveal-delay': `${REVEAL_START_MS + (count - index - 1) * REVEAL_STEP_MS}ms`,
  } as CSSProperties;

  return (
    <article
      aria-label={`${run.label}, ${run.range}, ${signedNumber(run.delta)} followers, ${signedPercent(run.percent)}, baseline comparison: ${baseline}`}
      className="follower-tile relative w-full overflow-hidden rounded-[3px] border border-white/12 bg-[#0A0A0C]"
      data-current={isCurrent ? 'true' : 'false'}
      data-run={run.id}
    >
      <div aria-hidden="true" className="follower-tile-reveal absolute inset-[1px] rounded-[2px]" style={style}>
        <div className="flex h-full flex-col justify-between px-3.5 py-3 sm:px-5 sm:py-4">
          <div className="flex items-center justify-between gap-4">
            <span className="text-[9px] font-black uppercase tracking-[0.17em] text-white/78 sm:text-[10px]">{run.label}</span>
            <span className="text-right text-[8px] font-black uppercase tracking-[0.13em] text-white/48 sm:text-[9px]">{run.range}</span>
          </div>

          <div className="flex items-end justify-between gap-5">
            <div className="min-w-0">
              <span className="mb-1 flex items-center whitespace-nowrap text-[8px] font-black uppercase tracking-[0.16em] text-white/52 sm:text-[9px]">
                {run.delta < 0 ? 'Drop' : run.delta > 0 ? 'Rise' : 'Flat'}
                <span className="mx-1 text-white/28">·</span>
                <span className="text-white/82">{baseline}</span>
              </span>
              <span className="follower-delta block font-black text-white tabular-nums">
                {signedNumber(run.delta)}
              </span>
            </div>
            <div className="shrink-0 text-right">
              <span className="mb-1 block text-[8px] font-black uppercase tracking-[0.16em] text-white/52 sm:text-[9px]">Change</span>
              <span className="follower-percent block font-black text-white tabular-nums">
                {signedPercent(run.percent)}
              </span>
            </div>
          </div>
        </div>
      </div>
    </article>
  );
}

export default function Followers({
  windows,
  embedded = false,
  contextLabel = 'Feeder Reader',
}: {
  windows: FollowerRunWindow[];
  embedded?: boolean;
  contextLabel?: string;
}) {
  const runs: RunView[] = windows.slice(0, 3).map((window) => {
    const delta = window.endFollowers - window.startFollowers;
    return {
      ...window,
      delta,
      percent: window.startFollowers > 0 ? (delta / window.startFollowers) * 100 : 0,
    };
  });

  if (runs.length === 0) return null;

  const previousDelta = runs[1]?.delta ?? 0;
  const takeaway = runs[0]!.delta > 0 && previousDelta > 0
    ? `${(runs[0]!.delta / previousDelta).toFixed(1)}× the previous run.`
    : 'Same window. Different result.';
  const Root = embedded ? 'section' : 'main';

  return (
    <Root
      aria-label="Follower growth across comparable runs"
      className={`followers-page flex w-full items-center overflow-x-hidden bg-black px-3 text-white sm:px-8 lg:px-12 ${embedded ? 'box-border h-[100dvh] min-h-0' : 'min-h-[100svh] min-h-[100dvh]'}`}
      data-embedded={embedded ? 'true' : 'false'}
      data-testid="follower-runs"
      style={{
        paddingTop: 'max(28px, env(safe-area-inset-top, 0px))',
        paddingBottom: 'calc(56px + env(safe-area-inset-bottom, 0px))',
      }}
    >
      <style>{`
        .follower-tile-reveal {
          background: var(--tile-fill);
          box-shadow: inset 0 1px 0 rgba(255,255,255,0.18), inset 0 -1px 0 rgba(0,0,0,0.22);
          overflow: hidden;
        }
        .follower-tile-reveal::after {
          content: '';
          position: absolute;
          inset: 0;
          z-index: 20;
          border-radius: inherit;
          background: #0A0A0C;
          pointer-events: none;
          transform: translate3d(0, -101%, 0);
          animation: follower-tile-curtain 820ms cubic-bezier(0.18, 0.86, 0.22, 1) var(--reveal-delay) both;
        }
        @keyframes follower-tile-curtain {
          from { transform: translate3d(0, 0, 0); }
          to { transform: translate3d(0, -101%, 0); }
        }
        .follower-tile { height: 106px; }
        .followers-heading {
          max-width: 11ch;
          font-size: clamp(38px, 9vw, 72px);
          font-weight: 900;
          line-height: 0.88;
          letter-spacing: -0.06em;
        }
        .follower-delta {
          font-size: clamp(31px, 9vw, 46px);
          line-height: 0.78;
          letter-spacing: -0.055em;
        }
        .follower-percent {
          font-size: clamp(24px, 7vw, 38px);
          line-height: 0.8;
          letter-spacing: -0.05em;
        }
        @media (min-width: 640px) {
          .follower-tile { height: 124px; }
        }
        @media (max-height: 700px) and (max-width: 639px) {
          .followers-page { align-items: flex-start; }
          .followers-title-row { margin-top: 14px; }
          .followers-section { margin-top: 18px; }
          .follower-tile { height: 94px; }
          .followers-footer { margin-top: 12px; padding-top: 10px; }
        }
        @media (prefers-reduced-motion: reduce) {
          .follower-tile-reveal::after {
            animation: none;
          }
        }
      `}</style>

      <div className="mx-auto w-full max-w-[720px]">
        <div className="flex items-center justify-between gap-5">
          <span className="text-[9px] font-black uppercase tracking-[0.2em] text-[#E11D48]">Follower run</span>
          {embedded ? (
            <span className="text-[9px] font-black uppercase tracking-[0.16em] text-white/54">
              {contextLabel}
            </span>
          ) : (
            <Link
              className="text-[9px] font-black uppercase tracking-[0.16em] text-white/54 transition-colors hover:text-white focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]"
              href="/drop/artifacts"
            >
              All artifacts
            </Link>
          )}
        </div>

        <div className="followers-title-row mt-7 flex items-end justify-between gap-6 sm:mt-10">
          <h1 className="followers-heading">
            Growth accelerated.
          </h1>
          <span className="hidden shrink-0 text-[9px] font-black uppercase tracking-[0.16em] text-white/44 sm:block">01 / 03</span>
        </div>

        <section aria-label="Follower growth across three comparable runs" className="followers-section mt-8 w-full sm:mt-10 sm:max-w-[600px]">
          <div className="grid w-full gap-2 sm:gap-3" data-testid="follower-tile-stack">
            {runs.map((run, index) => (
              <RunTile count={runs.length} index={index} key={run.id} run={run} />
            ))}
          </div>
        </section>

        <div className="followers-footer mt-6 flex w-full items-center justify-between gap-5 border-t border-white/12 pt-4 sm:mt-8 sm:max-w-[600px]">
          <span className="text-[9px] font-black uppercase tracking-[0.15em] text-white/48">{takeaway}</span>
          <span className="text-[9px] font-black uppercase tracking-[0.15em] text-white/34">Usual · account baseline</span>
        </div>
      </div>
    </Root>
  );
}
