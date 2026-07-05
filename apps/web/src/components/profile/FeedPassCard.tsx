'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { ArrowUpRight } from 'lucide-react';

type Metrics = { likes: string; comments: string; views: string; postsTracked: string };
type Feeder = {
  handle: string;
  isAnchor: boolean;
  profilePicUrl?: string | null;
  followerCount?: number | null;
  metrics: Metrics;
};
type Feed = { id: string; title: string; feeders: Feeder[]; metrics: Metrics };

type FeedPassCardProps = {
  feeds: Feed[];
  slotPlanPrice: number;
  slotPostsCap: number;
  onManageSubscription?: () => void;
  manageBusy?: boolean;
};

function parseMetric(value: string | number | undefined) {
  if (typeof value === 'number') return value;
  if (!value) return 0;
  const raw = String(value).replace(/,/g, '');
  const n = Number(raw);
  return Number.isFinite(n) ? n : 0;
}

const OVERAGE_RATE = 15;

function FeedMeLogo({ className = '' }: { className?: string }) {
  return (
    <svg className={className} viewBox="0 0 512 512" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
      <rect x="70" y="70" width="372" height="372" rx="88" stroke="white" strokeWidth="34" />
      <path d="M256.427 176C209.902 176 172.19 213.712 172.19 260.237C172.19 306.762 209.902 344.474 256.427 344.474C302.952 344.474 340.664 306.762 340.664 260.237V227.827L280.449 244.736L295.564 176H256.427Z" fill="white" />
      <circle cx="355.5" cy="167.5" r="27.5" fill="white" />
    </svg>
  );
}

export default function FeedPassCard({
  feeds,
  slotPlanPrice,
  slotPostsCap,
  onManageSubscription,
  manageBusy = false,
}: FeedPassCardProps) {
  const [visibleSide, setVisibleSide] = useState<'front' | 'back'>('front');
  const [isCompressed, setIsCompressed] = useState(false);
  const flipLockRef = useRef(false);
  const flipTimerRef = useRef<number | null>(null);
  const sideTimerRef = useRef<number | null>(null);

  const billing = useMemo(() => {
    const allFeeders = feeds.flatMap((f) => f.feeders || []);
    const feederCount = allFeeders.length;
    const baseCost = feederCount * slotPlanPrice;
    const overages = allFeeders
      .map((f) => {
        const posts = parseMetric(f.metrics?.postsTracked);
        const excess = Math.max(0, posts - slotPostsCap);
        return { handle: f.handle, posts, excess, cost: excess * OVERAGE_RATE };
      })
      .filter((o) => o.excess > 0)
      .sort((a, b) => b.excess - a.excess);
    const overageCost = overages.reduce((s, o) => s + o.cost, 0);
    return { feederCount, baseCost, overages, overageCost, total: baseCost + overageCost };
  }, [feeds, slotPlanPrice, slotPostsCap]);

  const feederCount = billing.feederCount;
  const overageCount = billing.overages.length;
  const isBackVisible = visibleSide === 'back';
  const toggleFlip = () => {
    if (flipLockRef.current) return;
    flipLockRef.current = true;
    setIsCompressed(true);
    if (sideTimerRef.current !== null) {
      window.clearTimeout(sideTimerRef.current);
    }
    if (flipTimerRef.current !== null) {
      window.clearTimeout(flipTimerRef.current);
    }
    sideTimerRef.current = window.setTimeout(() => {
      setVisibleSide((side) => (side === 'front' ? 'back' : 'front'));
      window.requestAnimationFrame(() => setIsCompressed(false));
      sideTimerRef.current = null;
    }, 170);
    flipTimerRef.current = window.setTimeout(() => {
      flipLockRef.current = false;
      flipTimerRef.current = null;
    }, 430);
  };

  useEffect(() => {
    return () => {
      if (sideTimerRef.current !== null) {
        window.clearTimeout(sideTimerRef.current);
      }
      if (flipTimerRef.current !== null) {
        window.clearTimeout(flipTimerRef.current);
      }
    };
  }, []);

  return (
    <div className="w-full">
      <div
        role="button"
        tabIndex={0}
        aria-label={isBackVisible ? 'Feed Pass billing breakdown. Tap to flip back.' : `Feed Pass. ${feederCount} active feeders. Tap for billing.`}
        onKeyDown={(event) => {
          if (event.key === 'Enter' || event.key === ' ') {
            event.preventDefault();
            toggleFlip();
          }
        }}
        onClick={toggleFlip}
        className="feedpass-card relative w-full cursor-pointer select-none outline-none"
        data-compressed={isCompressed ? 'true' : 'false'}
        data-side={visibleSide}
      >
        {!isBackVisible ? (
          <div className="feedpass-face feedpass-front">
            <div className="feedpass-base feedpass-base-front" />
            <div className="feedpass-shine" />
            <div className="feedpass-surface-glow" />

            <div className="relative z-10 flex h-full flex-col justify-between p-5 sm:p-6 lg:p-6 xl:p-7">
              <div className="flex items-center justify-between gap-3">
                <span className="text-[14px] font-black uppercase tracking-[0.14em] text-[#FFE4E6] sm:text-[16px] lg:text-[18px] xl:text-[18px]">
                  Feed Pass
                </span>
                <div className="flex items-center gap-1.5 rounded-full border border-white/16 bg-white/[0.14] px-3 py-1.5">
                  <div className="h-[7px] w-[7px] rounded-full bg-white shadow-[0_0_8px_rgba(255,255,255,0.72)]" />
                  <span className="text-[10px] font-bold uppercase tracking-[0.14em] text-white/90 sm:text-[12px]">Active</span>
                </div>
              </div>

              <div className="flex flex-1 items-end justify-end">
                <div className="text-right">
                  <div className="text-[88px] font-black leading-none tracking-[-0.04em] text-white sm:text-[110px] lg:text-[140px] xl:text-[160px]">
                    {feederCount}
                  </div>
                  <div className="mt-1 text-[12px] font-black uppercase tracking-[0.14em] text-white/68 sm:text-[14px] lg:text-[14px] xl:text-[16px]">
                    Active Feeders
                  </div>
                </div>
              </div>

              <div className="flex items-end justify-between gap-4">
                <FeedMeLogo className="h-[52px] w-[52px] drop-shadow-[0_0_12px_rgba(255,255,255,0.14)] sm:h-[60px] sm:w-[60px] lg:h-[68px] lg:w-[68px] xl:h-[74px] xl:w-[74px]" />
                <div className="text-right">
                  <div className="text-[10px] font-bold uppercase tracking-[0.14em] text-white/34 sm:text-[10px] lg:text-[12px]">
                    tap to view billing
                  </div>
                </div>
              </div>
            </div>
          </div>
        ) : (
          <div className="feedpass-face feedpass-back">
            <div className="feedpass-base feedpass-base-back" />

            <div className="relative z-10 flex h-full flex-col justify-between p-5 sm:p-6 lg:p-7">
              <div className="flex items-center justify-between">
                <span className="text-[12px] font-black uppercase tracking-[0.14em] text-white/56 sm:text-[12px]">
                  Billing summary
                </span>
                <span className="text-[10px] font-black uppercase tracking-[0.14em] text-white/42 sm:text-[12px]">
                  {feederCount} feeder{feederCount === 1 ? '' : 's'} active
                </span>
              </div>

              <div className="flex flex-col gap-3.5 sm:gap-4.5">
                <div className="flex items-baseline justify-between">
                  <div className="text-[12px] font-black uppercase tracking-[0.14em] text-white/54 sm:text-[12px]">Base cost</div>
                  <div className="flex items-baseline gap-1.5">
                    <span className="text-[22px] font-black tracking-[-0.04em] text-white/92 sm:text-[22px]">
                      ₹{billing.baseCost.toLocaleString('en-IN')}
                    </span>
                    <span className="text-[10px] font-black uppercase tracking-[0.14em] text-white/38 sm:text-[10px]">
                      {billing.feederCount}×₹{slotPlanPrice}
                    </span>
                  </div>
                </div>
                <div className="h-px bg-white/[0.1]" />
                <div className="flex items-baseline justify-between">
                  <div className="text-[12px] font-black uppercase tracking-[0.14em] text-white/54 sm:text-[12px]">Overages</div>
                  <div className="flex items-baseline gap-1.5">
                    <span className={billing.overageCost > 0
                      ? 'text-[22px] font-black tracking-[-0.04em] text-[var(--fm-accent-bright)] sm:text-[22px]'
                      : 'text-[22px] font-black tracking-[-0.04em] text-white/92 sm:text-[22px]'}>
                      {billing.overageCost > 0 ? `₹${billing.overageCost.toLocaleString('en-IN')}` : '₹0'}
                    </span>
                    <span className="text-[10px] font-black uppercase tracking-[0.14em] text-white/38 sm:text-[10px]">
                      {overageCount > 0 ? `${overageCount} over cap` : 'none'}
                    </span>
                  </div>
                </div>
              </div>

              <div className="flex items-center justify-between rounded-[18px] border border-[var(--fm-accent-soft)]/18 bg-[var(--fm-accent)] px-4 py-3.5 shadow-[0_10px_32px_rgb(var(--fm-accent-rgb)/0.2)] sm:rounded-[18px] sm:py-4">
                <div>
                  <div className="text-[10px] font-black uppercase tracking-[0.14em] text-white/70 sm:text-[10px]">Total this cycle</div>
                  <div className="text-[28px] font-black leading-none tracking-[-0.04em] text-white sm:text-[34px]">
                    ₹{billing.total.toLocaleString('en-IN')}
                  </div>
                </div>
                <div className="text-right">
                  <div className="text-[10px] font-black uppercase tracking-[0.14em] text-white/68 sm:text-[10px]">{slotPostsCap} posts</div>
                  <div className="text-[8px] font-black uppercase tracking-[0.14em] text-white/54 sm:text-[10px]">incl. per feeder</div>
                </div>
              </div>

              <div className="flex items-center justify-between">
                <button
                  type="button"
                  onPointerDown={(event) => {
                    event.stopPropagation();
                  }}
                  onClick={(event) => {
                    event.stopPropagation();
                    onManageSubscription?.();
                  }}
                  disabled={manageBusy}
                  className="flex items-center gap-2 text-left transition-opacity duration-200 hover:opacity-80 active:opacity-60 disabled:cursor-not-allowed disabled:opacity-40"
                >
                  <div className="flex h-6 w-6 items-center justify-center rounded-full bg-white/[0.12] sm:h-7 sm:w-7">
                    <ArrowUpRight size={12} className="text-white/72" />
                  </div>
                  <span className="text-[10px] font-black uppercase tracking-[0.14em] text-white/58 sm:text-[12px]">
                    Full breakdown
                  </span>
                </button>
                <span className="text-[8px] font-black uppercase tracking-[0.14em] text-white/28 sm:text-[10px]">tap to flip</span>
              </div>
            </div>
          </div>
        )}
      </div>

      <style>{`
        .feedpass-card {
          aspect-ratio: 1.586;
          transform: translate3d(0, 0, 0) scaleX(1);
          transform-origin: center;
          isolation: isolate;
          transition:
            transform 170ms cubic-bezier(0.32, 0.72, 0, 1),
            filter 220ms ease;
          will-change: transform;
        }
        .feedpass-card[data-compressed='true'] {
          transform: translate3d(0, 0, 0) scaleX(0.035);
          filter: saturate(0.96);
        }
        @media (hover: hover) and (pointer: fine) {
          .feedpass-card[data-side='front'][data-compressed='false']:hover {
            transform: translate3d(0, -3px, 0) scale(1.006);
            filter: saturate(1.03);
          }
        }
        .feedpass-face {
          position: absolute;
          inset: 0;
          overflow: hidden;
          border-radius: 30px;
          box-shadow:
            0 22px 54px rgb(var(--fm-accent-rgb)/0.12),
            0 8px 24px rgba(0, 0, 0, 0.32);
        }
        .feedpass-back {
          box-shadow:
            0 24px 60px rgba(0, 0, 0, 0.48),
            inset 0 0 0 1px rgba(255, 255, 255, 0.05);
        }
        .feedpass-base {
          position: absolute;
          inset: 0;
        }
        .feedpass-base-front {
          background:
            linear-gradient(180deg, rgba(255,255,255,0.12) 0%, rgba(255,255,255,0.025) 28%, rgba(0,0,0,0.12) 100%),
            radial-gradient(circle at 82% 14%, rgba(255,255,255,0.16), transparent 24%),
            linear-gradient(140deg, #5f071d 0%, #9f1239 27%, #E11D48 52%, #be123c 72%, #4c0519 100%);
        }
        .feedpass-base-back {
          background:
            linear-gradient(180deg, rgba(255,255,255,0.06) 0%, rgba(255,255,255,0.018) 24%, rgba(0,0,0,0.22) 100%),
            radial-gradient(ellipse 92% 70% at 82% 12%, rgba(255,255,255,0.08) 0%, transparent 56%),
            linear-gradient(160deg, #0b0b0b 0%, #060606 44%, #020202 100%);
        }
        .feedpass-surface-glow {
          position: absolute;
          inset: 0;
          background: radial-gradient(circle at 16% 88%, rgba(255,255,255,0.07), transparent 24%);
          mix-blend-mode: screen;
        }
        .feedpass-shine {
          position: absolute;
          inset: 0;
          transform: translate3d(-38%, 0, 0);
          background:
            linear-gradient(58deg, rgba(255,255,255,0) 0%, rgba(255,255,255,0) 31%, rgba(255,255,255,0.25) 48%, rgba(244,63,94,0.13) 55%, rgba(255,255,255,0) 70%, rgba(255,255,255,0) 100%);
          opacity: 0.72;
          mix-blend-mode: screen;
          animation: feedpass-shine 5.8s ease-in-out infinite;
          will-change: transform, opacity;
        }
        @keyframes feedpass-shine {
          0%, 18% { transform: translate3d(-48%, 0, 0); opacity: 0.34; }
          42% { transform: translate3d(42%, 0, 0); opacity: 0.76; }
          100% { transform: translate3d(42%, 0, 0); opacity: 0.34; }
        }
        @media (prefers-reduced-motion: reduce) {
          .feedpass-card,
          .feedpass-shine {
            animation: none !important;
            transition: none !important;
          }
        }
        @media (min-width: 640px) {
          .feedpass-face {
            border-radius: 32px;
          }
        }
      `}</style>
    </div>
  );
}
