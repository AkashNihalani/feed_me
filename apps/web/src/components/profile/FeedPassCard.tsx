'use client';

import { useState, useEffect, useRef, useMemo, useCallback } from 'react';
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
  const [isFlipped, setIsFlipped] = useState(false);
  const [isHovered, setIsHovered] = useState(false);
  const [time, setTime] = useState(0);
  const cardRef = useRef<HTMLDivElement>(null);
  const timeRef = useRef<number>(0);
  const swayRef = useRef<number>(0);
  const swayAngle = useRef({ x: 0, y: 0, dx: 0.06, dy: 0.09 });

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

  useEffect(() => {
    const tick = () => {
      setTime((t) => t + 0.008);
      timeRef.current = requestAnimationFrame(tick);
    };
    timeRef.current = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(timeRef.current);
  }, []);

  useEffect(() => {
    if (isHovered || isFlipped) return;
    const card = cardRef.current;
    if (!card) return;
    const sway = () => {
      const a = swayAngle.current;
      a.x += a.dx;
      a.y += a.dy;
      if (Math.abs(a.x) > 4) a.dx *= -1;
      if (Math.abs(a.y) > 5.5) a.dy *= -1;
      card.style.transform = `rotateX(${a.x}deg) rotateY(${a.y}deg)`;
      swayRef.current = requestAnimationFrame(sway);
    };
    swayRef.current = requestAnimationFrame(sway);
    return () => cancelAnimationFrame(swayRef.current);
  }, [isHovered, isFlipped]);

  const handleMouseMove = useCallback((e: React.MouseEvent<HTMLDivElement>) => {
    if (isFlipped) return;
    const card = cardRef.current;
    if (!card) return;
    const rect = card.getBoundingClientRect();
    const cx = rect.left + rect.width / 2;
    const cy = rect.top + rect.height / 2;
    const ax = ((e.clientY - cy) / (rect.height / 2)) * 8;
    const ay = (-(e.clientX - cx) / (rect.width / 2)) * 8;
    card.style.transform = `rotateX(${ax}deg) rotateY(${ay}deg)`;
  }, [isFlipped]);

  const handleMouseEnter = useCallback(() => {
    setIsHovered(true);
    cancelAnimationFrame(swayRef.current);
  }, []);

  const handleMouseLeave = useCallback(() => {
    setIsHovered(false);
    const card = cardRef.current;
    if (card && !isFlipped) {
      card.style.transition = 'transform 0.6s cubic-bezier(0.32, 0.72, 0, 1)';
      card.style.transform = 'rotateX(0deg) rotateY(0deg)';
      window.setTimeout(() => {
        if (card) card.style.transition = 'transform 0.1s ease-out';
      }, 600);
    }
  }, [isFlipped]);

  const handleFlip = useCallback(() => {
    setIsFlipped((f) => !f);
    const card = cardRef.current;
    if (card) {
      card.style.transition = 'transform 0.6s cubic-bezier(0.32, 0.72, 0, 1)';
      card.style.transform = `rotateY(${!isFlipped ? 180 : 0}deg)`;
      window.setTimeout(() => {
        if (card) card.style.transition = 'transform 0.1s ease-out';
      }, 600);
    }
  }, [isFlipped]);

  const feederCount = billing.feederCount;
  const overageCount = billing.overages.length;

  return (
    <div className="w-full" style={{ perspective: '3000px' }}>
      <div
        ref={cardRef}
        role="button"
        tabIndex={0}
        aria-label={isFlipped ? 'Feed Pass billing breakdown. Tap to flip back.' : `Feed Pass. ${feederCount} active feeders. Tap for billing.`}
        onKeyDown={(e) => {
          if (e.key === 'Enter' || e.key === ' ') {
            e.preventDefault();
            handleFlip();
          }
        }}
        onClick={handleFlip}
        onMouseMove={handleMouseMove}
        onMouseEnter={handleMouseEnter}
        onMouseLeave={handleMouseLeave}
        className="relative w-full cursor-pointer select-none"
        style={{
          transformStyle: 'preserve-3d',
          transition: 'transform 0.1s ease-out',
          aspectRatio: '1.586',
          willChange: 'transform',
        }}
      >
        <div
          className="absolute inset-0 overflow-hidden rounded-[30px] sm:rounded-[32px]"
          style={{
            backfaceVisibility: 'hidden',
            WebkitBackfaceVisibility: 'hidden',
            transform: 'translateZ(1px)',
            boxShadow: '0 22px 64px rgba(225,29,72,0.14), 0 8px 24px rgba(0,0,0,0.42)',
          }}
        >
          <div
            className="absolute inset-0"
            style={{
              background:
                'linear-gradient(140deg, #5f071d 0%, #9f1239 24%, #E11D48 48%, #be123c 68%, #4c0519 100%)',
            }}
          />
          <div
            className="absolute inset-0"
            style={{
              background: `
                radial-gradient(ellipse 96% 82% at ${42 + Math.sin(time * 0.32) * 12}% ${48 + Math.cos(time * 0.35) * 12}%, rgba(255,255,255,0.12) 0%, transparent 66%),
                radial-gradient(ellipse 72% 58% at ${72 + Math.cos(time * 0.25) * 16}% ${26 + Math.sin(time * 0.28) * 18}%, rgba(244,63,94,0.38) 0%, transparent 54%),
                radial-gradient(ellipse 58% 62% at ${24 + Math.sin(time * 0.41) * 13}% ${72 + Math.cos(time * 0.23) * 10}%, rgba(127,29,29,0.36) 0%, transparent 52%)
              `,
            }}
          />
          <div
            className="absolute inset-0"
            style={{
              background:
                'linear-gradient(180deg, rgba(255,255,255,0.12) 0%, rgba(255,255,255,0.03) 22%, rgba(0,0,0,0.1) 100%)',
            }}
          />
          <div
            className="absolute inset-0"
            style={{
              background: `
                radial-gradient(circle at 80% 18%, rgba(255,255,255,0.14), transparent 18%),
                radial-gradient(circle at 12% 88%, rgba(255,255,255,0.08), transparent 22%)
              `,
              mixBlendMode: 'screen',
            }}
          />

          <div className="absolute inset-0 overflow-hidden">
            <div className="feedpass-stars-small" />
            <div className="feedpass-stars-medium" />
            <div className="feedpass-stars-large" />
            <div className="feedpass-stars-twinkle" />
            <div className="feedpass-particles" />
            <div
              className="absolute inset-0 feedpass-holo-band"
              style={{
                background:
                  'linear-gradient(56deg, rgba(255,255,255,0) 0%, rgba(255,255,255,0) 26%, rgba(225,29,72,0.16) 38%, rgba(255,255,255,0.34) 50%, rgba(225,29,72,0.16) 62%, rgba(255,255,255,0) 74%, rgba(255,255,255,0) 100%)',
                backgroundSize: '180% 100%',
              }}
            />
          </div>

            <div className="absolute inset-0 z-10 flex flex-col justify-between p-5 sm:p-6 lg:p-6 xl:p-7">
              <div className="flex items-center justify-between gap-3">
                <span className="text-[13px] font-black uppercase tracking-[0.16em] text-[#FFE4E6] sm:text-[15px] lg:text-[17px] xl:text-[18px]" style={{ textShadow: '0 0 18px rgba(225,29,72,0.55)' }}>
                  Feed Pass
                </span>
                <div className="flex items-center gap-1.5 rounded-full border border-white/16 bg-white/12 px-3 py-1.5 backdrop-blur-[8px]">
                  <div className="h-[7px] w-[7px] rounded-full bg-white shadow-[0_0_8px_rgba(255,255,255,0.72)] feedpass-dot-pulse" />
                  <span className="text-[10px] font-bold uppercase tracking-[0.14em] text-white/90 sm:text-[11px]">Active</span>
                </div>
              </div>

              <div className="flex flex-1 items-end justify-end">
                <div className="text-right">
                  <div
                    className="text-[88px] font-black leading-none tracking-[-0.05em] text-white sm:text-[110px] lg:text-[140px] xl:text-[160px]"
                    style={{ textShadow: '0 0 60px rgba(225,29,72,0.34), 0 0 120px rgba(225,29,72,0.14), 0 2px 4px rgba(0,0,0,0.45)' }}
                  >
                    {feederCount}
                  </div>
                  <div className="mt-1 text-[11px] font-black uppercase tracking-[0.15em] text-white/68 sm:text-[13px] lg:text-[14px] xl:text-[15px]">
                    Active Feeders
                  </div>
                </div>
              </div>

              <div className="flex items-end justify-between gap-4">
                <div className="feedpass-logo-glow">
                  <FeedMeLogo className="h-[52px] w-[52px] sm:h-[60px] sm:w-[60px] lg:h-[68px] lg:w-[68px] xl:h-[74px] xl:w-[74px]" />
                </div>
                <div className="text-right">
                  <div className="text-[9px] font-bold uppercase tracking-[0.12em] text-white/32 sm:text-[10px] lg:text-[11px]">tap to view billing</div>
                </div>
              </div>
            </div>
        </div>

        <div
          className="absolute inset-0 overflow-hidden rounded-[30px] sm:rounded-[32px]"
          style={{
            backfaceVisibility: 'hidden',
            WebkitBackfaceVisibility: 'hidden',
            transform: 'rotateY(180deg) translateZ(1px)',
            boxShadow: '0 28px 80px rgba(0,0,0,0.68), inset 0 0 0 1px rgba(255,255,255,0.05)',
          }}
        >
          {/* Deep black base with restrained glass reflections */}
          <div
            className="absolute inset-0"
            style={{
              background: 'linear-gradient(160deg, #0b0b0b 0%, #060606 44%, #020202 100%)',
            }}
          />
          <div
            className="absolute inset-0"
            style={{
              background:
                'radial-gradient(ellipse 92% 70% at 82% 12%, rgba(255,255,255,0.08) 0%, transparent 56%), radial-gradient(ellipse 68% 56% at 18% 100%, rgba(255,255,255,0.04) 0%, transparent 58%)',
            }}
          />
          <div
            className="absolute inset-0"
            style={{
              background:
                'linear-gradient(180deg, rgba(255,255,255,0.06) 0%, rgba(255,255,255,0.02) 18%, rgba(0,0,0,0) 44%, rgba(0,0,0,0.2) 100%)',
            }}
          />
          <div
            className="absolute inset-0 rounded-[30px] sm:rounded-[32px]"
            style={{ boxShadow: 'inset 0 1px 0 rgba(255,255,255,0.06), inset 0 -1px 0 rgba(0,0,0,0.4)' }}
          />

          <div className="absolute inset-0 z-10 flex flex-col justify-between p-5 sm:p-6 lg:p-7">
            {/* Top row — label + feeder count */}
            <div className="flex items-center justify-between">
              <span className="text-[11px] font-black uppercase tracking-[0.18em] text-white/56 sm:text-[12px]">
                Billing summary
              </span>
              <span className="text-[10px] font-black uppercase tracking-[0.14em] text-white/42 sm:text-[11px]">
                {feederCount} feeder{feederCount === 1 ? '' : 's'} active
              </span>
            </div>

            {/* Center content — base + overage as clean typography rows */}
            <div className="flex flex-col gap-3.5 sm:gap-4.5">
              <div className="flex items-baseline justify-between">
                <div className="text-[11px] font-black uppercase tracking-[0.14em] text-white/54 sm:text-[12px]">Base cost</div>
                <div className="flex items-baseline gap-1.5">
                  <span className="text-[20px] font-black tracking-[-0.03em] text-white/92 sm:text-[24px]">
                    ₹{billing.baseCost.toLocaleString('en-IN')}
                  </span>
                  <span className="text-[9px] font-black uppercase tracking-[0.1em] text-white/38 sm:text-[10px]">
                    {billing.feederCount}×₹{slotPlanPrice}
                  </span>
                </div>
              </div>
              <div className="h-px bg-white/[0.1]" />
              <div className="flex items-baseline justify-between">
                <div className="text-[11px] font-black uppercase tracking-[0.14em] text-white/54 sm:text-[12px]">Overages</div>
                <div className="flex items-baseline gap-1.5">
                  <span className={billing.overageCost > 0
                    ? 'text-[20px] font-black tracking-[-0.03em] text-[#E11D48] sm:text-[24px]'
                    : 'text-[20px] font-black tracking-[-0.03em] text-white/92 sm:text-[24px]'}>
                    {billing.overageCost > 0 ? `₹${billing.overageCost.toLocaleString('en-IN')}` : '₹0'}
                  </span>
                  <span className="text-[9px] font-black uppercase tracking-[0.1em] text-white/38 sm:text-[10px]">
                    {overageCount > 0 ? `${overageCount} over cap` : 'none'}
                  </span>
                </div>
              </div>
            </div>

            {/* Vibrant neon total pill */}
            <div
              className="flex items-center justify-between rounded-[16px] border border-[#FDA4AF]/20 bg-[#E11D48] px-4 py-3.5 sm:rounded-[18px] sm:py-4"
              style={{ boxShadow: '0 10px 36px rgba(225,29,72,0.24), 0 0 60px rgba(225,29,72,0.1)' }}
            >
              <div>
                <div className="text-[9px] font-black uppercase tracking-[0.14em] text-white/70 sm:text-[10px]">Total this cycle</div>
                <div className="text-[30px] font-black leading-none tracking-[-0.05em] text-white sm:text-[36px]">
                  ₹{billing.total.toLocaleString('en-IN')}
                </div>
              </div>
              <div className="text-right">
                <div className="text-[9px] font-black uppercase tracking-[0.12em] text-white/68 sm:text-[10px]">{slotPostsCap} posts</div>
                <div className="text-[8px] font-black uppercase tracking-[0.1em] text-white/54 sm:text-[9px]">incl. per feeder</div>
              </div>
            </div>

            {/* Manage button — minimal, anchored at bottom */}
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
                <span className="text-[10px] font-black uppercase tracking-[0.12em] text-white/58 sm:text-[11px]">
                  Full breakdown
                </span>
              </button>
              <span className="text-[8px] font-black uppercase tracking-[0.12em] text-white/28 sm:text-[9px]">tap to flip</span>
            </div>
          </div>
        </div>
      </div>

      <style>{`
        .feedpass-stars-small,
        .feedpass-stars-medium,
        .feedpass-stars-large,
        .feedpass-stars-twinkle {
          position: absolute;
          width: 100%;
          height: 100%;
        }
        .feedpass-stars-small {
          background-image:
            radial-gradient(1px 1px at 20px 30px, rgba(225,29,72,0.74), transparent),
            radial-gradient(1px 1px at 40px 70px, rgba(255,255,255,0.9), transparent),
            radial-gradient(1px 1px at 50px 160px, rgba(225,29,72,0.7), transparent),
            radial-gradient(1px 1px at 90px 40px, rgba(255,255,255,0.85), transparent),
            radial-gradient(1px 1px at 130px 80px, rgba(255,228,230,0.66), transparent),
            radial-gradient(1px 1px at 160px 120px, rgba(255,255,255,0.8), transparent);
          background-size: 200px 200px;
          opacity: 0.42;
        }
        .feedpass-stars-medium {
          background-image:
            radial-gradient(1.5px 1.5px at 150px 150px, rgba(225,29,72,0.68), transparent),
            radial-gradient(1.5px 1.5px at 220px 220px, rgba(255,255,255,0.88), transparent),
            radial-gradient(1.5px 1.5px at 80px 250px, rgba(255,228,230,0.62), transparent),
            radial-gradient(1.5px 1.5px at 180px 80px, rgba(255,255,255,0.82), transparent);
          background-size: 300px 300px;
          opacity: 0.4;
        }
        .feedpass-stars-large {
          background-image:
            radial-gradient(2px 2px at 100px 100px, rgba(225,29,72,0.75), transparent),
            radial-gradient(2px 2px at 200px 200px, rgba(255,255,255,0.92), transparent),
            radial-gradient(2px 2px at 300px 300px, rgba(225,29,72,0.62), transparent);
          background-size: 400px 400px;
          opacity: 0.5;
          animation: feedpass-stars-drift 100s linear infinite;
        }
        .feedpass-stars-twinkle {
          background-image:
            radial-gradient(2px 2px at 50px 50px, rgba(225,29,72,0.86), transparent),
            radial-gradient(2px 2px at 150px 150px, rgba(255,255,255,0.84), transparent),
            radial-gradient(2px 2px at 250px 250px, rgba(255,228,230,0.8), transparent);
          background-size: 300px 300px;
          opacity: 0;
          animation: feedpass-twinkle 4s ease-in-out infinite alternate;
        }
        .feedpass-particles {
          position: absolute;
          width: 100%;
          height: 100%;
          background-image:
            radial-gradient(1px 1px at 10% 10%, rgba(225,29,72,0.72), transparent),
            radial-gradient(1px 1px at 25% 25%, rgba(255,255,255,0.58), transparent),
            radial-gradient(1px 1px at 40% 40%, rgba(225,29,72,0.64), transparent),
            radial-gradient(1px 1px at 55% 55%, rgba(255,255,255,0.5), transparent),
            radial-gradient(1px 1px at 70% 70%, rgba(225,29,72,0.62), transparent),
            radial-gradient(1px 1px at 85% 85%, rgba(255,255,255,0.46), transparent),
            radial-gradient(1px 1px at 95% 15%, rgba(225,29,72,0.56), transparent),
            radial-gradient(1px 1px at 15% 85%, rgba(255,255,255,0.38), transparent);
          background-size: 150% 150%;
          animation: feedpass-particles-float 20s ease infinite;
          opacity: 0.56;
        }
        .feedpass-holo-band {
          animation: feedpass-holo 4.5s ease-in-out infinite;
          mix-blend-mode: screen;
        }
        .feedpass-dot-pulse {
          animation: feedpass-dot-pulse 2s ease-in-out infinite;
        }
        .feedpass-logo-glow {
          filter: drop-shadow(0 0 12px rgba(255,255,255,0.14));
        }
        .feedpass-scroll::-webkit-scrollbar {
          width: 3px;
        }
        .feedpass-scroll::-webkit-scrollbar-track {
          background: transparent;
        }
        .feedpass-scroll::-webkit-scrollbar-thumb {
          background: rgba(225,29,72,0.18);
          border-radius: 4px;
        }

        @keyframes feedpass-stars-drift {
          0% { background-position: 0 0, 0 0, 0 0; }
          100% { background-position: 400px 400px, 300px 300px, 200px 200px; }
        }
        @keyframes feedpass-twinkle {
          0% { opacity: 0.06; }
          50% { opacity: 0.56; }
          100% { opacity: 0.2; }
        }
        @keyframes feedpass-particles-float {
          0% { background-position: 0% 0%; }
          50% { background-position: 75% 75%; }
          100% { background-position: 0% 0%; }
        }
        @keyframes feedpass-holo {
          0% { background-position: -25% 0%; opacity: 0.55; }
          50% { background-position: 110% 0%; opacity: 0.95; }
          100% { background-position: -25% 0%; opacity: 0.55; }
        }
        @keyframes feedpass-dot-pulse {
          0%, 100% { box-shadow: 0 0 4px rgba(255,255,255,0.56); }
          50% { box-shadow: 0 0 10px rgba(255,255,255,0.9), 0 0 20px rgba(244,63,94,0.42); }
        }

        @media (prefers-reduced-motion: reduce) {
          .feedpass-stars-large,
          .feedpass-stars-twinkle,
          .feedpass-particles,
          .feedpass-holo-band,
          .feedpass-dot-pulse {
            animation: none !important;
          }
          .feedpass-stars-twinkle { opacity: 0.2; }
          .feedpass-dot-pulse { box-shadow: 0 0 6px rgba(255,255,255,0.72); }
        }
      `}</style>
    </div>
  );
}
