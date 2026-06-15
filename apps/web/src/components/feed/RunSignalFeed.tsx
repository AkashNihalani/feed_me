'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { Activity, ArrowUpRight, ChevronRight, Eye, RefreshCcw, Sparkles, TrendingUp, X, Zap } from 'lucide-react';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { GRID_ITEM_EASE, GRID_LAYOUT_SPRING } from '@/lib/motion';
import { cn } from '@/lib/utils';
import type { RunSignal, RunSignalEvidence, RunSignalKind, RunSignalMetric } from '@/types/runSignals';

type RunSignalFeedProps = {
  feedId: string;
  selectedHandle?: string;
};

const KIND_META: Record<RunSignalKind, {
  label: string;
  tone: string;
  icon: typeof TrendingUp;
  verb: string;
}> = {
  trend: { label: 'Trend', tone: 'text-[#E11D48] bg-[#E11D48]/10 border-[#E11D48]/18', icon: TrendingUp, verb: 'Do more' },
  watch: { label: 'Watch', tone: 'text-[#7C2D12] bg-[#F97316]/10 border-[#F97316]/18 dark:text-[#FDBA74]', icon: Eye, verb: 'Watch' },
  easy_win: { label: 'Easy win', tone: 'text-[#0F766E] bg-[#14B8A6]/10 border-[#14B8A6]/18 dark:text-[#5EEAD4]', icon: Zap, verb: 'Try next' },
  what_changed: { label: 'Changed', tone: 'text-[#4338CA] bg-[#6366F1]/10 border-[#6366F1]/18 dark:text-[#A5B4FC]', icon: RefreshCcw, verb: 'New shift' },
  durability: { label: 'Durability', tone: 'text-[#BE123C] bg-[#FB7185]/10 border-[#FB7185]/18 dark:text-[#FDA4AF]', icon: Activity, verb: 'Has legs' },
};

function mediaProxyUrl(postKey: string | null | undefined): string {
  const key = String(postKey || '').trim();
  return key ? `/api/media?postKey=${encodeURIComponent(key)}&role=thumbnail` : '';
}

function formatMultiple(value: number | null): string | null {
  if (value == null || !Number.isFinite(value)) return null;
  return `${value.toFixed(value >= 10 ? 0 : 1).replace(/\.0$/, '')}x`;
}

function evidenceStat(post: RunSignalEvidence): string {
  if (post.placed) return post.placed.replace(/^top\s+/i, 'Top ');
  const multiple = formatMultiple(post.views_vs_usual);
  if (multiple) return `${multiple} usual`;
  if (post.legs) return 'Has legs';
  return 'Proof';
}

function trimTitle(value: string): string {
  return String(value || 'Evidence post').replace(/\s+/g, ' ').trim();
}

function kindMeta(kind: RunSignalKind) {
  return KIND_META[kind] || KIND_META.trend;
}

function EvidenceThumb({
  post,
  index,
  compact = false,
  showTitle = true,
}: {
  post: RunSignalEvidence;
  index: number;
  compact?: boolean;
  showTitle?: boolean;
}) {
  const src = post.thumbnail_url || mediaProxyUrl(post.post_key);
  const [dead, setDead] = useState(false);
  const title = trimTitle(post.title);

  return (
    <div
      className={cn(
        'group/evidence relative isolate overflow-hidden border border-black/[0.06] bg-[#f7eef2] shadow-[0_14px_30px_-24px_rgba(15,23,42,0.55)] dark:border-white/[0.08] dark:bg-white/[0.06]',
        compact ? 'aspect-[4/5] rounded-[16px]' : 'aspect-[16/11] rounded-[18px]',
      )}
    >
      {src && !dead ? (
        // eslint-disable-next-line @next/next/no-img-element -- dynamic post thumbnails are served through the media proxy
        <img
          src={src}
          alt=""
          className="h-full w-full object-cover transition-transform duration-500 ease-out group-hover/evidence:scale-[1.035]"
          loading="lazy"
          decoding="async"
          onError={() => setDead(true)}
        />
      ) : (
        <div className="flex h-full w-full items-center justify-center bg-[radial-gradient(circle_at_28%_18%,rgba(225,29,72,0.2),transparent_44%),linear-gradient(135deg,#fff,#f9eef2)] px-4 text-center text-[9px] font-black uppercase tracking-[0.14em] text-[#E11D48]/58 dark:bg-[radial-gradient(circle_at_28%_18%,rgba(251,113,133,0.22),transparent_44%),linear-gradient(135deg,#171717,#09090b)] dark:text-[#FDA4AF]/58">
          {title.slice(0, 34)}
        </div>
      )}
      <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.02),transparent_38%,rgba(0,0,0,0.78))]" />
      <div className="absolute left-2.5 top-2.5 rounded-full bg-black/58 px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.13em] text-white backdrop-blur-md">
        {String(index + 1).padStart(2, '0')}
      </div>
      <div className="absolute inset-x-0 bottom-0 p-2.5">
        <div className="inline-flex rounded-full bg-white/18 px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-white backdrop-blur-md">
          {evidenceStat(post)}
        </div>
        {showTitle && (
          <div className="mt-1.5 line-clamp-2 text-[9px] font-black uppercase leading-snug tracking-[0.08em] text-white/92">
            {title}
          </div>
        )}
      </div>
    </div>
  );
}

function SignalEvidenceCarousel({ signal }: { signal: RunSignal }) {
  const reduceMotion = useReducedMotion();
  const [activeIndex, setActiveIndex] = useState(0);
  const total = signal.evidence.length;

  useEffect(() => {
    if (reduceMotion || total <= 1) return undefined;
    const timer = window.setInterval(() => {
      setActiveIndex((current) => (current + 1) % total);
    }, 3600);
    return () => window.clearInterval(timer);
  }, [reduceMotion, total]);

  if (total === 0) return null;

  return (
    <div
      className="relative mt-4 h-[236px] overflow-visible sm:mt-0 sm:h-full sm:min-h-[242px] md:min-h-[258px]"
      data-signal-evidence-carousel="true"
    >
      <div className="absolute inset-y-0 left-1/2 w-[min(178px,58vw)] -translate-x-1/2 sm:w-[min(188px,100%)] md:w-[min(206px,100%)]">
        {signal.evidence.map((post, index) => {
          const offset = (index - activeIndex + total) % total;
          const position = offset === 0 ? 'center' : offset === 1 ? 'right' : offset === total - 1 ? 'left' : 'hidden';
          const active = position === 'center';

          const styleByPosition = {
            center: {
              x: '0%',
              y: 0,
              rotate: 0,
              scale: 1,
              opacity: 1,
              filter: 'blur(0px) brightness(1)',
              zIndex: 30,
            },
            right: {
              x: '35%',
              y: 16,
              rotate: 7,
              scale: 0.84,
              opacity: 0.6,
              filter: 'blur(1.1px) brightness(0.82)',
              zIndex: 18,
            },
            left: {
              x: '-35%',
              y: 16,
              rotate: -7,
              scale: 0.84,
              opacity: 0.52,
              filter: 'blur(1.25px) brightness(0.78)',
              zIndex: 16,
            },
            hidden: {
              x: '0%',
              y: 24,
              rotate: 0,
              scale: 0.72,
              opacity: 0,
              filter: 'blur(4px) brightness(0.7)',
              zIndex: 0,
            },
          }[position];

          return (
            <motion.div
              key={`${signal.id}:proof-carousel:${post.post_key || post.title}:${index}`}
              className="absolute inset-0"
              initial={false}
              animate={styleByPosition}
              transition={
                reduceMotion
                  ? { duration: 0.01 }
                  : {
                      x: { duration: 0.78, ease: [0.16, 1, 0.3, 1] },
                      y: { duration: 0.78, ease: [0.16, 1, 0.3, 1] },
                      rotate: { duration: 0.78, ease: [0.16, 1, 0.3, 1] },
                      scale: { duration: 0.78, ease: [0.16, 1, 0.3, 1] },
                      opacity: { duration: 0.52, ease: GRID_ITEM_EASE },
                      filter: { duration: 0.64, ease: GRID_ITEM_EASE },
                    }
              }
              style={{
                pointerEvents: active ? 'auto' : 'none',
                transformOrigin: 'center center',
                willChange: 'transform, opacity, filter',
              }}
            >
              <EvidenceThumb post={post} index={index} compact showTitle={active} />
            </motion.div>
          );
        })}
      </div>

      <div
        aria-hidden="true"
        className="pointer-events-none absolute bottom-5 left-1/2 h-8 w-[76%] -translate-x-1/2 rounded-full bg-black/16 blur-2xl dark:bg-black/46"
      />

      {total > 1 && (
        <div className="absolute bottom-2 left-1/2 z-40 flex -translate-x-1/2 gap-1.5 rounded-full bg-black/38 px-2 py-1 backdrop-blur-md">
          {signal.evidence.map((post, index) => (
            <span
              key={`${signal.id}:dot:${post.post_key || post.title}:${index}`}
              className={cn(
                'h-1.5 rounded-full transition-all duration-300',
                index === activeIndex ? 'w-4 bg-white' : 'w-1.5 bg-white/42',
              )}
            />
          ))}
        </div>
      )}
    </div>
  );
}

function MetricPill({ metric }: { metric: RunSignalMetric }) {
  return (
    <span
      className={cn(
        'inline-flex min-w-0 items-baseline gap-1.5 rounded-full border px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.11em]',
        metric.accent
          ? 'border-[#E11D48]/18 bg-[#E11D48]/10 text-[#E11D48] dark:text-[#FB7185]'
          : 'border-black/[0.06] bg-white/60 text-black/48 dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/44',
      )}
    >
      <span className="text-[10px] text-black dark:text-white">{metric.value}</span>
      <span className="truncate">{metric.detail || metric.label}</span>
    </span>
  );
}

function SignalCard({
  signal,
  index,
  onOpen,
}: {
  signal: RunSignal;
  index: number;
  onOpen: (signal: RunSignal) => void;
}) {
  const reduceMotion = useReducedMotion();
  const meta = kindMeta(signal.kind);
  const Icon = meta.icon;
  const delay = Math.min(index * 0.035, 0.18);

  return (
    <motion.button
      type="button"
      layout
      onClick={() => onOpen(signal)}
      data-run-signal-card="true"
      className="group/signal relative isolate grid w-full grid-cols-1 overflow-hidden rounded-[22px] border border-black/[0.06] bg-white/88 p-4 text-left shadow-[0_16px_44px_-34px_rgba(15,23,42,0.48),inset_0_1px_0_rgba(255,255,255,0.78)] transition-colors dark:border-white/[0.08] dark:bg-white/[0.045] sm:grid-cols-[minmax(0,1fr)_minmax(236px,0.42fr)] sm:gap-5 sm:p-5 lg:grid-cols-[minmax(0,1fr)_minmax(250px,0.42fr)]"
      initial={reduceMotion ? false : { opacity: 0, y: 18, scale: 0.982 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      exit={{ opacity: 0, y: -10, scale: 0.986 }}
      whileTap={{ scale: 0.992 }}
      transition={{
        layout: GRID_LAYOUT_SPRING,
        opacity: { duration: 0.22, delay, ease: GRID_ITEM_EASE },
        y: { duration: 0.3, delay, ease: GRID_ITEM_EASE },
        scale: { duration: 0.28, delay, ease: GRID_ITEM_EASE },
      }}
      style={{ contentVisibility: 'auto', containIntrinsicSize: '260px', willChange: 'transform, opacity' }}
    >
      <div
        aria-hidden
        className="pointer-events-none absolute inset-0 -z-10 opacity-80"
        style={{
          background: 'radial-gradient(ellipse 72% 82% at 0% 0%, rgba(225,29,72,0.11), transparent 54%)',
        }}
      />

      <div className="min-w-0">
        <div className="flex flex-wrap items-center gap-2">
          <span className={cn('inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em]', meta.tone)}>
            <Icon size={11} strokeWidth={3} />
            {meta.label}
          </span>
          <span className="rounded-full border border-black/[0.05] bg-white/62 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-black/40 dark:border-white/[0.07] dark:bg-white/[0.05] dark:text-white/36">
            {signal.account}
          </span>
          <span className="text-[8px] font-black uppercase tracking-[0.16em] text-black/30 dark:text-white/24">
            {signal.runLabel}
          </span>
        </div>

        <h3 className="mt-3 max-w-[760px] text-[25px] font-black leading-[0.98] tracking-normal text-black dark:text-white sm:text-[30px] lg:text-[34px]">
          {signal.headline}
        </h3>
        <p className="mt-3 line-clamp-3 max-w-[780px] text-[12.5px] font-bold leading-relaxed text-black/58 dark:text-white/46 sm:text-[13.5px]">
          {signal.explainer}
        </p>

        <div className="mt-4 flex flex-wrap gap-1.5">
          {signal.metrics.slice(0, 4).map((metric) => (
            <MetricPill key={`${signal.id}:${metric.label}`} metric={metric} />
          ))}
        </div>

        <div className="mt-4 flex items-center justify-between gap-3 border-t border-black/[0.055] pt-3 dark:border-white/[0.06]">
          <span className="text-[10px] font-black uppercase tracking-[0.16em] text-black/34 dark:text-white/28">
            {meta.verb}
          </span>
          <span className="inline-flex items-center gap-1 text-[10px] font-black uppercase tracking-[0.14em] text-[#E11D48] opacity-80 transition group-hover/signal:translate-x-0.5 group-hover/signal:opacity-100">
            Open evidence
            <ChevronRight size={12} strokeWidth={3} />
          </span>
        </div>
      </div>

      <SignalEvidenceCarousel signal={signal} />
    </motion.button>
  );
}

function SignalPopup({
  signal,
  onClose,
}: {
  signal: RunSignal | null;
  onClose: () => void;
}) {
  useEffect(() => {
    if (!signal) return undefined;
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', handleKey);
    return () => window.removeEventListener('keydown', handleKey);
  }, [onClose, signal]);

  if (typeof document === 'undefined') return null;

  return createPortal(
    <AnimatePresence>
      {signal && (
        <motion.div
          className="fixed inset-0 z-[1100] flex items-end justify-center bg-black/42 px-3 pb-3 pt-[calc(14px+env(safe-area-inset-top))] text-black backdrop-blur-[10px] dark:text-white sm:items-center sm:p-5"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.22, ease: GRID_ITEM_EASE }}
          onClick={onClose}
        >
          <motion.div
            className="relative flex max-h-[88dvh] w-full max-w-[1180px] flex-col overflow-hidden rounded-[28px] border border-white/70 bg-[#FAF9F6] shadow-[0_28px_90px_rgba(0,0,0,0.34)] dark:border-white/[0.08] dark:bg-[#08080a] sm:max-h-[86dvh] sm:grid sm:grid-cols-[minmax(0,1fr)_360px]"
            initial={{ opacity: 0, y: 34, scale: 0.982, filter: 'blur(10px)' }}
            animate={{ opacity: 1, y: 0, scale: 1, filter: 'blur(0px)' }}
            exit={{ opacity: 0, y: 30, scale: 0.982, filter: 'blur(10px)' }}
            transition={{
              opacity: { duration: 0.24, ease: GRID_ITEM_EASE },
              y: { type: 'spring', stiffness: 250, damping: 30, mass: 0.9 },
              scale: { duration: 0.26, ease: GRID_ITEM_EASE },
              filter: { duration: 0.24, ease: GRID_ITEM_EASE },
            }}
            onClick={(event) => event.stopPropagation()}
          >
            <button
              type="button"
              onClick={onClose}
              className="absolute right-3 top-3 z-20 flex h-10 w-10 items-center justify-center rounded-full border border-black/[0.08] bg-white/86 text-black shadow-[0_12px_30px_-18px_rgba(0,0,0,0.42)] transition hover:bg-[#E11D48] hover:text-white active:scale-95 dark:border-white/[0.08] dark:bg-white/[0.08] dark:text-white"
              aria-label="Close signal"
            >
              <X size={18} strokeWidth={3} />
            </button>

            <div className="hide-scrollbar min-h-0 overflow-y-auto px-5 pb-6 pt-7 sm:px-7 sm:pb-8 sm:pt-8 lg:px-8">
              <div className="flex flex-wrap items-center gap-2 pr-12">
                {(() => {
                  const meta = kindMeta(signal.kind);
                  const Icon = meta.icon;
                  return (
                    <span className={cn('inline-flex items-center gap-1.5 rounded-full border px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em]', meta.tone)}>
                      <Icon size={11} strokeWidth={3} />
                      {meta.label}
                    </span>
                  );
                })()}
                <span className="text-[9px] font-black uppercase tracking-[0.17em] text-black/36 dark:text-white/30">
                  {signal.account} · {signal.runLabel}
                </span>
              </div>

              <h2 className="mt-4 max-w-[780px] text-[36px] font-black leading-[0.94] tracking-normal text-black dark:text-white sm:text-[48px] lg:text-[58px]">
                {signal.headline}
              </h2>
              <p className="mt-5 max-w-[760px] text-[15px] font-bold leading-relaxed text-black/66 dark:text-white/56 sm:text-[17px]">
                {signal.explainer}
              </p>

              <div className="mt-6 flex flex-wrap gap-2">
                {signal.metrics.map((metric) => (
                  <MetricPill key={`popup:${signal.id}:${metric.label}`} metric={metric} />
                ))}
              </div>

              <div className="mt-7 border-t border-black/[0.08] pt-5 dark:border-white/[0.08]">
                <div className="mb-3 text-[10px] font-black uppercase tracking-[0.2em] text-black/38 dark:text-white/30">Evidence posts</div>
                <div className="grid grid-cols-1 gap-3 sm:grid-cols-3">
                  {signal.evidence.map((post, index) => (
                    <div key={`popup-proof:${signal.id}:${post.post_key || post.title}:${index}`} className="min-w-0">
                      <EvidenceThumb post={post} index={index} showTitle={false} />
                      <div className="mt-2.5 flex items-start justify-between gap-2">
                        <div className="min-w-0">
                          <div className="line-clamp-2 break-words text-[11px] font-black uppercase leading-snug tracking-[0.08em] text-black/72 dark:text-white/72">
                            {trimTitle(post.title)}
                          </div>
                          <div className="mt-1 text-[9px] font-black uppercase tracking-[0.12em] text-black/34 dark:text-white/28">
                            {[post.placed, formatMultiple(post.views_vs_usual), post.legs ? 'legs' : null].filter(Boolean).join(' · ') || 'proof'}
                          </div>
                        </div>
                        {post.post_url && (
                          <a
                            href={post.post_url}
                            target="_blank"
                            rel="noopener noreferrer"
                            className="flex h-8 w-8 shrink-0 items-center justify-center rounded-full bg-[#E11D48] text-white shadow-[0_10px_22px_-14px_rgba(225,29,72,0.8)] transition hover:scale-105 active:scale-95"
                            aria-label="Open evidence post"
                          >
                            <ArrowUpRight size={14} strokeWidth={3} />
                          </a>
                        )}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            </div>

            <aside className="hidden min-h-0 border-l border-black/[0.08] bg-white/72 p-5 dark:border-white/[0.08] dark:bg-white/[0.03] sm:block">
              <div className="text-[10px] font-black uppercase tracking-[0.18em] text-black/38 dark:text-white/30">Quick read</div>
              <div className="mt-4 grid gap-2">
                {signal.metrics.slice(0, 5).map((metric) => (
                  <div
                    key={`rail:${signal.id}:${metric.label}`}
                    className={cn(
                      'rounded-[16px] border p-3',
                      metric.accent
                        ? 'border-[#E11D48]/18 bg-[#E11D48]/10'
                        : 'border-black/[0.06] bg-white/76 dark:border-white/[0.08] dark:bg-white/[0.05]',
                    )}
                  >
                    <div className={cn('text-[24px] font-black leading-none', metric.accent ? 'text-[#E11D48] dark:text-[#FB7185]' : 'text-black dark:text-white')}>
                      {metric.value}
                    </div>
                    <div className="mt-1 text-[8px] font-black uppercase tracking-[0.14em] text-black/36 dark:text-white/30">
                      {metric.label}{metric.detail ? ` · ${metric.detail}` : ''}
                    </div>
                  </div>
                ))}
              </div>
              <div className="mt-5 rounded-[18px] border border-[#E11D48]/12 bg-[#E11D48]/[0.055] p-4">
                <div className="flex items-center gap-2 text-[9px] font-black uppercase tracking-[0.16em] text-[#E11D48] dark:text-[#FB7185]">
                  <Sparkles size={12} strokeWidth={3} />
                  Feed Me read
                </div>
                <p className="mt-2 text-[12px] font-bold leading-relaxed text-black/58 dark:text-white/46">
                  This signal is generated from the latest completed run and pinned to the posts that prove it.
                </p>
              </div>
            </aside>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>,
    document.body,
  );
}

export default function RunSignalFeed({ feedId, selectedHandle = 'all' }: RunSignalFeedProps) {
  const [signals, setSignals] = useState<RunSignal[]>([]);
  const [loaded, setLoaded] = useState(false);
  const [activeSignal, setActiveSignal] = useState<RunSignal | null>(null);

  useEffect(() => {
    let cancelled = false;

    async function loadSignals() {
      setLoaded(false);
      try {
        const params = new URLSearchParams({ feedId, limit: '24' });
        if (selectedHandle && selectedHandle !== 'all') params.set('handle', selectedHandle);
        const response = await fetch(`/api/feed/run-signals?${params.toString()}`, {
          cache: 'no-store',
          credentials: 'include',
        });
        if (!response.ok) {
          if (!cancelled) setSignals([]);
          return;
        }
        const payload = await response.json() as { signals?: RunSignal[] };
        if (!cancelled) setSignals(Array.isArray(payload.signals) ? payload.signals : []);
      } catch {
        if (!cancelled) setSignals([]);
      } finally {
        if (!cancelled) setLoaded(true);
      }
    }

    loadSignals();
    return () => {
      cancelled = true;
    };
  }, [feedId, selectedHandle]);

  const accountCount = useMemo(() => new Set(signals.map((signal) => signal.account)).size, [signals]);
  const closePopup = useCallback(() => setActiveSignal(null), []);

  return (
    <section className="mt-7 w-full sm:mt-9 lg:mt-10" data-run-signal-feed="true">
      <div className="mb-3 flex items-end justify-between gap-3 border-b border-black/[0.08] pb-2.5 dark:border-white/[0.08]">
        <div className="min-w-0">
          <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.18em] text-[#E11D48]">
            <span className="h-2 w-2 rounded-full bg-[#E11D48] shadow-[0_0_16px_rgba(225,29,72,0.42)]" />
            Latest intelligence
          </div>
          <h2 className="mt-1 text-[26px] font-black leading-none tracking-normal text-black dark:text-white sm:text-[34px]">
            Feedbook
          </h2>
        </div>
        <div className="shrink-0 rounded-full border border-black/[0.06] bg-white/70 px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.14em] text-black/42 shadow-[inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/34">
          {loaded ? `${signals.length} signals${accountCount > 1 ? ` · ${accountCount} feeders` : ''}` : 'Reading'}
        </div>
      </div>

      <AnimatePresence mode="popLayout">
        {signals.length > 0 ? (
          <motion.div layout className="grid gap-3 lg:gap-4 xl:grid-cols-2">
            {signals.map((signal, index) => (
              <SignalCard key={signal.id} signal={signal} index={index} onOpen={setActiveSignal} />
            ))}
          </motion.div>
        ) : loaded ? (
          <motion.div
            layout
            className="rounded-[22px] border border-black/[0.06] bg-white/72 p-5 text-[12px] font-bold leading-relaxed text-black/46 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-white/[0.08] dark:bg-white/[0.04] dark:text-white/38"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -8 }}
            transition={{ duration: 0.22, ease: GRID_ITEM_EASE }}
          >
            Feedbook will appear when a feeder completes a run and generates frontend intelligence.
          </motion.div>
        ) : (
          <motion.div
            layout
            className="grid gap-3 xl:grid-cols-2"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            {[0, 1].map((index) => (
              <div key={`signal-skeleton:${index}`} className="h-[246px] animate-pulse rounded-[22px] border border-black/[0.05] bg-white/54 dark:border-white/[0.06] dark:bg-white/[0.04]" />
            ))}
          </motion.div>
        )}
      </AnimatePresence>

      <SignalPopup signal={activeSignal} onClose={closePopup} />
    </section>
  );
}
