'use client';

import { type CSSProperties, useEffect, useMemo, useState } from 'react';
import { ArrowLeft, ArrowUpRight, BrainCircuit, X } from 'lucide-react';
import { useRouter } from 'next/navigation';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import { FEEDERBOARD_FOCUSES, focusForHandle, type FeederboardFocus, type MetricCard, type ProofBlock } from '@/data/feederboardOfficialPayloads';

type FeederboardClientProps = {
  feedId: string;
  selectedHandle?: string;
};

const APPLE_EASE = [0.32, 0.72, 0, 1] as const;

function mediaProxyUrl(postKey: string | null | undefined): string {
  const key = (postKey || '').trim();
  return key ? `/api/media?postKey=${encodeURIComponent(key)}&role=thumbnail` : '';
}


function MetricChip({ metric }: { metric: MetricCard }) {
  return (
    <div
      className={[
        'rounded-[14px] border px-3 py-2.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]',
        metric.accent
          ? 'border-[#E11D48]/22 bg-[#E11D48]/10 dark:border-[#FB7185]/24 dark:bg-[#FB7185]/12'
          : 'border-black/[0.06] bg-white dark:border-white/[0.08] dark:bg-white/[0.06]',
      ].join(' ')}
    >
      <div
        className={[
          'text-[8px] font-black uppercase tracking-[0.18em]',
          metric.accent ? 'text-[#9F1239]/74 dark:text-[#FDA4AF]/78' : 'text-black/36 dark:text-white/34',
        ].join(' ')}
      >
        {metric.label}
      </div>
      <div
        className={[
          'mt-1 text-[18px] font-black leading-none',
          metric.accent ? 'text-[#9F1239] dark:text-[#FFE4E6]' : 'text-black dark:text-white',
        ].join(' ')}
      >
        {metric.value}
      </div>
      {metric.detail && (
        <div className="mt-1 text-[9px] font-black uppercase tracking-[0.14em] text-black/32 dark:text-white/28">{metric.detail}</div>
      )}
    </div>
  );
}

function DefRow({ label, text }: { label: string; text: string }) {
  return (
    <>
      <dt className="text-[10px] font-black uppercase tracking-[0.18em] text-[#E11D48] sm:pt-1">{label}</dt>
      <dd className="mb-3 mt-1 text-[14.5px] font-bold leading-[1.65] text-black/64 dark:text-white/62 sm:mb-0 sm:mt-0">{text}</dd>
    </>
  );
}

function PatternList({ label, items }: { label: string; items: string[] }) {
  return (
    <div className="rounded-[22px] border border-black/[0.06] bg-white/72 p-5 shadow-[inset_0_1px_0_rgba(255,255,255,0.78)] dark:border-white/[0.08] dark:bg-white/[0.05] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]">
      <div className="text-[10px] font-black uppercase tracking-[0.2em] text-[#E11D48]">{label}</div>
      <ul className="mt-4 space-y-3 text-[14.5px] font-bold leading-[1.55] text-black/64 dark:text-white/62">
        {items.map((item, i) => (
          <li key={`${label}:${i}`} className="flex gap-3">
            <span className="mt-[0.62em] h-1.5 w-1.5 shrink-0 rounded-full bg-[#E11D48]" />
            <span>{item}</span>
          </li>
        ))}
      </ul>
    </div>
  );
}

function Thumb({
  post,
  className = '',
  index,
  total,
  showCaption = false,
}: {
  post: ProofBlock;
  className?: string;
  index?: number;
  total?: number;
  showCaption?: boolean;
}) {
  const fallback = mediaProxyUrl(post.post_key);
  const [dead, setDead] = useState(false);
  const microMetric = showCaption ? post.metrics.find((m) => !m.accent) || post.metrics[0] : null;

  return (
    <div className={['relative overflow-hidden bg-[#f5f0f2] dark:bg-white/[0.06]', className].join(' ')}>
      {!dead && fallback ? (
        // eslint-disable-next-line @next/next/no-img-element -- dynamic post thumbnails are served through the media proxy
        <img
          src={fallback}
          alt=""
          className="h-full w-full object-cover"
          onError={() => setDead(true)}
        />
      ) : (
        <div className="flex h-full w-full items-center justify-center bg-[radial-gradient(circle_at_28%_20%,rgba(225,29,72,0.16),transparent_42%),linear-gradient(135deg,#fff,#f9eef2)] px-5 text-center text-[11px] font-black uppercase tracking-[0.14em] text-[#E11D48]/42 dark:bg-[radial-gradient(circle_at_28%_20%,rgba(251,113,133,0.22),transparent_42%),linear-gradient(135deg,#171717,#09090b)] dark:text-[#FDA4AF]/54">
          {post.proof_label}
        </div>
      )}

      {showCaption && index !== undefined && total !== undefined && (
        <div className="pointer-events-none absolute right-2.5 top-2.5 z-10 flex h-6 items-center justify-center rounded-full bg-black/58 px-2 text-[9.5px] font-black tracking-wider text-white backdrop-blur-md">
          {index}/{total}
        </div>
      )}

      {showCaption && (
        <div className="pointer-events-none absolute inset-x-0 bottom-0 flex items-end justify-between gap-2 bg-[linear-gradient(0deg,rgba(0,0,0,0.74),transparent_82%)] px-3 pb-3 pt-12">
          <div className="line-clamp-2 min-w-0 text-[9px] font-black uppercase leading-snug tracking-[0.16em] text-white">
            {index !== undefined && <span className="opacity-66">{String(index).padStart(2, '0')} · </span>}
            {post.proof_label}
          </div>
          {microMetric && (
            <div className="shrink-0 rounded-full bg-white/22 px-2 py-1 text-[9px] font-black uppercase tracking-[0.14em] text-white backdrop-blur-md">
              {microMetric.value}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function FocusCard({
  focus,
  onOpen,
}: {
  focus: FeederboardFocus;
  onOpen: (focus: FeederboardFocus) => void;
}) {
  const [carouselIndex, setCarouselIndex] = useState(0);
  const [isPaused, setIsPaused] = useState(false);
  const [isDesktopStage, setIsDesktopStage] = useState(false);

  useEffect(() => {
    if (typeof window === 'undefined') return undefined;
    const query = window.matchMedia('(min-width: 1280px)');
    const sync = () => setIsDesktopStage(query.matches);
    sync();
    query.addEventListener('change', sync);
    return () => query.removeEventListener('change', sync);
  }, []);

  useEffect(() => {
    if (focus.proof_rail.length <= 1 || isPaused) return undefined;
    if (typeof window !== 'undefined' && window.matchMedia('(prefers-reduced-motion: reduce)').matches) return undefined;

    const interval = window.setInterval(() => {
      setCarouselIndex((current) => (current + 1) % focus.proof_rail.length);
    }, 2600);

    return () => window.clearInterval(interval);
  }, [focus.proof_rail.length, isPaused]);

  const sideOffset = isDesktopStage ? 202 : 124;

  return (
    <motion.button
      type="button"
      onClick={() => onOpen(focus)}
      onBlur={() => setIsPaused(false)}
      onFocus={() => setIsPaused(true)}
      onMouseEnter={() => setIsPaused(true)}
      onMouseLeave={() => setIsPaused(false)}
      initial={{ opacity: 0, y: 14, scale: 0.985 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={{ duration: 0.28, ease: APPLE_EASE }}
      whileTap={{ scale: 0.992 }}
      className="group relative overflow-hidden rounded-[30px] border border-black/[0.06] bg-white/82 p-4 text-left shadow-[0_26px_80px_-54px_rgba(15,23,42,0.75),inset_0_1px_0_rgba(255,255,255,0.8)] backdrop-blur-2xl dark:border-white/[0.08] dark:bg-[#101014]/86 dark:shadow-[0_28px_90px_-52px_rgba(0,0,0,0.95),inset_0_1px_0_rgba(255,255,255,0.08)] sm:p-6 lg:p-7 xl:p-8"
    >
      <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_82%_14%,rgba(225,29,72,0.16),transparent_38%),linear-gradient(135deg,rgba(255,255,255,0.96),rgba(255,241,246,0.7))] dark:bg-[radial-gradient(circle_at_82%_14%,rgba(251,113,133,0.18),transparent_38%),linear-gradient(135deg,rgba(24,24,27,0.96),rgba(9,9,11,0.88))]" />

      <div className="relative z-10 grid gap-5 xl:grid-cols-[minmax(0,0.84fr)_minmax(520px,1.16fr)] xl:items-center xl:gap-8">
        <div className="flex min-h-0 flex-col xl:py-3">
          <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1 text-[10px] font-black uppercase tracking-[0.16em] text-black/42 dark:text-white/36">
            <span className="text-[#E11D48]">●</span>
            <span className="text-black/72 dark:text-white/76">{focus.account}</span>
            <span className="text-black/22 dark:text-white/18">·</span>
            <span>{focus.focus_overview.tile_label}</span>
          </div>

          <h2 className="mt-4 max-w-[680px] text-[31px] font-black leading-[0.93] tracking-normal text-black dark:text-white sm:mt-5 sm:text-[42px] lg:max-w-[760px] lg:text-[48px] xl:max-w-[560px] xl:text-[50px]">
            {focus.focus_overview.tile_headline}
          </h2>

          <p className="mt-3 max-w-[590px] text-[13px] font-bold leading-relaxed text-black/52 line-clamp-2 dark:text-white/48 sm:mt-4 sm:text-[13.5px] sm:line-clamp-3 xl:max-w-[470px]">
            {focus.focus_overview.tile_read}
          </p>

          <div className="mt-4 flex flex-wrap items-center gap-x-3 gap-y-1.5 text-[9.5px] font-black uppercase tracking-[0.14em] sm:mt-5 sm:gap-x-4 sm:text-[10px]">
            {focus.focusMetrics.map((m) => (
              <div key={`${focus.focus_id}:${m.label}`} className="flex items-center gap-1.5">
                <span className={m.accent ? 'text-[#E11D48] dark:text-[#FB7185]' : 'text-black/74 dark:text-white/70'}>{m.value}</span>
                <span className="text-black/38 dark:text-white/34">{m.detail || m.label}</span>
              </div>
            ))}
          </div>

          <div className="mt-4 flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.18em] text-black/40 transition group-hover:text-[#E11D48] dark:text-white/34 dark:group-hover:text-[#FB7185] sm:mt-7">
            <span>Open read</span>
            <ArrowUpRight size={13} strokeWidth={3} />
          </div>
        </div>

        <div className="relative flex h-[230px] items-center justify-center overflow-hidden rounded-[28px] bg-[radial-gradient(circle_at_50%_45%,rgba(225,29,72,0.13),transparent_46%),linear-gradient(180deg,rgba(255,255,255,0.1),rgba(244,63,94,0.06))] [perspective:1200px] dark:bg-[radial-gradient(circle_at_50%_45%,rgba(251,113,133,0.16),transparent_46%),linear-gradient(180deg,rgba(255,255,255,0.05),rgba(251,113,133,0.07))] sm:h-[292px] lg:h-[330px] xl:h-[430px]">
          <div className="pointer-events-none absolute inset-x-8 bottom-8 h-20 rounded-full bg-black/14 blur-2xl dark:bg-black/45" />
          <div className="pointer-events-none absolute inset-y-8 left-1/2 w-[58%] -translate-x-1/2 rounded-full bg-[#E11D48]/10 blur-3xl dark:bg-[#FB7185]/12" />
          <div className="pointer-events-none absolute left-5 top-5 hidden rounded-full border border-black/[0.06] bg-white/64 px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.16em] text-black/42 backdrop-blur-xl dark:border-white/[0.08] dark:bg-white/[0.07] dark:text-white/38 xl:block">
            Proof rail
          </div>
          {focus.proof_rail.map((post, i) => {
            const relative = (i - carouselIndex + focus.proof_rail.length) % focus.proof_rail.length;
            const isLead = relative === 0;
            const isRight = relative === 1;
            const isLeft = relative === focus.proof_rail.length - 1;
            const visible = isLead || isRight || isLeft;
            const z = isLead ? 30 : isRight ? 20 : 10;
            const opacity = visible ? (isLead ? 1 : 0.62) : 0;
            const x = isLead ? 0 : isLeft ? -sideOffset : isRight ? sideOffset : 0;
            const y = isLead ? 0 : 9;
            const scale = isLead ? 1 : 0.78;
            const filter = isLead ? 'blur(0px) saturate(1)' : 'blur(1.8px) saturate(0.68)';
            return (
              <motion.div
                key={post.post_key}
                className="absolute h-[206px] w-[152px] will-change-transform sm:h-[258px] sm:w-[190px] lg:h-[292px] lg:w-[216px] xl:h-[376px] xl:w-[276px]"
                initial={false}
                animate={{ x, y, scale, opacity, filter }}
                transition={{ duration: 1.05, ease: [0.22, 1, 0.36, 1] }}
                style={{
                  zIndex: z,
                }}
              >
                <Thumb
                  post={post}
                  index={i + 1}
                  total={focus.proof_rail.length}
                  showCaption
                  className={[
                    'h-full w-full rounded-[22px] border border-white/80 shadow-[0_30px_60px_-32px_rgba(15,23,42,0.78)] dark:border-white/12 dark:shadow-[0_30px_70px_-34px_rgba(0,0,0,0.95)]',
                    isLead ? 'ring-1 ring-white/90 dark:ring-white/14' : '',
                  ].join(' ')}
                />
              </motion.div>
            );
          })}
        </div>
      </div>
    </motion.button>
  );
}

type ModalTab = 'pattern' | 'proofs';

function PatternView({ focus }: { focus: FeederboardFocus }) {
  return (
    <div className="mx-auto max-w-[760px]">
      <div className="rounded-[24px] border border-[#E11D48]/16 bg-[#E11D48]/8 p-6 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-[#FB7185]/18 dark:bg-[#FB7185]/10 dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]">
        <div className="text-[10px] font-black uppercase tracking-[0.2em] text-[#E11D48]">The hook</div>
        <p className="mt-3 text-[20px] font-black leading-[1.24] text-black dark:text-white sm:text-[23px]">
          {focus.focus_overview.the_hook}
        </p>
      </div>

      <div className="mt-8 space-y-5 text-[16px] font-extrabold leading-[1.65] text-black/72 dark:text-white/68 sm:text-[17px]">
        {focus.focus_overview.the_breakdown.map((p, i) => (
          <p key={`${focus.focus_id}:read:${i}`}>{p}</p>
        ))}
      </div>

      <dl className="mt-10 grid gap-x-8 gap-y-2 sm:grid-cols-[150px_minmax(0,1fr)] sm:gap-y-6">
        <DefRow label="Why it works" text={focus.focus_overview.why_it_works} />
      </dl>

      <div className="mt-10 grid gap-4 md:grid-cols-2">
        <PatternList label="What to keep" items={focus.focus_overview.what_to_keep} />
        <PatternList label="What kills it" items={focus.focus_overview.what_kills_it} />
      </div>
    </div>
  );
}

function ProofsView({
  focus,
  selectedIndex,
  setSelectedIndex,
  selectedPost,
}: {
  focus: FeederboardFocus;
  selectedIndex: number;
  setSelectedIndex: (i: number) => void;
  selectedPost: ProofBlock;
}) {
  return (
    <div>
      <div className="hide-scrollbar -mx-6 flex gap-4 overflow-x-auto px-6 pb-2 sm:-mx-9 sm:px-9 lg:-mx-12 lg:px-12">
        {focus.proof_rail.map((post, i) => {
          const active = i === selectedIndex;
          return (
            <button
              key={post.post_key}
              type="button"
              onClick={() => setSelectedIndex(i)}
              className="group shrink-0 text-left"
            >
              <div
                className={[
                  'overflow-hidden rounded-[22px] border-[2.5px] transition',
                  active
                    ? 'border-[#E11D48] shadow-[0_24px_50px_-26px_rgba(225,29,72,0.62)] dark:border-[#FB7185]'
                    : 'border-transparent opacity-60 group-hover:opacity-100',
                ].join(' ')}
              >
                <Thumb
                  post={post}
                  index={i + 1}
                  total={focus.proof_rail.length}
                  showCaption
                  className="h-[260px] w-[210px] sm:h-[300px] sm:w-[240px]"
                />
              </div>
              <div className="mt-2.5 text-[10px] font-black uppercase tracking-[0.16em]">
                {active ? (
                  <span className="text-[#E11D48]">Selected ↓</span>
                ) : (
                  <span className="text-black/38 dark:text-white/34">Proof {i + 1}</span>
                )}
              </div>
            </button>
          );
        })}
      </div>

      <div className="mt-9 grid gap-8 lg:grid-cols-[minmax(0,0.85fr)_minmax(0,1.15fr)]">
        <div>
          <Thumb
            post={selectedPost}
            className="aspect-[4/5] w-full rounded-[24px] shadow-[0_28px_60px_-36px_rgba(15,23,42,0.7)] dark:shadow-[0_28px_70px_-36px_rgba(0,0,0,0.95)]"
          />
          <div className="mt-5 grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-2 xl:grid-cols-3">
            {selectedPost.metrics.map((m) => (
              <MetricChip key={`${m.label}:${m.value}`} metric={m} />
            ))}
          </div>
        </div>

        <div className="min-w-0">
          <div className="text-[10px] font-black uppercase tracking-[0.2em] text-[#E11D48]">
            {selectedPost.proof_label}
          </div>
          <h3 className="mt-2 text-[26px] font-black leading-[1.05] tracking-normal text-black dark:text-white sm:text-[34px]">
            {selectedPost.proof_headline}
          </h3>

          <p className="mt-5 text-[15.5px] font-extrabold leading-[1.65] text-black/72 dark:text-white/68 sm:text-[16px]">
            {selectedPost.post_read}
          </p>

          <div className="mt-7 rounded-[22px] border border-[#E11D48]/16 bg-[#E11D48]/8 p-6 shadow-[inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-[#FB7185]/18 dark:bg-[#FB7185]/10 dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]">
            <div className="text-[10px] font-black uppercase tracking-[0.2em] text-[#E11D48]">What clicked</div>
            <p className="mt-3 text-[18px] font-black leading-[1.25] text-black dark:text-white sm:text-[20px]">
              {selectedPost.what_clicked}
            </p>
          </div>

          <dl className="mt-7 grid gap-x-8 gap-y-2 sm:grid-cols-[140px_minmax(0,1fr)] sm:gap-y-5">
            <dt className="text-[10px] font-black uppercase tracking-[0.18em] text-black/40 dark:text-white/36 sm:pt-1">Evidence</dt>
            <dd>
              <ul className="space-y-2.5 text-[14px] font-bold leading-[1.55] text-black/64 dark:text-white/62">
                {selectedPost.evidence.map((e, i) => (
                  <li key={`${selectedPost.post_key}:ev:${i}`} className="flex gap-3">
                    <span className="mt-[0.6em] h-1.5 w-1.5 shrink-0 rounded-full bg-[#E11D48]" />
                    <span>{e}</span>
                  </li>
                ))}
              </ul>
            </dd>
          </dl>
        </div>
      </div>
    </div>
  );
}

function FocusModal({
  focus,
  onClose,
}: {
  focus: FeederboardFocus | null;
  onClose: () => void;
}) {
  const [tab, setTab] = useState<ModalTab>('pattern');
  const [selectedIndex, setSelectedIndex] = useState(0);
  const selectedPost = focus?.proof_rail[selectedIndex] ?? focus?.proof_rail[0] ?? null;

  useEffect(() => {
    if (!focus) return undefined;
    const previous = document.documentElement.style.overflow;
    document.documentElement.style.overflow = 'hidden';
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        onClose();
        return;
      }
      if (tab === 'proofs') {
        if (event.key === 'ArrowLeft') {
          setSelectedIndex((i) => Math.max(0, i - 1));
        } else if (event.key === 'ArrowRight') {
          setSelectedIndex((i) => Math.min(focus.proof_rail.length - 1, i + 1));
        } else if (/^[1-9]$/.test(event.key)) {
          const idx = parseInt(event.key, 10) - 1;
          if (idx >= 0 && idx < focus.proof_rail.length) setSelectedIndex(idx);
        }
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.documentElement.style.overflow = previous;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [focus, onClose, tab]);

  if (typeof document === 'undefined') return null;

  return createPortal(
    <AnimatePresence>
      {focus && selectedPost && (
        <motion.div
          className="fixed inset-0 z-[340] flex items-end justify-center px-0 sm:items-center sm:px-4 sm:py-5"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.18 }}
        >
          <motion.div
            className="absolute inset-0 bg-black/66 backdrop-blur-[9px]"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
          />

          <motion.div
            role="dialog"
            aria-modal="true"
            aria-label={focus.focus_overview.modal_headline}
            initial={{ y: '100%', opacity: 0.96 }}
            animate={{ y: 0, opacity: 1 }}
            exit={{ y: '100%', opacity: 0.96 }}
            transition={{ type: 'spring', stiffness: 260, damping: 34, mass: 0.9 }}
            onClick={(event) => event.stopPropagation()}
            className="relative flex h-[min(900px,calc(100dvh-34px))] w-full flex-col overflow-hidden rounded-t-[28px] border border-black/[0.06] bg-[#fbfbfb] text-black shadow-[0_34px_100px_rgba(0,0,0,0.46)] dark:border-white/[0.08] dark:bg-[#08080a] dark:text-white sm:w-[min(1400px,calc(100vw-40px))] sm:rounded-[28px]"
          >
            <button
              type="button"
              onClick={onClose}
              className="absolute right-4 top-4 z-30 flex h-10 w-10 items-center justify-center rounded-full border border-black/[0.06] bg-white/84 text-black/46 shadow-[inset_0_1px_0_rgba(255,255,255,0.78)] backdrop-blur-xl transition hover:text-black dark:border-white/[0.08] dark:bg-white/[0.08] dark:text-white/54 dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.08)] dark:hover:text-white"
              aria-label="Close Feederboard read"
            >
              <X size={17} strokeWidth={2.6} />
            </button>

            <header className="shrink-0 border-b border-black/[0.06] bg-[radial-gradient(circle_at_88%_8%,rgba(225,29,72,0.14),transparent_42%),linear-gradient(180deg,#fff,#fbfbfb)] px-6 pt-7 dark:border-white/[0.08] dark:bg-[radial-gradient(circle_at_88%_8%,rgba(251,113,133,0.16),transparent_42%),linear-gradient(180deg,#111114,#08080a)] sm:px-9 sm:pt-9 lg:px-12">
              <div className="flex flex-wrap items-center gap-x-2.5 gap-y-1 pr-12 text-[10px] font-black uppercase tracking-[0.16em] text-black/42 dark:text-white/36">
                <span className="text-[#E11D48]">●</span>
                <span className="text-black/72 dark:text-white/76">{focus.account}</span>
                <span className="text-black/22 dark:text-white/18">·</span>
                <span>{focus.focus_overview.tile_label}</span>
              </div>
              <h2 className="mt-4 max-w-[1080px] text-[30px] font-black leading-[0.96] tracking-normal text-black dark:text-white sm:text-[44px] lg:text-[54px]">
                {focus.focus_overview.modal_headline}
              </h2>

              <div className="mt-7 flex items-center gap-1">
                {(['pattern', 'proofs'] as ModalTab[]).map((t) => {
                  const active = tab === t;
                  return (
                    <button
                      key={t}
                      type="button"
                      onClick={() => setTab(t)}
                      className={[
                        'relative flex items-center gap-1.5 px-4 py-3 text-[11px] font-black uppercase tracking-[0.16em] transition',
                        active ? 'text-[#E11D48] dark:text-[#FB7185]' : 'text-black/38 hover:text-black/64 dark:text-white/34 dark:hover:text-white/64',
                      ].join(' ')}
                    >
                      <span>{t === 'pattern' ? 'Pattern' : 'Proofs'}</span>
                      {t === 'proofs' && <span className="text-black/30 dark:text-white/28">· {focus.proof_rail.length}</span>}
                      {active && <span className="absolute inset-x-3 -bottom-px h-[2.5px] rounded-full bg-[#E11D48]" />}
                    </button>
                  );
                })}
              </div>
            </header>

            <div className="min-h-0 flex-1 overflow-y-auto px-6 pb-[calc(36px+env(safe-area-inset-bottom))] pt-8 sm:px-9 sm:pt-10 lg:px-12">
              {tab === 'pattern' ? (
                <PatternView focus={focus} />
              ) : (
                <ProofsView
                  focus={focus}
                  selectedIndex={selectedIndex}
                  setSelectedIndex={setSelectedIndex}
                  selectedPost={selectedPost}
                />
              )}
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>,
    document.body,
  );
}

export default function FeederboardClient({
  feedId,
  selectedHandle = 'all',
}: FeederboardClientProps) {
  const router = useRouter();
  const { appShellStyle, isStandaloneMode, useTranslucentBrowserChrome } = useMobileImmersiveViewport();
  const bottomClearance = useTranslucentBrowserChrome
    ? 'calc(20px + env(safe-area-inset-bottom))'
    : isStandaloneMode
      ? 'calc(132px + env(safe-area-inset-bottom))'
      : 'calc(96px + env(safe-area-inset-bottom))';
  const [activeAccount, setActiveAccount] = useState<string>(() => {
    const handle = String(selectedHandle || '').replace(/^@+/, '').toLowerCase();
    if (handle.includes('anuj')) return '@anuj.mp4';
    if (handle.includes('sugar')) return '@trysugar';
    if (handle.includes('saniya')) return '@saniyamirwani';
    return '@saniyamirwani';
  });
  const [activeFocus, setActiveFocus] = useState<FeederboardFocus | null>(null);
  const visibleFocuses = useMemo(() => focusForHandle(activeAccount), [activeAccount]);
  const activeFeed = visibleFocuses[0] ?? FEEDERBOARD_FOCUSES[0];

  return (
    <div
      className="relative min-h-[100dvh] overflow-x-hidden overflow-y-auto bg-[#fbfbfb] text-foreground dark:bg-[#050506] dark:text-white"
      style={{ ...appShellStyle, paddingBottom: bottomClearance } as CSSProperties}
    >
      <div className="pointer-events-none fixed inset-0 z-0 bg-[radial-gradient(circle_at_18%_0%,rgba(225,29,72,0.1),transparent_34%),linear-gradient(180deg,#fff,#fafafa)] dark:bg-[radial-gradient(circle_at_18%_0%,rgba(251,113,133,0.13),transparent_34%),linear-gradient(180deg,#08080a,#050506)]" />

      <main className="relative z-10 mx-auto flex w-full max-w-[1500px] flex-col px-3 pb-10 pt-[calc(16px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-5 lg:px-8">
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={() => router.push(`/?id=${feedId}`, { scroll: false })}
            className="flex h-10 w-10 shrink-0 items-center justify-center rounded-[14px] border border-black/[0.06] bg-white/76 text-black/52 shadow-[inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/54 dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]"
            aria-label="Back to feed dashboard"
          >
            <ArrowLeft size={20} strokeWidth={2.6} />
          </button>

          <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-[14px] border border-[#E11D48]/18 bg-[#E11D48]/10 text-[#BE123C] shadow-[inset_0_1px_0_rgba(255,255,255,0.62)] dark:border-[#FB7185]/20 dark:bg-[#FB7185]/12 dark:text-[#FDA4AF] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]">
            <BrainCircuit size={20} strokeWidth={2.6} />
          </div>

          <div className="flex min-w-0 flex-1 items-baseline gap-3">
            <h1 className="truncate text-[24px] font-black leading-none tracking-normal text-black dark:text-white sm:text-[30px]">
              Feederboard
            </h1>
            <span className="hidden text-[10px] font-black uppercase tracking-[0.16em] text-black/34 dark:text-white/30 sm:inline">
              Content intelligence
            </span>
          </div>
        </div>

        <section className="mt-5 rounded-[28px] border border-black/[0.06] bg-white/72 p-4 shadow-[0_26px_80px_-62px_rgba(15,23,42,0.82),inset_0_1px_0_rgba(255,255,255,0.86)] backdrop-blur-2xl dark:border-white/[0.08] dark:bg-white/[0.045] dark:shadow-[0_28px_92px_-58px_rgba(0,0,0,1),inset_0_1px_0_rgba(255,255,255,0.08)] sm:p-5">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
            <div>
              <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.18em] text-[#E11D48]">
                <span className="h-2 w-2 rounded-full bg-[#E11D48]" />
                Currently reading
              </div>
              <h2 className="mt-2 text-[34px] font-black leading-none tracking-normal text-black dark:text-white sm:text-[46px]">
                {activeFeed.account}
              </h2>
              <p className="mt-2 text-[12px] font-black uppercase tracking-[0.12em] text-black/36 dark:text-white/30">
                {activeFeed.accountMeta}
              </p>
            </div>

            <div className="flex flex-wrap gap-2 rounded-full border border-black/[0.06] bg-white/70 p-1.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.82)] dark:border-white/[0.08] dark:bg-white/[0.05] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.08)]">
              {FEEDERBOARD_FOCUSES.map((focus) => (
                <button
                  key={focus.account}
                  type="button"
                  onClick={() => setActiveAccount(focus.account)}
                  className={[
                    'rounded-full px-4 py-2 text-[9px] font-black uppercase tracking-[0.14em] transition',
                    activeAccount === focus.account
                      ? 'bg-[#E11D48] text-white shadow-[0_12px_24px_-16px_rgba(225,29,72,0.9)] dark:bg-[#FB7185] dark:text-[#24040b]'
                      : 'text-black/34 hover:bg-black/[0.04] hover:text-black/60 dark:text-white/34 dark:hover:bg-white/[0.06] dark:hover:text-white/68',
                  ].join(' ')}
                >
                  {focus.account}
                </button>
              ))}
            </div>
          </div>

          <div className="mt-5 grid gap-4 xl:grid-cols-2">
            {visibleFocuses.map((focus) => (
              <FocusCard key={`${focus.account}:${focus.focus_id}`} focus={focus} onOpen={setActiveFocus} />
            ))}
          </div>
        </section>
      </main>

      <FocusModal
        key={activeFocus ? `${activeFocus.account}:${activeFocus.focus_id}` : 'closed'}
        focus={activeFocus}
        onClose={() => setActiveFocus(null)}
      />
    </div>
  );
}
