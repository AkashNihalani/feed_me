'use client';

import { useState, useEffect, useCallback, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Heart, MessageCircle, Eye } from 'lucide-react';
import { cn } from '@/lib/utils';

export type TickerItem = {
  id: string;
  handle: string;
  likesDelta: number;
  commentsDelta: number;
  viewsDelta: number;
  postsDelta: number;
};

interface FlipTickerProps {
  items: TickerItem[];
  className?: string;
}

/* ── Timing ── */
const DESKTOP_CYCLE_MS = 10_000;
const MOBILE_PHASE_MS = 3_000;   // 3s handle + 3s metrics = 6s cycle
const STAGGER_S = 0.22;          // seconds between each cascading value

/* ── Easing — heavy, deliberate, luxury ── */
const LUXURY_EASE = [0.16, 0.85, 0.1, 1] as const;
const FLIP_DURATION = 1.4;

function compactSigned(value: number) {
  const abs = Math.abs(value);
  const sign = value > 0 ? '+' : value < 0 ? '-' : '';
  if (abs >= 1_000_000) return `${sign}${(abs / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
  if (abs >= 1_000) return `${sign}${(abs / 1_000).toFixed(1).replace(/\.0$/, '')}K`;
  return `${sign}${abs}`;
}

/* ── Value chip (just the colored badge) ── */
function ValueBadge({ value }: { value: number }) {
  const pos = value > 0;
  const neg = value < 0;
  return (
    <span
      className={cn(
        'inline-flex items-center gap-0.5 whitespace-nowrap rounded-[8px] font-black tracking-[0.06em]',
        pos && 'border border-[#FB7185]/60 bg-[#E11D48]/78 text-white dark:bg-[#E11D48] dark:text-white dark:border-[#E11D48]/30 dark:shadow-[0_0_10px_rgba(225,29,72,0.18)]',
        neg && 'border border-black/8 bg-black/8 text-black/50 dark:bg-white/6 dark:text-white/40 dark:border-white/8',
        !pos && !neg && 'border border-black/4 bg-black/4 text-black/35 dark:bg-white/4 dark:text-white/30 dark:border-white/4',
      )}
    >
      {compactSigned(value)}
      {pos && <span className="opacity-80">▲</span>}
      {neg && <span className="opacity-50">▼</span>}
      {!pos && !neg && <span className="opacity-30">~</span>}
    </span>
  );
}

/* ═══════════════════════════════════════════
   DESKTOP — fixed labels, each value flips independently
   Labels are absolutely static. Values sit in fixed-width
   containers so the flip never pushes siblings.
   ═══════════════════════════════════════════ */
function DesktopTicker({ items, className }: FlipTickerProps) {
  const [index, setIndex] = useState(0);

  const next = useCallback(() => {
    setIndex((i) => (i + 1) % items.length);
  }, [items.length]);

  useEffect(() => {
    if (items.length <= 1) return;
    const id = setInterval(next, DESKTOP_CYCLE_MS);
    return () => clearInterval(id);
  }, [next, items.length]);

  if (items.length === 0) return null;

  const item = items[index % items.length];

  return (
    <div
      className={cn(
        'relative hidden h-[46px] w-full items-center overflow-hidden rounded-[14px] lg:flex',
        'bg-black/[0.02] border border-black/[0.03]',
        'shadow-[inset_0_1px_2px_rgba(0,0,0,0.03)]',
        'dark:bg-white/[0.02] dark:border-white/[0.04]',
        'dark:shadow-[inset_0_1px_2px_rgba(0,0,0,0.2)]',
        className,
      )}
      style={{ perspective: 1000 }}
    >
      <div className="flex w-full items-center justify-evenly px-6">
        {/* ── Handle slot ── */}
        <div className="relative flex h-[32px] min-w-[120px] items-center justify-center" style={{ perspective: 1000 }}>
          <AnimatePresence mode="wait">
            <motion.span
              key={`h-${item.id}`}
              className="absolute inset-0 flex items-center justify-center text-[13px] font-black uppercase tracking-[0.12em] text-black/55 dark:text-white/50"
              initial={{ rotateX: 80, opacity: 0, filter: 'blur(2px)' }}
              animate={{ rotateX: 0, opacity: 1, filter: 'blur(0px)' }}
              exit={{ rotateX: -80, opacity: 0, filter: 'blur(2px)' }}
              transition={{ duration: FLIP_DURATION, ease: LUXURY_EASE }}
              style={{ transformOrigin: 'center center', backfaceVisibility: 'hidden' }}
            >
              {item.handle}
            </motion.span>
          </AnimatePresence>
        </div>

        {/* ── Likes: static label + flipping value ── */}
        <div className="flex items-center gap-2">
          <span className="text-[11px] font-black uppercase tracking-[0.14em] text-black/30 dark:text-white/25">
            Likes
          </span>
          <div className="relative flex h-[28px] min-w-[72px] items-center justify-center text-[13px]" style={{ perspective: 1000 }}>
            <AnimatePresence mode="wait">
              <motion.div
                key={`l-${item.id}`}
                className="absolute inset-0 flex items-center justify-center"
                initial={{ rotateX: 80, opacity: 0, filter: 'blur(2px)' }}
                animate={{ rotateX: 0, opacity: 1, filter: 'blur(0px)' }}
                exit={{ rotateX: -80, opacity: 0, filter: 'blur(2px)' }}
                transition={{ duration: FLIP_DURATION, ease: LUXURY_EASE, delay: STAGGER_S }}
                style={{ transformOrigin: 'center center', backfaceVisibility: 'hidden' }}
              >
                <ValueBadge value={item.likesDelta} />
              </motion.div>
            </AnimatePresence>
          </div>
        </div>

        {/* ── Comments: static label + flipping value ── */}
        <div className="flex items-center gap-2">
          <span className="text-[11px] font-black uppercase tracking-[0.14em] text-black/30 dark:text-white/25">
            Comments
          </span>
          <div className="relative flex h-[28px] min-w-[72px] items-center justify-center text-[13px]" style={{ perspective: 1000 }}>
            <AnimatePresence mode="wait">
              <motion.div
                key={`c-${item.id}`}
                className="absolute inset-0 flex items-center justify-center"
                initial={{ rotateX: 80, opacity: 0, filter: 'blur(2px)' }}
                animate={{ rotateX: 0, opacity: 1, filter: 'blur(0px)' }}
                exit={{ rotateX: -80, opacity: 0, filter: 'blur(2px)' }}
                transition={{ duration: FLIP_DURATION, ease: LUXURY_EASE, delay: STAGGER_S * 2 }}
                style={{ transformOrigin: 'center center', backfaceVisibility: 'hidden' }}
              >
                <ValueBadge value={item.commentsDelta} />
              </motion.div>
            </AnimatePresence>
          </div>
        </div>

        {/* ── Views: static label + flipping value ── */}
        <div className="flex items-center gap-2">
          <span className="text-[11px] font-black uppercase tracking-[0.14em] text-black/30 dark:text-white/25">
            Views
          </span>
          <div className="relative flex h-[28px] min-w-[72px] items-center justify-center text-[13px]" style={{ perspective: 1000 }}>
            <AnimatePresence mode="wait">
              <motion.div
                key={`v-${item.id}`}
                className="absolute inset-0 flex items-center justify-center"
                initial={{ rotateX: 80, opacity: 0, filter: 'blur(2px)' }}
                animate={{ rotateX: 0, opacity: 1, filter: 'blur(0px)' }}
                exit={{ rotateX: -80, opacity: 0, filter: 'blur(2px)' }}
                transition={{ duration: FLIP_DURATION, ease: LUXURY_EASE, delay: STAGGER_S * 3 }}
                style={{ transformOrigin: 'center center', backfaceVisibility: 'hidden' }}
              >
                <ValueBadge value={item.viewsDelta} />
              </motion.div>
            </AnimatePresence>
          </div>
        </div>
      </div>
    </div>
  );
}

/* ═══════════════════════════════════════════
   MOBILE — 3s handle → 3s metrics = 6s cycle
   Phase 0: handle name fills the slot (big, bold)
   Phase 1: three bold metric chips with icons
   ═══════════════════════════════════════════ */
function MobileTicker({ items, className }: FlipTickerProps) {
  const [index, setIndex] = useState(0);
  const [phase, setPhase] = useState<0 | 1>(0);
  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const clearTimer = useCallback(() => {
    if (timerRef.current) { clearTimeout(timerRef.current); timerRef.current = null; }
  }, []);

  useEffect(() => {
    if (items.length === 0) return;
    clearTimer();

    if (phase === 0) {
      timerRef.current = setTimeout(() => setPhase(1), MOBILE_PHASE_MS);
    } else {
      timerRef.current = setTimeout(() => {
        setIndex((i) => (items.length <= 1 ? i : (i + 1) % items.length));
        setPhase(0);
      }, MOBILE_PHASE_MS);
    }

    return clearTimer;
  }, [phase, index, items.length, clearTimer]);

  if (items.length === 0) return null;

  const item = items[index % items.length];

  return (
    <div
      className={cn(
        'relative flex h-[44px] w-full items-center overflow-hidden rounded-[14px] lg:hidden',
        'bg-black/[0.02] border border-black/[0.03]',
        'shadow-[inset_0_1px_2px_rgba(0,0,0,0.03)]',
        'dark:bg-white/[0.02] dark:border-white/[0.04]',
        'dark:shadow-[inset_0_1px_2px_rgba(0,0,0,0.2)]',
        className,
      )}
      style={{ perspective: 1000 }}
    >
      <AnimatePresence mode="wait">
        {phase === 0 ? (
          <motion.div
            key={`mh-${item.id}-${index}`}
            className="flex w-full items-center justify-center px-4"
            initial={{ rotateX: 80, opacity: 0, filter: 'blur(2px)' }}
            animate={{ rotateX: 0, opacity: 1, filter: 'blur(0px)' }}
            exit={{ rotateX: -80, opacity: 0, filter: 'blur(2px)' }}
            transition={{ duration: FLIP_DURATION, ease: LUXURY_EASE }}
            style={{ transformOrigin: 'center center', backfaceVisibility: 'hidden' }}
          >
            <span className="text-[14px] font-black uppercase tracking-[0.16em] text-black/60 dark:text-white/55 sm:text-[15px]">
              {item.handle}
            </span>
          </motion.div>
        ) : (
          <motion.div
            key={`mm-${item.id}-${index}`}
            className="flex w-full items-center justify-evenly px-3"
            initial={{ rotateX: 80, opacity: 0, filter: 'blur(2px)' }}
            animate={{ rotateX: 0, opacity: 1, filter: 'blur(0px)' }}
            exit={{ rotateX: -80, opacity: 0, filter: 'blur(2px)' }}
            transition={{ duration: FLIP_DURATION, ease: LUXURY_EASE }}
            style={{ transformOrigin: 'center center', backfaceVisibility: 'hidden' }}
          >
            <div className="flex items-center gap-1.5">
              <Heart size={14} strokeWidth={2.5} className="text-black/35 dark:text-white/30" />
              <span className="text-[13px]"><ValueBadge value={item.likesDelta} /></span>
            </div>
            <div className="flex items-center gap-1.5">
              <MessageCircle size={14} strokeWidth={2.5} className="text-black/35 dark:text-white/30" />
              <span className="text-[13px]"><ValueBadge value={item.commentsDelta} /></span>
            </div>
            <div className="flex items-center gap-1.5">
              <Eye size={14} strokeWidth={2.5} className="text-black/35 dark:text-white/30" />
              <span className="text-[13px]"><ValueBadge value={item.viewsDelta} /></span>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

/* ═══════════════════════════════════════════
   Exported wrapper — renders both, CSS hides one
   ═══════════════════════════════════════════ */
export default function FlipTicker({ items, className }: FlipTickerProps) {
  if (items.length === 0) return null;

  return (
    <>
      <DesktopTicker items={items} className={className} />
      <MobileTicker items={items} className={className} />
    </>
  );
}
