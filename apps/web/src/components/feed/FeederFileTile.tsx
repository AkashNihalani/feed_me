'use client';

import { ArrowUpRight, BrainCircuit, LayoutGrid, Sparkles } from 'lucide-react';
import { motion, useReducedMotion } from 'framer-motion';
import { GRID_ITEM_EASE } from '@/lib/motion';

type FeederFileTileProps = {
  feedTitle: string;
  feederCount: number;
  trackedPosts: string;
  onOpen: () => void;
};

function formatCompact(value: string | number | null | undefined): string {
  if (value == null) return '0';
  const raw = typeof value === 'number' ? value : Number.parseFloat(String(value).replace(/[,\s]/g, ''));
  if (!Number.isFinite(raw)) return String(value || '0');
  if (raw >= 1_000_000) return `${(raw / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
  if (raw >= 1_000) return `${(raw / 1_000).toFixed(1).replace(/\.0$/, '')}K`;
  return new Intl.NumberFormat('en-US').format(Math.round(raw));
}

function MiniStat({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[14px] border border-foreground/[0.07] bg-white/46 px-3 py-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.46)] backdrop-blur-[14px] dark:border-white/[0.07] dark:bg-white/[0.045]">
      <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/38 dark:text-white/34">{label}</div>
      <div className="mt-1.5 text-[19px] font-black leading-none tracking-normal text-foreground dark:text-white">{value}</div>
    </div>
  );
}

export default function FeederFileTile({
  feedTitle,
  feederCount,
  trackedPosts,
  onOpen,
}: FeederFileTileProps) {
  const prefersReducedMotion = useReducedMotion();

  return (
    <motion.button
      layout
      type="button"
      onClick={onOpen}
      initial={prefersReducedMotion ? false : { opacity: 0, y: 18, scale: 0.97 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      transition={
        prefersReducedMotion
          ? { duration: 0.01 }
          : {
              opacity: { duration: 0.22, ease: GRID_ITEM_EASE },
              y: { type: 'spring', stiffness: 270, damping: 30, mass: 0.9 },
              scale: { duration: 0.32, ease: GRID_ITEM_EASE },
            }
      }
      whileTap={{ scale: 0.985 }}
      className="fm-depth-glass group relative isolate flex min-h-[180px] flex-col justify-between overflow-hidden rounded-[22px] p-4 text-left"
      aria-label={`Open Feeder File for ${feedTitle}`}
    >
      <div className="pointer-events-none absolute inset-0 z-0">
        <div className="absolute right-[-26%] top-[-42%] h-52 w-52 rounded-full bg-[radial-gradient(circle,rgba(225,29,72,0.28),transparent_68%)] blur-[6px]" />
        <div className="absolute inset-0 bg-[linear-gradient(135deg,rgba(225,29,72,0.1),transparent_46%,rgba(255,255,255,0.12))] dark:bg-[linear-gradient(135deg,rgba(225,29,72,0.16),transparent_50%,rgba(255,255,255,0.04))]" />
      </div>

      <div className="relative z-10 flex items-start justify-between gap-3">
        <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-[14px] border border-[#E11D48]/18 bg-[#E11D48]/10 text-[#BE123C] shadow-[inset_0_1px_0_rgba(255,255,255,0.62)] dark:border-[#FB7185]/20 dark:bg-[#E11D48]/16 dark:text-white">
          <BrainCircuit size={20} strokeWidth={2.6} />
        </div>
        <div className="flex items-center gap-1.5 rounded-full border border-foreground/[0.07] bg-white/52 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/46 backdrop-blur-[14px] dark:border-white/[0.08] dark:bg-white/[0.06] dark:text-white/42">
          <Sparkles size={11} strokeWidth={2.8} />
          Intelligence
        </div>
      </div>

      <div className="relative z-10 mt-5">
        <div className="flex items-center gap-2 text-[9px] font-black uppercase tracking-[0.18em] text-foreground/42 dark:text-white/36">
          <LayoutGrid size={12} strokeWidth={2.8} />
          Feeder File
        </div>
        <h3 className="mt-1.5 text-[25px] font-black leading-[0.92] tracking-normal text-foreground dark:text-white">
          Content intelligence
        </h3>
        <p className="mt-2 line-clamp-2 text-[11px] font-bold leading-relaxed text-foreground/50 dark:text-white/42">
          Fresh 90D pools for the posts and patterns working inside {feedTitle}.
        </p>
      </div>

      <div className="relative z-10 mt-5 grid grid-cols-[1fr_1fr_auto] gap-2">
        <MiniStat label="Feeders" value={String(feederCount)} />
        <MiniStat label="Posts" value={formatCompact(trackedPosts)} />
        <span className="flex h-full min-h-[54px] w-12 items-center justify-center rounded-[14px] bg-[#E11D48] text-white shadow-[0_12px_24px_-16px_rgba(225,29,72,0.92)] transition-transform group-hover:translate-x-0.5 group-hover:-translate-y-0.5">
          <ArrowUpRight size={18} strokeWidth={2.8} />
        </span>
      </div>
    </motion.button>
  );
}
