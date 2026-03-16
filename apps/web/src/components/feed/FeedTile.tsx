'use client';

import { motion } from 'framer-motion';
import { Crown, Target, Trash2 } from 'lucide-react';
import { cn } from '@/lib/utils';

function formatCompact(val: string): string {
  const num = parseInt(val.replace(/,/g, ''), 10);
  if (isNaN(num)) return val;
  if (num >= 1_000_000) return (num / 1_000_000).toFixed(1).replace(/\.0$/, '') + 'M';
  if (num >= 1_000) return (num / 1_000).toFixed(1).replace(/\.0$/, '') + 'K';
  return num.toString();
}

interface FeedTileProps {
  title: string;
  count: number;
  anchor?: string;
  metrics?: {
    likes: string;
    comments: string;
    views: string;
    postsTracked: string;
  };
  onClick: () => void;
  onDelete?: () => void;
  index: number;
  layoutId?: string;
}

function StatPill({ label, value }: { label: string; value: string }) {
  return (
    <div className="fm-depth-chip rounded-[12px] px-3 py-2.5">
      <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/54 dark:text-white/42 fm-depth-title">{label}</div>
      <div className="mt-1 text-[18px] font-black leading-none tracking-[-0.03em] text-foreground dark:text-white fm-depth-title">{value}</div>
    </div>
  );
}

export default function FeedTile({ title, count, anchor, metrics, onClick, onDelete, index }: FeedTileProps) {
  return (
    <motion.div
      role="button"
      tabIndex={0}
      onClick={onClick}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          onClick();
        }
      }}
      className="fm-depth-glass group relative flex min-h-[220px] w-full cursor-pointer flex-col justify-between overflow-hidden rounded-[28px] p-4 text-left shadow-[0_12px_30px_-10px_rgba(15,23,42,0.22),0_24px_54px_-18px_rgba(15,23,42,0.16)] sm:min-h-[240px] sm:p-5 dark:shadow-[0_16px_36px_-14px_rgba(0,0,0,0.62),0_34px_70px_-22px_rgba(0,0,0,0.5)]"
      initial={{ y: 20, scale: 0.97 }}
      animate={{ y: 0, scale: 1 }}
      transition={{ delay: index * 0.06, duration: 0.42, ease: [0.22, 1, 0.36, 1] }}
      whileTap={{ scale: 0.985 }}
    >
      <div className="relative z-10 flex h-full flex-col justify-between">
        <div className="flex items-start justify-between gap-3">
          <div className="flex flex-wrap items-center gap-2">
            <div className="fm-depth-chip flex items-center gap-2 rounded-[13px] px-3 py-1.5">
              <Target size={14} strokeWidth={2.8} className="text-foreground/72 dark:text-white/68" />
              <span className="text-[10px] font-black uppercase tracking-[0.14em] text-foreground/72 dark:text-white/66">{count} {count === 1 ? 'Feeder' : 'Feeders'}</span>
            </div>
            {anchor ? (
              <div className="fm-depth-chip flex items-center gap-1.5 rounded-[13px] px-3 py-1.5 dark:border-[#4f6410] dark:bg-[#171d07]">
                <Crown size={14} strokeWidth={2.8} className="text-black/70 dark:text-[#CCFF00]" />
                <span className="max-w-[120px] truncate text-[10px] font-black uppercase tracking-[0.12em] text-black/80 dark:text-[#CCFF00]">{anchor}</span>
              </div>
            ) : null}
          </div>

          {onDelete ? (
            <motion.button
              type="button"
              whileTap={{ scale: 0.92 }}
              onClick={(event) => {
                event.stopPropagation();
                onDelete();
              }}
              className="fm-depth-chip flex h-10 w-10 items-center justify-center rounded-[14px] text-foreground/54 dark:text-white/46"
              aria-label={`Delete ${title}`}
            >
              <Trash2 size={16} strokeWidth={2.4} />
            </motion.button>
          ) : null}
        </div>

        <div className="mt-6 flex-1 sm:mt-8">
          <span className="text-[10px] font-black uppercase tracking-[0.22em] text-foreground/42 dark:text-white/34 fm-depth-title">
            Feed
          </span>
          <h2 className="mt-2 max-w-[92%] text-[28px] font-black uppercase leading-[0.9] tracking-[-0.04em] text-foreground sm:max-w-[88%] sm:text-[38px] dark:text-white fm-depth-title">
            {title}
          </h2>
        </div>

        <div className="mt-5 grid grid-cols-2 gap-2 sm:mt-6 sm:grid-cols-4">
          <StatPill label="Likes" value={formatCompact(metrics?.likes || '0')} />
          <StatPill label="Comments" value={formatCompact(metrics?.comments || '0')} />
          <StatPill label="Views" value={formatCompact(metrics?.views || '0')} />
          <StatPill label="Posts" value={formatCompact(metrics?.postsTracked || '0')} />
        </div>

        <div className="mt-4 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2.5">
            <motion.span
              animate={{ opacity: [0.55, 1, 0.55] }}
              transition={{ duration: 2.2, repeat: Infinity, ease: 'easeInOut' }}
              className={cn(
                'h-2.5 w-2.5 rounded-full',
                count > 0 ? 'bg-black shadow-[0_0_8px_rgba(0,0,0,0.3)] dark:bg-[#CCFF00] dark:shadow-[0_0_12px_rgba(204,255,0,0.7)]' : 'bg-red-500 shadow-[0_0_10px_rgba(239,68,68,0.5)]'
              )}
            />
            <span className={cn('text-[10px] font-black uppercase tracking-[0.18em]', count > 0 ? 'text-[#7ca100] dark:text-[#CCFF00] dark:drop-shadow-[0_0_10px_rgba(204,255,0,0.45)]' : 'text-red-500')}>
              {count > 0 ? 'Tracking' : 'Empty'}
            </span>
          </div>
          <span className="shrink-0 text-[10px] font-black uppercase tracking-[0.18em] text-foreground/40 dark:text-white/32">Tap to open</span>
        </div>
      </div>
    </motion.div>
  );
}
