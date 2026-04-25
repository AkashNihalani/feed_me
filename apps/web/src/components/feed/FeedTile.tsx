'use client';

import { motion, useReducedMotion } from 'framer-motion';
import { Crown, Target, Trash2 } from 'lucide-react';
import { GRID_ITEM_EASE } from '@/lib/motion';
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
  feeders?: Array<{
    handle: string;
    profilePicUrl?: string | null;
  }>;
  metrics?: {
    likes: string;
    comments: string;
    views: string;
    postsTracked: string;
  };
  onClick: () => void;
  onPreview?: () => void;
  onDelete?: () => void;
  index: number;
  enableEntranceAnimation?: boolean;
}

function StatPill({ label, value }: { label: string; value: string }) {
  return (
    <div className="fm-depth-chip rounded-[12px] px-3 py-2.5">
      <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/54 dark:text-white/42 fm-depth-title">{label}</div>
      <div className="mt-1 text-[18px] font-black leading-none tracking-[-0.03em] text-foreground dark:text-white fm-depth-title">{value}</div>
    </div>
  );
}

function FeederAvatarStack({ feeders }: { feeders?: FeedTileProps['feeders'] }) {
  const visibleFeeders = (feeders || []).slice(0, 3);
  const remainingCount = Math.max(0, (feeders?.length || 0) - visibleFeeders.length);
  if (visibleFeeders.length === 0) return null;

  const labels = visibleFeeders.map((feeder) => `@${feeder.handle}`).join(' · ');

  return (
    <div className="mt-4 flex items-center gap-3">
      <div className="flex items-center -space-x-2.5">
        {visibleFeeders.map((feeder) => {
          const initial = feeder.handle.slice(0, 1).toUpperCase() || 'F';
          return (
            <div
              key={feeder.handle}
              className="relative flex h-10 w-10 items-center justify-center overflow-hidden rounded-full border border-white/78 bg-[linear-gradient(135deg,rgba(225,29,72,0.22),rgba(255,255,255,0.92))] text-[11px] font-black uppercase tracking-[0.08em] text-[#7F1D1D] shadow-[0_6px_18px_rgba(15,23,42,0.12),inset_0_1px_0_rgba(255,255,255,0.78)] dark:border-white/12 dark:bg-[linear-gradient(135deg,rgba(225,29,72,0.7),rgba(24,24,27,0.95))] dark:text-white"
            >
              {feeder.profilePicUrl ? (
                <img src={feeder.profilePicUrl} alt={`@${feeder.handle}`} className="h-full w-full object-cover" />
              ) : (
                <span>{initial}</span>
              )}
            </div>
          );
        })}
        {remainingCount > 0 ? (
          <div className="relative flex h-10 w-10 items-center justify-center rounded-full border border-white/74 bg-black/[0.04] text-[10px] font-black uppercase tracking-[0.08em] text-foreground shadow-[0_6px_16px_rgba(15,23,42,0.08),inset_0_1px_0_rgba(255,255,255,0.8)] dark:border-white/10 dark:bg-white/[0.06] dark:text-white">
            +{remainingCount}
          </div>
        ) : null}
      </div>

      <div className="min-w-0">
        <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/38 dark:text-white/30 fm-depth-title">
          Feeder roster
        </div>
        <div className="mt-1 truncate text-[11px] font-black uppercase tracking-[0.1em] text-foreground/70 dark:text-white/68 fm-depth-title">
          {labels}
          {remainingCount > 0 ? ` · +${remainingCount} more` : ''}
        </div>
      </div>
    </div>
  );
}

export default function FeedTile({
  title,
  count,
  anchor,
  feeders,
  metrics,
  onClick,
  onPreview,
  onDelete,
  index,
  enableEntranceAnimation = true,
}: FeedTileProps) {
  const prefersReducedMotion = useReducedMotion();
  const shouldAnimateEntrance = enableEntranceAnimation && !prefersReducedMotion;
  const enterDelay = Math.min(index * 0.055, 0.28);

  return (
    <motion.div
      role="button"
      tabIndex={0}
      onClick={onClick}
      onPointerEnter={onPreview}
      onFocus={onPreview}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          onClick();
        }
      }}
      className="fm-depth-glass group relative flex min-h-[220px] w-full cursor-pointer flex-col justify-between overflow-hidden rounded-[28px] p-4 text-left shadow-[0_12px_30px_-10px_rgba(15,23,42,0.22),0_24px_54px_-18px_rgba(15,23,42,0.16)] sm:min-h-[240px] sm:p-5 lg:min-h-[200px] dark:shadow-[0_16px_36px_-14px_rgba(0,0,0,0.62),0_34px_70px_-22px_rgba(0,0,0,0.5)]"
      initial={shouldAnimateEntrance ? { y: 22, scale: 0.985, opacity: 0 } : false}
      animate={{ y: 0, scale: 1, opacity: 1 }}
      transition={shouldAnimateEntrance
        ? {
            opacity: { duration: 0.22, delay: 0.06 + enterDelay, ease: GRID_ITEM_EASE },
            y: { type: 'spring', stiffness: 270, damping: 30, mass: 0.9, delay: 0.06 + enterDelay },
            scale: { duration: 0.34, delay: 0.06 + enterDelay, ease: GRID_ITEM_EASE },
          }
        : { duration: 0.01 }}
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
              <div className="fm-depth-chip flex items-center gap-1.5 rounded-[13px] border-[#E11D48]/22 bg-[#E11D48]/10 px-3 py-1.5 dark:border-[#E11D48]/28 dark:bg-[#E11D48]/12">
                <Crown size={14} strokeWidth={2.8} className="text-[#BE123C] dark:text-white/78" />
                <span className="max-w-[120px] truncate text-[10px] font-black uppercase tracking-[0.12em] text-[#BE123C] dark:text-white/78">{anchor}</span>
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
          <FeederAvatarStack feeders={feeders} />
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
                count > 0 ? 'bg-[#E11D48] shadow-[0_6px_12px_-8px_rgba(225,29,72,0.32)]' : 'bg-red-500 shadow-[0_6px_12px_-8px_rgba(239,68,68,0.35)]'
              )}
            />
            <span className={cn('text-[10px] font-black uppercase tracking-[0.18em]', count > 0 ? 'text-foreground/64 dark:text-white/62' : 'text-red-500')}>
              {count > 0 ? 'Tracking' : 'Empty'}
            </span>
          </div>
          <span className="shrink-0 text-[10px] font-black uppercase tracking-[0.18em] text-foreground/40 dark:text-white/32">Tap to open</span>
        </div>
      </div>
    </motion.div>
  );
}
