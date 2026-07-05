'use client';

/* eslint-disable @next/next/no-img-element -- Feeder avatars use direct dynamic profile URLs. */

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
    <div className="fm-depth-chip rounded-[14px] px-3 py-2.5">
      <div className="text-[10px] font-semibold text-foreground/55 dark:text-white/50">{label}</div>
      <div className="mt-1 text-[18px] font-black leading-none tracking-[-0.04em] text-foreground dark:text-white fm-depth-title">{value}</div>
    </div>
  );
}

function FeederAvatarCluster({ feeders }: { feeders?: FeedTileProps['feeders'] }) {
  const visibleFeeders = (feeders || []).slice(0, 3);
  const remainingCount = Math.max(0, (feeders?.length || 0) - visibleFeeders.length);
  if (visibleFeeders.length === 0) return null;

  return (
    <div className="flex shrink-0 items-center -space-x-3.5 sm:-space-x-4 lg:-space-x-5">
      {visibleFeeders.map((feeder, i) => {
        const initial = feeder.handle.slice(0, 1).toUpperCase() || 'F';
        return (
          <div
            key={feeder.handle}
            style={{ zIndex: visibleFeeders.length - i }}
            className="relative flex h-12 w-12 items-center justify-center overflow-hidden rounded-full border-[2.5px] border-white bg-[linear-gradient(135deg,rgb(var(--fm-accent-rgb)/0.22),rgba(255,255,255,0.92))] text-[14px] font-black text-[#7F1D1D] shadow-[0_10px_24px_-8px_rgba(15,23,42,0.32)] sm:h-14 sm:w-14 sm:text-[16px] lg:h-16 lg:w-16 dark:border-[#1c1c1f] dark:bg-[linear-gradient(135deg,rgb(var(--fm-accent-rgb)/0.7),rgba(24,24,27,0.95))] dark:text-white dark:shadow-[0_12px_28px_-10px_rgba(0,0,0,0.7)]"
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
        <div className="relative z-0 flex h-12 w-12 items-center justify-center rounded-full border-[2.5px] border-white bg-black/[0.05] text-[12px] font-bold text-foreground/70 shadow-[0_10px_24px_-8px_rgba(15,23,42,0.22)] backdrop-blur-none sm:h-14 sm:w-14 lg:h-16 lg:w-16 dark:border-[#1c1c1f] dark:bg-white/[0.08] dark:text-white/70">
            +{remainingCount}
        </div>
      ) : null}
    </div>
  );
}

function FeederHandlesLine({ feeders }: { feeders?: FeedTileProps['feeders'] }) {
  const visibleFeeders = (feeders || []).slice(0, 3);
  const remainingCount = Math.max(0, (feeders?.length || 0) - visibleFeeders.length);
  if (visibleFeeders.length === 0) return null;

  const labels = visibleFeeders.map((feeder) => `@${feeder.handle}`).join(' · ');

  return (
    <div className="mt-2.5 truncate text-[12px] font-medium text-foreground/65 dark:text-white/60">
      {labels}
      {remainingCount > 0 ? ` · +${remainingCount} more` : ''}
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
  const enterDelay = Math.min(index * 0.026, 0.2);

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
      className="fm-depth-glass group relative flex min-h-[220px] w-full cursor-pointer flex-col justify-between overflow-hidden rounded-[28px] p-4 text-left shadow-[0_12px_30px_-10px_rgba(15,23,42,0.22),0_24px_54px_-18px_rgba(15,23,42,0.16)] sm:min-h-[240px] sm:p-5 lg:min-h-[230px] lg:p-6 dark:shadow-[0_16px_36px_-14px_rgba(0,0,0,0.62),0_34px_70px_-22px_rgba(0,0,0,0.5)]"
      initial={shouldAnimateEntrance ? { y: 18, scale: 0.975 } : false}
      animate={{ y: 0, scale: 1 }}
      transition={shouldAnimateEntrance
        ? {
            y: { duration: 0.24, delay: enterDelay, ease: GRID_ITEM_EASE },
            scale: { duration: 0.24, delay: enterDelay, ease: GRID_ITEM_EASE },
          }
        : { duration: 0.01 }}
      whileTap={{ scale: 0.996 }}
    >
      <div className="relative z-10 flex h-full flex-col justify-between">
        <div className="flex items-start justify-between gap-3">
          <div className="flex flex-wrap items-center gap-2">
            <div className="fm-depth-chip flex items-center gap-2 rounded-[14px] px-3 py-1.5">
              <Target size={14} strokeWidth={2.6} className="text-foreground/55 dark:text-white/50" />
              <span className="text-[12px] font-semibold text-foreground/65 dark:text-white/60">{count} {count === 1 ? 'feeder' : 'feeders'}</span>
            </div>
            {anchor ? (
              <div className="fm-depth-chip flex items-center gap-1.5 rounded-[14px] border-[var(--fm-accent)]/22 bg-[var(--fm-accent)]/10 px-3 py-1.5 dark:border-[var(--fm-accent)]/28 dark:bg-[var(--fm-accent)]/12">
                <Crown size={14} strokeWidth={2.6} className="text-[var(--fm-accent-deep)] dark:text-[var(--fm-accent-bright)]" />
                <span className="max-w-[120px] truncate text-[12px] font-bold text-[var(--fm-accent-deep)] dark:text-[var(--fm-accent-bright)]">@{anchor}</span>
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
              className="fm-depth-chip flex h-10 w-10 items-center justify-center rounded-[14px] text-foreground/45 dark:text-white/40"
              aria-label={`Delete ${title}`}
            >
              <Trash2 size={16} strokeWidth={2.4} />
            </motion.button>
          ) : null}
        </div>

        <div className="mt-6 flex flex-1 items-center justify-between gap-4 sm:mt-8 lg:gap-6">
          <div className="min-w-0">
            <span className="text-[10px] font-black uppercase tracking-[0.22em] text-foreground/45 dark:text-white/38 fm-depth-title">
              Feed
            </span>
            <h2 className="mt-2 text-[28px] font-black uppercase leading-[0.9] tracking-[-0.04em] text-foreground sm:text-[34px] dark:text-white fm-depth-title">
              {title}
            </h2>
            <FeederHandlesLine feeders={feeders} />
          </div>
          <FeederAvatarCluster feeders={feeders} />
        </div>

        <div className="mt-5 grid grid-cols-2 gap-2 sm:mt-6 sm:grid-cols-4">
          <StatPill label="Likes" value={formatCompact(metrics?.likes || '0')} />
          <StatPill label="Comments" value={formatCompact(metrics?.comments || '0')} />
          <StatPill label="Views" value={formatCompact(metrics?.views || '0')} />
          <StatPill label="Posts" value={formatCompact(metrics?.postsTracked || '0')} />
        </div>

        <div className="mt-4 flex items-center justify-between gap-3">
          <div className="flex items-center gap-2.5">
            <span
              className={cn(
                'fm-live-dot h-2.5 w-2.5 rounded-full',
                count > 0 ? 'bg-[var(--fm-accent)] shadow-[0_6px_12px_-8px_rgb(var(--fm-accent-rgb)/0.32)]' : 'bg-red-500 shadow-[0_6px_12px_-8px_rgba(239,68,68,0.35)]'
              )}
            />
            <span className={cn('text-[10px] font-black uppercase tracking-[0.14em]', count > 0 ? 'text-foreground/60 dark:text-white/55' : 'text-red-500')}>
              {count > 0 ? 'Tracking' : 'Empty'}
            </span>
          </div>
        </div>
      </div>
    </motion.div>
  );
}
