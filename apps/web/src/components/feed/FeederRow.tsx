'use client';

import { motion } from 'framer-motion';
import { Crown, ExternalLink, Target, Trash2 } from 'lucide-react';
import { cn } from '@/lib/utils';

interface FeederRowProps {
  handle: string;
  isAnchor: boolean;
  profilePicUrl?: string | null;
  metrics: {
    likes: string;
    comments: string;
    views: string;
    postsTracked: string;
  };
  onMakeAnchor: () => void;
  onRemove: () => void;
  onOpenFeed: () => void;
  index: number;
}

function formatCompact(val: string): string {
  const num = parseInt(val.replace(/,/g, ''), 10);
  if (isNaN(num)) return val;
  if (num >= 1000000) return (num / 1000000).toFixed(1).replace(/\.0$/, '') + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1).replace(/\.0$/, '') + 'K';
  return num.toString();
}

export default function FeederRow({ handle, isAnchor, profilePicUrl, metrics, onMakeAnchor, onRemove, onOpenFeed }: FeederRowProps) {
  return (
    <motion.div
      layout
      initial={{ opacity: 0, scale: 0.95, y: 10 }}
      animate={{ opacity: 1, scale: 1, y: 0, zIndex: isAnchor ? 50 : 1 }}
      exit={{ opacity: 0, scale: 0.9 }}
      whileTap={{ scale: 0.985 }}
      transition={{ layout: { duration: 0.28, type: 'spring', stiffness: 220, damping: 24 } }}
      className={cn(
        'fm-depth-glass group relative flex flex-col justify-between overflow-hidden rounded-[22px] p-4',
        isAnchor && 'ring-1 ring-[#E11D48]/35'
      )}
      style={{ willChange: 'transform' }}
    >
      {isAnchor && (
        <div className="absolute right-0 top-0 z-20 flex items-center gap-1 rounded-bl-[16px] border border-[#FB7185] bg-[#E11D48] px-3 py-1 text-[9px] font-black uppercase text-white shadow-[0_4px_12px_rgba(225,29,72,0.24)]">
          <Crown size={12} strokeWidth={3} />ANCHOR
        </div>
      )}

      {/* Identity row */}
      <div className="relative z-10 mb-3 flex items-center gap-3">
        <div className={cn(
          'fm-depth-chip flex h-11 w-11 shrink-0 items-center justify-center overflow-hidden rounded-[14px] text-lg font-black',
          isAnchor ? 'border-[#FB7185] bg-[#E11D48] text-white shadow-[0_8px_18px_-10px_rgba(225,29,72,0.28)]' : 'text-foreground'
        )}>
          {profilePicUrl
            ? <img src={profilePicUrl} alt={`@${handle}`} className="h-full w-full object-cover" />
            : handle.substring(0, 2).toUpperCase()
          }
        </div>
        <div className="min-w-0 flex-1">
          <h3 className="truncate text-[18px] font-black uppercase leading-none tracking-tight text-foreground">
            {handle}
          </h3>
          <span className="mt-0.5 block text-[10px] font-black uppercase tracking-[0.1em] text-foreground/40">
            @{handle}
          </span>
        </div>
      </div>

      {/* Metrics — 2x2 grid for breathing room */}
      <div className="relative z-10 mb-3 grid grid-cols-2 gap-2 lg:grid-cols-4 lg:gap-1.5">
        <MetricStamp label="Likes" value={formatCompact(metrics.likes)} isAnchor={isAnchor} />
        <MetricStamp label="Views" value={formatCompact(metrics.views)} isAnchor={isAnchor} />
        <MetricStamp label="Comms" value={formatCompact(metrics.comments)} isAnchor={isAnchor} />
        <MetricStamp label="Posts" value={formatCompact(metrics.postsTracked)} isAnchor={isAnchor} />
      </div>

      {/* Actions */}
      <div className="relative z-10 mt-auto flex items-center justify-between gap-2 border-t border-black/5 pt-2.5 dark:border-white/6">
        {!isAnchor ? (
          <motion.button whileHover={{ scale: 1.05 }} whileTap={{ scale: 0.95 }} onClick={onMakeAnchor}
            className="flex items-center gap-1.5 text-[10px] font-black uppercase tracking-widest text-foreground/60 transition-all hover:text-foreground dark:hover:text-white">
            <Crown size={14} strokeWidth={3} />Make Anchor
          </motion.button>
        ) : (
          <motion.span animate={{ opacity: [0.8, 1, 0.8] }} transition={{ duration: 2, repeat: Infinity, ease: 'easeInOut' }}
            className="inline-flex items-center gap-1.5 rounded-full border border-[#FB7185] bg-[#E11D48] px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.14em] text-white shadow-[0_6px_12px_-8px_rgba(225,29,72,0.3)]">
            <Target size={14} strokeWidth={3} />Tracking
          </motion.span>
        )}
        <div className="flex items-center gap-1.5">
          <motion.button
            whileTap={{ scale: 0.96 }}
            onClick={onOpenFeed}
            className="inline-flex items-center gap-1.5 rounded-full bg-[#E11D48] px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.12em] text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.72),0_6px_14px_rgba(225,29,72,0.22)]"
          >
            <ExternalLink size={13} strokeWidth={2.6} />
            All Posts
          </motion.button>
          <motion.button whileTap={{ scale: 0.9 }} onClick={onRemove} className="fm-depth-chip rounded-[8px] p-1.5 text-foreground/50 transition-colors">
            <Trash2 size={14} strokeWidth={2.5} />
          </motion.button>
        </div>
      </div>
    </motion.div>
  );
}

function MetricStamp({ label, value, isAnchor }: { label: string; value: string; isAnchor: boolean }) {
  return (
    <div className={cn(
      'grid grid-cols-1 rounded-[10px] fm-depth-chip px-3 py-2.5',
      isAnchor && 'border-[#E11D48]/22 bg-[#E11D48]/[0.06] dark:bg-[#E11D48]/[0.08]'
    )}>
      <span className={cn('block text-[9px] font-black uppercase tracking-[0.1em]', isAnchor ? 'text-foreground/52 dark:text-white/52' : 'text-foreground/40')}>
        {label}
      </span>
      <span className={cn('mt-1 block text-[18px] font-black leading-none tabular-nums fm-depth-title', isAnchor ? 'text-foreground dark:text-white/92' : 'text-foreground/85')}>
        {value}
      </span>
    </div>
  );
}
