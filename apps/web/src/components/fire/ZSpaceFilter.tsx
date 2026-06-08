'use client';

import { useEffect, useMemo, useState } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { Check, ChevronDown, ChevronRight } from 'lucide-react';
import { useAppHaptics } from '@/lib/haptics';
import { cn } from '@/lib/utils';
import { FireFeedOption, FireFilterState, FireMediaFilter } from './types';

interface ZSpaceFilterProps {
  isOpen: boolean;
  onClose: () => void;
  filters: FireFilterState;
  availableFeeds: FireFeedOption[];
  availableCheckpoints: string[];
  onChange: (next: FireFilterState) => void;
}

const MEDIA_FILTER_OPTIONS: { label: string; value: FireMediaFilter }[] = [
  { label: 'IMAGES', value: 'IMAGE' },
  { label: 'CAROUSELS', value: 'CAROUSEL' },
  { label: 'REELS', value: 'REEL' },
  { label: 'ALL', value: 'ALL' },
];

export default function ZSpaceFilter({
  isOpen,
  onClose,
  filters,
  availableFeeds,
  availableCheckpoints,
  onChange,
}: ZSpaceFilterProps) {
  const { play } = useAppHaptics();
  const [expandedFeeds, setExpandedFeeds] = useState<Record<string, boolean>>({});
  const [isDesktop, setIsDesktop] = useState(() => typeof window !== 'undefined' && window.matchMedia('(min-width: 1024px)').matches);

  useEffect(() => {
    const mql = window.matchMedia('(min-width: 1024px)');
    const handler = (event: MediaQueryListEvent) => setIsDesktop(event.matches);
    mql.addEventListener('change', handler as (event: MediaQueryListEvent) => void);
    return () => mql.removeEventListener('change', handler as (event: MediaQueryListEvent) => void);
  }, []);

  const selectedFeederCount = useMemo(
    () => Object.values(filters.selectedFeederIdsByFeed).reduce((sum, ids) => sum + ids.length, 0),
    [filters.selectedFeederIdsByFeed],
  );

  const toggleFeed = (feedId: number) => {
    const key = String(feedId);
    const isSelected = filters.selectedFeedIds.includes(feedId);
    if (isSelected) {
      const nextFeederState = { ...filters.selectedFeederIdsByFeed };
      delete nextFeederState[key];
      onChange({
        ...filters,
        selectedFeedIds: filters.selectedFeedIds.filter((id) => id !== feedId),
        selectedFeederIdsByFeed: nextFeederState,
      });
      return;
    }
    onChange({
      ...filters,
      selectedFeedIds: [...filters.selectedFeedIds, feedId],
    });
    setExpandedFeeds((current) => ({ ...current, [key]: true }));
  };

  const toggleFeeder = (feedId: number, feederId: number) => {
    const key = String(feedId);
    const currentIds = filters.selectedFeederIdsByFeed[key] || [];
    const hasFeeder = currentIds.includes(feederId);
    const nextIds = hasFeeder ? currentIds.filter((id) => id !== feederId) : [...currentIds, feederId];
    const nextFeederState = { ...filters.selectedFeederIdsByFeed };
    if (nextIds.length === 0) delete nextFeederState[key];
    else nextFeederState[key] = nextIds;

    const nextFeedIds = filters.selectedFeedIds.includes(feedId)
      ? filters.selectedFeedIds
      : [...filters.selectedFeedIds, feedId];

    onChange({
      ...filters,
      selectedFeedIds: nextFeedIds,
      selectedFeederIdsByFeed: nextFeederState,
    });
  };

  const toggleCheckpoint = (checkpoint: string) => {
    const normalized = checkpoint.toUpperCase();
    const hasCheckpoint = filters.selectedCheckpoints.includes(normalized);
    onChange({
      ...filters,
      selectedCheckpoints: hasCheckpoint
        ? filters.selectedCheckpoints.filter((value) => value !== normalized)
        : [...filters.selectedCheckpoints, normalized],
    });
  };

  return (
    <AnimatePresence>
      {isOpen && (
        <motion.div
          initial={false}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0.999 }}
          transition={{ duration: 0.36, ease: [0.22, 1, 0.36, 1] }}
          className="fixed inset-0 z-[200] flex items-end justify-center pointer-events-auto sm:items-center"
          onClick={() => {
            play('navReselect');
            onClose();
          }}
        >
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.34, ease: [0.22, 1, 0.36, 1] }}
            className={cn(
              'absolute inset-0 bg-black/50 dark:bg-black/65',
            )}
          />

          <motion.div
            initial={{ opacity: 0, scale: 0.975, y: 36 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.982, y: 22 }}
            transition={{ type: 'spring', stiffness: 360, damping: 34, mass: 0.92 }}
            className={cn(
              'relative mb-0 flex w-full max-w-2xl flex-col overflow-hidden rounded-t-[36px] sm:mb-12 sm:rounded-[36px]',
              'border border-white/80 border-t-white/90 bg-white/92',
              'shadow-[0_1px_0_rgba(255,255,255,0.95)_inset,0_-1px_0_rgba(0,0,0,0.03)_inset,0_24px_64px_-16px_rgba(0,0,0,0.15)]',
              'dark:border-white/[0.08] dark:border-t-white/[0.12] dark:bg-[rgba(10,10,12,0.94)]',
              'dark:shadow-[0_1px_0_rgba(255,255,255,0.06)_inset,0_-1px_0_rgba(0,0,0,0.5)_inset,0_32px_80px_rgba(0,0,0,0.6)]',
            )}
            onClick={(event) => event.stopPropagation()}
          >
            <div
              className="pointer-events-none absolute inset-[1px] z-0 rounded-[35px] dark:hidden"
              style={{ boxShadow: 'inset 0 2px 4px rgba(255,255,255,0.7), inset 0 -2px 6px rgba(0,0,0,0.04)' }}
            />

            <div className="relative z-10 flex max-h-[85vh] flex-col gap-8 overflow-y-auto px-6 py-8 sm:px-8 sm:py-9">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <div className="text-[10px] font-black uppercase tracking-[0.2em] text-black/50 dark:text-white/40">
                    Feed List
                  </div>
                  <div className="mt-2 text-[22px] font-black uppercase tracking-[0.08em] text-black dark:text-white">
                    Feed Filter
                  </div>
                  <div className="mt-2 text-[11px] font-black uppercase tracking-[0.14em] text-black/36 dark:text-white/30">
                    {filters.selectedFeedIds.length === 0 ? 'ALL FEEDS' : `${filters.selectedFeedIds.length} FEEDS`} · {selectedFeederCount} FEEDERS
                  </div>
                </div>
                <motion.button
                  type="button"
                  whileTap={{ scale: 0.94 }}
                  onClick={onClose}
                  className="rounded-[16px] border border-white/70 bg-white/60 p-2.5 text-black/55 shadow-[0_2px_6px_rgba(0,0,0,0.08),0_1px_0_rgba(255,255,255,0.9)_inset] dark:border-white/10 dark:bg-white/[0.06] dark:text-white/46 dark:shadow-[0_2px_8px_rgba(0,0,0,0.4),0_1px_0_rgba(255,255,255,0.06)_inset]"
                >
                  <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round">
                    <line x1="6" y1="6" x2="18" y2="18" />
                    <line x1="18" y1="6" x2="6" y2="18" />
                  </svg>
                </motion.button>
              </div>

              {!isDesktop && (
              <div className="flex flex-col gap-4">
                <div className="text-[10px] font-black uppercase tracking-[0.2em] text-black/50 dark:text-white/40">
                  Post Type
                </div>

                <div className="grid grid-cols-2 gap-2.5 sm:grid-cols-4">
                  {MEDIA_FILTER_OPTIONS.map((option) => {
                    const isActive = filters.mediaFilter === option.value;
                    return (
                      <motion.button
                        key={option.value}
                        type="button"
                        whileTap={{ scale: 0.94 }}
                        onClick={() => {
                          if (!isActive) play('snapLock');
                          onChange({ ...filters, mediaFilter: option.value });
                        }}
                        className={cn(
                          'relative flex items-center justify-center rounded-[18px] px-2 py-4 transition-colors duration-200',
                          isActive
                            ? 'bg-black text-white shadow-[0_4px_16px_rgba(0,0,0,0.2),inset_0_1px_1px_rgba(255,255,255,0.1)] dark:bg-[#E11D48] dark:text-white dark:shadow-[0_4px_20px_rgba(225,29,72,0.2),inset_0_1px_2px_rgba(255,255,255,0.5)]'
                            : 'border border-black/5 bg-black/5 text-black/60 shadow-[inset_0_2px_8px_rgba(0,0,0,0.04)] dark:border-white/5 dark:bg-white/5 dark:text-white/50 dark:shadow-[inset_0_2px_8px_rgba(0,0,0,0.3)]',
                        )}
                      >
                        <span className="text-[13px] font-black tracking-[0.08em]">{option.label}</span>
                      </motion.button>
                    );
                  })}
                </div>
              </div>
              )}

              <div className="flex flex-col gap-4">
                <div className="text-[10px] font-black uppercase tracking-[0.2em] text-black/50 dark:text-white/40">
                  Feed List
                </div>

                <div
                  className="flex max-h-[280px] flex-col gap-2 overflow-y-auto rounded-[20px] p-1.5 hide-scrollbar"
                  style={{ WebkitOverflowScrolling: 'touch' }}
                >
                  {availableFeeds.map((feed) => {
                    const isSelected = filters.selectedFeedIds.includes(feed.id);
                    const isExpanded = isSelected || Boolean(expandedFeeds[String(feed.id)]);
                    const selectedFeederIds = filters.selectedFeederIdsByFeed[String(feed.id)] || [];

                    return (
                      <div key={feed.id} className="rounded-[22px] border border-black/5 bg-black/[0.03] p-2 dark:border-white/6 dark:bg-white/[0.03]">
                        <div className="flex items-center gap-2">
                          <motion.button
                            type="button"
                            whileTap={{ scale: 0.98 }}
                            onClick={() => {
                              play('snapLock');
                              toggleFeed(feed.id);
                            }}
                            className={cn(
                              'flex min-w-0 flex-1 items-center justify-between rounded-[18px] px-4 py-3.5 text-left transition-colors duration-200',
                              isSelected
                                ? 'border border-black/5 bg-white text-black shadow-[0_4px_16px_rgba(0,0,0,0.08),inset_0_1px_1px_rgba(255,255,255,0.9)] dark:border-white/10 dark:bg-black dark:text-[#E11D48] dark:shadow-[0_8px_24px_rgba(0,0,0,0.4),inset_0_1px_1px_rgba(255,255,255,0.05)]'
                                : 'text-black/60 dark:text-white/52',
                            )}
                          >
                            <div className="min-w-0">
                              <div className="truncate text-[15px] font-black tracking-tight sm:text-[16px]">
                                {feed.name}
                              </div>
                              <div className="mt-1 text-[9px] font-black uppercase tracking-[0.16em] text-black/35 dark:text-white/28">
                                {feed.feeders.length} feeders
                                {selectedFeederIds.length > 0 ? ` · ${selectedFeederIds.length} active` : ''}
                              </div>
                            </div>

                            {isSelected && (
                              <div className="ml-3 flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-black/5 dark:bg-[#E11D48]/10">
                                <Check size={14} strokeWidth={4} className="text-black dark:text-[#E11D48]" />
                              </div>
                            )}
                          </motion.button>

                          <motion.button
                            type="button"
                            whileTap={{ scale: 0.94 }}
                            onClick={() => setExpandedFeeds((current) => ({ ...current, [String(feed.id)]: !isExpanded }))}
                            className="flex h-11 w-11 shrink-0 items-center justify-center rounded-[16px] border border-black/5 bg-white/50 text-black/45 shadow-[0_2px_8px_rgba(0,0,0,0.06),inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-white/10 dark:bg-white/[0.06] dark:text-white/42 dark:shadow-[0_8px_18px_rgba(0,0,0,0.35),inset_0_1px_0_rgba(255,255,255,0.08)]"
                          >
                            {isExpanded ? <ChevronDown size={18} /> : <ChevronRight size={18} />}
                          </motion.button>
                        </div>

                        {isExpanded && (
                          <div className="mt-3 flex flex-wrap gap-2 px-1 pb-1">
                            {feed.feeders.length === 0 ? (
                              <div className="rounded-[14px] border border-dashed border-black/8 px-3 py-2 text-[10px] font-black uppercase tracking-[0.14em] text-black/35 dark:border-white/10 dark:text-white/30">
                                No feeders yet
                              </div>
                            ) : (
                              feed.feeders.map((feeder) => {
                                const isFeederSelected = selectedFeederIds.includes(feeder.id);
                                return (
                                  <motion.button
                                    key={feeder.id}
                                    type="button"
                                    whileTap={{ scale: 0.96 }}
                                    onClick={() => {
                                      play('snapLock');
                                      toggleFeeder(feed.id, feeder.id);
                                    }}
                                    className={cn(
                                      'rounded-[14px] px-3 py-2 text-[10px] font-black uppercase tracking-[0.14em] transition-colors duration-200',
                                      isFeederSelected
                                        ? 'bg-[#E11D48] text-white shadow-[0_8px_18px_rgba(225,29,72,0.26),inset_0_1px_0_rgba(255,255,255,0.7)]'
                                        : 'border border-black/6 bg-white/55 text-black/54 shadow-[0_2px_6px_rgba(0,0,0,0.05),inset_0_1px_0_rgba(255,255,255,0.7)] dark:border-white/10 dark:bg-white/[0.06] dark:text-white/48 dark:shadow-[0_8px_18px_rgba(0,0,0,0.3),inset_0_1px_0_rgba(255,255,255,0.06)]',
                                    )}
                                  >
                                    @{feeder.handle.toUpperCase()}
                                  </motion.button>
                                );
                              })
                            )}
                          </div>
                        )}
                      </div>
                    );
                  })}
                </div>
              </div>

              {!isDesktop && (
              <div className="flex flex-col gap-4">
                <div className="text-[10px] font-black uppercase tracking-[0.2em] text-black/50 dark:text-white/40">
                  Checkpoints
                </div>

                <div className="flex flex-wrap gap-2.5">
                  <motion.button
                    type="button"
                    whileTap={{ scale: 0.95 }}
                    onClick={() => onChange({ ...filters, selectedCheckpoints: [] })}
                    className={cn(
                      'rounded-[16px] px-4 py-3 text-[11px] font-black uppercase tracking-[0.16em] transition-colors duration-200',
                      filters.selectedCheckpoints.length === 0
                        ? 'bg-black text-white shadow-[0_4px_16px_rgba(0,0,0,0.2),inset_0_1px_1px_rgba(255,255,255,0.1)] dark:bg-[#E11D48] dark:text-white dark:shadow-[0_4px_20px_rgba(225,29,72,0.2),inset_0_1px_2px_rgba(255,255,255,0.5)]'
                        : 'border border-black/5 bg-black/5 text-black/56 shadow-[inset_0_2px_8px_rgba(0,0,0,0.04)] dark:border-white/5 dark:bg-white/5 dark:text-white/48 dark:shadow-[inset_0_2px_8px_rgba(0,0,0,0.3)]',
                    )}
                  >
                    ALL
                  </motion.button>

                  {availableCheckpoints.map((checkpoint) => {
                    const isSelected = filters.selectedCheckpoints.includes(checkpoint);
                    return (
                      <motion.button
                        key={checkpoint}
                        type="button"
                        whileTap={{ scale: 0.95 }}
                        onClick={() => toggleCheckpoint(checkpoint)}
                        className={cn(
                          'rounded-[16px] px-4 py-3 text-[11px] font-black uppercase tracking-[0.16em] transition-colors duration-200',
                          isSelected
                            ? 'bg-black text-white shadow-[0_4px_16px_rgba(0,0,0,0.2),inset_0_1px_1px_rgba(255,255,255,0.1)] dark:bg-[#E11D48] dark:text-white dark:shadow-[0_4px_20px_rgba(225,29,72,0.2),inset_0_1px_2px_rgba(255,255,255,0.5)]'
                            : 'border border-black/5 bg-black/5 text-black/56 shadow-[inset_0_2px_8px_rgba(0,0,0,0.04)] dark:border-white/5 dark:bg-white/5 dark:text-white/48 dark:shadow-[inset_0_2px_8px_rgba(0,0,0,0.3)]',
                        )}
                      >
                        {checkpoint}
                      </motion.button>
                    );
                  })}

                  {availableCheckpoints.length === 0 && (
                    <div className="rounded-[16px] border border-dashed border-black/8 px-4 py-3 text-[10px] font-black uppercase tracking-[0.14em] text-black/35 dark:border-white/10 dark:text-white/30">
                      No checkpoint data for this selection
                    </div>
                  )}
                </div>
              </div>
              )}

              <div className="pt-1">
                <motion.button
                  type="button"
                  onClick={() => {
                    play('navSwitch');
                    onClose();
                  }}
                  whileTap={{ scale: 0.95 }}
                  className={cn(
                    'group relative w-full overflow-hidden rounded-[24px] py-5',
                    'bg-black shadow-[0_8px_24px_rgba(0,0,0,0.2),inset_0_1px_2px_rgba(255,255,255,0.2)]',
                    'dark:bg-[#E11D48] dark:shadow-[0_8px_32px_rgba(225,29,72,0.25),inset_0_1px_2px_rgba(255,255,255,0.6)]',
                  )}
                >
                  <span className="relative z-10 text-[16px] font-black uppercase tracking-[0.15em] text-white">
                    Find Your Fire
                  </span>
                </motion.button>
              </div>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
