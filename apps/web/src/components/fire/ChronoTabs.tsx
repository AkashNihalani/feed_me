import { useEffect, useMemo, useState } from 'react';
import { LayoutGroup, motion } from 'framer-motion';
import { useAppHaptics } from '@/lib/haptics';

export type ChronoTabsProps = {
  days: string[];
  activeDay: string;
  onChange: (dayKey: string) => void;
  compact?: boolean;
  highlightId?: string;
};

export default function ChronoTabs({ days, activeDay, onChange, compact = false, highlightId }: ChronoTabsProps) {
  const sortedDays = useMemo(() => [...days].sort((a, b) => a.localeCompare(b)), [days]);
  const { play } = useAppHaptics();
  const [optimisticDay, setOptimisticDay] = useState(activeDay);
  const activeCandidate = sortedDays.includes(optimisticDay) ? optimisticDay : activeDay;
  const activePillLayoutId = highlightId ?? `chrono-pill-bg-${compact ? 'compact' : 'regular'}`;

  useEffect(() => {
    setOptimisticDay(activeDay);
  }, [activeDay]);

  return (
    <div className={compact ? 'pointer-events-auto w-full max-w-[940px]' : 'pointer-events-auto w-full max-w-[560px]'}>
      {/* Recessed pill track — matches dashboard timeframe selector */}
      <LayoutGroup id={activePillLayoutId}>
        <div className={[
          'relative flex items-center gap-1 border border-black/[0.04] bg-black/[0.03] shadow-[inset_0_2px_4px_rgba(0,0,0,0.06),inset_0_-1px_0_rgba(255,255,255,0.5)] dark:border-white/[0.05] dark:bg-white/[0.03] dark:shadow-[inset_0_2px_4px_rgba(0,0,0,0.3),inset_0_-1px_0_rgba(255,255,255,0.03)]',
          compact ? 'rounded-[14px] p-[3px]' : 'rounded-[18px] p-1',
        ].join(' ')}>
          {sortedDays.map((dateStr) => {
            const isActive = activeCandidate === dateStr;
            const dayNumber = dateStr.split('-')[2];

            return (
              <motion.button
                key={dateStr}
                type="button"
                onClick={() => {
                  if (dateStr === activeCandidate) return;
                  setOptimisticDay(dateStr);
                  play('snapLock');
                  onChange(dateStr);
                }}
                whileTap={{ scale: 0.96 }}
                className={[
                  'relative flex-1 text-center font-black',
                  compact ? 'rounded-[10px] py-1.5 sm:py-2' : 'rounded-[14px] py-2.5 sm:py-3',
                  isActive ? 'z-10' : 'z-0',
                ].join(' ')}
                style={{ WebkitTapHighlightColor: 'transparent' }}
              >
                {isActive && (
                  <motion.span
                    layoutId={activePillLayoutId}
                    layout
                    className={[
                      'absolute inset-0 bg-[var(--fm-accent)] shadow-[0_4px_16px_rgb(var(--fm-accent-rgb)/0.25),0_1px_2px_rgba(0,0,0,0.08)] dark:shadow-[0_4px_20px_rgb(var(--fm-accent-rgb)/0.2),0_12px_28px_rgba(0,0,0,0.5)]',
                      compact ? 'rounded-[10px]' : 'rounded-[14px]',
                    ].join(' ')}
                    transition={{ type: 'spring', stiffness: 360, damping: 34, mass: 0.82 }}
                  />
                )}
                <motion.span
                  className={[
                    'relative z-10 inline-block leading-none transition-colors duration-200',
                    compact
                      ? 'text-[12px] tracking-[-0.04em] sm:text-[14px]'
                      : 'text-[16px] tracking-[-0.04em] sm:text-[16px]',
                    isActive ? 'text-white' : 'text-black/40 dark:text-white/35',
                  ].join(' ')}
                  animate={{
                    scale: isActive ? (compact ? 1.28 : 1.48) : 1,
                    opacity: isActive ? 1 : 0.78,
                  }}
                  transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.72 }}
                >
                  {dayNumber}
                </motion.span>
              </motion.button>
            );
          })}
        </div>
      </LayoutGroup>
    </div>
  );
}
