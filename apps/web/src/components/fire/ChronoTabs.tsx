import { useMemo } from 'react';
import { motion } from 'framer-motion';
import { useAppHaptics } from '@/lib/haptics';

export type ChronoTabsProps = {
  days: string[];
  activeDay: string;
  onChange: (dayKey: string) => void;
  compact?: boolean;
};

export default function ChronoTabs({ days, activeDay, onChange, compact = false }: ChronoTabsProps) {
  const sortedDays = useMemo(() => [...days].sort((a, b) => a.localeCompare(b)), [days]);
  const { play } = useAppHaptics();

  return (
    <div className={compact ? 'pointer-events-auto w-full max-w-[520px]' : 'pointer-events-auto w-full max-w-[560px]'}>
      {/* Recessed pill track — matches dashboard timeframe selector */}
      <div className={[
        'relative flex items-center gap-1 border border-black/[0.04] bg-black/[0.03] shadow-[inset_0_2px_4px_rgba(0,0,0,0.06),inset_0_-1px_0_rgba(255,255,255,0.5)] dark:border-white/[0.05] dark:bg-white/[0.03] dark:shadow-[inset_0_2px_4px_rgba(0,0,0,0.3),inset_0_-1px_0_rgba(255,255,255,0.03)]',
        compact ? 'rounded-[14px] p-[3px]' : 'rounded-[16px] p-1',
      ].join(' ')}>
        {sortedDays.map((dateStr) => {
          const isActive = activeDay === dateStr;
          const dayNumber = dateStr.split('-')[2];

          return (
            <motion.button
              key={dateStr}
              type="button"
              onClick={() => {
                play('snapLock');
                onChange(dateStr);
              }}
              whileTap={{ scale: 0.95 }}
              className={[
                'relative flex-1 text-center font-black',
                compact ? 'rounded-[10px] py-1.5 sm:py-2' : 'rounded-[12px] py-2.5 sm:py-3',
                isActive
                  ? compact
                    ? 'z-10 text-[16px] tracking-[-0.04em] text-black sm:text-[18px]'
                    : 'z-10 text-[22px] tracking-[-0.04em] text-black sm:text-[28px]'
                  : compact
                    ? 'z-0 text-[11px] tracking-[-0.02em] text-black/40 sm:text-[12px] dark:text-white/35'
                    : 'z-0 text-[14px] tracking-[-0.02em] text-black/40 sm:text-[16px] dark:text-white/35',
              ].join(' ')}
              style={{ WebkitTapHighlightColor: 'transparent' }}
            >
              {isActive && (
                <motion.span
                  layoutId="chrono-pill-bg"
                  className={[
                    'absolute inset-0 bg-[#CCFF00] shadow-[0_4px_16px_rgba(204,255,0,0.25),0_1px_2px_rgba(0,0,0,0.08)] dark:shadow-[0_4px_20px_rgba(204,255,0,0.2),0_12px_28px_rgba(0,0,0,0.5)]',
                    compact ? 'rounded-[10px]' : 'rounded-[12px]',
                  ].join(' ')}
                  transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }}
                />
              )}
              <span className="relative z-10">{dayNumber}</span>
            </motion.button>
          );
        })}
      </div>
    </div>
  );
}
