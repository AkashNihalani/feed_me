'use client';

import { motion, AnimatePresence } from 'framer-motion';
import { useAppHaptics } from '@/lib/haptics';
import { cn } from '@/lib/utils';
import { Check } from 'lucide-react';

export type FilterScope = 'ALL' | 'ANCHOR' | string;
export type FilterThreshold = '10' | '25' | '50' | 'ALL';

interface ZSpaceFilterProps {
  isOpen: boolean;
  onClose: () => void;
  activeThreshold: FilterThreshold;
  activeScope: FilterScope;
  availableScopes?: string[];
  onChange: (threshold: FilterThreshold, scope: FilterScope) => void;
}

export default function ZSpaceFilter({
  isOpen,
  onClose,
  activeThreshold,
  activeScope,
  availableScopes = [],
  onChange
}: ZSpaceFilterProps) {
  const { play } = useAppHaptics();

  const thresholds: { label: string; value: FilterThreshold }[] = [
    { label: 'TOP 10', value: '10' },
    { label: 'TOP 25', value: '25' },
    { label: 'TOP 50', value: '50' },
    { label: 'ALL SIGNALS', value: 'ALL' },
  ];

  const scopes: { label: string; value: FilterScope }[] = [
    { label: 'ALL FEEDS', value: 'ALL' },
    ...availableScopes.map((s) => ({ label: `@${s.toUpperCase()}`, value: s }))
  ];

  return (
    <AnimatePresence>
      {isOpen && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.3, ease: [0.25, 0.1, 0.25, 1] }}
          className="fixed inset-0 z-[200] flex items-end sm:items-center justify-center pointer-events-auto"
          onClick={() => {
            play('navReselect');
            onClose();
          }}
        >
          {/* Deep Blur Backdrop */}
          <div className="absolute inset-0 bg-black/28 backdrop-blur-md dark:bg-black/48" />

          {/* Modal Container */}
          <motion.div
            initial={{ y: 0, opacity: 0, scale: 0.95, translateY: '100%' }}
            animate={{ y: 0, opacity: 1, scale: 1, translateY: '0%' }}
            exit={{ y: 0, opacity: 0, scale: 0.95, translateY: '100%' }}
            transition={{
              type: 'spring',
              stiffness: 450,
              damping: 35,
              mass: 0.8
            }}
            className={cn(
              'relative w-full max-w-lg mb-0 sm:mb-12 flex flex-col',
              'rounded-t-[36px] sm:rounded-[36px] overflow-hidden',
              'bg-white/70 backdrop-blur-[40px] backdrop-saturate-[180%]',
              'border border-white/80 border-t-white/90',
              'shadow-[0_1px_0_rgba(255,255,255,0.95)_inset,0_-1px_0_rgba(0,0,0,0.03)_inset,0_24px_64px_-16px_rgba(0,0,0,0.15)]',
              'dark:bg-[rgba(6,6,6,0.7)] dark:border-white/[0.08] dark:border-t-white/[0.12]',
              'dark:shadow-[0_1px_0_rgba(255,255,255,0.06)_inset,0_-1px_0_rgba(0,0,0,0.5)_inset,0_32px_80px_rgba(0,0,0,0.6)]'
            )}
            onClick={(e) => e.stopPropagation()}
          >
            {/* Modal Inner Shadow */}
            <div className="pointer-events-none absolute inset-[1px] rounded-[35px] z-0 dark:hidden"
                style={{
                  boxShadow: 'inset 0 2px 4px rgba(255,255,255,0.7), inset 0 -2px 6px rgba(0,0,0,0.04)',
                }}
              />

            <div className="relative z-10 px-6 py-8 sm:px-8 sm:py-10 flex flex-col gap-10">
              
              {/* L1: Signal Threshold */}
              <div className="flex flex-col gap-4">
                <div className="text-[10px] font-black uppercase tracking-[0.2em] text-black/50 dark:text-white/40">
                  L1: Signal Target
                </div>
                
                <div className="grid grid-cols-2 gap-2.5 sm:grid-cols-4">
                  {thresholds.map((t) => {
                    const isActive = activeThreshold === t.value;
                    return (
                      <motion.button
                        key={t.value}
                        type="button"
                        whileTap={{ scale: 0.94 }}
                        onClick={() => {
                          if (!isActive) play('snapLock');
                          onChange(t.value, activeScope);
                        }}
                        className={cn(
                          'relative flex items-center justify-center rounded-[18px] py-4 px-2',
                          'transition-colors duration-200',
                          isActive 
                            ? 'bg-black shadow-[0_4px_16px_rgba(0,0,0,0.2),inset_0_1px_1px_rgba(255,255,255,0.1)] dark:bg-[#CCFF00] dark:shadow-[0_4px_20px_rgba(204,255,0,0.2),inset_0_1px_2px_rgba(255,255,255,0.5)]' 
                            : 'bg-black/5 border border-black/5 dark:bg-white/5 dark:border-white/5 shadow-[inset_0_2px_8px_rgba(0,0,0,0.04)] dark:shadow-[inset_0_2px_8px_rgba(0,0,0,0.3)]'
                        )}
                      >
                        <span className={cn(
                          'text-[13px] font-black tracking-[0.08em]',
                          isActive 
                            ? 'text-white dark:text-black' 
                            : 'text-black/60 dark:text-white/50'
                        )}>
                          {t.label}
                        </span>
                      </motion.button>
                    );
                  })}
                </div>
              </div>

              {/* L2: Feed Scope */}
              <div className="flex flex-col gap-4">
                <div className="text-[10px] font-black uppercase tracking-[0.2em] text-black/50 dark:text-white/40">
                  L2: Radar Scope
                </div>
                
                {/* Scrollable vertical list for lots of handles */}
                <div 
                  className="flex flex-col gap-2 max-h-[220px] overflow-y-auto hide-scrollbar rounded-[20px] p-1.5 -mx-1.5"
                  style={{ WebkitOverflowScrolling: 'touch' }}
                >
                  {scopes.map((s) => {
                    const isActive = activeScope === s.value;
                    return (
                      <motion.button
                        key={s.value}
                        type="button"
                        whileTap={{ scale: 0.97 }}
                        onClick={() => {
                          if (!isActive) play('snapLock');
                          onChange(activeThreshold, s.value);
                        }}
                        className={cn(
                          'relative flex items-center justify-between rounded-[20px] px-5 py-4',
                          'transition-colors duration-200 w-full text-left cursor-pointer',
                          isActive 
                            ? 'bg-white shadow-[0_4px_16px_rgba(0,0,0,0.08),inset_0_1px_1px_rgba(255,255,255,0.9)] border border-black/5 dark:bg-black dark:border-white/10 dark:shadow-[0_8px_24px_rgba(0,0,0,0.4),inset_0_1px_1px_rgba(255,255,255,0.05)]' 
                            : 'bg-transparent border-transparent'
                        )}
                      >
                        <span className={cn(
                          'text-[15px] sm:text-[16px] font-black tracking-tight truncate pr-4',
                          isActive 
                            ? 'text-black drop-shadow-sm dark:text-[#CCFF00] dark:drop-shadow-[0_0_12px_rgba(204,255,0,0.3)]' 
                            : 'text-black/60 dark:text-white/50'
                        )}>
                          {s.label}
                        </span>
                        
                        {isActive && (
                          <div className="flex shrink-0 items-center justify-center h-6 w-6 rounded-full bg-black/5 dark:bg-[#CCFF00]/10">
                            <Check size={14} strokeWidth={4} className="text-black dark:text-[#CCFF00]" />
                          </div>
                        )}
                      </motion.button>
                    );
                  })}
                </div>
              </div>

              {/* Action Button */}
              <div className="pt-2">
                <motion.button
                  type="button"
                  onClick={() => {
                    play('navSwitch');
                    onClose();
                  }}
                  whileTap={{ scale: 0.95 }}
                  className={cn(
                    'group relative w-full overflow-hidden rounded-[24px] py-5',
                    'bg-black dark:bg-[#CCFF00]',
                    'shadow-[0_8px_24px_rgba(0,0,0,0.2),inset_0_1px_2px_rgba(255,255,255,0.2)] dark:shadow-[0_8px_32px_rgba(204,255,0,0.25),inset_0_1px_2px_rgba(255,255,255,0.6)]'
                  )}
                >
                  <span className="relative z-10 text-[16px] font-black uppercase tracking-[0.15em] text-white dark:text-black">
                    FIND YOUR FIRE
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
