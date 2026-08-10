'use client';

import { useEffect, type CSSProperties } from 'react';
import Link from 'next/link';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { ArrowUpRight, CreditCard, X } from 'lucide-react';
import { EmbeddedFundPage } from '@/components/tabs/FundTab';
import { useAppHaptics } from '@/lib/haptics';
import { cn } from '@/lib/utils';

type FundPanelOverlayProps = {
  open: boolean;
  onClose: () => void;
};

export default function FundPanelOverlay({ open, onClose }: FundPanelOverlayProps) {
  const { play } = useAppHaptics();
  const portalTarget = typeof document === 'undefined' ? null : document.body;

  useEffect(() => {
    if (!open) return undefined;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      play('navReselect');
      onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [onClose, open, play]);

  if (!portalTarget) return null;

  return createPortal((
    <AnimatePresence>
      {open && (
        <motion.div
          role="dialog"
          aria-modal="true"
          aria-label="Fund and account"
          initial={false}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.3, ease: [0.22, 1, 0.36, 1] }}
          className="fm-chrome-safe-overlay fixed inset-0 z-[410] flex items-end justify-center pointer-events-auto sm:items-center"
          onClick={() => {
            play('navReselect');
            onClose();
          }}
        >
          <motion.div
            aria-hidden="true"
            className="fm-chrome-safe-backdrop"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.28, ease: [0.22, 1, 0.36, 1] }}
            style={{ '--fm-overlay-bg': 'rgba(0,0,0,0.58)' } as CSSProperties}
          />

          <motion.div
            initial={{ opacity: 0, scale: 0.975, y: 38 }}
            animate={{ opacity: 1, scale: 1, y: 0 }}
            exit={{ opacity: 0, scale: 0.982, y: 24 }}
            transition={{ type: 'spring', stiffness: 340, damping: 34, mass: 0.94 }}
            className={cn(
              'relative z-10 mb-0 flex h-[92vh] w-full flex-col overflow-hidden rounded-t-[34px]',
              'border border-white/80 border-t-white/90 bg-white/94',
              'shadow-[0_1px_0_rgba(255,255,255,0.95)_inset,0_-1px_0_rgba(0,0,0,0.03)_inset,0_24px_64px_-16px_rgba(0,0,0,0.15)]',
              'dark:border-white/[0.08] dark:border-t-white/[0.12] dark:bg-[rgba(10,10,12,0.96)]',
              'dark:shadow-[0_1px_0_rgba(255,255,255,0.06)_inset,0_-1px_0_rgba(0,0,0,0.5)_inset,0_32px_80px_rgba(0,0,0,0.62)]',
              'sm:mb-8 sm:h-[min(86vh,920px)] sm:max-w-[min(1040px,calc(100vw-28px))] sm:rounded-[34px]',
            )}
            onClick={(event) => event.stopPropagation()}
          >
            <div
              className="pointer-events-none absolute inset-[1px] z-0 rounded-[34px] dark:hidden"
              style={{ boxShadow: 'inset 0 2px 4px rgba(255,255,255,0.7), inset 0 -2px 6px rgba(0,0,0,0.04)' }}
            />

            <div className="relative z-10 flex shrink-0 items-center justify-between gap-3 border-b border-black/6 px-4 py-3.5 dark:border-white/8 sm:px-6 sm:py-4">
              <div className="flex min-w-0 items-center gap-3">
                <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-[16px] bg-[var(--fm-accent)] text-white shadow-[0_8px_18px_rgb(var(--fm-accent-rgb)/0.26),inset_0_1px_0_rgba(255,255,255,0.62)]">
                  <CreditCard size={18} strokeWidth={2.7} />
                </div>
                <div className="min-w-0">
                  <div className="truncate text-[18px] font-black uppercase tracking-[-0.04em] text-foreground dark:text-white sm:text-[22px]">
                    Fund
                  </div>
                  <div className="mt-0.5 truncate text-[8px] font-black uppercase tracking-[0.14em] text-foreground/42 dark:text-white/34 sm:text-[10px]">
                    Feed Pass · alerts · account
                  </div>
                </div>
              </div>

              <div className="flex shrink-0 items-center gap-2">
                <Link
                  href="/profile"
                  onClick={() => {
                    play('navSwitch');
                    onClose();
                  }}
                  className="hidden items-center gap-1.5 rounded-[14px] border border-black/6 bg-white/62 px-3 py-2 text-[10px] font-black uppercase tracking-[0.14em] text-foreground/58 shadow-[0_3px_10px_rgba(15,23,42,0.05),inset_0_1px_0_rgba(255,255,255,0.75)] dark:border-white/8 dark:bg-white/[0.06] dark:text-white/52 sm:inline-flex"
                >
                  Full page
                  <ArrowUpRight size={13} strokeWidth={2.6} />
                </Link>
                <motion.button
                  type="button"
                  whileTap={{ scale: 0.94 }}
                  onClick={() => {
                    play('navReselect');
                    onClose();
                  }}
                  aria-label="Close fund panel"
                  className="flex h-10 w-10 items-center justify-center rounded-[16px] border border-black/6 bg-white/62 text-foreground/56 shadow-[0_3px_10px_rgba(15,23,42,0.05),inset_0_1px_0_rgba(255,255,255,0.75)] dark:border-white/8 dark:bg-white/[0.06] dark:text-white/48"
                >
                  <X size={17} strokeWidth={2.7} />
                </motion.button>
              </div>
            </div>

            <div
              className="hide-scrollbar relative z-10 flex-1 overflow-y-auto overflow-x-hidden px-2 py-3 sm:px-4 sm:py-4"
              style={{ WebkitOverflowScrolling: 'touch' }}
            >
              <EmbeddedFundPage />
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  ), portalTarget);
}
