'use client';

import Link from 'next/link';
import { useEffect } from 'react';
import { usePathname } from 'next/navigation';
import { LayoutGrid, Flame, IndianRupee } from 'lucide-react';
import { motion } from 'framer-motion';
import { useAppHaptics } from '@/lib/haptics';
import { cn } from '@/lib/utils';

const NAV_ITEMS = [
  { label: 'Feed', href: '/', icon: LayoutGrid },
  { label: 'Fire', href: '/fire', icon: Flame },
  { label: 'Fund', href: '/profile', icon: IndianRupee },
];

export default function BottomNav() {
  const pathname = usePathname();
  const { play } = useAppHaptics();

  useEffect(() => {
    try {
      if (!pathname || pathname === '/login') return;
      const isTab = pathname === '/' || pathname === '/fire' || pathname === '/profile';
      if (!isTab) return;
      const existing = sessionStorage.getItem('feedme:last-tab');
      const intent = sessionStorage.getItem('feedme:intent');
      if (pathname === '/' && existing && existing !== '/' && intent !== '/') return;
      sessionStorage.setItem('feedme:last-tab', pathname);
      sessionStorage.setItem('feedme:last-tab-ts', String(Date.now()));
    } catch {}
  }, [pathname]);

  if (pathname === '/login') return null;

  return (
    <div className="fixed bottom-[calc(12px+env(safe-area-inset-bottom))] left-0 right-0 z-50 flex justify-center pointer-events-none md:bottom-5">
      <div className={cn(
        'pointer-events-auto relative flex items-center gap-0.5 overflow-hidden rounded-[28px] px-1 py-1',
        /* Neumorphic frosted glass — matching header depth */
        'bg-white/65 backdrop-blur-[48px] backdrop-saturate-[200%]',
        'border border-white/80 border-t-white/90',
        'shadow-[0_1px_0_rgba(255,255,255,0.92)_inset,0_-1px_0_rgba(0,0,0,0.03)_inset,0_4px_8px_rgba(0,0,0,0.04),0_12px_28px_-4px_rgba(0,0,0,0.1),0_28px_56px_-10px_rgba(0,0,0,0.08)]',
        'dark:bg-[rgba(6,6,6,0.65)] dark:border-white/[0.07] dark:border-t-white/[0.12]',
        'dark:shadow-[0_1px_0_rgba(255,255,255,0.06)_inset,0_-1px_0_rgba(0,0,0,0.5)_inset,0_8px_16px_rgba(0,0,0,0.4),0_24px_48px_-8px_rgba(0,0,0,0.5)]',
      )}>
        {/* Inner bevel overlay for neumorphic depth */}
        <div className="pointer-events-none absolute inset-0 rounded-[28px] z-0 dark:opacity-0 transition-opacity"
          style={{
            background: 'linear-gradient(180deg, rgba(255,255,255,0.45) 0%, rgba(255,255,255,0) 40%, rgba(0,0,0,0.01) 100%)',
          }}
        />

        {NAV_ITEMS.map((item) => {
          const isActive = pathname === item.href;
          return (
            <Link key={item.label} href={item.href} className="group relative z-10"
              onClick={() => {
                play(isActive ? 'navReselect' : 'navSwitch');
                try {
                  sessionStorage.setItem('feedme:intent', item.href);
                  sessionStorage.setItem('feedme:intent-ts', String(Date.now()));
                } catch {}
              }}>
              <motion.div whileTap={{ scale: 0.92 }} transition={{ type: 'spring', stiffness: 500, damping: 28 }}
                className={cn('relative flex min-w-[86px] flex-col items-center justify-center rounded-[22px] px-3 py-2.5',
                  isActive ? 'text-black dark:text-black' : 'text-black/50 dark:text-white/45')}>
                {isActive && (
                  <motion.span layoutId="nav-active-pill"
                    className="absolute inset-0 rounded-[22px] bg-[#CCFF00] shadow-[0_4px_16px_rgba(204,255,0,0.25),0_1px_2px_rgba(0,0,0,0.08)] dark:shadow-[0_4px_20px_rgba(204,255,0,0.25),0_8px_24px_rgba(0,0,0,0.4)]"
                    transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }} />
                )}
                <span className="relative z-10">
                  <item.icon size={21} strokeWidth={2.75} />
                </span>
                <span className={cn('relative z-10 mt-1 text-[10px] font-black uppercase tracking-[0.18em]',
                  isActive ? 'text-black' : '')}>
                  {item.label}
                </span>
              </motion.div>
            </Link>
          );
        })}
      </div>
    </div>
  );
}
