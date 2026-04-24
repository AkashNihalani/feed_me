'use client';

import Link from 'next/link';
import { useEffect } from 'react';
import { usePathname, useRouter } from 'next/navigation';
import { LayoutGrid, Flame } from 'lucide-react';
import { LayoutGroup, motion } from 'framer-motion';
import { useAppHaptics } from '@/lib/haptics';
import { cn } from '@/lib/utils';

type NavIconProps = {
  size?: number;
  className?: string;
  strokeWidth?: number;
};

function FundNavIcon({ size = 20, className = '' }: NavIconProps) {
  return (
    <span
      aria-hidden="true"
      className={cn('flex items-center justify-center font-black leading-none tracking-[-0.08em]', className)}
      style={{ fontSize: size + 1, transform: 'translateY(1px)' }}
    >
      ₹
    </span>
  );
}

const NAV_ITEMS = [
  { label: 'Feed', href: '/', icon: LayoutGrid },
  { label: 'Fire', href: '/fire', icon: Flame },
  { label: 'Fund', href: '/profile', icon: FundNavIcon },
];

export default function BottomNav() {
  const pathname = usePathname();
  const router = useRouter();
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

  useEffect(() => {
    router.prefetch('/');
    router.prefetch('/fire');
    router.prefetch('/profile');
  }, [router]);

  if (pathname === '/login') return null;

  return (
    <div className="fixed bottom-[calc(12px+env(safe-area-inset-bottom))] left-0 right-0 z-[180] flex justify-center pointer-events-none md:bottom-5">
      <div className="fm-depth-chrome fm-depth-chrome--nav pointer-events-auto flex items-center gap-0.5 px-1 py-1 lg:rounded-[26px]">
        <LayoutGroup id="feedme-bottom-nav">
          {NAV_ITEMS.map((item) => {
            const isActive = pathname === item.href;
            return (
              <Link key={item.label} href={item.href} prefetch className="group relative z-10"
                onClick={() => {
                  play(isActive ? 'navReselect' : 'navSwitch');
                  if (isActive && item.href === '/fire' && typeof window !== 'undefined') {
                    window.dispatchEvent(new CustomEvent('feedme:fire-tab-reselect'));
                  }
                  try {
                    sessionStorage.setItem('feedme:intent', item.href);
                    sessionStorage.setItem('feedme:intent-ts', String(Date.now()));
                  } catch {}
                }}>
                <motion.div whileTap={{ scale: 0.92 }} transition={{ type: 'spring', stiffness: 500, damping: 28 }}
                  className={cn(
                    'relative flex min-w-[86px] flex-col items-center justify-center rounded-[22px] px-3 py-2.5 lg:min-w-[78px] lg:px-3 lg:py-2',
                    isActive ? 'text-white' : 'text-[#8f6b75] dark:text-[#a3828b]')}>
                  {isActive && (
                    <motion.span
                      layoutId="nav-active-pill"
                      initial={false}
                      className="absolute inset-0 rounded-[22px] bg-[#E11D48] shadow-[0_4px_16px_rgba(225,29,72,0.25),0_1px_2px_rgba(0,0,0,0.08)] dark:shadow-[0_4px_20px_rgba(225,29,72,0.25),0_8px_24px_rgba(0,0,0,0.4)]"
                      transition={{ type: 'spring', stiffness: 420, damping: 32, mass: 0.8 }}
                    />
                  )}
                  <span className="relative z-10">
                    <item.icon size={20} strokeWidth={2.75} />
                  </span>
                  <span className="relative z-10 mt-1 text-[10px] font-black uppercase tracking-[0.18em]">
                    {item.label}
                  </span>
                </motion.div>
              </Link>
            );
          })}
        </LayoutGroup>
      </div>
    </div>
  );
}
