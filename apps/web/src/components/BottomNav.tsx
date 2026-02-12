'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { Zap, Flag, Fuel } from 'lucide-react';
import { cn } from '@/lib/utils';

export default function BottomNav() {
  const pathname = usePathname();

  if (pathname === '/login') return null;

  const NAV_ITEMS = [
    { label: 'Feed', href: '/', icon: Zap },
    { label: 'Flags', href: '/flags', icon: Flag },
    { label: 'Fuel', href: '/profile', icon: Fuel },
  ];

  return (
    <div className="fixed bottom-5 left-0 right-0 z-50 flex justify-center md:hidden pointer-events-none">
      <div className="pointer-events-auto flex items-center gap-3 bg-white/95 backdrop-blur-md border-2 border-black p-2 rounded-full shadow-[4px_4px_0px_0px_rgba(0,0,0,1)]">
        {NAV_ITEMS.map((item) => {
          const isActive = pathname === item.href;
          return (
            <Link
              key={item.label}
              href={item.href}
              className={cn(
                "relative flex flex-col items-center justify-center w-16 h-16 rounded-full border-2 border-transparent transition-all",
                isActive
                  ? "text-black"
                  : "text-gray-500 hover:text-black"
              )}
            >
              <item.icon size={22} className={cn(isActive ? "fill-current" : "fill-none")} />
              <span className="text-[10px] font-black uppercase mt-1">{item.label}</span>
              {isActive && (
                <span className="absolute -bottom-1 h-[6px] w-[6px] rounded-full bg-lime border-2 border-black" />
              )}
            </Link>
          );
        })}
      </div>
    </div>
  );
}
