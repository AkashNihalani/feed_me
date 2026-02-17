'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { LayoutGrid, Flag, Fuel } from 'lucide-react';
import { motion } from 'framer-motion';
import { cn } from '@/lib/utils';

export default function BottomNav() {
  const pathname = usePathname();

  if (pathname === '/login') return null;

  const NAV_ITEMS = [
    { label: 'Feed', href: '/', icon: LayoutGrid },
    { label: 'Flags', href: '/flags', icon: Flag },
    { label: 'Fuel', href: '/profile', icon: Fuel },
  ];

  return (
    <div className="fixed bottom-5 left-0 right-0 z-50 flex justify-center pointer-events-none">
      <div className="pointer-events-auto flex items-center gap-3 bg-white/95 backdrop-blur-md border-2 border-black p-2 rounded-full shadow-[4px_4px_0px_0px_rgba(0,0,0,1)] relative">
        {NAV_ITEMS.map((item) => {
          const isActive = pathname === item.href;
          return (
            <Link
              key={item.label}
              href={item.href}
              className={cn(
                "relative flex flex-col items-center justify-center w-16 h-16 rounded-full border-2 border-transparent transition-colors z-10",
                isActive
                  ? "text-black"
                  : "text-gray-500 hover:text-black"
              )}
            >
              {isActive && (
                <motion.div
                  layoutId="nav-active"
                  className="absolute -bottom-1 w-1.5 h-1.5 bg-[#CCFF00] rounded-full shadow-[0_0_8px_#CCFF00]"
                  transition={{ type: "spring", stiffness: 300, damping: 30 }}
                />
              )}
              <item.icon size={22} className={cn("relative z-20 transition-all duration-300", isActive ? "text-black scale-110" : "text-neutral-500 group-hover:text-black")} />
              <span className={cn(
                  "relative z-20 text-[10px] font-black uppercase mt-1 mb-1 transition-colors duration-300 tracking-widest",
                  isActive ? "text-black" : "text-neutral-500"
              )}>{item.label}</span>
            </Link>
          );
        })}
      </div>
    </div>
  );
}
