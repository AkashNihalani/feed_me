'use client';

import { motion, HTMLMotionProps } from 'framer-motion';
import { cn } from '@/lib/utils';

interface GlassShellProps extends HTMLMotionProps<'div'> {
  children: React.ReactNode;
  className?: string;
  intensity?: 'subtle' | 'medium' | 'strong';
}

export default function GlassShell({ 
  children, 
  className, 
  intensity = 'medium',
  ...props 
}: GlassShellProps) {
  const intensityClasses = {
    subtle: 'bg-white/26 dark:bg-black/22 backdrop-blur-[30px] backdrop-saturate-[165%]',
    medium: 'bg-white/34 dark:bg-black/30 backdrop-blur-[52px] backdrop-saturate-[210%]',
    strong: 'bg-white/42 dark:bg-black/40 backdrop-blur-[68px] backdrop-saturate-[250%]',
  };

  return (
    <motion.div
      className={cn(
        'relative rounded-[32px] border border-white/38 dark:border-white/18 overflow-hidden',
        'shadow-[0_26px_66px_-12px_rgba(0,0,0,0.18),inset_0_1px_2px_rgba(255,255,255,0.82),inset_0_-18px_26px_rgba(255,255,255,0.14)]',
        'dark:shadow-[0_26px_66px_-12px_rgba(0,0,0,0.64),inset_0_1px_2px_rgba(255,255,255,0.16),inset_0_-18px_26px_rgba(0,0,0,0.32)]',
        intensityClasses[intensity],
        className
      )}
      {...props}
    >
      {/* Subtle grain texture overlay */}
      <div 
        className="absolute inset-0 pointer-events-none opacity-[0.03] rounded-[32px]"
        style={{
          backgroundImage: `url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E")`,
        }}
      />
      <div className="pointer-events-none absolute inset-0 fm-glass-shell-highlight" />
      <div className="relative z-10">
        {children}
      </div>
    </motion.div>
  );
}
