'use client';

import { motion, HTMLMotionProps } from 'framer-motion';
import { cn } from '@/lib/utils';

export interface GlassCardProps extends HTMLMotionProps<'div'> {
  children: React.ReactNode;
  className?: string;
  intensity?: 'subtle' | 'medium' | 'strong';
  interactive?: boolean;
  expandable?: boolean;
  layoutId?: string;
}

export function GlassCard({
  children,
  className,
  intensity = 'medium',
  interactive = false,
  expandable = false,
  layoutId,
  ...props
}: GlassCardProps) {
  const intensityClasses = {
    subtle: 'bg-white/26 dark:bg-black/40 backdrop-blur-[30px] backdrop-saturate-[165%]',
    medium: 'bg-white/34 dark:bg-black/60 backdrop-blur-[52px] backdrop-saturate-[210%]',
    strong: 'bg-white/42 dark:bg-black/90 backdrop-blur-[68px] backdrop-saturate-[250%]',
  };

  const MotionComponent = motion.div;

  return (
    <MotionComponent
      layoutId={layoutId}
      className={cn(
        'relative rounded-[28px] border border-white/40 overflow-hidden',
        'shadow-[0_40px_80px_-16px_rgba(0,0,0,0.4),inset_0_1px_2px_rgba(255,255,255,0.88),inset_0_-18px_26px_rgba(0,0,0,0.6)]',
        'dark:border-white/10 dark:shadow-[0_45px_90px_-20px_rgba(0,0,0,0.9),inset_0_1px_2px_rgba(255,255,255,0.22),inset_0_-18px_26px_rgba(0,0,0,0.6)]',
        intensityClasses[intensity],
        interactive && 'cursor-pointer transition-all duration-300',
        className
      )}
      style={{ willChange: 'transform' }}
      {...props}
    >
      {/* Grain texture overlay */}
      <div
        className="absolute inset-0 pointer-events-none opacity-[0.03] rounded-[28px]"
        style={{
          backgroundImage: `url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)'/%3E%3C/svg%3E")`,
        }}
      />
      
      {/* Glass highlight overlay */}
      <div className="pointer-events-none absolute inset-0 rounded-[28px] bg-gradient-to-br from-white/35 via-transparent to-transparent opacity-80" />
      
      {/* Content */}
      <div className="relative z-10 h-full">
        {children}
      </div>
    </MotionComponent>
  );
}

export default GlassCard;
