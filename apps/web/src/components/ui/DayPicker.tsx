'use client';

import { useRef, useEffect } from 'react';
import { motion } from 'framer-motion';
import { format, isToday, isFuture, isSameDay } from 'date-fns';
import { cn } from '@/lib/utils';

interface DayPickerProps {
  days: Date[];
  activeDate: Date;
  onSelect: (date: Date) => void;
}

export function DayPicker({ days, activeDate, onSelect }: DayPickerProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  // Auto-scroll to selected day on mount or change
  useEffect(() => {
    if (containerRef.current) {
      const selectedEl = containerRef.current.querySelector('[data-selected="true"]');
      if (selectedEl) {
        selectedEl.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
      }
    }
  }, [activeDate]);

  const handleSelect = (date: Date) => {
    // Prevent selecting Today or Future (Elastic Snap-back Logic handled by parent state usually, 
    // but here we just ignore the click or trigger a shake)
    if (isToday(date) || isFuture(date)) {
        // Optional: Trigger haptic or visual resist
        return;
    }
    onSelect(date);
  };

  return (
    <div className="relative w-full h-24 bg-neutral-100 border-y-4 border-black overflow-hidden select-none">
      
      {/* Central Lens / Needle */}
      <div className="absolute left-1/2 top-0 bottom-0 w-1 bg-lime z-20 -translate-x-1/2 shadow-[0_0_10px_#CCFF00]" />
      <div className="absolute left-1/2 top-0 -translate-x-1/2 -translate-y-1/2 w-4 h-4 bg-black rotate-45 z-30" />
      <div className="absolute left-1/2 bottom-0 -translate-x-1/2 translate-y-1/2 w-4 h-4 bg-black rotate-45 z-30" />

      {/* Scroll Container (The Tape) */}
      <div 
        ref={containerRef}
        className="flex items-center h-full overflow-x-auto px-[50%] snap-x snap-mandatory no-scrollbar"
      >
        {days.map((date) => {
          const isSelected = isSameDay(date, activeDate);
          const isRestricted = isToday(date) || isFuture(date);
          
          return (
            <div
              key={date.toISOString()}
              className="relative flex-shrink-0 snap-center"
            >
                <button
                    onClick={() => handleSelect(date)}
                    data-selected={isSelected}
                    className={cn(
                        "h-full w-20 flex flex-col items-center justify-center relative group transition-opacity",
                        isRestricted ? "opacity-100 cursor-not-allowed" : "cursor-pointer hover:bg-black/5"
                    )}
                >
                    {/* Ticks */}
                    <div className="absolute top-0 w-0.5 h-3 bg-black" />
                    <div className="absolute bottom-0 w-0.5 h-3 bg-black" />
                    
                    {/* Minor Ticks */}
                    <div className="absolute top-0 right-0 w-px h-1.5 bg-neutral-400" />
                    <div className="absolute bottom-0 right-0 w-px h-1.5 bg-neutral-400" />
                    
                    {isRestricted ? (
                        // HAZARD ZONE
                        <div className="w-full h-full flex items-center justify-center bg-[repeating-linear-gradient(45deg,#000,#000_5px,#CCFF00_5px,#CCFF00_10px)] opacity-80">
                            {/* No Text, just hazard tape */}
                        </div>
                    ) : (
                        // DATE CONTENT
                        <div className={cn(
                            "transition-all duration-300 flex flex-col items-center",
                            isSelected ? "scale-125 opacity-100" : "opacity-40 scale-90"
                        )}>
                            <span className="text-[10px] font-black uppercase tracking-wider font-mono">
                                {format(date, 'MMM')}
                            </span>
                            <span className={cn(
                                "text-3xl font-black leading-none",
                                isSelected && "text-black"
                            )}>
                                {format(date, 'dd')}
                            </span>
                        </div>
                    )}
                </button>
            </div>
          );
        })}
      </div>
      
      {/* Side Vignettes/Fade */}
      <div className="absolute inset-y-0 left-0 w-12 bg-gradient-to-r from-neutral-100 to-transparent pointer-events-none z-10" />
      <div className="absolute inset-y-0 right-0 w-12 bg-gradient-to-l from-neutral-100 to-transparent pointer-events-none z-10" />
    </div>
  );
}
