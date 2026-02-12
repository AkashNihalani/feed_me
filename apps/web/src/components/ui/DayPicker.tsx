'use client';

import { useRef, useEffect } from 'react';
import { motion } from 'framer-motion';
import { format, isToday, isYesterday } from 'date-fns';
import { cn } from '@/lib/utils';
import { ArrowLeft, ArrowRight } from 'lucide-react';

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

  return (
    <div className="relative w-full overflow-hidden neo-card p-0 bg-white">
      {/* Scroll Container */}
      <div 
        ref={containerRef}
        className="flex items-center gap-3 overflow-x-auto p-4 snap-x snap-mandatory no-scrollbar"
        style={{ scrollPaddingLeft: '50%', scrollPaddingRight: '50%' }}
      >
        {days.map((date) => {
          const isSelected = date.toDateString() === activeDate.toDateString();
          const isCurrentDay = isToday(date);
          
          let label = format(date, 'EEE');
          if (isCurrentDay) label = 'TODAY';
          else if (isYesterday(date)) label = 'YESTERDAY';

          return (
            <motion.button
              key={date.toISOString()}
              data-selected={isSelected}
              onClick={() => onSelect(date)}
              whileTap={{ scale: 0.95 }}
              className={cn(
                "relative flex-shrink-0 snap-center flex flex-col items-center justify-center transition-all duration-300 border-2 border-black select-none",
                isSelected 
                  ? "w-24 h-24 bg-black text-white shadow-[4px_4px_0px_0px_var(--color-lime)] z-10 scale-105" 
                  : "w-20 h-20 bg-white text-black hover:bg-neutral-100 opacity-60 hover:opacity-100"
              )}
            >
              <div className={cn(
                "text-[10px] uppercase font-black tracking-wider mb-1",
                isSelected ? "text-lime" : "text-neutral-500"
              )}>
                {label}
              </div>
              <div className="text-3xl font-black leading-none">
                {format(date, 'dd')}
              </div>
              <div className="text-[10px] uppercase font-bold mt-1 opacity-60">
                {format(date, 'MMM')}
              </div>
            </motion.button>
          );
        })}
      </div>
      
      {/* Fade Gradients for visual cue of scrolling */}
      <div className="absolute top-0 bottom-0 left-0 w-8 bg-gradient-to-r from-white to-transparent pointer-events-none" />
      <div className="absolute top-0 bottom-0 right-0 w-8 bg-gradient-to-l from-white to-transparent pointer-events-none" />
    </div>
  );
}
