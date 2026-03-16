'use client';

import { useRef, useEffect } from 'react';
import { format, parseISO } from 'date-fns';

interface TitanScrubberProps {
  days: string[]; // ISO date strings e.g. "2026-02-23"
  activeDay: string;
  onChange: (day: string) => void;
}

export default function TitanScrubber({ days, activeDay, onChange }: TitanScrubberProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const itemRefs = useRef<(HTMLDivElement | null)[]>([]);

  // Find the most recent day (index 0)
  const isNowActive = activeDay === days[0];

  useEffect(() => {
    // Snap scroll to active item on mount or external activeDay change
    const activeIndex = days.indexOf(activeDay);
    if (activeIndex !== -1 && itemRefs.current[activeIndex] && scrollRef.current) {
      const el = itemRefs.current[activeIndex]!;
      const container = scrollRef.current;
      
      // Calculate scroll position to center the active element
      const containerHemi = container.offsetWidth / 2;
      const elHemi = el.offsetWidth / 2;
      const targetScroll = el.offsetLeft - containerHemi + elHemi;
      
      container.scrollTo({ left: targetScroll, behavior: 'smooth' });
    }
  }, [activeDay, days]);

  const handleScroll = () => {
    if (!scrollRef.current) return;
    
    // Find which item is closest to the center
    const container = scrollRef.current;
    const centerLine = container.scrollLeft + container.offsetWidth / 2;
    
    let closestIndex = 0;
    let minDistance = Infinity;

    itemRefs.current.forEach((el, index) => {
      if (!el) return;
      
      const elCenter = el.offsetLeft + el.offsetWidth / 2;
      const distance = Math.abs(centerLine - elCenter);
      
      if (distance < minDistance) {
        minDistance = distance;
        closestIndex = index;
      }
    });

    // If the closest item has changed, update the active state
    const newActiveDay = days[closestIndex];
    if (newActiveDay && newActiveDay !== activeDay) {
      onChange(newActiveDay);
    }
  };

  const handleDragEnd = () => {
    // Debounce or slightly delay the manual snap if CSS scroll-snap isn't enough, 
    // but CSS snap-mandatory usually handles the final resting place well.
  };

  return (
    <div className="relative w-full overflow-hidden select-none bg-black border-b border-[#222]">
      
      {/* Edge Fade Masks for the "Void" effect */}
      <div className="absolute inset-y-0 left-0 w-16 bg-gradient-to-r from-black to-transparent z-10 pointer-events-none" />
      <div className="absolute inset-y-0 right-0 w-16 bg-gradient-to-l from-black to-transparent z-10 pointer-events-none" />
      
      {/* Marker for "NOW" on the right edge if we are on the most recent day */}
      <div 
        className={`absolute inset-y-0 right-4 flex items-center z-20 pointer-events-none transition-opacity duration-300 ${
          isNowActive ? 'opacity-100' : 'opacity-0'
        }`}
      >
        <span className="text-brand-green font-black tracking-widest text-xs animate-pulse shadow-[0_0_10px_rgba(204,255,0,0.5)]">NOW</span>
      </div>

      <div 
        ref={scrollRef}
        onScroll={handleScroll}
        onPointerUp={handleDragEnd}
        className="flex overflow-x-auto snap-x snap-mandatory scrollbar-hide py-6 items-center w-full px-[50%]"
        style={{ scrollBehavior: 'smooth' }}
      >
        {/* We map days backwards so Yesterday (index 0) is on the right visually in LTR, 
            or keep them strict chronological? The blueprint says user scrubs backward. 
            So rightmost = NOW (days[0]), leftmost = 7 days ago. */}
        {days.slice().reverse().map((dayStr, reversedIdx) => {
          const actualIdx = days.length - 1 - reversedIdx;
          const isActive = dayStr === activeDay;
          let formatted = "";
          try {
            formatted = format(parseISO(dayStr), "MMM d").toUpperCase();
          } catch (e) {
            formatted = dayStr;
          }

          return (
            <div 
              key={dayStr}
              ref={el => { itemRefs.current[actualIdx] = el; }}
              className={`
                shrink-0 snap-center transition-all duration-300 ease-out px-8 text-center cursor-pointer
                ${isActive 
                  ? 'scale-100 opacity-100' 
                  : 'scale-50 opacity-30 hover:opacity-60'
                }
              `}
              onClick={() => onChange(dayStr)}
            >
              <h2 
                className={`
                  font-black uppercase tracking-tighter whitespace-nowrap transition-all duration-300
                  ${isActive ? 'text-6xl text-white drop-shadow-[0_4px_10px_rgba(255,255,255,0.2)]' : 'text-3xl text-gray-400'}
                `}
              >
                {formatted}
              </h2>
            </div>
          );
        })}
      </div>
    </div>
  );
}
