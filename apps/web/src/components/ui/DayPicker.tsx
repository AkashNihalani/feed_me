'use client';

import { format, isSameDay } from 'date-fns';
import { cn } from '@/lib/utils';

interface DayCounts {
  spark: number;
  burn: number;
  blaze: number;
}

interface DayPickerProps {
  days: Date[];
  activeDate: Date;
  onSelect: (date: Date) => void;
  dayCounts?: Record<string, DayCounts>;
}

export function DayPicker({ days, activeDate, onSelect, dayCounts }: DayPickerProps) {
  return (
    <div className="w-full bg-black py-2 px-1">
      <div className="grid gap-px" style={{ gridTemplateColumns: `repeat(${days.length}, 1fr)` }}>
        {days.map((date, idx) => {
          const isSelected = isSameDay(date, activeDate);
          const dateKey = format(date, 'yyyy-MM-dd');
          const counts = dayCounts?.[dateKey];
          const total = counts ? counts.spark + counts.burn + counts.blaze : 0;

          // Heat bar segment percentages
          const sparkPct = total > 0 ? (counts!.spark / total) * 100 : 0;
          const burnPct = total > 0 ? (counts!.burn / total) * 100 : 0;
          const blazePct = total > 0 ? (counts!.blaze / total) * 100 : 0;

          return (
            <button
              key={date.toISOString()}
              onClick={() => onSelect(date)}
              className={cn(
                'relative flex flex-col items-center justify-center py-2 px-1 transition-all duration-200 cursor-pointer group',
                isSelected
                  ? '-translate-y-1 z-10 bg-neutral-900 shadow-[0_6px_20px_rgba(204,255,0,0.15),0_2px_8px_rgba(0,0,0,0.6)] rounded-sm'
                  : 'hover:bg-white/[0.03]',
                // Divider on left side (skip first)
                idx > 0 && !isSelected && 'border-l border-neutral-800'
              )}
            >
              {/* Symmetrical accent bars when selected */}
              {isSelected && (
                <>
                  <div className="absolute left-0 top-1 bottom-1 w-[3px] bg-[#CCFF00] rounded-r-full shadow-[0_0_6px_rgba(204,255,0,0.4)]" />
                  <div className="absolute right-0 top-1 bottom-1 w-[3px] bg-[#CCFF00] rounded-l-full shadow-[0_0_6px_rgba(204,255,0,0.4)]" />
                </>
              )}

              {/* Day of week */}
              <span
                className={cn(
                  'font-mono text-[10px] uppercase tracking-wider leading-none mb-1',
                  isSelected ? 'text-[#CCFF00]' : 'text-neutral-600 group-hover:text-neutral-400'
                )}
              >
                {format(date, 'EEE')}
              </span>

              {/* Date number — hero */}
              <span
                className={cn(
                  'text-2xl font-black leading-none tracking-tighter transition-all duration-200',
                  isSelected ? 'text-white scale-110' : 'text-neutral-500 group-hover:text-neutral-300'
                )}
              >
                {format(date, 'dd')}
              </span>

              {/* Month */}
              <span
                className={cn(
                  'font-mono text-[10px] uppercase tracking-wider leading-none mt-1',
                  isSelected ? 'text-[#CCFF00]' : 'text-neutral-600 group-hover:text-neutral-400'
                )}
              >
                {format(date, 'MMM')}
              </span>

              {/* Heat bar */}
              <div className={cn(
                'w-full h-1 mt-2 flex overflow-hidden rounded-sm',
                isSelected && 'h-1.5'
              )}>
                {total > 0 ? (
                  <>
                    {sparkPct > 0 && (
                      <div className="h-full bg-[#CCFF00]" style={{ width: `${sparkPct}%` }} />
                    )}
                    {burnPct > 0 && (
                      <div className="h-full bg-[#FF6B00]" style={{ width: `${burnPct}%` }} />
                    )}
                    {blazePct > 0 && (
                      <div className="h-full bg-[#FF003C]" style={{ width: `${blazePct}%` }} />
                    )}
                  </>
                ) : (
                  <div className="h-full w-full bg-neutral-800" />
                )}
              </div>
            </button>
          );
        })}
      </div>
    </div>
  );
}
