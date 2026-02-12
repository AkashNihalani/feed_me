'use client';

import * as React from 'react';
import { ChevronLeft, ChevronRight, X } from 'lucide-react';
import { 
  format, 
  addMonths, 
  subMonths, 
  startOfMonth, 
  endOfMonth, 
  eachDayOfInterval, 
  isSameMonth, 
  isSameDay, 
  isToday,
  startOfWeek,
  endOfWeek,
  isWithinInterval
} from 'date-fns';
import { cn } from '@/lib/utils';

export type DateRange = {
  from: Date | null;
  to: Date | null;
};

interface BrutalistCalendarProps {
  isOpen: boolean;
  onClose: () => void;
  selectedRange: DateRange;
  onSelect: (range: DateRange) => void;
}

export function BrutalistCalendar({ isOpen, onClose, selectedRange, onSelect }: BrutalistCalendarProps) {
  const [currentMonth, setCurrentMonth] = React.useState(new Date());
  
  // Reset view when opened
  React.useEffect(() => {
    if (isOpen && selectedRange.from) {
      setCurrentMonth(selectedRange.from);
    } else {
        setCurrentMonth(new Date());
    }
  }, [isOpen]);

  if (!isOpen) return null;

  const daysInMonth = eachDayOfInterval({
    start: startOfWeek(startOfMonth(currentMonth)),
    end: endOfWeek(endOfMonth(currentMonth))
  });

  const handleDayClick = (day: Date) => {
    if (!selectedRange.from || (selectedRange.from && selectedRange.to)) {
      // Start new range
      onSelect({ from: day, to: null });
    } else {
      // Complete range
      // Ensure from is before to
      if (day < selectedRange.from) {
        onSelect({ from: day, to: selectedRange.from });
      } else {
        onSelect({ from: selectedRange.from, to: day });
      }
    }
  };

  const isSelected = (day: Date) => {
    if (selectedRange.from && isSameDay(day, selectedRange.from)) return true;
    if (selectedRange.to && isSameDay(day, selectedRange.to)) return true;
    return false;
  };

  const isInRange = (day: Date) => {
    if (selectedRange.from && selectedRange.to) {
      return isWithinInterval(day, { start: selectedRange.from, end: selectedRange.to });
    }
    return false;
  };

  return (
    <div className="absolute top-full left-0 mt-2 z-50 animate-enter">
      <div className="bg-white dark:bg-black border-4 border-black dark:border-white p-4 shadow-hard min-w-[320px]">
        {/* Header */}
        <div className="flex items-center justify-between mb-4">
          <button 
            onClick={() => setCurrentMonth(subMonths(currentMonth, 1))}
            className="p-1 hover:bg-black hover:text-white dark:hover:bg-white dark:hover:text-black border-2 border-transparent hover:border-black dark:hover:border-white transition-colors"
          >
            <ChevronLeft size={20} strokeWidth={3} />
          </button>
          
          <h4 className="text-lg font-black uppercase tracking-tighter">
            {format(currentMonth, 'MMMM yyyy')}
          </h4>

          <div className="flex gap-2">
            <button 
                onClick={() => setCurrentMonth(addMonths(currentMonth, 1))}
                className="p-1 hover:bg-black hover:text-white dark:hover:bg-white dark:hover:text-black border-2 border-transparent hover:border-black dark:hover:border-white transition-colors"
            >
                <ChevronRight size={20} strokeWidth={3} />
            </button>
            <button 
                onClick={onClose}
                className="p-1 hover:bg-red-500 hover:text-white border-2 border-transparent hover:border-black transition-colors ml-2"
            >
                <X size={20} strokeWidth={3} />
            </button>
          </div>
        </div>

        {/* Days Header */}
        <div className="grid grid-cols-7 mb-2 border-b-2 border-black dark:border-white pb-2">
          {['Su', 'Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa'].map(day => (
            <div key={day} className="text-center text-xs font-black uppercase text-gray-400">
              {day}
            </div>
          ))}
        </div>

        {/* Calendar Grid */}
        <div className="grid grid-cols-7 gap-1">
          {daysInMonth.map((day, idx) => {
            const isSelectedDay = isSelected(day);
            const isInRangeDay = isInRange(day);
            const isCurrentMonth = isSameMonth(day, currentMonth);

            return (
              <button
                key={idx}
                onClick={() => handleDayClick(day)}
                className={cn(
                  "h-14 text-lg font-black flex items-center justify-center transition-all border-2 border-transparent relative",
                  !isCurrentMonth && "opacity-25",
                  
                  // Hover effects
                  "hover:border-black dark:hover:border-white",

                  // Selection Styles
                  isSelectedDay ? "bg-black text-white dark:bg-white dark:text-black shadow-[4px_4px_0px_0px_#000] dark:shadow-[4px_4px_0px_0px_#fff] z-10 scale-110" : 
                  isInRangeDay ? "bg-lime/30 text-black dark:bg-lime/20 dark:text-white" : 
                  "hover:bg-gray-100 dark:hover:bg-gray-800"
                )}
              >
                {format(day, 'd')}
              </button>
            );
          })}
        </div>

        {/* Footer info selected */}
        <div className="mt-4 pt-4 border-t-2 border-black dark:border-white text-xs font-mono font-bold text-center">
            {selectedRange.from ? format(selectedRange.from, 'MMM d') : '...'} 
            {' - '}
            {selectedRange.to ? format(selectedRange.to, 'MMM d') : '...'}
        </div>
        
        {selectedRange.from && selectedRange.to && (
             <button 
                onClick={onClose}
                className="w-full mt-2 bg-lime text-black border-2 border-black font-black uppercase py-2 hover:translate-y-px hover:shadow-none shadow-[2px_2px_0px_0px_#000] transition-all"
            >
                Confirm Range
            </button>
        )}
      </div>
    </div>
  );
}
