'use client';

import { useMemo, useState, useEffect } from 'react';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { HeatmapPoint } from './dashboardTypes';

interface TooltipData {
  date: string;
  count: number;
  x: number;
  y: number;
}

interface DayCell {
  date: string;
  iso: string;
  monthLabel: string;
  dayOfMonth: number;
  count: number;
  level: number;
}

const DAYS_PER_WEEK = 7;

function formatDate(date: Date) {
  return new Intl.DateTimeFormat('en-US', {
    timeZone: 'UTC',
    month: 'short',
    day: 'numeric',
    year: 'numeric',
  }).format(date);
}

function isoKey(date: Date) {
  const y = date.getUTCFullYear();
  const m = String(date.getUTCMonth() + 1).padStart(2, '0');
  const d = String(date.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${d}`;
}

function parseDayKey(day: string): Date {
  const [y, m, d] = day.split('-').map((n) => Number(n));
  return new Date(Date.UTC(y, m - 1, d));
}

function addUtcDays(date: Date, days: number): Date {
  const d = new Date(date);
  d.setUTCDate(d.getUTCDate() + days);
  return d;
}

function startOfWeek(date: Date) {
  const d = new Date(date);
  const day = d.getUTCDay();
  const diff = day === 0 ? -6 : 1 - day;
  d.setUTCDate(d.getUTCDate() + diff);
  d.setUTCHours(0, 0, 0, 0);
  return d;
}

function generateHeatmapData(days: HeatmapPoint[], weeks: number): DayCell[][] {
  const data: DayCell[][] = [];
  const byDate = new Map<string, number>(days.map((d) => [d.day_ist, Number(d.post_count) || 0]));
  const firstTrackedDay = days.length > 0 ? startOfWeek(parseDayKey(days[0].day_ist)) : null;
  const now = new Date();
  const todayUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const currentWeekStart = startOfWeek(todayUtc);
  const totalWeeks = Math.max(1, weeks);
  const nominalWindowStart = addUtcDays(currentWeekStart, -((totalWeeks - 1) * 7));
  const windowStart =
    firstTrackedDay && firstTrackedDay.getTime() > nominalWindowStart.getTime()
      ? firstTrackedDay
      : nominalWindowStart;
  for (let w = 0; w < totalWeeks; w++) {
    const week: DayCell[] = [];
    for (let d = 0; d < DAYS_PER_WEEK; d++) {
      const date = addUtcDays(windowStart, w * 7 + d);
      const count = byDate.get(isoKey(date)) ?? 0;
      let level = 0;
      if (count === 1) level = 1;
      else if (count === 2) level = 2;
      else if (count >= 3 && count < 5) level = 3;
      else if (count >= 5) level = 4;
      week.push({
        date: formatDate(date),
        iso: isoKey(date),
        monthLabel: new Intl.DateTimeFormat('en-US', { timeZone: 'UTC', month: 'short' }).format(date),
        dayOfMonth: date.getUTCDate(),
        count,
        level,
      });
    }
    data.push(week);
  }
  return data;
}

function levelClassName(level: number) {
  if (level === 0) return 'bg-black/14 dark:bg-white/10 border-black/18 dark:border-white/18';
  if (level === 1) return 'bg-[#CCFF00]/15 border-[#CCFF00]/20';
  if (level === 2) return 'bg-[#CCFF00]/35 border-[#CCFF00]/40';
  if (level === 3) return 'bg-[#CCFF00]/60 border-[#CCFF00]/70';
  return 'bg-[#CCFF00] border-[#CCFF00]';
}

export default function PostingHeatmap({ days, weeks }: { days: HeatmapPoint[]; weeks: number }) {
  const [tooltip, setTooltip] = useState<TooltipData | null>(null);
  const [mounted, setMounted] = useState(false);

  useEffect(() => {
    setMounted(true);
  }, []);

  const handleMouseEnter = (e: React.MouseEvent | React.TouchEvent, dayData: DayCell) => {
    const rect = (e.currentTarget as HTMLElement).getBoundingClientRect();
    // Portal tooltips need coordinates relative to window viewport
    setTooltip({
      date: dayData.date,
      count: dayData.count,
      x: rect.left + rect.width / 2,
      y: rect.top,
    });
  };

  const heatmapData = useMemo(() => {
    const normalized = (days || []).map((d) => ({
      day_ist: isoKey(parseDayKey(d.day_ist)),
      post_count: Number(d.post_count) || 0,
    }));
    return generateHeatmapData(normalized, weeks);
  }, [days, weeks]);

  return (
    <div className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-4">

      <div className="relative z-10 flex h-full justify-between flex-col">
        <span className="text-[10px] sm:text-[11px] font-black uppercase tracking-[0.14em] text-black dark:text-white/50 mb-1 fm-depth-title">Posting Pattern</span>
        
        {/* The Grid Outer Box */}
        <div className="flex w-full overflow-x-auto hide-scrollbar pt-1 pb-1">
          <div className="flex flex-1 justify-between gap-1 min-w-max fm-depth-inner rounded-[12px] p-2 sm:p-2.5">
            {/* Days Y-Axis */}
            <div className="flex flex-col justify-between pr-1 border-r border-black/5 dark:border-white/5 pt-[14px] sm:pt-[16px]">
              {['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'].map((day) => (
                <div key={day} className="flex flex-1 items-center">
                  <span className="text-[8px] sm:text-[9px] font-black uppercase leading-[100%] tracking-[0.1em] text-foreground/42">
                    {day}
                  </span>
                </div>
              ))}
            </div>

            {/* Matrix Columns (Weeks) */}
            <div className="flex flex-1 justify-between gap-1 sm:gap-1.5 pl-1 h-full">
              {heatmapData.map((week, wIndex) => {
                // Month label for the first week of a month
                let monthLabel = '';
                if (week[0] && week[0].dayOfMonth <= 7) monthLabel = week[0].monthLabel;

                return (
                  <div key={wIndex} className="flex flex-col flex-1 justify-between gap-1 sm:gap-1.5 min-w-[16px]">
                    <div className="flex h-[12px] sm:h-[14px] items-center text-[8px] sm:text-[9px] font-black uppercase tracking-[0.05em] text-foreground/60 drop-shadow-sm self-center">
                      {monthLabel}
                    </div>
                    {week.map((dayData, dIndex) => {
                      const isPeak = dayData.level === 4;
                      // Base deep bevel styling for cells
                      let cellBevel = 'shadow-[0_2px_4px_rgba(0,0,0,0.1),inset_0_1px_1px_rgba(255,255,255,0.3)] dark:shadow-[0_2px_4px_rgba(0,0,0,0.8),inset_0_1px_1px_rgba(255,255,255,0.1)]';
                      if (dayData.level > 0) {
                         cellBevel = 'shadow-[0_2px_6px_rgba(204,255,0,0.2),inset_0_1px_1px_rgba(255,255,255,0.6)] dark:shadow-[0_2px_8px_rgba(204,255,0,0.15),inset_0_1px_2px_rgba(255,255,255,0.3)]';
                      }

                      return (
                        <div key={dIndex} className="relative flex w-full aspect-square max-w-[32px] mx-auto items-center justify-center group/cell">
                          <motion.button
                            whileHover={{ scale: 1.2, zIndex: 50, borderRadius: '4px' }}
                            whileTap={{ scale: 0.90 }}
                            animate={isPeak ? { filter: ['brightness(1)', 'brightness(1.4)', 'brightness(1)'] } : undefined}
                            transition={isPeak ? { duration: 2.2, repeat: Infinity, ease: 'easeInOut' } : undefined}
                            className={`block h-full w-full rounded-[3px] sm:rounded-[4px] border border-black/10 dark:border-transparent ${levelClassName(dayData.level)} ${cellBevel}`}
                            onMouseEnter={(e) => handleMouseEnter(e, dayData)}
                            onMouseLeave={() => setTooltip(null)}
                            onTouchStart={(e) => handleMouseEnter(e, dayData)}
                          />
                        </div>
                      );
                    })}
                  </div>
                );
              })}
            </div>
          </div>
        </div>
      </div>

      {/* Dynamic Floating Glass Tooltip - Rendered via Portal to break out of overflow-hidden parent */}
      {mounted && createPortal(
        <AnimatePresence>
          {tooltip && (
            <motion.div
              initial={{ opacity: 0, y: 15, scale: 0.95 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, scale: 0.95 }}
              className="pointer-events-none fixed z-[99999] -translate-x-1/2 -translate-y-[calc(100%+12px)] rounded-[12px] border border-black/10 bg-white/95 px-5 py-4 shadow-[0_16px_32px_rgba(0,0,0,0.15)] backdrop-blur-2xl dark:border-white/10 dark:bg-[#0A0A0A]/95 dark:shadow-[0_16px_32px_rgba(0,0,0,0.8)] min-w-[160px]"
              style={{ left: tooltip.x, top: tooltip.y }}
            >
              <div className="text-[10px] font-black uppercase tracking-[0.14em] text-black/50 dark:text-white/40 mb-2">{tooltip.date}</div>
              <div className="flex items-baseline gap-1.5">
                <span className="text-[24px] font-black leading-none text-black dark:text-white">{tooltip.count}</span>
                <span className="text-[12px] font-black uppercase tracking-[0.05em] text-[#CCFF00]">Posts</span>
              </div>
            </motion.div>
          )}
        </AnimatePresence>,
        document.body
      )}
    </div>
  );
}
