'use client';

import { Fragment, useEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { HeatmapPoint } from './dashboardTypes';

interface TooltipData {
  date: string;
  iso: string;
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
const DAY_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'] as const;

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

function clamp(value: number, min: number, max: number) {
  return Math.min(Math.max(value, min), max);
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
  const now = new Date();
  const todayUtc = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate()));
  const currentWeekStart = startOfWeek(todayUtc);
  const totalWeeks = Math.max(1, weeks);
  const windowStart = addUtcDays(currentWeekStart, -((totalWeeks - 1) * 7));
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
  if (level === 0) return 'bg-black/[0.055] border-black/[0.09] dark:bg-white/10 dark:border-white/18';
  if (level === 1) return 'bg-[#FFF1F2] border-[#FDA4AF]/72 dark:bg-[#E11D48]/18 dark:border-[#E11D48]/26';
  if (level === 2) return 'bg-[#FFE4E6] border-[#FB7185]/80 dark:bg-[#E11D48]/34 dark:border-[#E11D48]/44';
  if (level === 3) return 'bg-[#FB7185]/80 border-[#E11D48]/70 dark:bg-[#E11D48]/62 dark:border-[#E11D48]/72';
  return 'bg-[#E11D48] border-[#BE123C] dark:bg-[#E11D48] dark:border-[#FB7185]/75';
}

export default function PostingHeatmap({ days, weeks }: { days: HeatmapPoint[]; weeks: number }) {
  const [tooltip, setTooltip] = useState<TooltipData | null>(null);
  const mounted = typeof document !== 'undefined';
  const rootRef = useRef<HTMLDivElement>(null);
  const mobileSheetRef = useRef<HTMLDivElement>(null);
  const recentTouchAtRef = useRef(0);

  const showTooltip = (target: HTMLElement, dayData: DayCell) => {
    const rect = target.getBoundingClientRect();
    setTooltip({
      date: dayData.date,
      iso: dayData.iso,
      count: dayData.count,
      x: rect.left + rect.width / 2,
      y: rect.top,
    });
  };

  const toggleTooltip = (target: HTMLElement, dayData: DayCell) => {
    const rect = target.getBoundingClientRect();
    setTooltip((current) => {
      if (current?.iso === dayData.iso) return null;
      return {
        date: dayData.date,
        iso: dayData.iso,
        count: dayData.count,
        x: rect.left + rect.width / 2,
        y: rect.top,
      };
    });
  };

  const heatmapData = useMemo(() => {
    const normalized = (days || []).map((d) => ({
      day_ist: isoKey(parseDayKey(d.day_ist)),
      post_count: Number(d.post_count) || 0,
    }));
    return generateHeatmapData(normalized, weeks).reverse();
  }, [days, weeks]);

  const clampedTooltipX = useMemo(() => {
    if (!tooltip || typeof window === 'undefined') return tooltip?.x ?? 0;
    return clamp(tooltip.x, 110, window.innerWidth - 110);
  }, [tooltip]);

  useEffect(() => {
    if (!tooltip || !mounted) return undefined;

    const handlePointerDown = (event: PointerEvent) => {
      const target = event.target as Node | null;
      if (!target) return;
      if (rootRef.current?.contains(target)) return;
      if (mobileSheetRef.current?.contains(target)) return;
      setTooltip(null);
    };

    const handleEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setTooltip(null);
    };

    document.addEventListener('pointerdown', handlePointerDown);
    document.addEventListener('keydown', handleEscape);
    return () => {
      document.removeEventListener('pointerdown', handlePointerDown);
      document.removeEventListener('keydown', handleEscape);
    };
  }, [mounted, tooltip]);

  return (
    <div ref={rootRef} className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3 sm:p-3.5 lg:p-4">

      <div className="relative z-10 flex h-full justify-between flex-col">
        <div className="mb-1 flex items-center justify-between gap-3">
          <span className="fm-label fm-depth-title">Posting Calendar</span>
          <span className="rounded-full bg-[#E11D48] px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.12em] text-white shadow-[0_4px_10px_rgba(225,29,72,0.2)]">
            Rolling 90D
          </span>
        </div>
        
        <div className="min-w-0 w-full pt-2 pb-1">
          <div className="fm-depth-inner min-w-0 rounded-[14px] p-2.5 sm:p-3">
            <div className="-mx-1 overflow-x-auto overflow-y-hidden px-1 pb-2 overscroll-x-contain [scrollbar-width:none] [-webkit-overflow-scrolling:touch] [&::-webkit-scrollbar]:hidden sm:mx-0 sm:overflow-visible sm:px-0 sm:pb-0">
            <div
              className="grid min-w-max items-center gap-x-1.5 gap-y-2 sm:min-w-0 sm:gap-x-2 sm:gap-y-2"
              style={{ gridTemplateColumns: `30px repeat(${heatmapData.length}, minmax(54px, 1fr))` }}
            >
              <div />
              {heatmapData.map((week, wIndex) => {
                const monthStartDay = week.find((day) => day.dayOfMonth <= 7);
                const monthLabel = monthStartDay?.monthLabel ?? '';
                return (
                  <div key={`month-${wIndex}`} className="flex h-[14px] items-center justify-center text-[9px] font-black uppercase tracking-[0.05em] text-foreground/60 drop-shadow-sm">
                    {monthLabel}
                  </div>
                );
              })}

              {DAY_LABELS.map((dayLabel, rowIndex) => (
                <Fragment key={dayLabel}>
                  <div className="flex h-full items-center pr-1">
                    <span className="text-[9px] font-black uppercase leading-[100%] tracking-[0.1em] text-foreground/42">
                      {dayLabel}
                    </span>
                  </div>
                  {heatmapData.map((week, wIndex) => {
                    const dayData = week[rowIndex];
                    const isPeak = dayData.level === 4;
                    let cellBevel = 'shadow-[0_2px_4px_rgba(15,23,42,0.08),inset_0_1px_1px_rgba(255,255,255,0.28)] dark:shadow-[0_2px_4px_rgba(0,0,0,0.8),inset_0_1px_1px_rgba(255,255,255,0.1)]';
                    if (dayData.level > 0) {
                      cellBevel = 'shadow-[0_2px_8px_rgba(225,29,72,0.14),inset_0_1px_1px_rgba(255,255,255,0.42)] dark:shadow-[0_2px_8px_rgba(225,29,72,0.18),inset_0_1px_2px_rgba(255,255,255,0.3)]';
                    }

                    return (
                      <div key={`${dayData.iso}-${dayData.count}`} className="group/cell relative flex h-11 w-11 items-center justify-center justify-self-center sm:h-auto sm:aspect-square sm:w-full sm:max-w-[38px]">
                        <motion.button
                          initial={{ opacity: 0.24, scale: 0.82, filter: 'brightness(0.72)' }}
                          animate={isPeak
                            ? { opacity: [0.94, 1, 0.94], scale: [1, 1.03, 1], filter: ['brightness(0.94)', 'brightness(1.08)', 'brightness(0.94)'] }
                            : { opacity: dayData.level > 0 ? 1 : 0.88, scale: 1, filter: dayData.level > 0 ? 'brightness(1)' : 'brightness(0.96)' }}
                          transition={isPeak
                            ? { duration: 3, repeat: Infinity, ease: 'easeInOut', delay: (wIndex * DAYS_PER_WEEK + rowIndex) * 0.01 }
                            : { duration: 0.68, ease: [0.22, 1, 0.36, 1], delay: (wIndex * DAYS_PER_WEEK + rowIndex) * 0.004 }}
                          whileHover={{ scale: 1.1, zIndex: 50, borderRadius: '4px' }}
                          whileTap={{ scale: 0.9 }}
                          className={`block h-full w-full rounded-[8px] border border-black/10 sm:rounded-[4px] dark:border-transparent ${levelClassName(dayData.level)} ${cellBevel}`}
                          type="button"
                          aria-label={`${dayData.count} posts on ${dayData.date}`}
                          onPointerEnter={(e) => {
                            if (e.pointerType !== 'mouse') return;
                            showTooltip(e.currentTarget, dayData);
                          }}
                          onPointerLeave={(e) => {
                            if (e.pointerType !== 'mouse') return;
                            setTooltip(null);
                          }}
                          onPointerUp={(e) => {
                            if (e.pointerType === 'mouse') return;
                            e.preventDefault();
                            recentTouchAtRef.current = Date.now();
                            toggleTooltip(e.currentTarget, dayData);
                          }}
                          onClick={(e) => {
                            if (Date.now() - recentTouchAtRef.current < 650) return;
                            if (e.detail === 0) {
                              toggleTooltip(e.currentTarget, dayData);
                              return;
                            }
                            toggleTooltip(e.currentTarget, dayData);
                          }}
                        />
                      </div>
                    );
                  })}
                </Fragment>
              ))}
            </div>
            </div>
          </div>
        </div>
      </div>

      {/* Dynamic Floating Glass Tooltip - Rendered via Portal to break out of overflow-hidden parent */}
      {mounted && createPortal(
        <AnimatePresence>
          {tooltip && (
            <>
              <motion.div
                initial={{ opacity: 0, y: 15, scale: 0.95 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, scale: 0.95 }}
                className="pointer-events-none fixed z-[99999] hidden min-w-[160px] -translate-x-1/2 -translate-y-[calc(100%+12px)] rounded-[12px] border border-black/10 bg-white/95 px-5 py-4 shadow-[0_16px_32px_rgba(0,0,0,0.15)] backdrop-blur-2xl dark:border-white/10 dark:bg-[#0A0A0A]/95 dark:shadow-[0_16px_32px_rgba(0,0,0,0.8)] sm:block"
                style={{ left: clampedTooltipX, top: tooltip.y }}
              >
                <div className="mb-2 text-[10px] font-black uppercase tracking-[0.14em] text-black/50 dark:text-white/40">{tooltip.date}</div>
                <div className="flex items-baseline gap-1.5">
                  <span className="text-[24px] font-black leading-none text-black dark:text-white">{tooltip.count}</span>
                  <span className="text-[12px] font-black uppercase tracking-[0.05em] text-black/58 dark:text-white/64">Posts</span>
                </div>
              </motion.div>

              <motion.div
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: 20 }}
                ref={mobileSheetRef}
                className="fixed inset-x-3 bottom-[calc(88px+env(safe-area-inset-bottom))] z-[99999] rounded-[18px] border border-black/10 bg-white/96 px-4 py-3 shadow-[0_18px_40px_rgba(0,0,0,0.18)] backdrop-blur-2xl dark:border-white/10 dark:bg-[#0A0A0A]/96 dark:shadow-[0_18px_40px_rgba(0,0,0,0.82)] sm:hidden"
              >
                <div className="flex items-start justify-between gap-3">
                  <div>
                    <div className="text-[9px] font-black uppercase tracking-[0.16em] text-black/46 dark:text-white/38">{tooltip.date}</div>
                    <div className="mt-2 flex items-end gap-3">
                      <div className="text-[26px] font-black leading-none tracking-[-0.04em] text-black dark:text-white">
                        {tooltip.count}
                      </div>
                      <div className="pb-0.5 text-[11px] font-black uppercase tracking-[0.14em] text-black/58 dark:text-white/64">
                        Posts Logged
                      </div>
                    </div>
                  </div>
                  <button
                    type="button"
                    onClick={() => setTooltip(null)}
                    className="inline-flex h-8 w-8 items-center justify-center rounded-full border border-black/10 bg-black/[0.04] text-[14px] font-black text-black/60 transition hover:bg-black/[0.07] dark:border-white/10 dark:bg-white/[0.05] dark:text-white/60 dark:hover:bg-white/[0.08]"
                    aria-label="Close posting calendar details"
                  >
                    ×
                  </button>
                </div>
              </motion.div>
            </>
          )}
        </AnimatePresence>,
        document.body
      )}
    </div>
  );
}
