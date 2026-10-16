'use client';

import {
  Fragment,
  memo,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  useSyncExternalStore,
  type RefObject,
} from 'react';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { Download } from 'lucide-react';
import {
  BEAT_ARRIVE_S,
  BEAT_EASE,
  BEAT_EASE_CSS,
  BEAT_LEAVE_EASE,
  BEAT_LEAVE_S,
  BEAT_MAX_STEPS,
  beatDelay,
} from '@/lib/motion';
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

export type PostingCalendarExportControl = {
  open: boolean;
  isExporting: boolean;
  onOpen: () => void;
};

const DAYS_PER_WEEK = 7;
const DAY_LABELS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'] as const;

// Empty days rest slightly dimmed (the old entrance's resting opacity), set statically.
const EMPTY_CELL_OPACITY = 0.88;
const EMPTY_CELL_STYLE = { opacity: EMPTY_CELL_OPACITY };

// Tooltip / sheet: opacity plus one transform string (compositor), on the beat.
const FLOAT_HIDDEN = { opacity: 0, transform: 'translateY(15px) scale(0.95)' };
const FLOAT_SHOWN = { opacity: 1, transform: 'translateY(0px) scale(1)', transition: { duration: BEAT_ARRIVE_S, ease: BEAT_EASE } };
const FLOAT_GONE = { opacity: 0, transform: 'translateY(0px) scale(0.95)', transition: { duration: BEAT_LEAVE_S, ease: BEAT_LEAVE_EASE } };
const SHEET_HIDDEN = { opacity: 0, transform: 'translateY(20px)' };
const SHEET_SHOWN = { opacity: 1, transform: 'translateY(0px)', transition: { duration: BEAT_ARRIVE_S, ease: BEAT_EASE } };
const SHEET_GONE = { opacity: 0, transform: 'translateY(20px)', transition: { duration: BEAT_LEAVE_S, ease: BEAT_LEAVE_EASE } };

// The floating tooltip is the sm+ layout and the bottom sheet the phone layout. Only the one that's showing is
// mounted, so this mirrors Tailwind's `sm` breakpoint (40rem) in JS.
const SM_UP_QUERY = '(min-width: 40rem)';
let smUpQueryList: MediaQueryList | null = null;

function smUpMedia(): MediaQueryList {
  smUpQueryList ??= window.matchMedia(SM_UP_QUERY);
  return smUpQueryList;
}

function subscribeSmUp(onChange: () => void) {
  const media = smUpMedia();
  if (typeof media.addEventListener === 'function') {
    media.addEventListener('change', onChange);
    return () => media.removeEventListener('change', onChange);
  }
  media.addListener(onChange);
  return () => media.removeListener(onChange);
}

function getSmUp() {
  return smUpMedia().matches;
}

function getSmUpOnServer() {
  return false;
}

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
  if (level === 1) return 'bg-[#FFF1F2] border-[var(--fm-accent-soft)]/72 dark:bg-[var(--fm-accent)]/18 dark:border-[var(--fm-accent)]/26';
  if (level === 2) return 'bg-[#FFE4E6] border-[var(--fm-accent-bright)]/80 dark:bg-[var(--fm-accent)]/34 dark:border-[var(--fm-accent)]/44';
  if (level === 3) return 'bg-[var(--fm-accent-bright)]/80 border-[var(--fm-accent)]/70 dark:bg-[var(--fm-accent)]/62 dark:border-[var(--fm-accent)]/72';
  return 'bg-[var(--fm-accent)] border-[var(--fm-accent-deep)] dark:bg-[var(--fm-accent)] dark:border-[var(--fm-accent-bright)]/75';
}

function cellBevelClassName(level: number) {
  if (level > 0) {
    return 'shadow-[0_2px_8px_rgb(var(--fm-accent-rgb)/0.14),inset_0_1px_1px_rgba(255,255,255,0.42)] dark:shadow-[0_2px_8px_rgb(var(--fm-accent-rgb)/0.18),inset_0_1px_2px_rgba(255,255,255,0.3)]';
  }
  return 'shadow-[0_2px_4px_rgba(15,23,42,0.08),inset_0_1px_1px_rgba(255,255,255,0.28)] dark:shadow-[0_2px_4px_rgba(0,0,0,0.8),inset_0_1px_1px_rgba(255,255,255,0.1)]';
}

function reducedMotionPreferred(): boolean {
  return typeof window !== 'undefined' && window.matchMedia?.('(prefers-reduced-motion: reduce)').matches === true;
}

const CELL_DEAL_ID = 'fm-heatmap-cell';

/**
 * Cells rest in their final state. On arrival every cell settles in (a soft scale-up and fade) in a sweep across the
 * weeks on the page beat; afterwards only cells whose count changed re-deal. ONE WAAPI animation per cell on
 * opacity + transform — compositor-only, no framer component per cell, and nothing loops.
 */
function useCellDeal(gridRef: RefObject<HTMLElement | null>, trigger: unknown) {
  const countsRef = useRef<Map<string, string> | null>(null);

  useLayoutEffect(() => {
    const grid = gridRef.current;
    if (!grid) return;
    const cells = Array.from(grid.querySelectorAll<HTMLElement>('[data-cell]'));
    const previous = countsRef.current;
    const next = new Map<string, string>();
    for (const cell of cells) next.set(cell.dataset.cell ?? '', cell.dataset.cellCount ?? '');
    countsRef.current = next;
    if (reducedMotionPreferred()) return;

    for (const cell of cells) {
      const key = cell.dataset.cell ?? '';
      if (previous && previous.get(key) === next.get(key)) continue;
      for (const running of cell.getAnimations()) {
        if (running.id === CELL_DEAL_ID) running.cancel();
      }
      cell.animate(
        [
          { opacity: 0.24, transform: 'scale(0.82)' },
          { opacity: Number(cell.dataset.cellOpacity) || 1, transform: 'scale(1)' },
        ],
        {
          id: CELL_DEAL_ID,
          duration: BEAT_ARRIVE_S * 1000,
          delay: beatDelay(Number(cell.dataset.cellStep) || 0) * 1000,
          easing: BEAT_EASE_CSS,
          fill: 'backwards',
        },
      );
    }
  }, [gridRef, trigger]);
}

type HeatmapGridProps = {
  weeks: DayCell[][];
  onHover: (target: HTMLElement, dayData: DayCell) => void;
  onLeave: () => void;
  onToggle: (target: HTMLElement, dayData: DayCell) => void;
};

// The 91 cells only re-render when the data does — never for a tooltip change or a parent re-render.
const HeatmapGrid = memo(function HeatmapGrid({ weeks, onHover, onLeave, onToggle }: HeatmapGridProps) {
  const gridRef = useRef<HTMLDivElement>(null);
  const recentTouchAtRef = useRef(0);
  useCellDeal(gridRef, weeks);
  const lastCellIndex = Math.max(1, weeks.length * DAYS_PER_WEEK - 1);

  return (
    <div
      ref={gridRef}
      className="grid min-w-max items-center gap-x-1.5 gap-y-2 sm:gap-x-2 sm:gap-y-2"
      style={{ gridTemplateColumns: `30px repeat(${weeks.length}, minmax(54px, 1fr))` }}
    >
      <div />
      {weeks.map((week, wIndex) => {
        const monthStartDay = week.find((day) => day.dayOfMonth <= 7);
        const monthLabel = monthStartDay?.monthLabel ?? '';
        return (
          <div key={`month-${wIndex}`} className="flex h-[14px] items-center justify-center text-[10px] font-black uppercase tracking-[0.06em] text-foreground/60 drop-shadow-sm">
            {monthLabel}
          </div>
        );
      })}

      {DAY_LABELS.map((dayLabel, rowIndex) => (
        <Fragment key={dayLabel}>
          <div className="flex h-full items-center pr-1">
            <span className="text-[10px] font-black uppercase leading-[100%] tracking-[0.14em] text-foreground/42">
              {dayLabel}
            </span>
          </div>
          {weeks.map((week, wIndex) => {
            const dayData = week[rowIndex];
            const isEmpty = dayData.level === 0;
            // week by week, top to bottom: the same sweep the cells always arrived in, spread over the beat
            const order = wIndex * DAYS_PER_WEEK + rowIndex;

            return (
              <div key={dayData.iso} className="group/cell relative flex h-11 w-11 items-center justify-center justify-self-center sm:h-auto sm:aspect-square sm:w-full sm:max-w-[38px]">
                <button
                  data-cell={dayData.iso}
                  data-cell-count={dayData.count}
                  data-cell-step={(order / lastCellIndex) * BEAT_MAX_STEPS}
                  data-cell-opacity={isEmpty ? EMPTY_CELL_OPACITY : 1}
                  className={`block h-full w-full rounded-[10px] border border-black/10 transition-[scale] duration-200 ease-out hover:z-50 hover:scale-110 hover:rounded-[4px] active:scale-90 sm:rounded-[6px] dark:border-transparent ${levelClassName(dayData.level)} ${cellBevelClassName(dayData.level)}`}
                  style={isEmpty ? EMPTY_CELL_STYLE : undefined}
                  type="button"
                  aria-label={`${dayData.count} posts on ${dayData.date}`}
                  onPointerEnter={(e) => {
                    if (e.pointerType !== 'mouse') return;
                    onHover(e.currentTarget, dayData);
                  }}
                  onPointerLeave={(e) => {
                    if (e.pointerType !== 'mouse') return;
                    onLeave();
                  }}
                  onPointerUp={(e) => {
                    if (e.pointerType === 'mouse') return;
                    e.preventDefault();
                    recentTouchAtRef.current = Date.now();
                    onToggle(e.currentTarget, dayData);
                  }}
                  onClick={(e) => {
                    if (Date.now() - recentTouchAtRef.current < 650) return;
                    if (e.detail === 0) {
                      onToggle(e.currentTarget, dayData);
                      return;
                    }
                    onToggle(e.currentTarget, dayData);
                  }}
                />
              </div>
            );
          })}
        </Fragment>
      ))}
    </div>
  );
});

function PostingHeatmap({
  days,
  weeks,
  exportControl,
}: {
  days: HeatmapPoint[];
  weeks: number;
  exportControl?: PostingCalendarExportControl;
}) {
  const [tooltip, setTooltip] = useState<TooltipData | null>(null);
  const mounted = typeof document !== 'undefined';
  const isSmUp = useSyncExternalStore(subscribeSmUp, getSmUp, getSmUpOnServer);
  const rootRef = useRef<HTMLDivElement>(null);
  const mobileSheetRef = useRef<HTMLDivElement>(null);

  const showTooltip = useCallback((target: HTMLElement, dayData: DayCell) => {
    const rect = target.getBoundingClientRect();
    setTooltip({
      date: dayData.date,
      iso: dayData.iso,
      count: dayData.count,
      x: rect.left + rect.width / 2,
      y: rect.top,
    });
  }, []);

  const hideTooltip = useCallback(() => setTooltip(null), []);

  const toggleTooltip = useCallback((target: HTMLElement, dayData: DayCell) => {
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
  }, []);

  const heatmapData = useMemo(() => {
    const normalized = (days || []).map((d) => ({
      day_ist: isoKey(parseDayKey(d.day_ist)),
      post_count: Number(d.post_count) || 0,
    }));
    return generateHeatmapData(normalized, weeks).reverse();
  }, [days, weeks]);

  const openExport = () => {
    setTooltip(null);
    exportControl?.onOpen();
  };

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
          <div className="flex shrink-0 items-center gap-1.5">
            {exportControl ? (
              <button
                type="button"
                onClick={openExport}
                disabled={exportControl.isExporting}
                className="inline-flex h-7 shrink-0 items-center justify-center gap-1.5 rounded-full border border-[var(--fm-accent)]/24 bg-[var(--fm-accent)]/10 px-2.5 text-[8px] font-black uppercase tracking-[0.14em] text-[var(--fm-accent-deep)] shadow-[0_4px_10px_rgb(var(--fm-accent-rgb)/0.08)] outline-none transition hover:bg-[var(--fm-accent)]/14 focus-visible:ring-2 focus-visible:ring-[var(--fm-accent)]/24 active:scale-[0.96] disabled:cursor-not-allowed disabled:opacity-55 dark:border-[var(--fm-accent)]/28 dark:bg-[var(--fm-accent)]/16 dark:text-[var(--fm-accent-soft)]"
                aria-label="Export posting calendar workbook"
                aria-haspopup="dialog"
                aria-expanded={exportControl.open}
              >
                <Download size={12} strokeWidth={2.8} />
                <span>Export</span>
              </button>
            ) : null}
            <span className="rounded-full bg-[var(--fm-accent)] px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-white shadow-[0_4px_10px_rgb(var(--fm-accent-rgb)/0.2)]">
              Rolling 90D
            </span>
          </div>
        </div>

        <div className="min-w-0 w-full pt-2 pb-1">
          <div className="fm-depth-inner min-w-0 rounded-[14px] p-2.5 sm:p-3">
            <div className="-mx-1 overflow-x-auto overflow-y-hidden px-1 pb-2 overscroll-x-contain [scrollbar-width:none] [-webkit-overflow-scrolling:touch] [&::-webkit-scrollbar]:hidden sm:mx-0 sm:px-0 sm:pb-2">
              <HeatmapGrid weeks={heatmapData} onHover={showTooltip} onLeave={hideTooltip} onToggle={toggleTooltip} />
            </div>
          </div>
        </div>
      </div>

      {/* Floating tooltip (sm+) or bottom sheet (phone) — only the one in use is mounted. Rendered via portal to break
          out of the overflow-hidden parent; solid fills, no backdrop blur. */}
      {mounted && createPortal(
        <AnimatePresence>
          {tooltip && (isSmUp ? (
            <motion.div
              key="float"
              initial={FLOAT_HIDDEN}
              animate={FLOAT_SHOWN}
              exit={FLOAT_GONE}
              className="pointer-events-none fixed z-[99999] min-w-[160px] -translate-x-1/2 -translate-y-[calc(100%+12px)] rounded-[14px] border border-black/10 bg-white/[0.98] px-5 py-4 shadow-[0_16px_32px_rgba(0,0,0,0.15)] dark:border-white/10 dark:bg-[var(--fm-ink)]/[0.98] dark:shadow-[0_16px_32px_rgba(0,0,0,0.8)]"
              style={{ left: clampedTooltipX, top: tooltip.y }}
            >
              <div className="mb-2 text-[10px] font-black uppercase tracking-[0.14em] text-black/50 dark:text-white/40">{tooltip.date}</div>
              <div className="flex items-baseline gap-1.5">
                <span className="text-[22px] font-black leading-none text-black dark:text-white">{tooltip.count}</span>
                <span className="text-[12px] font-black uppercase tracking-[0.06em] text-black/58 dark:text-white/64">Posts</span>
              </div>
            </motion.div>
          ) : (
            <motion.div
              key="sheet"
              initial={SHEET_HIDDEN}
              animate={SHEET_SHOWN}
              exit={SHEET_GONE}
              ref={mobileSheetRef}
              className="fixed inset-x-3 bottom-[calc(88px+env(safe-area-inset-bottom))] z-[99999] rounded-[18px] border border-black/10 bg-white/[0.985] px-4 py-3 shadow-[0_18px_40px_rgba(0,0,0,0.18)] dark:border-white/10 dark:bg-[var(--fm-ink)]/[0.985] dark:shadow-[0_18px_40px_rgba(0,0,0,0.82)]"
            >
              <div className="flex items-start justify-between gap-3">
                <div>
                  <div className="text-[10px] font-black uppercase tracking-[0.14em] text-black/46 dark:text-white/38">{tooltip.date}</div>
                  <div className="mt-2 flex items-end gap-3">
                    <div className="text-[28px] font-black leading-none tracking-[-0.04em] text-black dark:text-white">
                      {tooltip.count}
                    </div>
                    <div className="pb-0.5 text-[12px] font-black uppercase tracking-[0.14em] text-black/58 dark:text-white/64">
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
          ))}
        </AnimatePresence>,
        document.body
      )}

    </div>
  );
}

const MemoizedPostingHeatmap = memo(PostingHeatmap);
MemoizedPostingHeatmap.displayName = 'PostingHeatmap';

export default MemoizedPostingHeatmap;
