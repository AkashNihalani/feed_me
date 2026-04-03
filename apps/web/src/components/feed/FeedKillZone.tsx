'use client';

import { KillzonePoint } from './dashboardTypes';

function hourLabel(hour: number): string {
  const normalized = ((hour % 24) + 24) % 24;
  const suffix = normalized >= 12 ? 'PM' : 'AM';
  const twelve = normalized % 12 === 0 ? 12 : normalized % 12;
  return `${twelve} ${suffix}`;
}

export default function FeedKillZone({ hours }: { hours: KillzonePoint[] }) {
  const byHour = new Map<number, number>();
  for (let h = 0; h < 24; h++) byHour.set(h, 0);
  for (const row of hours || []) {
    const hour = Number(row.hour_ist);
    if (!Number.isFinite(hour) || hour < 0 || hour > 23) continue;
    byHour.set(hour, Number(row.post_count) || 0);
  }

  const totalPosts = Array.from(byHour.values()).reduce((s, v) => s + v, 0);
  const hasData = totalPosts > 0;

  let peakStart = 18;
  let peakCount = 0;
  if (hasData) {
    let bestCount = -1;
    for (let h = 0; h < 24; h++) {
      const score = (byHour.get(h) || 0) + (byHour.get((h + 1) % 24) || 0);
      if (score > bestCount) {
        bestCount = score;
        peakStart = h;
        peakCount = score;
      }
    }
  }
  const peakEnd = (peakStart + 2) % 24;
  const arcShare = hasData ? Math.max(8, Math.round((peakCount / totalPosts) * 100)) : null;
  const maxCount = hasData ? Math.max(1, ...Array.from(byHour.values())) : 0;

  // 24 hour bars layout
  const hourKeys = Array.from({ length: 24 }, (_, i) => i);

  return (
    <div className="fm-depth-glass relative flex h-full w-full flex-col overflow-hidden rounded-[22px] p-3.5 sm:p-4 lg:p-5">
      <div className="relative z-10 flex h-full flex-col">
        <div className="mb-3 flex items-center justify-between">
          <span className="fm-label fm-depth-title">Kill Zone</span>
          <span className="fm-depth-chip rounded-full px-2 py-0.5 text-[8px] font-black uppercase tracking-[0.14em] text-foreground/40 dark:text-white/40">
            Best window
          </span>
        </div>

        {/* Prime time badge */}
        <div className="mb-3 flex items-center gap-3">
          <div className="flex items-baseline gap-1.5">
            <span className="text-[22px] sm:text-[26px] font-black leading-none tracking-[-0.04em] text-foreground dark:text-white fm-depth-title">
              {hasData ? hourLabel(peakStart) : '--'}
            </span>
            <span className="text-[11px] font-black uppercase tracking-[0.1em] text-foreground/30 dark:text-white/30">to</span>
            <span className="text-[22px] sm:text-[26px] font-black leading-none tracking-[-0.04em] text-foreground dark:text-white fm-depth-title">
              {hasData ? hourLabel(peakEnd) : '--'}
            </span>
          </div>
          <div className="ml-auto flex items-center gap-2">
            <div className="fm-depth-chip rounded-[10px] px-2.5 py-1.5 text-right">
              <div className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/36 dark:text-white/32">Share</div>
              <div className="text-[16px] font-black leading-none text-foreground dark:text-[#CCFF00] fm-depth-title">{arcShare == null ? '--' : `${arcShare}%`}</div>
            </div>
            <div className="fm-depth-chip rounded-[10px] px-2.5 py-1.5 text-right">
              <div className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/36 dark:text-white/32">Posts</div>
              <div className="text-[16px] font-black leading-none text-foreground dark:text-white fm-depth-title">{Math.max(0, peakCount)}</div>
            </div>
          </div>
        </div>

        {/* 24-hour bar chart — minimal */}
        <div className="flex flex-1 items-end gap-[2px] min-h-[80px]">
          {hourKeys.map((h) => {
            const count = byHour.get(h) || 0;
            const pct = !hasData ? 0 : count <= 0 ? 4 : (count / maxCount) * 100;
            const isKill = h >= peakStart && h < peakStart + 2;
            const isKillWrap = peakStart + 2 > 24 && h < (peakStart + 2) % 24;
            const inZone = hasData && (isKill || isKillWrap);

            return (
              <div key={h} className="relative flex flex-1 flex-col items-center justify-end h-full">
                <div
                  className={`w-full rounded-t-[3px] transition-all duration-500 ease-out ${
                    inZone
                      ? 'bg-[#CCFF00] shadow-[0_0_8px_rgba(204,255,0,0.4)] dark:bg-[#CCFF00] dark:shadow-[0_0_12px_rgba(204,255,0,0.3)]'
                      : 'bg-black/10 dark:bg-white/12'
                  }`}
                  style={{ height: `${pct}%` }}
                />
                {/* Show hour labels at 6-hour intervals */}
                {h % 6 === 0 && (
                  <div className="mt-1 text-[7px] font-black text-foreground/28 dark:text-white/24">
                    {h === 0 ? '12A' : h === 6 ? '6A' : h === 12 ? '12P' : '6P'}
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
