'use client';

import { useRef } from 'react';
import { motion } from 'framer-motion';
import FeedAscentChart from './FeedAscentChart';
import FeedVelocityBars from './FeedVelocityBars';
import FeedApexArch from './FeedApexArch';
import FeedExportTile from './FeedExportTile';
import FeedKillZone from './FeedKillZone';
import FeedScatterField from './FeedScatterField';
import FeedPatternBoard from './FeedPatternBoard';
import PostingHeatmap from './PostingHeatmap';
import { DashboardPayload, TIMEFRAME_TO_DAYS, Timeframe } from './dashboardTypes';

type ActiveFeed = {
  id: string;
};

interface FeedDetailV2Props {
  activeFeed: ActiveFeed | null | undefined;
  children: React.ReactNode;
  timeframe: Timeframe;
  dashboardData: DashboardPayload | null;
  usePageScroll?: boolean;
  bottomClearance?: string;
  immersiveBrowserMode?: boolean;
  exportScopeLabel: string;
  exportFrom: string;
  exportTo: string;
  onExportFromChange: (value: string) => void;
  onExportToChange: (value: string) => void;
  onExport: () => void;
}

const staggerContainer = {
  hidden: {},
  visible: {
    transition: { staggerChildren: 0.03, delayChildren: 0.03 },
  },
};

const tileVariant = {
  hidden: { y: 8, opacity: 0 },
  visible: {
    y: 0,
    opacity: 1,
    transition: { duration: 0.4, ease: [0.25, 0.1, 0.25, 1] as [number, number, number, number] },
  },
};

export default function FeedDetailV2({
  activeFeed,
  children,
  timeframe,
  dashboardData,
  usePageScroll = false,
  bottomClearance = 'calc(120px + env(safe-area-inset-bottom))',
  immersiveBrowserMode = false,
  exportScopeLabel,
  exportFrom,
  exportTo,
  onExportFromChange,
  onExportToChange,
  onExport,
}: FeedDetailV2Props) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const heatmapWeeks = 13;
  if (!activeFeed) return null;

  return (
    <div
      ref={scrollRef}
      className={
        usePageScroll
          ? 'w-full min-h-[var(--fm-app-height,100dvh)] overflow-visible overflow-x-hidden scroll-smooth transform-gpu'
          : 'hide-scrollbar h-full w-full overflow-y-auto overflow-x-hidden overscroll-y-contain scroll-smooth transform-gpu'
      }
      style={{
        WebkitOverflowScrolling: 'touch',
        scrollPaddingTop: 'calc(198px + env(safe-area-inset-top))',
        scrollPaddingBottom: bottomClearance,
      }}
    >
      {/* Header clearance — tighter to reduce dead space before first tile */}
      <div className="h-[calc(152px+env(safe-area-inset-top))] shrink-0 sm:h-[calc(160px+env(safe-area-inset-top))] lg:h-[148px]" />

      {/* ═══ BENTO GRID ═══ */}
      <motion.div
        variants={staggerContainer}
        initial="hidden"
        animate="visible"
        className="fm-tab-canvas-shell mx-auto w-full px-2 sm:px-0 transform-gpu"
        style={{ paddingBottom: bottomClearance }}
      >
        <div className="bento-feed-grid grid gap-2.5 sm:gap-3 lg:gap-3.5">
          {/* Ascent — hero card */}
          <motion.div data-lock-id="ascent" variants={tileVariant} style={{ gridArea: 'ascent' }} className={`fm-feed-mobile-panel min-w-0 min-h-[210px] sm:min-h-[240px] lg:min-h-[260px] xl:min-h-[272px] ${immersiveBrowserMode ? 'fm-feed-immersive-panel' : ''}`}>
            <FeedAscentChart timeframe={timeframe} series={dashboardData?.ascent_series ?? []} />
          </motion.div>

          {/* Performance */}
          <motion.div data-lock-id="pulse" variants={tileVariant} style={{ gridArea: 'pulse' }} className={`fm-feed-mobile-panel min-w-0 min-h-[245px] sm:min-h-[245px] lg:min-h-[260px] xl:min-h-[272px] ${immersiveBrowserMode ? 'fm-feed-immersive-panel' : ''}`}>
            <FeedVelocityBars summary={dashboardData?.summary ?? null} />
          </motion.div>

          {/* Export */}
          <motion.div data-lock-id="export" variants={tileVariant} style={{ gridArea: 'export' }} className={`fm-feed-mobile-panel min-w-0 min-h-[172px] sm:min-h-[178px] lg:min-h-[188px] xl:min-h-[196px] ${immersiveBrowserMode ? 'fm-feed-immersive-panel' : ''}`}>
            <FeedExportTile
              scopeLabel={exportScopeLabel}
              from={exportFrom}
              to={exportTo}
              onFromChange={onExportFromChange}
              onToChange={onExportToChange}
              onExport={onExport}
            />
          </motion.div>

          {/* Apex Arch */}
          <motion.div data-lock-id="apex" variants={tileVariant} style={{ gridArea: 'apex' }} className={`fm-feed-mobile-panel min-w-0 min-h-[232px] sm:min-h-[210px] lg:min-h-[188px] xl:min-h-[196px] ${immersiveBrowserMode ? 'fm-feed-immersive-panel' : ''}`}>
            <FeedApexArch mix={dashboardData?.apex_mix ?? []} />
          </motion.div>

          {/* Kill Zone */}
          <motion.div data-lock-id="kill" variants={tileVariant} style={{ gridArea: 'kill' }} className={`fm-feed-mobile-panel min-w-0 min-h-[168px] sm:min-h-[186px] lg:min-h-[188px] xl:min-h-[196px] ${immersiveBrowserMode ? 'fm-feed-immersive-panel' : ''}`}>
            <FeedKillZone hours={dashboardData?.killzone_hours ?? []} days={dashboardData?.killzone_days ?? []} />
          </motion.div>

          {/* Scatter */}
          <motion.div data-lock-id="scatter" variants={tileVariant} style={{ gridArea: 'scatter' }} className={`fm-feed-mobile-panel min-w-0 min-h-[248px] sm:min-h-[248px] lg:min-h-[232px] xl:min-h-[240px] ${immersiveBrowserMode ? 'fm-feed-immersive-panel' : ''}`}>
            <FeedScatterField points={dashboardData?.scatter_points ?? []} windowDays={TIMEFRAME_TO_DAYS[timeframe]} />
          </motion.div>

          {/* Heatmap */}
          <motion.div data-lock-id="heatmap" variants={tileVariant} style={{ gridArea: 'heatmap' }} className="min-w-0 min-h-[240px] sm:min-h-[228px] lg:min-h-[210px] xl:min-h-[218px]">
            <PostingHeatmap days={dashboardData?.heatmap_daily ?? []} weeks={heatmapWeeks} />
          </motion.div>

          {/* Targets */}
          <motion.div data-lock-id="targets" variants={tileVariant} style={{ gridArea: 'targets' }}>
            <div className="w-full pt-1 pb-4 lg:pt-2">
              <div className="mb-3">
                <FeedPatternBoard patterns={dashboardData?.pattern_board ?? []} />
              </div>
              <div className="mb-3 border-b border-foreground/10 pb-2 fm-label fm-depth-title">Target Acquisition List</div>
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:gap-4 xl:grid-cols-3 2xl:grid-cols-4">
                {children}
              </div>
            </div>
          </motion.div>
        </div>

      </motion.div>
    </div>
  );
}
