'use client';

import { useRef } from 'react';
import { motion } from 'framer-motion';
import FeedAscentChart from './FeedAscentChart';
import FeedVelocityBars from './FeedVelocityBars';
import FeedApexArch from './FeedApexArch';
import FeedKillZone from './FeedKillZone';
import FeedScatterField from './FeedScatterField';
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
}

const staggerContainer = {
  hidden: {},
  visible: {
    transition: { staggerChildren: 0.05, delayChildren: 0.05 },
  },
};

const tileVariant = {
  hidden: { y: 12, opacity: 0 },
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
}: FeedDetailV2Props) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const heatmapWeeks = Math.max(1, Math.ceil(TIMEFRAME_TO_DAYS[timeframe] / 7));
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
        scrollPaddingTop: 'calc(220px + env(safe-area-inset-top))',
        scrollPaddingBottom: bottomClearance,
      }}
    >
      {/* Header clearance */}
      <div className="h-[calc(190px+env(safe-area-inset-top))] shrink-0 sm:h-[calc(196px+env(safe-area-inset-top))] lg:h-[224px]" />

      {/* ═══ BENTO GRID ═══ */}
      <motion.div
        variants={staggerContainer}
        initial="hidden"
        animate="visible"
        className="fm-tab-canvas-shell mx-auto w-full px-2 sm:px-0 transform-gpu"
        style={{ paddingBottom: bottomClearance }}
      >
        <div className="bento-feed-grid grid gap-3 sm:gap-4 lg:gap-4 xl:gap-4">
          {/* Ascent — hero card */}
          <motion.div data-lock-id="ascent" variants={tileVariant} style={{ gridArea: 'ascent' }} className={`fm-feed-mobile-panel min-w-0 min-h-[220px] sm:min-h-[260px] lg:min-h-[300px] xl:min-h-[300px] ${immersiveBrowserMode ? 'fm-feed-immersive-panel' : ''}`}>
            <FeedAscentChart timeframe={timeframe} series={dashboardData?.ascent_series ?? []} />
          </motion.div>

          {/* Pulse */}
          <motion.div data-lock-id="pulse" variants={tileVariant} style={{ gridArea: 'pulse' }} className={`fm-feed-mobile-panel min-w-0 min-h-[170px] sm:min-h-[210px] lg:min-h-[300px] xl:min-h-[300px] ${immersiveBrowserMode ? 'fm-feed-immersive-panel' : ''}`}>
            <FeedVelocityBars summary={dashboardData?.summary ?? null} />
          </motion.div>

          {/* Apex Arch */}
          <motion.div data-lock-id="apex" variants={tileVariant} style={{ gridArea: 'apex' }} className={`fm-feed-mobile-panel min-w-0 min-h-[170px] sm:min-h-[210px] lg:min-h-[240px] xl:min-h-[240px] ${immersiveBrowserMode ? 'fm-feed-immersive-panel' : ''}`}>
            <FeedApexArch mix={dashboardData?.apex_mix ?? []} />
          </motion.div>

          {/* Heatmap */}
          <motion.div data-lock-id="heatmap" variants={tileVariant} style={{ gridArea: 'heatmap' }} className="min-w-0 min-h-[180px] sm:min-h-[200px] lg:min-h-[240px] xl:min-h-[240px]">
            <PostingHeatmap days={dashboardData?.heatmap_daily ?? []} weeks={heatmapWeeks} />
          </motion.div>

          {/* Kill Zone */}
          <motion.div data-lock-id="kill" variants={tileVariant} style={{ gridArea: 'kill' }} className={`fm-feed-mobile-panel min-w-0 min-h-[160px] sm:min-h-[200px] lg:min-h-[240px] xl:min-h-[240px] ${immersiveBrowserMode ? 'fm-feed-immersive-panel' : ''}`}>
            <FeedKillZone hours={dashboardData?.killzone_hours ?? []} />
          </motion.div>

          {/* Scatter */}
          <motion.div data-lock-id="scatter" variants={tileVariant} style={{ gridArea: 'scatter' }} className={`fm-feed-mobile-panel min-w-0 min-h-[220px] sm:min-h-[260px] lg:min-h-[280px] xl:min-h-[280px] xl:max-h-[380px] ${immersiveBrowserMode ? 'fm-feed-immersive-panel' : ''}`}>
            <FeedScatterField points={dashboardData?.scatter_points ?? []} />
          </motion.div>

          {/* Targets */}
          <motion.div data-lock-id="targets" variants={tileVariant} style={{ gridArea: 'targets' }}>
            <div className="w-full pt-3 pb-6 lg:pt-4">
              <div className="mb-4 border-b border-foreground/10 pb-2 fm-label fm-depth-title">Target Acquisition List</div>
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
