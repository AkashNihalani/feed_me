'use client';

import { useMemo, useRef } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
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

const MOBILE_TILE_HEIGHTS = {
  ascent: { minHeight: 'clamp(296px, calc(var(--fm-feed-mobile-section-height) - 14px), 540px)' },
  standard: { minHeight: 'clamp(184px, calc((var(--fm-feed-mobile-section-height) - var(--fm-feed-stack-gap)) / 2), 248px)' },
  compact: { minHeight: 'clamp(164px, calc((var(--fm-feed-mobile-section-height) - var(--fm-feed-stack-gap)) / 2), 208px)' },
  scatter: { minHeight: 'clamp(248px, calc(var(--fm-feed-mobile-section-height) - 14px), 420px)' },
  heatmap: { minHeight: 'clamp(252px, calc(var(--fm-feed-mobile-section-height) - 14px), 430px)' },
} as const;

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

  const ascentTile = <FeedAscentChart timeframe={timeframe} series={dashboardData?.ascent_series ?? []} />;
  const velocityTile = <FeedVelocityBars summary={dashboardData?.summary ?? null} />;
  const killZoneTile = <FeedKillZone hours={dashboardData?.killzone_hours ?? []} days={dashboardData?.killzone_days ?? []} />;
  const apexTile = <FeedApexArch mix={dashboardData?.apex_mix ?? []} />;
  const scatterTile = <FeedScatterField points={dashboardData?.scatter_points ?? []} windowDays={TIMEFRAME_TO_DAYS[timeframe]} />;
  const heatmapTile = <PostingHeatmap days={dashboardData?.heatmap_daily ?? []} weeks={heatmapWeeks} />;
  const exportTile = (
    <FeedExportTile
      scopeLabel={exportScopeLabel}
      from={exportFrom}
      to={exportTo}
      onFromChange={onExportFromChange}
      onToChange={onExportToChange}
      onExport={onExport}
    />
  );

  const mobileSections = useMemo(() => [
    {
      id: 'ascent',
      items: [
        {
          key: 'ascent',
          node: ascentTile,
          className: immersiveBrowserMode ? 'fm-feed-immersive-panel' : '',
          style: MOBILE_TILE_HEIGHTS.ascent,
        },
      ],
    },
    {
      id: 'performance',
      items: [
        {
          key: 'velocity',
          node: velocityTile,
          className: immersiveBrowserMode ? 'fm-feed-immersive-panel' : '',
          style: MOBILE_TILE_HEIGHTS.standard,
        },
        {
          key: 'kill',
          node: killZoneTile,
          className: immersiveBrowserMode ? 'fm-feed-immersive-panel' : '',
          style: MOBILE_TILE_HEIGHTS.standard,
        },
      ],
    },
    {
      id: 'mix-export',
      items: [
        {
          key: 'apex',
          node: apexTile,
          className: immersiveBrowserMode ? 'fm-feed-immersive-panel' : '',
          style: MOBILE_TILE_HEIGHTS.standard,
        },
        {
          key: 'export',
          node: exportTile,
          className: immersiveBrowserMode ? 'fm-feed-immersive-panel' : '',
          style: MOBILE_TILE_HEIGHTS.compact,
        },
      ],
    },
    {
      id: 'scatter',
      items: [
        {
          key: 'scatter',
          node: scatterTile,
          className: immersiveBrowserMode ? 'fm-feed-immersive-panel' : '',
          style: MOBILE_TILE_HEIGHTS.scatter,
        },
      ],
    },
    {
      id: 'heatmap',
      items: [
        {
          key: 'heatmap',
          node: heatmapTile,
          className: '',
          style: MOBILE_TILE_HEIGHTS.heatmap,
        },
      ],
    },
  ], [apexTile, ascentTile, exportTile, heatmapTile, immersiveBrowserMode, killZoneTile, scatterTile, velocityTile]);

  return (
    <div
      ref={scrollRef}
      className={
        usePageScroll
          ? 'fm-feed-detail-scroll w-full min-h-[var(--fm-app-height,100dvh)] overflow-visible overflow-x-hidden scroll-smooth transform-gpu'
          : 'fm-feed-detail-scroll hide-scrollbar h-full w-full snap-y snap-mandatory overflow-y-auto overflow-x-hidden overscroll-y-contain scroll-smooth transform-gpu'
      }
      style={{
        WebkitOverflowScrolling: 'touch',
        scrollPaddingTop: 'calc(var(--fm-mobile-detail-header-offset) + 6px)',
        scrollPaddingBottom: bottomClearance,
        ['--fm-feed-bottom-clearance' as string]: bottomClearance,
      }}
    >
      <div className="shrink-0" style={{ height: 'var(--fm-mobile-detail-header-offset)' }} />

      <div className="lg:hidden" style={{ paddingBottom: bottomClearance }}>
        {mobileSections.map((section, sectionIndex) => (
          <section
            key={section.id}
            className="snap-start snap-always flex min-h-[var(--fm-feed-mobile-section-height)] items-center px-2 py-2 sm:px-3"
            style={{ scrollMarginTop: 'calc(var(--fm-mobile-detail-header-offset) + 10px)' }}
          >
            <div className="fm-tab-canvas-shell mx-auto flex w-full">
              <div className="mx-auto flex w-full max-w-[760px] flex-col justify-center gap-3">
                {section.items.map((item, itemIndex) => (
                  <motion.div
                    key={item.key}
                    variants={tileVariant}
                    initial="hidden"
                    animate="visible"
                    transition={sectionIndex === 0 && itemIndex === 0 ? undefined : { delay: itemIndex * 0.05 }}
                    className={`min-w-0 ${item.className}`}
                    style={item.style}
                  >
                    {item.node}
                  </motion.div>
                ))}
              </div>
            </div>
          </section>
        ))}

        <div className="mt-2 px-2 pb-1 sm:px-3">
          <div className="fm-tab-canvas-shell mx-auto">
            <div className="w-full pt-1 pb-4">
              <div className="mb-4">
                <FeedPatternBoard patterns={dashboardData?.pattern_board ?? []} />
              </div>
              <div className="mb-3 border-b border-foreground/10 pb-2 fm-label fm-depth-title">Target Acquisition List</div>
              <motion.div layout className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                <AnimatePresence mode="popLayout">
                  {children}
                </AnimatePresence>
              </motion.div>
            </div>
          </div>
        </div>
      </div>

      <motion.div
        variants={staggerContainer}
        initial="hidden"
        animate="visible"
        className="fm-tab-canvas-shell mx-auto hidden w-full px-2 sm:px-0 transform-gpu lg:block"
        style={{ paddingBottom: bottomClearance }}
      >
        <div className="bento-feed-grid grid gap-2.5 sm:gap-3 lg:gap-3.5">
          <motion.div data-lock-id="ascent" variants={tileVariant} style={{ gridArea: 'ascent' }} className="fm-feed-mobile-panel min-w-0 min-h-[260px] xl:min-h-[272px]">
            {ascentTile}
          </motion.div>

          <motion.div data-lock-id="pulse" variants={tileVariant} style={{ gridArea: 'pulse' }} className="fm-feed-mobile-panel min-w-0 min-h-[260px] xl:min-h-[272px]">
            {velocityTile}
          </motion.div>

          <motion.div data-lock-id="export" variants={tileVariant} style={{ gridArea: 'export' }} className="fm-feed-mobile-panel min-w-0 min-h-[188px] xl:min-h-[196px]">
            {exportTile}
          </motion.div>

          <motion.div data-lock-id="apex" variants={tileVariant} style={{ gridArea: 'apex' }} className="fm-feed-mobile-panel min-w-0 min-h-[188px] xl:min-h-[196px]">
            {apexTile}
          </motion.div>

          <motion.div data-lock-id="kill" variants={tileVariant} style={{ gridArea: 'kill' }} className="fm-feed-mobile-panel min-w-0 min-h-[188px] xl:min-h-[196px]">
            {killZoneTile}
          </motion.div>

          <motion.div data-lock-id="scatter" variants={tileVariant} style={{ gridArea: 'scatter' }} className="fm-feed-mobile-panel min-w-0 min-h-[232px] xl:min-h-[240px]">
            {scatterTile}
          </motion.div>

          <motion.div data-lock-id="heatmap" variants={tileVariant} style={{ gridArea: 'heatmap' }} className="min-w-0 min-h-[210px] xl:min-h-[218px]">
            {heatmapTile}
          </motion.div>

          <motion.div data-lock-id="targets" variants={tileVariant} style={{ gridArea: 'targets' }}>
            <div className="w-full pt-1 pb-4 lg:pt-2">
              <div className="mb-3">
                <FeedPatternBoard patterns={dashboardData?.pattern_board ?? []} />
              </div>
              <div className="mb-3 border-b border-foreground/10 pb-2 fm-label fm-depth-title">Target Acquisition List</div>
              <motion.div layout className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:gap-4 xl:grid-cols-3 2xl:grid-cols-4">
                <AnimatePresence mode="popLayout">
                  {children}
                </AnimatePresence>
              </motion.div>
            </div>
          </motion.div>
        </div>
      </motion.div>
    </div>
  );
}
