'use client';

import { CSSProperties, ReactNode, RefObject, useEffect, useMemo, useRef, useState } from 'react';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import FeedAscentChart from './FeedAscentChart';
import FeedVelocityBars from './FeedVelocityBars';
import FeedApexArch from './FeedApexArch';
import FeedExportTile from './FeedExportTile';
import FeedKillZone from './FeedKillZone';
import FeedScatterField from './FeedScatterField';
import FeedPostingPattern from './FeedPostingPattern';
import FeederPoolHero from './FeederPoolHero';
import PostingHeatmap from './PostingHeatmap';
import FeedEngagementAverages from './FeedEngagementAverages';
import { DashboardPayload, Timeframe } from './dashboardTypes';
import { GRID_ITEM_EASE } from '@/lib/motion';

type ActiveFeed = {
  id: string;
};

interface FeedDetailV2Props {
  activeFeed: ActiveFeed | null | undefined;
  children: ReactNode;
  timeframe: Timeframe;
  dashboardData: DashboardPayload | null;
  baselineDashboardData?: DashboardPayload | null;
  selectedHandle?: string;
  usePageScroll?: boolean;
  mobileSnapSections?: boolean;
  bottomClearance?: string;
  immersiveBrowserMode?: boolean;
  exportScopeLabel: string;
  exportFrom: string;
  exportTo: string;
  onExportFromChange: (value: string) => void;
  onExportToChange: (value: string) => void;
  onExport: () => void;
}

type MobileSectionItem = {
  key: string;
  node: ReactNode;
  className: string;
  style: CSSProperties;
};

type MobileSection = {
  id: string;
  items: MobileSectionItem[];
};

const DASHBOARD_TILE_BASE_DELAY = 0.08;
const DASHBOARD_TILE_STAGGER = 0.055;

function createDashboardTileTransition(delay = 0) {
  return {
    opacity: { duration: 0.22, delay, ease: GRID_ITEM_EASE },
    y: { type: 'spring', stiffness: 270, damping: 30, mass: 0.9, delay },
    scale: { duration: 0.34, delay, ease: GRID_ITEM_EASE },
  } as const;
}

function createStaggerContainer(reduceMotion: boolean) {
  return {
    hidden: {},
    visible: {
      transition: reduceMotion
        ? { staggerChildren: 0, delayChildren: 0 }
        : { staggerChildren: DASHBOARD_TILE_STAGGER, delayChildren: DASHBOARD_TILE_BASE_DELAY },
    },
  };
}

function createTileVariant(reduceMotion: boolean) {
  return {
    hidden: reduceMotion ? { y: 0, opacity: 1, scale: 1 } : { y: 18, opacity: 0, scale: 0.985 },
    visible: {
      y: 0,
      opacity: 1,
      scale: 1,
      transition: reduceMotion
        ? { duration: 0.01 }
        : createDashboardTileTransition(),
    },
  };
}

const MOBILE_TILE_HEIGHTS = {
  ascent: { minHeight: 'clamp(296px, calc(var(--fm-feed-mobile-section-height) - 14px), 540px)' },
  standard: { minHeight: 'clamp(184px, calc((var(--fm-feed-mobile-section-height) - var(--fm-feed-stack-gap)) / 2), 248px)' },
  compact: { minHeight: 'clamp(164px, calc((var(--fm-feed-mobile-section-height) - var(--fm-feed-stack-gap)) / 2), 208px)' },
  scatter: { minHeight: 'clamp(248px, calc(var(--fm-feed-mobile-section-height) - 14px), 420px)' },
  scatterPair: { minHeight: 'clamp(226px, calc((var(--fm-feed-mobile-section-height) - var(--fm-feed-stack-gap)) * 0.56), 340px)' },
  engagement: { minHeight: 'clamp(192px, calc((var(--fm-feed-mobile-section-height) - var(--fm-feed-stack-gap)) * 0.44), 258px)' },
  pattern: { minHeight: 'clamp(276px, calc(var(--fm-feed-mobile-section-height) - 14px), 450px)' },
  heatmap: { minHeight: 'clamp(252px, calc(var(--fm-feed-mobile-section-height) - 14px), 430px)' },
} as const;

function DeferredMobileSection({
  section,
  sectionIndex,
  scrollRootRef,
  usePageScroll,
  mobileSnapSections,
  eager,
  reduceMotion,
}: {
  section: MobileSection;
  sectionIndex: number;
  scrollRootRef: RefObject<HTMLDivElement | null>;
  usePageScroll: boolean;
  mobileSnapSections: boolean;
  eager: boolean;
  reduceMotion: boolean;
}) {
  const sectionRef = useRef<HTMLElement | null>(null);
  const renderImmediately = eager || usePageScroll || (typeof window !== 'undefined' && typeof window.IntersectionObserver !== 'function');
  const [isReady, setIsReady] = useState(() => (
    renderImmediately
  ));
  const isSectionReady = isReady || renderImmediately;
  const tileVariant = useMemo(() => createTileVariant(reduceMotion), [reduceMotion]);

  useEffect(() => {
    if (renderImmediately || isReady || typeof window === 'undefined') return;

    const node = sectionRef.current;
    if (!node) return;

    const observer = new IntersectionObserver(
      ([entry]) => {
        if (!entry.isIntersecting && entry.intersectionRatio <= 0) return;
        setIsReady(true);
        observer.disconnect();
      },
      {
        root: usePageScroll ? null : scrollRootRef.current,
        rootMargin: '120% 0px 120% 0px',
        threshold: 0.01,
      },
    );

    observer.observe(node);
    return () => observer.disconnect();
  }, [isReady, renderImmediately, scrollRootRef, usePageScroll]);

  return (
    <section
      ref={sectionRef}
      className={
        mobileSnapSections
          ? 'snap-start snap-always flex min-h-[var(--fm-feed-mobile-section-height)] items-center px-2 py-2 sm:px-3'
          : 'px-2 py-2 sm:px-3'
      }
      style={{
        scrollMarginTop: mobileSnapSections ? 'calc(var(--fm-mobile-detail-header-offset) + 10px)' : undefined,
      }}
    >
      <div className="fm-tab-canvas-shell mx-auto flex w-full">
        <div className={`mx-auto flex w-full max-w-[760px] flex-col gap-3 ${mobileSnapSections ? 'justify-center' : ''}`}>
          {section.items.map((item, itemIndex) => (
            isSectionReady ? (
              <motion.div
                key={item.key}
                variants={tileVariant}
                initial="hidden"
                animate="visible"
                transition={reduceMotion || (sectionIndex === 0 && itemIndex === 0)
                  ? undefined
                  : createDashboardTileTransition(DASHBOARD_TILE_BASE_DELAY + itemIndex * DASHBOARD_TILE_STAGGER)}
                className={`min-w-0 ${item.className}`}
                style={item.style}
              >
                {item.node}
              </motion.div>
            ) : (
              <div key={item.key} className={`min-w-0 ${item.className}`} style={item.style}>
                <div className="fm-depth-glass h-full w-full rounded-[22px] border border-white/62 bg-white/48 dark:border-white/8 dark:bg-white/[0.03]" />
              </div>
            )
          ))}
        </div>
      </div>
    </section>
  );
}

export default function FeedDetailV2({
  activeFeed,
  children,
  timeframe,
  dashboardData,
  baselineDashboardData = null,
  selectedHandle = 'all',
  usePageScroll = false,
  mobileSnapSections = false,
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
  const prefersReducedMotion = useReducedMotion();
  const reduceMotion = Boolean(prefersReducedMotion);
  const staggerContainer = useMemo(() => createStaggerContainer(reduceMotion), [reduceMotion]);
  const tileVariant = useMemo(() => createTileVariant(reduceMotion), [reduceMotion]);

  if (!activeFeed) return null;

  const ascentTile = <FeedAscentChart timeframe={timeframe} series={dashboardData?.ascent_series ?? []} />;
  const velocityTile = (
    <FeedVelocityBars
      summary={dashboardData?.summary ?? null}
      baselineSummary={baselineDashboardData?.summary ?? dashboardData?.summary ?? null}
    />
  );
  const killZoneTile = <FeedKillZone hours={dashboardData?.killzone_hours ?? []} days={dashboardData?.killzone_days ?? []} />;
  const apexTile = <FeedApexArch mix={dashboardData?.apex_mix ?? []} />;
  const scatterTile = <FeedScatterField points={dashboardData?.scatter_points ?? []} />;
  const engagementTile = <FeedEngagementAverages rows={dashboardData?.engagement_averages ?? []} />;
  const postingPatternTile = <FeedPostingPattern pattern={dashboardData?.posting_pattern ?? null} timeframe={timeframe} />;
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

  const mobileSections: MobileSection[] = [
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
          style: MOBILE_TILE_HEIGHTS.scatterPair,
        },
        {
          key: 'engagement',
          node: engagementTile,
          className: immersiveBrowserMode ? 'fm-feed-immersive-panel' : '',
          style: MOBILE_TILE_HEIGHTS.engagement,
        },
      ],
    },
    {
      id: 'posting-pattern',
      items: [
        {
          key: 'posting-pattern',
          node: postingPatternTile,
          className: immersiveBrowserMode ? 'fm-feed-immersive-panel' : '',
          style: MOBILE_TILE_HEIGHTS.pattern,
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
  ];

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
          <DeferredMobileSection
            key={section.id}
            section={section}
            sectionIndex={sectionIndex}
            scrollRootRef={scrollRef}
            usePageScroll={usePageScroll}
            mobileSnapSections={mobileSnapSections}
            eager={sectionIndex === 0 || usePageScroll}
            reduceMotion={reduceMotion}
          />
        ))}

        <div className="mt-2 px-2 pb-1 sm:px-3">
          <div className="fm-tab-canvas-shell mx-auto">
            <div className="w-full pt-1 pb-4">
              <div className="mb-4">
                <FeederPoolHero selectedHandle={selectedHandle} />
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

          <motion.div data-lock-id="engagement" variants={tileVariant} style={{ gridArea: 'engagement' }} className="fm-feed-mobile-panel min-w-0 min-h-[232px] xl:min-h-[240px]">
            {engagementTile}
          </motion.div>

          <motion.div data-lock-id="pattern" variants={tileVariant} style={{ gridArea: 'pattern' }} className="fm-feed-mobile-panel min-w-0 min-h-[252px] xl:min-h-[264px]">
            {postingPatternTile}
          </motion.div>

          <motion.div data-lock-id="heatmap" variants={tileVariant} style={{ gridArea: 'heatmap' }} className="min-w-0 min-h-[252px] xl:min-h-[264px]">
            {heatmapTile}
          </motion.div>

          <motion.div data-lock-id="targets" variants={tileVariant} style={{ gridArea: 'targets' }}>
            <div className="w-full pt-1 pb-4 lg:pt-2">
              <div className="mb-3">
                <FeederPoolHero selectedHandle={selectedHandle} />
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
