'use client';

import { useMemo, useState, type CSSProperties } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { FireCard3DProps } from './FireCard3D';
import { parseFirewatchData } from './firewatchUtils';

function percentText(value: number | null): string {
  return value == null || !Number.isFinite(value) ? '--' : `${Math.round(value)}%`;
}

function liftText(value: number | null): string {
  return value == null || !Number.isFinite(value) ? '--' : `${value.toFixed(1)}x`;
}

function compactCount(value: number | null, suffix: string): string {
  return value == null || !Number.isFinite(value) ? `-- ${suffix}` : `${Math.round(value)} ${suffix}`;
}

function clampStyle(lines: number): CSSProperties {
  return {
    display: '-webkit-box',
    WebkitLineClamp: lines,
    WebkitBoxOrient: 'vertical',
    overflow: 'hidden',
  };
}

function Mosaic({ posts }: { posts: ReturnType<typeof parseFirewatchData>['coverPosts'] }) {
  const primary = posts[0];
  const secondary = posts.slice(1, 4);

  return (
    <div className="absolute inset-x-0 top-0 h-[62%] overflow-hidden bg-black">
      {posts.length === 0 ? (
        <div className="absolute inset-0 bg-[radial-gradient(circle_at_20%_20%,rgba(225,29,72,0.28),transparent_44%),radial-gradient(circle_at_80%_10%,rgba(255,255,255,0.14),transparent_28%),linear-gradient(180deg,#151515_0%,#050505_100%)]" />
      ) : (
        <div className="grid h-full grid-cols-[1.35fr_0.9fr] gap-[2px] bg-black">
          <div className="relative overflow-hidden">
            {/* eslint-disable-next-line @next/next/no-img-element -- firewatch mosaic uses dynamic feed thumbnails in a dense decorative layout */}
            <img src={primary?.thumbnailUrl || ''} alt="" className="h-full w-full object-cover" />
          </div>
          <div className="grid grid-rows-3 gap-[2px]">
            {secondary.map((post) => (
              <div key={post.postKey} className="relative overflow-hidden">
                {/* eslint-disable-next-line @next/next/no-img-element -- firewatch mosaic uses dynamic feed thumbnails in a dense decorative layout */}
                <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" />
              </div>
            ))}
            {secondary.length < 3 && Array.from({ length: 3 - secondary.length }).map((_, index) => (
              <div
                key={`fill-${index}`}
                className="bg-[radial-gradient(circle_at_30%_30%,rgba(255,255,255,0.08),transparent_42%),linear-gradient(180deg,rgba(255,255,255,0.06),rgba(255,255,255,0.01))]"
              />
            ))}
          </div>
        </div>
      )}
      <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.04)_0%,rgba(0,0,0,0.16)_42%,rgba(0,0,0,0.74)_100%)]" />
    </div>
  );
}

function ProofStat({
  label,
  value,
  accent = false,
}: {
  label: string;
  value: string;
  accent?: boolean;
}) {
  return (
    <div className="min-w-0 rounded-[10px] border border-black/6 bg-white/48 px-2 py-1.5 shadow-[inset_0_1px_0_rgba(255,255,255,0.68)] dark:border-white/10 dark:bg-white/8">
      <div className="truncate text-[7px] font-black uppercase tracking-[0.12em] text-foreground/38 dark:text-white/34">
        {label}
      </div>
      <div className={`mt-0.5 truncate text-[10px] font-black uppercase tracking-[0.04em] ${accent ? 'text-[#E11D48]' : 'text-foreground/78 dark:text-white/76'}`}>
        {value}
      </div>
    </div>
  );
}

function SupportPreviewStrip({ posts }: { posts: ReturnType<typeof parseFirewatchData>['supportPosts'] }) {
  const visiblePosts = posts.slice(0, 3);
  if (visiblePosts.length === 0) return null;

  return (
    <div className="flex w-[86px] shrink-0 items-center gap-1.5 overflow-hidden sm:w-[96px]">
      {visiblePosts.map((post) => (
        <button
          key={post.postKey}
          type="button"
          onClick={(event) => {
            event.stopPropagation();
            if (post.postUrl) window.open(post.postUrl, '_blank', 'noreferrer');
          }}
          className="relative h-10 min-w-0 flex-1 overflow-hidden rounded-[10px] border border-white/55 bg-black/12 shadow-[0_8px_16px_rgba(0,0,0,0.12)] dark:border-white/12 dark:bg-white/8"
          aria-label={`Open supporting post by ${post.handle || 'feed'}`}
        >
          {/* eslint-disable-next-line @next/next/no-img-element -- hot support thumbnails use dynamic feed media */}
          <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" />
        </button>
      ))}
    </div>
  );
}

export function FireWatchCard3D({
  item,
  forcedOpen = false,
  highlighted = false,
  layoutMode = 'mobile',
  onOpenDetails,
}: FireCard3DProps) {
  const [openLocal, setOpenLocal] = useState(false);
  const isDesktopCard = layoutMode === 'desktop';
  const isCardInteractive = highlighted || forcedOpen;
  const isOpen = forcedOpen || (isCardInteractive && openLocal);
  const data = useMemo(() => parseFirewatchData(item), [item]);
  const patternTags = data.commonPattern.length > 0
    ? data.commonPattern
    : data.requiredCues.length > 0
      ? data.requiredCues
      : data.cues;
  const liftOrGap = data.anchorGap != null
    ? `+${Math.round(data.anchorGap)} gap`
    : data.recentLift != null
      ? `${liftText(data.recentLift)} lift`
      : null;
  const proofStats = [
    { label: 'Avg top', value: percentText(data.avgHotPercentile), accent: true },
    { label: 'Proof', value: compactCount(data.matchCount, 'hot') },
    { label: data.feedersCount != null && data.feedersCount > 1 ? 'Spread' : 'Format', value: data.feedersCount != null && data.feedersCount > 1 ? compactCount(data.feedersCount, 'feeders') : data.mediaType },
    ...(liftOrGap ? [{ label: data.anchorGap != null ? 'Gap' : 'Lift', value: liftOrGap }] : []),
    ...(data.confidence ? [{ label: 'Read', value: data.confidence }] : []),
  ];

  const handleCardActivate = () => {
    if (isDesktopCard) {
      onOpenDetails?.();
      return;
    }
    setOpenLocal((value) => !value);
  };

  return (
    <motion.div
      role="button"
      tabIndex={0}
      onClick={handleCardActivate}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          handleCardActivate();
        }
      }}
      className={isDesktopCard
        ? 'fm-fire-card-shell relative block w-full aspect-[5/6] 2xl:aspect-[11/14] overflow-hidden rounded-[24px] text-left'
        : 'relative block w-full aspect-[4/5] overflow-hidden rounded-[26px] text-left fm-depth-glass sm:rounded-[32px]'}
      style={{
        WebkitTapHighlightColor: 'transparent',
        maxHeight: isDesktopCard ? undefined : 'var(--fire-card-max-height, 78svh)',
        aspectRatio: isDesktopCard ? undefined : 'var(--fire-card-aspect, 4 / 5)',
        boxShadow: isDesktopCard
          ? highlighted
            ? '0 24px 46px rgba(0,0,0,0.34)'
            : '0 14px 28px rgba(0,0,0,0.24)'
          : highlighted
            ? '0 18px 38px rgba(0,0,0,0.24)'
            : '0 10px 22px rgba(0,0,0,0.16)',
        willChange: isDesktopCard ? 'auto' : 'transform',
      }}
      whileTap={{ scale: 0.994 }}
      transition={{ duration: 0.08, ease: [0.22, 1, 0.36, 1] }}
    >
      <div className="absolute inset-0 bg-[#050505]" />
      <Mosaic posts={data.coverPosts} />

      <div className="absolute inset-x-0 top-0 z-10 flex items-start justify-between p-3">
        <div className={isDesktopCard
          ? 'fm-fire-card-pill rounded-full px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.16em] text-white/86'
          : 'rounded-full border border-white/18 bg-black/44 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.16em] text-white/86 backdrop-blur-xl'}
        >
          Hot
        </div>
        <div className={isDesktopCard
          ? 'fm-fire-card-pill rounded-full px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.16em] text-white/64'
          : 'rounded-full border border-white/14 bg-black/36 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.16em] text-white/64 backdrop-blur-xl'}
        >
          {data.familyLabel}
        </div>
      </div>

      <div className="absolute inset-x-0 bottom-0 z-10 p-3 sm:p-4">
        <div className={isDesktopCard
          ? 'fm-fire-card-panel rounded-[24px] p-3'
          : 'rounded-[24px] border border-white/14 bg-[linear-gradient(180deg,rgba(14,14,14,0.24),rgba(10,10,10,0.84))] p-3 shadow-[0_22px_48px_rgba(0,0,0,0.42)] backdrop-blur-[18px]'}
        >
          <div className="flex items-end justify-between gap-3">
            <div className="min-w-0">
              <div className="text-[9px] font-black uppercase tracking-[0.16em] text-white/52">
                Avg Top
              </div>
              <div className={`mt-1 font-black leading-[0.86] tracking-[-0.055em] ${isDesktopCard ? 'text-[42px] 2xl:text-[50px]' : 'text-[54px] sm:text-[62px]'} ${data.avgHotPercentile != null && data.avgHotPercentile <= 15 ? 'text-[#E11D48] drop-shadow-[0_0_16px_rgba(225,29,72,0.58)]' : 'text-white'}`}>
                {percentText(data.avgHotPercentile)}
              </div>
            </div>
            <div className="shrink-0 text-right">
              <div className="text-[9px] font-black uppercase tracking-[0.16em] text-white/52">
                Proof
              </div>
              <div className={`${isDesktopCard ? 'text-[14px] 2xl:text-[16px]' : 'text-[16px] sm:text-[18px]'} mt-1 font-black uppercase tracking-[0.12em] text-white/86`}>
                {compactCount(data.matchCount, 'hot')}
              </div>
              <div className={`${isDesktopCard ? 'text-[11px] 2xl:text-[12px]' : 'text-[12px] sm:text-[13px]'} mt-1 max-w-[132px] truncate font-black uppercase tracking-[0.16em] text-white/64`}>
                {data.feedersCount != null && data.feedersCount > 1 ? compactCount(data.feedersCount, 'feeders') : data.feedName}
              </div>
            </div>
          </div>
        </div>
      </div>

      <AnimatePresence initial={false}>
        {!isDesktopCard && isOpen && (
          <motion.div
            className="absolute inset-x-2 top-2 bottom-2 z-20"
            initial={{ opacity: 0, y: 10, scale: 0.986 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: -6, scale: 0.992 }}
            transition={{ type: 'spring', stiffness: 420, damping: 34, mass: 0.58 }}
            style={{ willChange: 'transform, opacity' }}
          >
            <div className="relative flex h-full flex-col overflow-hidden rounded-[24px] border border-white/80 bg-white/70 p-2 sm:p-3 shadow-[0_32px_80px_rgba(0,0,0,0.34),inset_0_1px_0_rgba(255,255,255,0.95),inset_0_-16px_32px_rgba(255,255,255,0.1)] backdrop-blur-[48px] backdrop-saturate-[220%] dark:border-white/[0.08] dark:bg-[rgba(10,10,10,0.75)] dark:shadow-[0_40px_100px_rgba(0,0,0,0.8),inset_0_1px_0_rgba(255,255,255,0.1),inset_0_-1px_0_rgba(0,0,0,0.5)]">
              <div className="pointer-events-none absolute inset-0 rounded-[24px] bg-gradient-to-br from-white/90 via-white/40 to-transparent dark:from-white/10 dark:via-white/[0.02] dark:to-transparent" />
              <div className="relative z-10 flex min-h-0 flex-1 flex-col">
                <div className="flex h-full min-h-0 flex-col gap-1.5 overflow-hidden sm:gap-2">
                  <div className="shrink-0 rounded-[16px] border border-white/60 bg-white/56 p-2.5 shadow-[0_12px_28px_rgba(0,0,0,0.16),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/18 dark:bg-black/42">
                    <div className="flex items-center justify-between gap-2">
                      <div className="text-[8px] font-black uppercase tracking-[0.18em] text-[#E11D48]">
                        Hot
                      </div>
                      <div className="truncate text-[8px] font-black uppercase tracking-[0.14em] text-foreground/42">
                        {data.feedName} · {data.mediaType}
                      </div>
                    </div>
                    <div
                      className="mt-1 text-[19px] font-black leading-[0.94] tracking-[-0.025em] text-foreground/95 sm:text-[21px]"
                      style={clampStyle(3)}
                    >
                      {data.patternLabel}
                    </div>
                  </div>

                  <div className="min-h-0 flex-1 rounded-[16px] border border-white/60 bg-white/50 p-2.5 shadow-[0_12px_28px_rgba(0,0,0,0.14)] dark:border-white/18 dark:bg-black/38">
                    <div className="flex h-full min-h-0 flex-col gap-1.5 overflow-hidden">
                      {data.whatHappened && (
                        <div className="min-h-0">
                          <div className="text-[7px] font-black uppercase tracking-[0.16em] text-foreground/42 dark:text-white/34">What happened</div>
                          <p className="mt-0.5 text-[10px] font-semibold leading-snug text-foreground/76 dark:text-white/70 sm:text-[11px]" style={clampStyle(3)}>
                            {data.whatHappened}
                          </p>
                        </div>
                      )}
                      {data.whyItMayHaveHappened && (
                        <div className="min-h-0">
                          <div className="text-[7px] font-black uppercase tracking-[0.16em] text-foreground/42 dark:text-white/34">Why</div>
                          <p className="mt-0.5 text-[9px] font-medium leading-snug text-foreground/52 dark:text-white/46 sm:text-[10px]" style={clampStyle(3)}>
                            {data.whyItMayHaveHappened}
                          </p>
                        </div>
                      )}
                      {patternTags.length > 0 && (
                        <div className="flex shrink-0 flex-wrap gap-1">
                          {patternTags.slice(0, 4).map((tag) => (
                            <span
                              key={`${item.id}-${tag}`}
                              className="rounded-full border border-black/8 bg-white/62 px-1.5 py-0.5 text-[7px] font-black uppercase tracking-[0.11em] text-foreground/64 shadow-[inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/12 dark:bg-white/10 dark:text-white/66"
                            >
                              {tag}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>
                  </div>

                  {data.doNext && (
                    <div className="shrink-0 rounded-[12px] bg-[#E11D48] px-2.5 py-2 text-[9.5px] font-black leading-snug text-white shadow-[0_10px_24px_rgba(225,29,72,0.22)] sm:text-[10px]" style={clampStyle(2)}>
                      {data.doNext}
                    </div>
                  )}

                  <div className="grid shrink-0 grid-cols-3 gap-1.5">
                    {proofStats.slice(0, 6).map((stat) => (
                      <ProofStat key={`${stat.label}-${stat.value}`} label={stat.label} value={stat.value} accent={stat.accent} />
                    ))}
                  </div>

                  <div className="flex shrink-0 gap-1.5">
                    {data.watchout && (
                      <p className="min-w-0 flex-1 rounded-[12px] border border-white/50 bg-white/36 px-2 py-1.5 text-[8px] font-semibold leading-snug text-foreground/42 dark:border-white/12 dark:bg-black/28 dark:text-white/34" style={clampStyle(2)}>
                        {data.watchout}
                      </p>
                    )}
                    <SupportPreviewStrip posts={data.supportPosts} />
                  </div>
                </div>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
