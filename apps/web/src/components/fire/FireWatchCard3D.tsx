'use client';

import { useMemo, useState } from 'react';
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

function Mosaic({ posts }: { posts: ReturnType<typeof parseFirewatchData>['coverPosts'] }) {
  const primary = posts[0];
  const secondary = posts.slice(1, 4);

  return (
    <div className="absolute inset-x-0 top-0 h-[62%] overflow-hidden">
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
      <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.04)_0%,rgba(0,0,0,0.18)_42%,rgba(0,0,0,0.78)_100%)]" />
    </div>
  );
}

function SupportRail({
  posts,
  title,
}: {
  posts: ReturnType<typeof parseFirewatchData>['supportPosts'];
  title: string;
}) {
  if (posts.length === 0) return null;

  return (
    <div className="mt-3">
      <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/48">
        {title}
      </div>
      <div className="hide-scrollbar mt-2 -mx-0.5 flex gap-2 overflow-x-auto pb-1">
        {posts.map((post) => (
          <button
            key={post.postKey}
            type="button"
            onClick={(event) => {
              event.stopPropagation();
              if (post.postUrl) window.open(post.postUrl, '_blank', 'noreferrer');
            }}
            className="shrink-0 text-left"
          >
            <div className="h-20 w-16 overflow-hidden rounded-[12px] border border-white/55 bg-black/12 shadow-[0_8px_18px_rgba(0,0,0,0.18)] dark:border-white/12 dark:bg-white/8">
              {/* eslint-disable-next-line @next/next/no-img-element -- firewatch support thumbnails are dynamic feed assets without stable intrinsic metadata */}
              <img src={post.thumbnailUrl} alt="" className="h-full w-full object-cover" />
            </div>
            <div className="mt-1 max-w-16 truncate text-[8px] font-black uppercase tracking-[0.13em] text-foreground/68 dark:text-white/70">
              @{(post.handle || 'feed').toUpperCase()}
            </div>
          </button>
        ))}
      </div>
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
  const coverTags = (data.requiredCues.length > 0 ? data.requiredCues : data.cues).slice(0, 2);

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
        ? 'fm-fire-card-shell relative block w-full aspect-[11/14] overflow-hidden rounded-[24px] text-left'
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
          Firewatch
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
          : 'rounded-[24px] border border-white/14 bg-[linear-gradient(180deg,rgba(14,14,14,0.22),rgba(10,10,10,0.82))] p-3 shadow-[0_22px_48px_rgba(0,0,0,0.42)] backdrop-blur-[18px]'}
        >
          <div className="flex items-end justify-between gap-3">
            <div>
              <div className="text-[9px] font-black uppercase tracking-[0.16em] text-white/52">
                Avg Top
              </div>
              <div className="mt-1 text-[38px] font-black leading-[0.9] tracking-[-0.05em] text-white">
                {percentText(data.avgHotPercentile)}
              </div>
            </div>
            <div className="text-right">
              <div className="text-[9px] font-black uppercase tracking-[0.16em] text-white/52">
                {data.mediaType}
              </div>
              <div className="mt-1 text-[12px] font-black uppercase tracking-[0.16em] text-white/78">
                {compactCount(data.matchCount, 'hot')}
              </div>
              <div className="mt-1 text-[12px] font-black uppercase tracking-[0.16em] text-white/62">
                {data.feedersCount != null && data.feedersCount > 1 ? compactCount(data.feedersCount, 'feeders') : data.feedName}
              </div>
            </div>
          </div>

          <div className="mt-3 flex flex-wrap gap-1.5">
            {coverTags.map((tag) => (
              <span
                key={`${item.id}-${tag}`}
                className="rounded-full border border-white/10 bg-white/10 px-2 py-1 text-[8px] font-black uppercase tracking-[0.13em] text-white/74"
              >
                {tag}
              </span>
            ))}
            {data.anchorGap != null && (
              <span className="rounded-full border border-white/10 bg-white/10 px-2 py-1 text-[8px] font-black uppercase tracking-[0.13em] text-white/74">
                Gap +{Math.round(data.anchorGap)}
              </span>
            )}
            {data.recentLift != null && (
              <span className="rounded-full border border-white/10 bg-white/10 px-2 py-1 text-[8px] font-black uppercase tracking-[0.13em] text-white/74">
                {liftText(data.recentLift)}
              </span>
            )}
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
                <div className="hide-scrollbar min-h-0 flex-1 overflow-y-auto pr-0.5">
                  <div className="rounded-[16px] border border-white/60 bg-white/56 p-3 shadow-[0_12px_28px_rgba(0,0,0,0.18),inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/18 dark:bg-black/42">
                    <div className="flex items-center justify-between gap-2">
                      <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/62">
                        Firewatch
                      </div>
                      <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/44">
                        {data.familyLabel}
                      </div>
                    </div>
                    <div className="mt-1.5 text-[18px] font-black leading-[0.94] tracking-[-0.025em] text-foreground/95">
                      {data.patternLabel}
                    </div>
                    <div className="mt-1.5 text-[9px] font-black uppercase tracking-[0.16em] text-foreground/46">
                      {data.feedName} · {data.mediaType} · D7
                    </div>
                  </div>

                  <div className="mt-2 grid grid-cols-2 gap-2">
                    <div className="rounded-[14px] bg-[#E11D48] p-3 text-white shadow-[0_10px_24px_rgba(225,29,72,0.32)]">
                      <div className="text-[8px] font-black uppercase tracking-[0.16em] text-white/72">Avg Top</div>
                      <div className="mt-1 text-[28px] font-black leading-none tracking-[-0.05em]">
                        {percentText(data.avgHotPercentile)}
                      </div>
                    </div>
                    <div className="rounded-[14px] border border-white/55 bg-white/45 p-3 shadow-[0_12px_28px_rgba(0,0,0,0.18)] dark:border-white/22 dark:bg-black/45">
                      <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/52">Coverage</div>
                      <div className="mt-1 text-[18px] font-black leading-none text-foreground/94">{compactCount(data.matchCount, 'hot')}</div>
                      <div className="mt-1 text-[11px] font-black uppercase tracking-[0.14em] text-foreground/64">
                        {data.feedersCount != null && data.feedersCount > 1 ? compactCount(data.feedersCount, 'feeders') : data.mediaType}
                      </div>
                    </div>
                    {data.anchorGap != null && (
                      <div className="rounded-[14px] border border-white/55 bg-white/45 p-3 shadow-[0_12px_28px_rgba(0,0,0,0.18)] dark:border-white/22 dark:bg-black/45">
                        <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/52">Anchor Gap</div>
                        <div className="mt-1 text-[24px] font-black leading-none text-foreground/94">+{Math.round(data.anchorGap)}</div>
                      </div>
                    )}
                    {data.recentLift != null && (
                      <div className="rounded-[14px] border border-white/55 bg-white/45 p-3 shadow-[0_12px_28px_rgba(0,0,0,0.18)] dark:border-white/22 dark:bg-black/45">
                        <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/52">Recent Lift</div>
                        <div className="mt-1 text-[24px] font-black leading-none text-foreground/94">{liftText(data.recentLift)}</div>
                      </div>
                    )}
                  </div>

                  <div className="mt-3 rounded-[16px] border border-white/60 bg-white/50 p-3 shadow-[0_12px_28px_rgba(0,0,0,0.16)] dark:border-white/18 dark:bg-black/38">
                    <div className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/48">Shared Tags</div>
                    <div className="mt-2 flex flex-wrap gap-1.5">
                      {[...(data.requiredCues.length > 0 ? data.requiredCues : data.cues)].slice(0, 4).map((tag) => (
                        <span
                          key={`${item.id}-${tag}`}
                          className="rounded-full border border-black/8 bg-white/62 px-2 py-1 text-[8px] font-black uppercase tracking-[0.13em] text-foreground/72 shadow-[inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/12 dark:bg-white/10 dark:text-white/72"
                        >
                          {tag}
                        </span>
                      ))}
                    </div>
                  </div>

                  <SupportRail posts={data.supportPosts} title="Supporting Posts" />
                  <SupportRail posts={data.anchorSupportPosts} title="Anchor Compare" />
                </div>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
