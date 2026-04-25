'use client';

import { startTransition, useCallback, useEffect, useMemo, useState } from 'react';
import { ArrowLeft, ArrowUpRight, Image as ImageIcon, LayoutGrid, PlaySquare } from 'lucide-react';
import { useRouter } from 'next/navigation';
import { AnimatePresence, motion } from 'framer-motion';
import { cn } from '@/lib/utils';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import { GRID_LAYOUT_SPRING, GRID_ITEM_EASE, PILL_SPRING } from '@/lib/motion';

type FeederPostItem = {
  postKey: string;
  postUrl: string | null;
  thumbnailUrl: string | null;
  mediaType: 'image' | 'carousel' | 'reel' | 'unknown';
  postedAt: string | null;
  latestCheckpoint: string;
  latestBusinessDayIst: string | null;
  latestPercentile: number | null;
  views: number | null;
  likes: number | null;
  comments: number | null;
};

type FeederPayload = {
  feedId: number;
  feedName: string;
  handle: string;
  profilePicUrl: string | null;
  followerCount: number | null;
  trackedPosts: number;
  topPercentile: number | null;
};

type SortMode = 'percentile' | 'newest' | 'oldest';
type MediaFilter = 'all' | 'image' | 'carousel' | 'reel';


function formatCompact(value: number | null) {
  if (value == null || !Number.isFinite(value)) return '--';
  if (value >= 1000000) return `${(value / 1000000).toFixed(1).replace(/\.0$/, '')}M`;
  if (value >= 1000) return `${(value / 1000).toFixed(1).replace(/\.0$/, '')}K`;
  return new Intl.NumberFormat('en-US').format(Math.round(value));
}

function formatFollowers(value: number | null) {
  if (value == null || !Number.isFinite(value)) return '--';
  return new Intl.NumberFormat('en-US').format(Math.round(value));
}

function formatDate(value: string | null) {
  if (!value) return 'No post date';
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) return 'No post date';
  return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' });
}

function mediaLabel(value: MediaFilter | FeederPostItem['mediaType']) {
  if (value === 'all') return 'All';
  if (value === 'carousel') return 'Carousel';
  if (value === 'reel') return 'Reel';
  if (value === 'image') return 'Image';
  return 'Unknown';
}

function feederInitial(handle: string | null | undefined) {
  const clean = String(handle || '').replace(/^@+/, '').trim();
  return clean.slice(0, 1).toUpperCase() || 'F';
}

function MediaTypeBadge({ type }: { type: FeederPostItem['mediaType'] }) {
  return (
    <Badge>
      <span className="inline-flex items-center gap-1.5">
        {type === 'carousel' ? (
          <LayoutGrid size={11} strokeWidth={2.6} />
        ) : type === 'reel' ? (
          <PlaySquare size={11} strokeWidth={2.6} />
        ) : (
          <ImageIcon size={11} strokeWidth={2.6} />
        )}
        {mediaLabel(type)}
      </span>
    </Badge>
  );
}

export default function FeederPostsClient({
  feedId,
  handle,
}: {
  feedId: string;
  handle: string;
}) {
  const router = useRouter();
  const { appShellStyle, isStandaloneMode, useTranslucentBrowserChrome } = useMobileImmersiveViewport();
  const normalizedHandle = decodeURIComponent(handle).replace(/^@+/, '').toLowerCase();
  const [data, setData] = useState<{ feeder: FeederPayload; posts: FeederPostItem[] } | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [sortMode, setSortMode] = useState<SortMode>('percentile');
  const [mediaFilter, setMediaFilter] = useState<MediaFilter>('all');
  const bottomClearance = useTranslucentBrowserChrome
    ? 'calc(20px + env(safe-area-inset-bottom))'
    : isStandaloneMode
      ? 'calc(132px + env(safe-area-inset-bottom))'
      : 'calc(124px + env(safe-area-inset-bottom))';

  const updateSortMode = useCallback((next: SortMode) => {
    startTransition(() => {
      setSortMode(next);
    });
  }, []);

  const updateMediaFilter = useCallback((next: MediaFilter) => {
    startTransition(() => {
      setMediaFilter(next);
    });
  }, []);

  useEffect(() => {
    let cancelled = false;

    fetch(`/api/feed/feeder-posts?feedId=${encodeURIComponent(feedId)}&handle=${encodeURIComponent(normalizedHandle)}`, {
      cache: 'no-store',
    })
      .then(async (res) => {
        const json = await res.json();
        if (!res.ok) throw new Error(json.error || 'Failed to load feeder posts');
        return json as { feeder: FeederPayload; posts: FeederPostItem[] };
      })
      .then((json) => {
        if (cancelled) return;
        setData(json);
      })
      .catch((err: unknown) => {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : 'Failed to load feeder posts');
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    return () => {
      cancelled = true;
    };
  }, [feedId, normalizedHandle]);

  const filteredPosts = useMemo(() => {
    const source = data?.posts || [];
    const mediaScoped = mediaFilter === 'all'
      ? source
      : source.filter((post) => post.mediaType === mediaFilter);

    return [...mediaScoped].sort((a, b) => {
      if (sortMode === 'newest') return Date.parse(b.postedAt || '') - Date.parse(a.postedAt || '');
      if (sortMode === 'oldest') return Date.parse(a.postedAt || '') - Date.parse(b.postedAt || '');

      const percentileA = a.latestPercentile ?? Number.POSITIVE_INFINITY;
      const percentileB = b.latestPercentile ?? Number.POSITIVE_INFINITY;
      if (percentileA !== percentileB) return percentileA - percentileB;
      return Date.parse(b.postedAt || '') - Date.parse(a.postedAt || '');
    });
  }, [data?.posts, mediaFilter, sortMode]);

  const feeder = data?.feeder || null;

  return (
    <div
      className="relative min-h-[100dvh] overflow-x-hidden overflow-y-auto bg-background text-foreground"
      style={{ ...appShellStyle, paddingBottom: bottomClearance }}
    >
      <div className="pointer-events-none fixed inset-0 z-0 bg-white dark:bg-[#030303]" />

      <div className="relative z-10 mx-auto flex w-full max-w-[1360px] flex-col px-3 pb-10 pt-[calc(16px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-4 lg:px-6">
        {/* ═══ IDENTITY ROW ═══ */}
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={() => router.push(`/?id=${feedId}`, { scroll: false })}
            className="flex h-10 w-10 shrink-0 items-center justify-center rounded-[14px] fm-depth-chip text-foreground/56 dark:text-white/50"
            aria-label="Back to feed dashboard"
          >
            <ArrowLeft size={20} strokeWidth={2.6} />
          </button>

          <div className="flex h-11 w-11 shrink-0 items-center justify-center overflow-hidden rounded-[14px] border border-white/78 bg-[linear-gradient(135deg,rgba(225,29,72,0.18),rgba(255,255,255,0.92))] text-[13px] font-black uppercase tracking-[0.06em] text-[#881337] shadow-[0_4px_12px_rgba(15,23,42,0.08),inset_0_1px_0_rgba(255,255,255,0.84)] dark:border-white/10 dark:bg-[linear-gradient(135deg,rgba(225,29,72,0.6),rgba(24,24,27,0.96))] dark:text-white">
            {feeder?.profilePicUrl ? (
              // eslint-disable-next-line @next/next/no-img-element
              <img src={feeder.profilePicUrl} alt={`@${feeder.handle || normalizedHandle}`} className="h-full w-full object-cover" />
            ) : (
              <span>{feederInitial(feeder?.handle || normalizedHandle)}</span>
            )}
          </div>

          <div className="min-w-0 flex-1">
            <h1 className="truncate text-[24px] font-black leading-none tracking-[-0.04em] text-black dark:text-white sm:text-[28px]">
              @{feeder?.handle || normalizedHandle}
            </h1>
            <div className="mt-1 flex items-center gap-3 text-[10px] font-black uppercase tracking-[0.12em] text-foreground/40 dark:text-white/36">
              <span>{formatFollowers(feeder?.followerCount ?? null)} followers</span>
              <span className="text-foreground/16 dark:text-white/14">|</span>
              <span>{String(feeder?.trackedPosts ?? '--')} posts</span>
            </div>
          </div>

          {/* Hero percentile — inline, not a fat box */}
          <div className="hidden shrink-0 text-right sm:block">
            <div className="text-[9px] font-black uppercase tracking-[0.14em] text-foreground/36 dark:text-white/32">Best post</div>
            <div className="mt-0.5 text-[28px] font-black leading-none tracking-[-0.05em] text-[#E11D48]">
              {feeder?.topPercentile != null ? `${Math.round(feeder.topPercentile)}%` : '--'}
            </div>
          </div>
        </div>

        {/* ═══ FILTER BAR ═══ */}
        <div className="mt-3 flex items-center gap-3 overflow-x-auto">
          {/* Sort pills */}
          <div className="hide-scrollbar flex shrink-0 items-center gap-1.5">
            {([
              ['percentile', 'Top %'],
              ['newest', 'Newest'],
              ['oldest', 'Oldest'],
            ] as Array<[SortMode, string]>).map(([value, label]) => (
              <button
                key={value}
                type="button"
                onClick={() => updateSortMode(value)}
                className={cn(
                  'relative overflow-hidden rounded-full px-3.5 py-1.5 text-[10px] font-black uppercase tracking-[0.1em] transition-colors duration-200',
                  sortMode === value ? 'text-white' : 'fm-depth-chip text-foreground/46 dark:text-white/40',
                )}
              >
                {sortMode === value && (
                  <motion.span
                    layoutId="feeder-sort-pill-bg"
                    transition={PILL_SPRING}
                    className="absolute inset-0 rounded-full bg-[#E11D48] shadow-[0_4px_12px_rgba(225,29,72,0.22)]"
                  />
                )}
                <span className="relative z-10">{label}</span>
              </button>
            ))}
          </div>

          <div className="h-5 w-px shrink-0 bg-foreground/10 dark:bg-white/8" />

          {/* Media filter pills */}
          <div className="hide-scrollbar flex shrink-0 items-center gap-1.5">
            {(['all', 'image', 'carousel', 'reel'] as MediaFilter[]).map((value) => (
              <button
                key={value}
                type="button"
                onClick={() => updateMediaFilter(value)}
                className={cn(
                  'relative overflow-hidden whitespace-nowrap rounded-full px-3.5 py-1.5 text-[10px] font-black uppercase tracking-[0.1em] transition-colors duration-200',
                  mediaFilter === value ? 'text-white' : 'fm-depth-chip text-foreground/46 dark:text-white/40',
                )}
              >
                {mediaFilter === value && (
                  <motion.span
                    layoutId="feeder-media-pill-bg"
                    transition={PILL_SPRING}
                    className="absolute inset-0 rounded-full bg-[#E11D48] shadow-[0_4px_12px_rgba(225,29,72,0.22)]"
                  />
                )}
                <span className="relative z-10">{mediaLabel(value)}</span>
              </button>
            ))}
          </div>

          {/* Mobile-only best percentile */}
          <div className="ml-auto shrink-0 text-right sm:hidden">
            <div className="text-[8px] font-black uppercase tracking-[0.12em] text-foreground/32">Best</div>
            <div className="text-[18px] font-black leading-none tracking-[-0.04em] text-[#E11D48]">
              {feeder?.topPercentile != null ? `${Math.round(feeder.topPercentile)}%` : '--'}
            </div>
          </div>
        </div>

        <div className="relative z-10 mt-4">
          {loading ? (
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 2xl:grid-cols-5">
              {Array.from({ length: 6 }).map((_, index) => (
                <div key={index} className="fm-depth-glass aspect-[4/5] animate-pulse rounded-[24px] bg-black/[0.05] md:aspect-[10/11] dark:bg-white/[0.04]" />
              ))}
            </div>
          ) : error ? (
            <div className="fm-depth-glass rounded-[24px] p-8 text-center">
              <div className="text-[12px] font-black uppercase tracking-[0.16em] text-foreground/40">Unable to load feeder posts</div>
              <div className="mt-2 text-[14px] font-bold text-foreground/58">{error}</div>
            </div>
          ) : filteredPosts.length === 0 ? (
            <div className="fm-depth-glass rounded-[24px] p-10 text-center">
              <div className="text-[12px] font-black uppercase tracking-[0.16em] text-foreground/40">No tracked posts in this selection</div>
              <div className="mt-2 text-[14px] font-bold text-foreground/58">Try another sort or media filter.</div>
            </div>
          ) : (
            <motion.div layout className="grid grid-cols-1 gap-3 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 2xl:grid-cols-5">
              <AnimatePresence initial={false} mode="popLayout">
                {filteredPosts.map((post) => (
                  <FeederPostCard key={post.postKey} post={post} />
                ))}
              </AnimatePresence>
            </motion.div>
          )}
        </div>
      </div>
    </div>
  );
}

function FeederPostCard({ post }: { post: FeederPostItem }) {
  return (
    <motion.a
      layout
      href={post.postUrl || undefined}
      target={post.postUrl ? '_blank' : undefined}
      rel={post.postUrl ? 'noreferrer' : undefined}
      initial={{ opacity: 0, y: 18, scale: 0.975 }}
      animate={{ opacity: 1, y: 0, scale: 1 }}
      exit={{ opacity: 0, y: 10, scale: 0.97 }}
      transition={{
        layout: GRID_LAYOUT_SPRING,
        opacity: { duration: 0.18, ease: GRID_ITEM_EASE },
        y: { duration: 0.24, ease: GRID_ITEM_EASE },
        scale: { duration: 0.24, ease: GRID_ITEM_EASE },
      }}
      className="fm-depth-glass group relative flex aspect-[4/5] flex-col overflow-hidden rounded-[24px] p-3 md:aspect-[10/11] xl:aspect-[20/21]"
    >
      <div className="relative flex-1 overflow-hidden rounded-[18px]">
        {post.thumbnailUrl ? (
          // eslint-disable-next-line @next/next/no-img-element
          <img
            src={post.thumbnailUrl}
            alt=""
            loading="lazy"
            className="absolute inset-0 h-full w-full object-cover transition-transform duration-500 group-hover:scale-[1.03]"
          />
        ) : (
          <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,_rgba(225,29,72,0.28),_rgba(255,255,255,0.2)_35%,_rgba(0,0,0,0.06)_100%)] dark:bg-[radial-gradient(circle_at_top,_rgba(225,29,72,0.24),_rgba(255,255,255,0.05)_35%,_rgba(0,0,0,0.5)_100%)]" />
        )}

        <div className="absolute inset-0 bg-gradient-to-t from-black/82 via-black/18 to-black/4 dark:from-black/78 dark:via-black/16 dark:to-black/5" />

        <div className="absolute left-3 right-3 top-3 flex items-center justify-between gap-2">
          <Badge>{post.latestCheckpoint}</Badge>
          <MediaTypeBadge type={post.mediaType} />
        </div>

        <div className="absolute bottom-3 left-3 right-3">
          <div className="flex items-center justify-between gap-3">
            <div className="min-w-0">
              <div className="text-[9px] font-black uppercase tracking-[0.16em] text-white/65">
                {formatDate(post.postedAt)}
              </div>
              <div className="mt-1 text-[24px] font-black leading-none tracking-[-0.05em] text-white">
                {post.latestPercentile != null ? `Top ${Math.round(post.latestPercentile)}%` : '--'}
              </div>
            </div>
            {post.postUrl ? (
              <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-full bg-[#E11D48] text-white shadow-[0_8px_18px_rgba(225,29,72,0.28)]">
                <ArrowUpRight size={18} strokeWidth={2.6} />
              </div>
            ) : null}
          </div>
        </div>
      </div>

      <div className="mt-3 grid grid-cols-3 gap-2">
        <MetricCell label="Views" value={formatCompact(post.views)} />
        <MetricCell label="Likes" value={formatCompact(post.likes)} />
        <MetricCell label="Comms" value={formatCompact(post.comments)} />
      </div>
    </motion.a>
  );
}


function Badge({ children }: { children: React.ReactNode }) {
  return (
    <div className="rounded-full border border-white/14 bg-black/42 px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.14em] text-white/82 backdrop-blur-xl dark:border-white/14 dark:bg-black/44">
      {children}
    </div>
  );
}

function MetricCell({ label, value }: { label: string; value: string }) {
  return (
    <div className="rounded-[14px] border border-black/6 bg-black/[0.03] px-3 py-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.62)] dark:border-white/8 dark:bg-white/[0.03] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]">
      <div className="text-[8px] font-black uppercase tracking-[0.14em] text-foreground/38">{label}</div>
      <div className="mt-1 text-[15px] font-black leading-none tracking-[-0.03em] text-foreground dark:text-white">{value}</div>
    </div>
  );
}
