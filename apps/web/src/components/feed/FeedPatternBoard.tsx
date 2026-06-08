'use client';

import { type CSSProperties, type ReactNode, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion } from 'framer-motion';
import { X } from 'lucide-react';
import { type PatternBoardItem, type PatternBoardSignalCard, type PatternBoardSupportPost } from './dashboardTypes';

type FeedPatternBoardProps = {
  patterns: PatternBoardItem[];
};

type HeadlineNumber = {
  value: string;
  qualifier: string;
};

type PreviewAsset = {
  key: string;
  src: string;
  fallbackSrc: string;
  post: PatternBoardSupportPost | null;
};

function toFiniteNumber(value: unknown): number | null {
  const numeric = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function familyTag(context: PatternBoardItem['context']): string {
  if (context === 'cross') return 'Feed';
  if (context === 'anchor') return 'Gap';
  return 'Read';
}

function mediaProxyUrl(postKey: string | null | undefined): string {
  const key = (postKey || '').trim();
  return key ? `/api/media?postKey=${encodeURIComponent(key)}&role=thumbnail` : '';
}

function previewAsset(post: PatternBoardSupportPost | null, forceFallback = false): PreviewAsset {
  const fallback = mediaProxyUrl(post?.post_key);
  const src = forceFallback ? fallback : post?.thumbnail_url || fallback;
  return {
    key: `${post?.post_key || 'none'}:${src || ''}`,
    src,
    fallbackSrc: fallback,
    post,
  };
}

function preloadPreviewAssets(posts: PatternBoardSupportPost[], limit = posts.length) {
  if (typeof window === 'undefined') return;
  const seen = new Set<string>();
  for (const post of posts.slice(0, limit)) {
    const asset = previewAsset(post);
    const src = asset.src || asset.fallbackSrc;
    if (!src || seen.has(src)) continue;
    seen.add(src);
    const image = new window.Image();
    image.src = src;
  }
}

function compactNumber(value: number | null): string {
  if (value == null || !Number.isFinite(value)) return '--';
  const rounded = Math.round(value);
  if (Math.abs(rounded) >= 1_000_000) return `${(rounded / 1_000_000).toFixed(1).replace(/\.0$/, '')}M`;
  if (Math.abs(rounded) >= 1_000) return `${(rounded / 1_000).toFixed(1).replace(/\.0$/, '')}K`;
  return String(rounded);
}

function pickHeadlineNumber(item: PatternBoardItem): HeadlineNumber | null {
  const recentLift = toFiniteNumber(item.recent_lift);
  const anchorGap = toFiniteNumber(item.anchor_gap);
  const matchCount = toFiniteNumber(item.match_count);
  const avgPctile = toFiniteNumber(item.avg_hot_percentile);

  if (recentLift != null && recentLift >= 1.5) {
    return { value: `${recentLift.toFixed(1)}×`, qualifier: '14d lift' };
  }
  if (anchorGap != null && anchorGap >= 6) {
    return { value: `+${Math.round(anchorGap)}`, qualifier: 'anchor gap' };
  }
  if (matchCount != null && matchCount > 0) {
    return { value: compactNumber(matchCount), qualifier: 'winners' };
  }
  if (avgPctile != null) {
    return { value: `${Math.round(avgPctile)}%`, qualifier: 'avg top' };
  }
  return null;
}

function patternPills(pattern: PatternBoardItem): Array<{ label: string; value: string }> {
  const matchCount = toFiniteNumber(pattern.match_count);
  const feedersCount = toFiniteNumber(pattern.feeders_count);
  const avgPctile = toFiniteNumber(pattern.avg_hot_percentile);

  return [
    matchCount != null && matchCount > 0 ? { label: 'Winners', value: compactNumber(matchCount) } : null,
    feedersCount != null && feedersCount > 1 ? { label: 'Spread', value: compactNumber(feedersCount) } : null,
    avgPctile != null ? { label: 'Avg', value: `Top ${Math.round(avgPctile)}%` } : null,
  ].filter((entry): entry is { label: string; value: string } => Boolean(entry)).slice(0, 3);
}

function readableSignalCode(value: string | null | undefined): string {
  return String(value || 'Signal')
    .replace(/_/g, ' ')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function normalizedConfidence(value: string | null | undefined): string {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized === 'high' || normalized === 'medium' || normalized === 'low') return normalized;
  return '';
}

function evidenceLabel(post: PatternBoardSupportPost | null | undefined): string {
  return post?.evidence_label || '';
}

function evidenceToneClasses(post: PatternBoardSupportPost | null | undefined): string {
  if (post?.evidence_tone === 'positive') return 'border-emerald-300/24 bg-emerald-300/14 text-emerald-100';
  if (post?.evidence_tone === 'negative') return 'border-[#E11D48]/28 bg-[#E11D48]/16 text-[#FDA4AF]';
  return 'border-white/[0.1] bg-white/[0.08] text-white/72';
}

function evidenceSummary(post: PatternBoardSupportPost | null | undefined): string {
  const label = evidenceLabel(post).toLowerCase();
  if (label === 'pick') return 'Picked proof';
  if (label === 'drop') return 'Dropped proof';
  if (label === 'mid') return 'Middle read';
  return '';
}

function readablePostRole(value: string | null | undefined): string {
  const normalized = String(value || '').trim().toLowerCase();
  const labels: Record<string, string> = {
    trigger_core: 'Main evidence',
    trigger_support: 'Supporting evidence',
    reference_typical: 'Baseline reference',
    reference_strong: 'Stronger reference',
    reference_no_jump: 'No-jump reference',
    reference_other_format: 'Other-format reference',
    reference_anchor: 'Primary reference',
    reference_feed: 'Feed reference',
    reference_context: 'Comparison reference',
  };
  if (labels[normalized]) return labels[normalized];
  return normalized
    .replace(/_/g, ' ')
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function proofMetrics(pattern: PatternBoardItem): Array<{ label: string; value: string }> {
  const avgPctile = toFiniteNumber(pattern.avg_hot_percentile);
  const matchCount = toFiniteNumber(pattern.match_count);
  const feedersCount = toFiniteNumber(pattern.feeders_count);
  const recentLift = toFiniteNumber(pattern.recent_lift);
  const anchorGap = toFiniteNumber(pattern.anchor_gap);

  return [
    avgPctile != null ? { label: 'Avg top', value: `${Math.round(avgPctile)}%` } : null,
    matchCount != null && matchCount > 0 ? { label: 'Matches', value: compactNumber(matchCount) } : null,
    feedersCount != null && feedersCount > 1 ? { label: 'Feeders', value: compactNumber(feedersCount) } : null,
    recentLift != null && recentLift > 0 ? { label: 'Lift', value: `${recentLift.toFixed(1)}×` } : null,
    anchorGap != null && anchorGap > 0 ? { label: 'Gap', value: `+${Math.round(anchorGap)}` } : null,
  ].filter((entry): entry is { label: string; value: string } => Boolean(entry)).slice(0, 5);
}

function ProofTile({
  post,
  large,
}: {
  post: PatternBoardSupportPost;
  large: boolean;
}) {
  const fallback = mediaProxyUrl(post.post_key);
  const [src, setSrc] = useState(() => post.thumbnail_url || fallback);
  const [dead, setDead] = useState(false);
  const handle = (post.handle || 'feed').replace(/^@+/, '').toUpperCase();
  const label = evidenceLabel(post);

  return (
    <a
      href={post.post_url || '#'}
      target="_blank"
      rel="noreferrer"
      className={[
        'group relative block min-w-0 overflow-hidden rounded-[14px] border border-white/14 bg-white/[0.055]',
        'shadow-[inset_0_1px_0_rgba(255,255,255,0.08),0_10px_22px_-16px_rgba(0,0,0,0.6)]',
        large ? 'col-span-2 row-span-2' : '',
      ].filter(Boolean).join(' ')}
      onClick={(event) => {
        if (!post.post_url) {
          event.preventDefault();
        } else {
          event.stopPropagation();
        }
      }}
      aria-label={`Open @${handle} support post`}
    >
      {src && !dead ? (
        // eslint-disable-next-line @next/next/no-img-element -- firewatch thumbnails are dynamic feed assets
        <img
          src={src}
          alt=""
          className="h-full w-full object-cover transition duration-300 group-hover:scale-[1.035]"
          onError={() => {
            if (fallback && src !== fallback) {
              setSrc(fallback);
              return;
            }
            setDead(true);
          }}
        />
      ) : (
        <div className="h-full w-full bg-[radial-gradient(circle_at_28%_24%,rgba(225,29,72,0.22),transparent_42%),linear-gradient(135deg,rgba(255,255,255,0.08),rgba(255,255,255,0.025))]" />
      )}
      <div className="pointer-events-none absolute inset-x-0 bottom-0 flex min-w-0 items-center gap-1.5 bg-[linear-gradient(180deg,transparent_0%,rgba(0,0,0,0.78)_100%)] px-2 pb-1.5 pt-5">
        {label && (
          <span className={[
            'shrink-0 rounded-full border px-1.5 py-0.5 text-[6px] font-black uppercase tracking-[0.1em]',
            evidenceToneClasses(post),
          ].join(' ')}
          >
            {label}
          </span>
        )}
        <div className="min-w-0 truncate text-[8px] font-black uppercase tracking-[0.12em] text-white/86">
          @{handle}
        </div>
      </div>
    </a>
  );
}

function ProofMosaic({ posts }: { posts: PatternBoardSupportPost[] }) {
  const proofPosts = posts.slice(0, 4);
  if (proofPosts.length === 0) {
    return (
      <div className="flex h-[124px] items-center justify-center rounded-[18px] border border-white/[0.08] bg-[radial-gradient(circle_at_34%_22%,rgba(225,29,72,0.16),transparent_44%),rgba(255,255,255,0.035)] text-[10px] font-black uppercase tracking-[0.18em] text-foreground/28 dark:text-white/24 sm:h-[140px]">
        Awaiting Proof
      </div>
    );
  }

  if (proofPosts.length === 1) {
    return (
      <div className="grid h-[124px] grid-cols-1 sm:h-[140px]">
        <ProofTile post={proofPosts[0]} large />
      </div>
    );
  }

  return (
    <div className="grid h-[124px] grid-cols-4 grid-rows-2 gap-1.5 sm:h-[140px]">
      {proofPosts.map((post, index) => (
        <ProofTile
          key={`${post.post_key}:${post.thumbnail_url || ''}:${index}`}
          post={post}
          large={index === 0 && proofPosts.length >= 3}
        />
      ))}
    </div>
  );
}

function FeaturedPreviewLayer({
  asset,
  animateIn = true,
  onAnimationComplete,
  onUnavailable,
}: {
  asset: PreviewAsset;
  animateIn?: boolean;
  onAnimationComplete?: () => void;
  onUnavailable?: () => void;
}) {
  const [src, setSrc] = useState(asset.src);
  const [dead, setDead] = useState(false);
  const reportedUnavailable = useRef(false);

  const markUnavailable = () => {
    setDead(true);
    if (reportedUnavailable.current) return;
    reportedUnavailable.current = true;
    onUnavailable?.();
  };

  return (
    <motion.div
      initial={animateIn ? { opacity: 0.7, scale: 1.025, filter: 'blur(24px)' } : false}
      animate={{ opacity: 1, scale: 1, filter: 'blur(0px)' }}
      transition={{ duration: 2.75, ease: [0.16, 1, 0.3, 1] }}
      onAnimationComplete={onAnimationComplete}
      className="absolute inset-0"
    >
      {src && !dead ? (
        // eslint-disable-next-line @next/next/no-img-element -- dashboard thumbnails are dynamic feed assets
        <img
          src={src}
          alt=""
          className="h-full w-full object-cover"
          loading="eager"
          fetchPriority="high"
          decoding="async"
          onError={() => {
            if (asset.fallbackSrc && src !== asset.fallbackSrc) {
              setSrc(asset.fallbackSrc);
              return;
            }
            markUnavailable();
          }}
        />
      ) : (
        <div className="h-full w-full" />
      )}
    </motion.div>
  );
}

function FeaturedPreview({
  post,
  onImageUnavailable,
}: {
  post: PatternBoardSupportPost | null;
  onImageUnavailable?: () => void;
}) {
  const targetAsset = useMemo(
    () => previewAsset(post),
    [post],
  );
  const [visibleAsset, setVisibleAsset] = useState(targetAsset);
  const [incomingAsset, setIncomingAsset] = useState<PreviewAsset | null>(null);

  useEffect(() => {
    if (targetAsset.key === visibleAsset.key || typeof window === 'undefined') return;

    let cancelled = false;
    const revealAsset = (asset: PreviewAsset) => {
      if (cancelled) return;
      setIncomingAsset(asset);
    };

    if (!targetAsset.src) {
      revealAsset(targetAsset);
      return () => {
        cancelled = true;
      };
    }

    const primaryImage = new window.Image();
    primaryImage.onload = () => revealAsset(targetAsset);
    primaryImage.onerror = () => {
      const fallbackAsset = previewAsset(targetAsset.post, true);
      if (!fallbackAsset.src || fallbackAsset.src === targetAsset.src) {
        revealAsset({ ...targetAsset, src: '' });
        return;
      }

      const fallbackImage = new window.Image();
      fallbackImage.onload = () => revealAsset(fallbackAsset);
      fallbackImage.onerror = () => revealAsset({ ...fallbackAsset, src: '' });
      fallbackImage.src = fallbackAsset.src;
    };
    primaryImage.src = targetAsset.src;

    return () => {
      cancelled = true;
    };
  }, [targetAsset, visibleAsset.key]);

  return (
    <div className="relative h-full min-h-0 overflow-hidden rounded-[22px] border-0 bg-[radial-gradient(circle_at_28%_20%,rgba(225,29,72,0.18),transparent_42%),linear-gradient(135deg,rgba(255,255,255,0.08),rgba(255,255,255,0.025))] outline-none ring-0 lg:rounded-[24px]">
      <FeaturedPreviewLayer
        key={visibleAsset.key}
        asset={visibleAsset}
        animateIn={false}
        onUnavailable={onImageUnavailable}
      />
      {incomingAsset && incomingAsset.key !== visibleAsset.key && (
        <FeaturedPreviewLayer
          key={incomingAsset.key}
          asset={incomingAsset}
          onUnavailable={onImageUnavailable}
          onAnimationComplete={() => {
            setVisibleAsset(incomingAsset);
            setIncomingAsset(null);
          }}
        />
      )}
      {post && (
        <div className="pointer-events-none absolute inset-x-0 top-0 flex items-start justify-end gap-3 bg-[linear-gradient(180deg,rgba(0,0,0,0.58)_0%,transparent_100%)] px-3 py-3">
          <span className="rounded-full border border-white/[0.08] bg-black/34 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-white/76">
            @{(post.handle || 'feed').replace(/^@+/, '')}
          </span>
        </div>
      )}
    </div>
  );
}

function RailThumbnail({
  post,
  active,
  index,
  onClick,
}: {
  post: PatternBoardSupportPost;
  active: boolean;
  index: number;
  onClick: () => void;
}) {
  const fallback = mediaProxyUrl(post.post_key);
  const [src, setSrc] = useState(() => post.thumbnail_url || fallback);
  const [dead, setDead] = useState(false);
  const handle = (post.handle || 'feed').replace(/^@+/, '').toUpperCase();
  const label = evidenceLabel(post);

  return (
    <motion.button
      type="button"
      layout
      data-rail-index={index}
      onClick={onClick}
      className={[
        'group relative h-[70px] min-w-[58px] overflow-hidden rounded-[14px] border bg-white/[0.04] transition duration-300 lg:h-[62px] xl:h-[70px]',
        active
          ? 'border-[#FB7185]/48 opacity-100 shadow-[0_12px_28px_rgba(225,29,72,0.16)]'
          : 'border-white/[0.08] opacity-58 hover:opacity-100',
      ].join(' ')}
      animate={{ scale: active ? 1 : 0.965 }}
      transition={{ duration: 0.34, ease: [0.22, 1, 0.36, 1] }}
      aria-label={`Show @${handle} support post`}
    >
      {active && (
        <motion.div
          layoutId="support-rail-active"
          className="pointer-events-none absolute inset-0 z-20 rounded-[14px] ring-2 ring-[#FB7185]/72 ring-offset-2 ring-offset-[#080808]"
          transition={{ type: 'spring', stiffness: 360, damping: 34, mass: 0.8 }}
        />
      )}
      {src && !dead ? (
        // eslint-disable-next-line @next/next/no-img-element -- dashboard thumbnails are dynamic feed assets
        <img
          src={src}
          alt=""
          className="h-full w-full object-cover transition duration-300 group-hover:scale-[1.04]"
          onError={() => {
            if (fallback && src !== fallback) {
              setSrc(fallback);
              return;
            }
            setDead(true);
          }}
        />
      ) : (
        <div className="h-full w-full bg-[radial-gradient(circle_at_28%_24%,rgba(225,29,72,0.22),transparent_42%),rgba(255,255,255,0.035)]" />
      )}
      <div className="pointer-events-none absolute inset-x-0 bottom-0 z-10 flex min-w-0 items-center gap-1 bg-[linear-gradient(180deg,transparent_0%,rgba(0,0,0,0.84)_100%)] px-1.5 pb-1 pt-5">
        {label && (
          <span className={[
            'shrink-0 rounded-full border px-1 py-0.5 text-[5px] font-black uppercase tracking-[0.08em] lg:text-[5.75px]',
            evidenceToneClasses(post),
          ].join(' ')}
          >
            {label}
          </span>
        )}
        <div className="min-w-0 truncate text-[6px] font-black uppercase tracking-[0.1em] text-white/82">
          @{handle}
        </div>
      </div>
    </motion.button>
  );
}

function SupportPostRail({
  posts,
  selectedIndex,
  onSelect,
}: {
  posts: PatternBoardSupportPost[];
  selectedIndex: number;
  onSelect: (index: number) => void;
}) {
  const railRef = useRef<HTMLDivElement | null>(null);

  useEffect(() => {
    const target = railRef.current?.querySelector<HTMLElement>(`[data-rail-index="${selectedIndex}"]`);
    target?.scrollIntoView({ behavior: 'smooth', block: 'nearest', inline: 'center' });
  }, [selectedIndex]);

  if (posts.length === 0) return null;
  return (
    <div className="min-w-0">
      <div className="mb-1.5 flex items-center justify-between gap-3 xl:mb-2">
        <div className="text-[8px] font-black uppercase tracking-[0.18em] text-white/28">
          Supporting posts
        </div>
        <div className="text-[8px] font-black uppercase tracking-[0.16em] text-white/22">
          {selectedIndex + 1}/{posts.length}
        </div>
      </div>
      <div
        ref={railRef}
        className="hide-scrollbar grid overflow-x-auto pb-1"
        style={{
          gridTemplateColumns: `repeat(${posts.length}, minmax(58px, 1fr))`,
          gap: '8px',
        }}
      >
        {posts.map((post, index) => (
          <RailThumbnail
            key={`${post.post_key}:${post.thumbnail_url || ''}:${index}`}
            post={post}
            active={index === selectedIndex}
            index={index}
            onClick={() => onSelect(index)}
          />
        ))}
      </div>
    </div>
  );
}

function DetailSection({
  label,
  children,
  tone = 'default',
  className = '',
}: {
  label: string;
  children: ReactNode;
  tone?: 'default' | 'action' | 'warning';
  className?: string;
}) {
  const isAction = tone === 'action';
  const isWarning = tone === 'warning';
  const bodyClass = isAction
    ? 'mt-2 break-words text-[18px] font-black leading-[1.16] tracking-normal text-white/88 sm:text-[19px] md:text-[20px] lg:text-[22px] xl:text-[24px] [text-wrap:balance]'
    : 'mt-2 break-words text-[13px] font-semibold leading-relaxed text-white/76 sm:text-[14px] md:text-[15px] md:leading-[1.48] lg:text-[16px] xl:text-[17px]';

  return (
    <motion.section
      animate={isAction
        ? {
          borderColor: ['rgba(225,29,72,0.24)', 'rgba(251,113,133,0.36)', 'rgba(225,29,72,0.24)'],
          boxShadow: [
            'inset 0 1px 0 rgba(255,255,255,0.03), 0 0 0 rgba(225,29,72,0)',
            'inset 0 1px 0 rgba(255,255,255,0.04), 0 0 28px rgba(225,29,72,0.08)',
            'inset 0 1px 0 rgba(255,255,255,0.03), 0 0 0 rgba(225,29,72,0)',
          ],
        }
        : undefined}
      transition={isAction ? { duration: 4.2, repeat: Infinity, ease: 'easeInOut' } : undefined}
      className={[
        'fm-signal-scroll-tile relative h-auto rounded-[18px] border px-4 py-3.5 md:rounded-[16px] md:px-4 md:py-3.5 lg:px-5 lg:py-4 xl:px-6 xl:py-5',
        isAction
          ? 'border-[#E11D48]/24 bg-[#E11D48]/12'
          : isWarning
            ? 'border-white/[0.08] bg-white/[0.045]'
            : 'border-white/[0.07] bg-white/[0.035]',
        className,
      ].join(' ')}
    >
      <div className={isAction
        ? 'text-[8px] font-black uppercase tracking-[0.18em] text-[#FDA4AF] md:text-[9px] xl:text-[9.5px]'
        : 'text-[8px] font-black uppercase tracking-[0.18em] text-white/30 md:text-[9px] xl:text-[9.5px]'}
      >
        {label}
      </div>
      <div className={bodyClass}>
        {children}
      </div>
    </motion.section>
  );
}

function SignalInsightDialog({
  pattern,
  onClose,
}: {
  pattern: PatternBoardItem | null;
  onClose: () => void;
}) {
  const card: PatternBoardSignalCard | null = pattern?.signal_card ?? null;
  const patternId = pattern?.firewatch_id ?? null;
  const supportPosts = useMemo(() => pattern?.support_posts ?? [], [pattern?.support_posts]);
  const [selectedPostSelection, setSelectedPostSelection] = useState<{ patternId: string | null; index: number }>({
    patternId: null,
    index: 0,
  });
  const rawSelectedPostIndex = selectedPostSelection.patternId === patternId ? selectedPostSelection.index : 0;
  const selectedPostIndex = supportPosts.length > 0
    ? Math.min(rawSelectedPostIndex, supportPosts.length - 1)
    : 0;
  const selectedPost = supportPosts[selectedPostIndex] ?? supportPosts[0] ?? null;
  const title = card?.title || pattern?.pattern_label || readableSignalCode(pattern?.signal_code);
  const metrics = pattern ? proofMetrics(pattern) : [];
  const confidence = normalizedConfidence(card?.confidence);
  const metricLine = card?.metric_line || '';
  const selectedPostRole = readablePostRole(selectedPost?.post_role);
  const selectedEvidenceSummary = evidenceSummary(selectedPost);
  const failedPreviewKeys = useRef<Set<string>>(new Set());
  const contentPanelRef = useRef<HTMLDivElement | null>(null);

  const setSelectedPostIndex = useCallback((nextIndex: number | ((currentIndex: number) => number)) => {
    setSelectedPostSelection((current) => {
      const currentIndex = current.patternId === patternId ? current.index : 0;
      const index = typeof nextIndex === 'function' ? nextIndex(currentIndex) : nextIndex;
      return { patternId, index };
    });
  }, [patternId]);

  const handlePreviewUnavailable = useCallback(() => {
    if (!selectedPost?.post_key || supportPosts.length <= 1) return;
    failedPreviewKeys.current.add(selectedPost.post_key);
    const replacementIndex = supportPosts.findIndex((post, index) => (
      index !== selectedPostIndex && !failedPreviewKeys.current.has(post.post_key)
    ));
    if (replacementIndex >= 0) setSelectedPostIndex(replacementIndex);
  }, [selectedPost, selectedPostIndex, setSelectedPostIndex, supportPosts]);

  useEffect(() => {
    if (!pattern || supportPosts.length <= 1 || typeof window === 'undefined') return;
    const timer = window.setInterval(() => {
      setSelectedPostIndex((current) => (current + 1) % supportPosts.length);
    }, 6200);
    return () => window.clearInterval(timer);
  }, [pattern, setSelectedPostIndex, supportPosts.length]);

  useEffect(() => {
    if (!pattern) return;
    preloadPreviewAssets(supportPosts, 4);
  }, [pattern, supportPosts]);

  useEffect(() => {
    failedPreviewKeys.current.clear();
    if (patternId == null || typeof window === 'undefined') return;
    const frame = window.requestAnimationFrame(() => {
      contentPanelRef.current?.scrollTo({ top: 0, left: 0, behavior: 'auto' });
    });
    return () => window.cancelAnimationFrame(frame);
  }, [patternId]);

  useEffect(() => {
    if (!pattern || typeof window === 'undefined') return;
    const previousOverflow = document.documentElement.style.overflow;
    document.documentElement.style.overflow = 'hidden';
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.documentElement.style.overflow = previousOverflow;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [onClose, pattern]);

  const dialog = (
    <AnimatePresence>
      {pattern && (
        <motion.div
          className="fixed inset-0 z-[320] flex items-end justify-center px-0 sm:items-center sm:px-3 sm:py-3 md:px-4 md:py-4 lg:px-5 lg:py-5"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.2, ease: [0.22, 1, 0.36, 1] }}
        >
          <motion.div
            className="absolute inset-0 bg-black/82"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            onClick={onClose}
          />

          <motion.div
            role="dialog"
            aria-modal="true"
            aria-label={title}
            initial={{ y: '100%', opacity: 0.96 }}
            animate={{ y: 0, opacity: 1 }}
            exit={{ y: '100%', opacity: 0.96 }}
            transition={{ type: 'spring', stiffness: 260, damping: 32, mass: 0.95 }}
            onClick={(event) => event.stopPropagation()}
            className="relative flex h-[var(--signal-dialog-height)] w-full flex-col overflow-hidden rounded-t-[28px] border border-white/[0.08] bg-[#080808] text-white shadow-[0_-18px_60px_rgba(0,0,0,0.55)] sm:grid sm:w-[min(1120px,calc(100vw-24px))] sm:grid-cols-[minmax(220px,0.78fr)_minmax(0,1.22fr)] sm:rounded-[24px] sm:shadow-[0_32px_90px_rgba(0,0,0,0.72)] md:w-[min(1180px,calc(100vw-40px))] md:grid-cols-[minmax(300px,0.78fr)_minmax(0,1.22fr)] md:rounded-[28px] lg:w-[min(1500px,calc(100vw-64px))] lg:grid-cols-[minmax(410px,500px)_minmax(0,1fr)] xl:w-[min(1600px,calc(100vw-80px))] xl:grid-cols-[minmax(450px,540px)_minmax(0,1fr)]"
            style={{
              maxHeight: 'min(790px, calc(100dvh - 56px - env(safe-area-inset-top)))',
              '--signal-dialog-height': 'min(790px, calc(100dvh - 56px - env(safe-area-inset-top)))',
            } as CSSProperties}
          >
            <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-[#E11D48]/42 to-transparent" />
            <div className="mx-auto mt-3 h-1.5 w-12 rounded-full bg-white/14 md:hidden" />

            <button
              type="button"
              onClick={onClose}
              className="absolute right-4 top-4 z-20 flex h-9 w-9 items-center justify-center rounded-full border border-white/[0.08] bg-white/[0.06] text-white/54 transition hover:bg-white/[0.1] hover:text-white/82 md:right-5 md:top-5"
              aria-label="Close signal card"
            >
              <X size={16} strokeWidth={2.4} />
            </button>

            <div className="flex shrink-0 flex-col justify-center gap-3 px-5 pb-4 pt-5 sm:min-h-0 sm:shrink sm:p-4 md:p-5 lg:gap-4 xl:p-6">
              <div className="flex min-h-0 items-center justify-center">
                <div className="aspect-[4/5] w-[min(62vw,236px)] max-w-full sm:w-full sm:max-w-[292px] md:max-w-[330px] lg:max-w-[420px] xl:max-w-[455px]">
                  <FeaturedPreview post={selectedPost} onImageUnavailable={handlePreviewUnavailable} />
                </div>
              </div>
              {selectedPost && (
                <div className="flex items-center justify-between gap-3 px-1">
                  <div className="min-w-0">
                    <div className="truncate text-[10px] font-black uppercase tracking-[0.18em] text-white/32 xl:text-[11px]">
                      Selected proof
                    </div>
                    <div className="mt-1 truncate text-[16px] font-bold text-white/72 lg:text-[17px] xl:text-[18px]">
                      {selectedEvidenceSummary || selectedPostRole || selectedPost.media_type || 'Post read'}
                    </div>
                    {selectedEvidenceSummary && selectedPostRole && (
                      <div className="mt-0.5 truncate text-[12px] font-bold text-white/38 xl:text-[13px]">
                        {selectedPostRole}
                      </div>
                    )}
                  </div>
                  <div className="flex shrink-0 items-center gap-2">
                    <span className="rounded-full border border-white/[0.08] bg-white/[0.055] px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-white/48">
                      {selectedPostIndex + 1}/{supportPosts.length}
                    </span>
                    {evidenceLabel(selectedPost) && (
                      <span className={[
                        'rounded-full border px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.16em] xl:text-[9px]',
                        evidenceToneClasses(selectedPost),
                      ].join(' ')}
                      >
                        {evidenceLabel(selectedPost)}
                      </span>
                    )}
                  </div>
                </div>
              )}
              <SupportPostRail
                posts={supportPosts}
                selectedIndex={selectedPostIndex}
                onSelect={setSelectedPostIndex}
              />
            </div>

            <div className="relative min-h-0 flex-1 overflow-hidden sm:h-full">
              <div
                ref={contentPanelRef}
                className="hide-scrollbar h-full min-h-0 touch-pan-y overflow-y-auto overscroll-y-contain px-5 pb-[calc(18px+env(safe-area-inset-bottom))] pt-5 sm:flex sm:flex-col sm:overflow-y-auto sm:px-5 sm:py-5 md:px-6 md:py-6 lg:px-8 lg:py-7 xl:px-9 xl:py-8"
              >
                <div className="shrink-0 pr-10 md:pr-12">
                  <div className="flex flex-wrap items-center gap-2 sm:gap-1.5">
                    <span className="text-[9px] font-black uppercase tracking-[0.2em] text-[#E11D48]/70 xl:text-[10px]">
                      Feed_Me read
                    </span>
                    {confidence && (
                      <span className="rounded-full border border-[#E11D48]/18 bg-[#E11D48]/10 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.16em] text-[#FDA4AF]/82 md:px-2.5 md:py-1 md:text-[8px]">
                        {confidence}
                      </span>
                    )}
                  </div>
                  <h3 className="mt-3 max-w-[calc(100%-10px)] text-[30px] font-black leading-[1.03] tracking-normal text-white [text-wrap:balance] sm:w-full sm:max-w-[min(760px,calc(100%-56px))] sm:text-[28px] sm:leading-[1.02] md:mt-3 md:max-w-[min(920px,calc(100%-64px))] md:text-[34px] lg:text-[46px] lg:leading-[1] xl:max-w-[min(1040px,calc(100%-70px))] xl:text-[54px]">
                    {title}
                  </h3>
                  {metricLine && (
                    <div className="mt-3 max-w-[min(880px,calc(100%-60px))] text-[12px] font-black uppercase tracking-[0.13em] text-white/38 md:text-[13px] xl:text-[14px]">
                      {metricLine}
                    </div>
                  )}
                </div>

              {metrics.length > 0 && (
                <div className="mt-4 grid shrink-0 grid-cols-3 gap-1.5 sm:mt-5 sm:gap-2 xl:gap-2.5">
                  {metrics.map((metric) => (
                    <div
                      key={`${pattern.firewatch_id}:${metric.label}`}
                      className="min-w-0 rounded-[12px] border border-white/[0.06] bg-white/[0.035] px-2.5 py-2 sm:px-3 sm:py-2.5 md:px-4 md:py-3 lg:px-5 lg:py-4 xl:px-5 xl:py-4"
                    >
                      <div className="truncate text-[7px] font-black uppercase tracking-[0.16em] text-white/28 md:text-[9px] xl:text-[10px]">
                        {metric.label}
                      </div>
                      <div className="mt-1 truncate text-[18px] font-black tracking-normal text-white/92 sm:text-[24px] md:text-[30px] lg:text-[40px] xl:text-[44px]">
                        {metric.value}
                      </div>
                    </div>
                  ))}
                </div>
              )}

              <div className="mt-5 grid gap-2.5 md:mt-5 md:grid-cols-1 md:items-start xl:grid-cols-12 xl:gap-3">
                {card?.read && (
                  <DetailSection label="Read" className="xl:col-span-12">
                    {card.read}
                  </DetailSection>
                )}
                {card?.evidence_pressure?.length ? (
                  <DetailSection label="Evidence pressure" className="xl:col-span-12">
                    <div className="space-y-2 md:space-y-1.5">
                      {card.evidence_pressure.slice(0, 5).map((note, index) => (
                        <div key={`${pattern.firewatch_id}:note:${index}`} className="flex gap-2">
                          <span className="mt-[0.45em] h-1.5 w-1.5 shrink-0 rounded-full bg-[#E11D48]/70" />
                          <span>{note}</span>
                        </div>
                      ))}
                    </div>
                  </DetailSection>
                ) : null}
              </div>
            </div>
            </div>
          </motion.div>
        </motion.div>
      )}
    </AnimatePresence>
  );

  if (typeof document === 'undefined') return null;
  return createPortal(dialog, document.body);
}

function PatternCard({
  pattern,
  selected,
  pulse,
  onSelect,
}: {
  pattern: PatternBoardItem;
  selected: boolean;
  pulse: boolean;
  onSelect: (pattern: PatternBoardItem) => void;
}) {
  const headline = pickHeadlineNumber(pattern);
  const cues = (pattern.cues || []).slice(0, 3);
  const pills = patternPills(pattern);
  const cardTitle = pattern.signal_card?.title || pattern.pattern_label || 'Pattern';
  const metricLine = pattern.signal_card?.metric_line || '';
  const readPreview = pattern.signal_card?.read || '';

  const selectCard = () => onSelect(pattern);

  return (
    <motion.div
      role="button"
      tabIndex={0}
      onClick={selectCard}
      onKeyDown={(event) => {
        if (event.key === 'Enter' || event.key === ' ') {
          event.preventDefault();
          selectCard();
        }
      }}
      initial={{ opacity: 0, y: 5 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.24, ease: [0.22, 1, 0.36, 1] }}
      className={[
        'group relative min-w-0 overflow-hidden rounded-[22px] p-2.5 text-left tabular-nums outline-none transition',
        'border border-black/[0.05] bg-white/85 ring-1 ring-inset ring-white/50',
        'shadow-[inset_0_1px_0_rgba(255,255,255,0.74),0_18px_36px_-26px_rgba(15,23,42,0.2)]',
        'dark:border-white/[0.07] dark:bg-white/[0.035] dark:ring-white/[0.045]',
        'dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.055),0_22px_40px_-28px_rgba(0,0,0,0.68)]',
        selected ? 'ring-[rgba(225,29,72,0.32)] dark:ring-[rgba(225,29,72,0.34)]' : '',
        pulse ? 'fm-firewatch-hero-pulse' : '',
      ].filter(Boolean).join(' ')}
    >
      <div className="pointer-events-none absolute inset-x-0 top-0 h-20 bg-[radial-gradient(circle_at_22%_0%,rgba(225,29,72,0.16),transparent_50%)] opacity-70" />

      <div className="relative">
        <ProofMosaic posts={pattern.support_posts || []} />

        <div className="mt-3 flex items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="line-clamp-2 text-[18px] font-black leading-[1.02] tracking-normal text-foreground dark:text-white sm:text-[20px]">
              {cardTitle}
            </div>
            {metricLine && (
              <div className="mt-1.5 line-clamp-1 text-[9px] font-black uppercase tracking-[0.12em] text-foreground/42 dark:text-white/34">
                {metricLine}
              </div>
            )}
            {readPreview && (
              <div className="mt-2 line-clamp-3 text-[12px] font-semibold leading-snug text-foreground/58 dark:text-white/50 sm:text-[13px]">
                {readPreview}
              </div>
            )}
            {cues.length > 0 && (
              <div className="mt-1.5 flex min-w-0 flex-wrap gap-1.5">
                {cues.map((cue) => (
                  <span
                    key={`${pattern.firewatch_id}:${cue}`}
                    className="max-w-full truncate rounded-full border border-black/[0.05] bg-black/[0.035] px-2 py-1 text-[8px] font-black uppercase tracking-[0.13em] text-foreground/52 dark:border-white/[0.08] dark:bg-white/[0.045] dark:text-white/44"
                  >
                    {cue}
                  </span>
                ))}
              </div>
            )}
          </div>

          <div className="shrink-0 text-right">
            <div className="text-[9px] font-black uppercase tracking-[0.18em] text-foreground/42 dark:text-white/34">
              {familyTag(pattern.context)}
            </div>
            {headline && (
              <>
                <div className="mt-2 text-[26px] font-black leading-none tracking-[-0.04em] text-foreground dark:text-white">
                  {headline.value}
                </div>
                <div className="mt-1 text-[8px] font-black uppercase tracking-[0.18em] text-foreground/46 dark:text-white/34">
                  {headline.qualifier}
                </div>
              </>
            )}
          </div>
        </div>

        {pills.length > 0 && (
          <div className="mt-3 grid grid-cols-3 gap-1.5">
            {pills.map((pill) => (
              <div
                key={`${pattern.firewatch_id}:${pill.label}`}
                className="min-w-0 rounded-[12px] border border-black/[0.04] bg-white/44 px-2.5 py-2 shadow-[inset_0_1px_0_rgba(255,255,255,0.58)] dark:border-white/[0.055] dark:bg-white/[0.032] dark:shadow-[inset_0_1px_0_rgba(255,255,255,0.04)]"
              >
                <div className="truncate text-[7px] font-black uppercase tracking-[0.16em] text-foreground/34 dark:text-white/28 sm:text-[7.5px]">
                  {pill.label}
                </div>
                <div className="mt-0.5 truncate text-[15px] font-black tracking-normal text-foreground/88 dark:text-white/80 sm:text-[16px]">
                  {pill.value}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </motion.div>
  );
}

export default function FeedPatternBoard({ patterns }: FeedPatternBoardProps) {
  const searchParams = useSearchParams();
  const firewatchParam = searchParams?.get('firewatch') ?? null;
  const sectionRef = useRef<HTMLDivElement | null>(null);
  const [activePattern, setActivePattern] = useState<PatternBoardItem | null>(null);
  const [pulseCard, setPulseCard] = useState(false);

  const linkedPatternId = useMemo(() => {
    if (!firewatchParam) return null;
    return patterns.some((entry) => entry.firewatch_id === firewatchParam) ? firewatchParam : null;
  }, [firewatchParam, patterns]);

  useEffect(() => {
    if (!linkedPatternId) return;
    let pulseFrame: number | null = null;
    const frame = requestAnimationFrame(() => {
      sectionRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
      setPulseCard(false);
      pulseFrame = requestAnimationFrame(() => setPulseCard(true));
    });
    const clearPulse = window.setTimeout(() => setPulseCard(false), 900);
    return () => {
      cancelAnimationFrame(frame);
      if (pulseFrame != null) cancelAnimationFrame(pulseFrame);
      window.clearTimeout(clearPulse);
    };
  }, [linkedPatternId]);

  const selectedId = activePattern?.firewatch_id || linkedPatternId;

  useEffect(() => {
    for (const pattern of patterns.slice(0, 8)) {
      preloadPreviewAssets(pattern.support_posts || [], 2);
    }
  }, [patterns]);

  const openPattern = useCallback((pattern: PatternBoardItem) => {
    const supportPostsForPattern = pattern.support_posts || [];
    preloadPreviewAssets(supportPostsForPattern, 4);

    if (typeof window === 'undefined') {
      setActivePattern(pattern);
      return;
    }

    const firstAsset = previewAsset(supportPostsForPattern[0] || null);
    const firstSrc = firstAsset.src || firstAsset.fallbackSrc;
    if (!firstSrc) {
      setActivePattern(pattern);
      return;
    }

    let opened = false;
    const open = () => {
      if (opened) return;
      opened = true;
      setActivePattern(pattern);
    };
    const image = new window.Image();
    image.onload = open;
    image.onerror = open;
    image.src = firstSrc;
    window.setTimeout(open, 520);
  }, []);

  if (patterns.length === 0) {
    return (
      <div className="fm-feed-mobile-panel min-w-0" ref={sectionRef} id="firewatch-section">
        <div className="flex items-baseline justify-between gap-3">
          <div className="text-[10px] font-black uppercase tracking-[0.18em] text-foreground/40 dark:text-white/34">
            Firewatch
          </div>
          <div className="text-[9px] font-bold uppercase tracking-[0.16em] text-foreground/30 dark:text-white/24">
            awaiting D7
          </div>
        </div>
      </div>
    );
  }

  return (
    <div
      className="fm-feed-mobile-panel min-w-0"
      ref={sectionRef}
      id="firewatch-section"
      data-firewatch-active={selectedId || ''}
    >
      <div className="flex items-baseline justify-between gap-3">
        <div className="text-[10px] font-black uppercase tracking-[0.18em] text-foreground/44 dark:text-white/36">
          Firewatch
        </div>
        <div className="text-[9px] font-bold uppercase tracking-[0.16em] text-foreground/34 dark:text-white/26">
          {patterns.length} active
        </div>
      </div>

      <div className="mt-3 grid grid-cols-1 gap-2.5 md:grid-cols-2 2xl:grid-cols-3">
        {patterns.map((pattern) => (
          <PatternCard
            key={pattern.firewatch_id}
            pattern={pattern}
            selected={pattern.firewatch_id === selectedId}
            pulse={pulseCard && pattern.firewatch_id === linkedPatternId}
            onSelect={openPattern}
          />
        ))}
      </div>

      <SignalInsightDialog
        key={activePattern?.firewatch_id || 'closed'}
        pattern={activePattern}
        onClose={() => setActivePattern(null)}
      />
    </div>
  );
}
