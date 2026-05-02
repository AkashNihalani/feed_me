'use client';

import { type ReactNode, useEffect, useMemo, useRef, useState } from 'react';
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
  post: PatternBoardSupportPost | null;
};

function toFiniteNumber(value: unknown): number | null {
  const numeric = typeof value === 'number' ? value : Number(value);
  return Number.isFinite(numeric) ? numeric : null;
}

function familyTag(context: PatternBoardItem['context']): string {
  if (context === 'cross') return 'CRS';
  if (context === 'anchor') return 'ANC';
  return 'OWN';
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
    post,
  };
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
  const triggerCount = toFiniteNumber(pattern.trigger_count);

  return [
    matchCount != null && matchCount > 0 ? { label: 'Winners', value: compactNumber(matchCount) } : null,
    feedersCount != null && feedersCount > 1 ? { label: 'Spread', value: compactNumber(feedersCount) } : null,
    avgPctile != null ? { label: 'Avg', value: `Top ${Math.round(avgPctile)}%` } : null,
    triggerCount != null && triggerCount > 0 ? { label: 'Hits', value: compactNumber(triggerCount) } : null,
  ].filter((entry): entry is { label: string; value: string } => Boolean(entry)).slice(0, 3);
}

function readableSignalCode(value: string | null | undefined): string {
  return String(value || 'Signal')
    .replace(/_/g, ' ')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function readablePatternType(value: string | null | undefined): string {
  return String(value || '')
    .replace(/_/g, ' ')
    .toLowerCase()
    .replace(/\b\w/g, (char) => char.toUpperCase());
}

function normalizedConfidence(value: string | null | undefined): string {
  const normalized = String(value || '').trim().toLowerCase();
  if (normalized === 'high' || normalized === 'medium' || normalized === 'low') return normalized;
  return '';
}

function proofMetrics(pattern: PatternBoardItem): Array<{ label: string; value: string }> {
  const avgPctile = toFiniteNumber(pattern.avg_hot_percentile);
  const matchCount = toFiniteNumber(pattern.match_count);
  const feedersCount = toFiniteNumber(pattern.feeders_count);
  const triggerCount = toFiniteNumber(pattern.trigger_count);
  const recentLift = toFiniteNumber(pattern.recent_lift);
  const anchorGap = toFiniteNumber(pattern.anchor_gap);

  return [
    avgPctile != null ? { label: 'Avg top', value: `${Math.round(avgPctile)}%` } : null,
    matchCount != null && matchCount > 0 ? { label: 'Matches', value: compactNumber(matchCount) } : null,
    feedersCount != null && feedersCount > 1 ? { label: 'Feeders', value: compactNumber(feedersCount) } : null,
    recentLift != null && recentLift > 0 ? { label: 'Lift', value: `${recentLift.toFixed(1)}×` } : null,
    anchorGap != null && anchorGap > 0 ? { label: 'Gap', value: `+${Math.round(anchorGap)}` } : null,
    triggerCount != null && triggerCount > 0 ? { label: 'Hits', value: compactNumber(triggerCount) } : null,
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
      <div className="pointer-events-none absolute inset-x-0 bottom-0 bg-[linear-gradient(180deg,transparent_0%,rgba(0,0,0,0.7)_100%)] px-2 pb-1.5 pt-5">
        <div className="truncate text-[8px] font-black uppercase tracking-[0.12em] text-white/86">
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
  onAnimationComplete,
}: {
  asset: PreviewAsset;
  onAnimationComplete?: () => void;
}) {
  return (
    <motion.div
      initial={{ opacity: 0.7, scale: 1.025, filter: 'blur(24px)' }}
      animate={{ opacity: 1, scale: 1, filter: 'blur(0px)' }}
      transition={{ duration: 2.75, ease: [0.16, 1, 0.3, 1] }}
      onAnimationComplete={onAnimationComplete}
      className="absolute inset-0"
    >
      {asset.src ? (
        // eslint-disable-next-line @next/next/no-img-element -- dashboard thumbnails are dynamic feed assets
        <img
          src={asset.src}
          alt=""
          className="h-full w-full object-cover"
        />
      ) : (
        <div className="h-full w-full" />
      )}
    </motion.div>
  );
}

function FeaturedPreview({ post }: { post: PatternBoardSupportPost | null }) {
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
      <FeaturedPreviewLayer asset={visibleAsset} />
      {incomingAsset && incomingAsset.key !== visibleAsset.key && (
        <FeaturedPreviewLayer
          key={incomingAsset.key}
          asset={incomingAsset}
          onAnimationComplete={() => {
            setVisibleAsset(incomingAsset);
            setIncomingAsset(null);
          }}
        />
      )}
    </div>
  );
}

function RailThumbnail({
  post,
  active,
  onClick,
}: {
  post: PatternBoardSupportPost;
  active: boolean;
  onClick: () => void;
}) {
  const fallback = mediaProxyUrl(post.post_key);
  const [src, setSrc] = useState(() => post.thumbnail_url || fallback);
  const [dead, setDead] = useState(false);
  const handle = (post.handle || 'feed').replace(/^@+/, '').toUpperCase();

  return (
    <button
      type="button"
      onClick={onClick}
      className={[
        'group relative h-[66px] w-[58px] shrink-0 overflow-hidden rounded-[14px] border bg-white/[0.04] transition lg:h-[58px] lg:w-[50px] xl:h-[66px] xl:w-[58px]',
        active
          ? 'border-[#FB7185]/58 shadow-[0_0_0_1px_rgba(225,29,72,0.22),0_10px_26px_rgba(225,29,72,0.16)]'
          : 'border-white/[0.08] opacity-62 hover:opacity-100',
      ].join(' ')}
      aria-label={`Show @${handle} support post`}
    >
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
      <div className="pointer-events-none absolute inset-x-0 bottom-0 bg-[linear-gradient(180deg,transparent_0%,rgba(0,0,0,0.82)_100%)] px-1.5 pb-1 pt-5">
        <div className="truncate text-[6px] font-black uppercase tracking-[0.1em] text-white/82">
          @{handle}
        </div>
      </div>
    </button>
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
      <div className="hide-scrollbar flex gap-2 overflow-x-auto pb-0.5">
        {posts.map((post, index) => (
          <RailThumbnail
            key={`${post.post_key}:${post.thumbnail_url || ''}:${index}`}
            post={post}
            active={index === selectedIndex}
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
}: {
  label: string;
  children: ReactNode;
  tone?: 'default' | 'action' | 'warning';
}) {
  return (
    <section
      className={[
        'min-h-0 rounded-[18px] border px-4 py-3.5 lg:overflow-hidden lg:rounded-[15px] lg:px-3.5 lg:py-3 xl:rounded-[16px] xl:px-4 xl:py-3.5',
        tone === 'action'
          ? 'border-[#E11D48]/24 bg-[#E11D48]/12'
          : tone === 'warning'
            ? 'border-white/[0.08] bg-white/[0.045]'
            : 'border-white/[0.07] bg-white/[0.035]',
      ].join(' ')}
    >
      <div className={tone === 'action'
        ? 'text-[8px] font-black uppercase tracking-[0.18em] text-[#FDA4AF] lg:text-[7px]'
        : 'text-[8px] font-black uppercase tracking-[0.18em] text-white/30 lg:text-[7px]'}
      >
        {label}
      </div>
      <div className="mt-2 text-[13px] font-semibold leading-relaxed text-white/76 sm:text-[14px] lg:text-[12px] lg:leading-[1.45] xl:text-[13px]">
        {children}
      </div>
    </section>
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
  const supportPosts = pattern?.support_posts ?? [];
  const [selectedPostIndex, setSelectedPostIndex] = useState(0);
  const selectedPost = supportPosts[selectedPostIndex] ?? supportPosts[0] ?? null;
  const title = card?.title || pattern?.pattern_label || readableSignalCode(pattern?.signal_code);
  const commonPattern = card?.common_pattern?.length ? card.common_pattern : pattern?.cues || [];
  const metrics = pattern ? proofMetrics(pattern) : [];
  const confidence = normalizedConfidence(card?.confidence);
  const patternType = readablePatternType(card?.pattern_type);

  useEffect(() => {
    if (!pattern || supportPosts.length <= 1 || typeof window === 'undefined') return;
    const timer = window.setInterval(() => {
      setSelectedPostIndex((current) => (current + 1) % supportPosts.length);
    }, 6200);
    return () => window.clearInterval(timer);
  }, [pattern, supportPosts.length]);

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
          className="fixed inset-0 z-[320] flex items-end justify-center px-0 lg:items-center lg:px-5 lg:py-5"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.2, ease: [0.22, 1, 0.36, 1] }}
        >
          <motion.div
            className="absolute inset-0 bg-black/72 backdrop-blur-[8px]"
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
            className="relative flex w-full flex-col overflow-hidden rounded-t-[28px] border border-white/[0.08] bg-[#080808] text-white shadow-[0_-18px_60px_rgba(0,0,0,0.55)] lg:grid lg:w-[min(1360px,calc(100vw-72px))] lg:grid-cols-[minmax(340px,440px)_minmax(0,1fr)] lg:rounded-[28px] lg:shadow-[0_32px_90px_rgba(0,0,0,0.72)] xl:w-[min(1460px,calc(100vw-96px))] xl:grid-cols-[minmax(400px,500px)_minmax(0,1fr)]"
            style={{ maxHeight: 'min(820px, calc(100dvh - 72px - env(safe-area-inset-top)))' }}
          >
            <div className="pointer-events-none absolute inset-x-0 top-0 h-px bg-gradient-to-r from-transparent via-[#E11D48]/42 to-transparent" />
            <div className="mx-auto mt-3 h-1.5 w-12 rounded-full bg-white/14 lg:hidden" />

            <button
              type="button"
              onClick={onClose}
              className="absolute right-4 top-4 z-20 flex h-9 w-9 items-center justify-center rounded-full border border-white/[0.08] bg-white/[0.06] text-white/54 transition hover:bg-white/[0.1] hover:text-white/82 lg:right-5 lg:top-5"
              aria-label="Close signal card"
            >
              <X size={16} strokeWidth={2.4} />
            </button>

            <div className="hidden min-h-0 flex-col justify-center gap-3 p-4 lg:flex xl:p-5">
              <div className="flex min-h-0 items-center justify-center">
                <div className="aspect-[4/5] w-full max-w-[410px] xl:max-w-[460px]">
                  <FeaturedPreview post={selectedPost} />
                </div>
              </div>
              <SupportPostRail
                posts={supportPosts}
                selectedIndex={selectedPostIndex}
                onSelect={setSelectedPostIndex}
              />
            </div>

            <div className="min-h-0 overflow-y-auto px-5 pb-[calc(18px+env(safe-area-inset-bottom))] pt-5 lg:overflow-hidden lg:px-7 lg:py-7 xl:px-8 xl:py-8">
              <div className="shrink-0 pr-10 lg:pr-12">
                <div className="flex flex-wrap items-center gap-2 lg:gap-1.5">
                  <span className="text-[9px] font-black uppercase tracking-[0.2em] text-[#E11D48]/70 lg:text-[8px]">
                    Signal intelligence
                  </span>
                  <span className="rounded-full border border-white/[0.08] bg-white/[0.045] px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.16em] text-white/38 lg:px-2 lg:py-0.5 lg:text-[7px]">
                    {readableSignalCode(pattern.signal_code)}
                  </span>
                  {confidence && (
                    <span className="rounded-full border border-[#E11D48]/18 bg-[#E11D48]/10 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.16em] text-[#FDA4AF]/82 lg:px-2 lg:py-0.5 lg:text-[7px]">
                      {confidence}
                    </span>
                  )}
                </div>
                <h3 className="mt-3 max-w-[18ch] text-[26px] font-black leading-[1.02] tracking-[-0.04em] text-white sm:text-[32px] lg:mt-2 lg:w-full lg:max-w-[min(760px,calc(100%-64px))] lg:text-[clamp(22px,2.5vh,30px)] lg:leading-[1.06] lg:[text-wrap:balance] xl:max-w-[min(860px,calc(100%-70px))] xl:text-[clamp(25px,2.55vh,34px)]">
                  {title}
                </h3>
                {patternType && (
                  <div className="mt-3 text-[10px] font-black uppercase tracking-[0.17em] text-white/28 lg:mt-2 lg:text-[8px]">
                    {patternType}
                  </div>
                )}
              </div>

              <div className="mt-4 space-y-3 lg:hidden">
                <div className="mx-auto aspect-[4/5] w-[min(76vw,286px)] max-w-full sm:w-[min(62vw,304px)]">
                  <FeaturedPreview post={selectedPost} />
                </div>
                <SupportPostRail
                  posts={supportPosts}
                  selectedIndex={selectedPostIndex}
                  onSelect={setSelectedPostIndex}
                />
              </div>

              {metrics.length > 0 && (
                <div className="mt-4 grid shrink-0 grid-cols-2 gap-2 sm:grid-cols-3 lg:mt-3 lg:grid-cols-3 lg:gap-1.5 xl:mt-4 xl:grid-cols-5 xl:gap-2">
                  {metrics.map((metric) => (
                    <div
                      key={`${pattern.firewatch_id}:${metric.label}`}
                      className="min-w-0 rounded-[14px] border border-white/[0.06] bg-white/[0.035] px-3 py-2.5 lg:rounded-[12px] lg:px-2.5 lg:py-1.5 xl:rounded-[14px] xl:px-3 xl:py-2"
                    >
                      <div className="truncate text-[7px] font-black uppercase tracking-[0.16em] text-white/28">
                        {metric.label}
                      </div>
                      <div className="mt-1 truncate text-[15px] font-black tracking-[-0.03em] text-white/88 lg:text-[13px] xl:text-[15px]">
                        {metric.value}
                      </div>
                    </div>
                  ))}
                </div>
              )}

              <div className="mt-5 space-y-3 lg:mt-4 lg:grid lg:min-h-0 lg:grid-cols-2 lg:items-start lg:gap-3 lg:space-y-0 xl:gap-3.5">
                {card?.what_happened && (
                  <DetailSection label="What happened">
                    {card.what_happened}
                  </DetailSection>
                )}
                {card?.why_it_may_have_happened && (
                  <DetailSection label="Why it moved">
                    {card.why_it_may_have_happened}
                  </DetailSection>
                )}
                {commonPattern.length > 0 && (
                  <DetailSection label="Common pattern">
                    <div className="flex flex-wrap gap-1.5 lg:gap-1.5">
                      {commonPattern.slice(0, 5).map((cue) => (
                        <span
                          key={`${pattern.firewatch_id}:detail:${cue}`}
                          className="rounded-full border border-white/[0.08] bg-white/[0.05] px-2.5 py-1.5 text-[9px] font-black uppercase tracking-[0.12em] text-white/60 lg:px-3 lg:py-1.5 lg:text-[8.5px] lg:tracking-[0.1em] xl:px-3.5 xl:text-[9px]"
                        >
                          {cue}
                        </span>
                      ))}
                    </div>
                  </DetailSection>
                )}
                {card?.do_next && (
                  <DetailSection label="Do next" tone="action">
                    {card.do_next}
                  </DetailSection>
                )}
                {card?.watchout && (
                  <DetailSection label="Watchout" tone="warning">
                    {card.watchout}
                  </DetailSection>
                )}
                {card?.per_post_notes?.length ? (
                  <DetailSection label="Post notes">
                    <div className="space-y-2 lg:space-y-1.5">
                      {card.per_post_notes.slice(0, 4).map((note, index) => (
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
        'border border-black/[0.05] bg-white/58 ring-1 ring-inset ring-white/50',
        'shadow-[inset_0_1px_0_rgba(255,255,255,0.74),0_18px_36px_-26px_rgba(15,23,42,0.2)]',
        'backdrop-blur-[38px] backdrop-saturate-[190%]',
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
            <div className="truncate text-[18px] font-black leading-[1.02] tracking-[-0.035em] text-foreground dark:text-white sm:text-[20px]">
              {pattern.pattern_label || 'Pattern'}
            </div>
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
                <div className="truncate text-[7px] font-black uppercase tracking-[0.16em] text-foreground/34 dark:text-white/28">
                  {pill.label}
                </div>
                <div className="mt-0.5 truncate text-[12px] font-black tracking-[-0.02em] text-foreground/84 dark:text-white/74">
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
            onSelect={setActivePattern}
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
