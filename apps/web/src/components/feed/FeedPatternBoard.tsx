'use client';

import { useEffect, useMemo, useRef, useState } from 'react';
import { useSearchParams } from 'next/navigation';
import { motion } from 'framer-motion';
import { PatternBoardItem, PatternBoardSupportPost } from './dashboardTypes';

type FeedPatternBoardProps = {
  patterns: PatternBoardItem[];
};

type HeadlineNumber = {
  value: string;
  qualifier: string;
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

function PatternCard({
  pattern,
  selected,
  pulse,
  onSelect,
}: {
  pattern: PatternBoardItem;
  selected: boolean;
  pulse: boolean;
  onSelect: (id: string) => void;
}) {
  const headline = pickHeadlineNumber(pattern);
  const cues = (pattern.cues || []).slice(0, 3);
  const pills = patternPills(pattern);

  const selectCard = () => onSelect(pattern.firewatch_id);

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
  const [activeId, setActiveId] = useState<string | null>(null);
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

  const selectedId = activeId || linkedPatternId;

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
            onSelect={setActiveId}
          />
        ))}
      </div>
    </div>
  );
}
