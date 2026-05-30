'use client';

import { type CSSProperties, type UIEvent, type RefObject, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { ArrowLeft, ArrowUpRight, ChevronRight, X } from 'lucide-react';
import { useRouter } from 'next/navigation';
import { createPortal } from 'react-dom';
import { AnimatePresence, motion, useScroll, useTransform, useSpring } from 'framer-motion';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import { GRID_ITEM_EASE, GRID_LAYOUT_SPRING } from '@/lib/motion';
import type { FeederFilePattern, MetricCard, ProofBlock } from '@/types/feederFile';

/* ─── types ─── */

type FeederFileClientProps = {
  feedId: string;
  selectedHandle?: string;
};

/* ─── thumbnail cache ─── */

const THUMBNAIL_FAILURE_TTL_MS = 10 * 60 * 1000;
const thumbnailFailureCache = new Map<string, number>();

function mediaProxyUrl(postKey: string | null | undefined): string {
  const key = (postKey || '').trim();
  return key ? `/api/media?postKey=${encodeURIComponent(key)}&role=thumbnail` : '';
}

function instagramPostUrl(postKey: string | null | undefined, postUrl?: string | null): string {
  const explicitUrl = String(postUrl || '').trim();
  if (explicitUrl) return explicitUrl;
  const cleanKey = String(postKey || '').trim().split('#')[0];
  const parts = cleanKey.split('/').filter(Boolean);
  const shortcode = parts.at(-1);
  const mediaType = parts[0] === 'reel' ? 'reel' : 'p';
  return shortcode ? `https://www.instagram.com/${mediaType}/${encodeURIComponent(shortcode)}/` : 'https://www.instagram.com/';
}

function isThumbnailFailureCached(postKey: string | null | undefined): boolean {
  const key = (postKey || '').trim();
  if (!key) return false;
  const failedAt = thumbnailFailureCache.get(key);
  if (!failedAt) return false;
  if (Date.now() - failedAt > THUMBNAIL_FAILURE_TTL_MS) {
    thumbnailFailureCache.delete(key);
    return false;
  }
  return true;
}

function rememberThumbnailFailure(postKey: string | null | undefined) {
  const key = (postKey || '').trim();
  if (key) thumbnailFailureCache.set(key, Date.now());
}

/* ─── constants ─── */

const APPLE_EASE = [0.32, 0.72, 0, 1] as const;
const ACCENT = '#E11D48';
const DEFAULT_ACCOUNT = '';
const STORY_RING_RADIUS = 44;
const STORY_RING_CIRCUMFERENCE = 2 * Math.PI * STORY_RING_RADIUS;
const POPUP_MOBILE_EXIT_MS = 520;
const POPUP_DESKTOP_EXIT_MS = 460;
const FEEDER_DECK_SWAP_SPRING = { type: 'spring', stiffness: 250, damping: 28, mass: 0.94 } as const;

const ACCOUNT_INITIALS: Record<string, string> = {
  '@anuj.mp4': 'AJ',
  '@lakmeindia': 'LK',
};

const EMPTY_PROOF: ProofBlock = {
  post_key: '',
  post_url: null,
  proof_label: '',
  proof_headline: '',
  post_read: '',
  what_clicked: '',
  evidence: [],
  metrics: [],
};

type FeedAvatarMap = Partial<Record<string, string>>;

function accountForHandle(raw: string | null | undefined): string | null {
  const handle = String(raw || '').trim().replace(/^@+/, '').toLowerCase();
  return handle && handle !== 'all' ? `@${handle}` : null;
}

function patternsForAccount(selectedAccount: string, patterns: FeederFilePattern[]): FeederFilePattern[] {
  const handle = String(selectedAccount || '').replace(/^@+/, '').toLowerCase();
  if (!handle || handle === 'all') return patterns;
  return patterns.filter((pattern) => pattern.account.replace(/^@+/, '').toLowerCase() === handle);
}

function TypebackText({ value }: { value: string }) {
  const [displayValue, setDisplayValue] = useState(value);
  const displayRef = useRef(value);
  const targetRef = useRef(value);
  const [typing, setTyping] = useState(false);

  useEffect(() => {
    targetRef.current = value;
    let cancelled = false;
    let timer: number | null = null;

    if (typeof window !== 'undefined' && window.matchMedia?.('(prefers-reduced-motion: reduce)').matches) {
      timer = window.setTimeout(() => {
        if (cancelled) return;
        displayRef.current = value;
        setDisplayValue(value);
        setTyping(false);
      }, 0);

      return () => {
        cancelled = true;
        if (timer !== null) window.clearTimeout(timer);
      };
    }

    const write = (next: string) => {
      displayRef.current = next;
      setDisplayValue(next);
    };

    const step = () => {
      if (cancelled) return;

      const current = displayRef.current;
      const target = targetRef.current;

      if (current === target) {
        setTyping(false);
        return;
      }

      setTyping(true);

      if (current.length > 0 && !target.startsWith(current)) {
        write(current.slice(0, -1));
        timer = window.setTimeout(step, 22);
        return;
      }

      write(target.slice(0, current.length + 1));
      timer = window.setTimeout(step, 34);
    };

    timer = window.setTimeout(step, 90);

    return () => {
      cancelled = true;
      if (timer !== null) window.clearTimeout(timer);
    };
  }, [value]);

  return (
    <span className="inline-flex min-w-0 items-baseline">
      <span>{displayValue}</span>
      <motion.span
        aria-hidden="true"
        className="ml-1 inline-block h-[0.82em] w-[0.08em] rounded-full bg-[#E11D48]"
        animate={{ opacity: typing ? [0.25, 1, 0.35] : 0 }}
        transition={{ duration: 0.58, repeat: typing ? Infinity : 0, ease: 'easeInOut' }}
      />
    </span>
  );
}

/* ─── corner ticks ─── */

function CornerTicks({ color = ACCENT, size = 12, inset = 8 }: { color?: string; size?: number; inset?: number }) {
  const s = `${size}px`;
  const shared: CSSProperties = { position: 'absolute', width: s, height: s, borderColor: color, pointerEvents: 'none' };
  return (
    <>
      <span style={{ ...shared, top: inset, left: inset, borderTop: `2px solid ${color}`, borderLeft: `2px solid ${color}` }} />
      <span style={{ ...shared, top: inset, right: inset, borderTop: `2px solid ${color}`, borderRight: `2px solid ${color}` }} />
      <span style={{ ...shared, bottom: inset, left: inset, borderBottom: `2px solid ${color}`, borderLeft: `2px solid ${color}` }} />
      <span style={{ ...shared, bottom: inset, right: inset, borderBottom: `2px solid ${color}`, borderRight: `2px solid ${color}` }} />
    </>
  );
}

/* ─── thumb (proof thumbnail) ─── */

function Thumb({
  post,
  className = '',
  index,
  total,
  showCaption = false,
  onUnavailable,
  fit = 'cover',
}: {
  post: ProofBlock;
  className?: string;
  index?: number;
  total?: number;
  showCaption?: boolean;
  onUnavailable?: (postKey: string) => void;
  fit?: 'cover' | 'contain';
}) {
  const fallback = mediaProxyUrl(post.post_key);
  const [failedPostKeys, setFailedPostKeys] = useState<Set<string>>(() => new Set());
  const dead = failedPostKeys.has(post.post_key) || isThumbnailFailureCached(post.post_key);
  const microMetric = showCaption ? post.metrics.find((m) => !m.accent) || post.metrics[0] : null;

  return (
    <div className={['relative overflow-hidden bg-[#f5f0f2] dark:bg-white/[0.06]', className].join(' ')}>
      {!dead && fallback ? (
        // eslint-disable-next-line @next/next/no-img-element -- dynamic post thumbnails are served through the media proxy
        <img
          src={fallback}
          alt=""
          className={[
            'h-full w-full transition-transform duration-700 ease-out',
            fit === 'contain' ? 'object-contain' : 'object-cover group-hover/card:scale-[1.035]',
          ].join(' ')}
          loading="lazy"
          decoding="async"
          onError={() => {
            rememberThumbnailFailure(post.post_key);
            onUnavailable?.(post.post_key);
            setFailedPostKeys((current) => {
              if (current.has(post.post_key)) return current;
              const next = new Set(current);
              next.add(post.post_key);
              return next;
            });
          }}
        />
      ) : (
        <div className="flex h-full w-full items-center justify-center bg-[radial-gradient(circle_at_28%_20%,rgba(225,29,72,0.16),transparent_42%),linear-gradient(135deg,#fff,#f9eef2)] px-5 text-center text-[11px] font-black uppercase tracking-[0.14em] text-[#E11D48]/42 dark:bg-[radial-gradient(circle_at_28%_20%,rgba(251,113,133,0.22),transparent_42%),linear-gradient(135deg,#171717,#09090b)] dark:text-[#FDA4AF]/54">
          {post.proof_label}
        </div>
      )}

      {showCaption && index !== undefined && total !== undefined && (
        <div className="pointer-events-none absolute right-2.5 top-2.5 z-10 flex h-6 items-center justify-center rounded-full bg-black/64 px-2 text-[9.5px] font-black tracking-wider text-white">
          {index}/{total}
        </div>
      )}

      {showCaption && (
        <div className="pointer-events-none absolute inset-x-0 bottom-0 flex items-end justify-between gap-2 bg-[linear-gradient(0deg,rgba(0,0,0,0.74),transparent_82%)] px-3 pb-3 pt-12">
          <div className="line-clamp-2 min-w-0 text-[9px] font-black uppercase leading-snug tracking-[0.16em] text-white">
            {index !== undefined && <span className="opacity-66">{String(index).padStart(2, '0')} / </span>}
            {post.proof_label}
          </div>
          {microMetric && (
            <div className="shrink-0 rounded-full bg-white/24 px-2 py-1 text-[9px] font-black uppercase tracking-[0.14em] text-white">
              {microMetric.value}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

/* ─── metric pill (compact inline) ─── */

function readParagraphs(text: string, targetCount = 3): string[] {
  const cleaned = text.trim();
  if (!cleaned) return [];

  const sentences = cleaned
    .split(/(?<=[.!?])\s+(?=(?:"|[A-Z0-9]))/)
    .map((sentence) => sentence.trim())
    .filter(Boolean);

  if (sentences.length <= 1) return [cleaned];
  if (sentences.length <= targetCount) return sentences;

  const paragraphs: string[] = [];
  let cursor = 0;
  while (cursor < sentences.length && paragraphs.length < targetCount) {
    const remainingSentences = sentences.length - cursor;
    const remainingParagraphs = targetCount - paragraphs.length;
    const take = Math.ceil(remainingSentences / remainingParagraphs);
    paragraphs.push(sentences.slice(cursor, cursor + take).join(' '));
    cursor += take;
  }

  return paragraphs;
}

function metricByLabel(metrics: MetricCard[], label: string): MetricCard | undefined {
  return metrics.find((metric) => metric.label.toLowerCase() === label.toLowerCase());
}

function buildProofSignalCards(
  proof: ProofBlock,
  pattern: FeederFilePattern,
  proofIndex: number,
  total: number,
): MetricCard[] {
  const proofMetric = metricByLabel(proof.metrics, 'Proof');
  const bestRank = metricByLabel(proof.metrics, 'Best rank');
  const baseline = metricByLabel(proof.metrics, 'Baseline');
  const fallbackBaseline = pattern.proofs.flatMap((post) => post.metrics).find((metric) => metric.label === 'Baseline');
  const views = metricByLabel(proof.metrics, 'Views');
  const comments = metricByLabel(proof.metrics, 'Comments');
  const signal = metricByLabel(proof.metrics, 'Signal');
  const support = metricByLabel(proof.metrics, 'Support');
  const mode = metricByLabel(proof.metrics, 'Mode');
  const patternRank = pattern.patternMetrics.find((metric) => metric.accent) || pattern.patternMetrics[0];
  const patternSignal = metricByLabel(pattern.patternMetrics, 'Signal');

  return [
    {
      label: 'Proof role',
      value: proofMetric?.value || `${proofIndex + 1}/${total}`,
      detail: proofMetric?.detail || 'selected proof',
      accent: true,
    },
    {
      label: 'Performance',
      value: bestRank?.value || patternRank?.value || 'Pattern',
      detail: bestRank?.detail || patternRank?.detail || 'feeder context',
    },
    {
      label: 'Primary signal',
      value: signal?.value || patternSignal?.value || comments?.label || 'Read',
      detail: signal?.detail || patternSignal?.detail || comments?.detail || 'why it lifts',
    },
    {
      label: 'Pattern role',
      value: support?.value || 'Core',
      detail: support?.detail || mode?.detail || 'same pattern mechanic',
    },
    {
      label: 'Lift context',
      value: baseline?.value || fallbackBaseline?.value || 'Tracked',
      detail: baseline?.detail || fallbackBaseline?.detail || 'pattern lift proxy',
    },
    {
      label: 'Scale cue',
      value: views?.value || comments?.value || signal?.value || 'Qual',
      detail: views?.detail || comments?.detail || signal?.detail || 'selected proof',
    },
  ];
}

function hasRenderableProofs(pattern: FeederFilePattern): boolean {
  return Array.isArray(pattern.proofs) && pattern.proofs.some((proof) => Boolean(proof?.post_key));
}

/* ═══════════════════════════════════════════
   1. STORY RING STRIP
   ═══════════════════════════════════════════ */

function StoryRing({
  account,
  active,
  isAnchor,
  profilePicUrl,
  onSelect,
}: {
  account: string;
  active: boolean;
  isAnchor: boolean;
  profilePicUrl?: string | null;
  onSelect: () => void;
}) {
  const initials = ACCOUNT_INITIALS[account] || account.replace(/^@+/, '').slice(0, 2).toUpperCase();
  const [failedProfilePicUrl, setFailedProfilePicUrl] = useState<string | null>(null);
  const showProfilePic = Boolean(profilePicUrl) && failedProfilePicUrl !== profilePicUrl;

  return (
    <button
      type="button"
      onClick={onSelect}
      className="group flex shrink-0 flex-col items-center gap-1.5"
    >
      {/* ring + portrait */}
      <div className="relative">
        <div className="relative flex h-[96px] w-[96px] items-center justify-center rounded-full p-[6px] transition-all duration-300 sm:h-[100px] sm:w-[100px]">
          <span
            className={[
              'absolute inset-0 rounded-full transition-colors duration-300',
              active
                ? 'bg-[#FFE4EA] shadow-[0_10px_26px_-18px_rgba(225,29,72,0.9)] dark:bg-[#3F0F1B]'
                : 'bg-black/[0.08] dark:bg-white/[0.12]',
            ].join(' ')}
          />
          {active && (
            <>
              <motion.span
                className="pointer-events-none absolute -inset-1 rounded-full bg-[#E11D48]/28 blur-md"
                initial={{ opacity: 0, scale: 0.86 }}
                animate={{ opacity: [0, 0.72, 0], scale: [0.86, 1.18, 1.02] }}
                transition={{ duration: 0.62, ease: APPLE_EASE, times: [0, 0.38, 1] }}
              />
              <motion.svg
                className="pointer-events-none absolute inset-0 z-10 h-full w-full overflow-visible"
                viewBox="0 0 100 100"
                aria-hidden="true"
              >
                <g transform="rotate(-90 50 50)">
                  <circle
                    cx="50"
                    cy="50"
                    r={STORY_RING_RADIUS}
                    fill="none"
                    stroke="rgba(225,29,72,0.24)"
                    strokeWidth="8"
                  />
                  <motion.circle
                    cx="50"
                    cy="50"
                    r={STORY_RING_RADIUS}
                    fill="none"
                    stroke={ACCENT}
                    strokeWidth="8"
                    strokeLinecap="round"
                    strokeDasharray={STORY_RING_CIRCUMFERENCE}
                    initial={{ opacity: 0.92, strokeDashoffset: STORY_RING_CIRCUMFERENCE }}
                    animate={{ opacity: 1, strokeDashoffset: 0 }}
                    transition={{ duration: 0.72, ease: APPLE_EASE }}
                    style={{ filter: 'drop-shadow(0 3px 8px rgba(225,29,72,0.42))' }}
                  />
                </g>
              </motion.svg>
            </>
          )}
          <div className="relative z-20 flex h-full w-full items-center justify-center overflow-hidden rounded-full border-[2px] border-white bg-[linear-gradient(135deg,#fce7f3,#fff1f2)] text-[26px] font-black text-[#9F1239] dark:border-[#09090b] dark:bg-[linear-gradient(135deg,#1c1917,#18181b)] dark:text-[#FDA4AF] sm:text-[28px]">
            {showProfilePic ? (
              // eslint-disable-next-line @next/next/no-img-element -- feeder avatars are proxied dynamic profile images
              <img
                src={profilePicUrl || ''}
                alt={account}
                className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-105"
                loading="lazy"
                decoding="async"
                onError={() => setFailedProfilePicUrl(profilePicUrl || null)}
              />
            ) : (
              initials
            )}
          </div>
        </div>

        {/* anchor dot */}
        {isAnchor && (
          <span className="absolute -bottom-0.5 left-1/2 h-3 w-3 -translate-x-1/2 rounded-full border-[2px] border-white bg-[#E11D48] dark:border-[#09090b]" />
        )}
      </div>

      {/* handle label */}
      <span
        className={[
          'text-[10px] font-black uppercase tracking-[0.12em] transition-colors duration-200',
          active ? 'text-[#E11D48] dark:text-[#FB7185]' : 'text-black/38 dark:text-white/34',
        ].join(' ')}
        style={{ fontFamily: 'monospace' }}
      >
        {account}
      </span>
    </button>
  );
}

function StoryStrip({
  accounts,
  activeAccount,
  anchorAccount,
  avatarUrls,
  onSelectAccount,
}: {
  accounts: string[];
  activeAccount: string;
  anchorAccount: string;
  avatarUrls: FeedAvatarMap;
  onSelectAccount: (account: string) => void;
}) {
  return (
    <div className="hide-scrollbar flex gap-8 overflow-x-auto px-5 pb-2 sm:gap-8 sm:px-1">
      {accounts.map((account) => (
        <StoryRing
          key={account}
          account={account}
          active={activeAccount === account}
          isAnchor={anchorAccount === account}
          profilePicUrl={avatarUrls[account]}
          onSelect={() => onSelectAccount(account)}
        />
      ))}
    </div>
  );
}

/* ═══════════════════════════════════════════
   2. PATTERN CARDS
   ═══════════════════════════════════════════ */

function PatternCard({
  pattern,
  patternIndex,
  onOpen,
}: {
  pattern: FeederFilePattern;
  patternIndex: number;
  onOpen: (pattern: FeederFilePattern) => void;
}) {
  const [, setFailedRailKeys] = useState<Set<string>>(
    () => new Set(pattern.proofs.filter((post) => isThumbnailFailureCached(post.post_key)).map((post) => post.post_key)),
  );

  const noteThumbnailUnavailable = useCallback((postKey: string) => {
    setFailedRailKeys((current) => {
      if (current.has(postKey)) return current;
      const next = new Set(current);
      next.add(postKey);
      return next;
    });
  }, []);

  const rankMetric = pattern.patternMetrics.find((m) => m.accent) || pattern.patternMetrics[0];

  // --- PREMIUM AUTOMATIC STACKED CAROUSEL STATE ---
  const [activeIndex, setActiveIndex] = useState(0);
  const totalProofs = pattern.proofs.length;
  const activeProof = pattern.proofs[activeIndex] || pattern.proofs[0];
  const visibleCount = Math.min(totalProofs, 3);
  const coverSlots = Array.from({ length: totalProofs <= 1 ? 1 : visibleCount }, (_, slot) => {
    const proofIndex = (activeIndex + slot) % totalProofs;
    const post = pattern.proofs[proofIndex];
    return post ? { post, proofIndex, slot } : null;
  }).filter((slot): slot is { post: ProofBlock; proofIndex: number; slot: number } => Boolean(slot));

  useEffect(() => {
    if (totalProofs <= 1) return;
    const interval = setInterval(() => {
      setActiveIndex((prev) => (prev + 1) % totalProofs);
    }, 4600); // Smooth gentle transition cycle
    return () => clearInterval(interval);
  }, [totalProofs]);

  const enterDelay = Math.min(patternIndex * 0.034, 0.18);
  const mobileCoverHeight = 'clamp(286px, 78vw, 326px)';

  return (
    <motion.button
      layout
      type="button"
      onClick={() => onOpen(pattern)}
      className="group/card relative overflow-hidden rounded-[24px] border border-black/[0.06] bg-[linear-gradient(135deg,#ffffff,#fff3f7)] p-5 text-left shadow-[0_12px_36px_-28px_rgba(15,23,42,0.5),inset_0_1px_0_rgba(255,255,255,0.82)] dark:border-white/[0.08] dark:bg-[linear-gradient(135deg,#18181b,#09090b)] dark:shadow-[0_14px_44px_-30px_rgba(0,0,0,0.9),inset_0_1px_0_rgba(255,255,255,0.08)] sm:rounded-[26px] sm:p-6 md:p-7 lg:p-8"
      initial={{ opacity: 0, y: 24, scale: 0.982, filter: 'blur(10px)' }}
      animate={{ opacity: 1, y: 0, scale: 1, filter: 'blur(0px)' }}
      exit={{ opacity: 0, y: -14, scale: 0.976, filter: 'blur(8px)' }}
      whileHover={{ y: -4 }}
      whileTap={{ scale: 0.992 }}
      transition={{
        layout: GRID_LAYOUT_SPRING,
        opacity: { duration: 0.24, delay: enterDelay, ease: GRID_ITEM_EASE },
        y: { duration: 0.34, delay: enterDelay, ease: GRID_ITEM_EASE },
        scale: { duration: 0.34, delay: enterDelay, ease: GRID_ITEM_EASE },
        filter: { duration: 0.3, delay: enterDelay, ease: GRID_ITEM_EASE },
      }}
      style={{ willChange: 'opacity, transform, filter' }}
    >
      {/* rank badge */}
      {rankMetric && (
        <div className="absolute right-5 top-5 z-10 hidden rounded-full bg-[#E11D48] px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.12em] text-white shadow-[0_8px_20px_-10px_rgba(225,29,72,0.7)] sm:block sm:right-6 sm:top-6 md:right-7 md:top-7 lg:right-8 lg:top-8">
          {rankMetric.value}
        </div>
      )}

      {activeProof && coverSlots.length > 0 && (
        <div
          data-pattern-hero="mobile"
          className="relative -mx-5 -mt-5 mb-8 overflow-hidden rounded-t-[24px] sm:hidden"
        >
          <div className="relative z-10">
            <AnimatePresence initial={false}>
              {coverSlots.map(({ post, proofIndex, slot }) => {
                const isTop = slot === 0;
                const cardTop = isTop ? 0 : slot === 1 ? 30 : 60;
                const sideInset = isTop ? 0 : slot === 1 ? 6 : 12;
                const restingScale = isTop ? 1 : slot === 1 ? 0.95 : 0.90;
                const restingOpacity = isTop ? 1 : slot === 1 ? 0.90 : 0.78;

                // Add conditional suffix to card key when looping back to force true unmount exit
                const isLoopingBack = proofIndex === (activeIndex - 1 + totalProofs) % totalProofs;
                const cardKey = isLoopingBack ? `${post.post_key}-loop` : post.post_key;

                return (
                  <motion.div
                    key={cardKey}
                    data-pattern-hero-frame={isTop ? 'true' : undefined}
                    data-pattern-stack-layer={!isTop ? 'true' : undefined}
                    className="absolute overflow-hidden rounded-[22px] border border-black/[0.08] dark:border-white/[0.12] bg-[#f5f0f2] shadow-[0_18px_48px_-30px_rgba(15,23,42,0.62),inset_0_-1px_0_rgba(255,255,255,0.78)] dark:bg-white/[0.07] dark:shadow-[0_20px_56px_-30px_rgba(0,0,0,0.95)]"
                    initial={{ y: cardTop + 10, scale: restingScale - 0.02, opacity: 0, zIndex: 10 }}
                    animate={{ y: cardTop, left: sideInset, right: sideInset, scale: restingScale, opacity: restingOpacity, zIndex: isTop ? 20 : 14 - slot }}
                    exit={isTop ? {
                      y: -360,
                      scale: 1.012,
                      opacity: 0,
                      zIndex: 30,
                      transition: {
                        y: { type: 'spring', stiffness: 85, damping: 20, mass: 1.05 },
                        scale: { duration: 0.4, ease: 'easeOut' },
                        opacity: { duration: 0.46, delay: 0.12, ease: 'easeOut' }
                      }
                    } : {
                      opacity: 0,
                      scale: restingScale - 0.02,
                      transition: { duration: 0.3, ease: 'easeOut' }
                    }}
                    transition={{
                      duration: 0.8,
                      ease: [0.16, 1, 0.3, 1]
                    }}
                    style={{
                      height: `calc(${mobileCoverHeight} - 32px)`,
                      transformOrigin: 'center top',
                      willChange: 'opacity, transform',
                    }}
                  >
                    <Thumb
                      post={post}
                      showCaption={false}
                      onUnavailable={noteThumbnailUnavailable}
                      className="h-full w-full"
                    />
                    <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.06),transparent_38%,rgba(0,0,0,0.72))]" />
                    {isTop && (
                      <div className="pointer-events-none absolute left-4 right-4 top-4 z-20 flex items-center justify-between gap-3">
                        <div
                          data-pattern-proof-chip="true"
                          className="rounded-full bg-white/20 px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.18em] text-white shadow-[0_8px_22px_-16px_rgba(0,0,0,0.76)] backdrop-blur-md"
                        >
                          {proofIndex + 1}/{pattern.proofs.length}
                        </div>

                        <div className="flex items-center gap-2 rounded-full bg-white/20 px-3 py-1.5 text-[10px] font-black uppercase tracking-[0.16em] text-white shadow-[0_8px_22px_-16px_rgba(0,0,0,0.76)] backdrop-blur-md">
                          <span className="flex -space-x-1">
                            {Array.from({ length: Math.min(totalProofs, 4) }, (_, dot) => (
                              <span
                                key={`depth-dot-${pattern.pattern_id}-${dot}`}
                                className="block h-2 w-2 rounded-full border border-white/50 bg-white/75"
                              />
                            ))}
                          </span>
                          <span>{totalProofs} {totalProofs === 1 ? 'proof' : 'proofs'}</span>
                        </div>
                      </div>
                    )}
                  </motion.div>
                );
              })}
            </AnimatePresence>
          </div>

          {coverSlots.length > 1 && (
            <div
              aria-hidden="true"
              className="pointer-events-none absolute inset-x-6 bottom-0 z-[7] h-8 rounded-full bg-black/14 blur-2xl dark:bg-black/50"
            />
          )}

          <div className="relative z-0" style={{ height: mobileCoverHeight }} aria-hidden="true" />
        </div>
      )}

      {/* Two-column grid layout for pattern covers */}
      <div className="grid w-full grid-cols-1 items-center gap-6 sm:grid-cols-[minmax(0,1fr)_minmax(210px,36%)] md:grid-cols-[minmax(0,1fr)_minmax(250px,38%)] md:gap-8 lg:grid-cols-[minmax(0,1fr)_minmax(280px,38%)] xl:grid-cols-[minmax(0,1fr)_minmax(250px,36%)] 2xl:grid-cols-[minmax(0,1fr)_minmax(290px,38%)]">
        
        {/* Left Column: Text detail + metrics */}
        <div className="flex flex-col h-full justify-between min-w-0">
          <div>
            <div className="hidden items-center gap-2 text-[11px] font-black uppercase tracking-[0.2em] text-black/38 dark:text-white/34 sm:flex" style={{ fontFamily: 'monospace' }}>
              <span className="text-[#E11D48]">PATTERN {String(patternIndex + 1).padStart(2, '0')}</span>
            </div>

            <h2 className="max-w-[560px] text-[29px] font-black leading-[1.01] tracking-normal text-black dark:text-white sm:mt-5 sm:pr-4 sm:text-[34px] md:text-[38px] lg:text-[42px]">
              {pattern.pattern.tile_headline}
            </h2>
          </div>

          {/* bottom: metrics + open cue */}
          <div className="mt-4 flex items-center justify-between gap-4 border-t border-black/[0.04] pt-3 dark:border-white/[0.04] sm:mt-5 sm:pt-4">
            <div className="flex flex-wrap items-center gap-x-1.5 sm:gap-x-3 gap-y-1 text-[9px] sm:text-[10px] md:text-[11px] font-black uppercase tracking-[0.03em] sm:tracking-[0.14em]">
              {pattern.patternMetrics.slice(1).map((m) => (
                <div key={`${pattern.pattern_id}:${m.label}`} className="flex items-center gap-1">
                  <span className="text-black/64 dark:text-white/58">{m.value}</span>
                  <span className="text-black/32 dark:text-white/28">{m.detail || m.label}</span>
                </div>
              ))}
            </div>

            <div className="flex shrink-0 items-center gap-1 text-[10.5px] sm:text-[11.5px] font-black uppercase tracking-[0.14em] text-black/36 transition group-hover/card:text-[#E11D48] dark:text-white/30 dark:group-hover/card:text-[#FB7185]">
              <span>Open</span>
              <ArrowUpRight size={12} strokeWidth={3} />
            </div>
          </div>
        </div>

        {/* Right Column: Desktop proof deck */}
        <div data-pattern-rail="desktop" className="hidden min-w-0 shrink-0 select-none items-center justify-end overflow-visible py-1 sm:flex">
          <div className="relative flex h-[218px] w-full max-w-[240px] items-center justify-center overflow-visible md:h-[274px] md:max-w-[280px] lg:h-[316px] lg:max-w-[308px] xl:h-[278px] xl:max-w-[270px] 2xl:h-[318px] 2xl:max-w-[310px]">
            {pattern.proofs.map((post, i) => {
              let position: 'center' | 'left' | 'right' | 'hidden' = 'hidden';
              
              if (i === activeIndex) {
                position = 'center';
              } else if (i === (activeIndex + 1) % totalProofs) {
                position = 'right';
              } else if (i === (activeIndex - 1 + totalProofs) % totalProofs && totalProofs > 2) {
                position = 'left';
              }

              let positionClasses = "";
              
              if (position === 'center') {
                positionClasses = "z-30 translate-x-0 rotate-0 scale-100 opacity-100 blur-0 shadow-[0_24px_58px_-18px_rgba(15,23,42,0.58)] group-hover/card:scale-[1.02] group-hover/card:shadow-[0_30px_68px_-18px_rgba(15,23,42,0.68)] dark:shadow-[0_28px_64px_-16px_rgba(0,0,0,0.92)] dark:group-hover/card:shadow-[0_34px_78px_-14px_rgba(0,0,0,0.98)] pointer-events-auto";
              } else if (position === 'left') {
                positionClasses = "z-10 scale-[0.82] -translate-x-[42px] md:-translate-x-[58px] lg:-translate-x-[68px] xl:-translate-x-[56px] 2xl:-translate-x-[68px] rotate-[-7deg] opacity-56 blur-[1px] brightness-[0.82] group-hover/card:-translate-x-[50px] md:group-hover/card:-translate-x-[68px] lg:group-hover/card:-translate-x-[78px] xl:group-hover/card:-translate-x-[66px] 2xl:group-hover/card:-translate-x-[78px] group-hover/card:rotate-[-10deg] group-hover/card:opacity-72 group-hover/card:blur-[0.25px] pointer-events-none";
              } else if (position === 'right') {
                positionClasses = "z-10 scale-[0.82] translate-x-[42px] md:translate-x-[58px] lg:translate-x-[68px] xl:translate-x-[56px] 2xl:translate-x-[68px] rotate-[7deg] opacity-56 blur-[1px] brightness-[0.82] group-hover/card:translate-x-[50px] md:group-hover/card:translate-x-[68px] lg:group-hover/card:translate-x-[78px] xl:group-hover/card:translate-x-[66px] 2xl:group-hover/card:translate-x-[78px] group-hover/card:rotate-[10deg] group-hover/card:opacity-72 group-hover/card:blur-[0.25px] pointer-events-none";
              } else {
                positionClasses = "z-0 scale-[0.72] translate-x-0 rotate-0 opacity-0 blur-[4px] pointer-events-none";
              }

              return (
                <div
                  key={post.post_key}
                  className={[
                    "absolute aspect-[4/5] w-[154px] shrink-0 overflow-hidden rounded-[22px] bg-[#f5f0f2] shadow-[0_10px_24px_-16px_rgba(15,23,42,0.45)] transition-all duration-[980ms] md:w-[190px] lg:w-[218px] xl:w-[188px] 2xl:w-[218px] dark:bg-white/[0.07]",
                    positionClasses
                  ].join(" ")}
                  style={{
                    transitionTimingFunction: 'cubic-bezier(0.16, 1, 0.3, 1)'
                  }}
                >
                  <Thumb
                    post={post}
                    index={i + 1}
                    total={pattern.proofs.length}
                    showCaption={false}
                    onUnavailable={noteThumbnailUnavailable}
                    className="h-full w-full"
                  />
                  <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.03),transparent_36%,rgba(0,0,0,0.76))]" />
                  {position === 'center' && <CornerTicks color={ACCENT} size={10} inset={8} />}
                  <div className="absolute bottom-3 left-3 rounded-full bg-black/50 px-2.5 py-1 text-[9px] font-black uppercase tracking-[0.16em] text-white shadow-[0_10px_24px_-18px_rgba(0,0,0,0.9)] backdrop-blur-md">
                    {i + 1}/{pattern.proofs.length}
                  </div>
                </div>
              );
            })}
          </div>
        </div>

      </div>
    </motion.button>
  );
}

/* ═══════════════════════════════════════════
   3. POPUP – MOBILE (card-based sections)
   ═══════════════════════════════════════════ */

function MobileBreakdownStepCard({
  item,
  index,
  containerRef,
}: {
  item: string;
  index: number;
  containerRef: RefObject<HTMLDivElement | null>;
}) {
  const cardRef = useRef<HTMLDivElement>(null);
  const { scrollYProgress } = useScroll({
    target: cardRef,
    container: containerRef,
    offset: ["start end", "end start"],
  });

  const opacity = useTransform(
    scrollYProgress,
    [0, 0.35, 0.5, 0.65, 1],
    [0.72, 0.38, 0.06, 0.38, 0.72]
  );

  const scale = useTransform(
    scrollYProgress,
    [0, 0.35, 0.5, 0.65, 1],
    [1.24, 1.1, 0.96, 1.1, 1.24]
  );

  const y = useTransform(
    scrollYProgress,
    [0, 0.35, 0.5, 0.65, 1],
    [24, 8, 0, -8, -24]
  );

  const smoothOpacity = useSpring(opacity, { stiffness: 90, damping: 24, mass: 0.8 });
  const smoothScale = useSpring(scale, { stiffness: 90, damping: 24, mass: 0.8 });
  const smoothY = useSpring(y, { stiffness: 90, damping: 24, mass: 0.8 });

  return (
    <div
      ref={cardRef}
      className="relative overflow-hidden rounded-[16px] border border-black/[0.05] bg-black/[0.015] p-5 transition-colors duration-300 dark:border-white/[0.05] dark:bg-white/[0.015]"
    >
      {/* Centered massive background step number watermark with buttery spring scrolling */}
      <motion.div
        className="absolute inset-0 flex items-center justify-center font-mono font-black text-[#E11D48] dark:text-[#FB7185] select-none pointer-events-none z-0"
        style={{
          fontSize: 'min(170px, 38vw)',
          lineHeight: 1,
          opacity: smoothOpacity,
          scale: smoothScale,
          y: smoothY,
        }}
      >
        {String(index + 1).padStart(2, '0')}
      </motion.div>
      {/* Bigger and clearer full-width foreground text */}
      <p className="relative z-10 w-full text-[15.5px] font-extrabold leading-[1.52] text-black/88 dark:text-zinc-100">
        {item}
      </p>
    </div>
  );
}

function MobilePopup({
  pattern,
  proofIndex,
  setProofIndex,
  onClose,
  isClosing,
}: {
  pattern: FeederFilePattern;
  proofIndex: number;
  setProofIndex: (i: number) => void;
  onClose: () => void;
  isClosing: boolean;
}) {
  const [scrollContainer, setScrollContainer] = useState<HTMLDivElement | null>(null);
  const mobileContainerRef = useRef<HTMLDivElement | null>(null);
  const assignScrollContainer = useCallback((node: HTMLDivElement | null) => {
    mobileContainerRef.current = node;
    setScrollContainer(node);
  }, []);
  const proof = pattern.proofs[proofIndex];
  const total = pattern.proofs.length;
  const [showBreakdown, setShowBreakdown] = useState(false);
  const [closeVisible, setCloseVisible] = useState(true);
  const proofReadRef = useRef<HTMLElement>(null);
  const lastScrollTopRef = useRef(0);
  const closeVisibleRef = useRef(true);
  const scrollIntentRef = useRef({ up: 0, down: 0 });
  const safeProof = proof ?? EMPTY_PROOF;
  const proofReadParagraphs = useMemo(() => readParagraphs(safeProof.post_read), [safeProof.post_read]);
  const proofSignalCards = useMemo(
    () => buildProofSignalCards(safeProof, pattern, proofIndex, total),
    [pattern, safeProof, proofIndex, total],
  );

  const goToProof = useCallback(
    (next: number, scrollTarget: 'current' | 'proof' = 'current') => {
      const wrapped = (next + total) % total;
      setProofIndex(wrapped);
      if (scrollTarget === 'proof') {
        requestAnimationFrame(() => {
          proofReadRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
      }
    },
    [total, setProofIndex],
  );

  const setCloseVisibility = useCallback((visible: boolean) => {
    if (closeVisibleRef.current === visible) return;
    closeVisibleRef.current = visible;
    setCloseVisible(visible);
  }, []);

  const handlePopupScroll = useCallback((event: UIEvent<HTMLDivElement>) => {
    const nextTop = event.currentTarget.scrollTop;
    const delta = nextTop - lastScrollTopRef.current;
    const intent = scrollIntentRef.current;

    if (nextTop < 48) {
      intent.up = 0;
      intent.down = 0;
      setCloseVisibility(true);
    } else if (delta < -1) {
      intent.up += Math.abs(delta);
      intent.down = 0;
      if (intent.up >= 22) setCloseVisibility(true);
    } else if (delta > 1) {
      intent.down += delta;
      intent.up = 0;
      if (intent.down >= 48 && nextTop > 150) setCloseVisibility(false);
    }

    lastScrollTopRef.current = nextTop;
  }, [setCloseVisibility]);

  if (!proof || total === 0) return null;

  return (
    <motion.div
      ref={assignScrollContainer}
      className="fixed inset-0 z-[1000] overflow-y-auto overflow-x-hidden bg-[#FAF9F6]/94 text-[#111111] backdrop-blur-xl transition-colors duration-300 dark:bg-[#09090b]/94 dark:text-white"
      initial={{ opacity: 0, y: 28, scale: 0.982, filter: 'blur(14px)' }}
      animate={isClosing
        ? { opacity: 0, y: 34, scale: 0.976, filter: 'blur(16px)' }
        : { opacity: 1, y: 0, scale: 1, filter: 'blur(0px)' }}
      exit={{ opacity: 0, y: 34, scale: 0.976, filter: 'blur(16px)', transition: { duration: 0.5, ease: APPLE_EASE } }}
      transition={{
        opacity: { duration: isClosing ? 0.36 : 0.32, ease: APPLE_EASE },
        y: { type: 'spring', stiffness: isClosing ? 180 : 230, damping: isClosing ? 26 : 30, mass: 0.9 },
        scale: { type: 'spring', stiffness: isClosing ? 180 : 230, damping: isClosing ? 27 : 30, mass: 0.9 },
        filter: { duration: isClosing ? 0.5 : 0.42, ease: APPLE_EASE },
      }}
      style={{ willChange: 'opacity, transform, filter' }}
      onScroll={handlePopupScroll}
    >
      <div className="pointer-events-none sticky top-0 z-[1020] flex h-0 justify-end px-3.5 pt-[calc(10px+env(safe-area-inset-top))]">
        <motion.button
          type="button"
          onClick={onClose}
          data-mobile-popup-close="true"
          className={[
            'flex h-12 w-12 items-center justify-center rounded-full border border-white/18 bg-[#E11D48] text-white shadow-[0_18px_38px_-16px_rgba(225,29,72,0.95),0_0_0_1px_rgba(255,255,255,0.14)_inset] backdrop-blur-xl transition-colors hover:bg-[#BE123C] active:scale-95 dark:border-white/16 dark:bg-[#E11D48] dark:shadow-[0_18px_42px_-14px_rgba(225,29,72,0.8),0_0_0_1px_rgba(255,255,255,0.12)_inset]',
            closeVisible ? 'pointer-events-auto' : 'pointer-events-none',
          ].join(' ')}
          aria-label="Close"
          initial={{ opacity: 0, scale: 0.92, y: -14, filter: 'blur(5px)' }}
          animate={closeVisible
            ? { opacity: 1, scale: 1, y: 0, filter: 'blur(0px)' }
            : { opacity: 0, scale: 0.92, y: -16, filter: 'blur(5px)' }}
          transition={{
            opacity: { duration: closeVisible ? 0.5 : 0.42, ease: APPLE_EASE },
            y: { type: 'spring', stiffness: closeVisible ? 135 : 120, damping: closeVisible ? 24 : 25, mass: 1.15 },
            scale: { type: 'spring', stiffness: closeVisible ? 145 : 120, damping: closeVisible ? 25 : 26, mass: 1.15 },
            filter: { duration: closeVisible ? 0.46 : 0.38, ease: APPLE_EASE },
          }}
          whileTap={{ scale: 0.92 }}
          style={{ willChange: 'opacity, transform, filter' }}
        >
          <X size={21} strokeWidth={3.2} />
        </motion.button>
      </div>

      <div
        style={{ paddingTop: 'calc(72px + env(safe-area-inset-top))' }}
        className="px-0 pb-[calc(32px+env(safe-area-inset-bottom))]"
      >

        {/* ── SECTION 1: Pattern Overview ── */}
        <section className="border-y border-x-0 border-black/[0.06] bg-white/90 px-5 py-6 shadow-sm dark:border-white/[0.06] dark:bg-zinc-900/60 sm:backdrop-blur-md lg:backdrop-blur-none">
          <div className="text-[11.5px] font-black uppercase tracking-[0.22em] text-[#E11D48] dark:text-[#FB7185] font-mono">
            Pattern overview
          </div>

          <h3 className="mt-3 text-[clamp(28px,6vw,34px)] font-serif font-extrabold leading-[1.08] tracking-tight text-black dark:text-white">
            {pattern.pattern.modal_headline}
          </h3>

          <p className="mt-4 text-[15px] font-black leading-[1.38] text-black/80 dark:text-white/76 border-l-4 border-[#E11D48] pl-3.5 italic">
            {pattern.pattern.the_hook}
          </p>

          {/* metrics strip */}
          <div className="mt-5 grid grid-cols-3 gap-3 border-t border-black/[0.08] dark:border-white/[0.07] pt-4">
            {pattern.patternMetrics.map((metric) => (
              <div key={`mf:${metric.label}`} className="min-w-0">
                <div className={['text-[17px] font-mono font-black leading-none', metric.accent ? 'text-[#E11D48] dark:text-[#FB7185]' : 'text-black dark:text-white'].join(' ')}>
                  {metric.value}
                </div>
                <div className="mt-1 text-[7.5px] font-black uppercase tracking-[0.06em] leading-tight text-black/40 dark:text-white/30">
                  {metric.detail || metric.label}
                </div>
              </div>
            ))}
          </div>

          {/* breakdown toggle */}
          <button
            type="button"
            onClick={() => setShowBreakdown(!showBreakdown)}
            className="mt-5 flex w-full items-center justify-between rounded-xl border border-black/10 dark:border-white/10 bg-white dark:bg-zinc-900 px-4.5 py-3.5 shadow-[0_4px_12px_rgba(0,0,0,0.03)] active:scale-[0.99] transition-all duration-200"
          >
            <div className="flex items-center gap-2.5">
              <span className="flex h-2 w-2 rounded-full bg-[#E11D48] dark:bg-[#FB7185]" />
              <span className="text-[11.5px] font-black uppercase tracking-[0.18em] text-black dark:text-white font-mono">Pattern breakdown</span>
            </div>
            <div className="flex items-center gap-1.5">
              <span className="text-[9px] font-bold uppercase tracking-[0.08em] text-black/40 dark:text-white/40 font-mono">
                {showBreakdown ? 'Hide' : 'Expand'}
              </span>
              <ChevronRight
                size={14}
                strokeWidth={3}
                className={['text-[#E11D48] dark:text-[#FB7185] transition-transform duration-300', showBreakdown ? 'rotate-90' : ''].join(' ')}
              />
            </div>
          </button>
          <AnimatePresence>
            {showBreakdown && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: 'auto', opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                transition={{ duration: 0.26, ease: APPLE_EASE }}
                className="overflow-hidden"
              >
                <div className="flex flex-col gap-3.5 pt-4 w-full">
                  {scrollContainer && pattern.pattern.the_breakdown.map((item, i) => (
                    <MobileBreakdownStepCard
                      key={`mbd:${i}`}
                      item={item}
                      index={i}
                      containerRef={mobileContainerRef}
                    />
                  ))}
                </div>

                <p className="mt-4 text-[13px] font-semibold leading-[1.52] text-black/50 dark:text-white/42">
                  {pattern.pattern.why_it_works}
                </p>

                <div className="mt-4 grid gap-3">
                  <div className="rounded-[14px] border border-black/[0.05] dark:border-white/[0.05] bg-black/[0.01] dark:bg-white/[0.025] p-4">
                    <div className="text-[11px] font-black uppercase tracking-[0.2em] text-[#E11D48] dark:text-[#FB7185] font-mono">Keep</div>
                    <ul className="mt-2.5 space-y-3">
                      {pattern.pattern.what_to_keep.map((item, i) => (
                        <li key={`mk:${i}`} className="flex gap-3 items-center">
                          <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-lg bg-[#E11D48] text-[11px] font-black text-white shadow-sm">+</span>
                          <span className="text-[13.5px] font-bold leading-[1.3] text-black dark:text-zinc-100">{item}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                  <div className="rounded-[14px] border border-black/[0.05] dark:border-white/[0.05] bg-black/[0.005] dark:bg-white/[0.02] p-4">
                    <div className="text-[11px] font-black uppercase tracking-[0.2em] text-black dark:text-white font-mono">Kills</div>
                    <ul className="mt-2.5 space-y-3">
                      {pattern.pattern.what_kills_it.map((item, i) => (
                        <li key={`mki:${i}`} className="flex gap-3 items-center">
                          <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-lg bg-black/10 dark:bg-white/10 text-[11px] font-black text-black/50 dark:text-white/40">-</span>
                          <span className="text-[13.5px] font-semibold leading-[1.3] text-black/80 dark:text-zinc-300">{item}</span>
                        </li>
                      ))}
                    </ul>
                  </div>
                </div>
              </motion.div>
            )}
          </AnimatePresence>
        </section>

        {/* ── SECTION 2: Evidence ── */}
        <div className="mt-5 flex items-center gap-2 px-5 text-[13px] font-black uppercase tracking-[0.2em] text-black dark:text-white font-mono">
          Evidence · {total} proofs
        </div>
        <section className="mt-2 border-y border-x-0 border-black/[0.06] dark:border-white/[0.06] bg-white/90 dark:bg-zinc-900/60 px-5 py-4 shadow-sm">
          {/* proof selection cards */}
          <div className="hide-scrollbar -mx-1 flex gap-3 overflow-x-auto px-1 pb-1">
            {pattern.proofs.map((post, i) => {
              const active = i === proofIndex;
              const metric = post.metrics.find((m) => m.accent) || post.metrics[0];
              return (
                <button
                  key={post.post_key}
                  type="button"
                  onClick={() => goToProof(i, 'proof')}
                  className={[
                    'group/card w-[152px] shrink-0 overflow-hidden rounded-[14px] border text-left transition-all duration-200 active:scale-[0.98]',
                    active
                      ? 'border-[#E11D48]/56 bg-black/[0.02] dark:bg-white/[0.05] shadow-[0_4px_12px_rgba(225,29,72,0.1)]'
                      : 'border-black/[0.05] dark:border-white/[0.05] opacity-50 hover:opacity-75',
                  ].join(' ')}
                >
                  <div className="relative aspect-[4/5] overflow-hidden">
                    <Thumb post={post} className={['h-full w-full object-cover transition-all duration-300', active ? '' : 'grayscale opacity-60 contrast-[1.1]'].join(' ')} />
                    <div className="absolute left-1.5 top-1.5 rounded-full bg-[#E11D48] px-2 py-0.5 text-[7px] font-black uppercase tracking-[0.1em] text-white">
                      {metric.value}
                    </div>
                  </div>
                  <div className="px-3 py-2.5">
                    <div className="text-[10.5px] font-black uppercase leading-[1.1] tracking-[0.05em] text-black/80 dark:text-white/70">
                      {post.proof_label}
                    </div>
                    <div className="mt-1 text-[8.5px] font-black uppercase tracking-[0.12em] text-black/80 dark:text-zinc-200 font-mono">
                      Proof {i + 1}/{total}
                    </div>
                  </div>
                </button>
              );
            })}
          </div>
        </section>

        {/* ── SECTION 3: Proof Read ── */}
        <div className="mt-5 flex items-center gap-2 px-5 text-[13px] font-black uppercase tracking-[0.2em] text-black dark:text-white font-mono">
          Proof read
        </div>
        <section
          ref={proofReadRef}
          style={{ scrollMarginTop: 'calc(120px + env(safe-area-inset-top))' }}
          className="mt-2 border-y border-x-0 border-black/[0.06] dark:border-white/[0.06] bg-white/90 dark:bg-zinc-900/60 px-5 py-6 shadow-sm"
        >
          <h4 className="text-[25px] font-serif font-extrabold leading-[1.1] tracking-tight text-black dark:text-white">
            {proof.proof_headline}
          </h4>

          <a
            href={instagramPostUrl(proof.post_key, proof.post_url)}
            target="_blank"
            rel="noopener noreferrer"
            className="mt-4 inline-flex h-11 items-center gap-2 rounded-full bg-[#E11D48] px-4 text-[10px] font-black uppercase tracking-[0.14em] text-white shadow-[0_10px_24px_-16px_rgba(225,29,72,0.8)] active:scale-[0.98]"
            aria-label="Open selected proof on Instagram"
          >
            <span>Open on Instagram</span>
            <ArrowUpRight size={13} strokeWidth={3} />
          </a>

          <div className="mt-5 border-t-2 border-[#E11D48] pt-3.5">
            <div className="text-[11px] font-black uppercase tracking-[0.2em] text-[#E11D48] dark:text-[#FB7185] font-mono">What clicked</div>
            <p className="mt-2 text-[clamp(16px,4.2vw,20px)] font-black leading-[1.3] text-[#E11D48] dark:text-[#FB7185] italic pl-3 border-l-2 border-[#E11D48]/30">
              &ldquo;{proof.what_clicked}&rdquo;
            </p>
          </div>

          {/* Bold Premium Stats Panel Grid */}
          <div className="mt-4 grid grid-cols-2 gap-2.5">
            {proofSignalCards.map((metric, i) => (
              <div
                key={`mstat:${proof.post_key}:${i}`}
                className={[
                  'relative min-w-0 rounded-xl border pl-3.5 pr-2 py-2.5 overflow-hidden',
                  metric.accent
                    ? 'border-[#E11D48]/20 bg-[#E11D48]/[0.02] dark:bg-[#E11D48]/[0.05]'
                    : 'border-black/[0.05] dark:border-white/[0.05] bg-black/[0.015] dark:bg-white/[0.015]',
                ].join(' ')}
              >
                <span className={['absolute left-0 top-0 bottom-0 w-[2.5px]', metric.accent ? 'bg-[#E11D48]' : 'bg-black/10 dark:bg-white/10'].join(' ')} />
                <div className={['text-[20px] font-mono font-black leading-none', metric.accent ? 'text-[#E11D48] dark:text-[#FB7185]' : 'text-black dark:text-white'].join(' ')}>
                  {metric.value}
                </div>
                <div className="mt-1 text-[7.5px] font-black uppercase tracking-[0.06em] leading-tight text-black/64 dark:text-zinc-400 font-mono">
                  {metric.label} {metric.detail ? `· ${metric.detail}` : ''}
                </div>
              </div>
            ))}
          </div>

          {/* Post Read: Phase Timeline on Mobile */}
          <div className="mt-4 space-y-3.5">
            {proofReadParagraphs.map((paragraph, i) => (
              <div key={`mpr:${proof.post_key}:${i}`} className="relative flex flex-col rounded-[18px] border border-black/[0.045] dark:border-white/[0.05] bg-black/[0.015] dark:bg-white/[0.015] p-4">
                <div className="flex items-center justify-between mb-3 text-[11.5px] font-black uppercase tracking-[0.18em] text-[#E11D48] dark:text-[#FB7185] font-mono border-b border-black/[0.05] dark:border-white/[0.05] pb-2">
                  <span>{['THE BUILD', 'THE MOVE', 'THE HOLD'][i] ?? `PHASE ${String(i + 1).padStart(2, '0')}`}</span>
                  <span className="h-1.5 w-1.5 rounded-full bg-[#E11D48] animate-pulse" />
                </div>
                <p className="text-[13.5px] font-semibold leading-relaxed text-black/68 dark:text-zinc-300">
                  {paragraph}
                </p>
              </div>
            ))}
          </div>

          <div className="mt-5 rounded-[14px] border border-black/[0.05] dark:border-white/[0.05] bg-black/[0.01] dark:bg-white/[0.025] p-4">
            <div className="text-[11.5px] font-black uppercase tracking-[0.2em] text-black dark:text-white font-mono">Evidence</div>
            <ul className="mt-2.5 space-y-2 text-[12px] font-bold leading-[1.42] text-black/60 dark:text-white/46">
              {proof.evidence.map((e, i) => (
                <li key={`mev:${i}`} className="flex gap-2.5">
                  <span className="mt-[0.4em] h-1.5 w-1.5 shrink-0 rounded-full bg-[#E11D48]" />
                  <span>{e}</span>
                </li>
              ))}
            </ul>
          </div>
        </section>
      </div>
    </motion.div>
  );
}

/* ═══════════════════════════════════════════
   3b. POPUP – DESKTOP (magazine editorial)
   ═══════════════════════════════════════════ */

function DesktopPopup({
  pattern,
  proofIndex,
  setProofIndex,
  onClose,
  isClosing,
}: {
  pattern: FeederFilePattern;
  proofIndex: number;
  setProofIndex: (i: number) => void;
  onClose: () => void;
  isClosing: boolean;
}) {
  const proof = pattern.proofs[proofIndex];
  const total = pattern.proofs.length;
  const [direction, setDirection] = useState(0);
  const proofReadRef = useRef<HTMLElement>(null);
  const safeProof = proof ?? EMPTY_PROOF;
  const proofReadParagraphs = useMemo(() => readParagraphs(safeProof.post_read), [safeProof.post_read]);
  const proofSignalCards = useMemo(
    () => buildProofSignalCards(safeProof, pattern, proofIndex, total),
    [pattern, safeProof, proofIndex, total],
  );

  const patternPerformanceStats = useMemo(() => {
    const proofMetrics = pattern.proofs.flatMap((post) => post.metrics);
    const baselineMetric = proofMetrics.find((metric) => metric.label === 'Baseline');
    const viewsMetric = proofMetrics.find((metric) => metric.label === 'Views');
    return [
      ...pattern.patternMetrics,
      ...(baselineMetric ? [{ label: 'Lift proof', value: baselineMetric.value, detail: baselineMetric.detail }] : []),
      ...(viewsMetric ? [{ label: 'Reach proof', value: viewsMetric.value, detail: 'views in this pattern' }] : []),
    ];
  }, [pattern.patternMetrics, pattern.proofs]);

  const selectProof = useCallback(
    (next: number, scrollTarget: 'current' | 'proof' = 'current') => {
      if (next < 0 || next >= total) return;
      setDirection(next > proofIndex ? 1 : -1);
      setProofIndex(next);
      if (scrollTarget === 'proof') {
        requestAnimationFrame(() => {
          proofReadRef.current?.scrollIntoView({ behavior: 'smooth', block: 'start' });
        });
      }
    },
    [proofIndex, total, setProofIndex],
  );

  if (!proof || total === 0) return null;

  return (
    <motion.div
      className="fixed inset-0 z-[1000] flex flex-col bg-[#FAF9F6]/96 text-[#111111] backdrop-blur-xl transition-colors duration-300 dark:bg-[#09090b]/96 dark:text-[#f4f4f5]"
      initial={{ opacity: 0, y: 18, scale: 0.982, filter: 'blur(12px)' }}
      animate={isClosing
        ? { opacity: 0, y: 24, scale: 0.98, filter: 'blur(14px)' }
        : { opacity: 1, y: 0, scale: 1, filter: 'blur(0px)' }}
      exit={{ opacity: 0, y: 24, scale: 0.98, filter: 'blur(14px)', transition: { duration: 0.44, ease: APPLE_EASE } }}
      transition={{
        opacity: { duration: isClosing ? 0.32 : 0.3, ease: APPLE_EASE },
        y: { type: 'spring', stiffness: isClosing ? 190 : 240, damping: isClosing ? 28 : 32, mass: 0.9 },
        scale: { type: 'spring', stiffness: isClosing ? 190 : 240, damping: isClosing ? 28 : 32, mass: 0.9 },
        filter: { duration: isClosing ? 0.44 : 0.38, ease: APPLE_EASE },
      }}
      style={{ willChange: 'opacity, transform, filter' }}
    >
      {/* close button */}
      <button
        type="button"
        onClick={onClose}
        className="fixed right-[clamp(20px,4vw,80px)] top-[clamp(20px,3.2vh,42px)] z-[1020] flex h-13 w-13 cursor-pointer items-center justify-center rounded-full border border-black/10 bg-white/90 text-black shadow-[0_8px_32px_-12px_rgba(0,0,0,0.3)] transition duration-200 hover:scale-108 hover:bg-[#E11D48] hover:text-white hover:border-[#E11D48] active:scale-95 dark:border-white/10 dark:bg-zinc-900/90 dark:text-white dark:hover:bg-[#E11D48] dark:hover:border-[#E11D48] sm:backdrop-blur-md lg:backdrop-blur-none"
        aria-label="Close"
      >
        <X size={22} strokeWidth={3} />
      </button>

      {/* ── HERO: full-width headline zone ── */}
      <div className="shrink-0 px-[clamp(20px,4vw,80px)] pb-6 pt-[clamp(22px,2.8vh,40px)]">
        {/* breadcrumb */}
        <div className="mb-5 flex items-center gap-3 text-[10px] font-black uppercase tracking-[0.25em] text-black/40 dark:text-white/34 font-mono">
          <span className="rounded-md border border-[#E11D48]/18 bg-[#E11D48]/10 px-3 py-1.5 text-[10.5px] font-black uppercase tracking-[0.14em] text-[#E11D48] dark:text-[#FB7185] shadow-[0_1px_3px_rgba(0,0,0,0.02)]">
            {pattern.pattern.tile_label}
          </span>
          <span className="h-1.5 w-1.5 rounded-full bg-[#E11D48] animate-pulse" />
          <span>Pattern {String(proofIndex + 1).padStart(2, '0')}</span>
          <span className="h-1.5 w-1.5 rounded-full bg-[#E11D48]" />
          <span className="text-[#E11D48] dark:text-[#FB7185] font-black">{pattern.account}</span>
        </div>

        {/* massive headline */}
        <h2
          className="max-w-[min(100%,1680px)] text-left text-[clamp(38px,3.5vw,62px)] font-serif font-semibold leading-[1.05] tracking-tight text-[#08080a] dark:text-zinc-50"
          style={{ textWrap: 'balance' }}
        >
          {pattern.pattern.modal_headline}
        </h2>

        {/* accent divider */}
        <div className="relative mt-8 h-[1px] w-full bg-black/[0.08] dark:bg-white/[0.08]">
          <div className="absolute left-0 top-0 h-[3px] -translate-y-1/3 w-[clamp(180px,24vw,400px)] bg-[#E11D48] shadow-[0_2px_10px_rgba(225,29,72,0.4)]" />
        </div>
      </div>

      {/* ── BODY: 2-panel below hero ── */}
      <div className="flex min-h-0 flex-1 overflow-hidden">

        {/* ── LEFT PANEL: scrollable content ── */}
        <div className="hide-scrollbar min-w-0 flex-1 overflow-y-auto">
          <div className="px-[clamp(20px,4vw,80px)] pb-12">

            {/* TOP SECTION: PATTERN READ + BREAKDOWN (Side-by-Side) */}
            <div className="grid items-stretch gap-[clamp(16px,1.8vw,28px)] grid-cols-1 xl:grid-cols-2">
              {/* Pattern Read */}
              <div className="relative flex h-full min-w-0 flex-col justify-between gap-6 overflow-hidden rounded-[24px] border border-black/[0.055] bg-white/88 p-[clamp(20px,2vw,32px)] shadow-[0_18px_42px_-36px_rgba(15,23,42,0.18)] transition-all duration-300 hover:shadow-lg dark:border-white/[0.06] dark:bg-zinc-900/82">
                <div className="flex flex-col gap-6">
                  <div className="flex items-center gap-2.5 text-[14px] font-black uppercase tracking-[0.25em] text-[#E11D48] dark:text-[#FB7185] font-mono">
                    <span className="h-2 w-2 rounded-full bg-[#E11D48] animate-pulse" />
                    Pattern read
                  </div>
                  <p className="text-[clamp(28px,2.2vw,36px)] font-black leading-[1.08] text-[#E11D48] dark:text-[#FB7185] border-l-4 border-[#E11D48] pl-4 italic tracking-tight">
                    {pattern.pattern.the_hook}
                  </p>
                  <p className="text-[clamp(15px,1.05vw,18px)] font-bold leading-relaxed text-black/76 dark:text-zinc-200">
                    {pattern.pattern.why_it_works}
                  </p>
                </div>
                
                {/* Telemetry Footer */}
                <div className="pt-5 flex items-center justify-between border-t border-black/[0.06] dark:border-white/[0.06] text-[8.5px] font-mono uppercase tracking-[0.15em] text-black/32 dark:text-zinc-500">
                  <span>Core Mechanic</span>
                  <span>{pattern.accountMeta || 'Pattern Lift'}</span>
                </div>
              </div>

              {/* Breakdown */}
              <div className="flex h-full min-w-0 flex-col rounded-[24px] border border-black/[0.055] bg-white/88 p-[clamp(18px,1.7vw,28px)] shadow-[0_18px_42px_-36px_rgba(15,23,42,0.18)] transition-all duration-300 hover:shadow-lg dark:border-white/[0.06] dark:bg-zinc-900/82">
                <div className="mb-5 font-mono text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white">Breakdown</div>
                <div className="flex flex-col gap-3.5 w-full items-stretch">
                  {pattern.pattern.the_breakdown.map((item, i) => (
                    <div key={`dbd:${i}`} className="flex w-full min-h-0 items-center gap-[clamp(16px,1.5vw,26px)] rounded-[16px] border border-black/[0.03] bg-black/[0.015] p-[clamp(14px,1.2vw,20px)] transition-colors duration-300 hover:border-[#E11D48]/18 dark:border-white/[0.03] dark:bg-white/[0.015]">
                      <div className="shrink-0 font-mono text-[clamp(36px,2.4vw,48px)] font-black leading-none text-[#E11D48] dark:text-[#FB7185] drop-shadow-[0_2px_4px_rgba(225,29,72,0.15)]">
                        {String(i + 1).padStart(2, '0')}
                      </div>
                      <p className="flex-1 min-w-0 text-[clamp(14.5px,0.92vw,17px)] font-extrabold leading-[1.38] text-black/76 dark:text-zinc-200">
                        {item}
                      </p>
                    </div>
                  ))}
                </div>

                {/* Telemetry Footer */}
                <div className="mt-auto pt-5 flex items-center justify-between border-t border-black/[0.06] dark:border-white/[0.06] text-[8.5px] font-mono uppercase tracking-[0.15em] text-black/32 dark:text-zinc-500">
                  <span>Sequence Steps</span>
                  <span>Verified Signal</span>
                </div>
              </div>
            </div>

            {/* BOTTOM SECTION: GUARDRAILS (Full-Width with Keep & Kills Side-by-Side) */}
            <div className="mt-[clamp(16px,1.8vw,28px)]">
              {/* Guardrails */}
              <div className="relative flex min-w-0 flex-col justify-between gap-6 rounded-[24px] border border-black/[0.055] bg-white/88 p-[clamp(20px,2vw,32px)] shadow-[0_18px_42px_-36px_rgba(15,23,42,0.18)] dark:border-white/[0.06] dark:bg-zinc-900/82">
                <div>
                  <div className="mb-6 text-[14px] font-black uppercase tracking-[0.25em] text-[#E11D48] dark:text-[#FB7185] font-mono">Guardrails</div>
                  <div className="grid grid-cols-1 md:grid-cols-2 gap-8 items-stretch">
                    {pattern.pattern.what_to_keep.length > 0 && (
                      <div className="flex flex-col gap-3">
                        <div className="text-[13px] font-black uppercase tracking-[0.22em] text-[#E11D48] dark:text-[#FB7185] font-mono">Keep</div>
                        <ul className="space-y-4">
                          {pattern.pattern.what_to_keep.map((item, i) => (
                            <li key={`desktop-pattern-keep:${i}`} className="flex gap-4 items-center">
                              <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-[#E11D48] text-[13px] font-black text-white shadow-[0_2px_8px_rgba(225,29,72,0.3)]">
                                +
                              </span>
                              <span className="text-[clamp(14.5px,0.95vw,16.5px)] font-bold leading-[1.35] text-black dark:text-zinc-100">{item}</span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}
                    {pattern.pattern.what_kills_it.length > 0 && (
                      <div className={pattern.pattern.what_to_keep.length > 0 ? "border-t md:border-t-0 md:border-l border-black/[0.06] dark:border-white/[0.06] pt-5 md:pt-0 md:pl-8 flex flex-col gap-3" : "flex flex-col gap-3"}>
                        <div className="text-[13px] font-black uppercase tracking-[0.22em] text-black dark:text-white font-mono">Kills</div>
                        <ul className="space-y-4">
                          {pattern.pattern.what_kills_it.map((item, i) => (
                            <li key={`desktop-pattern-kill:${i}`} className="flex gap-4 items-center">
                              <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded-lg bg-black/10 dark:bg-white/10 text-[13px] font-black text-black/60 dark:text-white/60">
                                -
                              </span>
                              <span className="text-[clamp(14.5px,0.9vw,16px)] font-bold leading-[1.35] text-black/80 dark:text-zinc-300">{item}</span>
                            </li>
                          ))}
                        </ul>
                      </div>
                    )}
                  </div>
                </div>

                {/* Telemetry Footer */}
                <div className="pt-5 flex items-center justify-between border-t border-black/[0.06] dark:border-white/[0.06] text-[8.5px] font-mono uppercase tracking-[0.15em] text-black/32 dark:text-zinc-500">
                  <span>Guardrail Limits</span>
                  <span>Atmospheric Constraint</span>
                </div>
              </div>
            </div>

            {/* ── EVIDENCE CARDS (sleek video-cover horizontal filmstrip) ── */}
            <div className="mt-8 border-t border-black/[0.08] dark:border-white/[0.08] pt-6">
                  <div className="mb-5 font-mono text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white">
                Evidence · {total} proofs — click to read
              </div>
              <div className="grid gap-4 lg:grid-cols-3">
                {pattern.proofs.map((post, i) => {
                  const active = i === proofIndex;
                  return (
                    <motion.button
                      key={post.post_key}
                      type="button"
                      onClick={() => selectProof(i, 'proof')}
                      whileHover={{ y: -6, transition: { duration: 0.2 } }}
                      whileTap={{ scale: 0.985 }}
                      className={[
                        'group/card relative w-full aspect-[16/10] overflow-hidden rounded-[22px] border text-left transition-all duration-300 cursor-pointer shadow-md hover:shadow-xl',
                        active
                          ? 'border-[#E11D48] ring-2 ring-[#E11D48]/30 dark:ring-[#E11D48]/40 shadow-[0_20px_40px_-20px_rgba(225,29,72,0.2)]'
                          : 'border-black/[0.08] dark:border-white/[0.08] bg-[#FAF9F6] dark:bg-[#141416]/20 opacity-75 hover:opacity-100'
                      ].join(' ')}
                    >
                      {/* Full-size media thumbnail behind gradient overlay */}
                      <Thumb post={post} className={['absolute inset-0 h-full w-full object-cover transition-all duration-500 ease-out group-hover/card:scale-[1.035]', active ? '' : 'grayscale opacity-60 contrast-[1.1] group-hover/card:grayscale-0 group-hover/card:opacity-100'].join(' ')} />
                      <div className="absolute inset-0 bg-[linear-gradient(180deg,rgba(0,0,0,0.1)_40%,rgba(0,0,0,0.85)_100%)] z-10 transition-opacity duration-300 group-hover/card:bg-[linear-gradient(180deg,rgba(0,0,0,0.05)_30%,rgba(0,0,0,0.9)_100%)]" />

                      {/* Active Top Bezel Accent */}
                      {active && (
                        <span className="absolute top-0 inset-x-0 h-[3px] bg-[#E11D48] z-20 shadow-[0_2px_10px_rgba(225,29,72,0.6)]" />
                      )}

                      {/* Monospaced Index Tag */}
                      <div className="absolute left-4 top-4 z-20 rounded-md border border-white/10 bg-black/72 px-2 py-0.5 font-mono text-[8px] font-black uppercase tracking-[0.08em] text-white">
                        {String(i + 1).padStart(2, '0')}
                      </div>

                      {/* Editorial Content Overlay inside Thumbnail */}
                      <div className="absolute inset-x-0 bottom-0 z-20 p-5 flex flex-col justify-end">
                        <div className="text-[8px] font-black uppercase tracking-[0.2em] text-[#FB7185] font-mono leading-none">
                          {post.proof_label}
                        </div>
                        <h4 className="mt-2 text-[clamp(14px,1.15vw,17px)] font-black leading-[1.28] text-white transition-colors group-hover/card:text-[#FDA4AF]">
                          {post.proof_headline}
                        </h4>
                        <div className="mt-3 flex items-center gap-1.5 text-[8px] font-mono uppercase tracking-widest text-white/50 opacity-0 transition-all duration-300 translate-y-1 group-hover/card:opacity-100 group-hover/card:translate-y-0">
                          <span>View proof details</span>
                          <ChevronRight size={8} strokeWidth={3} className="transition-transform group-hover/card:translate-x-0.5 text-[#FB7185]" />
                        </div>
                      </div>
                    </motion.button>
                  );
                })}
              </div>
            </div>

            {/* ── PROOF READ ── */}
            <section ref={proofReadRef} className="mt-8 scroll-mt-4 border-t border-black/[0.08] dark:border-white/[0.08] pt-6">
              <div className="flex items-center justify-between gap-4">
                <div className="text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white font-mono">
                  Current read · Proof {proofIndex + 1}/{total}
                </div>
                <div className="rounded-full border border-[#E11D48]/20 bg-[#E11D48]/[0.05] px-3 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-[#E11D48] dark:text-[#FB7185] font-mono">
                  Selected proof
                </div>
              </div>

              <AnimatePresence mode="wait">
                <motion.div
                  key={`proof-read-transition:${proof.post_key}`}
                  initial={{ opacity: 0, y: 15 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -15 }}
                  transition={{ duration: 0.28, ease: APPLE_EASE }}
                >
                  <h3
                    className="mt-4 max-w-none text-[clamp(30px,2.4vw,44px)] font-serif font-semibold leading-[1.1] tracking-tight text-[#08080a] dark:text-zinc-50"
                    style={{ textWrap: 'balance' }}
                  >
                    {proof.proof_headline}
                  </h3>

                  {/* What Clicked: Full-Width Callout quote */}
                  <div className="relative mt-6 overflow-hidden rounded-[22px] border border-black/[0.055] dark:border-white/[0.06] bg-white/80 dark:bg-zinc-900/70 p-6 shadow-[0_18px_48px_-30px_rgba(225,29,72,0.15)] hover:shadow-md transition-shadow duration-300">
                    {/* top indicator bar */}
                    <span className="absolute top-0 inset-x-0 h-[3px] bg-[#E11D48] shadow-[0_1px_8px_rgba(225,29,72,0.4)]" />
                    <div className="text-[13.5px] font-black uppercase tracking-[0.25em] text-[#E11D48] dark:text-[#FB7185] font-mono mb-3.5">What clicked</div>
                    <p className="text-[clamp(22px,1.6vw,28px)] font-black leading-[1.25] text-[#E11D48] dark:text-[#FB7185] italic pl-5 border-l-4 border-[#E11D48] tracking-tight">
                      &ldquo;{proof.what_clicked}&rdquo;
                    </p>
                  </div>

                  {/* Post Read: Full-Width Editorial Block with dynamic phase columns */}
                  <div className="mt-4 rounded-[22px] border border-black/[0.06] dark:border-white/[0.07] bg-white/80 dark:bg-zinc-900/70 p-[clamp(20px,2vw,32px)] shadow-lg hover:shadow-xl transition-shadow duration-300">
                    <div className="text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white font-mono mb-4.5">Post read</div>
                    <div className="grid gap-4 md:grid-cols-2 lg:grid-cols-3">
                      {proofReadParagraphs.map((paragraph, i) => (
                        <div 
                          key={`dpr:${proof.post_key}:${i}`} 
                          className="relative flex flex-col rounded-[20px] border border-black/[0.05] bg-white/90 p-[clamp(16px,1.5vw,24px)] shadow-[0_8px_30px_rgb(0,0,0,0.03)] transition-all duration-300 hover:border-[#E11D48]/30 hover:shadow-lg dark:border-white/[0.06] dark:bg-zinc-950/70 dark:shadow-none"
                        >
                          {/* Phase Indicator */}
                          <div className="flex items-center justify-between mb-4 text-[clamp(13px,0.95vw,14.5px)] font-black uppercase tracking-widest text-[#E11D48] dark:text-[#FB7185] font-mono border-b border-black/[0.05] dark:border-white/[0.05] pb-2.5">
                            <span>{['THE BUILD', 'THE MOVE', 'THE HOLD'][i] ?? `PHASE ${String(i + 1).padStart(2, '0')}`}</span>
                            <span className="h-1.5 w-1.5 rounded-full bg-[#E11D48] animate-pulse" />
                          </div>
                          <p className="text-[clamp(15px,1.05vw,17.5px)] font-extrabold leading-[1.58] text-black/80 dark:text-zinc-200">
                            {paragraph}
                          </p>
                        </div>
                      ))}
                    </div>
                  </div>
                </motion.div>
              </AnimatePresence>
            </section>
          </div>
        </div>

        {/* ── RIGHT PANEL: proof + metrics sidebar ── */}
        <div className="hide-scrollbar w-[clamp(360px,28vw,520px)] shrink-0 overflow-y-auto border-l border-black/[0.08] dark:border-white/[0.08] bg-[linear-gradient(180deg,#ffffff_0%,#faf9f5_100%)] dark:bg-[linear-gradient(180deg,#0a0a0c_0%,#050506_100%)] transition-colors duration-300">
          <div className="px-[clamp(24px,2.4vw,42px)] pb-10 pt-1">

            {/* Feeder weight (pattern context before selected proof) */}
            <div>
              <div className="mb-4 text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white font-mono">Feeder weight</div>
              <div className="grid grid-cols-2 gap-x-4 gap-y-3">
                {patternPerformanceStats.map((metric, index) => {
                  const val = metric.value || '';
                  const isLong = val.length > 8;
                  const sizeClass = isLong
                    ? 'text-[clamp(18px,1.4vw,22px)] font-bold leading-none'
                    : 'text-[clamp(26px,1.9vw,36px)] font-mono font-black leading-none';
                  return (
                    <div key={`dps:${metric.label}:${index}`} className="min-w-0 border-b border-black/[0.06] dark:border-white/[0.06] pb-2.5">
                      <div className={[sizeClass, metric.accent ? 'text-[#E11D48] dark:text-[#FB7185]' : 'text-[#060607] dark:text-zinc-100'].join(' ')}>
                        {metric.value}
                      </div>
                      <div className="mt-1.5 text-[8.5px] font-black uppercase tracking-[0.16em] text-black/32 dark:text-zinc-500">
                        {metric.label} · {metric.detail || 'signal'}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Proof image card */}
            <div className="group/img relative mt-5 overflow-hidden rounded-[18px] border border-black/[0.07] dark:border-white/[0.07] bg-[#111] shadow-[0_20px_48px_-24px_rgba(0,0,0,0.45)]">
              <div className="relative aspect-[3/4] overflow-hidden">
                <AnimatePresence mode="popLayout" initial={false}>
                  <motion.div
                    key={`dimg:${proof.post_key}`}
                    className="absolute inset-0"
                    initial={{ x: direction > 0 ? 50 : -50, opacity: 0.5, scale: 0.97 }}
                    animate={{ x: 0, opacity: 1, scale: 1 }}
                    exit={{ x: direction > 0 ? -50 : 50, opacity: 0.5, scale: 0.97 }}
                    transition={{ duration: 0.28, ease: APPLE_EASE }}
                  >
                    <Thumb post={proof} className="h-full w-full" />
                  </motion.div>
                </AnimatePresence>

                {/* Precision CRT Scanline Grid Overlay */}
                <div className="absolute inset-0 pointer-events-none border border-white/5 bg-[linear-gradient(rgba(255,255,255,0.015)_1px,transparent_1px),linear-gradient(90deg,rgba(255,255,255,0.015)_1px,transparent_1px)] bg-[size:24px_24px] opacity-40 z-10" />

                {/* Diagnostic Central Reticle */}
                <div className="absolute inset-0 pointer-events-none z-10 flex items-center justify-center opacity-30">
                  <div className="h-14 w-14 border border-dashed border-white/20 rounded-full flex items-center justify-center">
                    <div className="h-2 w-2 bg-[#E11D48] rounded-full animate-ping" />
                    <div className="h-1.5 w-1.5 bg-[#E11D48] rounded-full absolute" />
                  </div>
                </div>

                <CornerTicks color={ACCENT} size={9} inset={7} />

                {/* Live Diagnostic Header Labels */}
                <div className="pointer-events-none absolute left-3 top-3 z-10 flex items-center gap-2 rounded-md border border-white/10 bg-black/78 px-2 py-0.5 font-mono text-[8.5px] uppercase tracking-wider text-white">
                  <span className="h-1.5 w-1.5 rounded-full bg-[#E11D48] animate-pulse" />
                  <span>[ANALYZER FEED // LIVE]</span>
                </div>
                <div className="pointer-events-none absolute right-3 top-3 z-10 rounded-md border border-white/10 bg-black/78 px-2 py-0.5 font-mono text-[8.5px] uppercase tracking-wider text-white">
                  <span>FPS: 30 // HD // {proofIndex + 1}/{total}</span>
                </div>

                {/* always-visible blurred glass nav arrows */}
                <button type="button" onClick={() => selectProof((proofIndex - 1 + total) % total)}
                  className="absolute left-3 top-1/2 z-20 flex h-8 w-8 -translate-y-1/2 cursor-pointer items-center justify-center rounded-full border border-white/10 bg-black/58 text-white opacity-70 transition-all hover:scale-105 hover:opacity-100 active:scale-95"
                  aria-label="Previous proof">
                  <ChevronRight size={13} strokeWidth={2.8} className="rotate-180" />
                </button>
                <button type="button" onClick={() => selectProof((proofIndex + 1) % total)}
                  className="absolute right-3 top-1/2 z-20 flex h-8 w-8 -translate-y-1/2 cursor-pointer items-center justify-center rounded-full border border-white/10 bg-black/58 text-white opacity-70 transition-all hover:scale-105 hover:opacity-100 active:scale-95"
                  aria-label="Next proof">
                  <ChevronRight size={13} strokeWidth={2.8} />
                </button>

                {/* Telemetry bottom line */}
                <div className="pointer-events-none absolute bottom-14 left-4 z-20 flex items-center gap-2.5 rounded border border-white/5 bg-black/58 px-2 py-0.5 font-mono text-[8px] uppercase tracking-[0.16em] text-white/50">
                  <span>RATIO: 3:4</span>
                  <span className="h-1 w-1 rounded-full bg-white/20" />
                  <span>BITRATE: 4.8MB/S</span>
                  <span className="h-1 w-1 rounded-full bg-white/20" />
                  <span>KEY: {proof.post_key.slice(0, 8)}</span>
                </div>

                <a
                  href={instagramPostUrl(proof.post_key, proof.post_url)}
                  target="_blank"
                  rel="noopener noreferrer"
                  onClick={(event) => event.stopPropagation()}
                  className="absolute bottom-3 right-3 z-30 inline-flex items-center gap-1.5 rounded-full border border-white/12 bg-black/68 px-3 py-1.5 text-[9px] font-black uppercase tracking-[0.14em] text-white shadow-[0_10px_24px_-16px_rgba(0,0,0,0.8)] transition hover:border-[#FB7185]/60 hover:bg-[#E11D48] hover:text-white active:scale-95"
                  aria-label="Open selected proof on Instagram"
                >
                  <span>Instagram</span>
                  <ArrowUpRight size={11} strokeWidth={3} />
                </a>

                <div className="pointer-events-none absolute inset-x-0 bottom-0 z-10 bg-[linear-gradient(0deg,rgba(0,0,0,0.78),transparent_72%)] px-4 pb-3.5 pt-14">
                  <div className="text-[19px] font-black leading-[1] text-white">{proof.proof_label}</div>
                </div>
              </div>
            </div>

            {/* Signal Bites (selected proof context below thumbnail) */}
            <div className="mt-5 border-t border-black/[0.08] dark:border-white/[0.08] pt-4">
              <div className="mb-3.5 text-[14px] font-black uppercase tracking-[0.25em] text-black dark:text-white font-mono">Signal bites</div>
              <div className="grid grid-cols-2 gap-2.5">
                {proofSignalCards.map((metric) => {
                  const val = metric.value || '';
                  const isLong = val.length > 8;
                  const sizeClass = isLong
                    ? 'text-[clamp(18px,1.4vw,22px)] font-bold leading-none'
                    : 'text-[clamp(26px,2vw,36px)] font-black leading-none';
                  return (
                    <div key={`dsb:${proof.post_key}:${metric.label}`}
                      className={[
                        'relative min-w-0 rounded-xl border pl-4 pr-3 py-3 overflow-hidden transition-all duration-300 hover:scale-[1.02] hover:shadow-md',
                        metric.accent
                          ? 'border-[#E11D48]/20 bg-[#E11D48]/[0.02] dark:bg-[#E11D48]/[0.05]'
                          : 'border-black/[0.06] dark:border-white/[0.06] bg-white dark:bg-zinc-900/60'
                      ].join(' ')}
                    >
                      {/* status accent strip */}
                      <span className={['absolute left-0 top-0 bottom-0 w-[3px]', metric.accent ? 'bg-[#E11D48]' : 'bg-black/10 dark:bg-white/10'].join(' ')} />

                      <div className={[sizeClass, 'font-mono tracking-tight', metric.accent ? 'text-[#E11D48] dark:text-[#FB7185] drop-shadow-[0_2px_4px_rgba(225,29,72,0.15)]' : 'text-[#060607] dark:text-zinc-100'].join(' ')}>
                        {metric.value}
                      </div>
                      <div className="mt-1.5 text-[8.5px] font-black uppercase tracking-[0.14em] text-black/40 dark:text-zinc-500">
                        {metric.label}{metric.detail ? ` · ${metric.detail}` : ''}
                      </div>
                    </div>
                  );
                })}
              </div>
            </div>

            {/* Post evidence list */}
            <div className="mt-6 border-t-2 border-[#E11D48]/20 dark:border-[#E11D48]/30 pt-5">
              <div className="mb-4 text-[14px] font-black uppercase tracking-[0.25em] text-[#E11D48] dark:text-[#FB7185] font-mono">Post evidence</div>
              <div className="grid gap-2.5">
                {proof.evidence.map((item, i) => (
                  <div key={`desktop-rail-evidence:${proof.post_key}:${i}`} className="grid grid-cols-[30px_minmax(0,1fr)] gap-3 rounded-[14px] border border-black/[0.06] dark:border-white/[0.06] bg-white/70 dark:bg-zinc-900/40 p-3.5 shadow-sm hover:scale-[1.01] transition-transform duration-300">
                    <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-lg bg-[#E11D48]/10 text-[10px] font-black text-[#E11D48] dark:text-[#FB7185] font-mono">{i + 1}</span>
                    <span className="text-[clamp(13px,0.86vw,16px)] font-semibold leading-[1.38] text-black/70 dark:text-zinc-300 self-center">{item}</span>
                  </div>
                ))}
              </div>
            </div>
          </div>
        </div>
      </div>
    </motion.div>
  );
}

/* ═══════════════════════════════════════════
   3c. POPUP WRAPPER (responsive)
   ═══════════════════════════════════════════ */

function PatternPopup({
  pattern,
  onClose,
}: {
  pattern: FeederFilePattern | null;
  onClose: () => void;
}) {
  const [proofIndex, setProofIndex] = useState(0);
  const [isClosing, setIsClosing] = useState(false);
  const closeTimerRef = useRef<number | null>(null);
  const [isDesktop, setIsDesktop] = useState(
    () => typeof window !== 'undefined' && window.matchMedia('(min-width: 1024px)').matches,
  );

  const requestClose = useCallback(() => {
    if (isClosing || !pattern) return;
    setIsClosing(true);
    if (closeTimerRef.current !== null) {
      window.clearTimeout(closeTimerRef.current);
    }
    closeTimerRef.current = window.setTimeout(() => {
      closeTimerRef.current = null;
      onClose();
      setIsClosing(false);
    }, isDesktop ? POPUP_DESKTOP_EXIT_MS : POPUP_MOBILE_EXIT_MS);
  }, [isClosing, isDesktop, onClose, pattern]);

  useEffect(() => {
    if (!pattern) return undefined;
    if (closeTimerRef.current !== null) {
      window.clearTimeout(closeTimerRef.current);
      closeTimerRef.current = null;
    }
    const frame = window.requestAnimationFrame(() => {
      setIsClosing(false);
      setProofIndex(0);
    });
    return () => window.cancelAnimationFrame(frame);
  }, [pattern]);

  useEffect(() => {
    return () => {
      if (closeTimerRef.current !== null) {
        window.clearTimeout(closeTimerRef.current);
      }
    };
  }, []);

  useEffect(() => {
    const mql = window.matchMedia('(min-width: 1024px)');
    const handler = (e: MediaQueryListEvent) => setIsDesktop(e.matches);
    mql.addEventListener('change', handler);
    return () => mql.removeEventListener('change', handler);
  }, []);

  // Lock body scroll + keyboard
  useEffect(() => {
    if (!pattern) return undefined;
    const previous = document.documentElement.style.overflow;
    document.documentElement.style.overflow = 'hidden';

    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        requestClose();
        return;
      }
      if (event.key === 'ArrowLeft') {
        setProofIndex((i) => Math.max(0, i - 1));
      } else if (event.key === 'ArrowRight') {
        setProofIndex((i) => Math.min(pattern.proofs.length - 1, i + 1));
      } else if (/^[1-9]$/.test(event.key)) {
        const idx = parseInt(event.key, 10) - 1;
        if (idx >= 0 && idx < pattern.proofs.length) setProofIndex(idx);
      }
    };

    window.addEventListener('keydown', onKeyDown);
    return () => {
      document.documentElement.style.overflow = previous;
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [pattern, requestClose]);

  if (typeof document === 'undefined') return null;

  return createPortal(
    <AnimatePresence>
      {pattern && isDesktop && (
        <DesktopPopup
          key="desktop-popup"
          pattern={pattern}
          proofIndex={proofIndex}
          setProofIndex={setProofIndex}
          onClose={requestClose}
          isClosing={isClosing}
        />
      )}
      {pattern && !isDesktop && (
        <MobilePopup
          key="mobile-popup"
          pattern={pattern}
          proofIndex={proofIndex}
          setProofIndex={setProofIndex}
          onClose={requestClose}
          isClosing={isClosing}
        />
      )}
    </AnimatePresence>,
    document.body,
  );
}

/* ═══════════════════════════════════════════
   4. ROOT COMPONENT
   ═══════════════════════════════════════════ */

export default function FeederFileClient({
  feedId,
  selectedHandle = 'all',
}: FeederFileClientProps) {
  const router = useRouter();
  const { appShellStyle, isStandaloneMode, useTranslucentBrowserChrome } = useMobileImmersiveViewport();
  const bottomClearance = useTranslucentBrowserChrome
    ? 'calc(20px + env(safe-area-inset-bottom))'
    : isStandaloneMode
      ? 'calc(132px + env(safe-area-inset-bottom))'
      : 'calc(96px + env(safe-area-inset-bottom))';

  const [activeAccount, setActiveAccount] = useState<string>(
    () => accountForHandle(selectedHandle) ?? DEFAULT_ACCOUNT,
  );
  const [avatarUrls, setAvatarUrls] = useState<FeedAvatarMap>({});
  const [dbPatterns, setDbPatterns] = useState<FeederFilePattern[]>([]);
  const [patternsLoaded, setPatternsLoaded] = useState(false);

  const anchorAccount = useMemo(() => {
    return accountForHandle(selectedHandle) ?? DEFAULT_ACCOUNT;
  }, [selectedHandle]);

  const [activePattern, setActivePattern] = useState<FeederFilePattern | null>(null);
  const sourcePatterns = dbPatterns;
  const accounts = useMemo(() => {
    const next = Array.from(new Set(sourcePatterns.map((pattern) => pattern.account).filter(Boolean)));
    return next;
  }, [sourcePatterns]);
  const selectedHandleAccount = accountForHandle(selectedHandle);
  const selectedAccount = accounts.includes(activeAccount)
    ? activeAccount
    : selectedHandleAccount && accounts.includes(selectedHandleAccount)
      ? selectedHandleAccount
      : accounts[0] || selectedHandleAccount || DEFAULT_ACCOUNT;
  const visiblePatterns = useMemo(() => patternsForAccount(selectedAccount, sourcePatterns), [selectedAccount, sourcePatterns]);
  const renderablePatterns = useMemo(() => visiblePatterns.filter(hasRenderableProofs), [visiblePatterns]);
  const activeFeed = renderablePatterns[0] ?? visiblePatterns[0] ?? null;
  const activeAccountLabel = activeFeed?.account ?? (patternsLoaded ? selectedAccount || 'Feeder File' : 'Loading feeder file');
  const activeAccountMeta = activeFeed?.accountMemoryMeta ?? activeFeed?.accountMeta ?? (
    patternsLoaded
      ? 'Waiting for D7-qualified pattern reads'
      : 'Reading official DB payloads'
  );

  useEffect(() => {
    let cancelled = false;
    setDbPatterns([]);
    setPatternsLoaded(false);

    async function loadFeederFilePatterns() {
      try {
        const params = new URLSearchParams({ feedId });
        const response = await fetch(`/api/feed/feeder-file?${params.toString()}`, { cache: 'no-store', credentials: 'include' });
        if (!response.ok) {
          if (!cancelled) setDbPatterns([]);
          return;
        }
        const payload = await response.json() as { patterns?: FeederFilePattern[] };
        if (!cancelled) setDbPatterns(Array.isArray(payload.patterns) ? payload.patterns : []);
      } catch {
        if (!cancelled) setDbPatterns([]);
      } finally {
        if (!cancelled) setPatternsLoaded(true);
      }
    }

    loadFeederFilePatterns();

    return () => {
      cancelled = true;
    };
  }, [feedId]);

  useEffect(() => {
    let cancelled = false;

    async function loadFeederAvatars() {
      try {
        const response = await fetch('/api/feed', { cache: 'no-store', credentials: 'include' });
        if (!response.ok) return;
        const payload = await response.json() as {
          feeds?: Array<{
            feeders?: Array<{
              handle?: string | null;
              profilePicUrl?: string | null;
            }>;
          }>;
        };
        const next: FeedAvatarMap = {};

        for (const feed of payload.feeds || []) {
          for (const feeder of feed.feeders || []) {
            const account = accountForHandle(feeder.handle);
            if (account && feeder.profilePicUrl && !next[account]) {
              next[account] = feeder.profilePicUrl;
            }
          }
        }

        if (!cancelled) setAvatarUrls(next);
      } catch {
        if (!cancelled) setAvatarUrls({});
      }
    }

    loadFeederAvatars();

    return () => {
      cancelled = true;
    };
  }, []);

  return (
    <div
      className="relative min-h-[100dvh] overflow-x-hidden bg-[linear-gradient(180deg,#fff,#fafafa)] text-foreground dark:bg-[linear-gradient(180deg,#08080a,#050506)] dark:text-white"
      style={{ ...appShellStyle, paddingBottom: bottomClearance } as CSSProperties}
    >
      <main className="relative mx-auto flex w-full max-w-[1500px] flex-col px-3 pb-10 pt-[calc(16px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-5 lg:px-8">

        {/* ── masthead + rings ── */}
        <div className="-mx-3 overflow-hidden sm:mx-0">
          <div className="flex items-center gap-3 rounded-none bg-white/96 px-5 pb-4 pt-3 dark:bg-[#08080a]/96 sm:gap-2 sm:rounded-b-[18px] sm:px-0 sm:pb-3 sm:pt-0 sm:backdrop-blur-xl lg:bg-white/96 lg:backdrop-blur-none lg:dark:bg-[#08080a]/96">
            <button
              type="button"
              onClick={() => router.push(`/?id=${feedId}`, { scroll: false })}
              className="group/back -ml-1 flex h-11 w-11 shrink-0 items-center justify-center rounded-full bg-transparent text-black transition-all hover:bg-black/[0.04] active:scale-95 dark:text-white dark:hover:bg-white/[0.07] sm:h-8 sm:w-8"
              aria-label="Back to feed dashboard"
            >
              <ArrowLeft className="h-8 w-8 transition-transform group-hover/back:-translate-x-0.5 sm:h-5 sm:w-5" strokeWidth={3.35} />
            </button>

            <div className="flex min-w-0 flex-1 flex-col justify-center gap-1 sm:gap-0.5">
              <h1 className="truncate font-mono text-[33px] font-black uppercase leading-none tracking-[0.02em] text-black dark:text-white sm:text-[22px] sm:leading-none sm:tracking-[0.04em]">
                Feeder File
              </h1>
              <span className="font-mono text-[9px] font-black uppercase tracking-[0.24em] text-black/34 dark:text-white/30 sm:text-[8px] sm:tracking-[0.14em] sm:text-black/26 sm:dark:text-white/24">
                Content intelligence
              </span>
            </div>
          </div>

          <div className="rounded-none bg-white/94 px-0 pb-5 pt-2 dark:bg-[#08080a]/94 sm:rounded-b-[22px] sm:px-4 sm:pb-4 sm:backdrop-blur-xl lg:bg-white/94 lg:backdrop-blur-none lg:dark:bg-[#08080a]/94">
            <StoryStrip
              accounts={accounts}
              activeAccount={selectedAccount}
              anchorAccount={anchorAccount}
              avatarUrls={avatarUrls}
              onSelectAccount={setActiveAccount}
            />
          </div>
        </div>

        {/* ── "Feeding on @handle" hero section ── */}
        <section className="mt-6 overflow-hidden sm:mt-8">
          <motion.div
            initial={{ opacity: 0, y: 10, filter: 'blur(6px)' }}
            animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
            transition={{
              opacity: { duration: 0.24, ease: GRID_ITEM_EASE },
              y: FEEDER_DECK_SWAP_SPRING,
              filter: { duration: 0.26, ease: GRID_ITEM_EASE },
            }}
            style={{ willChange: 'opacity, transform, filter' }}
          >
            <div className="flex items-center gap-2 text-[10px] font-black uppercase tracking-[0.18em] text-[#E11D48]">
              <span className="h-2 w-2 rounded-full bg-[#E11D48]" />
              Feeding on
            </div>
            <h2 className="mt-2 min-h-[1em] text-[36px] font-black leading-none tracking-normal text-black dark:text-white sm:text-[48px] lg:text-[54px]">
              <TypebackText value={activeAccountLabel} />
            </h2>
            <p className="mt-2 text-[11px] font-black uppercase tracking-[0.12em] text-black/34 dark:text-white/28">
              {activeAccountMeta}
            </p>
          </motion.div>
        </section>

        {/* ── pattern cards grid ── */}
        <section className="mt-2 sm:mt-8">
          <AnimatePresence initial={false} mode="sync">
            <motion.div
              key={`feeder-patterns:${selectedAccount}`}
              data-feeder-pattern-deck="true"
              className="grid gap-8 sm:gap-10 xl:grid-cols-2"
              initial={{ opacity: 0, y: 22, scale: 0.985, filter: 'blur(10px)' }}
              animate={{ opacity: 1, y: 0, scale: 1, filter: 'blur(0px)' }}
              exit={{ opacity: 0, y: -14, scale: 0.992, filter: 'blur(8px)' }}
              transition={{
                opacity: { duration: 0.22, ease: GRID_ITEM_EASE },
                y: FEEDER_DECK_SWAP_SPRING,
                scale: FEEDER_DECK_SWAP_SPRING,
                filter: { duration: 0.26, ease: GRID_ITEM_EASE },
              }}
              style={{ willChange: 'opacity, transform, filter' }}
            >
              <AnimatePresence mode="popLayout">
                {renderablePatterns.map((pattern, i) => (
                  <PatternCard
                    key={`${pattern.account}:${pattern.pattern_id}`}
                    pattern={pattern}
                    patternIndex={i}
                    onOpen={setActivePattern}
                  />
                ))}
              </AnimatePresence>
              {renderablePatterns.length === 0 && (
                <motion.div
                  layout
                  className="rounded-[22px] border border-black/[0.06] bg-white/76 p-6 text-[12px] font-bold leading-relaxed text-black/46 shadow-[inset_0_1px_0_rgba(255,255,255,0.72)] dark:border-white/[0.08] dark:bg-white/[0.04] dark:text-white/40"
                  initial={{ opacity: 0, y: 12, scale: 0.985 }}
                  animate={{ opacity: 1, y: 0, scale: 1 }}
                  exit={{ opacity: 0, y: -8, scale: 0.975 }}
                  transition={{
                    layout: GRID_LAYOUT_SPRING,
                    opacity: { duration: 0.2, ease: GRID_ITEM_EASE },
                    y: { duration: 0.28, ease: GRID_ITEM_EASE },
                    scale: { duration: 0.28, ease: GRID_ITEM_EASE },
                  }}
                >
                  {patternsLoaded
                    ? 'Feeder file pattern reads will appear after D7-qualified posts generate fingerprints, post breakdowns, and a compiled feeder file.'
                    : 'Loading feeder file pattern reads from the database.'}
                </motion.div>
              )}
            </motion.div>
          </AnimatePresence>
        </section>
      </main>

      {/* ── popup ── */}
      <PatternPopup
        key={activePattern ? `${activePattern.account}:${activePattern.pattern_id}` : 'closed'}
        pattern={activePattern}
        onClose={() => setActivePattern(null)}
      />
    </div>
  );
}
