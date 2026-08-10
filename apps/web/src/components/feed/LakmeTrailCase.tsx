'use client';

import Image from 'next/image';
import { useEffect, useRef, useState } from 'react';
import { AnimatePresence, cubicBezier, motion, type PanInfo, useReducedMotion } from 'framer-motion';
import { RotateCcw, X } from 'lucide-react';
import { useAppHaptics } from '@/lib/haptics';
import styles from './LakmeTrailCase.module.css';

type TrailRole = 'run' | 'history';

type TrailPost = {
  id: string;
  name: string;
  date: string;
  landing: number;
  topPercent: number;
  multiple: number;
  angle: number;
  role: TrailRole;
  image: string;
  objectPosition?: string;
  construction: string;
  fingerprint: string;
};

type StageSize = { width: number; height: number };
type Point = { x: number; y: number };
type Geometry = { center: Point; innerRadius: number; outerRadius: number };
type SequencePhase = 'lines' | 'sweep' | 'cards';

const ACCENT = '#ff174f';
const BANDS = [20, 40, 60, 80, 100];
const FOCUS_POST_ID = 'reset-color-trick';
const ZONE_COLORS = ['#090909', '#0d0d0d', '#080808', '#101010', '#0a0a0a'];
const ANIMATION_ORDER = [
  'swatch-the-shades',
  'download-in-progress',
  'pose-crash-cut',
  'cycle-through-trends',
  'reset-color-trick',
  'trend-card-proof',
  'mirror-apply',
  'swap-ice-sorbet',
  'snap-cooling-switch',
  'all-day-lip-stain-test',
];
const TRAJECTORY_START = 260;
const POST_STAGGER = 80;
const TRAJECTORY_DURATION = 900;
const SWEEP_LEAD_IN = 220;
const SWEEP_DURATION = 1150;
const LINE_FADE_FRACTION = 0.055;
const CARD_ENTER_DURATION = 260;
const SWEEP_SEQUENCE_DURATION = SWEEP_LEAD_IN + SWEEP_DURATION + CARD_ENTER_DURATION;
const TRAJECTORY_SEQUENCE_DURATION = TRAJECTORY_START + (ANIMATION_ORDER.length - 1) * POST_STAGGER + TRAJECTORY_DURATION;
const SPREAD_EASE = cubicBezier(0.32, 0.72, 0, 1);
const HANDOFF_EASE = cubicBezier(0.77, 0, 0.175, 1);

const POSTS: TrailPost[] = [
  {
    id: 'snap-cooling-switch',
    name: 'Snap Cooling Switch',
    date: '20 May',
    landing: 86,
    topPercent: 13,
    multiple: 3.53,
    angle: -165,
    role: 'history',
    image: '/lakme-case/snap-cooling-switch.jpg',
    construction: 'A cooling claim becomes a quick prop switch.',
    fingerprint: 'Cold prop → product jar → sorbet prop → logo exit',
  },
  {
    id: 'all-day-lip-stain-test',
    name: 'All-day Lip Stain Test',
    date: '25 May',
    landing: 90,
    topPercent: 18,
    multiple: 2.8,
    angle: 136,
    role: 'history',
    image: '/lakme-case/reset-color-trick.jpg',
    construction: 'A wear test turns the product claim into a timed reveal.',
    fingerprint: 'Fresh application → day-in-motion cuts → stain check → product close',
  },
  {
    id: 'swap-ice-sorbet',
    name: 'Swap Ice to Sorbet',
    date: '30 May',
    landing: 67,
    topPercent: 11,
    multiple: 3.7,
    angle: 7,
    role: 'history',
    image: '/lakme-case/swap-ice-to-sorbet.jpg',
    construction: 'A familiar ice test is replaced by the product.',
    fingerprint: 'Calendar → ice dunk → product application → logo end',
  },
  {
    id: 'mirror-apply',
    name: 'Mirror-apply Makeup',
    date: '10 Jun',
    landing: 47,
    topPercent: 7,
    multiple: 4.62,
    angle: 161,
    role: 'history',
    image: '/lakme-case/mirror-apply-makeup.jpg',
    construction: 'The application becomes a workplace joke.',
    fingerprint: 'Meme text → laptop-as-mirror application → three-person sequence',
  },
  {
    id: 'trend-card-proof',
    name: 'Trend Card Proof',
    date: '11 Jun',
    landing: 30,
    topPercent: 21,
    multiple: 2.22,
    angle: -58,
    role: 'history',
    image: '/lakme-case/cycle-through-trends.jpg',
    objectPosition: '50% 28%',
    construction: 'The product proof is delivered inside a trend-card beat.',
    fingerprint: 'Prompt card → deadpan reaction → product proof → branded close',
  },
  {
    id: 'swatch-the-shades',
    name: 'Swatch the Shades',
    date: '12 Jun',
    landing: 58,
    topPercent: 20,
    multiple: 2.22,
    angle: -104,
    role: 'run',
    image: '/lakme-case/swatch-the-shades.jpg',
    construction: 'The shade range becomes the repeated action.',
    fingerprint: 'Shade on pencil → repeated arm swatches → full lineup → logo',
  },
  {
    id: 'download-in-progress',
    name: 'Download in Progress',
    date: '14 Jun',
    landing: 69,
    topPercent: 31,
    multiple: 2.1,
    angle: 111,
    role: 'run',
    image: '/lakme-case/cycle-through-trends.jpg',
    objectPosition: '50% 24%',
    construction: 'A screen-like progress cue delays the product reveal.',
    fingerprint: 'Loading overlay → held reaction → product appearance → branded close',
  },
  {
    id: 'pose-crash-cut',
    name: 'Pose Crash Cut',
    date: '12 Jun',
    landing: 72,
    topPercent: 9,
    multiple: 5.12,
    angle: 45,
    role: 'run',
    image: '/lakme-case/pose-crash-cut.jpg',
    construction: 'A polished pose is interrupted by the proof beat.',
    fingerprint: 'Beauty pose → abrupt interruption → product answer → close-up',
  },
  {
    id: 'cycle-through-trends',
    name: 'Cycle Through Trends',
    date: '11 Jun',
    landing: 84,
    topPercent: 11,
    multiple: 5.99,
    angle: -130,
    role: 'run',
    image: '/lakme-case/cycle-through-trends.jpg',
    objectPosition: '50% 64%',
    construction: 'Trend cards turn a product set into a character beat.',
    fingerprint: 'BTS interview → trend cards → deadpan reaction → branded close-up',
  },
  {
    id: 'reset-color-trick',
    name: 'Reset the Color Trick',
    date: '13 Jun',
    landing: 97,
    topPercent: 3,
    multiple: 357.9,
    angle: -38,
    role: 'run',
    image: '/lakme-case/reset-color-trick.jpg',
    construction: 'The failed trick creates the question; the product answers it.',
    fingerprint: 'Magic trick → failed red check → hand-cover reset → product reveal',
  },
];

function clamp(value: number, min = 0, max = 1) {
  return Math.min(max, Math.max(min, value));
}

function easeOutCubic(value: number) {
  return 1 - Math.pow(1 - clamp(value), 3);
}

function animationIndex(post: TrailPost) {
  return ANIMATION_ORDER.indexOf(post.id);
}

function radarRevealFraction(post: TrailPost) {
  return ((post.angle + 90 + 360) % 360) / 360;
}

function geometryFor(size: StageSize): Geometry {
  const extent = Math.min(size.width, size.height);
  const outerRadius = Math.max(0, extent / 2 - Math.max(5, extent * 0.012));
  return {
    center: { x: size.width / 2, y: size.height / 2 },
    innerRadius: outerRadius * 0.24,
    outerRadius,
  };
}

function radiusForLanding(landing: number, geometry: Geometry) {
  return geometry.innerRadius + (geometry.outerRadius - geometry.innerRadius) * clamp(landing / 100);
}

function pointFor(post: TrailPost, size: StageSize): Point {
  const geometry = geometryFor(size);
  const angle = (post.angle * Math.PI) / 180;
  const radius = radiusForLanding(post.landing, geometry);
  const halfWidth = markerSize(post, size) / 2 + 1;
  const halfHeight = markerHeight(post, size) / 2 + 1;
  return {
    x: clamp(geometry.center.x + Math.cos(angle) * radius, halfWidth, size.width - halfWidth),
    y: clamp(geometry.center.y + Math.sin(angle) * radius, halfHeight, size.height - halfHeight),
  };
}

function markerSize(post: TrailPost, size: StageSize) {
  const base = clamp(size.width * 0.135, 50, 72);
  return Math.round(base + (post.role === 'run' ? 2 : 0) + (post.id === FOCUS_POST_ID ? 10 : 0));
}

function markerHeight(post: TrailPost, size: StageSize) {
  return Math.round(markerSize(post, size) * 1.18);
}

function formatMultiple(value: number) {
  return `${value >= 100 ? value.toFixed(1) : value.toFixed(value >= 10 ? 1 : 2)}×`;
}

function roleLabel(role: TrailRole) {
  return role === 'run' ? 'From this run' : 'Earlier reference';
}

function OrbitCanvas({ size, replayKey, reducedMotion, phase }: { size: StageSize; replayKey: number; reducedMotion: boolean; phase: SequencePhase }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || size.width <= 0 || size.height <= 0) return undefined;

    const context = canvas.getContext('2d');
    if (!context) return undefined;

    const ratio = Math.min(Math.max(window.devicePixelRatio || 1, 1), 2);
    canvas.width = Math.round(size.width * ratio);
    canvas.height = Math.round(size.height * ratio);
    canvas.style.width = `${size.width}px`;
    canvas.style.height = `${size.height}px`;
    context.setTransform(ratio, 0, 0, ratio, 0, 0);

    const geometry = geometryFor(size);
    const ringSpan = geometry.outerRadius - geometry.innerRadius;
    const start = performance.now();
    let frame = 0;

    const strokeArc = (radius: number, startAngle: number, endAngle: number, anticlockwise = false) => {
      context.beginPath();
      context.arc(geometry.center.x, geometry.center.y, radius, startAngle, endAngle, anticlockwise);
      context.stroke();
    };

    const fillRail = (origin: Point, current: Point, post: TrailPost, opacity = 1) => {
      const dx = current.x - origin.x;
      const dy = current.y - origin.y;
      const length = Math.hypot(dx, dy);
      if (length < 1) return;

      const isRun = post.role === 'run';
      context.lineCap = 'butt';
      context.globalAlpha = 0.92 * opacity;
      context.strokeStyle = '#000';
      context.lineWidth = isRun ? 7 : 6;
      context.shadowColor = 'transparent';
      context.shadowBlur = 0;
      context.beginPath();
      context.moveTo(origin.x, origin.y);
      context.lineTo(current.x, current.y);
      context.stroke();

      context.globalAlpha = (isRun ? 1 : 0.92) * opacity;
      context.strokeStyle = isRun ? ACCENT : '#efefea';
      context.lineWidth = isRun ? 3.1 : 2.2;
      context.shadowColor = 'transparent';
      context.shadowBlur = 0;
      context.beginPath();
      context.moveTo(origin.x, origin.y);
      context.lineTo(current.x, current.y);
      context.stroke();

      context.shadowColor = 'transparent';
      context.shadowBlur = 0;
    };

    const drawRadar = (angle: number, tailLength: number, alpha: number, needleProgress = 1) => {
      context.save();
      context.lineCap = 'round';
      BANDS.forEach((band, index) => {
        const gateStart = (index / BANDS.length) * 0.72;
        const bandProgress = clamp((needleProgress - gateStart) / 0.28);
        if (bandProgress <= 0) return;

        const radius = geometry.innerRadius + ringSpan * (band / 100) - 3;
        const phaseOffset = (index - (BANDS.length - 1) / 2) * 0.018;
        const head = angle + phaseOffset;
        const bandTail = tailLength * (0.78 + index * 0.055) * bandProgress;
        const hotLength = Math.min(bandTail, 0.05);

        context.globalAlpha = alpha * bandProgress * 0.96;
        context.strokeStyle = '#000';
        context.lineWidth = 5.2;
        context.shadowColor = 'transparent';
        context.shadowBlur = 0;
        strokeArc(radius, head - bandTail, head);

        context.strokeStyle = '#ff315f';
        context.lineWidth = 2.15 + index * 0.1;
        context.shadowColor = 'rgba(255, 23, 79, 0.38)';
        context.shadowBlur = 4;
        strokeArc(radius, head - bandTail, head);

        context.globalAlpha = alpha * bandProgress;
        context.strokeStyle = '#ffd5de';
        context.lineWidth = 0.85;
        context.shadowColor = 'transparent';
        context.shadowBlur = 0;
        strokeArc(radius, head - hotLength, head + 0.008);
      });
      context.restore();
    };

    const draw = (now: number) => {
      const elapsed = reducedMotion ? 0 : now - start;

      context.clearRect(0, 0, size.width, size.height);
      context.lineCap = 'round';
      context.lineJoin = 'miter';

      BANDS.forEach((band, index) => {
        const inner = geometry.innerRadius + ringSpan * ((band - 20) / 100);
        const outer = geometry.innerRadius + ringSpan * (band / 100);

        context.save();
        context.fillStyle = ZONE_COLORS[index];
        context.beginPath();
        context.arc(geometry.center.x, geometry.center.y, outer, 0, Math.PI * 2);
        context.arc(geometry.center.x, geometry.center.y, inner, 0, Math.PI * 2, true);
        context.fill('evenodd');
        context.restore();
      });

      BANDS.forEach((band) => {
        const radius = geometry.innerRadius + ringSpan * (band / 100);
        context.save();
        context.globalAlpha = 1;
        context.strokeStyle = '#000';
        context.lineWidth = band === 100 ? 9 : 7;
        strokeArc(radius, 0, Math.PI * 2);
        context.globalAlpha = band === 100 ? 0.5 : 0.15;
        context.strokeStyle = band === 100 ? '#777770' : '#f5f5f1';
        context.lineWidth = band === 100 ? 1.4 : 0.8;
        strokeArc(radius - (band === 100 ? 4 : 3), 0, Math.PI * 2);
        context.restore();
      });

      POSTS.forEach((post) => {
        if (phase !== 'lines' && phase !== 'sweep') return;

        const endpoint = pointFor(post, size);
        const angle = (post.angle * Math.PI) / 180;
        const originRadius = geometry.innerRadius + 2;
        const origin = {
          x: geometry.center.x + Math.cos(angle) * originRadius,
          y: geometry.center.y + Math.sin(angle) * originRadius,
        };
        const order = animationIndex(post);
        const launchTime = TRAJECTORY_START + order * POST_STAGGER;
        const progress = phase === 'lines'
          ? reducedMotion ? 1 : SPREAD_EASE(clamp((elapsed - launchTime) / TRAJECTORY_DURATION))
          : 1;
        const scanProgress = phase === 'sweep'
          ? clamp((elapsed - SWEEP_LEAD_IN) / SWEEP_DURATION)
          : 0;
        const revealAt = radarRevealFraction(post);
        const opacity = phase === 'sweep' && elapsed >= SWEEP_LEAD_IN
          ? clamp((revealAt - scanProgress) / LINE_FADE_FRACTION)
          : 1;
        if (progress <= 0) return;

        const current = {
          x: origin.x + (endpoint.x - origin.x) * progress,
          y: origin.y + (endpoint.y - origin.y) * progress,
        };
        context.save();
        fillRail(origin, current, post, opacity);
        context.restore();
      });

      if (!reducedMotion && phase === 'sweep' && elapsed <= SWEEP_LEAD_IN) {
        const chargeProgress = HANDOFF_EASE(clamp(elapsed / SWEEP_LEAD_IN));
        drawRadar(-Math.PI / 2, 0.11 * chargeProgress, chargeProgress, chargeProgress);
      }

      if (!reducedMotion && phase === 'sweep' && elapsed > SWEEP_LEAD_IN && elapsed <= SWEEP_LEAD_IN + SWEEP_DURATION) {
        const sweepElapsed = elapsed - SWEEP_LEAD_IN;
        const scanProgress = clamp(sweepElapsed / SWEEP_DURATION);
        const scanOut = easeOutCubic((SWEEP_DURATION - sweepElapsed) / 260);
        const angle = -Math.PI / 2 + scanProgress * Math.PI * 2;
        const tailLength = 0.12 + 0.16 * SPREAD_EASE(clamp(sweepElapsed / 520));
        drawRadar(angle, tailLength, scanOut);
      }

      const trajectoryActive = phase === 'lines' && elapsed < TRAJECTORY_SEQUENCE_DURATION;
      const sweepActive = phase === 'sweep' && elapsed < SWEEP_LEAD_IN + SWEEP_DURATION;
      if (!reducedMotion && (trajectoryActive || sweepActive)) frame = requestAnimationFrame(draw);
    };

    frame = requestAnimationFrame(draw);
    return () => cancelAnimationFrame(frame);
  }, [phase, reducedMotion, replayKey, size]);

  return <canvas ref={canvasRef} className={styles.canvas} aria-hidden="true" />;
}

export default function LakmeTrailCase() {
  const stageRef = useRef<HTMLDivElement>(null);
  const reducedMotionSwipeStart = useRef<number | null>(null);
  const prefersReducedMotion = useReducedMotion();
  const reducedMotion = Boolean(prefersReducedMotion);
  const [stageSize, setStageSize] = useState<StageSize>({ width: 0, height: 0 });
  const [selectedId, setSelectedId] = useState<string | null>(null);
  const [replayKey, setReplayKey] = useState(0);
  const [sequencePhase, setSequencePhase] = useState<SequencePhase>('lines');
  const [revealedIds, setRevealedIds] = useState<Set<string>>(() => new Set());
  const { play, isSupported: hapticsSupported } = useAppHaptics();
  const selected = selectedId ? POSTS.find((post) => post.id === selectedId) ?? null : null;
  const visibleSequencePhase = sequencePhase;

  useEffect(() => {
    const stage = stageRef.current;
    if (!stage) return undefined;

    const observer = new ResizeObserver(([entry]) => {
      const width = Math.round(entry.contentRect.width);
      const height = Math.round(entry.contentRect.height);
      setStageSize((current) => (current.width === width && current.height === height ? current : { width, height }));
    });
    observer.observe(stage);
    return () => observer.disconnect();
  }, []);

  useEffect(() => {
    if (reducedMotion) return undefined;
    if (sequencePhase === 'sweep') {
      const revealTimers = POSTS.map((post) => window.setTimeout(() => {
        setRevealedIds((current) => {
          const next = new Set(current);
          next.add(post.id);
          return next;
        });
      }, SWEEP_LEAD_IN + radarRevealFraction(post) * SWEEP_DURATION));
      const cardsTimer = window.setTimeout(() => setSequencePhase('cards'), SWEEP_SEQUENCE_DURATION);
      return () => {
        revealTimers.forEach((timer) => window.clearTimeout(timer));
        window.clearTimeout(cardsTimer);
      };
    }
    return undefined;
  }, [reducedMotion, sequencePhase]);

  useEffect(() => {
    if (!selectedId) return undefined;
    const closeOnEscape = (event: KeyboardEvent) => {
      if (event.key === 'Escape') setSelectedId(null);
    };
    window.addEventListener('keydown', closeOnEscape);
    return () => window.removeEventListener('keydown', closeOnEscape);
  }, [selectedId]);

  const selectPost = (post: TrailPost) => {
    setSelectedId(post.id);
    if (hapticsSupported) play('snapLock');
  };

  const replay = () => {
    setSelectedId(null);
    setRevealedIds(new Set());
    setSequencePhase('lines');
    setReplayKey((current) => current + 1);
    if (hapticsSupported) play('navReselect');
  };

  const revealPosts = () => {
    if (sequencePhase !== 'lines') return;
    setRevealedIds(new Set());
    setSequencePhase(reducedMotion ? 'cards' : 'sweep');
    if (hapticsSupported) play('snapLock');
  };

  const onSwipeEnd = (_: unknown, info: PanInfo) => {
    if (Math.abs(info.offset.y) < 64) return;
    if (sequencePhase === 'lines' && info.offset.y < 0) revealPosts();
    else if ((sequencePhase === 'cards' || sequencePhase === 'sweep') && info.offset.y > 0) replay();
  };

  return (
    <main className={styles.root}>
      <div className={styles.shell}>
        <header className={styles.header}>
          <div>
            <p className={styles.handle}>@lakmeindia</p>
            <p className={styles.accountMeta}>Product stress tests</p>
          </div>
          <div className={styles.headerRight}>
            <p className={styles.window}>Current 100 · 90D</p>
            <button type="button" className={styles.replayButton} onClick={replay} aria-label="Replay the trajectories">
              <RotateCcw aria-hidden="true" />
            </button>
          </div>
        </header>

        <section className={styles.experience} aria-labelledby="lakme-case-title">
          <div className={styles.orbitWrap}>
            <motion.div
              ref={stageRef}
              className={styles.orbitStage}
              data-phase={visibleSequencePhase}
              data-testid="lakme-trail-circle"
              role="group"
              tabIndex={0}
              aria-label={visibleSequencePhase === 'lines' ? 'Trajectory view. Swipe up to reveal posts.' : visibleSequencePhase === 'cards' ? 'Post view. Swipe down to return to trajectories.' : 'Reading the account trail.'}
              drag="y"
              dragConstraints={{ top: 0, bottom: 0 }}
              dragElastic={0.015}
              dragMomentum={false}
              dragTransition={{ bounceStiffness: 1000, bounceDamping: 100 }}
              dragSnapToOrigin
              onDragEnd={onSwipeEnd}
              onPointerDown={(event) => {
                if (!reducedMotion) return;
                reducedMotionSwipeStart.current = event.clientY;
                event.currentTarget.setPointerCapture(event.pointerId);
              }}
              onPointerUp={(event) => {
                if (!reducedMotion || reducedMotionSwipeStart.current === null) return;
                const offsetY = event.clientY - reducedMotionSwipeStart.current;
                reducedMotionSwipeStart.current = null;
                if (Math.abs(offsetY) < 64) return;
                if (sequencePhase === 'lines' && offsetY < 0) revealPosts();
                else if (sequencePhase === 'cards' && offsetY > 0) replay();
              }}
              onPointerCancel={() => {
                reducedMotionSwipeStart.current = null;
              }}
              onKeyDown={(event) => {
                if (sequencePhase === 'lines' && (event.key === 'ArrowUp' || event.key === 'Enter' || event.key === ' ')) {
                  event.preventDefault();
                  revealPosts();
                } else if ((sequencePhase === 'cards' || sequencePhase === 'sweep') && event.key === 'ArrowDown') {
                  event.preventDefault();
                  replay();
                }
              }}
              style={{ touchAction: 'pan-x' }}
            >
              <OrbitCanvas size={stageSize} replayKey={replayKey} reducedMotion={reducedMotion} phase={visibleSequencePhase} />

              {POSTS.map((post) => {
                const point = pointFor(post, stageSize);
                const visibleWidth = markerSize(post, stageSize);
                const visibleHeight = markerHeight(post, stageSize);
                const isFocusPost = post.id === FOCUS_POST_ID;
                const showCard = visibleSequencePhase === 'cards' || (visibleSequencePhase === 'sweep' && revealedIds.has(post.id));

                return (
                  <button
                    key={post.id}
                    type="button"
                    className={`${styles.nodeButton} ${styles[`nodeButton${post.role[0].toUpperCase()}${post.role.slice(1)}`]} ${isFocusPost ? styles.nodeButtonFocus : ''}`}
                    style={{ left: point.x, top: point.y, zIndex: post.role === 'run' ? 12 : 8 }}
                    onClick={() => selectPost(post)}
                    disabled={visibleSequencePhase !== 'cards'}
                    aria-label={`${post.name}, Top ${post.topPercent}%, ${formatMultiple(post.multiple)} baseline, ${roleLabel(post.role)}`}
                  >
                    <span
                      className={styles.nodeCard}
                      data-visible={showCard ? 'true' : 'false'}
                      style={{
                        width: visibleWidth,
                        height: visibleHeight,
                        display: showCard ? 'block' : 'none',
                      }}
                    >
                      <span className={styles.nodeImage}>
                        <Image
                          src={post.image}
                          alt=""
                          fill
                          sizes={`${visibleWidth}px`}
                          priority={post.role === 'run'}
                          style={{ objectPosition: post.objectPosition ?? '50% 38%' }}
                        />
                      </span>
                      {isFocusPost ? (
                        <span className={styles.nodeMetric}>
                          <strong>Top 3%</strong>
                          <span>357.9×</span>
                        </span>
                      ) : null}
                    </span>
                  </button>
                );
              })}

              <div className={styles.centerTitle} aria-hidden="true">
                <strong id="lakme-case-title">The test<br />becomes<br />the show</strong>
              </div>
            </motion.div>
          </div>

        </section>

        <footer className={styles.footer}>
          <p>Farther out = stronger landing</p>
          <span aria-live="polite">
            {visibleSequencePhase === 'lines'
              ? 'Swipe up to reveal the posts'
              : visibleSequencePhase === 'sweep'
                ? 'Turning trajectories into posts'
                : 'Tap a post · swipe down for trajectories'}
          </span>
        </footer>
      </div>

      <AnimatePresence>
        {selected ? (
          <motion.div
            className={styles.modalBackdrop}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: reducedMotion ? 0.08 : 0.18 }}
            onClick={() => setSelectedId(null)}
          >
            <motion.article
              className={styles.readCard}
              role="dialog"
              aria-modal="true"
              aria-labelledby="selected-post-title"
              data-testid="lakme-post-read"
              initial={reducedMotion ? { opacity: 0 } : { opacity: 0, transform: 'translate3d(0, 28px, 0) scale(0.985)' }}
              animate={{ opacity: 1, transform: 'translate3d(0, 0, 0) scale(1)' }}
              exit={reducedMotion ? { opacity: 0 } : { opacity: 0, transform: 'translate3d(0, 18px, 0) scale(0.99)' }}
              transition={reducedMotion ? { duration: 0.08 } : { type: 'spring', stiffness: 410, damping: 36, mass: 0.9 }}
              onClick={(event) => event.stopPropagation()}
            >
              <div className={styles.readVisual}>
                <Image src={selected.image} alt={`Cover of ${selected.name}`} fill priority sizes="(max-width: 560px) 100vw, 430px" style={{ objectPosition: selected.objectPosition ?? '50% 36%' }} />
              </div>
              <div className={styles.readBody}>
                <div className={styles.readTopline}>
                  <span className={selected.role === 'run' ? styles.readRoleRun : styles.readRoleHistory}>{roleLabel(selected.role)}</span>
                  <span>{selected.date}</span>
                </div>
                <button type="button" className={styles.closeButton} onClick={() => setSelectedId(null)} aria-label="Close post read">
                  <X aria-hidden="true" />
                </button>
                <h2 id="selected-post-title">{selected.name}</h2>
                <div className={styles.metrics}>
                  <span><b>Top {selected.topPercent}%</b>landing</span>
                  <span><b>{formatMultiple(selected.multiple)}</b>baseline</span>
                  <span><b>{selected.landing}%</b>trail cleared</span>
                </div>
                <p className={styles.construction}>{selected.construction}</p>
                <p className={styles.fingerprint}>{selected.fingerprint}</p>
              </div>
            </motion.article>
          </motion.div>
        ) : null}
      </AnimatePresence>
    </main>
  );
}
