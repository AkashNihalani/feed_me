'use client';

/* ─────────────────────────────────────────────────────────────
   POST SHEET — one post, in full, over the feed: the media whole
   (a carousel's slides, a reel's clip) and its reading beside it
   (PostReading: who and when, the number, the trail, the counts,
   the caption, the way out to Instagram) — the same reading the
   Day view shows on a wide screen.

   A wide screen: a two-part viewer, the media at full height on
   the left (9:16 for a reel, 4:5 for the rest), the reading on the
   right. A phone: a bottom sheet, the media across the top, the
   reading under it. It portals to the body (wearing fm-stream for
   the tokens) and sits over the nav.

   It rises in on the page beat's ease (transform and opacity) over
   a dim layer, and the reading's blocks follow it in one after
   another. It goes by the close button, a tap on the dim, Esc, or
   (a phone) a drag down on its top: the panel follows the finger;
   let go far enough down, or flick, and it goes, otherwise it
   settles back. It always leaves from wherever it is, and the feed
   under it takes taps again the moment it starts to go.
   ───────────────────────────────────────────────────────────── */

import { memo, useEffect, useId, useLayoutEffect, useRef, useState, type PointerEvent as ReactPointerEvent } from 'react';
import { createPortal } from 'react-dom';
import { useReducedMotion } from 'framer-motion';
import { X } from 'lucide-react';
import PostMedia from '@/components/feed/stream/PostMedia';
import PostReading from '@/components/feed/stream/PostReading';
import { type PostSheetProps, type StreamFeeder, type StreamPost } from '@/lib/feedStream/contract';
import { BEAT_EASE_CSS, BEAT_LEAVE_EASE, beatDelay } from '@/lib/motion';
import '@/components/feed/stream/stream.css';

const useIsomorphicLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

const OPEN_MS = 460;
const CLOSE_MS = 240;
const SETTLE_BACK_MS = 300;
// the reading's blocks, following the panel in
const BLOCK_MS = 520;
const BLOCK_FRAMES: Keyframe[] = [
  { opacity: 0, transform: 'translate3d(0px, 12px, 0px)' },
  { opacity: 1, transform: 'translate3d(0px, 0px, 0px)' },
];
// reduced motion: no travel, a short fade
const FADE_MS = 160;
const LEAVE_EASE_CSS = `cubic-bezier(${BEAT_LEAVE_EASE.join(', ')})`;
const OPEN_EASE_CSS = 'cubic-bezier(0.2, 0.9, 0.1, 1)';
const WIDE = '(min-width: 1024px)';
// where the panel rests out of sight: under the screen's edge on a phone; a little low and small, and faded, wide
const AWAY = 'translate3d(0px, 100%, 0px)';
const AWAY_WIDE = 'translate3d(0px, 18px, 0px) scale(0.955)';
const REST = 'translate3d(0px, 0px, 0px)';
// a drag let go this far down (or a third of the sheet, if that is less), or flicked down this fast, closes it
const DISMISS_PX = 140;
const DISMISS_SPEED = 0.6; // px per ms

type Shown = { post: StreamPost; feeder: StreamFeeder | null };
type Drag = { id: number; startY: number; lastY: number; lastT: number; speed: number; offset: number; height: number };

function stopMotion(element: HTMLElement) {
  element.getAnimations().forEach((animation) => animation.cancel());
}

function PostSheet({ post, feeder, onClose }: PostSheetProps) {
  const reduce = Boolean(useReducedMotion());
  const titleId = useId();
  const panelRef = useRef<HTMLDivElement | null>(null);
  const dimRef = useRef<HTMLDivElement | null>(null);
  const closeRef = useRef<HTMLButtonElement | null>(null);
  const drag = useRef<Drag | null>(null);
  // what the sheet shows: the open post, or the last one while it slides away
  const [shown, setShown] = useState<Shown | null>(post ? { post, feeder } : null);
  if (post && (!shown || shown.post !== post || shown.feeder !== feeder)) setShown({ post, feeder });
  const open = post != null;

  // in and out: transform and opacity only, from wherever the panel is (mid-drag, or mid-way through the last move)
  useIsomorphicLayoutEffect(() => {
    const panel = panelRef.current;
    const dim = dimRef.current;
    if (!panel || !dim) return undefined;
    const wide = window.matchMedia(WIDE).matches;
    const away = wide ? AWAY_WIDE : AWAY;
    const moving = panel.getAnimations().length > 0;
    const panelAt = panel.style.transform || (moving ? getComputedStyle(panel).transform : '');
    const panelOpacity = moving ? Number(getComputedStyle(panel).opacity) : null;
    const dimAt = dim.style.opacity || (dim.getAnimations().length ? getComputedStyle(dim).opacity : '');
    stopMotion(panel);
    stopMotion(dim);
    panel.style.transform = '';
    dim.style.opacity = '';
    const from = (rest: string, restOpacity: number): Keyframe => ({
      transform: panelAt && panelAt !== 'none' ? panelAt : rest,
      opacity: panelOpacity ?? restOpacity,
    });

    if (open) {
      if (reduce) {
        panel.animate([{ opacity: panelOpacity ?? 0 }, { opacity: 1 }], { duration: FADE_MS, easing: 'ease-out' });
      } else {
        panel.animate([from(moving ? REST : away, moving || !wide ? 1 : 0), { transform: REST, opacity: 1 }], { duration: OPEN_MS, easing: OPEN_EASE_CSS });
        // the reading follows the panel in, block by block
        if (!moving) {
          panel.querySelectorAll<HTMLElement>('[data-settle]').forEach((block) => {
            const step = Number(block.dataset.settle) || 0;
            block.animate(BLOCK_FRAMES, { duration: BLOCK_MS, delay: (0.12 + beatDelay(step * 1.5)) * 1000, easing: BEAT_EASE_CSS, fill: 'backwards' });
          });
        }
      }
      dim.animate([{ opacity: dimAt || 0 }, { opacity: 1 }], { duration: reduce ? FADE_MS : OPEN_MS, easing: 'ease-out' });
      return undefined;
    }

    const leave = reduce
      ? panel.animate([{ opacity: panelOpacity ?? 1 }, { opacity: 0 }], { duration: FADE_MS, easing: 'ease-in', fill: 'forwards' })
      : panel.animate([from(REST, 1), { transform: away, opacity: wide ? 0 : 1 }], { duration: CLOSE_MS, easing: LEAVE_EASE_CSS, fill: 'forwards' });
    dim.animate([{ opacity: dimAt || 1 }, { opacity: 0 }], { duration: reduce ? FADE_MS : CLOSE_MS, easing: 'ease-in', fill: 'forwards' });
    leave.onfinish = () => setShown(null);
    return () => {
      leave.onfinish = null;
    };
  }, [open, reduce]);

  // Esc closes
  useEffect(() => {
    if (!open) return undefined;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key !== 'Escape') return;
      event.preventDefault();
      onClose();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [open, onClose]);

  // focus moves into the sheet while it is open, and back to where it was after
  useEffect(() => {
    if (!open) return undefined;
    const before = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    closeRef.current?.focus({ preventScroll: true });
    return () => {
      if (before?.isConnected) before.focus({ preventScroll: true });
    };
  }, [open]);

  /* ── the drag down (a phone's sheet; its top is the handle) ── */

  const startDrag = (event: ReactPointerEvent<HTMLDivElement>) => {
    const panel = panelRef.current;
    if (!open || !panel || !event.isPrimary || event.button !== 0) return;
    if ((event.target as Element | null)?.closest('button, a')) return;
    if (window.matchMedia(WIDE).matches) return;
    stopMotion(panel);
    drag.current = {
      id: event.pointerId,
      startY: event.clientY,
      lastY: event.clientY,
      lastT: event.timeStamp,
      speed: 0,
      offset: 0,
      height: panel.offsetHeight,
    };
    event.currentTarget.setPointerCapture(event.pointerId);
  };

  const moveDrag = (event: ReactPointerEvent<HTMLDivElement>) => {
    const state = drag.current;
    const panel = panelRef.current;
    const dim = dimRef.current;
    if (!state || state.id !== event.pointerId || !panel || !dim) return;
    const pulled = event.clientY - state.startY;
    // down follows the finger; up gives a little, then holds
    state.offset = pulled > 0 ? pulled : pulled / 6;
    state.speed = (event.clientY - state.lastY) / Math.max(1, event.timeStamp - state.lastT);
    state.lastY = event.clientY;
    state.lastT = event.timeStamp;
    panel.style.transform = `translate3d(0px, ${state.offset}px, 0px)`;
    dim.style.opacity = String(1 - Math.min(1, Math.max(0, state.offset) / Math.max(1, state.height)) * 0.8);
  };

  const endDrag = (event: ReactPointerEvent<HTMLDivElement>) => {
    const state = drag.current;
    const panel = panelRef.current;
    const dim = dimRef.current;
    if (!state || state.id !== event.pointerId) return;
    drag.current = null;
    if (!panel || !dim) return;
    const far = state.offset > Math.min(DISMISS_PX, state.height / 3);
    const flicked = state.speed > DISMISS_SPEED && state.offset > 24;
    if (event.type === 'pointerup' && (far || flicked)) {
      // the close slides on from where the finger left it
      onClose();
      return;
    }
    // not far enough: back into place
    const at = panel.style.transform;
    const dimAt = dim.style.opacity;
    panel.style.transform = '';
    dim.style.opacity = '';
    if (at) panel.animate([{ transform: at }, { transform: REST }], { duration: SETTLE_BACK_MS, easing: BEAT_EASE_CSS });
    if (dimAt) dim.animate([{ opacity: dimAt }, { opacity: 1 }], { duration: SETTLE_BACK_MS, easing: BEAT_EASE_CSS });
  };

  if (!shown || typeof document === 'undefined') return null;
  const current = shown.post;
  const who = shown.feeder;

  return createPortal(
    <div
      className="fm-stream fixed inset-0 z-[200] flex items-end justify-center lg:items-center lg:p-8"
      style={{ pointerEvents: open ? undefined : 'none' }}
    >
      <div ref={dimRef} aria-hidden="true" onClick={onClose} className="absolute inset-0 touch-none bg-black/70" />
      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        className="relative flex max-h-[calc(100dvh-env(safe-area-inset-top)-16px)] w-full flex-col overflow-hidden rounded-t-[28px] bg-[var(--st-surface-2)] text-[var(--st-text)] shadow-[inset_0_1px_0_rgba(255,255,255,0.07),0_-30px_80px_-30px_rgba(0,0,0,0.9)] lg:h-[min(88dvh,840px)] lg:max-h-none lg:w-auto lg:max-w-[min(1180px,100%)] lg:flex-row lg:rounded-[28px] lg:shadow-[inset_0_0_0_1px_rgba(255,255,255,0.06),0_40px_120px_-30px_rgba(0,0,0,0.95)]"
      >
        {/* a phone's handle: drag the sheet down by its top */}
        <div
          onPointerDown={startDrag}
          onPointerMove={moveDrag}
          onPointerUp={endDrag}
          onPointerCancel={endDrag}
          onLostPointerCapture={endDrag}
          className="absolute inset-x-0 top-0 z-20 h-8 touch-none select-none lg:hidden"
        >
          <span aria-hidden="true" className="mx-auto mt-2.5 block h-[5px] w-10 rounded-full bg-white/40" />
        </div>

        {/* the media, whole */}
        <div
          className="relative aspect-[4/5] max-h-[54dvh] w-full shrink-0 overflow-hidden bg-black lg:aspect-auto lg:h-full lg:max-h-none lg:w-[calc(min(88dvh,840px)*4/5)] lg:max-w-[min(680px,58vw)]"
        >
          <PostMedia post={current} variant="sheet" active={open} eager fit="frame" className="absolute inset-0" />
        </div>

        {/* the reading */}
        <div className="hide-scrollbar relative min-h-0 flex-1 overflow-y-auto overscroll-contain px-5 pb-[calc(28px+env(safe-area-inset-bottom))] pt-6 lg:flex lg:w-[440px] lg:flex-none lg:flex-col lg:px-10 lg:py-10 xl:w-[480px]">
          <div className="lg:my-auto">
            <PostReading post={current} feeder={who} size="sheet" titleId={titleId} />
          </div>
        </div>

        <button
          ref={closeRef}
          type="button"
          onClick={onClose}
          aria-label="Close"
          className="absolute right-3 top-3 z-30 grid h-10 w-10 place-items-center rounded-full bg-black/55 text-white shadow-[inset_0_0_0_1px_rgba(255,255,255,0.12)] outline-none transition-transform duration-150 ease-out hover:bg-black/70 active:scale-[0.92] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)] lg:right-4 lg:top-4"
        >
          <X className="h-[18px] w-[18px]" strokeWidth={2.4} aria-hidden="true" />
        </button>
      </div>
    </div>,
    document.body,
  );
}

export default memo(PostSheet);
