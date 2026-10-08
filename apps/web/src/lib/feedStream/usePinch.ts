'use client';

/* ─────────────────────────────────────────────────────────────
   PINCH — two fingers on the Feed scale it live, the way Photos
   does, and on release either commit one step along ZOOM_ORDER
   (together: more posts a screen; apart: closer) or spring back.
   One step per gesture, at a clear threshold.

   Touch: a two-finger touchstart starts tracking; only then is a
   non-passive touchmove attached (so the page itself never zooms
   or scrolls under the pinch), and it comes off the moment a
   finger lifts. Scrolling never waits on a blocking listener.
   Desktop: a trackpad pinch (ctrl + wheel in Chrome and Firefox,
   gesture events in Safari), on fine pointers only.

   The focal is what sits under the fingers: the stream's box
   ([data-zid], a post or the profile) at the midpoint. The live
   scale is reported at most once a frame; the caller applies it
   as one transform.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useRef, type RefObject } from 'react';

export const PINCH_TOGETHER = 0.82;
export const PINCH_APART = 1.22;
// a live pinch is held within these
const SCALE_MIN = 0.6;
const SCALE_MAX = 1.6;
// ctrl + wheel: how much delta halves or doubles the scale, and the quiet that ends a wheel gesture
const WHEEL_SCALE = 260;
const WHEEL_IDLE_MS = 180;

// 1: together (more posts), -1: apart (closer)
export type PinchDirection = 1 | -1;
// x, y: the midpoint (viewport); zid: the stream's box under it (a post's day:slot, or 'profile')
export type PinchFocal = { x: number; y: number; zid: string | null; postKey: string | null };

export type PinchHandlers = {
  enabled: boolean;
  onStart: (focal: PinchFocal) => void;
  // the live scale, clamped
  onScale: (scale: number) => void;
  // released: past a threshold (1 or -1), or not far enough (0: spring back)
  onEnd: (direction: PinchDirection | 0) => void;
};

export function focalAt(x: number, y: number): PinchFocal {
  const focal: PinchFocal = { x, y, zid: null, postKey: null };
  if (typeof document === 'undefined') return focal;
  const hit = document.elementFromPoint(x, y);
  focal.zid = hit?.closest<HTMLElement>('[data-zid]')?.getAttribute('data-zid') ?? null;
  focal.postKey = hit?.closest<HTMLElement>('[data-post-key]')?.getAttribute('data-post-key') ?? null;
  return focal;
}

const clampScale = (scale: number) => Math.min(SCALE_MAX, Math.max(SCALE_MIN, scale));
const directionOf = (scale: number): PinchDirection | 0 => (scale <= PINCH_TOGETHER ? 1 : scale >= PINCH_APART ? -1 : 0);

type GestureLike = Event & { scale?: number; clientX?: number; clientY?: number };

function spread(touches: TouchList) {
  const a = touches[0];
  const b = touches[1];
  return {
    distance: Math.hypot(a.clientX - b.clientX, a.clientY - b.clientY),
    x: (a.clientX + b.clientX) / 2,
    y: (a.clientY + b.clientY) / 2,
  };
}

export function usePinch(targetRef: RefObject<HTMLElement | null>, handlers: PinchHandlers) {
  const handlersRef = useRef(handlers);
  useEffect(() => {
    handlersRef.current = handlers;
  });
  const { enabled } = handlers;

  useEffect(() => {
    const el = targetRef.current;
    if (!el || !enabled) return undefined;

    // one gesture at a time, from whichever input started it
    let source: 'touch' | 'gesture' | 'wheel' | null = null;
    let scale = 1;
    let frame = 0;
    const report = () => {
      frame = 0;
      if (source) handlersRef.current.onScale(scale);
    };
    const live = (next: number) => {
      scale = clampScale(next);
      if (!frame) frame = window.requestAnimationFrame(report);
    };
    const begin = (kind: 'touch' | 'gesture' | 'wheel', x: number, y: number) => {
      source = kind;
      scale = 1;
      handlersRef.current.onStart(focalAt(x, y));
    };
    const end = () => {
      if (!source) return;
      source = null;
      if (frame) {
        window.cancelAnimationFrame(frame);
        frame = 0;
      }
      handlersRef.current.onEnd(directionOf(scale));
    };

    /* touch */
    let startDistance = 1;
    const onMove = (event: TouchEvent) => {
      if (event.touches.length < 2) return;
      if (event.cancelable) event.preventDefault();
      live(spread(event.touches).distance / startDistance);
    };
    const detach = () => {
      el.removeEventListener('touchmove', onMove);
      el.removeEventListener('touchend', onTouchEnd);
      el.removeEventListener('touchcancel', onTouchEnd);
    };
    function onTouchEnd(event: TouchEvent) {
      if (event.touches.length >= 2) return;
      detach();
      if (source === 'touch') end();
    }
    const onStart = (event: TouchEvent) => {
      if (source || event.touches.length !== 2) return;
      const start = spread(event.touches);
      startDistance = Math.max(1, start.distance);
      begin('touch', start.x, start.y);
      el.addEventListener('touchmove', onMove, { passive: false });
      el.addEventListener('touchend', onTouchEnd, { passive: true });
      el.addEventListener('touchcancel', onTouchEnd, { passive: true });
    };

    /* Safari's gesture events: on iOS they shadow the touch pinch (only kept from zooming the page); on a Mac
       trackpad they are the pinch */
    const onGestureStart = (event: GestureLike) => {
      event.preventDefault();
      if (source) return;
      begin('gesture', event.clientX ?? window.innerWidth / 2, event.clientY ?? window.innerHeight / 2);
    };
    const onGestureChange = (event: GestureLike) => {
      event.preventDefault();
      if (source === 'gesture' && typeof event.scale === 'number') live(event.scale);
    };
    const onGestureEnd = (event: GestureLike) => {
      event.preventDefault();
      if (source === 'gesture') end();
    };

    /* ctrl + wheel. Only on a fine pointer: a phone never pays for a blocking wheel listener */
    const finePointer = window.matchMedia('(hover: hover) and (pointer: fine)').matches;
    let wheelTotal = 0;
    let wheelTimer = 0;
    const onWheel = (event: WheelEvent) => {
      if (!event.ctrlKey) return;
      event.preventDefault();
      if (!source) {
        wheelTotal = 0;
        begin('wheel', event.clientX, event.clientY);
      }
      if (source !== 'wheel') return;
      wheelTotal += event.deltaMode === 1 ? event.deltaY * 16 : event.deltaY;
      live(Math.exp(-wheelTotal / WHEEL_SCALE));
      window.clearTimeout(wheelTimer);
      wheelTimer = window.setTimeout(end, WHEEL_IDLE_MS);
    };

    el.addEventListener('touchstart', onStart, { passive: true });
    el.addEventListener('gesturestart', onGestureStart as EventListener);
    el.addEventListener('gesturechange', onGestureChange as EventListener);
    el.addEventListener('gestureend', onGestureEnd as EventListener);
    if (finePointer) el.addEventListener('wheel', onWheel, { passive: false });

    return () => {
      detach();
      window.clearTimeout(wheelTimer);
      if (frame) window.cancelAnimationFrame(frame);
      // a gesture cut short (the tab hiding) springs back rather than committing
      if (source) {
        source = null;
        handlersRef.current.onEnd(0);
      }
      el.removeEventListener('touchstart', onStart);
      el.removeEventListener('gesturestart', onGestureStart as EventListener);
      el.removeEventListener('gesturechange', onGestureChange as EventListener);
      el.removeEventListener('gestureend', onGestureEnd as EventListener);
      if (finePointer) el.removeEventListener('wheel', onWheel);
    };
  }, [enabled, targetRef]);
}
