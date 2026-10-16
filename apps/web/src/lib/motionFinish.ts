'use client';

/* ─────────────────────────────────────────────────────────────
   MOTION FINISH — framer-motion's compositor animations play from
   their first frame to their last.

   framer runs opacity / transform / clipPath / filter on the
   compositor (WAAPI). Two things went wrong at the ends:

   The last frame. When one finishes it hands the final value to
   the element's motion value — which only writes it to the
   element's style on framer's next frame — and cancels the WAAPI
   animation at once. In between, the element shows the style it
   had before the animation: whatever entered (opacity 0, 22px
   down) blinks back to its start for one frame. Here the final
   value is also written to the style right away, before the
   cancel: what framer writes a frame later is the same value.

   The first frame (WebKit only, see lib/webkitMotion). framer pins
   a new animation's clock to when it was asked for, and WebKit
   starts the clock in a frame that, after a switch, reaches the
   screen late: a staggered list arrived already arrived. There a
   new animation is held at its start instead (its first keyframe
   showing: framer's animations fill backwards) and set going once
   the frame that draws it is on screen.

   Imported once, from the app shell.
   ───────────────────────────────────────────────────────────── */

import { NativeAnimation, NativeAnimationExtended, setStyle } from 'framer-motion';
import { afterDrawn, holdsFlights } from '@/lib/webkitMotion';

type Native = {
  options?: { element?: unknown; name?: string };
  animation?: Animation;
  isStopped?: boolean;
  manualStartTime?: number | null;
  updateMotionValue: (value?: unknown) => void;
  __held?: boolean;
};

const proto = NativeAnimationExtended.prototype as unknown as Native & { __patched?: boolean };

if (typeof window !== 'undefined' && !proto.__patched && typeof proto.updateMotionValue === 'function') {
  proto.__patched = true;

  // the last frame: the final value goes on the element before the animation is cancelled
  const handOver = proto.updateMotionValue;
  proto.updateMotionValue = function updateMotionValue(this: Native, value?: unknown) {
    // called with a value only as the animation finishes (without one, it samples a running animation)
    const element = this.options?.element;
    const name = this.options?.name;
    if (value !== undefined && name && element instanceof HTMLElement) setStyle(element, name, value as string | number);
    return handOver.call(this, value);
  };

  // the first frame, on WebKit: framer sets a new animation's start time right after creating it; the start time is
  // turned into a hold instead
  const startTime = Object.getOwnPropertyDescriptor(NativeAnimation.prototype, 'startTime');
  if (holdsFlights() && startTime?.get && startTime.set) {
    const setStart = startTime.set;
    Object.defineProperty(NativeAnimationExtended.prototype, 'startTime', {
      configurable: true,
      get: startTime.get,
      set(this: Native, value: number) {
        const waapi = this.animation;
        if (this.__held || !waapi || waapi.playState === 'finished') {
          setStart.call(this, value);
          return;
        }
        this.__held = true;
        waapi.pause();
        waapi.currentTime = 0;
        // while held, framer reads the clock as starting now (an interruption samples from here)
        this.manualStartTime = performance.now();
        afterDrawn(() => {
          if (this.isStopped || waapi.playState !== 'paused') return;
          this.manualStartTime = null;
          waapi.play();
        });
      },
    });
  }
}
