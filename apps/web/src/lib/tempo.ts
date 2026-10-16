/* ─────────────────────────────────────────────────────────────
   TEMPO — how long the app takes over its motion, in one number.
   Every visible duration, stagger and delay is written at the
   pace it was designed at and stretched by TEMPO where it is
   defined (ms() for WAAPI and inline CSS, sec() for framer, and
   var(--fm-tempo) in the style sheets, set from the same value in
   globals.css), so a change here plays the whole app out evenly.
   Easing curves don't change, only time. Left out on purpose:
   what answers a finger (press feedback, a pinch springing back,
   the bottom-nav pill), what follows the scroll (the header
   fold), ambient loops (the tape, a poster's drift) and the
   functional clocks (WebKit holds, stale guards, load waits).
   ───────────────────────────────────────────────────────────── */

// keep in step with --fm-tempo in app/globals.css
export const TEMPO = 1.35;

export const ms = (value: number) => Math.round(value * TEMPO);
export const sec = (value: number) => value * TEMPO;

/* a spring that settles TEMPO times slower with the same feel: the same damping ratio, its period stretched */
export function slowSpring<T extends { stiffness: number; damping: number; mass: number }>(spring: T): T {
  return { ...spring, mass: spring.mass * TEMPO * TEMPO, damping: spring.damping * TEMPO };
}
