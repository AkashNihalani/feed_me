/* ─────────────────────────────────────────────────────────────
   WEBKIT MOTION — every iOS browser and Safari on the Mac run
   compositor animations (WAAPI, CSS animations, framer's opacity /
   transform) in another process, and start their clock in the
   frame that first draws them, before that frame reaches the
   screen. After a switch that frame is a heavy one, so it eats the
   start of everything that moves: a staggered list arrives already
   arrived. There, a move is held at its start and set going once
   its frame is on screen, two frames later (the Feed's flights,
   lib/feedStream/motion; FeedStream's holdsScroll).
   ───────────────────────────────────────────────────────────── */

let webkit: boolean | null = null;

/* WebKit: iOS (any browser on it) or desktop Safari */
export function holdsFlights() {
  // diagnostic: components/PerfProbe turns the holds off for a round to measure what they cost
  if (typeof window !== 'undefined' && (window as unknown as { __fmNoHold?: boolean }).__fmNoHold) return false;
  if (webkit == null) {
    const ua = typeof navigator === 'undefined' ? '' : navigator.userAgent;
    const ios = /iP(hone|od|ad)/.test(ua) || (/Macintosh/.test(ua) && typeof navigator !== 'undefined' && navigator.maxTouchPoints > 1);
    const safari = /AppleWebKit/.test(ua) && !/Chrome|Chromium|CriOS|Edg|OPR|Firefox|FxiOS/.test(ua);
    webkit = ios || safari;
  }
  return webkit;
}

// the longest a held move waits for its frame (a move created held carries this much delay, and is released early)
export const FLIGHT_HOLD_MS = 800;

/* calls `go` once the frame being drawn now is on screen: two frames on (WebKit only starts a frame once the last is
   shown). Returns a cancel. */
export function afterDrawn(go: () => void) {
  let second = 0;
  const first = window.requestAnimationFrame(() => {
    second = window.requestAnimationFrame(go);
  });
  return () => {
    window.cancelAnimationFrame(first);
    window.cancelAnimationFrame(second);
  };
}
