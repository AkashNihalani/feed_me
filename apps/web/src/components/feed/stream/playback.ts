/* ─────────────────────────────────────────────────────────────
   PLAYBACK — one preview plays at a time, app-wide.

   A video about to play claims playback with a way to pause it.
   The claim pauses whoever held it before (a card still easing out
   of view, the same post drawn twice mid-zoom), and the function it
   returns lets go: only if it still holds, so a late release never
   frees someone else's claim.
   ───────────────────────────────────────────────────────────── */

type Claim = { id: string; pause: () => void };

let holder: Claim | null = null;

export function claimPlayback(id: string, pause: () => void): () => void {
  const claim: Claim = { id, pause };
  const previous = holder;
  holder = claim;
  if (previous) {
    try {
      previous.pause();
    } catch {
      // a holder whose video already went away: nothing left to pause
    }
  }
  return () => {
    if (holder === claim) holder = null;
  };
}
