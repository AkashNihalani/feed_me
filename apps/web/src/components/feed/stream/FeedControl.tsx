'use client';

/* ─────────────────────────────────────────────────────────────
   FEED CONTROL — Feed's piece of the shared header's 44px slot:
   the zoom, and nothing else. It is the very control Lead's 30D
   is (TabHeader's TimeframeControl, with the zoom's labels): on a
   phone a chip that rolls to DAY · WEEK · MONTH and drops a menu
   of the other zooms; from sm up a segmented track. The order
   is chosen once, on the profile's tab row, never here as well.
   The pointer on a zoom (or a press) tells the feed it is coming,
   so its pictures are ready before it moves; the phone's chip
   opening its menu tells it the other zooms are (once the menu's
   first frame is on screen).
   ───────────────────────────────────────────────────────────── */

import { memo, useEffect, useRef, type SyntheticEvent } from 'react';
import { TimeframeControl } from '@/components/shell/TabHeader';
import { ZOOM_LABEL, ZOOM_ORDER, type FeedControlProps, type FeedMode } from '@/lib/feedStream/contract';
import { afterDrawn } from '@/lib/webkitMotion';

const zoomLabel = (mode: FeedMode) => ZOOM_LABEL[mode];
const byLabel = new Map<string, FeedMode>(ZOOM_ORDER.map((mode) => [ZOOM_LABEL[mode], mode]));

function FeedControl({ mode, onModeChange, onModeIntent }: FeedControlProps) {
  const openingRef = useRef<(() => void) | null>(null);
  useEffect(() => () => openingRef.current?.(), []);
  const intent = (event: SyntheticEvent) => {
    const button = (event.target as Element | null)?.closest?.('button');
    const next = byLabel.get(button?.textContent?.trim() ?? '');
    if (next && next !== mode) onModeIntent?.([next]);
  };
  /* the chip (it wears the zoom you're on) opening its menu: every other zoom, in one go, while the menu is read. Not
     at the press: once the menu's first frame is on screen, so the warm's work never lands in the frame that opens it */
  const opened = (event: SyntheticEvent) => {
    const button = (event.target as Element | null)?.closest?.('button');
    if (!button || button.getAttribute('aria-haspopup') !== 'menu' || button.getAttribute('aria-expanded') === 'true') return;
    openingRef.current?.();
    openingRef.current = afterDrawn(() => {
      openingRef.current = null;
      onModeIntent?.(ZOOM_ORDER.filter((other) => other !== mode));
    });
  };
  return (
    <span className="contents" onPointerOver={intent} onPointerDown={intent} onFocus={intent} onClick={opened}>
      <TimeframeControl id="feed" name="Zoom" value={mode} options={ZOOM_ORDER} label={zoomLabel} onChange={onModeChange} />
    </span>
  );
}

export default memo(FeedControl);
