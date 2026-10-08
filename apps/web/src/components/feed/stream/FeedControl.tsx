'use client';

/* ─────────────────────────────────────────────────────────────
   FEED CONTROL — Feed's piece of the shared header's 44px slot:
   the zoom, and nothing else. It is the very control Lead's 30D
   is (TabHeader's TimeframeControl, with the zoom's labels): on a
   phone a chip that rolls to DAY · WEEK · MONTH and drops a menu
   of the other zooms; from sm up a segmented track. The order
   is chosen once, on the profile's tab row, never here as well.
   The pointer on a zoom (or a press) tells the feed it is coming,
   so its pictures are ready before it moves.
   ───────────────────────────────────────────────────────────── */

import { memo, type SyntheticEvent } from 'react';
import { TimeframeControl } from '@/components/shell/TabHeader';
import { ZOOM_LABEL, ZOOM_ORDER, type FeedControlProps, type FeedMode } from '@/lib/feedStream/contract';

const zoomLabel = (mode: FeedMode) => ZOOM_LABEL[mode];
const byLabel = new Map<string, FeedMode>(ZOOM_ORDER.map((mode) => [ZOOM_LABEL[mode], mode]));

function FeedControl({ mode, onModeChange, onModeIntent }: FeedControlProps) {
  const intent = (event: SyntheticEvent) => {
    const label = (event.target as Element | null)?.closest?.('button')?.textContent?.trim() ?? '';
    const next = byLabel.get(label);
    if (next && next !== mode) onModeIntent?.(next);
  };
  return (
    <span className="contents" onPointerOver={intent} onPointerDown={intent} onFocus={intent}>
      <TimeframeControl id="feed" name="Zoom" value={mode} options={ZOOM_ORDER} label={zoomLabel} onChange={onModeChange} />
    </span>
  );
}

export default memo(FeedControl);
