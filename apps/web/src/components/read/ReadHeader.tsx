'use client';

/* ─────────────────────────────────────────────────────────────
   READ HEADER — what the app's one tab header (TabHeader, the
   same one Lead's page feeds) shows while Read is on screen, with
   Read's own pieces inside it:

   - the rail marks every feeder Read has a read for (and their
     feed) with the rose story ring;
   - it moves with the page, never ahead of it (leaving / armed,
     see TabHeader);
   - the view switch on the right (Runs · Repeats · Ranks), only
     while a read is open and only on a phone, where the lens
     isn't stuck under the header. It carries data-drop="lens",
     which the engine's click router handles;
   - the engine owns the moment a post is picked: it fills the
     readout (#rd-hread) and toggles `reading` on the title row
     (#rd-hdr), and the two trade places (readTab.css). It also
     reads #rd-rail to leave rail taps alone.
   ───────────────────────────────────────────────────────────── */

import { useState } from 'react';
import { AnimatePresence, motion, useReducedMotion } from 'framer-motion';
import { ChevronDown } from 'lucide-react';
import { HEADER_CONTROL, LEAVE_EASE, SLOT_ROLL, SLOT_SPRING, SOFT_EASE, useTabHeader } from '@/components/shell/TabHeader';
import { cn } from '@/lib/utils';
import { readOf, type RailFeed, type RailFeeder } from '@/components/read/readFeeds';

const VIEW_LABEL: Record<string, string> = { runs: 'Runs', repeats: 'Repeats', ranks: 'Ranks' };
const VIEW_ORDER: Record<string, number> = { runs: 0, repeats: 1, ranks: 2 };
const REDUCED = { duration: 0.14, ease: SOFT_EASE } as const;

const hasRead = (handle: string) => readOf(handle) != null;

export default function ReadHeader({
  feeds,
  fallback,
  feedId,
  handle,
  label,
  eyebrow,
  view,
  showView,
  leaving,
  armed,
  compressed,
  topRef,
  fontClass,
  onAll,
  onFeed,
  onFeeder,
}: {
  // null while the feeds load: the rail then holds the feeders Read has reads for (fallback)
  feeds: RailFeed[] | null;
  fallback: RailFeeder[];
  feedId: string | null | undefined;
  handle: string | null;
  label: string;
  eyebrow: string;
  view: string;
  showView: boolean;
  // the page is leaving for somewhere you just tapped: 'all', 'feed:<id>' or 'feeder:<handle>' (armed)
  leaving: boolean;
  armed: string | null;
  compressed: boolean;
  topRef: (el: HTMLDivElement | null) => void;
  fontClass: string;
  onAll: () => void;
  onFeed: (id: string) => void;
  onFeeder: (handle: string) => void;
}) {
  const reduce = Boolean(useReducedMotion());
  const viewLabel = VIEW_LABEL[view] || 'Runs';
  // which way the view moved, kept beside the view it was worked out for
  const [roll, setRoll] = useState({ view, dir: 1 });
  if (roll.view !== view) setRoll({ view, dir: (VIEW_ORDER[view] ?? 0) >= (VIEW_ORDER[roll.view] ?? 0) ? 1 : -1 });

  const viewSwitch = (
    <AnimatePresence initial={false}>
      {showView && !leaving ? (
        <motion.button
          key="view"
          type="button"
          data-drop="lens"
          aria-haspopup="true"
          aria-expanded="false"
          aria-label={`View: ${viewLabel}. Change how the wall is read`}
          initial={reduce ? { opacity: 0 } : { opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={reduce ? { opacity: 0 } : { opacity: 0, y: -6, transition: { duration: 0.28, delay: 0.12, ease: LEAVE_EASE } }}
          transition={reduce ? REDUCED : { duration: 0.6, delay: 0.12, ease: SOFT_EASE }}
          whileTap={{ scale: 0.97 }}
          className={cn(HEADER_CONTROL, 'pl-3 pr-2 text-[14px] tracking-[-0.01em] lg:hidden')}
        >
          <span className="relative grid h-[1.08em] w-[60px] shrink-0 place-items-center overflow-hidden leading-none">
            <AnimatePresence initial={false} custom={roll.dir}>
              <motion.span
                key={viewLabel}
                custom={roll.dir}
                variants={SLOT_ROLL}
                initial={reduce ? false : 'enter'}
                animate="center"
                exit={reduce ? { opacity: 0, transition: { duration: 0 } } : 'exit'}
                transition={reduce ? { duration: 0 } : SLOT_SPRING}
                className="absolute inset-0 grid place-items-center text-fg"
              >
                {viewLabel}
              </motion.span>
            </AnimatePresence>
          </span>
          <ChevronDown className="h-3.5 w-3.5 text-fg/54" aria-hidden="true" />
        </motion.button>
      ) : null}
    </AnimatePresence>
  );

  useTabHeader('read', {
    title: 'READ',
    eyebrow,
    label,
    control: viewSwitch,
    compressed,
    className: `rd-header ${fontClass}`,
    feeds,
    pendingFeeders: fallback,
    feedId,
    handle,
    marked: hasRead,
    feederLabel: (feederHandle) => (hasRead(feederHandle) ? `Read @${feederHandle}` : `@${feederHandle}, not read yet`),
    leaving,
    armed,
    onAll,
    onFeed,
    onFeeder,
    rowRef: topRef,
    rowId: 'rd-hdr',
    rowClassName: 'hdr',
    baseClassName: 'hbase',
    rowOverlay: <div className="rd hread" id="rd-hread" aria-live="polite" />,
    railId: 'rd-rail',
  });
  return null;
}
