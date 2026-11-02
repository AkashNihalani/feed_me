'use client';

/* ─────────────────────────────────────────────────────────────
   PROFILE STANDING — where the Feed's pick stands right now: its
   places on Lead's board for the last 30 days (components/lead/
   leadStanding), from the load Lead keeps anyway (warmed at launch,
   kept on the device), fetched once the feed is quiet, never while
   it draws its first screen.
   undefined: still coming; null: nothing to rank.
   ───────────────────────────────────────────────────────────── */

import { useEffect, useMemo, useState } from 'react';
import { keptLead, leadOnHand, loadLead, type LeadLoad } from '@/components/lead/leadData';
import { standingFor, type Standing } from '@/components/lead/leadStanding';
import type { StreamScope } from '@/lib/feedStream/contract';

// after the feed's first screen, in a quiet moment
function whenQuiet(run: () => void) {
  if (typeof window === 'undefined') return () => {};
  if (typeof window.requestIdleCallback === 'function') {
    const id = window.requestIdleCallback(run, { timeout: 2000 });
    return () => window.cancelIdleCallback(id);
  }
  const id = window.setTimeout(run, 600);
  return () => window.clearTimeout(id);
}

export function useStanding(scope: StreamScope): Standing | null | undefined {
  const [lead, setLead] = useState<LeadLoad | null>(() => leadOnHand());
  const [failed, setFailed] = useState(false);

  useEffect(() => {
    if (lead || failed) return undefined;
    let alive = true;
    const stop = whenQuiet(() => {
      void keptLead()
        .then((kept) => kept ?? loadLead())
        .then((load) => alive && setLead(load))
        .catch(() => alive && setFailed(true));
    });
    return () => {
      alive = false;
      stop();
    };
  }, [lead, failed]);

  return useMemo(() => {
    if (!lead) return failed ? null : undefined;
    const standing = standingFor(lead, { feedId: scope.feedId, handle: scope.handle });
    return standing.rows.length ? standing : null;
  }, [lead, failed, scope.feedId, scope.handle]);
}
