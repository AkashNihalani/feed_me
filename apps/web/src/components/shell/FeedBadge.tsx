'use client';

/* A feed's badge, inside its circle wherever it stands (the dock, the side rail): its icon (lib/feedIcons), or its letters.
   Its ink and its silver are the circle's own (the two places dress it a little differently). */

import type { CSSProperties } from 'react';
import { feedInitials } from '@/lib/feedLabels';
import { useFeedIcon } from '@/lib/feedIcons';

export default function FeedBadge({ id, title, className, style, size }: { id: string; title: string; className?: string; style?: CSSProperties; size: number }) {
  const icon = useFeedIcon(id, title);
  return (
    <span className={className} style={style}>
      {icon ? (
        // (drawn from our own set, checked to be lines alone: lib/feedIcons)
        <svg aria-hidden="true" viewBox="0 0 24 24" width={size} height={size} fill="none" stroke="currentColor" strokeWidth={2.3} strokeLinecap="round" strokeLinejoin="round" dangerouslySetInnerHTML={{ __html: icon }} />
      ) : feedInitials(title)}
    </span>
  );
}
