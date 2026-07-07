'use client';

/* ─────────────────────────────────────────────────────────────
   FEEDER STORY AVATAR — the one feeder circle.
   Extracted from FeedTab so every surface (feed dashboard trays,
   reader home tray, feeder reader page) renders the same identity:
   profile pic when we have one, seeded gradient + initials when not.
   ───────────────────────────────────────────────────────────── */

import type { CSSProperties } from 'react';
import { cn } from '@/lib/utils';

const STORY_GRADIENTS = [
  ['#1f2937', '#0f172a'],
  ['#b91c1c', '#7f1d1d'],
  ['#1d4ed8', '#1e3a8a'],
  ['#047857', '#064e3b'],
  ['#b45309', '#78350f'],
  ['#7c3aed', '#4c1d95'],
  ['#0891b2', '#155e75'],
  ['#475569', '#1e293b'],
] as const;

export function feederInitials(handle: string | null | undefined) {
  const clean = String(handle || '').replace(/^@+/, '').trim();
  if (!clean) return 'FM';
  return clean.slice(0, 2).toUpperCase();
}

export function storyGradientForHandle(handle: string | null | undefined) {
  const clean = String(handle || '');
  const seed = clean.split('').reduce((sum, char) => sum + char.charCodeAt(0), 0);
  return STORY_GRADIENTS[seed % STORY_GRADIENTS.length];
}

export default function FeederStoryAvatar({
  feeder,
  className,
  style,
}: {
  feeder: { handle: string; profilePicUrl?: string | null };
  className?: string;
  style?: CSSProperties;
}) {
  const [from, to] = storyGradientForHandle(feeder.handle);
  return (
    <span
      className={cn(
        'relative z-20 flex h-full w-full items-center justify-center overflow-hidden rounded-full bg-[linear-gradient(150deg,var(--fm-story-grad-from),var(--fm-story-grad-to))] text-[18px] font-black leading-none text-white shadow-[inset_0_1px_0_rgba(255,255,255,0.18)] [text-shadow:0_1px_3px_rgba(0,0,0,0.35)]',
        className,
      )}
      style={{
        ['--fm-story-grad-from' as string]: from,
        ['--fm-story-grad-to' as string]: to,
        ...style,
      }}
    >
      {feeder.profilePicUrl ? (
        // eslint-disable-next-line @next/next/no-img-element -- remote feeder media
        <img
          src={feeder.profilePicUrl}
          alt={`@${feeder.handle}`}
          className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-105"
          loading="lazy"
          decoding="async"
        />
      ) : (
        <span className="relative z-10">{feederInitials(feeder.handle)}</span>
      )}
    </span>
  );
}
