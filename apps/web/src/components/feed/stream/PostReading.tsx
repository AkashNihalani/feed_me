'use client';

/* ─────────────────────────────────────────────────────────────
   POST READING — one post, read: who posted it and when, the
   number (the lead, set big), where it landed and against what,
   its four reads as the trail, the counts, the caption's opening
   and the way out to Instagram.

   The Day view's reading on a wide screen (beside the media) and
   the post sheet's right side both draw this, so a post reads the
   same wherever it is opened. size 'stage' is the Day view's on a
   tall screen (the biggest numeral, the most air); 'sheet' a step
   smaller; 'compact' fits a short laptop's band whole (a smaller
   numeral, two lines of caption), so nothing spills under the
   header or the nav.

   Each block is marked .st-settle with its step in data-settle:
   the Day view settles them in, one after another, as its card
   arrives (ReelCard). Nothing here moves by itself.
   ───────────────────────────────────────────────────────────── */

import { memo, type CSSProperties } from 'react';
import { ArrowUpRight } from 'lucide-react';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import CheckpointTrail from '@/components/feed/stream/CheckpointTrail';
import {
  dayLabel,
  formatCount,
  formatMultiple,
  formatTopPercent,
  isWinner,
  istDay,
  shiftDay,
  type StreamFeeder,
  type StreamMediaType,
  type StreamPost,
} from '@/lib/feedStream/contract';
import { titleCase } from '@/lib/feedLabels';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

const KIND: Record<StreamMediaType, string> = { reel: 'Reel', carousel: 'Carousel', image: 'Post', unknown: 'Post' };

// each size's type and air (on the type scale; the numeral is a hero one-off)
const SIZE = {
  stage: { gap: 'gap-7', numeral: 'text-[104px]', sign: 'text-[56px]', measuring: 'text-[48px]', note: 'mt-4 text-[15px]', figures: 'gap-y-5', figure: 'text-[28px]', label: 'text-[14px]', caption: 'line-clamp-4 text-[15px]', button: 'h-12' },
  sheet: { gap: 'gap-6', numeral: 'text-[80px]', sign: 'text-[44px]', measuring: 'text-[40px]', note: 'mt-4 text-[14px]', figures: 'gap-y-4', figure: 'text-[24px]', label: 'text-[13px]', caption: 'line-clamp-4 text-[14px]', button: 'h-12' },
  compact: { gap: 'gap-4', numeral: 'text-[64px]', sign: 'text-[34px]', measuring: 'text-[34px]', note: 'mt-3 text-[13px]', figures: 'gap-y-3', figure: 'text-[22px]', label: 'text-[12px]', caption: 'line-clamp-2 text-[13px]', button: 'h-11' },
  // the last step: compact without the caption (the rest is on Instagram)
  tight: { gap: 'gap-3', numeral: 'text-[56px]', sign: 'text-[30px]', measuring: 'text-[28px]', note: 'mt-2 text-[12px]', figures: 'gap-y-2', figure: 'text-[18px]', label: 'text-[12px]', caption: 'hidden', button: 'h-10' },
} as const;
const IST_TIME = new Intl.DateTimeFormat('en-IN', { timeZone: 'Asia/Kolkata', hour: 'numeric', minute: '2-digit', hour12: true });

export type PostReadingProps = {
  post: StreamPost;
  feeder: StreamFeeder | null;
  size: 'stage' | 'sheet' | 'compact' | 'tight';
  // the sheet draws its own header (with the close button); the Day view's reading carries who and when itself
  withHeader?: boolean;
  titleId?: string;
};

// "Sat 4 Oct, 7:42 pm", "Today, 9:05 am" (IST)
export function postedLabel(iso: string) {
  const at = new Date(iso);
  if (Number.isNaN(at.getTime())) return null;
  return `${dayLabel(istDay(at)).long}, ${IST_TIME.format(at).toLowerCase()}`;
}

export function postMeta(post: StreamPost, feeder: StreamFeeder | null) {
  return [feeder?.feedTitle ? titleCase(feeder.feedTitle) : null, KIND[post.mediaType], postedLabel(post.postedAt)]
    .filter(Boolean)
    .join(' · ');
}

// 1.37 → "1.37%", 12.5 → "12.5%"
function formatRate(value: number | null) {
  if (value == null || !Number.isFinite(value)) return '—';
  return `${value.toFixed(value >= 10 ? 1 : 2).replace(/\.?0+$/, '')}%`;
}

// "D7 landed today" when the latest read is fresh
function landedNote(post: StreamPost, today: string) {
  if (!post.latestCheckpoint || !post.resultDay) return null;
  if (post.resultDay === today) return { text: `${post.latestCheckpoint} landed today`, today: true };
  if (post.resultDay === shiftDay(today, -1)) return { text: `${post.latestCheckpoint} landed yesterday`, today: false };
  return null;
}

export function PostWho({ post, feeder, size, titleId }: { post: StreamPost; feeder: StreamFeeder | null; size: PostReadingProps['size']; titleId?: string }) {
  const stage = size === 'stage';
  const meta = postMeta(post, feeder);
  return (
    <div className="flex min-w-0 items-center gap-3">
      <span className={cn('relative shrink-0 overflow-hidden rounded-full shadow-[0_0_0_1px_rgb(var(--fm-fg-rgb)/0.16)]', stage ? 'h-11 w-11' : 'h-10 w-10')}>
        <FeederStoryAvatar feeder={feeder ?? { handle: post.handle, profilePicUrl: null }} className={stage ? 'text-[16px]' : 'text-[14px]'} />
      </span>
      <div className="min-w-0 flex-1">
        <h2 id={titleId} className={cn('truncate font-bold leading-tight text-[var(--st-text)]', stage ? 'text-[18px]' : 'text-[16px]')}>@{post.handle}</h2>
        {meta ? <p className={cn('mt-1 truncate font-medium leading-tight text-[var(--st-text-2)]', stage ? 'text-[14px]' : 'text-[12px]')}>{meta}</p> : null}
      </div>
    </div>
  );
}

function PostReading({ post, feeder, size, withHeader = true, titleId }: PostReadingProps) {
  const look = SIZE[size];
  const top = post.topPercent;
  const measured = top != null && Number.isFinite(top);
  const winner = isWinner(top);
  const landed = landedNote(post, istDay());
  const value = measured ? formatTopPercent(top).slice(0, -1) : null;
  // only the counts a read has measured (a post still measuring has none yet, and shows no row of dashes)
  const figures = [
    post.likes != null ? { key: 'likes', value: formatCount(post.likes), label: post.likes === 1 ? 'like' : 'likes' } : null,
    post.comments != null ? { key: 'comments', value: formatCount(post.comments), label: post.comments === 1 ? 'comment' : 'comments' } : null,
    post.engagementRate != null ? { key: 'er', value: formatRate(post.engagementRate), label: 'engagement' } : null,
    post.multiple != null ? { key: 'usual', value: formatMultiple(post.multiple), label: 'their usual' } : null,
  ].filter((figure): figure is { key: string; value: string; label: string } => figure != null);

  return (
    <div className={cn('flex min-w-0 flex-col', look.gap)}>
      {withHeader ? (
        <div className="st-settle" data-settle="0" style={{ '--i': 0 } as CSSProperties}>
          <PostWho post={post} feeder={feeder} size={size} titleId={titleId} />
        </div>
      ) : null}

      {/* the number: the reading's one lead */}
      <div className="st-settle min-w-0" data-settle="1" style={{ '--i': 1 } as CSSProperties}>
        <p className="flex items-center gap-2 text-[12px] font-black uppercase leading-none tracking-[0.14em] text-[var(--st-text-3)]">
          {measured ? <>Top{post.latestCheckpoint ? <span className="text-[var(--st-text-2)]">at {post.latestCheckpoint}</span> : null}</> : 'Still measuring'}
        </p>
        {value ? (
          <p
            className={cn(
              'mt-3 flex items-start font-black leading-[0.82] tracking-[-0.05em] tabular-nums',
              look.numeral,
              winner ? 'text-[var(--fm-accent-text)]' : 'text-[var(--st-text)]',
            )}
          >
            {value}
            <span className={cn('ml-[0.04em] mt-[0.06em] tracking-[-0.02em]', look.sign)}>%</span>
          </p>
        ) : (
          <p className={cn('mt-3 font-black leading-[0.9] tracking-[-0.04em] text-[var(--st-text-2)]', look.measuring)}>Measuring</p>
        )}
        <p className={cn('flex flex-wrap items-center gap-x-2 gap-y-1 font-medium leading-snug text-[var(--st-text-2)]', look.note)}>
          {landed ? (
            <span className="inline-flex items-center gap-1.5 text-[var(--st-text)]">
              {landed.today ? <span aria-hidden="true" className="h-2 w-2 shrink-0 rounded-full bg-[var(--fm-accent-text)]" /> : null}
              {landed.text}
            </span>
          ) : null}
          {landed ? <span aria-hidden="true" className="text-[var(--st-text-3)]">·</span> : null}
          <span>{measured ? `ranked against @${post.handle}'s own posts` : 'its first rank lands at D1'}</span>
        </p>
      </div>

      <div className="st-settle min-w-0" data-settle="2" style={{ '--i': 2 } as CSSProperties}>
        <CheckpointTrail results={post.results} latest={post.latestCheckpoint} size="md" />
      </div>

      {figures.length ? (
        <dl className={cn('st-settle grid min-w-0 grid-cols-2 gap-x-6 sm:grid-cols-4', look.figures)} data-settle="3" style={{ '--i': 3 } as CSSProperties}>
          {figures.map((figure) => (
            <div key={figure.key} className="flex min-w-0 flex-col-reverse">
              <dt className={cn('mt-1.5 truncate font-medium leading-none text-[var(--st-text-2)]', look.label)}>{figure.label}</dt>
              <dd className={cn('truncate font-black leading-none tracking-[-0.04em] tabular-nums text-[var(--st-text)]', look.figure)}>{figure.value}</dd>
            </div>
          ))}
        </dl>
      ) : null}

      {post.caption ? (
        <p className={cn('st-settle min-w-0 font-medium leading-[1.5] text-[var(--st-text-2)]', look.caption)} data-settle="4" style={{ '--i': 4 } as CSSProperties}>
          {post.caption}
        </p>
      ) : null}

      {post.url ? (
        <a
          href={post.url}
          target="_blank"
          rel="noopener noreferrer"
          onClick={(event) => event.stopPropagation()}
          data-settle="5" style={{ '--i': 5 } as CSSProperties}
          className={cn('st-settle group/ig inline-flex w-fit items-center', look.button, 'gap-2 rounded-full bg-[var(--st-text)] pl-5 pr-4 text-[14px] font-bold text-[var(--st-bg)] outline-none transition-transform duration-150 ease-out active:scale-[0.97] focus-visible:ring-2 focus-visible:ring-[var(--fm-accent-bright)] focus-visible:ring-offset-2 focus-visible:ring-offset-[var(--st-bg)]')}
        >
          Open on Instagram
          <ArrowUpRight className="h-4 w-4 transition-transform duration-200 ease-out group-hover/ig:-translate-y-px group-hover/ig:translate-x-px" strokeWidth={2.4} aria-hidden="true" />
        </a>
      ) : null}
    </div>
  );
}

export default memo(PostReading);
