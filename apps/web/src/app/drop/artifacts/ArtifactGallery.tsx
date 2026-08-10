'use client';

import Link from 'next/link';
import { motion, useReducedMotion } from 'framer-motion';
import type { ReactNode } from 'react';
import Odometer from '@/components/login/Odometer';
import { cn } from '@/lib/utils';

export type GalleryPost = {
  id: string;
  title: string;
  thumbnail?: string;
  url?: string;
};

type Lane = 'Reel' | 'Carousel' | 'Image';
type SpecimenPost = GalleryPost & { lane: Lane };

const EASE = [0.22, 1, 0.36, 1] as const;

const FALLBACK_POSTS: GalleryPost[] = [
  { id: 'fallback-1', title: 'Cut to Ludo' },
  { id: 'fallback-2', title: 'Paper Reveal Cut' },
  { id: 'fallback-3', title: 'Reframe with punchline' },
  { id: 'fallback-4', title: 'Select Protein Source' },
  { id: 'fallback-5', title: 'Freeze-Frame Reveal' },
  { id: 'fallback-6', title: 'Split Product Functions' },
  { id: 'fallback-7', title: 'Food Tour Cuts' },
  { id: 'fallback-8', title: 'Route Stress to Hair Loss' },
  { id: 'fallback-9', title: 'Escalate a complaint' },
  { id: 'fallback-10', title: 'Count Down Then Show' },
];

const ARTIFACTS = [
  'D7 landing',
  'Breakaway',
  'D7 shape',
  'Run motion',
  'In flight',
  'Afterlife',
  'Lane field',
  'Lane shift',
  'Follower pulse',
  'Posting rhythm',
];

function Eyebrow({ children, accent = false }: { children: ReactNode; accent?: boolean }) {
  return (
    <span className={cn('text-[9px] font-black uppercase tracking-[0.2em]', accent ? 'text-[#E11D48]' : 'text-foreground/38')}>
      {children}
    </span>
  );
}

function Receipt({ children, accent = false }: { children: ReactNode; accent?: boolean }) {
  return (
    <span className={cn('inline-flex items-center border-l pl-3 text-[9px] font-black uppercase tracking-[0.15em]', accent ? 'border-[#E11D48] text-[#E11D48]' : 'border-foreground/18 text-foreground/44')}>
      {children}
    </span>
  );
}

function useSpecimenPosts(posts: GalleryPost[]) {
  const source = posts.length >= 8 ? posts : [...posts, ...FALLBACK_POSTS];
  return Array.from({ length: 14 }, (_, index): SpecimenPost => ({
    ...source[index % source.length],
    id: `${source[index % source.length].id}:${index}`,
    lane: (['Reel', 'Carousel', 'Image'] as const)[index % 3],
  }));
}

function PostCard({
  post,
  size = 'md',
  accent = false,
  label,
  className,
}: {
  post: SpecimenPost;
  size?: 'xs' | 'sm' | 'md' | 'lg';
  accent?: boolean;
  label?: string;
  className?: string;
}) {
  const tone = Array.from(post.id).reduce((sum, character) => sum + character.charCodeAt(0), 0) % 4;
  const fallbackBackground = [
    'linear-gradient(145deg, #111 12%, #710a25 72%, #E11D48 135%)',
    'linear-gradient(145deg, #080d1c 12%, #173b92 74%, #2563eb 140%)',
    'linear-gradient(145deg, #0a1510 12%, #176146 74%, #10b981 140%)',
    'linear-gradient(145deg, #17100e 12%, #81401f 74%, #f97316 140%)',
  ][tone];
  const sizes = {
    xs: 'w-[clamp(38px,12vw,52px)] rounded-[8px]',
    sm: 'w-[clamp(58px,19vw,86px)] rounded-[11px]',
    md: 'w-[clamp(82px,27vw,128px)] rounded-[13px]',
    lg: 'w-[clamp(108px,34vw,160px)] rounded-[15px]',
  };
  const frame = (
    <span
      className={cn(
        'relative block aspect-[4/5] overflow-hidden border bg-[#080808]',
        sizes[size],
        accent ? 'border-[#E11D48]/70' : 'border-foreground/14',
        className,
      )}
    >
      <span
        className="absolute inset-0 bg-cover bg-center"
        style={{
          backgroundImage: post.thumbnail ? `url("${post.thumbnail}"), ${fallbackBackground}` : fallbackBackground,
          backgroundSize: 'cover, cover',
          backgroundPosition: 'center, center',
        }}
      >
        <span className="absolute inset-x-0 bottom-0 h-1/2 bg-gradient-to-t from-black/75 to-transparent" />
        {size !== 'xs' ? (
          <span className="absolute inset-x-2 bottom-7 text-[9px] font-black leading-[0.95] tracking-[-0.02em] text-white/78">
            {post.title}
          </span>
        ) : null}
      </span>
      <span className="absolute inset-x-0 top-0 h-px bg-white/25" />
      {(label ?? post.lane) ? (
        <span className="absolute bottom-0 left-0 bg-black/75 px-1.5 py-1 text-[7px] font-black uppercase tracking-[0.12em] text-white/72">
          {label ?? post.lane}
        </span>
      ) : null}
      {post.url ? <span className="absolute right-1.5 top-1.5 text-[9px] font-black text-white/70">↗</span> : null}
    </span>
  );

  return post.url ? (
    <a
      href={post.url}
      target="_blank"
      rel="noreferrer"
      aria-label={`Open ${post.title}`}
      className="block rounded-[15px] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48] focus-visible:ring-offset-4 focus-visible:ring-offset-black"
    >
      {frame}
    </a>
  ) : frame;
}

function ArtifactSection({
  index,
  title,
  mode,
  headline,
  reason,
  gate,
  summary,
  reverse,
  children,
}: {
  index: number;
  title: string;
  mode: 'anchor' | 'context' | 'exception';
  headline: string;
  reason: string;
  gate: string;
  summary: string;
  reverse?: boolean;
  children: ReactNode;
}) {
  const id = `artifact-${String(index).padStart(2, '0')}`;
  return (
    <section id={id} aria-labelledby={`${id}-title`} className="flex min-h-[100svh] scroll-mt-0 items-center border-t border-foreground/10 px-6 py-10 sm:px-10 sm:py-16 lg:min-h-[100dvh] lg:px-16 lg:py-20">
      <div className="mx-auto w-full max-w-[1180px]">
        <div className="flex items-baseline justify-between gap-6">
          <div className="flex items-baseline gap-3">
            <span className="text-[11px] font-black tabular-nums text-[#E11D48]">{String(index).padStart(2, '0')}</span>
            <h2 id={`${id}-title`} className="text-[11px] font-black uppercase tracking-[0.16em] text-foreground/68">{title}</h2>
          </div>
          <span className="text-[8px] font-black uppercase tracking-[0.16em] text-foreground/28">{mode}</span>
        </div>

        <div className={cn('mt-6 grid items-center gap-5 sm:mt-8 sm:gap-8 lg:mt-10 lg:gap-[clamp(52px,7vw,112px)]', reverse ? 'lg:grid-cols-[270px_minmax(0,1fr)]' : 'lg:grid-cols-[minmax(0,1fr)_270px]')}>
          <figure className={cn('min-w-0', reverse && 'lg:order-2')}>
            <figcaption className="sr-only">{summary}</figcaption>
            {children}
          </figure>
          <aside className={cn(reverse && 'lg:order-1')}>
            <p className="max-w-[15ch] text-[clamp(27px,7vw,48px)] font-black leading-[0.92] tracking-[-0.05em] text-foreground">{headline}</p>
            <p className="mt-4 max-w-[42ch] border-l-2 border-[#E11D48] pl-4 text-[11px] font-bold leading-relaxed text-foreground/56 sm:text-[13px]">{reason}</p>
            <div className="mt-7 hidden border-t border-foreground/10 pt-4 lg:block">
              <Eyebrow>Surface when</Eyebrow>
              <p className="mt-2 text-[10px] font-black leading-relaxed text-foreground/38">{gate}</p>
            </div>
          </aside>
        </div>
      </div>
    </section>
  );
}

function Stage({ children, className }: { children: ReactNode; className?: string }) {
  return <div className={cn('relative min-h-[310px] overflow-hidden py-4 sm:min-h-[420px] sm:py-8 lg:min-h-[470px]', className)}>{children}</div>;
}

function Landing({ posts }: { posts: SpecimenPost[] }) {
  const reduce = Boolean(useReducedMotion());
  return (
    <Stage className="flex flex-col justify-between">
      <div className="flex items-baseline justify-between gap-5">
        <Eyebrow accent>D7 settled</Eyebrow>
        <Receipt>same seven-day window</Receipt>
      </div>
      <div className="flex items-end justify-center pb-3 pt-8 sm:pt-12">
        {posts.slice(0, 5).map((post, index) => (
          <motion.div
            key={post.id}
            className={cn('relative', index > 0 && '-ml-8 sm:-ml-6')}
            style={{ zIndex: 10 - index }}
            initial={reduce ? false : { opacity: 0, y: 54, rotate: index % 2 ? 7 : -7 }}
            whileInView={{ opacity: 1, y: 0, rotate: index === 0 ? 0 : index % 2 ? 2 : -2 }}
            viewport={{ once: false, amount: 0.55 }}
            transition={{ delay: index * 0.08, duration: 0.72, ease: EASE }}
          >
            <PostCard post={post} size={index === 0 ? 'lg' : 'md'} accent={index === 0} />
          </motion.div>
        ))}
      </div>
      <div className="flex flex-wrap items-center justify-between gap-4 border-t border-foreground/10 pt-5">
        <p className="text-[18px] font-black tracking-[-0.03em] text-foreground">Six posts settled.</p>
        <Receipt accent>median top 18%</Receipt>
      </div>
    </Stage>
  );
}

function Breakaway({ posts }: { posts: SpecimenPost[] }) {
  const reduce = Boolean(useReducedMotion());
  return (
    <Stage>
      <div className="flex items-baseline justify-between gap-5">
        <Eyebrow>Run 07 · D7</Eyebrow>
        <Receipt accent>29 points clear</Receipt>
      </div>
      <div className="relative mt-5 h-[245px] sm:mt-10 sm:h-[330px]">
        <motion.div
          className="absolute bottom-8 left-[2%] flex items-end sm:bottom-10 sm:left-[4%]"
          initial={reduce ? false : { opacity: 0, x: -24 }}
          whileInView={{ opacity: 1, x: 0 }}
          viewport={{ once: false, amount: 0.6 }}
          transition={{ duration: 0.62, ease: EASE }}
        >
          {posts.slice(1, 5).map((post, index) => (
            <div key={post.id} className={index ? '-ml-8' : ''} style={{ zIndex: 5 - index }}>
              <PostCard post={post} size="sm" />
            </div>
          ))}
          <span className="ml-3 text-[8px] font-black uppercase tracking-[0.15em] text-foreground/32">the run</span>
        </motion.div>
        <motion.div
          className="absolute bottom-2 right-[4%] sm:bottom-6 sm:right-[8%]"
          initial={reduce ? false : { opacity: 0, x: -120, scale: 0.84 }}
          whileInView={{ opacity: 1, x: 0, scale: 1 }}
          viewport={{ once: false, amount: 0.6 }}
          transition={{ delay: 0.24, duration: 0.82, ease: EASE }}
        >
          <PostCard post={posts[0]} size="lg" accent label="Top 2%" />
          <p className="mt-3 max-w-[118px] text-[11px] font-black leading-tight tracking-[-0.02em] text-foreground/72">{posts[0].title}</p>
        </motion.div>
        <motion.span
          aria-hidden
          className="absolute bottom-2 left-[50%] top-6 w-px origin-bottom bg-[#E11D48] sm:bottom-6 sm:left-[54%] sm:top-8"
          initial={reduce ? false : { scaleY: 0, opacity: 0 }}
          whileInView={{ scaleY: 1, opacity: 1 }}
          viewport={{ once: false, amount: 0.6 }}
          transition={{ delay: 0.58, duration: 0.5, ease: EASE }}
        />
      </div>
    </Stage>
  );
}

function Shape({ posts }: { posts: SpecimenPost[] }) {
  const reduce = Boolean(useReducedMotion());
  const rotations = [-26, -13, 0, 19, 34];
  return (
    <Stage>
      <div className="flex items-baseline justify-between gap-5">
        <Eyebrow>D7 shape</Eyebrow>
        <Receipt>5 top · 1 middle · 3 floor</Receipt>
      </div>
      <div className="relative mx-auto mt-4 h-[255px] max-w-[560px] sm:mt-8 sm:h-[330px]">
        {posts.slice(0, 5).map((post, index) => (
          <motion.div
            key={post.id}
            className="absolute bottom-9 left-1/2 origin-bottom sm:bottom-12"
            style={{ zIndex: index + 1 }}
            initial={reduce ? false : { x: '-50%', rotate: 0, y: 42, opacity: 0 }}
            whileInView={{ x: `calc(-50% + ${(index - 2) * 36}px)`, rotate: rotations[index], y: Math.abs(index - 2) * 7, opacity: 1 }}
            viewport={{ once: false, amount: 0.55 }}
            transition={{ delay: index * 0.07, duration: 0.74, ease: EASE }}
          >
            <PostCard post={post} size="md" accent={index === 0} label={index === 0 ? 'Top' : index === 4 ? 'Floor' : undefined} />
          </motion.div>
        ))}
        <span className="absolute bottom-1 left-1/2 h-2 w-2 -translate-x-1/2 rounded-full bg-[#E11D48]" />
        <span className="absolute bottom-0 left-4 text-[8px] font-black uppercase tracking-[0.16em] text-[#E11D48]">Top quarter</span>
        <span className="absolute bottom-0 right-4 text-[8px] font-black uppercase tracking-[0.16em] text-foreground/34">Floor</span>
      </div>
    </Stage>
  );
}

function RunSheet({
  label,
  landing,
  posts,
  active,
}: {
  label: string;
  landing: string;
  posts: SpecimenPost[];
  active?: boolean;
}) {
  return (
    <div className={cn('w-[min(76vw,330px)] border bg-black p-5 sm:p-6', active ? 'border-[#E11D48]/70' : 'border-foreground/16')}>
      <div className="flex items-baseline justify-between gap-5">
        <Eyebrow accent={active}>{label}</Eyebrow>
        <span className={cn('text-[19px] font-black tracking-[-0.04em]', active ? 'text-[#E11D48]' : 'text-foreground/42')}>{landing}</span>
      </div>
      <div className="mt-7 flex items-end">
        {posts.slice(0, 4).map((post, index) => (
          <div key={post.id} className={index ? '-ml-3' : ''} style={{ zIndex: 5 - index }}>
            <PostCard post={post} size="xs" accent={active && index === 0} />
          </div>
        ))}
      </div>
      <div className="mt-6 h-px bg-foreground/10" />
      <p className="mt-3 text-[8px] font-black uppercase tracking-[0.15em] text-foreground/30">equally mature D7 cohort</p>
    </div>
  );
}

function RunMotion({ posts }: { posts: SpecimenPost[] }) {
  const reduce = Boolean(useReducedMotion());
  return (
    <Stage>
      <div className="flex items-baseline justify-between gap-5">
        <Eyebrow>Comparable runs</Eyebrow>
        <Receipt accent>direction confirmed</Receipt>
      </div>
      <div className="relative mx-auto mt-5 h-[270px] max-w-[600px] sm:mt-9 sm:h-[345px]">
        <motion.div
          className="absolute left-[2%] top-4 sm:left-[4%] sm:top-10"
          initial={reduce ? false : { opacity: 0, x: -28, rotate: -4 }}
          whileInView={{ opacity: 0.46, x: 0, rotate: -2 }}
          viewport={{ once: false, amount: 0.55 }}
          transition={{ duration: 0.62, ease: EASE }}
        >
          <RunSheet label="Run 04" landing="Top 42%" posts={posts.slice(0, 4)} />
        </motion.div>
        <motion.div
          className="absolute right-[2%] top-12 sm:right-[4%] sm:top-20"
          initial={reduce ? false : { opacity: 0, x: 120, y: 26, rotate: 3 }}
          whileInView={{ opacity: 1, x: 0, y: 0, rotate: 0 }}
          viewport={{ once: false, amount: 0.55 }}
          transition={{ delay: 0.22, duration: 0.84, ease: EASE }}
        >
          <RunSheet label="Run 07" landing="Top 18%" posts={posts.slice(4, 8)} active />
        </motion.div>
      </div>
    </Stage>
  );
}

function InFlight({ post }: { post: SpecimenPost }) {
  const reduce = Boolean(useReducedMotion());
  return (
    <Stage>
      <div className="flex items-baseline justify-between gap-5">
        <Eyebrow accent>D3 · Top 7%</Eyebrow>
        <Receipt>no D7 yet</Receipt>
      </div>
      <div className="relative mx-auto mt-4 h-[265px] max-w-[560px] sm:mt-8 sm:h-[340px]">
        <motion.div
          className="absolute left-[4%] top-8 z-10 sm:left-[12%] sm:top-14"
          initial={reduce ? false : { opacity: 0, x: -68, rotate: -4 }}
          whileInView={{ opacity: 1, x: 'clamp(86px, 24vw, 150px)', rotate: 0 }}
          viewport={{ once: false, amount: 0.55 }}
          transition={{ duration: 0.9, ease: EASE }}
        >
          <PostCard post={post} size="lg" accent label="D3 · Top 7%" />
        </motion.div>
        <motion.div
          className="absolute bottom-1 right-[2%] z-20 h-[205px] w-[min(54vw,245px)] border border-foreground/22 bg-black p-4 sm:bottom-8 sm:right-[8%] sm:h-[255px] sm:p-5"
          initial={reduce ? false : { opacity: 0, y: 24 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: false, amount: 0.55 }}
          transition={{ delay: 0.22, duration: 0.62, ease: EASE }}
        >
          <span className="block h-[3px] w-12 bg-[#E11D48]" />
          <p className="mt-4 text-[34px] font-black leading-none tracking-[-0.07em] text-foreground sm:mt-5 sm:text-[40px]">D7</p>
          <p className="mt-3 max-w-[9ch] text-[13px] font-black leading-[1.05] text-foreground/70">Reader stays closed.</p>
          <p className="absolute bottom-4 left-4 text-[7px] font-black uppercase tracking-[0.15em] text-foreground/30 sm:bottom-5 sm:left-5 sm:text-[8px]">opens in four days</p>
        </motion.div>
      </div>
    </Stage>
  );
}

function Afterlife({ post }: { post: SpecimenPost }) {
  const reduce = Boolean(useReducedMotion());
  return (
    <Stage>
      <div className="flex items-baseline justify-between gap-5">
        <Eyebrow>D7 stayed on record</Eyebrow>
        <Receipt accent>+31 percentile points</Receipt>
      </div>
      <div className="relative mx-auto mt-5 h-[265px] max-w-[520px] sm:mt-10 sm:h-[330px]">
        <motion.div
          className="absolute left-[13%] top-5 opacity-40 grayscale sm:left-[22%] sm:top-8"
          initial={reduce ? false : { opacity: 0, x: -24 }}
          whileInView={{ opacity: 0.4, x: 0 }}
          viewport={{ once: false, amount: 0.55 }}
          transition={{ duration: 0.58, ease: EASE }}
        >
          <PostCard post={post} size="lg" label="D7 · Top 48%" />
        </motion.div>
        <motion.div
          className="absolute right-[12%] top-12 sm:right-[20%] sm:top-20"
          initial={reduce ? false : { opacity: 0, x: -86, y: -38, scale: 0.92 }}
          whileInView={{ opacity: 1, x: 0, y: 0, scale: 1 }}
          viewport={{ once: false, amount: 0.55 }}
          transition={{ delay: 0.28, duration: 0.84, ease: EASE }}
        >
          <PostCard post={{ ...post, id: `${post.id}:d21` }} size="lg" accent label="D21 · Top 17%" />
          <p className="mt-3 text-[10px] font-black uppercase tracking-[0.13em] text-[#E11D48]">late amendment</p>
        </motion.div>
      </div>
    </Stage>
  );
}

function LaneStack({
  name,
  posts,
  count,
  score,
  active,
}: {
  name: Lane;
  posts: SpecimenPost[];
  count: number;
  score: string;
  active?: boolean;
}) {
  const depth = Math.min(count, 4);
  return (
    <div className="min-w-0">
      <div className="relative mx-auto h-[210px] w-[112px] sm:h-[230px] sm:w-[128px] lg:h-[270px] lg:w-[148px]">
        {Array.from({ length: depth }).map((_, index) => (
          <div key={`${name}:${index}`} className="absolute left-1/2 top-0 -translate-x-1/2" style={{ transform: `translate(-50%, ${index * 11}px)`, zIndex: index + 1 }}>
            <PostCard post={posts[index % posts.length]} size="md" accent={active && index === depth - 1} label={name} className="!w-[clamp(76px,22vw,112px)] sm:!w-[128px] lg:!w-[148px]" />
          </div>
        ))}
      </div>
      <p className="text-center text-[14px] font-black tracking-[-0.02em] text-foreground">{name}</p>
      <p className="mt-1 text-center text-[8px] font-black uppercase tracking-[0.13em] text-foreground/34">{count} posts · {score}</p>
    </div>
  );
}

function LaneField({ posts }: { posts: SpecimenPost[] }) {
  const reduce = Boolean(useReducedMotion());
  const lanes: { name: Lane; count: number; score: string; posts: SpecimenPost[]; active?: boolean }[] = [
    { name: 'Reel', count: 8, score: '1.08× usual', posts: posts.slice(0, 4) },
    { name: 'Carousel', count: 5, score: '1.31× usual', posts: posts.slice(4, 8), active: true },
    { name: 'Image', count: 3, score: '0.94× usual', posts: posts.slice(8, 12) },
  ];
  return (
    <Stage>
      <div className="flex items-baseline justify-between gap-5">
        <Eyebrow>Format field · D7</Eyebrow>
        <Receipt accent>each versus its own history</Receipt>
      </div>
      <div className="mt-7 grid grid-cols-3 items-end gap-2 sm:mt-12 sm:gap-8">
        {lanes.map((lane, index) => (
          <motion.div
            key={lane.name}
            initial={reduce ? false : { opacity: 0, y: 34 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: false, amount: 0.55 }}
            transition={{ delay: index * 0.12, duration: 0.68, ease: EASE }}
          >
            <LaneStack {...lane} />
          </motion.div>
        ))}
      </div>
    </Stage>
  );
}

function LaneShift({ posts }: { posts: SpecimenPost[] }) {
  const reduce = Boolean(useReducedMotion());
  return (
    <Stage>
      <div className="flex items-baseline justify-between gap-5">
        <span>
          <Eyebrow>Run 06</Eyebrow>
          <span className="ml-3 text-[9px] font-black uppercase tracking-[0.14em] text-foreground/55">Reels led</span>
        </span>
        <span className="text-right">
          <Eyebrow accent>Run 07</Eyebrow>
          <span className="ml-3 text-[9px] font-black uppercase tracking-[0.14em] text-[#E11D48]">Carousels lead</span>
        </span>
      </div>
      <div className="relative mx-auto mt-4 h-[270px] max-w-[600px] sm:mt-7 sm:h-[345px]">
        <motion.div
          className="absolute left-1/2 top-5 sm:top-14"
          initial={reduce ? false : { opacity: 1, x: 0, y: 0, scale: 1, rotate: 0 }}
          whileInView={{ opacity: 0.34, x: -70, y: 42, scale: 0.82, rotate: -8 }}
          viewport={{ once: false, amount: 0.55 }}
          transition={{ delay: 0.42, duration: 0.8, ease: EASE }}
        >
          <div className="-translate-x-1/2">
            <PostCard post={{ ...posts[0], lane: 'Reel' }} size="lg" label="Previous lead · Reel" className="lg:!w-[210px]" />
          </div>
        </motion.div>
        {[1, 2, 3].map((offset, index) => (
          <motion.div
            key={posts[offset].id}
            className="absolute left-1/2 top-5 sm:top-14"
            style={{ zIndex: 8 + index }}
            initial={reduce ? false : { opacity: 0, x: 70, y: 78, rotate: 12, scale: 0.9 }}
            whileInView={{ opacity: 1, x: (index - 1) * 34, y: index * 15, rotate: (index - 1) * 4, scale: 1 }}
            viewport={{ once: false, amount: 0.55 }}
            transition={{ delay: 0.22 + index * 0.18, duration: 0.76, ease: EASE }}
          >
            <div className="-translate-x-1/2">
              <PostCard post={{ ...posts[offset], lane: 'Carousel' }} size="lg" accent={index === 2} label={index === 2 ? 'New lead · Carousel' : 'New D7 · Carousel'} className="lg:!w-[210px]" />
            </div>
          </motion.div>
        ))}
        <motion.div
          className="absolute bottom-1 left-1/2 -translate-x-1/2 whitespace-nowrap"
          initial={reduce ? false : { opacity: 0, y: 10 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: false, amount: 0.55 }}
          transition={{ delay: 0.9, duration: 0.45, ease: EASE }}
        >
          <Receipt accent>3 new D7 posts · 1.31× usual</Receipt>
        </motion.div>
      </div>
    </Stage>
  );
}

function FollowerPulse() {
  const reduce = Boolean(useReducedMotion());
  return (
    <Stage className="flex flex-col justify-between">
      <div className="flex items-baseline justify-between gap-5">
        <Eyebrow>Follower pulse</Eyebrow>
        <Receipt>observed · not attributed</Receipt>
      </div>
      <motion.div
        className="mx-auto w-full max-w-[520px] py-16"
        initial={reduce ? false : { opacity: 0, y: 24 }}
        whileInView={{ opacity: 1, y: 0 }}
        viewport={{ once: false, amount: 0.6 }}
        transition={{ duration: 0.7, ease: EASE }}
      >
        <div className="flex items-end justify-between gap-6 border-b border-foreground/14 pb-5">
          <span>
            <Eyebrow>Seven days ago</Eyebrow>
            <span className="mt-2 block text-[22px] font-black tabular-nums tracking-[-0.04em] text-foreground/32">128,420</span>
          </span>
          <span className="min-w-0 flex-1 text-right">
            <Eyebrow accent>Now</Eyebrow>
            <Odometer value={133220} animateOnMount revealDelayMs={280} className="mt-2 text-[clamp(34px,8vw,52px)] font-black tracking-[-0.06em] text-foreground" />
          </span>
        </div>
        <div className="mt-5 flex flex-wrap items-baseline justify-between gap-4">
          <p className="text-[17px] font-black tracking-[-0.03em] text-[#E11D48]">+4,800</p>
          <Receipt accent>5.6× usual weekly movement</Receipt>
        </div>
      </motion.div>
      <p className="max-w-[34ch] text-[10px] font-black leading-relaxed text-foreground/32">The count earns the room because it left the account’s own normal range.</p>
    </Stage>
  );
}

function PostingRhythm({ posts }: { posts: SpecimenPost[] }) {
  const reduce = Boolean(useReducedMotion());
  const weeks = [2, 3, 2, 0, 1, 5];
  return (
    <Stage>
      <div className="flex items-baseline justify-between gap-5">
        <Eyebrow>Six-week rhythm</Eyebrow>
        <Receipt accent>return after a pause</Receipt>
      </div>
      <div className="mt-7 grid grid-cols-6 gap-1.5 sm:mt-12 sm:gap-5">
        {weeks.map((count, weekIndex) => (
          <motion.div
            key={weekIndex}
            className="min-w-0 border-l border-foreground/12 pl-1.5 sm:pl-3"
            initial={reduce ? false : { opacity: 0, y: 22 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: false, amount: 0.55 }}
            transition={{ delay: weekIndex * 0.09, duration: 0.55, ease: EASE }}
          >
            <span className={cn('text-[8px] font-black uppercase tracking-[0.13em]', weekIndex === 5 ? 'text-[#E11D48]' : 'text-foreground/30')}>{weekIndex === 5 ? 'Now' : `W−${5 - weekIndex}`}</span>
            <div className="relative mt-4 h-[190px] sm:mt-5 sm:h-[270px]">
              {count === 0 ? <span className="absolute left-0 right-2 top-1/2 h-px bg-foreground/14" /> : null}
              {Array.from({ length: Math.min(count, 5) }).map((_, index) => (
                <div key={index} className="absolute left-0" style={{ top: `${index * 30}px`, zIndex: index + 1 }}>
                  <PostCard post={posts[(weekIndex + index) % posts.length]} size="xs" accent={weekIndex === 5 && index === count - 1} label="" />
                </div>
              ))}
            </div>
            <span className={cn('text-[18px] font-black tabular-nums', weekIndex === 5 ? 'text-[#E11D48]' : 'text-foreground/45')}>{count}</span>
          </motion.div>
        ))}
      </div>
    </Stage>
  );
}

export default function ArtifactGallery({ posts }: { posts: GalleryPost[] }) {
  const specimen = useSpecimenPosts(posts);
  return (
    <main id="top" className="min-h-[100svh] overflow-x-hidden bg-black text-white">
      <header className="relative flex min-h-[100svh] items-center overflow-hidden px-6 py-12 sm:px-10 lg:px-16 lg:py-16">
        <span aria-hidden className="absolute -right-24 -top-16 h-[280px] w-[280px] rounded-full border-[44px] border-[#E11D48] sm:-right-28 sm:-top-40 sm:h-[560px] sm:w-[560px] sm:border-[88px]" />
        <div className="relative mx-auto w-full max-w-[1180px]">
          <div className="flex items-baseline justify-between gap-6">
            <span>
              <Eyebrow accent>Feed Me · Feeder Reader</Eyebrow>
              <span className="mt-2 block text-[9px] font-black uppercase tracking-[0.16em] text-foreground/30">server-shaped specimen · all ten</span>
            </span>
            <Link href="/drop" className="text-[9px] font-black uppercase tracking-[0.16em] text-foreground/38 transition-colors hover:text-[#E11D48] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48] focus-visible:ring-offset-4 focus-visible:ring-offset-black">Back to Reader</Link>
          </div>
          <div className="mt-16 grid gap-10 lg:grid-cols-[minmax(0,1fr)_300px] lg:items-end">
            <div>
              <h1 className="max-w-[11ch] text-[clamp(48px,10vw,102px)] font-black leading-[0.82] tracking-[-0.07em] text-foreground">Ten ways the feed moves.</h1>
              <p className="mt-7 max-w-[560px] text-[14px] font-bold leading-relaxed text-foreground/52 sm:text-[16px]">One visual language. Ten different physical verbs. A real run surfaces only the three to five facts that clear their server gates.</p>
            </div>
            <div className="border-l-2 border-[#E11D48] pl-5">
              <p className="text-[13px] font-black leading-relaxed text-foreground/68">Bites remain D7 intelligence. These artifacts describe the measurable world around them.</p>
              <p className="mt-3 text-[9px] font-black uppercase tracking-[0.15em] text-foreground/30">No Bite ranking. No memory rewrite.</p>
            </div>
          </div>
          <nav aria-label="Artifact index" className="mt-14 grid grid-cols-2 gap-x-5 gap-y-3 border-t border-foreground/10 pt-6 sm:grid-cols-5">
            {ARTIFACTS.map((artifact, index) => (
              <a key={artifact} href={`#artifact-${String(index + 1).padStart(2, '0')}`} className="group flex items-baseline gap-2 text-[8px] font-black uppercase tracking-[0.13em] text-foreground/34 transition-colors hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[#E11D48]">
                <span className="text-[#E11D48]">{String(index + 1).padStart(2, '0')}</span>
                <span>{artifact}</span>
              </a>
            ))}
          </nav>
        </div>
      </header>

      <ArtifactSection index={1} title="D7 Landing · Settle" mode="anchor" headline="Six posts settled. One fair window." reason="The stable metric anchor for the run. The posts are the visual; the median stays a receipt." gate="At least three newly settled D7 posts. Full treatment only after a material landing change." summary="Six linked posts settle into a strongest-first D7 stack with a median top-18-percent receipt."><Landing posts={specimen.slice(0, 6)} /></ArtifactSection>
      <ArtifactSection index={2} title="Breakaway · Detach" mode="exception" headline="One post left the run." reason="The measured empty space makes the outlier obvious before the user reads the receipt." gate="Top-10% D7 post, at least 20 points ahead of the run median and 15 ahead of second place." summary="One linked post separates from the run pack across a measured gap." reverse><Breakaway posts={specimen.slice(1, 7)} /></ArtifactSection>
      <ArtifactSection index={3} title="D7 Shape · Fan" mode="context" headline="The average hid a split." reason="A fan made from real posts distinguishes a tight run from a divided one without anonymous marks." gate="At least five D7 posts and a material quartile concentration, split or wide IQR." summary="Five post cards fan from one hinge to reveal a split D7 distribution."><Shape posts={specimen.slice(2, 7)} /></ArtifactSection>
      <ArtifactSection index={4} title="Run Motion · Replace" mode="context" headline="The account changed position." reason="Current evidence replaces the prior run sheet. No trajectory graph appears during flat runs." gate="At least three comparable Reader cohorts and a sustained 10-point-or-greater movement or reversal." summary="The current D7 run sheet physically replaces a prior run sheet." reverse><RunMotion posts={specimen.slice(0, 8)} /></ArtifactSection>
      <ArtifactSection index={5} title="In Flight · Withhold" mode="exception" headline="It is moving early. D7 is still closed." reason="The partially filed post creates anticipation while refusing to make a prediction or enter memory early." gate="A D1 or D3 post is in the same-age top decile, with no valid D7 result and a sufficient comparator pool." summary="A D3 post stops partly inside a closed D7 sleeve."><InFlight post={specimen[5]} /></ArtifactSection>
      <ArtifactSection index={6} title="Afterlife · Echo" mode="exception" headline="The read closed. The post kept moving." reason="D21 becomes an amendment beside the preserved D7 observation, never a replacement for it." gate="One post moves at least 40 percentile points, or two move at least 25 in the same direction, from D7 to D21." summary="A D21 impression moves forward while the original D7 card remains on record." reverse><Afterlife post={specimen[6]} /></ArtifactSection>
      <ArtifactSection index={7} title="Lane Field · Weigh" mode="context" headline="Volume and lift are not the same thing." reason="Thumbnail-stack depth shows output. The receipt states how each format performed against its own history." gate="At least two formats with three valid D7 posts each and a material share or normalised-performance gap." summary="Reel, Carousel and Image thumbnail stacks compare output depth and performance versus lane history."><LaneField posts={specimen.slice(0, 12)} /></ArtifactSection>
      <ArtifactSection index={8} title="Lane Shift · Handoff" mode="exception" headline="Carousels moved in front." reason="New evidence takes the front of one shared stack and physically sends the previous leader behind." gate="Comparable lane windows, at least three posts per lane, a true leader reversal and a clear normalised margin." summary="Three new Carousel posts move to the front of the evidence stack and replace the previous Reel leader." reverse><LaneShift posts={specimen.slice(3, 9)} /></ArtifactSection>
      <ArtifactSection index={9} title="Follower Pulse · Roll" mode="exception" headline="The audience left its normal range." reason="The count behaves mechanically, not theatrically. Feed Me observes the breach without naming a cause." gate="At least six baseline weeks and a gain or loss that clears both the account-relative and absolute anomaly gates." summary="A restrained follower counter rolls from 128,420 to 133,220, showing an unusual seven-day gain."><FollowerPulse /></ArtifactSection>
      <ArtifactSection index={10} title="Posting Rhythm · Punctuate" mode="context" headline="The pause ended." reason="Actual post edges pack each week. Silence remains empty space; the returning posts become the punctuation." gate="Six weeks of coverage and a material burst, pause or return relative to the feeder’s usual posting gap." summary="Six weekly trays use linked post edges to show a pause followed by a five-post return." reverse><PostingRhythm posts={specimen.slice(4, 12)} /></ArtifactSection>

      <footer className="border-t border-foreground/10 px-6 py-20 sm:px-10 lg:px-16 lg:py-24">
        <div className="mx-auto flex max-w-[1180px] flex-col gap-10 sm:flex-row sm:items-end sm:justify-between">
          <div>
            <span className="block h-[3px] w-16 bg-[#E11D48]" />
            <p className="mt-6 max-w-[13ch] text-[clamp(34px,7vw,62px)] font-black leading-[0.9] tracking-[-0.055em] text-foreground">Ten candidates. Three to five earn the run.</p>
          </div>
          <div className="flex gap-6 text-[9px] font-black uppercase tracking-[0.15em]">
            <a href="#top" className="text-foreground/38 hover:text-foreground">Back to top</a>
            <Link href="/drop" className="text-[#E11D48]">Open Reader</Link>
          </div>
        </div>
      </footer>
    </main>
  );
}
