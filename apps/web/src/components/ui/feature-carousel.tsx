'use client';

/* eslint-disable @next/next/no-img-element -- carousel images are dynamic feed media assets */

import * as React from 'react';
import { motion, useReducedMotion } from 'framer-motion';
import { cn } from '@/lib/utils';

export type FeatureCarouselImage = {
  src: string;
  alt: string;
  label?: string;
};

type FeatureCarouselProps = React.HTMLAttributes<HTMLDivElement> & {
  images: FeatureCarouselImage[];
  intervalMs?: number;
  isPlaying?: boolean;
  density?: 'standard' | 'cover';
  axis?: 'x' | 'y';
};

function CarouselImageCard({ image }: { image: FeatureCarouselImage }) {
  const [failedSrc, setFailedSrc] = React.useState<string | null>(null);
  const failed = Boolean(image.src && failedSrc === image.src);

  return (
    <div className="relative h-full w-full overflow-hidden rounded-[30px] border border-white/[0.14] bg-black shadow-[0_30px_70px_-30px_rgba(0,0,0,0.9),inset_0_1px_0_rgba(255,255,255,0.1)]">
      {image.src && !failed ? (
        <img
          src={image.src}
          alt={image.alt}
          loading="lazy"
          draggable={false}
          onError={() => setFailedSrc(image.src)}
          className="h-full w-full object-cover"
        />
      ) : (
        <div className="flex h-full w-full flex-col justify-between bg-[radial-gradient(circle_at_25%_18%,rgba(225,29,72,0.28),transparent_38%),linear-gradient(145deg,rgba(255,255,255,0.08),rgba(255,255,255,0.02))] p-4">
          <div className="w-fit rounded-full border border-white/[0.12] bg-white/[0.06] px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-white/54">
            Preview pending
          </div>
          <div className="text-[10px] font-black uppercase tracking-[0.12em] text-white/36">
            {image.label || 'Evidence'}
          </div>
        </div>
      )}
      <div className="pointer-events-none absolute inset-0 rounded-[inherit] shadow-[inset_0_0_0_1px_rgba(255,255,255,0.04),inset_0_-110px_80px_-84px_rgba(0,0,0,0.96)]" />
      {image.label && (
        <div className="pointer-events-none absolute left-3 top-3 rounded-full border border-white/[0.12] bg-black/46 px-2.5 py-1 text-[8px] font-black uppercase tracking-[0.14em] text-white/72 backdrop-blur-[10px]">
          {image.label}
        </div>
      )}
    </div>
  );
}

function relativePosition(index: number, currentIndex: number, total: number): number {
  if (total <= 0) return 0;
  let position = (index - currentIndex + total) % total;
  if (position > Math.floor(total / 2)) position -= total;
  return position;
}

export function FeatureCarousel({
  images,
  intervalMs = 3800,
  isPlaying = true,
  density = 'standard',
  axis = 'x',
  className,
  ...props
}: FeatureCarouselProps) {
  const reduceMotion = useReducedMotion();
  const [currentIndex, setCurrentIndex] = React.useState(() => Math.floor(images.length / 2));
  const total = images.length;
  const safeIndex = total > 0 ? currentIndex % total : 0;

  React.useEffect(() => {
    if (reduceMotion || !isPlaying || total <= 1) return undefined;
    const timer = window.setInterval(() => {
      setCurrentIndex((index) => (index + 1) % total);
    }, intervalMs);
    return () => window.clearInterval(timer);
  }, [intervalMs, isPlaying, reduceMotion, total]);

  if (total === 0) {
    return (
      <div
        className={cn(
          'relative flex h-full min-h-[220px] w-full items-center justify-center overflow-hidden',
          className,
        )}
        {...props}
      >
        <div className="aspect-[4/5] h-[82%] rounded-[28px] border border-foreground/[0.08] bg-foreground/[0.04] dark:border-white/[0.1] dark:bg-white/[0.04]" />
      </div>
    );
  }

  return (
    <div
      className={cn(
        'relative h-full w-full overflow-hidden [perspective:1400px]',
        density === 'cover' ? 'min-h-0' : 'min-h-[240px]',
        className,
      )}
      {...props}
    >
      {total > 3 && (
        <div className="pointer-events-none absolute right-3 top-3 z-40 rounded-full border border-white/[0.18] bg-black/52 px-2.5 py-1 text-[7px] font-black uppercase tracking-[0.14em] text-white/76 shadow-[0_16px_30px_-20px_rgba(0,0,0,0.9)] backdrop-blur-[14px] sm:right-5 sm:top-5 sm:px-3 sm:py-1.5 sm:text-[9px]">
          {total} proof cards
        </div>
      )}
      <div
        className={cn(
          'absolute flex items-center justify-center',
          axis === 'y'
            ? density === 'cover'
              ? 'inset-x-0 inset-y-[-18%]'
              : 'inset-x-0 inset-y-[-12%]'
            : density === 'cover'
              ? 'inset-x-[-18%] inset-y-0'
              : 'inset-x-[-12%] inset-y-0',
        )}
      >
        {images.map((image, index) => {
          const position = relativePosition(index, safeIndex, total);
          const distance = Math.abs(position);
          const isCenter = position === 0;
          const isAdjacent = distance === 1;
          const isDepth = distance === 2;
          const visible = distance <= 2;
          const cover = density === 'cover';
          return (
            <motion.div
              key={`${image.src}-${index}`}
              className={cn(
                'absolute aspect-[4/5]',
                axis === 'y'
                  ? cover
                    ? 'w-[92%] max-w-[156px]'
                    : 'w-[88%] max-w-[132px]'
                  : cn('h-[88%]', cover ? 'max-h-[500px]' : 'max-h-[360px]'),
              )}
              initial={false}
              animate={{
                x: axis === 'y' ? `${position * (cover ? 5 : 4)}%` : `${position * (cover ? 62 : 58)}%`,
                y: axis === 'y' ? `${position * (cover ? 74 : 66)}%` : isDepth ? 10 : 0,
                scale: isCenter ? 1 : isAdjacent ? (cover ? 0.8 : 0.82) : cover ? 0.6 : 0.58,
                rotateX: axis === 'y' ? position * (cover ? 10 : 8) : 0,
                rotateY: axis === 'y' ? position * 2 : position * (cover ? -14 : -12),
                rotateZ: position * (cover ? 2.6 : 1.8),
                opacity: isCenter ? 1 : isAdjacent ? (cover ? 0.52 : 0.46) : cover ? 0.2 : 0.13,
                filter: isCenter ? 'blur(0px)' : isAdjacent ? 'blur(3px)' : 'blur(7px)',
              }}
              transition={{
                duration: reduceMotion ? 0 : cover ? 1.55 : 1.1,
                ease: [0.16, 1, 0.3, 1],
              }}
              style={{
                zIndex: isCenter ? 30 : isAdjacent ? 20 : 10 - distance,
                visibility: visible ? 'visible' : 'hidden',
              }}
            >
              <CarouselImageCard image={image} />
            </motion.div>
          );
        })}
      </div>
    </div>
  );
}
