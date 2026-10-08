'use client';

/* ─────────────────────────────────────────────────────────────
   POST MEDIA — a post's picture.

   PostPicture is the picture itself, filling whatever box it is
   given: always the poster (the stable /api/media picture), the
   same <img> in the same place whatever the view, so a post moved
   between the Feed's views never draws its picture again. A post
   with no picture to show (the stream says so) wears its drawn
   cover straight away (PostCover), and so does one whose picture
   fails. In a 4:5 frame (framed: the Day view, the sheet) a picture
   taller than the frame fills it; one 4:5 or wider shows whole,
   over the wash behind the frame.

   Over the poster, only while the post is the one in view (active):
   a reel's stored preview (muted, inline, looping, one at a time
   app-wide, shown only from its first presented frame; one that
   won't start within 2.5s, or errors, is let go and the poster
   drifts once instead), or a carousel's slides (a horizontal snap,
   its own scroller, dots, arrows on a wide screen, a video slide
   playing while it is the one showing). Nothing plays or drifts
   under reduced motion, with Save-Data on, or while the page is
   hidden.

   Wash is the picture shrunk to a few dozen pixels (softened and
   saturated there) and stretched over its box: the backdrop of a
   framed post, blurred for free.

   PostMedia (the sheet's) is a box with the wash behind a 4:5
   frame holding the picture. Whatever goes over a picture (a
   number, a ▶) is the piece's.
   ───────────────────────────────────────────────────────────── */

import { memo, useCallback, useEffect, useRef, useState, useSyncExternalStore, type MouseEvent as ReactMouseEvent, type RefObject, type SyntheticEvent } from 'react';
import { useReducedMotion } from 'framer-motion';
import { ChevronLeft, ChevronRight } from 'lucide-react';
import PostCover from '@/components/feed/stream/PostCover';
import { claimPlayback } from '@/components/feed/stream/playback';
import { mediaProxyUrl, type PostMediaProps, type StreamPost, type StreamSlide } from '@/lib/feedStream/contract';
import { BEAT_EASE_CSS } from '@/lib/motion';
import { cn } from '@/lib/utils';
import '@/components/feed/stream/stream.css';

// a preview that hasn't started playing by now is let go, and the poster drifts instead
const START_TIMEOUT_MS = 2500;
// the drift: one slow push-in over the poster, held where it ends (never looped)
const DRIFT_MS = 8000;
const DRIFT_EASE = 'cubic-bezier(0.25, 0.1, 0.25, 1)';
const DRIFT_FRAMES: Keyframe[] = [
  { transform: 'translate3d(0px, 0px, 0px) scale(1)' },
  { transform: 'translate3d(-6px, -5px, 0px) scale(1.06)' },
];
// a drift let go while its card is still on screen eases back to rest rather than jumping
const DRIFT_RELEASE_MS = 420;

const IMG = 'st-img absolute inset-0 block h-full w-full object-cover';
const VIDEO = 'st-video absolute inset-0 block h-full w-full object-cover';

type VideoWithFrames = HTMLVideoElement & { requestVideoFrameCallback?: (callback: () => void) => number };

/* the poster's picture, or null for the cover: none to show, or this one failed (it is forgotten when the post's
   URL changes). The picture handles its own failure (data-media-fallback="off" keeps the global swap away) */
function usePoster(post: StreamPost) {
  const [failedSrc, setFailedSrc] = useState<string | null>(null);
  const src = post.thumbnailUrl && post.thumbnailUrl !== failedSrc ? post.thumbnailUrl : null;
  const onError = useCallback((event: SyntheticEvent<HTMLImageElement>) => {
    setFailedSrc(event.currentTarget.getAttribute('src'));
  }, []);
  return { src, onError };
}

// a picture 4:5 or wider shows whole in a frame (letterboxed); a taller one fills it (stream.css, .st-framed)
const FRAME_ASPECT = 4 / 5;
function markShape(img: HTMLImageElement) {
  img.dataset.shape = img.naturalWidth / img.naturalHeight >= FRAME_ASPECT - 0.01 ? 'wide' : 'tall';
}

/* A picture already decoded in the cache shows at once; the rest develop when they load (stream.css, .st-img). One that
   loads within a beat of being drawn came from the cache too (WebKit reports even those a frame or two late): it shows
   at once as well, rather than developing up from dark on every move that brings it on screen */
const INSTANT_MS = 150;
const born = new WeakMap<HTMLImageElement, number>();

function markIfLoaded(img: HTMLImageElement | null) {
  if (!img) return;
  if (!born.has(img)) born.set(img, performance.now());
  if (img.complete && img.naturalWidth > 0) {
    markShape(img);
    img.dataset.instant = '';
    img.dataset.loaded = '';
  }
}

function markLoaded(event: SyntheticEvent<HTMLImageElement>) {
  const img = event.currentTarget;
  markShape(img);
  if (performance.now() - (born.get(img) ?? 0) < INSTANT_MS) {
    img.dataset.instant = '';
    img.dataset.loaded = '';
    return;
  }
  // one arriving later develops once it can be drawn: faded in while still decoding (decoding="async"), it showed
  // nothing for the first part of the fade, then the picture all at once
  const reveal = () => {
    img.dataset.loaded = '';
  };
  if (typeof img.decode === 'function') img.decode().then(reveal, reveal);
  else reveal();
}

/* ── the page's visibility, and the quiet signals ─────────────────────────── */

function subscribeVisibility(onChange: () => void) {
  document.addEventListener('visibilitychange', onChange);
  return () => document.removeEventListener('visibilitychange', onChange);
}

const readHidden = () => document.hidden;
// on the server and while hydrating the page counts as hidden, so a clip never mounts before the client can tell
const readHiddenOnServer = () => true;

function saveData() {
  if (typeof navigator === 'undefined') return false;
  return Boolean((navigator as Navigator & { connection?: { saveData?: boolean } }).connection?.saveData);
}

/* ── the wash ─────────────────────────────────────────────────────────────── */

const WASH_SIZE = 24;
const WASH_KEEP = 240;
const washes = new Map<string, HTMLCanvasElement>();

function washOf(src: string): Promise<HTMLCanvasElement | null> {
  const kept = washes.get(src);
  if (kept) return Promise.resolve(kept);
  const image = new Image();
  image.decoding = 'async';
  image.src = src;
  return image.decode().then(() => {
    const scale = WASH_SIZE / Math.max(image.naturalWidth, image.naturalHeight);
    const wash = document.createElement('canvas');
    wash.width = Math.max(1, Math.round(image.naturalWidth * scale));
    wash.height = Math.max(1, Math.round(image.naturalHeight * scale));
    const context = wash.getContext('2d');
    if (!context) return null;
    context.imageSmoothingEnabled = true;
    context.imageSmoothingQuality = 'high';
    // where the canvas filter exists (it is only ever drawn this small)
    if ('filter' in context) context.filter = 'blur(0.8px) saturate(1.25)';
    context.drawImage(image, 0, 0, wash.width, wash.height);
    if (washes.size >= WASH_KEEP) washes.delete(washes.keys().next().value as string);
    washes.set(src, wash);
    return wash;
  }, () => null);
}

export const Wash = memo(function Wash({ src, className }: { src: string | null; className?: string }) {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);
  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas || !src) return undefined;
    let live = true;
    void washOf(src).then((wash) => {
      if (!live || !wash) return;
      canvas.width = wash.width;
      canvas.height = wash.height;
      canvas.getContext('2d')?.drawImage(wash, 0, 0);
      canvas.dataset.ready = '';
    });
    return () => {
      live = false;
    };
  }, [src]);
  return (
    <span aria-hidden="true" className={cn('pointer-events-none absolute inset-0 block overflow-hidden bg-[var(--st-surface)]', className)}>
      {src ? <canvas ref={canvasRef} width={1} height={1} className="st-ambient absolute inset-0 block h-full w-full object-cover" /> : null}
      <span className="absolute inset-0 bg-[linear-gradient(to_bottom,rgb(0_0_0/0.2),rgb(0_0_0/0.55)_60%,rgb(0_0_0/0.75))]" />
    </span>
  );
});

/* ── a reel's preview over its poster ──────────────────────────────────────── */

function usePreview(post: StreamPost, active: boolean, posterRef: RefObject<HTMLImageElement | null>) {
  const videoRef = useRef<HTMLVideoElement | null>(null);
  const reduce = Boolean(useReducedMotion());
  const hidden = useSyncExternalStore(subscribeVisibility, readHidden, readHiddenOnServer);
  // still: no preview and no drift
  const still = reduce || hidden || saveData();
  // the preview that wouldn't play while the card was in view, forgotten once it leaves (the next visit tries again)
  const [failedUrl, setFailedUrl] = useState<string | null>(null);
  if (!active && failedUrl !== null) setFailedUrl(null);
  const failed = failedUrl !== null && failedUrl === post.previewUrl;
  const playing = active && !still && !failed && Boolean(post.previewUrl) && post.slides.length <= 1;
  const drifting = active && !still && failed;

  // claim playback, start it, reveal it on its first frame, or give up and let the poster drift
  useEffect(() => {
    const video = videoRef.current;
    const url = post.previewUrl;
    if (!playing || !video || !url) return undefined;
    let settled = false;
    let disposed = false;
    let timer = 0;
    const stopTrying = () => {
      settled = true;
      window.clearTimeout(timer);
    };
    const fail = () => {
      if (settled || disposed) return;
      stopTrying();
      setFailedUrl(url);
    };
    const reveal = () => {
      if (!disposed) video.dataset.playing = '';
    };
    const onPlaying = () => {
      if (settled || disposed) return;
      stopTrying();
      const onFrame = (video as VideoWithFrames).requestVideoFrameCallback;
      if (typeof onFrame === 'function') onFrame.call(video, reveal);
      else reveal();
    };
    const release = claimPlayback(post.key, () => {
      stopTrying();
      video.pause();
    });
    video.addEventListener('playing', onPlaying);
    video.addEventListener('error', fail);
    timer = window.setTimeout(fail, START_TIMEOUT_MS);
    // muted before play(), as a property and an attribute: what iOS checks to let it play inline on its own
    video.muted = true;
    video.defaultMuted = true;
    video.setAttribute('muted', '');
    video.src = url;
    const attempt = video.play();
    if (attempt) attempt.catch(fail);
    return () => {
      disposed = true;
      stopTrying();
      video.removeEventListener('playing', onPlaying);
      video.removeEventListener('error', fail);
      release();
      video.pause();
      delete video.dataset.playing;
      // let go of the clip and its decoder at once (iOS keeps them otherwise)
      video.removeAttribute('src');
      video.load();
    };
  }, [playing, post.key, post.previewUrl]);

  // the drift: one slow push-in on the poster, only after the preview gave up
  useEffect(() => {
    const poster = posterRef.current;
    if (!drifting || !poster) return undefined;
    const drift = poster.animate(DRIFT_FRAMES, { duration: DRIFT_MS, easing: DRIFT_EASE, fill: 'forwards' });
    return () => {
      if (!poster.isConnected) {
        drift.cancel();
        return;
      }
      const at = getComputedStyle(poster).transform;
      drift.cancel();
      if (at && at !== 'none') poster.animate([{ transform: at }, { transform: 'none' }], { duration: DRIFT_RELEASE_MS, easing: BEAT_EASE_CSS });
    };
  }, [drifting, posterRef]);

  return { videoRef, playing };
}

/* ── a carousel's slides over its poster ──────────────────────────────────── */

// a modal on screen right now (a closed one may still be in the page, at no size or fully faded)
function openDialog() {
  return Array.from(document.querySelectorAll<HTMLElement>('[role="dialog"][aria-modal="true"]')).some((dialog) => {
    const box = dialog.getBoundingClientRect();
    return box.width > 0 && box.height > 0 && getComputedStyle(dialog).opacity !== '0';
  });
}

const SLIDE_ARROW = 'pointer-events-auto absolute top-1/2 z-[3] grid h-10 w-10 -translate-y-1/2 place-items-center rounded-full bg-black/50 text-white opacity-0 shadow-[inset_0_0_0_1px_rgba(255,255,255,0.12)] outline-none transition-[opacity,transform] duration-200 ease-out hover:bg-black/65 focus-visible:opacity-100 active:scale-[0.92] group-hover/slides:opacity-100';

function SlideVideo({ post, slide, playing }: { post: StreamPost; slide: StreamSlide; playing: boolean }) {
  const ref = useRef<HTMLVideoElement | null>(null);
  useEffect(() => {
    const video = ref.current;
    if (!video || !playing) return undefined;
    const release = claimPlayback(`${post.key}:${slide.role}`, () => video.pause());
    video.muted = true;
    video.defaultMuted = true;
    const attempt = video.play();
    if (attempt) attempt.catch(() => undefined);
    return () => {
      release();
      video.pause();
    };
  }, [playing, post.key, slide.role]);
  return (
    <video
      ref={ref}
      src={mediaProxyUrl(post.key, slide.role)}
      className="absolute inset-0 block h-full w-full object-cover"
      muted
      playsInline
      loop
      preload="metadata"
      disablePictureInPicture
      disableRemotePlayback
      aria-hidden="true"
      tabIndex={-1}
    />
  );
}

function SlideImage({ post, slide, eager }: { post: StreamPost; slide: StreamSlide; eager: boolean }) {
  const [failed, setFailed] = useState(false);
  if (failed) return <PostCover handle={post.handle} />;
  return (
    // eslint-disable-next-line @next/next/no-img-element -- the stable media proxy, not a static asset
    <img
      ref={markIfLoaded}
      src={mediaProxyUrl(post.key, slide.role)}
      alt=""
      draggable={false}
      decoding="async"
      loading={eager ? 'eager' : 'lazy'}
      data-media-fallback="off"
      onLoad={markLoaded}
      onError={() => setFailed(true)}
      className={IMG}
    />
  );
}

function Slides({ post, controls }: { post: StreamPost; controls: 'top' | 'bottom' }) {
  const trackRef = useRef<HTMLDivElement | null>(null);
  const reduce = Boolean(useReducedMotion());
  const hidden = useSyncExternalStore(subscribeVisibility, readHidden, readHiddenOnServer);
  const slides = post.slides;
  const count = slides.length;
  const [at, setAt] = useState(0);

  // where the track rests: the slide nearest its scroll, read once a frame while it moves
  useEffect(() => {
    const track = trackRef.current;
    if (!track) return undefined;
    let frame = 0;
    const read = () => {
      frame = 0;
      const width = track.clientWidth || 1;
      setAt(Math.max(0, Math.min(count - 1, Math.round(track.scrollLeft / width))));
    };
    const onScroll = () => {
      if (!frame) frame = window.requestAnimationFrame(read);
    };
    track.addEventListener('scroll', onScroll, { passive: true });
    return () => {
      track.removeEventListener('scroll', onScroll);
      if (frame) window.cancelAnimationFrame(frame);
    };
  }, [count]);

  const go = useCallback((to: number) => {
    const track = trackRef.current;
    if (!track) return;
    const next = Math.max(0, Math.min(count - 1, to));
    track.scrollTo({ left: next * track.clientWidth, behavior: reduce ? 'auto' : 'smooth' });
  }, [count, reduce]);

  // the arrow keys flip slides while this carousel is the one in view
  useEffect(() => {
    const onKey = (event: KeyboardEvent) => {
      if (event.defaultPrevented || event.metaKey || event.ctrlKey || event.altKey) return;
      const target = event.target as HTMLElement | null;
      if (target && (target.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(target.tagName))) return;
      // a sheet open over the feed has the keys: a carousel behind it stays put (closed sheets stay in the page, unseen)
      if (!trackRef.current?.closest('[role="dialog"]') && openDialog()) return;
      if (event.key === 'ArrowRight') go(at + 1);
      else if (event.key === 'ArrowLeft') go(at - 1);
      else return;
      event.preventDefault();
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, [at, go]);

  const step = (delta: number) => (event: ReactMouseEvent<HTMLButtonElement>) => {
    event.stopPropagation();
    go(at + delta);
  };

  return (
    <span className="group/slides absolute inset-0 block">
      <div ref={trackRef} className="st-slides absolute inset-0 flex">
        {slides.map((slide, index) => (
          <div key={slide.role} className="st-slide relative h-full w-full shrink-0 overflow-hidden">
            {slide.video ? <SlideVideo post={post} slide={slide} playing={!hidden && index === at} /> : <SlideImage post={post} slide={slide} eager={index < 2} />}
          </div>
        ))}
      </div>
      {/* where you are: a dot per slide (the current one a short bar), and the count; they come up as the post comes to
          rest in the Day view, so they fade in (st-controls-in) */}
      <span aria-hidden="true" className={cn('st-controls-in pointer-events-none absolute inset-x-0 z-[2] flex justify-center gap-[5px]', controls === 'top' ? 'top-3' : 'bottom-3')}>
        {slides.map((slide, index) => (
          <span
            key={slide.role}
            className={cn('h-[6px] w-[6px] rounded-full bg-white shadow-[0_1px_3px_rgba(0,0,0,0.45)] transition-[transform,opacity] duration-300 ease-out', index === at ? 'scale-[1.35] opacity-100' : 'opacity-50')}
          />
        ))}
      </span>
      <span className={cn('st-controls-in pointer-events-none absolute right-3 z-[2] rounded-full bg-black/55 px-2 py-1 text-[11px] font-bold leading-none tabular-nums text-white', controls === 'top' ? 'top-3' : 'bottom-3')}>
        {at + 1}/{count}
      </span>
      <span className="pointer-events-none absolute inset-0 z-[3] hidden [@media(hover:hover)]:block">
        {at > 0 ? (
          <button type="button" aria-label="Previous slide" onClick={step(-1)} className={cn(SLIDE_ARROW, 'left-3')}>
            <ChevronLeft className="h-5 w-5" strokeWidth={2.4} aria-hidden="true" />
          </button>
        ) : null}
        {at < count - 1 ? (
          <button type="button" aria-label="Next slide" onClick={step(1)} className={cn(SLIDE_ARROW, 'right-3')}>
            <ChevronRight className="h-5 w-5" strokeWidth={2.4} aria-hidden="true" />
          </button>
        ) : null}
      </span>
    </span>
  );
}

/* ── the picture ──────────────────────────────────────────────────────────── */

export type PostPictureProps = {
  post: StreamPost;
  // the post in view (the Day view's, an open sheet's): its preview plays, its slides swipe
  active: boolean;
  // the first screen: fetch at high priority
  eager: boolean;
  // in a 4:5 frame: a picture 4:5 or wider shows whole (over the wash behind the frame), no tone under it
  framed: boolean;
  // where a carousel's dots sit
  controls?: 'top' | 'bottom';
  className?: string;
};

export const PostPicture = memo(function PostPicture({ post, active, eager, framed, controls = 'bottom', className }: PostPictureProps) {
  const posterRef = useRef<HTMLImageElement | null>(null);
  const picture = usePoster(post);
  const { videoRef, playing } = usePreview(post, active, posterRef);
  const setPoster = useCallback((img: HTMLImageElement | null) => {
    posterRef.current = img;
    markIfLoaded(img);
  }, []);
  const slides = active && post.slides.length > 1;
  return (
    <span data-picture="" className={cn('absolute inset-0 block overflow-hidden', framed && 'st-framed', className)}>
      {/* the feeder's tone under a picture still loading (a framed one shows the wash instead) */}
      {picture.src && !framed ? <PostCover handle={post.handle} bare /> : null}
      {picture.src ? (
        // eslint-disable-next-line @next/next/no-img-element -- the stable media proxy, not a static asset
        <img
          ref={setPoster}
          src={picture.src}
          alt=""
          draggable={false}
          decoding="async"
          // fetched as soon as its post is put up: the stream only puts up the posts within a screen or so of the one
          // in view, so this is the picture arriving before it scrolls on (lazy, Safari waited until it nearly had,
          // and pictures developed on screen as the page moved). The first screen's go first
          loading="eager"
          fetchPriority={eager ? 'high' : undefined}
          data-media-fallback="off"
          onLoad={markLoaded}
          onError={picture.onError}
          className={IMG}
        />
      ) : (
        <PostCover handle={post.handle} />
      )}
      {playing ? (
        <video ref={videoRef} className={VIDEO} muted playsInline loop preload="auto" disablePictureInPicture disableRemotePlayback aria-hidden="true" tabIndex={-1} />
      ) : null}
      {slides ? <Slides post={post} controls={controls} /> : null}
    </span>
  );
});

/* ── the sheet's media: the wash behind a 4:5 frame holding the picture ──── */

function PostMedia({ post, active = false, eager = false, controls = 'bottom', fit = 'cover', className }: PostMediaProps) {
  const framed = fit !== 'cover';
  return (
    <span data-post-media="" className={cn('relative block h-full w-full overflow-hidden rounded-[inherit] bg-[var(--st-surface)]', className)}>
      {framed ? <Wash src={post.thumbnailUrl} /> : null}
      <span className={cn('absolute block overflow-hidden', fit === 'portrait' ? 'inset-x-0 top-0 aspect-[4/5]' : 'inset-0')}>
        <PostPicture post={post} active={active} eager={eager} framed={framed} controls={controls} />
      </span>
    </span>
  );
}

export default memo(PostMedia);
