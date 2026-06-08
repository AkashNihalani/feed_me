'use client';

import { useState, useEffect, useRef, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Loader2, Eye, EyeOff, ExternalLink } from 'lucide-react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { cn } from '@/lib/utils';
import { getSupabase } from '@/lib/supabase';
import LiveDashboard from '@/components/login/LiveStats';
import { useLivePlatformStats } from '@/lib/useLiveStats';

type AuthMode = 'login' | 'signup' | 'forgot';

const APPLE_EASE = [0.32, 0.72, 0, 1] as const;

// One identity: white canvas, Feed Me red (#E11D48) punch. Palette is hardcoded
// (not theme tokens) so the login reads white even for users with theme=dark saved.
const INK = '#0B0B0F';
const RED = '#E11D48';

function safeInternalNextPath(value: string | null): string {
  if (!value || !value.startsWith('/') || value.startsWith('//')) {
    return '/';
  }

  try {
    const parsed = new URL(value, window.location.origin);
    return `${parsed.pathname}${parsed.search}${parsed.hash}`;
  } catch {
    return '/';
  }
}

/* ─────────────────────────────────────────────
   PAC-MAN — conveyor-belt logo motif

   Red Pac-Man on white, eating red-ink metric bubbles.
   Sits compact above the login card (desktop) / on top (mobile).
   ───────────────────────────────────────────── */

const METRIC_LABELS = ['Likes', 'Comments', 'Views'];

// Cycle duration — each bubble takes this long to travel one slot and get eaten
const CYCLE_DUR = 1.6;
// How many bubble slots visible to the right of the mouth
const VISIBLE_SLOTS = 4;

function PacManHero() {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const frameRef = useRef<number>(0);
  const t0Ref = useRef<number>(0);

  const getSizes = useCallback(() => {
    const vw = typeof window !== 'undefined' ? window.innerWidth : 1024;
    if (vw < 640) return { pacR: 34, bubbleR: 14, gap: 58, yCenter: 48 };
    if (vw < 1024) return { pacR: 46, bubbleR: 18, gap: 72, yCenter: 58 };
    // Desktop — compact; it sits above the login card now, not as the hero
    return { pacR: 52, bubbleR: 20, gap: 84, yCenter: 64 };
  }, []);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    t0Ref.current = performance.now();
    const dpr = window.devicePixelRatio || 1;

    const resize = () => {
      const rect = canvas.getBoundingClientRect();
      canvas.width = rect.width * dpr;
      canvas.height = rect.height * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
    };

    resize();
    window.addEventListener('resize', resize);

    const easeInOutCubic = (t: number) =>
      t < 0.5 ? 4 * t * t * t : 1 - Math.pow(-2 * t + 2, 3) / 2;
    const easeOutQuart = (t: number) => 1 - Math.pow(1 - t, 4);

    const tick = (now: number) => {
      const rect = canvas.getBoundingClientRect();
      const W = rect.width;
      ctx.clearRect(0, 0, W, rect.height);

      const { pacR, bubbleR, gap, yCenter } = getSizes();
      const elapsed = (now - t0Ref.current) / 1000;

      const pacCX = pacR + 12;
      const pacCY = yCenter;

      // The mouth tip: where the wedge opening meets the circle edge
      const mouthTipX = pacCX + pacR;

      const numMetrics = METRIC_LABELS.length;
      const cycleIndex = Math.floor(elapsed / CYCLE_DUR);
      const phase = (elapsed % CYCLE_DUR) / CYCLE_DUR;

      /* ── Phase timeline ──────────────────────────────
         0.00 → 0.45  Slide: bubbles glide left one full slot
         0.45 → 0.48  Dwell: bubble sits at mouth opening
         0.48 → 0.56  Snap shut: mouth closes, bubble vanishes at 0.52
         0.56 → 0.75  Reopen: mouth smoothly reopens
         0.75 → 1.00  Rest: clean gap before next cycle
         ─────────────────────────────────────────────── */

      const SLIDE_END   = 0.45;
      const SNAP_START  = 0.48;
      const SNAP_SHUT   = 0.56;
      const VANISH_AT   = 0.52; // bubble disappears exactly here
      const REOPEN_END  = 0.75;

      // ─── Slide progress ───
      let slideProgress = 0;
      if (phase < SLIDE_END) {
        slideProgress = easeInOutCubic(phase / SLIDE_END);
      } else {
        slideProgress = 1.0;
      }

      // ─── Mouth angle (degrees) ───
      let mouthAngle = 30;
      if (phase >= SNAP_START && phase < SNAP_SHUT) {
        const p = (phase - SNAP_START) / (SNAP_SHUT - SNAP_START);
        mouthAngle = 30 * (1 - p * p); // fast snap
      } else if (phase >= SNAP_SHUT && phase < REOPEN_END) {
        const p = (phase - SNAP_SHUT) / (REOPEN_END - SNAP_SHUT);
        mouthAngle = 30 * easeOutQuart(p); // gentle reopen
      } else if (phase < SNAP_START) {
        mouthAngle = 30; // held open, clean — no wobble
      }

      const mouthRad = (mouthAngle * Math.PI) / 180;

      // ─── Draw Pac-Man: clean red circle + wedge ───
      // Body — flat red circle with wedge cut
      ctx.fillStyle = RED;
      ctx.beginPath();
      ctx.moveTo(pacCX, pacCY);
      ctx.arc(pacCX, pacCY, pacR, mouthRad, Math.PI * 2 - mouthRad);
      ctx.closePath();
      ctx.fill();

      // ─── Draw Metric Bubbles ───
      // Slot 0 = the bubble being eaten this cycle (arrives at mouth when slide=1)
      // Slots 1, 2, 3... = queued to the right
      // Slot -1 = already eaten (should be invisible)

      for (let slot = -1; slot < VISIBLE_SLOTS + 2; slot++) {
        const iconIdx = ((cycleIndex + slot) % numMetrics + numMetrics) % numMetrics;

        // Position: when slideProgress=1, slot 0 lands exactly at mouthTipX
        const baseX = mouthTipX + (slot + 1) * gap - gap * slideProgress;

        // Cull off-screen
        if (baseX < pacCX - pacR) continue;
        if (baseX > W + gap) continue;

        // ─── Opacity & scale logic ───
        let opacity = 1.0;
        let scale = 1.0;

        const distToMouth = baseX - mouthTipX;

        // --- slot -1 is ALWAYS invisible (already eaten previous cycle) ---
        if (slot <= -1) {
          opacity = 0;
        }
        // Slot 0 is the one being eaten THIS cycle
        else if (slot === 0 && slideProgress >= 0.99) {
          // Bubble has arrived at mouth
          if (phase >= VANISH_AT) {
            // Gone — mouth has snapped over it
            opacity = 0;
          } else if (phase >= SNAP_START) {
            // Rapidly fade as mouth closes
            const p = (phase - SNAP_START) / (VANISH_AT - SNAP_START);
            opacity = Math.max(0, 1 - p);
            scale = 1 - p * 0.4;
          } else {
            // Sitting at mouth waiting to be eaten
            opacity = 1.0;
            scale = 0.95;
          }
        }
        // Any bubble that has moved past the mouth tip (distToMouth < 0) but isn't slot 0
        else if (distToMouth < -bubbleR * 0.3) {
          opacity = 0;
        }

        // Fade in from the right edge
        const rightFadeZone = W - gap;
        if (baseX > rightFadeZone) {
          opacity *= Math.max(0, 1 - (baseX - rightFadeZone) / gap);
        }

        if (opacity <= 0.01) continue;

        // No vertical bob — perfectly aligned conveyor belt
        const bx = baseX;
        const by = pacCY;

        ctx.save();
        ctx.globalAlpha = opacity;
        ctx.translate(bx, by);
        ctx.scale(scale, scale);

        // Bubble: soft red-tinted glass circle on white
        ctx.fillStyle = 'rgba(225, 29, 72, 0.06)';
        ctx.beginPath();
        ctx.arc(0, 0, bubbleR, 0, Math.PI * 2);
        ctx.fill();

        // Border
        ctx.strokeStyle = 'rgba(225, 29, 72, 0.22)';
        ctx.lineWidth = 1.2;
        ctx.beginPath();
        ctx.arc(0, 0, bubbleR, 0, Math.PI * 2);
        ctx.stroke();

        // Highlight — top-left specular
        ctx.strokeStyle = 'rgba(225, 29, 72, 0.12)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.arc(0, 0, bubbleR - 2, -Math.PI * 0.8, -Math.PI * 0.2);
        ctx.stroke();

        // Icon — bold red
        ctx.strokeStyle = 'rgba(225, 29, 72, 0.95)';
        ctx.fillStyle = 'rgba(225, 29, 72, 0.95)';
        ctx.lineWidth = 2.4;
        ctx.lineCap = 'round';
        ctx.lineJoin = 'round';
        const iconS = bubbleR * (iconIdx === 0 ? 0.82 : 0.72);
        drawMetricIcon(ctx, iconIdx, iconS);

        // Label — compact ink, well spaced
        ctx.fillStyle = `rgba(11, 11, 15, ${0.34 * opacity})`;
        ctx.font = `800 ${Math.max(6, bubbleR * 0.36)}px "Space Grotesk", sans-serif`;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';
        ctx.letterSpacing = '0.06em';
        ctx.fillText(METRIC_LABELS[iconIdx].toUpperCase(), 0, bubbleR + 6);

        ctx.restore();
      }

      // ─── Redraw Pac-Man on top to clip any bubble overlap ───
      ctx.fillStyle = RED;
      ctx.beginPath();
      ctx.moveTo(pacCX, pacCY);
      ctx.arc(pacCX, pacCY, pacR, mouthRad, Math.PI * 2 - mouthRad);
      ctx.closePath();
      ctx.fill();

      frameRef.current = requestAnimationFrame(tick);
    };

    frameRef.current = requestAnimationFrame(tick);

    return () => {
      cancelAnimationFrame(frameRef.current);
      window.removeEventListener('resize', resize);
    };
  }, [getSizes]);

  return (
    <div className="relative mb-1.5 w-full sm:mb-3 lg:mb-4">
      <canvas
        ref={canvasRef}
        className="h-[84px] w-full sm:h-[108px] lg:h-[136px]"
        style={{ imageRendering: 'auto' }}
      />
    </div>
  );
}

/* Canvas icon drawing — 3 metrics only: Heart, Comment, Users */
function drawMetricIcon(ctx: CanvasRenderingContext2D, idx: number, s: number) {
  ctx.beginPath();
  switch (idx) {
    case 0: // Heart — filled
      ctx.save();
      ctx.translate(0, s * 0.1);
      ctx.scale(1.18, 0.9);
      ctx.beginPath();
      ctx.moveTo(0, s * 0.48);
      ctx.bezierCurveTo(-s * 1.0, -s * 0.12, -s * 0.72, -s * 0.8, -s * 0.2, -s * 0.62);
      ctx.bezierCurveTo(-s * 0.06, -s * 0.57, 0, -s * 0.46, 0, -s * 0.34);
      ctx.bezierCurveTo(0, -s * 0.46, s * 0.06, -s * 0.57, s * 0.2, -s * 0.62);
      ctx.bezierCurveTo(s * 0.72, -s * 0.8, s * 1.0, -s * 0.12, 0, s * 0.48);
      ctx.closePath();
      ctx.fill();
      ctx.restore();
      break;
    case 1: // Comment bubble
      ctx.roundRect(-s * 0.65, -s * 0.5, s * 1.3, s * 0.85, s * 0.22);
      ctx.stroke();
      ctx.beginPath();
      ctx.moveTo(-s * 0.12, s * 0.35);
      ctx.lineTo(-s * 0.32, s * 0.7);
      ctx.lineTo(s * 0.18, s * 0.35);
      ctx.stroke();
      break;
    case 2: // Eye — Views
      ctx.ellipse(0, 0, s * 0.92, s * 0.55, 0, 0, Math.PI * 2);
      ctx.stroke();
      ctx.beginPath();
      ctx.arc(0, 0, s * 0.3, 0, Math.PI * 2);
      ctx.fill();
      break;
  }
}

/* ── Ambient Particles ── */
function particleSeed(index: number, salt: number) {
  const x = Math.sin(index * 47.23 + salt * 19.91) * 10000;
  return x - Math.floor(x);
}

function AmbientParticles() {
  const [particles] = useState(() =>
    Array.from({ length: 12 }, (_, i) => ({
      id: i,
      left: `${5 + particleSeed(i, 1) * 90}%`,
      top: `${5 + particleSeed(i, 2) * 90}%`,
      size: 1.5 + particleSeed(i, 3) * 2,
      opacity: 0.04 + particleSeed(i, 4) * 0.05,
      duration: 8 + particleSeed(i, 5) * 10,
      delay: particleSeed(i, 6) * 6,
    }))
  );

  return (
    <>
      {particles.map(p => (
        <div
          key={p.id}
          className="absolute rounded-full"
          style={{
            left: p.left,
            top: p.top,
            width: p.size,
            height: p.size,
            background: RED,
            opacity: p.opacity,
            animation: `loginFloat ${p.duration}s ease-in-out ${p.delay}s infinite`,
          }}
        />
      ))}
    </>
  );
}

/* ─────────────────────────────────────────────
   MAIN LOGIN PAGE
   ───────────────────────────────────────────── */
export default function LoginPage() {
  const router = useRouter();
  const [mode, setMode] = useState<AuthMode>('login');
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [isSuccess, setIsSuccess] = useState(false);
  const [showPassword, setShowPassword] = useState(false);
  const [focusedField, setFocusedField] = useState<string | null>(null);

  const liveStats = useLivePlatformStats();

  const getClient = () => getSupabase();

  useEffect(() => {
    const params = new URLSearchParams(window.location.search);
    if (params.get('message') === 'password_updated') {
      setMessage('Password updated. Sign in with your new credentials.');
      setMode('login');
    }
  }, []);

  const handleAuth = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);
    setError(null);
    setMessage(null);

    try {
      if (mode === 'login') {
        const res = await fetch('/api/auth/login', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ email, password }),
        });
        const body = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(body?.error || 'Login failed');

        setIsSuccess(true);
        setTimeout(() => {
          const nextPath = safeInternalNextPath(new URLSearchParams(window.location.search).get('next'));
          router.replace(nextPath);
          router.refresh();
        }, 1000);
      } else if (mode === 'signup') {
        const { error: signUpError } = await getClient().auth.signUp({
          email,
          password,
          options: { emailRedirectTo: `${location.origin}/auth/callback` },
        });
        if (signUpError) throw signUpError;
        setMessage('Account created! Please check your email to confirm.');
        setLoading(false);
      } else if (mode === 'forgot') {
        const { error: resetError } = await getClient().auth.resetPasswordForEmail(email, {
          redirectTo: `${location.origin}/auth/update-password`,
        });
        if (resetError) throw resetError;
        setMessage('Password reset link sent! Check your email.');
        setLoading(false);
      }
    } catch (err: unknown) {
      setError(err instanceof Error ? err.message : 'An error occurred');
      setLoading(false);
    }
  };

  const titles: Record<AuthMode, string> = {
    login: 'Welcome Back',
    signup: 'Get Started',
    forgot: 'Reset Access',
  };

  const subtitles: Record<AuthMode, string> = {
    login: 'Enter your credentials to continue',
    signup: 'Create your account to begin',
    forgot: 'We\'ll send you a reset link',
  };

  /* ── Shared sub-components used in both mobile & desktop layouts ── */

  const ambientBg = (
    <div className="pointer-events-none absolute inset-0 z-0">
      <div
        className="absolute -right-[12%] -top-[8%] h-[55vh] w-[55vh] rounded-full opacity-[0.03]"
        style={{
          background: 'radial-gradient(circle, #E11D48 0%, transparent 70%)',
          animation: 'meshDrift 18s ease-in-out infinite',
        }}
      />
      <div
        className="absolute -bottom-[12%] -left-[8%] h-[48vh] w-[48vh] rounded-full opacity-[0.025]"
        style={{
          background: 'radial-gradient(circle, #E11D48 0%, transparent 70%)',
          animation: 'meshDrift 22s ease-in-out infinite reverse',
        }}
      />
      <div
        className="absolute left-1/2 top-[18%] h-[320px] w-[320px] -translate-x-1/2 rounded-full border border-[#E11D48]/[0.05] opacity-40 sm:h-[440px] sm:w-[440px] lg:h-[520px] lg:w-[520px]"
        style={{
          background: 'radial-gradient(circle, transparent 55%, rgba(225,29,72,0.02) 100%)',
          animation: 'loginFloat 14s ease-in-out infinite',
        }}
      />
      <AmbientParticles />
      <div
        className="absolute inset-0 opacity-[0.02]"
        style={{
          backgroundImage:
            'url("data:image/svg+xml,%3Csvg viewBox=\'0 0 256 256\' xmlns=\'http://www.w3.org/2000/svg\'%3E%3Cfilter id=\'n\'%3E%3CfeTurbulence type=\'fractalNoise\' baseFrequency=\'0.9\' numOctaves=\'4\' stitchTiles=\'stitch\'/%3E%3C/filter%3E%3Crect width=\'100%25\' height=\'100%25\' filter=\'url(%23n)\' opacity=\'0.5\'/%3E%3C/svg%3E")',
        }}
      />
    </div>
  );

  const brandMark = (isLarge: boolean, align: 'center' | 'left' = 'center') => (
    <div className={cn(
      align === 'left' ? 'text-left' : 'text-center',
      isLarge ? 'mb-6' : 'mb-4 sm:mb-6',
    )}>
      <div className={cn('inline-flex items-center', isLarge ? 'gap-4 xl:gap-5' : 'gap-3 sm:gap-3.5')}>
        {/* eslint-disable-next-line @next/next/no-img-element -- static brand logo from /public */}
        <img
          src="/icon.svg"
          alt="Feed Me"
          draggable={false}
          className={cn(
            'shrink-0 select-none',
            isLarge ? 'h-[84px] w-[84px] xl:h-[104px] xl:w-[104px]' : 'h-[60px] w-[60px] sm:h-[76px] sm:w-[76px]',
          )}
        />
        <span className="inline-flex items-baseline gap-0.5">
          <span className={cn(
            'font-black tracking-[-0.04em]',
            isLarge ? 'text-[42px] xl:text-[50px]' : 'text-[32px] sm:text-[44px]',
          )} style={{ color: INK }}>
            FEED
          </span>
          <span
            className={cn(
              'font-black tracking-[-0.04em]',
              isLarge ? 'text-[42px] xl:text-[50px]' : 'text-[32px] sm:text-[44px]',
            )}
            style={{ color: RED }}
          >
            ME
          </span>
        </span>
      </div>
      {isLarge && (
        <p className="mt-3 text-[11px] font-bold uppercase tracking-[0.22em]" style={{ color: 'rgba(11,11,15,0.4)' }}>
          Instagram Analytics &middot; Reimagined
        </p>
      )}
    </div>
  );

  const footerLinks = (align: 'center' | 'left' = 'center') => (
    <div className={cn('flex items-center gap-4', align === 'left' ? 'justify-start' : 'justify-center')}>
      <a href="#" className="text-[9px] font-black uppercase tracking-[0.14em] text-[#0B0B0F]/35 transition-colors hover:text-[#0B0B0F]/65">Privacy</a>
      <span className="text-[#0B0B0F]/15">&middot;</span>
      <a href="#" className="text-[9px] font-black uppercase tracking-[0.14em] text-[#0B0B0F]/35 transition-colors hover:text-[#0B0B0F]/65">Terms</a>
      <span className="text-[#0B0B0F]/15">&middot;</span>
      <Link href="/" className="text-[9px] font-black uppercase tracking-[0.14em] text-[#0B0B0F]/35 transition-colors hover:text-[#E11D48]/70">Home &rarr;</Link>
    </div>
  );

  const keyframes = (
    <style dangerouslySetInnerHTML={{ __html: `
      @keyframes shimmer-sweep {
        0% { transform: translate(-50%, -50%) rotate(0deg); }
        100% { transform: translate(-50%, -50%) rotate(360deg); }
      }
      @keyframes cta-breathe {
        0%, 100% { box-shadow: 0 8px 18px -8px rgba(15,23,42,0.35); }
        50% { box-shadow: 0 12px 24px -8px rgba(15,23,42,0.45); }
      }
      @keyframes loginFloat {
        0%, 100% { transform: translateY(0); }
        50% { transform: translateY(-8px); }
      }
    `}} />
  );

  return (
    <div
      data-login-shell
      className="relative flex h-[100dvh] min-h-[100svh] w-full overflow-x-hidden overflow-y-auto bg-white selection:bg-[#E11D48]/25 selection:text-[#E11D48] lg:h-[100vh] lg:min-h-0 lg:overflow-hidden"
      style={{
        paddingTop: 'env(safe-area-inset-top, 0px)',
        paddingBottom: 'env(safe-area-inset-bottom, 0px)',
        WebkitOverflowScrolling: 'touch',
      }}
    >
      {ambientBg}
      {keyframes}

      {/* ── Top nav ── */}
      <div className="absolute right-4 top-4 z-20 sm:right-6 sm:top-6">
        <Link
          href="/"
          className="group flex items-center gap-1.5 rounded-full border border-[#0B0B0F]/[0.08] bg-white/60 px-4 py-2 text-[9px] font-black uppercase tracking-[0.18em] text-[#0B0B0F]/35 transition-colors hover:border-[#E11D48]/25 hover:text-[#E11D48]/70"
        >
          feedme.app
          <ExternalLink size={10} strokeWidth={3} className="opacity-50 transition-transform group-hover:translate-x-0.5" />
        </Link>
      </div>

      {/* ════════════════════════════════════════════
          DESKTOP: Two-panel split layout (lg+)
          Left  = Live dashboard hero (55%)
          Right = Pac-Man + login form card (45%)
         ════════════════════════════════════════════ */}

      {/* ── Left hero panel (desktop only): brand + live dashboard ── */}
      <div className="hidden lg:flex lg:w-[56%] relative z-10 flex-col items-start justify-center px-16 xl:px-24">
        <motion.div
          initial={{ opacity: 0, x: -30 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.9, ease: APPLE_EASE, delay: 0.15 }}
          className="flex w-full max-w-[920px] flex-col items-start"
        >
          {brandMark(true, 'left')}
          <LiveDashboard state={liveStats} className="mt-2" />
          <div className="mt-10">{footerLinks('left')}</div>
        </motion.div>
      </div>

      {/* ── Right panel (desktop): Pac-Man + form / Full column (mobile) ── */}
      <div className="relative z-10 flex min-h-[100svh] w-full flex-col items-center justify-start px-5 pb-12 pt-7 sm:py-10 lg:min-h-0 lg:w-[44%] lg:justify-center lg:py-0 lg:px-10 xl:px-16">

        {/* Mobile only: Pac-Man + brand stacked above the card */}
        <div className="lg:hidden w-full max-w-[460px] flex flex-col items-center">
          <PacManHero />
          {brandMark(false)}
        </div>

        {/* Desktop only: compact Pac-Man above the card */}
        <div className="hidden lg:block w-full max-w-[520px]">
          <PacManHero />
        </div>

        {/* ── Form card wrapper ── */}
        <div className="w-full max-w-[460px] lg:max-w-[520px]">
          <motion.div
            initial={{ opacity: 0, y: 32, scale: 0.94 }}
            animate={isSuccess
              ? { opacity: 0, scale: 1.04, y: -20, filter: 'blur(10px)' }
              : { opacity: 1, y: 0, scale: 1, filter: 'blur(0px)' }
            }
            transition={{ duration: isSuccess ? 0.5 : 0.8, ease: APPLE_EASE }}
            className="relative w-full overflow-hidden rounded-[24px] sm:rounded-[28px] lg:rounded-[32px]"
            style={{
              background: 'linear-gradient(180deg, rgba(255,255,255,0.98) 0%, rgba(255,255,255,0.94) 100%)',
              border: '1px solid rgba(14,19,28,0.08)',
              borderTopColor: 'rgba(255,255,255,0.96)',
              boxShadow: [
                '0 1px 0 rgba(255,255,255,0.9) inset',
                '0 1px 2px rgba(15,23,42,0.04)',
                '0 8px 16px rgba(15,23,42,0.06)',
                '0 24px 48px -8px rgba(15,23,42,0.12)',
                '0 48px 96px -16px rgba(15,23,42,0.16)',
              ].join(', '),
            }}
          >
            {/* Liquid glass — top sheen + faint red catch */}
            <div
              className="pointer-events-none absolute inset-0 z-0 rounded-[inherit]"
              style={{
                background: [
                  'radial-gradient(ellipse 55% 30% at 12% 0%, rgba(225,29,72,0.05) 0%, transparent 70%)',
                  'linear-gradient(180deg, rgba(255,255,255,0.55) 0%, transparent 18%)',
                ].join(', '),
              }}
            />

            {/* Inner depth ring */}
            <div
              className="pointer-events-none absolute inset-[1px] z-0 rounded-[23px] sm:rounded-[27px] lg:rounded-[31px]"
              style={{
                border: '1px solid rgba(15,23,42,0.04)',
                borderTopColor: 'rgba(255,255,255,0.9)',
              }}
            />

            <div className="relative z-10 px-7 py-7 sm:px-9 sm:py-9 lg:px-11 lg:py-14">

            {/* ── Tab switcher — light inset well ── */}
            <div
              className="mx-auto mb-7 flex max-w-[280px] items-center gap-1 rounded-[14px] p-1.5 sm:mb-8 lg:mb-9"
              style={{
                background: 'rgba(15,23,42,0.05)',
                border: '1px solid rgba(15,23,42,0.06)',
                boxShadow: [
                  'inset 0 2px 6px rgba(15,23,42,0.08)',
                  'inset 0 1px 1px rgba(15,23,42,0.05)',
                  '0 1px 0 rgba(255,255,255,0.85)',
                ].join(', '),
              }}
            >
              {(['login', 'signup'] as AuthMode[]).map((tab) => {
                const isActive = mode === tab || (mode === 'forgot' && tab === 'login');
                return (
                  <button
                    key={tab}
                    type="button"
                    onClick={() => { setMode(tab); setError(null); setMessage(null); }}
                    className={cn(
                      'relative flex-1 rounded-[10px] py-3 text-center text-[10px] font-black uppercase tracking-[0.16em] transition-colors duration-400',
                      isActive ? 'text-[#E11D48] z-10' : 'text-[#0B0B0F]/35 z-0 hover:text-[#0B0B0F]/60',
                    )}
                  >
                    {isActive && (
                      <motion.div
                        layoutId="login-tab-pill"
                        className="absolute inset-0 rounded-[10px]"
                        style={{
                          background: 'linear-gradient(180deg, #ffffff 0%, #f4f6f8 100%)',
                          border: '1px solid rgba(15,23,42,0.08)',
                          borderTopColor: 'rgba(255,255,255,1)',
                          boxShadow: [
                            'inset 0 1px 0 rgba(255,255,255,1)',
                            '0 2px 6px rgba(15,23,42,0.10)',
                            '0 1px 2px rgba(15,23,42,0.06)',
                          ].join(', '),
                        }}
                        transition={{ type: 'spring', stiffness: 380, damping: 32, mass: 0.8 }}
                      />
                    )}
                    <span className="relative z-10">{tab === 'login' ? 'Login' : 'Sign Up'}</span>
                  </button>
                );
              })}
            </div>

            {/* ── Title & subtitle — glass layer z-shift ── */}
            <div className="relative mb-6 lg:mb-7" style={{ minHeight: 56, perspective: '600px' }}>
              <AnimatePresence mode="wait">
                <motion.div
                  key={mode}
                  initial={{ opacity: 0, y: 18, scale: 0.95, rotateX: 8, filter: 'blur(6px)' }}
                  animate={{ opacity: 1, y: 0, scale: 1, rotateX: 0, filter: 'blur(0px)' }}
                  exit={{ opacity: 0, y: -14, scale: 0.96, rotateX: -6, filter: 'blur(4px)' }}
                  transition={{ duration: 0.45, ease: APPLE_EASE }}
                  className="text-center"
                  style={{ transformOrigin: 'center bottom' }}
                >
                  <h1
                    className="text-[24px] font-black tracking-[-0.025em] sm:text-[28px] lg:text-[34px]"
                    style={{ color: INK }}
                  >
                    {titles[mode]}
                  </h1>
                  <p className="mt-2 text-[11px] font-semibold tracking-wide lg:text-[12px]" style={{ color: 'rgba(11,11,15,0.45)' }}>
                    {subtitles[mode]}
                  </p>
                </motion.div>
              </AnimatePresence>
            </div>

            {/* ── Form ── */}
            <form onSubmit={handleAuth} className="space-y-5" style={{ perspective: '800px' }}>
              <AnimatePresence mode="wait">
                <motion.div
                  key={mode}
                  initial={{ opacity: 0, y: 20, scale: 0.96, rotateX: 6, filter: 'blur(4px)' }}
                  animate={{ opacity: 1, y: 0, scale: 1, rotateX: 0, filter: 'blur(0px)' }}
                  exit={{ opacity: 0, y: -16, scale: 0.96, rotateX: -5, filter: 'blur(3px)' }}
                  transition={{ duration: 0.45, ease: APPLE_EASE, delay: 0.04 }}
                  className="space-y-5"
                  style={{ transformOrigin: 'center top' }}
                >
                  {/* Email */}
                  <div>
                    <label className="mb-2.5 block text-[9px] font-black uppercase tracking-[0.22em]" style={{ color: 'rgba(11,11,15,0.45)' }}>
                      Email Address
                    </label>
                    <motion.div
                      animate={{
                        y: focusedField === 'email' ? -2 : 0,
                        boxShadow: focusedField === 'email'
                          ? 'inset 0 1px 3px rgba(15,23,42,0.05), 0 1px 0 rgba(255,255,255,0.9), 0 10px 26px -6px rgba(225,29,72,0.26)'
                          : 'inset 0 2px 5px rgba(15,23,42,0.06), 0 1px 0 rgba(255,255,255,0.8)',
                      }}
                      transition={{ duration: 0.5, ease: APPLE_EASE }}
                      className="relative overflow-hidden rounded-[14px]"
                      style={{
                        background: 'rgba(16,24,40,0.035)',
                        border: '1px solid rgba(15,23,42,0.07)',
                        boxShadow: 'inset 0 2px 5px rgba(15,23,42,0.06), 0 1px 0 rgba(255,255,255,0.8)',
                      }}
                    >
                      {/* Shimmer border — slow, smooth red sweep on focus */}
                      <motion.div
                        initial={false}
                        animate={{ opacity: focusedField === 'email' ? 1 : 0 }}
                        transition={{ duration: 0.6, ease: 'easeInOut' }}
                        className="pointer-events-none absolute inset-0 z-0"
                      >
                        <div
                          className="absolute left-1/2 top-1/2 h-[500px] w-[500px]"
                          style={{
                            animation: 'shimmer-sweep 3.5s linear infinite',
                            background: 'conic-gradient(from 180deg, transparent 0deg, transparent 218deg, rgba(225,29,72,0.22) 250deg, rgba(225,29,72,0.88) 285deg, rgba(225,29,72,0.34) 322deg, transparent 352deg)',
                          }}
                        />
                      </motion.div>

                      {/* Inner mask — reveals only the border glow */}
                      <div
                        className="pointer-events-none absolute inset-[1.5px] z-[1] rounded-[12.5px] transition-colors duration-500"
                        style={{ background: focusedField === 'email' ? 'rgba(255,255,255,1)' : 'rgba(247,248,250,1)' }}
                      />

                      <input
                        type="email"
                        value={email}
                        onChange={(e) => setEmail(e.target.value)}
                        onFocus={() => setFocusedField('email')}
                        onBlur={() => setFocusedField(null)}
                        required
                        placeholder="you@example.com"
                        autoComplete="email"
                        className="relative z-10 h-[54px] lg:h-[58px] w-full rounded-[14px] bg-transparent px-5 text-[16px] font-semibold outline-none placeholder:text-[#0B0B0F]/25 focus:ring-0"
                        style={{ color: INK, caretColor: RED }}
                      />
                    </motion.div>
                  </div>

                  {/* Password */}
                  {mode !== 'forgot' && (
                    <div>
                      <div className="mb-2.5 flex items-center justify-between">
                        <label className="text-[9px] font-black uppercase tracking-[0.22em]" style={{ color: 'rgba(11,11,15,0.45)' }}>
                          Password
                        </label>
                        {mode === 'login' && (
                          <button
                            type="button"
                            onClick={() => { setMode('forgot'); setError(null); setMessage(null); }}
                            className="text-[9px] font-black uppercase tracking-[0.12em] text-[#E11D48]/60 transition-colors hover:text-[#E11D48]"
                          >
                            Forgot?
                          </button>
                        )}
                      </div>
                      <motion.div
                        animate={{
                          y: focusedField === 'password' ? -2 : 0,
                          boxShadow: focusedField === 'password'
                            ? 'inset 0 1px 3px rgba(15,23,42,0.05), 0 1px 0 rgba(255,255,255,0.9), 0 10px 26px -6px rgba(225,29,72,0.26)'
                            : 'inset 0 2px 5px rgba(15,23,42,0.06), 0 1px 0 rgba(255,255,255,0.8)',
                        }}
                        transition={{ duration: 0.5, ease: APPLE_EASE }}
                        className="relative overflow-hidden rounded-[14px]"
                        style={{
                          background: 'rgba(16,24,40,0.035)',
                          border: '1px solid rgba(15,23,42,0.07)',
                          boxShadow: 'inset 0 2px 5px rgba(15,23,42,0.06), 0 1px 0 rgba(255,255,255,0.8)',
                        }}
                      >
                        {/* Shimmer border */}
                        <motion.div
                          initial={false}
                          animate={{ opacity: focusedField === 'password' ? 1 : 0 }}
                          transition={{ duration: 0.6, ease: 'easeInOut' }}
                          className="pointer-events-none absolute inset-0 z-0"
                        >
                          <div
                            className="absolute left-1/2 top-1/2 h-[500px] w-[500px]"
                            style={{
                              animation: 'shimmer-sweep 3.5s linear infinite',
                              background: 'conic-gradient(from 180deg, transparent 0deg, transparent 218deg, rgba(225,29,72,0.22) 250deg, rgba(225,29,72,0.88) 285deg, rgba(225,29,72,0.34) 322deg, transparent 352deg)',
                            }}
                          />
                        </motion.div>

                        {/* Inner mask */}
                        <div
                          className="pointer-events-none absolute inset-[1.5px] z-[1] rounded-[12.5px] transition-colors duration-500"
                          style={{ background: focusedField === 'password' ? 'rgba(255,255,255,1)' : 'rgba(247,248,250,1)' }}
                        />

                        <input
                          type={showPassword ? 'text' : 'password'}
                          value={password}
                          onChange={(e) => setPassword(e.target.value)}
                          onFocus={() => setFocusedField('password')}
                          onBlur={() => setFocusedField(null)}
                          required
                          placeholder="••••••••"
                          autoComplete={mode === 'login' ? 'current-password' : 'new-password'}
                          className="relative z-10 h-[54px] lg:h-[58px] w-full rounded-[14px] bg-transparent px-5 pr-12 text-[16px] font-semibold outline-none placeholder:text-[#0B0B0F]/25 focus:ring-0"
                          style={{ color: INK, caretColor: RED }}
                        />
                        <button
                          type="button"
                          onClick={() => setShowPassword(!showPassword)}
                          className="absolute right-2 top-1/2 z-10 -translate-y-1/2 rounded-[10px] p-2.5 text-[#0B0B0F]/25 transition-all hover:bg-[#0B0B0F]/[0.04] hover:text-[#0B0B0F]/55"
                        >
                          {showPassword ? <EyeOff size={16} strokeWidth={2.5} /> : <Eye size={16} strokeWidth={2.5} />}
                        </button>
                      </motion.div>
                    </div>
                  )}
                </motion.div>
              </AnimatePresence>

              {/* Error */}
              <AnimatePresence>
                {error && (
                  <motion.div
                    initial={{ opacity: 0, height: 0 }}
                    animate={{ opacity: 1, height: 'auto' }}
                    exit={{ opacity: 0, height: 0 }}
                    transition={{ duration: 0.3, ease: APPLE_EASE }}
                    className="overflow-hidden"
                  >
                    <div
                      className="rounded-[12px] px-4 py-3 text-[11px] font-bold text-[#b91c1c]"
                      style={{
                        background: 'rgba(225,29,72,0.06)',
                        border: '1px solid rgba(225,29,72,0.16)',
                        boxShadow: 'inset 0 1px 2px rgba(225,29,72,0.06)',
                      }}
                    >
                      {error}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>

              {/* Success message */}
              <AnimatePresence>
                {message && (
                  <motion.div
                    initial={{ opacity: 0, height: 0 }}
                    animate={{ opacity: 1, height: 'auto' }}
                    exit={{ opacity: 0, height: 0 }}
                    transition={{ duration: 0.3, ease: APPLE_EASE }}
                    className="overflow-hidden"
                  >
                    <div
                      className="rounded-[12px] px-4 py-3 text-[11px] font-bold text-[#E11D48]"
                      style={{
                        background: 'rgba(225,29,72,0.05)',
                        border: '1px solid rgba(225,29,72,0.14)',
                        boxShadow: 'inset 0 1px 2px rgba(225,29,72,0.05)',
                      }}
                    >
                      {message}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>

              {/* ── White-on-red CTA with periodic breathe glow ── */}
              <motion.button
                type="submit"
                disabled={loading || isSuccess}
                whileTap={{ scale: 0.97 }}
                layout
                className={cn(
                  'group relative mt-4 flex h-[56px] lg:h-[62px] w-full cursor-pointer items-center justify-center overflow-hidden rounded-[14px] lg:rounded-[16px]',
                  'text-[12px] lg:text-[13px] font-black uppercase tracking-[0.2em] text-white',
                  'disabled:opacity-40 disabled:cursor-not-allowed'
                )}
                style={{
                  background: RED,
                  animation: loading || isSuccess ? 'none' : 'cta-breathe 4s ease-in-out infinite',
                }}
                transition={{ layout: { duration: 0.4, ease: APPLE_EASE } }}
              >
                {/* Top highlight bevel */}
                <div className="pointer-events-none absolute inset-0 z-0 rounded-[14px]"
                  style={{ background: 'linear-gradient(180deg, rgba(255,255,255,0.22) 0%, transparent 45%, rgba(0,0,0,0.08) 100%)' }}
                />

                <AnimatePresence mode="wait">
                  <motion.span
                    key={loading ? 'loading' : mode}
                    initial={{ opacity: 0, y: 10, filter: 'blur(3px)' }}
                    animate={{ opacity: 1, y: 0, filter: 'blur(0px)' }}
                    exit={{ opacity: 0, y: -10, filter: 'blur(3px)' }}
                    transition={{ duration: 0.28, ease: APPLE_EASE }}
                    className="relative z-10 flex items-center gap-2"
                  >
                    {loading ? (
                      <Loader2 className="h-4.5 w-4.5 animate-spin" />
                    ) : (
                      <>
                        {mode === 'login' && 'Login'}
                        {mode === 'signup' && 'Create Account'}
                        {mode === 'forgot' && 'Send Reset Link'}
                      </>
                    )}
                  </motion.span>
                </AnimatePresence>
              </motion.button>
            </form>

            {/* Back link — only for forgot mode */}
            <AnimatePresence>
              {mode === 'forgot' && (
                <motion.div
                  initial={{ opacity: 0, y: 6 }}
                  animate={{ opacity: 1, y: 0 }}
                  exit={{ opacity: 0, y: -4 }}
                  transition={{ duration: 0.3, ease: APPLE_EASE }}
                  className="mt-6 text-center"
                >
                  <button
                    type="button"
                    onClick={() => { setMode('login'); setError(null); setMessage(null); }}
                    className="text-[10px] font-black uppercase tracking-[0.12em] text-[#0B0B0F]/30 transition-colors hover:text-[#E11D48]/70"
                  >
                    &larr; Back to Login
                  </button>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        </motion.div>
        </div>{/* end form card wrapper */}

        {/* Mobile only: live dashboard below the whole box */}
        <div className="mt-7 w-full max-w-[460px] lg:hidden">
          <LiveDashboard state={liveStats} />
        </div>

        {/* Mobile only: footer */}
        <div className="lg:hidden mt-8 w-full max-w-[460px]">
          {footerLinks('center')}
        </div>
      </div>{/* end right panel */}

      {/* ── Success pulse ring ── */}
      <AnimatePresence>
        {isSuccess && (
          <motion.div
            initial={{ scale: 0.3, opacity: 0.6 }}
            animate={{ scale: 5, opacity: 0 }}
            transition={{ duration: 1.2, ease: 'easeOut' }}
            className="pointer-events-none absolute z-20 h-32 w-32 rounded-full border border-[#E11D48]/40"
          />
        )}
      </AnimatePresence>

      {/* Autofill override — keep the field white with ink text */}
      <style jsx global>{`
        input:-webkit-autofill,
        input:-webkit-autofill:hover,
        input:-webkit-autofill:focus,
        input:-webkit-autofill:active {
          -webkit-box-shadow: 0 0 0 30px rgba(255,255,255,1) inset !important;
          -webkit-text-fill-color: #0B0B0F !important;
          caret-color: #E11D48 !important;
          transition: background-color 5000s ease-in-out 0s;
        }
      `}</style>
    </div>
  );
}
