'use client';

import { useState, useEffect, useRef, useCallback } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { Loader2, Eye, EyeOff, ExternalLink } from 'lucide-react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { cn } from '@/lib/utils';
import { getSupabase } from '@/lib/supabase';

type AuthMode = 'login' | 'signup' | 'forgot';

const APPLE_EASE = [0.32, 0.72, 0, 1] as const;

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
   PAC-MAN HERO — Premium conveyor-belt animation

   Only 3 metrics: Likes, Comments, Followers
   Circle with wedge — no eye, clean geometric shape
   Monotone white bubbles, prominent icons
   Previous bubble fully gone before next slides in
   ───────────────────────────────────────────── */

const METRIC_LABELS = ['Likes', 'Comments', 'Followers'];

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
    // Desktop — larger for the hero panel
    return { pacR: 72, bubbleR: 28, gap: 108, yCenter: 90 };
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

      // ─── Draw Pac-Man: clean circle + wedge ───

      // Minimal glow — barely visible ambient
      const glowGrad = ctx.createRadialGradient(pacCX, pacCY, pacR * 0.8, pacCX, pacCY, pacR * 1.3);
      glowGrad.addColorStop(0, 'rgba(225,29,72, 0.04)');
      glowGrad.addColorStop(1, 'rgba(225,29,72, 0)');
      ctx.fillStyle = glowGrad;
      ctx.beginPath();
      ctx.arc(pacCX, pacCY, pacR * 1.3, 0, Math.PI * 2);
      ctx.fill();

      // Body — flat lime circle with wedge cut
      ctx.fillStyle = '#E11D48';
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

        // --- THE FIX: slot -1 is ALWAYS invisible (already eaten previous cycle) ---
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
            // Sitting at mouth waiting to be eaten — slight scale down to show "about to be consumed"
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

        // Bubble: frosted glass circle — monotone white
        ctx.fillStyle = 'rgba(255, 255, 255, 0.07)';
        ctx.beginPath();
        ctx.arc(0, 0, bubbleR, 0, Math.PI * 2);
        ctx.fill();

        // Border
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.18)';
        ctx.lineWidth = 1.2;
        ctx.beginPath();
        ctx.arc(0, 0, bubbleR, 0, Math.PI * 2);
        ctx.stroke();

        // Highlight — top-left specular
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.10)';
        ctx.lineWidth = 1;
        ctx.beginPath();
        ctx.arc(0, 0, bubbleR - 2, -Math.PI * 0.8, -Math.PI * 0.2);
        ctx.stroke();

        // Icon — large, bold, prominent white
        ctx.strokeStyle = 'rgba(255, 255, 255, 0.92)';
        ctx.fillStyle = 'rgba(255, 255, 255, 0.92)';
        ctx.lineWidth = 2.4;
        ctx.lineCap = 'round';
        ctx.lineJoin = 'round';
        const iconS = bubbleR * 0.55;
        drawMetricIcon(ctx, iconIdx, iconS);

        // Label — compact, well spaced
        ctx.fillStyle = `rgba(255, 255, 255, ${0.3 * opacity})`;
        ctx.font = `800 ${Math.max(6, bubbleR * 0.36)}px "Space Grotesk", sans-serif`;
        ctx.textAlign = 'center';
        ctx.textBaseline = 'top';
        ctx.letterSpacing = '0.06em';
        ctx.fillText(METRIC_LABELS[iconIdx].toUpperCase(), 0, bubbleR + 6);

        ctx.restore();
      }

      // ─── Redraw Pac-Man on top to clip any bubble overlap ───
      ctx.fillStyle = '#E11D48';
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
    <div className="relative w-full mb-2 sm:mb-4 lg:mb-8">
      <canvas
        ref={canvasRef}
        className="w-full h-[100px] sm:h-[124px] lg:h-[190px]"
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
      ctx.moveTo(0, s * 0.35);
      ctx.bezierCurveTo(-s * 0.9, -s * 0.4, -s * 0.1, -s * 0.95, 0, -s * 0.35);
      ctx.bezierCurveTo(s * 0.1, -s * 0.95, s * 0.9, -s * 0.4, 0, s * 0.35);
      ctx.closePath();
      ctx.fill();
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
    case 2: // Users / Followers
      // Head
      ctx.arc(0, -s * 0.25, s * 0.28, 0, Math.PI * 2);
      ctx.stroke();
      // Body arc
      ctx.beginPath();
      ctx.arc(0, s * 0.75, s * 0.5, -Math.PI * 0.85, -Math.PI * 0.15);
      ctx.stroke();
      break;
  }
}

/* ── Ambient Particles ── */
function AmbientParticles() {
  const [particles] = useState(() =>
    Array.from({ length: 12 }, (_, i) => ({
      id: i,
      left: `${5 + Math.random() * 90}%`,
      top: `${5 + Math.random() * 90}%`,
      size: 1.5 + Math.random() * 2,
      opacity: 0.05 + Math.random() * 0.05,
      duration: 8 + Math.random() * 10,
      delay: Math.random() * 6,
    }))
  );

  return (
    <>
      {particles.map(p => (
        <div
          key={p.id}
          className="absolute rounded-full bg-[#E11D48]"
          style={{
            left: p.left,
            top: p.top,
            width: p.size,
            height: p.size,
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
        className="absolute -right-[12%] -top-[8%] h-[55vh] w-[55vh] rounded-full opacity-[0.05]"
        style={{
          background: 'radial-gradient(circle, #E11D48 0%, transparent 70%)',
          animation: 'meshDrift 18s ease-in-out infinite',
        }}
      />
      <div
        className="absolute -bottom-[12%] -left-[8%] h-[45vh] w-[45vh] rounded-full opacity-[0.03]"
        style={{
          background: 'radial-gradient(circle, #E11D48 0%, transparent 70%)',
          animation: 'meshDrift 22s ease-in-out infinite reverse',
        }}
      />
      <div
        className="absolute left-1/2 top-[18%] h-[320px] w-[320px] -translate-x-1/2 rounded-full border border-white/[0.02] opacity-30 sm:h-[440px] sm:w-[440px] lg:h-[520px] lg:w-[520px]"
        style={{
          background: 'radial-gradient(circle, transparent 55%, rgba(225,29,72,0.015) 100%)',
          animation: 'loginFloat 14s ease-in-out infinite',
        }}
      />
      <AmbientParticles />
      <div
        className="absolute inset-0 opacity-[0.018]"
        style={{
          backgroundImage:
            'url("data:image/svg+xml,%3Csvg viewBox=\'0 0 256 256\' xmlns=\'http://www.w3.org/2000/svg\'%3E%3Cfilter id=\'n\'%3E%3CfeTurbulence type=\'fractalNoise\' baseFrequency=\'0.9\' numOctaves=\'4\' stitchTiles=\'stitch\'/%3E%3C/filter%3E%3Crect width=\'100%25\' height=\'100%25\' filter=\'url(%23n)\' opacity=\'0.5\'/%3E%3C/svg%3E")',
        }}
      />
    </div>
  );

  const brandMark = (isLarge: boolean) => (
    <div className={isLarge ? 'mb-4 text-center lg:mb-6' : 'mb-6 text-center sm:mb-8'}>
      <div className="inline-flex items-baseline gap-0.5">
        <span className={cn(
          'font-black tracking-[-0.04em] text-white',
          isLarge ? 'text-[64px] xl:text-[76px]' : 'text-[32px] sm:text-[44px]',
        )}>
          FEED
        </span>
        <span
          className={cn(
            'font-black tracking-[-0.04em] text-[#E11D48]',
            isLarge ? 'text-[64px] xl:text-[76px]' : 'text-[32px] sm:text-[44px]',
          )}
          style={{ textShadow: '0 0 40px rgba(225,29,72,0.35)' }}
        >
          ME
        </span>
      </div>
      {isLarge && (
        <p className="mt-3 text-[11px] font-bold uppercase tracking-[0.22em] text-white/15">
          Instagram Analytics &middot; Reimagined
        </p>
      )}
    </div>
  );

  const trustFooter = (
    <div className="flex w-full flex-col items-center gap-3">
      <p className="text-[9px] font-bold uppercase tracking-[0.2em] text-white/10">
        Tracking 50M+ posts &middot; 10K feeds active
      </p>
      <div className="flex items-center gap-4">
        <a href="#" className="text-[9px] font-black uppercase tracking-[0.14em] text-white/12 transition-colors hover:text-white/25">Privacy</a>
        <span className="text-white/5">&middot;</span>
        <a href="#" className="text-[9px] font-black uppercase tracking-[0.14em] text-white/12 transition-colors hover:text-white/25">Terms</a>
        <span className="text-white/5">&middot;</span>
        <Link href="/" className="text-[9px] font-black uppercase tracking-[0.14em] text-white/12 transition-colors hover:text-[#E11D48]/35">Home &rarr;</Link>
      </div>
    </div>
  );

  const keyframes = (
    <style dangerouslySetInnerHTML={{ __html: `
      @keyframes shimmer-sweep {
        0% { transform: translate(-50%, -50%) rotate(0deg); }
        100% { transform: translate(-50%, -50%) rotate(360deg); }
      }
      @keyframes cta-breathe {
        0%, 100% { box-shadow: 0 0 0px rgba(225,29,72,0), 0 2px 8px rgba(0,0,0,0.4); }
        50% { box-shadow: 0 0 18px rgba(225,29,72,0.06), 0 2px 8px rgba(0,0,0,0.4); }
      }
      @keyframes loginFloat {
        0%, 100% { transform: translateY(0); }
        50% { transform: translateY(-8px); }
      }
    `}} />
  );

  return (
    <div
      className="relative flex min-h-[100svh] w-full overflow-hidden bg-[#030303] selection:bg-[#E11D48]/30 selection:text-white lg:h-[100vh] lg:min-h-0"
      style={{
        paddingTop: 'env(safe-area-inset-top, 0px)',
        paddingBottom: 'env(safe-area-inset-bottom, 0px)',
      }}
    >
      {ambientBg}
      {keyframes}

      {/* ── Top nav ── */}
      <div className="absolute right-4 top-4 z-20 sm:right-6 sm:top-6">
        <Link
          href="/"
          className="group flex items-center gap-1.5 rounded-full border border-white/[0.05] bg-white/[0.02] px-4 py-2 text-[9px] font-black uppercase tracking-[0.18em] text-white/25 backdrop-blur-sm transition-all hover:border-[#E11D48]/15 hover:text-[#E11D48]/50"
        >
          feedme.app
          <ExternalLink size={10} strokeWidth={3} className="opacity-40 transition-transform group-hover:translate-x-0.5" />
        </Link>
      </div>

      {/* ════════════════════════════════════════════
          DESKTOP: Two-panel split layout (lg+)
          Left  = Hero animation + brand (55%)
          Right = Login form card (45%)
         ════════════════════════════════════════════ */}

      {/* ── Left hero panel (desktop only) ── */}
      <div className="hidden lg:flex lg:w-[55%] relative z-10 flex-col items-center justify-center px-12 xl:px-20">
        <motion.div
          initial={{ opacity: 0, x: -30 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.9, ease: APPLE_EASE, delay: 0.15 }}
          className="flex flex-col items-center"
        >
          <PacManHero />
          {brandMark(true)}
          <div className="mt-8">{trustFooter}</div>
        </motion.div>
      </div>

      {/* ── Right form panel (desktop) / Full column (mobile) ── */}
      <div className="relative z-10 flex w-full flex-col items-center justify-center px-5 py-8 sm:py-14 lg:w-[45%] lg:py-0 lg:px-10 xl:px-16">

        {/* Mobile only: Pac-Man + brand stacked above the card */}
        <div className="lg:hidden w-full max-w-[460px] flex flex-col items-center">
          <PacManHero />
          {brandMark(false)}
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
              background: 'linear-gradient(170deg, rgba(14,14,18,0.95) 0%, rgba(8,8,10,0.98) 40%, rgba(4,4,6,1) 100%)',
              backdropFilter: 'blur(80px) saturate(180%)',
              WebkitBackdropFilter: 'blur(80px) saturate(180%)',
              border: '1px solid rgba(255, 255, 255, 0.05)',
              borderTopColor: 'rgba(255, 255, 255, 0.08)',
              boxShadow: [
                '0 2px 4px rgba(0,0,0,0.2)',
                '0 8px 16px rgba(0,0,0,0.3)',
                '0 24px 48px -8px rgba(0,0,0,0.6)',
                '0 48px 96px -16px rgba(0,0,0,0.85)',
                'inset 0 1px 0 rgba(255,255,255,0.06)',
                'inset 0 -1px 0 rgba(0,0,0,0.6)',
              ].join(', '),
            }}
          >
            {/* Liquid glass — top-left refraction + edge catch */}
            <div
              className="pointer-events-none absolute inset-0 z-0 rounded-[inherit]"
              style={{
                background: [
                  'radial-gradient(ellipse 55% 30% at 12% 0%, rgba(255,255,255,0.06) 0%, transparent 70%)',
                  'radial-gradient(ellipse 25% 50% at 100% 30%, rgba(225,29,72,0.008) 0%, transparent 50%)',
                  'linear-gradient(180deg, rgba(255,255,255,0.025) 0%, transparent 20%)',
                ].join(', '),
              }}
            />

            {/* Inner depth ring */}
            <div
              className="pointer-events-none absolute inset-[1px] z-0 rounded-[23px] sm:rounded-[27px] lg:rounded-[31px]"
              style={{
                border: '1px solid rgba(255,255,255,0.025)',
                borderTopColor: 'rgba(255,255,255,0.05)',
              }}
            />

            <div className="relative z-10 px-7 py-8 sm:px-9 sm:py-10 lg:px-11 lg:py-14">

            {/* ── Tab switcher — neomorphic inset well ── */}
            <div
              className="mx-auto mb-9 flex max-w-[280px] items-center gap-1 rounded-[14px] p-1.5"
              style={{
                background: 'rgba(0, 0, 0, 0.7)',
                border: '1px solid rgba(0, 0, 0, 0.9)',
                boxShadow: [
                  'inset 0 3px 12px rgba(0,0,0,1)',
                  'inset 0 1px 2px rgba(0,0,0,0.8)',
                  '0 1px 0 rgba(255,255,255,0.03)',
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
                      isActive ? 'text-white z-10' : 'text-white/18 z-0 hover:text-white/35',
                    )}
                  >
                    {isActive && (
                      <motion.div
                        layoutId="login-tab-pill"
                        className="absolute inset-0 rounded-[10px]"
                        style={{
                          background: 'linear-gradient(180deg, rgba(30,30,34,1) 0%, rgba(18,18,22,1) 100%)',
                          border: '1px solid rgba(255,255,255,0.07)',
                          borderTopColor: 'rgba(255,255,255,0.12)',
                          boxShadow: [
                            'inset 0 1px 0 rgba(255,255,255,0.07)',
                            'inset 0 -1px 0 rgba(0,0,0,0.4)',
                            '0 4px 14px rgba(0,0,0,0.7)',
                            '0 1px 3px rgba(0,0,0,0.4)',
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
            <div className="relative mb-7" style={{ minHeight: 56, perspective: '600px' }}>
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
                    className="text-[24px] font-black tracking-[-0.025em] text-white sm:text-[28px] lg:text-[34px]"
                    style={{ textShadow: '0 2px 16px rgba(0,0,0,0.7)' }}
                  >
                    {titles[mode]}
                  </h1>
                  <p className="mt-2 text-[11px] font-semibold text-white/25 tracking-wide lg:text-[12px]">
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
                    <label className="mb-2.5 block text-[9px] font-black uppercase tracking-[0.22em] text-white/30">
                      Email Address
                    </label>
                    <motion.div
                      animate={{
                        y: focusedField === 'email' ? -2 : 0,
                        boxShadow: focusedField === 'email'
                          ? 'inset 0 2px 12px rgba(0,0,0,0.8), 0 1px 0 rgba(255,255,255,0.04), 0 6px 20px rgba(0,0,0,0.4)'
                          : 'inset 0 3px 16px rgba(0,0,0,1), 0 1px 0 rgba(255,255,255,0.02)',
                      }}
                      transition={{ duration: 0.5, ease: APPLE_EASE }}
                      className="relative overflow-hidden rounded-[14px]"
                      style={{
                        background: 'rgba(0, 0, 0, 0.5)',
                        border: '1px solid rgba(255,255,255,0.03)',
                        boxShadow: 'inset 0 3px 16px rgba(0,0,0,1), 0 1px 0 rgba(255,255,255,0.02)',
                      }}
                    >
                      {/* Shimmer border — slow, smooth, premium sweep */}
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
                            background: 'conic-gradient(from 180deg, transparent 0deg, transparent 220deg, rgba(225,29,72,0.06) 250deg, rgba(225,29,72,0.35) 285deg, rgba(225,29,72,0.12) 320deg, transparent 350deg)',
                          }}
                        />
                      </motion.div>

                      {/* Inner mask — reveals only the border glow */}
                      <div
                        className="pointer-events-none absolute inset-[1.5px] z-[1] rounded-[12.5px] transition-colors duration-500"
                        style={{ background: focusedField === 'email' ? 'rgba(6,6,8,0.97)' : 'rgba(0,0,0,0.5)' }}
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
                        className="relative z-10 h-[54px] lg:h-[58px] w-full rounded-[14px] bg-transparent px-5 text-[14px] font-semibold text-white outline-none placeholder:text-white/[0.08] focus:ring-0"
                        style={{ caretColor: '#E11D48' }}
                      />
                    </motion.div>
                  </div>

                  {/* Password */}
                  {mode !== 'forgot' && (
                    <div>
                      <div className="mb-2.5 flex items-center justify-between">
                        <label className="text-[9px] font-black uppercase tracking-[0.22em] text-white/30">
                          Password
                        </label>
                        {mode === 'login' && (
                          <button
                            type="button"
                            onClick={() => { setMode('forgot'); setError(null); setMessage(null); }}
                            className="text-[9px] font-black uppercase tracking-[0.12em] text-[#E11D48]/40 transition-colors hover:text-[#E11D48]/70"
                          >
                            Forgot?
                          </button>
                        )}
                      </div>
                      <motion.div
                        animate={{
                          y: focusedField === 'password' ? -2 : 0,
                          boxShadow: focusedField === 'password'
                            ? 'inset 0 2px 12px rgba(0,0,0,0.8), 0 1px 0 rgba(255,255,255,0.04), 0 6px 20px rgba(0,0,0,0.4)'
                            : 'inset 0 3px 16px rgba(0,0,0,1), 0 1px 0 rgba(255,255,255,0.02)',
                        }}
                        transition={{ duration: 0.5, ease: APPLE_EASE }}
                        className="relative overflow-hidden rounded-[14px]"
                        style={{
                          background: 'rgba(0, 0, 0, 0.5)',
                          border: '1px solid rgba(255,255,255,0.03)',
                          boxShadow: 'inset 0 3px 16px rgba(0,0,0,1), 0 1px 0 rgba(255,255,255,0.02)',
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
                              background: 'conic-gradient(from 180deg, transparent 0deg, transparent 220deg, rgba(225,29,72,0.06) 250deg, rgba(225,29,72,0.35) 285deg, rgba(225,29,72,0.12) 320deg, transparent 350deg)',
                            }}
                          />
                        </motion.div>

                        {/* Inner mask */}
                        <div
                          className="pointer-events-none absolute inset-[1.5px] z-[1] rounded-[12.5px] transition-colors duration-500"
                          style={{ background: focusedField === 'password' ? 'rgba(6,6,8,0.97)' : 'rgba(0,0,0,0.5)' }}
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
                          className="relative z-10 h-[54px] lg:h-[58px] w-full rounded-[14px] bg-transparent px-5 pr-12 text-[14px] font-semibold text-white outline-none placeholder:text-white/[0.08] focus:ring-0"
                          style={{ caretColor: '#E11D48' }}
                        />
                        <button
                          type="button"
                          onClick={() => setShowPassword(!showPassword)}
                          className="absolute right-2 top-1/2 z-10 -translate-y-1/2 rounded-[10px] p-2.5 text-white/15 transition-all hover:bg-white/[0.04] hover:text-white/35"
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
                      className="rounded-[12px] px-4 py-3 text-[11px] font-bold text-red-400/90"
                      style={{
                        background: 'rgba(180, 30, 30, 0.06)',
                        border: '1px solid rgba(180, 30, 30, 0.08)',
                        boxShadow: 'inset 0 2px 10px rgba(0,0,0,0.5)',
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
                      className="rounded-[12px] px-4 py-3 text-[11px] font-bold text-[#E11D48]/60"
                      style={{
                        background: 'rgba(225,29,72, 0.025)',
                        border: '1px solid rgba(225,29,72, 0.06)',
                        boxShadow: 'inset 0 2px 10px rgba(0,0,0,0.5)',
                      }}
                    >
                      {message}
                    </div>
                  </motion.div>
                )}
              </AnimatePresence>

              {/* ── Flat CTA with periodic subtle breathe glow ── */}
              <motion.button
                type="submit"
                disabled={loading || isSuccess}
                whileTap={{ scale: 0.97 }}
                layout
                className={cn(
                  'group relative mt-4 flex h-[56px] lg:h-[62px] w-full cursor-pointer items-center justify-center overflow-hidden rounded-[14px] lg:rounded-[16px]',
                  'text-[12px] lg:text-[13px] font-black uppercase tracking-[0.2em] text-[#0a0a0a]',
                  'disabled:opacity-30 disabled:cursor-not-allowed'
                )}
                style={{
                  background: '#E11D48',
                  animation: loading || isSuccess ? 'none' : 'cta-breathe 4s ease-in-out infinite',
                }}
                transition={{ layout: { duration: 0.4, ease: APPLE_EASE } }}
              >
                {/* Top highlight bevel */}
                <div className="pointer-events-none absolute inset-0 z-0 rounded-[14px]"
                  style={{ background: 'linear-gradient(180deg, rgba(255,255,255,0.18) 0%, transparent 40%, rgba(0,0,0,0.06) 100%)' }}
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
                    className="text-[10px] font-black uppercase tracking-[0.12em] text-white/18 transition-colors hover:text-[#E11D48]/50"
                  >
                    &larr; Back to Login
                  </button>
                </motion.div>
              )}
            </AnimatePresence>
          </div>
        </motion.div>
        </div>{/* end form card wrapper */}

        {/* Mobile only: trust footer */}
        <div className="lg:hidden mt-8 sm:mt-10 w-full max-w-[460px]">
          {trustFooter}
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

      {/* Autofill override */}
      <style jsx global>{`
        input:-webkit-autofill,
        input:-webkit-autofill:hover,
        input:-webkit-autofill:focus,
        input:-webkit-autofill:active {
          -webkit-box-shadow: 0 0 0 30px rgba(3,3,3,0.99) inset !important;
          -webkit-text-fill-color: white !important;
          caret-color: #E11D48 !important;
          transition: background-color 5000s ease-in-out 0s;
        }
      `}</style>
    </div>
  );
}
