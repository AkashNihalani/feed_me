'use client';

import { FormEvent, Suspense, useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';
import { Loader2 } from 'lucide-react';
import { createBrowserClient } from '@supabase/ssr';

type RecoveryState = 'verifying' | 'ready' | 'error' | 'success';

function UpdatePasswordContent() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const supabase = useMemo(
    () =>
      createBrowserClient(
        process.env.NEXT_PUBLIC_SUPABASE_URL!,
        process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
      ),
    []
  );

  const [password, setPassword] = useState('');
  const [confirmPassword, setConfirmPassword] = useState('');
  const [notice, setNotice] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [state, setState] = useState<RecoveryState>('verifying');
  const [submitting, setSubmitting] = useState(false);

  useEffect(() => {
    let cancelled = false;

    const verifyRecoverySession = async () => {
      const hashParams = new URLSearchParams(window.location.hash.replace(/^#/, ''));
      const queryCode = searchParams.get('code');
      const queryTokenHash = searchParams.get('token_hash');
      const queryType = searchParams.get('type');
      const hashAccessToken = hashParams.get('access_token');
      const hashRefreshToken = hashParams.get('refresh_token');
      const hashType = hashParams.get('type');

      try {
        if (queryCode) {
          const { error } = await supabase.auth.exchangeCodeForSession(queryCode);
          if (error) throw error;
        } else if (queryTokenHash && queryType) {
          const { error } = await supabase.auth.verifyOtp({
            token_hash: queryTokenHash,
            type: queryType as 'signup' | 'recovery' | 'invite' | 'email',
          });
          if (error) throw error;
        } else if (hashAccessToken && hashRefreshToken && hashType === 'recovery') {
          const { error } = await supabase.auth.setSession({
            access_token: hashAccessToken,
            refresh_token: hashRefreshToken,
          });
          if (error) throw error;
        }

        const {
          data: { session },
          error: sessionError,
        } = await supabase.auth.getSession();
        if (sessionError) throw sessionError;
        if (!session) throw new Error('This password reset link is invalid or has expired.');

        if (!cancelled) {
          setState('ready');
          setNotice('Set a new password to finish securing your FeedMe account.');
        }
      } catch (err) {
        if (!cancelled) {
          setState('error');
          setError(err instanceof Error ? err.message : 'Unable to verify this password reset link.');
        }
      }
    };

    verifyRecoverySession();

    return () => {
      cancelled = true;
    };
  }, [searchParams, supabase]);

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault();
    setError(null);
    setNotice(null);

    if (password.length < 8) {
      setError('Use at least 8 characters for your new password.');
      return;
    }
    if (password !== confirmPassword) {
      setError('Passwords do not match.');
      return;
    }

    setSubmitting(true);
    try {
      const { error } = await supabase.auth.updateUser({ password });
      if (error) throw error;

      setState('success');
      setNotice('Password updated. Redirecting you back to login.');
      window.history.replaceState({}, document.title, '/auth/update-password');
      setTimeout(() => {
        router.push('/login?message=password_updated');
      }, 900);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Unable to update your password.');
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="relative flex min-h-[100svh] items-center justify-center overflow-hidden bg-[#030303] px-6 text-white">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top,#E11D4814,transparent_28%),linear-gradient(180deg,rgba(255,255,255,0.02),transparent_42%)]" />
      <div className="relative w-full max-w-md border border-white/10 bg-black/80 p-8 shadow-[0_0_80px_rgba(225,29,72,0.08)]">
        <div className="mb-8">
          <p className="text-xs font-black uppercase tracking-[0.35em] text-[#E11D48]">FeedMe</p>
          <h1 className="mt-4 text-4xl font-black uppercase tracking-[-0.06em]">
            Reset access
          </h1>
          <p className="mt-3 text-sm uppercase tracking-[0.18em] text-white/55">
            Secure the account. Keep momentum.
          </p>
        </div>

        {state === 'verifying' ? (
          <div className="flex min-h-40 items-center justify-center text-sm uppercase tracking-[0.2em] text-white/65">
            <Loader2 className="mr-3 h-5 w-5 animate-spin text-[#E11D48]" />
            Verifying reset link
          </div>
        ) : null}

        {state === 'error' ? (
          <div className="space-y-4">
            <p className="border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-100">
              {error}
            </p>
            <Link
              href="/login"
              className="inline-flex h-11 items-center justify-center border border-white/15 px-5 text-xs font-black uppercase tracking-[0.22em] text-white transition hover:border-[#E11D48] hover:text-[#E11D48]"
            >
              Back to login
            </Link>
          </div>
        ) : null}

        {state === 'ready' || state === 'success' ? (
          <form className="space-y-4" onSubmit={handleSubmit}>
            {notice ? (
              <p className="border border-white/10 bg-white/5 px-4 py-3 text-sm text-white/78">
                {notice}
              </p>
            ) : null}
            {error ? (
              <p className="border border-red-500/30 bg-red-500/10 px-4 py-3 text-sm text-red-100">
                {error}
              </p>
            ) : null}

            <label className="block">
              <span className="mb-2 block text-xs font-black uppercase tracking-[0.22em] text-white/55">
                New password
              </span>
              <input
                type="password"
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                autoComplete="new-password"
                className="h-12 w-full border border-white/10 bg-white/5 px-4 text-base text-white outline-none transition focus:border-[#E11D48]"
                placeholder="At least 8 characters"
                disabled={submitting || state === 'success'}
              />
            </label>

            <label className="block">
              <span className="mb-2 block text-xs font-black uppercase tracking-[0.22em] text-white/55">
                Confirm password
              </span>
              <input
                type="password"
                value={confirmPassword}
                onChange={(event) => setConfirmPassword(event.target.value)}
                autoComplete="new-password"
                className="h-12 w-full border border-white/10 bg-white/5 px-4 text-base text-white outline-none transition focus:border-[#E11D48]"
                placeholder="Repeat your password"
                disabled={submitting || state === 'success'}
              />
            </label>

            <button
              type="submit"
              disabled={submitting || state === 'success'}
              className="inline-flex h-12 w-full items-center justify-center bg-[#E11D48] px-5 text-xs font-black uppercase tracking-[0.24em] text-white transition hover:brightness-105 disabled:cursor-not-allowed disabled:opacity-70"
            >
              {submitting ? (
                <>
                  <Loader2 className="mr-2 h-4 w-4 animate-spin" />
                  Updating
                </>
              ) : state === 'success' ? (
                'Password updated'
              ) : (
                'Reset password'
              )}
            </button>
          </form>
        ) : null}
      </div>
    </div>
  );
}

export default function UpdatePasswordPage() {
  return (
    <Suspense
      fallback={
        <div className="flex min-h-screen items-center justify-center bg-[#030303] text-white">
          <Loader2 className="h-8 w-8 animate-spin text-[#E11D48]" />
        </div>
      }
    >
      <UpdatePasswordContent />
    </Suspense>
  );
}
