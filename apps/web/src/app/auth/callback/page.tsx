'use client';

import { Suspense, useEffect } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import { createBrowserClient } from '@supabase/ssr';
import { Loader2 } from 'lucide-react';

function AuthCallbackContent() {
  const router = useRouter();
  const searchParams = useSearchParams();

  useEffect(() => {
    const supabase = createBrowserClient(
      process.env.NEXT_PUBLIC_SUPABASE_URL!,
      process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
    );

    const handleCallback = async () => {
      const code = searchParams.get('code');
      const token_hash = searchParams.get('token_hash');
      const type = searchParams.get('type');

      try {
        // Handle OAuth code exchange (e.g., Google login)
        if (code) {
          const { error } = await supabase.auth.exchangeCodeForSession(code);
          if (error) throw error;
        }
        
        // Handle email verification token (signup confirmation)
        if (token_hash && type) {
          const { error } = await supabase.auth.verifyOtp({
            token_hash,
            type: type as 'signup' | 'recovery' | 'invite' | 'email',
          });
          if (error) throw error;
        }

        // Success - redirect to main app
        router.push('/');
        router.refresh();
      } catch (err) {
        console.error('Auth callback error:', err);
        router.push('/login?error=verification_failed');
      }
    };

    handleCallback();
  }, [searchParams, router]);

  return (
    <div className="h-screen w-screen flex items-center justify-center bg-black text-white">
      <div className="text-center">
        <Loader2 className="animate-spin w-16 h-16 mx-auto mb-6 text-lime" />
        <h1 className="text-4xl font-black uppercase tracking-tighter italic">
          Verifying...
        </h1>
        <p className="text-gray-400 font-bold uppercase text-sm mt-2">
          Completing authentication
        </p>
      </div>
    </div>
  );
}

export default function AuthCallbackPage() {
  return (
    <Suspense fallback={
      <div className="h-screen w-screen flex items-center justify-center bg-black text-white">
        <Loader2 className="animate-spin w-16 h-16 text-lime" />
      </div>
    }>
      <AuthCallbackContent />
    </Suspense>
  );
}
