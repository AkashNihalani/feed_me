'use client';

import { createBrowserClient } from '@supabase/ssr';
import { useEffect, useState } from 'react';
import Link from 'next/link';
import { usePathname, useRouter } from 'next/navigation';
import { cn } from '@/lib/utils';
import { Zap, User, LogIn, LogOut, Flag, Fuel } from 'lucide-react';

export default function TopNav() {
  const pathname = usePathname();
  const router = useRouter();

  const supabase = createBrowserClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!
  );

  const [session, setSession] = useState<any>(null);

  useEffect(() => {
    const getSession = async () => {
      const { data: { session } } = await supabase.auth.getSession();
      setSession(session);
    };
    getSession();

    const { data: { subscription } } = supabase.auth.onAuthStateChange((_event: unknown, session: any) => {
      setSession(session);
    });

    return () => subscription.unsubscribe();
  }, [supabase]);

  const handleLogout = async () => {
    await supabase.auth.signOut();
    router.push('/login');
    router.refresh();
  };

  if (pathname === '/login') return null;

  return (
    <nav className="fixed top-0 left-0 right-0 z-50 px-5 py-4 pointer-events-none">
      <div className="grid grid-cols-[auto,1fr,auto] items-center gap-4">
        {/* Brand (hidden on mobile to avoid clash with tabs) */}
        <div className="pointer-events-auto hidden md:flex">
          <Link href="/" className="flex items-center gap-2 select-none">
            <span className="font-black text-xl tracking-tight leading-none bg-black text-white px-2 py-1 shadow-[2px_2px_0px_0px_var(--color-lime)]">FEED</span>
            <span className="font-black text-xl tracking-tight leading-none bg-lime text-black px-2 py-1 border-2 border-black -ml-1">ME</span>
          </Link>
        </div>

        {/* Center Tabs (desktop only) */}
        <div className="hidden md:flex pointer-events-auto justify-center">
          <div className="relative flex items-center bg-background border-2 border-foreground p-1 shadow-hard rounded-full gap-1">
            <div
              className={cn(
                "absolute top-1 bottom-1 rounded-full bg-foreground transition-all duration-300 ease-[cubic-bezier(0.2,0,0,1)]",
                pathname === '/' ? "left-1 w-[calc(33.33%-4px)]" :
                pathname === '/flags' ? "left-[calc(33.33%)] w-[calc(33.33%-4px)]" :
                pathname === '/profile' ? "left-[calc(66.66%)] w-[calc(33.33%-4px)]" : "hidden"
              )}
            />

            <Link
              href="/"
              className={cn(
                "px-6 py-2 rounded-full font-black uppercase text-sm flex items-center gap-2 relative z-10 transition-colors",
                pathname === '/' ? "text-background" : "text-neutral-gray hover:text-black"
              )}
            >
              <Zap size={16} className={pathname === '/' ? "fill-background" : "fill-none"} />
              Feed
            </Link>

            <Link
              href="/flags"
              className={cn(
                "px-6 py-2 rounded-full font-black uppercase text-sm flex items-center gap-2 relative z-10 transition-colors",
                pathname === '/flags' ? "text-background" : "text-neutral-gray hover:text-black"
              )}
            >
              <Flag size={16} className={pathname === '/flags' ? "fill-background" : "fill-none"} />
              Flags
            </Link>

            {session ? (
              <Link
                href="/profile"
                className={cn(
                  "px-6 py-2 rounded-full font-black uppercase text-sm flex items-center gap-2 relative z-10 transition-colors",
                  pathname === '/profile' ? "text-background" : "text-neutral-gray hover:text-black"
                )}
              >
                <Fuel size={16} className={pathname === '/profile' ? "fill-background" : "fill-none"} />
                Fuel
              </Link>
            ) : (
              <Link
                href="/login"
                className={cn(
                  "px-6 py-2 rounded-full font-black uppercase text-sm flex items-center gap-2 relative z-10 transition-colors",
                  pathname === '/login' ? "text-background" : "text-neutral-gray hover:text-black"
                )}
              >
                <LogIn size={16} />
                Login
              </Link>
            )}
          </div>
        </div>

        {/* Right (Logout) */}
        <div className="pointer-events-auto flex justify-end">
          {session && (
            <button
              onClick={handleLogout}
              className="p-2 bg-gray-100 hover:bg-black hover:text-white border-2 border-black transition-all rounded shadow-[2px_2px_0px_0px_rgba(0,0,0,1)]"
              title="Logout"
            >
              <LogOut size={16} />
            </button>
          )}
        </div>
      </div>
    </nav>
  );
}
