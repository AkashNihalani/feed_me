import { createServerClient, type CookieOptions } from '@supabase/ssr';
import { NextRequest, NextResponse } from 'next/server';

type CookieMutation = {
  name: string;
  value: string;
  options: CookieOptions;
};

export async function POST(request: NextRequest) {
  const payload = (await request.json().catch(() => ({}))) as { email?: string; password?: string };
  const email = (payload.email ?? '').trim();
  const password = payload.password ?? '';

  if (!email || !password) {
    return NextResponse.json({ error: 'Email and password are required.' }, { status: 400 });
  }

  const cookieMutations: CookieMutation[] = [];

  const supabase = createServerClient(
    process.env.NEXT_PUBLIC_SUPABASE_URL!,
    process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY!,
    {
      cookies: {
        get(name: string) {
          return request.cookies.get(name)?.value;
        },
        set(name: string, value: string, options: CookieOptions) {
          cookieMutations.push({ name, value, options });
        },
        remove(name: string, options: CookieOptions) {
          cookieMutations.push({ name, value: '', options });
        },
      },
    }
  );

  const { error } = await supabase.auth.signInWithPassword({ email, password });

  if (error) {
    return NextResponse.json({ error: error.message }, { status: 401 });
  }

  const response = NextResponse.json({ ok: true });
  for (const mutation of cookieMutations) {
    response.cookies.set({
      name: mutation.name,
      value: mutation.value,
      ...mutation.options,
    });
  }

  return response;
}
