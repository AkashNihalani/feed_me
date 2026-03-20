import { createServerClient, type CookieOptions } from '@supabase/ssr'
import { NextResponse, type NextRequest } from 'next/server'

const PROTECTED_PREFIXES = ['/profile', '/feed']

function isProtectedPath(pathname: string): boolean {
  return PROTECTED_PREFIXES.some((prefix) => pathname.startsWith(prefix))
}

export async function proxy(request: NextRequest) {
  const response = NextResponse.next({
    request: {
      headers: request.headers,
    },
  })

  try {
    const url = process.env.NEXT_PUBLIC_SUPABASE_URL
    const key = process.env.NEXT_PUBLIC_SUPABASE_ANON_KEY
    if (!url || !key) {
      return response
    }

    const supabase = createServerClient(url, key, {
      cookies: {
        get(name: string) {
          return request.cookies.get(name)?.value
        },
        set(name: string, value: string, options: CookieOptions) {
          response.cookies.set({ name, value, ...options })
        },
        remove(name: string, options: CookieOptions) {
          response.cookies.set({ name, value: '', ...options })
        },
      },
    })

    const {
      data: { user },
    } = await supabase.auth.getUser()

    if (isProtectedPath(request.nextUrl.pathname) && !user) {
      const loginUrl = new URL('/login', request.url)
      loginUrl.searchParams.set('next', request.nextUrl.pathname)
      return NextResponse.redirect(loginUrl)
    }

    if (request.nextUrl.pathname.startsWith('/login') && user) {
      const nextPath = request.nextUrl.searchParams.get('next') || '/'
      return NextResponse.redirect(new URL(nextPath, request.url))
    }
  } catch (err) {
    console.error('[proxy] Auth check failed, passing through:', err)
  }

  return response
}

export const config = {
  matcher: ['/profile/:path*', '/feed/:path*', '/login'],
}
