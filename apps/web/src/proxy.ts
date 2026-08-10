import { createServerClient, type CookieOptions } from '@supabase/ssr'
import { NextResponse, type NextRequest } from 'next/server'

const PROTECTED_PREFIXES = ['/profile', '/feed', '/lead', '/fire', '/command', '/drop', '/read']
const PROTECTED_EXACT_PATHS = new Set(['/'])

function safeInternalPath(value: string | null): string {
  if (!value || !value.startsWith('/') || value.startsWith('//')) {
    return '/'
  }

  try {
    const parsed = new URL(value, 'https://feedme.local')
    return `${parsed.pathname}${parsed.search}${parsed.hash}`
  } catch {
    return '/'
  }
}

function isProtectedPath(pathname: string): boolean {
  return PROTECTED_EXACT_PATHS.has(pathname) || PROTECTED_PREFIXES.some((prefix) => pathname.startsWith(prefix))
}

function redirectToLogin(request: NextRequest) {
  const loginUrl = new URL('/login', request.url)
  loginUrl.searchParams.set('next', safeInternalPath(`${request.nextUrl.pathname}${request.nextUrl.search}`))
  return NextResponse.redirect(loginUrl)
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
      return redirectToLogin(request)
    }

    if (request.nextUrl.pathname.startsWith('/login') && user) {
      const nextPath = safeInternalPath(request.nextUrl.searchParams.get('next'))
      return NextResponse.redirect(new URL(nextPath, request.url))
    }
  } catch (err) {
    console.error('[proxy] Auth check failed:', err)
    if (isProtectedPath(request.nextUrl.pathname)) {
      return redirectToLogin(request)
    }
  }

  return response
}

export const config = {
  matcher: ['/', '/lead/:path*', '/fire/:path*', '/profile/:path*', '/feed/:path*', '/command/:path*', '/drop/:path*', '/read/:path*', '/login'],
}
