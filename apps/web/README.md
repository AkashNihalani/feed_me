# FeedMe Web App

Next.js frontend for FeedMe.

## Local Development

1. Copy values into `.env.local` from your project secrets.
2. Keep `NEXT_PUBLIC_SITE_URL` and `NEXT_PUBLIC_BASE_URL` aligned for each environment.
3. Run:

```bash
npm run dev
```

## Auth Email Setup

Supabase remains the auth provider, but branded auth email assets live in:

- [`infra/supabase/auth-email/README.md`](/Users/Akash/feed_me/infra/supabase/auth-email/README.md)
- [`infra/supabase/auth-email/confirm-signup.html`](/Users/Akash/feed_me/infra/supabase/auth-email/confirm-signup.html)
- [`infra/supabase/auth-email/reset-password.html`](/Users/Akash/feed_me/infra/supabase/auth-email/reset-password.html)

The web app currently relies on:

- `/auth/callback` for signup and email verification completion
- `/auth/update-password` for password recovery completion

## Environment Notes

See [`.env.example`](/Users/Akash/feed_me/apps/web/.env.example) for the expected variable names.
