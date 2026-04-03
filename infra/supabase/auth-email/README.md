# FeedMe Auth Email Setup

This directory stores the branded Supabase Auth email assets for FeedMe.

## Scope

Supabase remains the auth provider and still owns:

- account confirmation links
- password recovery links
- invite links
- email-change links

FeedMe owns the sender identity, copy, and visual style by configuring custom SMTP and custom templates in the Supabase dashboard.

## Required Supabase Dashboard Settings

Open Supabase Dashboard for the linked project and update:

1. `Authentication -> URL Configuration`
2. `Authentication -> Email Templates`
3. `Project Settings -> Authentication -> SMTP Settings`

Use these values:

- Sender name: `FeedMe`
- Sender email: `feedme@onlyallai.in`
- SMTP host: `smtp.gmail.com`
- SMTP port: `587`
- SMTP user: your FeedMe mailbox
- SMTP password: your Gmail app password

Set the site URL to your canonical app URL:

- Production: `https://feedmemore.vercel.app`
- Local dev: `http://localhost:3000`

Add redirect URLs for:

- `https://feedmemore.vercel.app/auth/callback`
- `https://feedmemore.vercel.app/auth/update-password`
- `http://localhost:3000/auth/callback`
- `http://localhost:3000/auth/update-password`

If you use preview deployments, add each preview host that needs auth testing.

## Template Mapping

- Confirm signup: [`confirm-signup.html`](/Users/Akash/feed_me/infra/supabase/auth-email/confirm-signup.html)
- Reset password: [`reset-password.html`](/Users/Akash/feed_me/infra/supabase/auth-email/reset-password.html)
- Optional future templates:
  [`invite-user.html`](/Users/Akash/feed_me/infra/supabase/auth-email/invite-user.html)
  [`change-email.html`](/Users/Akash/feed_me/infra/supabase/auth-email/change-email.html)

## Subjects

Set these subjects in Supabase:

- Confirmation: `Confirm your FeedMe account`
- Recovery: `Reset your FeedMe password`
- Invite: `Join FeedMe`
- Email change: `Confirm your new FeedMe email`

## Notes

- Supabase template variables such as `{{ .ConfirmationURL }}`, `{{ .SiteURL }}`, and `{{ .Email }}` are expected to be available in the email editor.
- Keep the app auth flow unchanged. The web app already triggers signup and recovery through Supabase.
- The reset flow lands on `/auth/update-password`, which is implemented in the web app and completes `supabase.auth.updateUser({ password })`.
- Rotate SMTP and Supabase secrets if they were ever shared outside your local environment.
- These templates intentionally use a calmer, more trust-oriented layout to reduce the chance of Gmail flagging them as promotional or spammy. They do not depend on the Only Allai marketing site.
