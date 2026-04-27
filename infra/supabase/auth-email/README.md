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

- Confirm signup: `confirm-signup.html`
- Reset password: `reset-password.html`
- Invite user: `invite-user.html`
- Email change: `change-email.html`
- Magic link: `magic-link.html`
- Reauthentication: `reauthentication.html`

These templates share the same FeedMe email system with the red signature accent `#e11d48`.

## Subjects

Set these subjects in Supabase:

- Confirmation: `Confirm your FeedMe account`
- Recovery: `Reset your FeedMe password`
- Invite: `Join FeedMe`
- Email change: `Confirm your new FeedMe email`
- Magic link: `Sign in to FeedMe`
- Reauthentication: `Confirm this FeedMe action`

## Updating Hosted Supabase

Editing files in this repo does not update the hosted Supabase dashboard automatically. The dashboard stores its own template HTML, which is why an old green preview can still appear after local file changes.

Manual dashboard update:

1. Open `Authentication -> Email Templates`.
2. Open each template listed above.
3. Switch the Body editor to `Source`.
4. Paste the matching HTML file contents.
5. Save changes before moving to the next template.

Management API update:

```bash
node infra/supabase/auth-email/scripts/build-management-payload.mjs --write infra/supabase/auth-email/management-payload.json
SUPABASE_ACCESS_TOKEN="..." node infra/supabase/auth-email/scripts/push-management-payload.mjs
```

The Management API token needs Supabase auth config write access. The project ref defaults to `worqtdkvicuhmdgoncru`; override it with `SUPABASE_PROJECT_REF` if needed.

## Assets

The templates load email-safe PNG badges from the public Supabase `email-assets` bucket:

- `https://worqtdkvicuhmdgoncru.supabase.co/storage/v1/object/public/email-assets/feedme-confirm-badge-red-sticker-wide.png`
- `https://worqtdkvicuhmdgoncru.supabase.co/storage/v1/object/public/email-assets/feedme-confirm-badge-red-sticker-wide-dark.png`
- Password recovery uses `https://worqtdkvicuhmdgoncru.supabase.co/storage/v1/object/public/email-assets/feedme-reset-badge-wide.png`
- Password recovery dark mode uses `https://worqtdkvicuhmdgoncru.supabase.co/storage/v1/object/public/email-assets/feedme-reset-badge-wide-dark.png`

Source assets live in `apps/web/public/email/`. The Supabase storage URLs are preferred inside email templates because they are already public and independent from Vercel deploy timing.

## Layout

The templates use a full-width email canvas rather than a floating rounded card. The main content remains centered with bounded text width inside the canvas for readability on desktop and mobile.

## Non-Auth Transactional Emails

Purchase, invoice, and support acknowledgement templates live in `infra/email/transactional/`. Those are provider-neutral and can be wired into a transactional email provider when one is selected.

## Notes

- Supabase template variables such as `{{ .ConfirmationURL }}`, `{{ .SiteURL }}`, and `{{ .Email }}` are expected to be available in the email editor.
- Keep the app auth flow unchanged. The web app already triggers signup and recovery through Supabase.
- The reset flow lands on `/auth/update-password`, which is implemented in the web app and completes `supabase.auth.updateUser({ password })`.
- Rotate SMTP and Supabase secrets if they were ever shared outside your local environment.
- These templates intentionally use a calmer, more trust-oriented layout to reduce the chance of Gmail flagging them as promotional or spammy. They do not depend on the Only Allai marketing site.
