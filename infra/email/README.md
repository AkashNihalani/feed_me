# FeedMe Transactional Email Templates

This directory stores provider-neutral transactional email templates for FeedMe flows that are not owned by Supabase Auth.

## Templates

- `transactional/feed-pass-purchase-acknowledgement.html`
  Subject: `Feed Pass purchase confirmed`
- `transactional/invoice-acknowledgement.html`
  Subject: `We received your FeedMe invoice request`
- `transactional/bug-report-acknowledgement.html`
  Subject: `FeedMe bug report received`

## Placeholders

These templates use simple double-brace placeholders so they can be adapted to Resend, SendGrid, Postmark, Razorpay, or any other sender.

- `{{ customer_name }}`
- `{{ customer_email }}`
- `{{ amount }}`
- `{{ feed_pass_count }}`
- `{{ transaction_id }}`
- `{{ invoice_number }}`
- `{{ receipt_url }}`
- `{{ invoice_url }}`
- `{{ ticket_id }}`
- `{{ issue_summary }}`
- `{{ support_email }}`
- `{{ app_url }}`

Keep the FeedMe accent on `#e11d48`. The PNG badge assets live at `apps/web/public/email/` and are referenced from the public Supabase `email-assets` bucket with a cache-busting version query.
