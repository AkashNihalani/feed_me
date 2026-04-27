const projectRef = process.env.SUPABASE_PROJECT_REF || 'worqtdkvicuhmdgoncru';
const accessToken = process.env.SUPABASE_ACCESS_TOKEN;

if (!accessToken) {
  throw new Error('Set SUPABASE_ACCESS_TOKEN before verifying auth email templates.');
}

const contentKeys = [
  'mailer_templates_confirmation_content',
  'mailer_templates_recovery_content',
  'mailer_templates_invite_content',
  'mailer_templates_email_change_content',
  'mailer_templates_magic_link_content',
  'mailer_templates_reauthentication_content',
];

const response = await fetch(`https://api.supabase.com/v1/projects/${projectRef}/config/auth`, {
  headers: {
    Authorization: `Bearer ${accessToken}`,
  },
});

const text = await response.text();

if (!response.ok) {
  throw new Error(`Supabase auth config read failed (${response.status}): ${text}`);
}

const config = JSON.parse(text);
const missing = [];
const staleGreen = [];
const redAccent = [];
const storageBadge = [];
const boxedLayout = [];

for (const key of contentKeys) {
  const content = config[key];
  if (!content) {
    missing.push(key);
    continue;
  }
  if (content.includes('#ccff00') || content.includes('ccff00')) staleGreen.push(key);
  if (content.includes('#e11d48') || content.includes('#E11D48')) redAccent.push(key);
  if (content.includes('border-radius:40px') || content.includes('max-width:600px') || content.includes('padding:28px 12px')) {
    boxedLayout.push(key);
  }
  const expectedBadge = key === 'mailer_templates_recovery_content'
    ? 'email-assets/feedme-reset-badge-wide.png'
    : 'email-assets/feedme-confirm-badge-red-sticker-wide.png';
  if (content.includes(expectedBadge)) storageBadge.push(key);
}

if (missing.length || staleGreen.length || boxedLayout.length || redAccent.length !== contentKeys.length || storageBadge.length !== contentKeys.length) {
  console.log(JSON.stringify({ missing, staleGreen, boxedLayout, redAccent, storageBadge }, null, 2));
  process.exitCode = 1;
} else {
  console.log(`Verified ${contentKeys.length} hosted auth email templates: red accent, expected badge URLs, and full-width layout present; old green absent.`);
}
