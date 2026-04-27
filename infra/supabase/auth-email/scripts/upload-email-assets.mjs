import { readFile } from 'node:fs/promises';
import { createRequire } from 'node:module';
import path from 'node:path';

function parseEnv(source) {
  const values = {};
  for (const line of source.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith('#') || !trimmed.includes('=')) continue;
    const [key, ...parts] = trimmed.split('=');
    values[key] = parts.join('=').trim().replace(/^["']|["']$/g, '');
  }
  return values;
}

const repoRoot = path.resolve(import.meta.dirname, '../../../..');
const webRoot = path.join(repoRoot, 'apps/web');
const require = createRequire(path.join(webRoot, 'package.json'));
const { createClient } = require('@supabase/supabase-js');
const env = parseEnv(await readFile(path.join(webRoot, '.env.local'), 'utf8'));

const supabaseUrl = process.env.NEXT_PUBLIC_SUPABASE_URL || env.NEXT_PUBLIC_SUPABASE_URL;
const serviceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY || env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !serviceRoleKey) {
  throw new Error('Missing NEXT_PUBLIC_SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY');
}

const supabase = createClient(supabaseUrl, serviceRoleKey, {
  auth: { persistSession: false },
});

const assets = [
  {
    source: 'feedme-confirm-badge.png',
    target: 'feedme-confirm-badge.png',
  },
  {
    source: 'feedme-confirm-badge-dark.png',
    target: 'feedme-confirm-badge-dark.png',
  },
  {
    source: 'feedme-confirm-badge.png',
    target: 'feedme-confirm-badge-red-sticker.png',
  },
  {
    source: 'feedme-confirm-badge-dark.png',
    target: 'feedme-confirm-badge-red-sticker-dark.png',
  },
  {
    source: 'feedme-confirm-badge.png',
    target: 'feedme-confirm-badge-red-sticker-3d.png',
  },
  {
    source: 'feedme-confirm-badge-dark.png',
    target: 'feedme-confirm-badge-red-sticker-3d-dark.png',
  },
  {
    source: 'feedme-confirm-badge.png',
    target: 'feedme-confirm-badge-red-sticker-soft3d.png',
  },
  {
    source: 'feedme-confirm-badge-dark.png',
    target: 'feedme-confirm-badge-red-sticker-soft3d-dark.png',
  },
  {
    source: 'feedme-confirm-badge.png',
    target: 'feedme-confirm-badge-red-sticker-wide.png',
  },
  {
    source: 'feedme-confirm-badge-dark.png',
    target: 'feedme-confirm-badge-red-sticker-wide-dark.png',
  },
  {
    source: 'feedme-reset-badge.png',
    target: 'feedme-reset-badge-soft3d.png',
  },
  {
    source: 'feedme-reset-badge-dark.png',
    target: 'feedme-reset-badge-soft3d-dark.png',
  },
  {
    source: 'feedme-reset-badge.png',
    target: 'feedme-reset-badge-wide.png',
  },
  {
    source: 'feedme-reset-badge-dark.png',
    target: 'feedme-reset-badge-wide-dark.png',
  },
];

for (const asset of assets) {
  const bytes = await readFile(path.join(webRoot, 'public/email', asset.source));
  const { error } = await supabase.storage
    .from('email-assets')
    .upload(asset.target, bytes, {
      cacheControl: '60',
      contentType: 'image/png',
      upsert: true,
    });

  if (error) throw error;
  console.log(`Uploaded ${asset.target}`);
}
