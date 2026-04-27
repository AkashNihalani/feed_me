import { readFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const projectRef = process.env.SUPABASE_PROJECT_REF || 'worqtdkvicuhmdgoncru';
const accessToken = process.env.SUPABASE_ACCESS_TOKEN;

if (!accessToken) {
  throw new Error('Set SUPABASE_ACCESS_TOKEN before pushing auth email templates.');
}

const payloadPath = process.argv[2]
  ? path.resolve(process.cwd(), process.argv[2])
  : path.join(__dirname, '..', 'management-payload.json');

const payload = JSON.parse(await readFile(payloadPath, 'utf8'));
const response = await fetch(`https://api.supabase.com/v1/projects/${projectRef}/config/auth`, {
  method: 'PATCH',
  headers: {
    Authorization: `Bearer ${accessToken}`,
    'Content-Type': 'application/json',
  },
  body: JSON.stringify(payload),
});

const text = await response.text();

if (!response.ok) {
  throw new Error(`Supabase auth config update failed (${response.status}): ${text}`);
}

process.stdout.write(`Updated Supabase auth email templates for ${projectRef}.\n`);
