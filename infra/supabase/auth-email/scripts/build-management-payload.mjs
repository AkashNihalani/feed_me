import { readFile, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const templateDir = path.resolve(__dirname, '..');

const templates = [
  {
    file: 'confirm-signup.html',
    subject: 'Confirm your FeedMe account',
    subjectKey: 'mailer_subjects_confirmation',
    contentKey: 'mailer_templates_confirmation_content',
  },
  {
    file: 'reset-password.html',
    subject: 'Reset your FeedMe password',
    subjectKey: 'mailer_subjects_recovery',
    contentKey: 'mailer_templates_recovery_content',
  },
  {
    file: 'invite-user.html',
    subject: 'Join FeedMe',
    subjectKey: 'mailer_subjects_invite',
    contentKey: 'mailer_templates_invite_content',
  },
  {
    file: 'change-email.html',
    subject: 'Confirm your new FeedMe email',
    subjectKey: 'mailer_subjects_email_change',
    contentKey: 'mailer_templates_email_change_content',
  },
  {
    file: 'magic-link.html',
    subject: 'Sign in to FeedMe',
    subjectKey: 'mailer_subjects_magic_link',
    contentKey: 'mailer_templates_magic_link_content',
  },
  {
    file: 'reauthentication.html',
    subject: 'Confirm this FeedMe action',
    subjectKey: 'mailer_subjects_reauthentication',
    contentKey: 'mailer_templates_reauthentication_content',
  },
];

async function buildPayload() {
  const payload = {};

  for (const template of templates) {
    const html = await readFile(path.join(templateDir, template.file), 'utf8');
    payload[template.subjectKey] = template.subject;
    payload[template.contentKey] = html;
  }

  return payload;
}

const payload = await buildPayload();
const json = `${JSON.stringify(payload, null, 2)}\n`;
const writeIndex = process.argv.indexOf('--write');

if (writeIndex !== -1) {
  const outPath = process.argv[writeIndex + 1];
  if (!outPath) {
    throw new Error('Missing output path after --write');
  }
  await writeFile(path.resolve(process.cwd(), outPath), json);
} else {
  process.stdout.write(json);
}
