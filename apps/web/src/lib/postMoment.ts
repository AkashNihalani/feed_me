/* When a post went out, to the minute, and where that falls on the app's clock (IST).

   Since Aug 2026 the scraper hands back only a post's date, stored as 00:00 UTC (and dated in US Pacific, so some
   posts sit a day early). Every Instagram shortcode carries its media id, and the id carries the moment the post
   was created (milliseconds since Instagram's epoch, above bit 23): within a minute of going live for most posts,
   earlier for scheduled ones. A stored time that is real wins; a bare date gives way to the shortcode. */

const ALPHABET = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_';
const INSTAGRAM_EPOCH_MS = 1314220021721;
const IST_OFFSET_MS = 330 * 60 * 1000;

export function shortcodeOf(postUrl: string | null | undefined) {
  return /\/(?:p|reel|tv)\/([A-Za-z0-9_-]+)/i.exec(postUrl || '')?.[1] || '';
}

export function shortcodeMoment(shortcode: string) {
  const code = shortcode.slice(0, 11);
  if (code.length < 6) return null;
  let id = BigInt(0);
  for (const char of code) {
    const index = ALPHABET.indexOf(char);
    if (index < 0) return null;
    id = id * BigInt(64) + BigInt(index);
  }
  const ms = Number(id >> BigInt(23)) + INSTAGRAM_EPOCH_MS;
  return ms > Date.UTC(2012, 0, 1) && ms < Date.now() + 86_400_000 ? ms : null;
}

const isBareDate = (iso: string) => /T00:00:00(?:\.0+)?(?:Z|\+00:00)$/.test(iso) || /^\d{4}-\d{2}-\d{2}$/.test(iso);

/* ms since epoch, or null when neither the stored time nor the shortcode says */
export function postMoment(postedAt: string | null | undefined, postUrl: string | null | undefined) {
  const stored = Date.parse(postedAt || '');
  if (Number.isFinite(stored) && postedAt && !isBareDate(postedAt)) return stored;
  return shortcodeMoment(shortcodeOf(postUrl)) ?? (Number.isFinite(stored) ? stored : null);
}

/* the IST calendar: day key and weekday (0 = Monday) */
export function istDayKey(ms: number) {
  return new Date(ms + IST_OFFSET_MS).toISOString().slice(0, 10);
}

export function istWeekday(ms: number) {
  return (new Date(ms + IST_OFFSET_MS).getUTCDay() + 6) % 7;
}

/* midnight IST of a day key, as ms */
export function istDayStart(dayKey: string) {
  return Date.parse(`${dayKey}T00:00:00+05:30`);
}

export function addDays(dayKey: string, days: number) {
  return istDayKey(istDayStart(dayKey) + days * 86_400_000 + 3_600_000);
}
