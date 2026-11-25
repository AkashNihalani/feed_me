/* Me's posts: what was kept on the device last time, at once, then the fresh ones (one fetch at a time, again when what
   is on hand is ten minutes old). Forgotten with everything kept (keepStore). */

import type { MeData } from '@/app/api/me/route';
import { redirectToLoginOnce } from '@/lib/feedsStore';
import { onForget, readKeptValue, writeKeptValue } from '@/lib/keepStore';

const KEY = 'me:posts:v1';
const FRESH_MS = 10 * 60 * 1000;

let now: MeData | null = null;
let at = 0;
let asking: Promise<MeData> | null = null;

onForget(() => {
  now = null;
  at = 0;
});

export const meOnHand = () => now;

// what the device kept, unless something fresher came first
export async function keptMe(): Promise<MeData | null> {
  if (now) return now;
  const kept = await readKeptValue<MeData>(KEY);
  if (!now && kept && Array.isArray(kept.p) && Array.isArray(kept.h)) now = kept;
  return now;
}

export function loadMe(): Promise<MeData> {
  if (now && Date.now() - at < FRESH_MS) return Promise.resolve(now);
  asking ??= fetch('/api/me')
    .then((response) => {
      if (response.status === 401) redirectToLoginOnce();
      if (!response.ok) throw new Error(`me: ${response.status}`);
      return response.json() as Promise<MeData>;
    })
    .then((data) => {
      now = data;
      at = Date.now();
      void writeKeptValue(KEY, data);
      return data;
    })
    .finally(() => {
      asking = null;
    });
  return asking;
}

// feeders changed (Manage): the next look asks again
export function staleMe() {
  at = 0;
}
