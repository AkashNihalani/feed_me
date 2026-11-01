/* ─────────────────────────────────────────────────────────────
   READ 17's data, apart from its engine: which feeders have a read,
   and each read's file, fetched once a session and shared by the
   Read tab (Read17) and Feed's story (FeederStory), so Feed can ask
   for a read without loading the page's engine or its styles.
   ───────────────────────────────────────────────────────────── */

export const READ17_BASE = '/read17/';

// the feeders read this way: handle → its read's file, and the face its story wears
const READ17: Record<string, { acct: string; avatar: string }> = {
  'anuj.mp4': { acct: 'anuj', avatar: '/read-preview/anuj-run3/avatar.jpg' },
  lakmeindia: { acct: 'lakme', avatar: '/read-preview/lakme-run3/avatar.jpg' },
};
export const read17Of = (handle: string | null | undefined) => (handle ? READ17[handle]?.acct ?? null : null);
export const read17Avatar = (handle: string | null | undefined) => (handle ? READ17[handle]?.avatar ?? null : null);

export type Read = Record<string, unknown>;
const reads: Record<string, Promise<Read>> = {};
export const loadedReads: Record<string, Read> = {};
export function loadRead(acct: string) {
  reads[acct] ??= fetch(`${READ17_BASE}${acct}.json`)
    .then((response) => {
      if (!response.ok) throw new Error(`read17 ${acct}: ${response.status}`);
      return response.json() as Promise<Read>;
    })
    .then((read) => {
      loadedReads[acct] = read;
      return read;
    });
  // (a failed fetch is forgotten, so the next visit asks again)
  reads[acct].catch(() => { delete reads[acct]; });
  return reads[acct];
}

// a read's name for "watched": its number and its day, so the next read's story rings again
export function readId(read: Read | null | undefined): string | null {
  const voice = read?.voice as { read?: { n?: number; d?: string } } | undefined;
  return voice?.read?.n != null ? `${voice.read.n}:${voice.read.d ?? ''}` : null;
}
