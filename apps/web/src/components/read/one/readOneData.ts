/* ─────────────────────────────────────────────────────────────
   READ ONE's data, apart from its engine: which feeders are read as
   one page, and each read, fetched once a session from the app's own
   API (signed in only) and kept while the tab is hidden.
   ───────────────────────────────────────────────────────────── */

const READ_ONE = new Set(['anuj.mp4']);
export const readOneOf = (handle: string | null | undefined) => (handle && READ_ONE.has(handle) ? handle : null);

export type ReadOneData = { drop: Record<string, unknown>; mortems: Record<string, unknown> };

const reads: Record<string, Promise<ReadOneData>> = {};
export const loadedReadOnes: Record<string, ReadOneData> = {};
export function loadReadOne(handle: string) {
  reads[handle] ??= fetch(`/api/read/one?handle=${encodeURIComponent(handle)}`)
    .then((response) => {
      if (!response.ok) throw new Error(`read one ${handle}: ${response.status}`);
      return response.json() as Promise<ReadOneData>;
    })
    .then((read) => {
      loadedReadOnes[handle] = read;
      return read;
    });
  // (a failed fetch is forgotten, so the next visit asks again)
  reads[handle].catch(() => { delete reads[handle]; });
  return reads[handle];
}

// where each feeder's page was left (the engine's own note), so the tab comes back to it
export type ReadOneKeep = { was?: unknown };
const kept: Record<string, ReadOneKeep> = {};
export const keepOf = (handle: string) => (kept[handle] ??= {});

// what the page tells the app's chrome
export type ReadOneChrome = { file: { id: string; name: string } | null; win: number; away: boolean; art: string | null };
export const READ_ONE_FRAMES = [7, 30, 90] as const;
