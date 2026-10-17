/* ─────────────────────────────────────────────────────────────
   KEEP STORE — what a tab has loaded, kept on the device between
   launches when it is too big for localStorage (Lead's board: every
   feed's posts), in IndexedDB: one key, one value, read once at
   start and written when a fresh load comes in. So the tab opens on
   what it showed last time, at once, and brings it up to date in
   the background.

   It is your data on your device: it goes with Feed's (keep.ts,
   clearKept) whenever you sign out, the sign-in page is shown, or
   the server turns the session away. forgetKept() also tells the
   tabs that hold a copy in memory (onForget).
   ───────────────────────────────────────────────────────────── */

const DB_NAME = 'fm-keep';
const STORE = 'kv';

let opening: Promise<IDBDatabase | null> | null = null;

function db(): Promise<IDBDatabase | null> {
  if (opening) return opening;
  opening = new Promise((resolve) => {
    try {
      if (typeof indexedDB === 'undefined') {
        resolve(null);
        return;
      }
      const request = indexedDB.open(DB_NAME, 1);
      request.onupgradeneeded = () => {
        if (!request.result.objectStoreNames.contains(STORE)) request.result.createObjectStore(STORE);
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => resolve(null);
      request.onblocked = () => resolve(null);
    } catch {
      resolve(null);
    }
  });
  return opening;
}

function run<T>(mode: IDBTransactionMode, work: (store: IDBObjectStore) => IDBRequest<T> | null): Promise<T | null> {
  return db().then((database) => new Promise<T | null>((resolve) => {
    if (!database) {
      resolve(null);
      return;
    }
    try {
      const transaction = database.transaction(STORE, mode);
      // (a transaction that ends without its request answering: nothing)
      transaction.onabort = () => resolve(null);
      transaction.onerror = () => resolve(null);
      const request = work(transaction.objectStore(STORE));
      if (!request) {
        resolve(null);
        return;
      }
      request.onsuccess = () => resolve((request.result as T | undefined) ?? null);
      request.onerror = () => resolve(null);
    } catch {
      resolve(null);
    }
  }));
}

export function readKeptValue<T>(key: string): Promise<T | null> {
  return run<T>('readonly', (store) => store.get(key) as IDBRequest<T>);
}

export function writeKeptValue(key: string, value: unknown): Promise<void> {
  return run('readwrite', (store) => store.put(value, key)).then(() => undefined);
}

const forgetters = new Set<() => void>();

// a tab's copy in memory goes with the device's (sign-out, another account)
export function onForget(forget: () => void): () => void {
  forgetters.add(forget);
  return () => {
    forgetters.delete(forget);
  };
}

export function forgetKept() {
  forgetters.forEach((forget) => forget());
  void run('readwrite', (store) => store.clear());
}
