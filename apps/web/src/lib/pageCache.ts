type CacheEntry<T> = {
  ts: number;
  data: T;
};

const memoryCache = new Map<string, CacheEntry<unknown>>();

function readStorage<T>(key: string): CacheEntry<T> | null {
  try {
    const raw = sessionStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as CacheEntry<T>;
    if (!parsed || typeof parsed.ts !== 'number') return null;
    return parsed;
  } catch {
    return null;
  }
}

export function readCache<T>(key: string): CacheEntry<T> | null {
  const inMem = memoryCache.get(key) as CacheEntry<T> | undefined;
  if (inMem) return inMem;
  const stored = readStorage<T>(key);
  if (stored) memoryCache.set(key, stored as CacheEntry<unknown>);
  return stored;
}

export function getCache<T>(key: string, maxAgeMs: number): T | null {
  const entry = readCache<T>(key);
  if (!entry) return null;
  if (Date.now() - entry.ts > maxAgeMs) return null;
  return entry.data;
}

export function setCache<T>(key: string, data: T) {
  const entry: CacheEntry<T> = { ts: Date.now(), data };
  memoryCache.set(key, entry as CacheEntry<unknown>);
  try {
    sessionStorage.setItem(key, JSON.stringify(entry));
  } catch {
    // ignore storage failures
  }
}

export function removeCache(key: string) {
  memoryCache.delete(key);
  try {
    sessionStorage.removeItem(key);
  } catch {
    // ignore storage failures
  }
}

export function clearCacheByPrefix(prefix: string) {
  for (const key of Array.from(memoryCache.keys())) {
    if (key.startsWith(prefix)) memoryCache.delete(key);
  }

  try {
    for (let index = sessionStorage.length - 1; index >= 0; index -= 1) {
      const key = sessionStorage.key(index);
      if (key?.startsWith(prefix)) sessionStorage.removeItem(key);
    }
  } catch {
    // ignore storage failures
  }
}
