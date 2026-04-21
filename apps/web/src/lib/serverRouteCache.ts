type CacheEntry = {
  expiresAt: number;
  value: unknown;
};

const routeCache = new Map<string, CacheEntry>();
const inflightCache = new Map<string, Promise<unknown>>();

function sweepExpiredEntries(now = Date.now()) {
  for (const [key, entry] of routeCache.entries()) {
    if (entry.expiresAt <= now) {
      routeCache.delete(key);
    }
  }
}

export function readServerRouteCache<T>(key: string): T | null {
  const entry = routeCache.get(key);
  if (!entry) return null;
  if (entry.expiresAt <= Date.now()) {
    routeCache.delete(key);
    return null;
  }
  return entry.value as T;
}

export function writeServerRouteCache<T>(key: string, ttlMs: number, value: T): T {
  routeCache.set(key, {
    value,
    expiresAt: Date.now() + Math.max(1, ttlMs),
  });
  return value;
}

export async function withServerRouteCache<T>(
  key: string,
  ttlMs: number,
  loader: () => Promise<T>,
): Promise<T> {
  const cached = readServerRouteCache<T>(key);
  if (cached !== null) return cached;

  const inflight = inflightCache.get(key);
  if (inflight) return inflight as Promise<T>;

  const request = loader()
    .then((value) => writeServerRouteCache(key, ttlMs, value))
    .finally(() => {
      inflightCache.delete(key);
      if (routeCache.size > 250) {
        sweepExpiredEntries();
      }
    });

  inflightCache.set(key, request);
  return request;
}

export function invalidateServerRouteCacheByPrefix(prefix: string) {
  for (const key of routeCache.keys()) {
    if (key.startsWith(prefix)) routeCache.delete(key);
  }
  for (const key of inflightCache.keys()) {
    if (key.startsWith(prefix)) inflightCache.delete(key);
  }
}
