/* Instagram CDN links carry their own expiry: `oe`, hex unix seconds. Past it the CDN answers 403, so a link that
   has run out is not worth a request (the proxy's, or the browser's). A link without one is given the benefit of the
   doubt. The margin keeps a link that is about to lapse from being handed to a page that may sit open a while. */

const EXPIRY_MARGIN_MS = 10 * 60 * 1000;

export function instagramLinkExpiresAt(url: string | null | undefined): number | null {
  if (!url) return null;
  try {
    const oe = new URL(url).searchParams.get('oe');
    if (!oe || !/^[0-9a-f]{6,10}$/i.test(oe)) return null;
    return parseInt(oe, 16) * 1000;
  } catch {
    return null;
  }
}

export function instagramLinkLive(url: string | null | undefined, nowMs: number = Date.now()): boolean {
  if (!url || !url.trim()) return false;
  const expiresAt = instagramLinkExpiresAt(url);
  return expiresAt == null || expiresAt > nowMs + EXPIRY_MARGIN_MS;
}
