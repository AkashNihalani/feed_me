const LOCAL_FALLBACK_URL = 'http://localhost:3000';

function normalizeUrl(value?: string | null): string | null {
  const trimmed = value?.trim();
  if (!trimmed) return null;
  return trimmed.replace(/\/$/, '');
}

export function getSiteUrl(): string {
  const explicitSiteUrl = normalizeUrl(process.env.NEXT_PUBLIC_SITE_URL);
  if (explicitSiteUrl) return explicitSiteUrl;

  const explicitBaseUrl = normalizeUrl(process.env.NEXT_PUBLIC_BASE_URL);
  if (explicitBaseUrl) return explicitBaseUrl;

  const vercelUrl = normalizeUrl(process.env.VERCEL_URL);
  if (vercelUrl) {
    return vercelUrl.startsWith('http') ? vercelUrl : `https://${vercelUrl}`;
  }

  return LOCAL_FALLBACK_URL;
}
