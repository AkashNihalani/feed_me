import { redirect } from 'next/navigation';

// The Feed tab lives at the root (AppTabHost); /feed is kept as an address people type and old links use
export default async function FeedAlias({ searchParams }: { searchParams: Promise<Record<string, string | string[] | undefined>> }) {
  const params = new URLSearchParams();
  Object.entries(await searchParams).forEach(([key, value]) => {
    (Array.isArray(value) ? value : value != null ? [value] : []).forEach((item) => params.append(key, item));
  });
  const query = params.toString();
  redirect(query ? `/?${query}` : '/');
}
