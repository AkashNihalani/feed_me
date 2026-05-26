import FeederFileClient from '@/components/feed/FeederFileClient';

export default async function FeedFeederFilePage({
  params,
  searchParams,
}: {
  params: Promise<{ feedId: string }>;
  searchParams?: Promise<{ handle?: string | string[] }>;
}) {
  const { feedId } = await params;
  const query = searchParams ? await searchParams : {};
  const rawHandle = Array.isArray(query.handle) ? query.handle[0] : query.handle;
  return <FeederFileClient feedId={feedId} selectedHandle={rawHandle || 'all'} />;
}
