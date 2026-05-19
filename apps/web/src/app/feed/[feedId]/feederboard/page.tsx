import FeederboardClient from '@/components/feed/FeederboardClient';

export default async function FeedFeederboardPage({
  params,
  searchParams,
}: {
  params: Promise<{ feedId: string }>;
  searchParams?: Promise<{ handle?: string | string[] }>;
}) {
  const { feedId } = await params;
  const query = searchParams ? await searchParams : {};
  const rawHandle = Array.isArray(query.handle) ? query.handle[0] : query.handle;
  return <FeederboardClient feedId={feedId} selectedHandle={rawHandle || 'all'} />;
}
