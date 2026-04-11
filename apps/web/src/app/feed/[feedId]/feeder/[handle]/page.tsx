import FeederPostsClient from '@/components/feed/FeederPostsClient';

export default async function FeedFeederPage({
  params,
}: {
  params: Promise<{ feedId: string; handle: string }>;
}) {
  const { feedId, handle } = await params;
  return <FeederPostsClient feedId={feedId} handle={handle} />;
}
