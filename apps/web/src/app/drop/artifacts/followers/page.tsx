import type { Metadata } from 'next';
import Followers from './Followers';
import { FOLLOWER_RUN_WINDOWS } from './fixture';

export const metadata: Metadata = {
  title: 'Follower Run · Feed Me',
  description: 'Three comparable follower windows reveal how account growth accelerated.',
};

export default function FollowersPage() {
  return <Followers windows={FOLLOWER_RUN_WINDOWS} />;
}
