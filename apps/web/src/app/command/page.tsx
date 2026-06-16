import type { Metadata } from 'next';
import CommandHubClient from './CommandHubClient';
import './command.css';

export const metadata: Metadata = {
  title: 'Command Hub',
  description: 'Read-only FeedMe command hub.',
};

export default function CommandPage() {
  return <CommandHubClient />;
}
