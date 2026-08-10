import type { Metadata } from 'next';
import type { ReactNode } from 'react';

export const metadata: Metadata = {
  title: 'Recent',
};

export default function FireLayout({ children }: { children: ReactNode }) {
  return children;
}
