import type { Metadata } from 'next';
import type { ReactNode } from 'react';

export const metadata: Metadata = {
  title: 'Me',
};

export default function MeLayout({ children }: { children: ReactNode }) {
  return children;
}
