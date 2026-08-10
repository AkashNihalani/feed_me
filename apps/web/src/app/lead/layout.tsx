import type { Metadata } from 'next';
import type { ReactNode } from 'react';

export const metadata: Metadata = {
  title: 'Lead',
};

export default function LeadLayout({ children }: { children: ReactNode }) {
  return children;
}
