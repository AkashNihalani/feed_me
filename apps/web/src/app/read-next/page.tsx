import type { Metadata } from 'next';
import { Suspense } from 'react';
import ReadNext from '@/components/read/next/ReadNext';

export const metadata: Metadata = { title: 'Read' };

/* Read as Ask · Files · Feed (components/read/next), inside the app's own chrome: the side rail from a laptop up, the
   dock on a phone, both drawing the Read tab's header with your live feeds */
export default function ReadNextPage() {
  return (
    <Suspense fallback={null}>
      <ReadNext />
    </Suspense>
  );
}
