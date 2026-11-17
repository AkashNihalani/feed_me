import type { Metadata } from 'next';
import { Suspense } from 'react';
import ReadNextPreview from '@/components/read/next/ReadNextPreview';

export const metadata: Metadata = { title: 'Read' };

/* The same page for review while signed out: the real side rail and dock, fed a few sample feeds (ReadNextPreview) */
export default function ReadNextPreviewPage() {
  return (
    <Suspense fallback={null}>
      <ReadNextPreview />
    </Suspense>
  );
}
