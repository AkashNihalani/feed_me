'use client';

import { ReactNode } from 'react';

export default function RouteFade({ children }: { children: ReactNode }) {
  return <div className="h-full min-h-[100dvh] w-full">{children}</div>;
}
