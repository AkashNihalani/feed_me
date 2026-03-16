'use client';

import { FireItem } from './types';
import { FireCard3D } from './FireCard3D';

export type SnapFeedProps = {
  cards: FireItem[];
};

export default function SnapFeed({ cards }: SnapFeedProps) {
  if (cards.length === 0) {
    return (
      <div className="h-full w-full flex items-center justify-center text-gray-500 font-mono tracking-widest text-sm uppercase">
        No Signals Detected
      </div>
    );
  }

  return (
    <div 
      className="h-full w-full overflow-y-auto snap-y snap-mandatory scroll-smooth hide-scrollbar bg-[#050505]"
    >
      <div className="flex flex-col w-full pb-32">
        {cards.map((card, idx) => (
          <div 
            key={card.id || idx} 
            className="w-full shrink-0 flex items-center justify-center p-4 min-h-[75dvh] snap-center"
          >
            <div className="w-full max-w-lg">
               <FireCard3D item={card} />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}
