'use client';

import { ArrowLeft, BrainCircuit } from 'lucide-react';
import { useRouter } from 'next/navigation';
import FeederPoolHero from './FeederPoolHero';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';

type FeederboardClientProps = {
  feedId: string;
  selectedHandle?: string;
};

export default function FeederboardClient({
  feedId,
  selectedHandle = 'all',
}: FeederboardClientProps) {
  const router = useRouter();
  const { appShellStyle, isStandaloneMode, useTranslucentBrowserChrome } = useMobileImmersiveViewport();
  const bottomClearance = useTranslucentBrowserChrome
    ? 'calc(20px + env(safe-area-inset-bottom))'
    : isStandaloneMode
      ? 'calc(132px + env(safe-area-inset-bottom))'
      : 'calc(96px + env(safe-area-inset-bottom))';

  return (
    <div
      className="relative min-h-[100dvh] overflow-x-hidden overflow-y-auto bg-background text-foreground"
      style={{ ...appShellStyle, paddingBottom: bottomClearance }}
    >
      <div className="pointer-events-none fixed inset-0 z-0 bg-white dark:bg-[#030303]" />

      <main className="relative z-10 mx-auto flex w-full max-w-[1540px] flex-col px-3 pb-10 pt-[calc(16px+env(safe-area-inset-top)+var(--pwa-top-fix,0px))] sm:px-4 lg:px-6">
        <div className="flex items-center gap-3">
          <button
            type="button"
            onClick={() => router.push(`/?id=${feedId}`, { scroll: false })}
            className="flex h-10 w-10 shrink-0 items-center justify-center rounded-[14px] fm-depth-chip text-foreground/56 dark:text-white/50"
            aria-label="Back to feed dashboard"
          >
            <ArrowLeft size={20} strokeWidth={2.6} />
          </button>

          <div className="flex h-11 w-11 shrink-0 items-center justify-center rounded-[14px] border border-[#E11D48]/18 bg-[#E11D48]/10 text-[#BE123C] shadow-[inset_0_1px_0_rgba(255,255,255,0.62)] dark:border-[#FB7185]/20 dark:bg-[#E11D48]/16 dark:text-white">
            <BrainCircuit size={20} strokeWidth={2.6} />
          </div>

          <div className="min-w-0 flex-1">
            <h1 className="truncate text-[24px] font-black leading-none tracking-normal text-black dark:text-white sm:text-[30px]">
              Feederboard
            </h1>
            <div className="mt-1 text-[10px] font-black uppercase tracking-[0.12em] text-foreground/40 dark:text-white/36">
              Content intelligence
            </div>
          </div>
        </div>

        <div className="mt-4 sm:mt-5">
          <FeederPoolHero selectedHandle={selectedHandle} />
        </div>
      </main>
    </div>
  );
}
