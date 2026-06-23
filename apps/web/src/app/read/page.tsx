import FeederReaderCover from '@/components/feed/FeederReaderCover';
import FeederReaderPage from '@/components/feed/FeederReaderPage';

// Preview-only: the cover (dashboard scroll-stopper) on top, then the page it
// opens into. Token-driven for light/dark.
export default function ReadPreviewPage() {
  return (
    <main className="fm-dashboard-mesh h-full min-h-0 w-full max-w-[100dvw] overflow-y-auto overflow-x-hidden bg-background py-10 [-webkit-overflow-scrolling:touch] sm:py-14">
      <div className="mx-auto w-[min(760px,calc(100dvw-40px))] max-w-[calc(100dvw-40px)] sm:w-[min(760px,calc(100dvw-64px))] sm:max-w-[calc(100dvw-64px)]">
        <FeederReaderCover />
        <div className="my-10 text-center text-[10px] font-black uppercase tracking-[0.3em] text-foreground/25">
          — opens into —
        </div>
      </div>
      <div className="mx-auto w-[min(1240px,calc(100dvw-40px))] max-w-[calc(100dvw-40px)] sm:w-[min(1240px,calc(100dvw-64px))] sm:max-w-[calc(100dvw-64px)]">
        <FeederReaderPage />
      </div>
    </main>
  );
}
