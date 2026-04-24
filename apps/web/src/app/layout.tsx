import type { Metadata, Viewport } from "next";
import "./globals.css";
import { StatusBar } from "@/components/StatusBar";
import AppRouteTransition from "@/components/AppRouteTransition";
import BottomNav from "@/components/BottomNav";
import PwaNotificationsBridge from "@/components/PwaNotificationsBridge";
import { getSiteUrl } from "@/lib/site-url";

const metadataBase = new URL(getSiteUrl());
const themeBootstrapScript = `
(() => {
  try {
    const savedTheme = localStorage.getItem('theme');
    const isDark = savedTheme !== 'light';
    const root = document.documentElement;
    root.classList.toggle('dark', isDark);
    root.classList.toggle('light', !isDark);
    root.style.colorScheme = isDark ? 'dark' : 'light';
  } catch {
    const root = document.documentElement;
    root.classList.add('dark');
    root.classList.remove('light');
    root.style.colorScheme = 'dark';
  }
})();
`;

export const metadata: Metadata = {
  metadataBase,
  title: {
    default: "FeedMe | Social Intelligence",
    template: "%s | FeedMe"
  },
  description: "FeedMe — linked sheets, handle tracking, and social intelligence.",
  applicationName: "FeedMe",
  manifest: "/manifest.json",
  icons: {
    shortcut: ["/favicon.ico"],
    icon: [
      { url: "/icon.svg", type: "image/svg+xml" },
      { url: "/favicon.ico", type: "image/x-icon" },
      { url: "/favicon-16x16.png", sizes: "16x16", type: "image/png" },
      { url: "/favicon-32x32.png", sizes: "32x32", type: "image/png" },
      { url: "/icons/icon-192.png", sizes: "192x192", type: "image/png" },
      { url: "/icons/icon-512.png", sizes: "512x512", type: "image/png" }
    ],
    apple: [
      { url: "/apple-touch-icon.png", sizes: "180x180", type: "image/png" }
    ],
  },
  openGraph: {
    title: "FeedMe",
    description: "FeedMe — social intelligence dashboard",
    siteName: "FeedMe",
    images: [
      {
        url: "/icons/icon-512.png",
        width: 512,
        height: 512,
        alt: "FeedMe Logo",
      }
    ],
    type: "website",
  },
  appleWebApp: {
    capable: true,
    title: "FeedMe",
    statusBarStyle: "black-translucent",
    startupImage: [
      { url: "/apple-icon.png", media: "(device-width: 390px) and (device-height: 844px) and (-webkit-device-pixel-ratio: 3)" }
    ]
  },
  other: {
    "apple-mobile-web-app-capable": "yes",
    "mobile-web-app-capable": "yes",
  },
};

export const viewport: Viewport = {
  width: "device-width",
  initialScale: 1,
  viewportFit: "cover",
  themeColor: "#000000",
};

export default function RootLayout({
  children,
}: Readonly<{
  children: React.ReactNode;
}>) {
  return (
    <html lang="en" suppressHydrationWarning className="h-full min-h-[100dvh] w-full overflow-x-hidden bg-background">
      <body className="antialiased bg-background h-full min-h-[100dvh] w-full overflow-hidden transition-colors duration-300">
        <script dangerouslySetInnerHTML={{ __html: themeBootstrapScript }} />
        <main className="h-full min-h-[100dvh] w-full overflow-hidden">
          <AppRouteTransition>{children}</AppRouteTransition>
        </main>
        <PwaNotificationsBridge />
        <BottomNav />
        <StatusBar />
      </body>
    </html>
  );
}
