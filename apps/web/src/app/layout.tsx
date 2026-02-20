import type { Metadata, Viewport } from "next";
import "./globals.css";
import TopNav from "@/components/TopNav";
import { StatusBar } from "@/components/StatusBar";
import BottomNav from "@/components/BottomNav";

export const metadata: Metadata = {
  title: "FeedMe | Social Intelligence",
  description: "FeedMe — linked sheets, handle tracking, and social intelligence.",
  manifest: "/manifest.json",
  icons: {
    icon: [
      { url: "/icons/icon-192.png", sizes: "192x192", type: "image/png" },
      { url: "/icons/icon-512.png", sizes: "512x512", type: "image/png" }
    ],
    apple: [
      { url: "/apple-touch-icon.png", sizes: "180x180", type: "image/png" }
    ],
  },
  appleWebApp: {
    capable: true,
    title: "FeedMe",
    statusBarStyle: "black",
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
    <html lang="en" className="h-full w-full overflow-hidden">
      <body className="antialiased bg-background h-full w-full overflow-hidden transition-colors duration-300">
        <main className="h-full w-full overflow-hidden">{children}</main>
        <BottomNav />
        <StatusBar />
      </body>
    </html>
  );
}
