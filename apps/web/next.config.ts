import path from "node:path";
import type { NextConfig } from "next";

const nextConfig: NextConfig = {
  reactStrictMode: true,
  // the dev server opened from a phone on the same Wi-Fi (this Mac's address on the network)
  allowedDevOrigins: ["192.168.0.104", "192.168.0.114"],
  turbopack: {
    root: path.join(__dirname),
  },
};

export default nextConfig;
