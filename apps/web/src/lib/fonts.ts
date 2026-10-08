import { Space_Grotesk } from 'next/font/google';

/* The app's one typeface, self-hosted by next/font and declared once (the root layout puts its variable on <html>;
   Read's wrappers use the same instance). Space Grotesk ships 300–700, so font-black renders at 700. */
export const appFont = Space_Grotesk({
  subsets: ['latin'],
  weight: ['400', '500', '600', '700'],
  variable: '--fm-font',
  display: 'swap',
});
