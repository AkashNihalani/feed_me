/* The theme's constants and the script layout.tsx inlines before first paint (no React here, so the server layout
   can import it). lib/theme.ts reads the same key and resolves auto the same way. */

export type ThemePreference = 'light' | 'dark' | 'auto';
export type ResolvedTheme = 'light' | 'dark';

export const THEME_STORAGE_KEY = 'theme';
export const DEFAULT_THEME: ThemePreference = 'light';
export const DARK_QUERY = '(prefers-color-scheme: dark)';
// what the browser's own chrome (Safari's bars, the PWA's status area) is tinted to: each theme's page colour
export const THEME_COLORS: Record<ResolvedTheme, string> = { light: '#f4f7f9', dark: '#030303' };

export const THEME_BOOTSTRAP_SCRIPT = `
(() => {
  const root = document.documentElement;
  let theme = ${JSON.stringify(DEFAULT_THEME)};
  try {
    const saved = localStorage.getItem(${JSON.stringify(THEME_STORAGE_KEY)});
    const pref = saved === 'light' || saved === 'dark' || saved === 'auto' ? saved : ${JSON.stringify(DEFAULT_THEME)};
    theme = pref === 'auto' ? (window.matchMedia && window.matchMedia(${JSON.stringify(DARK_QUERY)}).matches ? 'dark' : 'light') : pref;
  } catch {}
  root.classList.toggle('dark', theme === 'dark');
  root.classList.toggle('light', theme !== 'dark');
  root.style.colorScheme = theme === 'dark' ? 'dark' : 'light';
  const color = theme === 'dark' ? ${JSON.stringify(THEME_COLORS.dark)} : ${JSON.stringify(THEME_COLORS.light)};
  document.querySelectorAll('meta[name="theme-color"]').forEach((meta) => meta.setAttribute('content', color));
})();
`;
