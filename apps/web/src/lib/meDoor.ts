/* The way to Me and back: the tab you came from, kept so the page it opened over is the page it goes back to. */

export type MainTab = 'lead' | 'feed' | 'read';

export function isMainTab(value: string | null): value is MainTab {
  return value === 'lead' || value === 'feed' || value === 'read';
}

export function openMe() {
  if (window.location.pathname === '/me') return;
  try {
    sessionStorage.setItem('feedme:intent', '/me');
  } catch {
    // storage blocked
  }
  window.history.pushState(null, '', '/me');
}
