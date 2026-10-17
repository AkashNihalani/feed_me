/* ─────────────────────────────────────────────────────────────
   LAUNCH — the app's first screen being ready. The launch veil
   (components/shell/LaunchVeil) covers a cold start, the first
   one after signing up or in (the device has never had a first
   screen since: LAUNCHED_KEY, taken away with what is kept), and
   lifts once the tab it opened on says its first screen is in
   (announceFirstScreen: Feed's first posts, Lead's board, Read's
   index) and the other tabs are warm, or after its cap.

   LAUNCH_SCRIPT runs before the first paint (an inline script in
   the root layout), so the veil is there from the first frame,
   never flashing in late over a page already drawn.
   ───────────────────────────────────────────────────────────── */

export const LAUNCH_CLASS = 'fm-launch';
export const LAUNCH_EVENT = 'fm:first-screen';
// set once the app has had its first screen on this device (and taken away with what is kept: sign-out, another
// account): not there means a cold start
export const LAUNCHED_KEY = 'fm:launched:v1';
// what Feed keeps on the device (feedStream/keep): a device with any of it has launched before, whatever the marker
const KEPT_PREFIX = 'feed:stream:keep:v1:';
const TAB_PATHS = ['/', '/lead', '/read'];

let firstScreen = false;
// the veil: up (a cold start's, from the first frame), leaving (its sheet going up), or none
let veil: 'up' | 'leaving' | 'none' = 'none';
export const LAUNCH_LEAVE_EVENT = 'fm:launch-leave';

export function setLaunchVeil(state: 'up' | 'leaving' | 'none') {
  if (veil === state) return;
  veil = state;
  if (state === 'leaving' && typeof window !== 'undefined') window.dispatchEvent(new Event(LAUNCH_LEAVE_EVENT));
}

/* the first screen, in, waits under the veil and comes in as it goes (FeedStream's arrival): the entrance is the
   launch's last beat, seen, not played under it */
export function launchHolds(): boolean {
  if (typeof document === 'undefined') return false;
  return veil !== 'leaving' && document.documentElement.classList.contains(LAUNCH_CLASS);
}

// a tab's first screen is in (once is enough: what comes after isn't the launch). The device has launched
export function announceFirstScreen() {
  if (firstScreen || typeof window === 'undefined') return;
  firstScreen = true;
  try {
    window.localStorage.setItem(LAUNCHED_KEY, '1');
  } catch {
    // storage unavailable
  }
  window.dispatchEvent(new Event(LAUNCH_EVENT));
}

// the device launched again from nothing (sign-out, another account): the next first screen is a cold start's
export function forgetLaunched() {
  try {
    window.localStorage.removeItem(LAUNCHED_KEY);
  } catch {
    // storage unavailable
  }
}

export function firstScreenShown() {
  return firstScreen;
}

// the app has never had its first screen on this device (since the last sign-out): it starts cold
export function startsCold(): boolean {
  try {
    const store = window.localStorage;
    if (store.getItem(LAUNCHED_KEY) !== null) return false;
    for (let at = 0; at < store.length; at += 1) {
      if (store.key(at)?.startsWith(KEPT_PREFIX)) return false;
    }
    return true;
  } catch {
    return false;
  }
}

export const LAUNCH_SCRIPT = `(function(){try{var p=location.pathname;if(${JSON.stringify(TAB_PATHS)}.indexOf(p)<0)return;if(localStorage.getItem(${JSON.stringify(LAUNCHED_KEY)})!==null)return;for(var i=0;i<localStorage.length;i++){var k=localStorage.key(i);if(k&&k.indexOf(${JSON.stringify(KEPT_PREFIX)})===0)return}document.documentElement.classList.add(${JSON.stringify(LAUNCH_CLASS)})}catch(e){}})();`;
