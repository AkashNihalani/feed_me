/* The app's single-key shortcuts (the rail's F · L · R, Feed's zooms, the time range's < and >) stand down while you
   type, and under a dialog that is on screen. */

// a dialog actually on screen (Read keeps its closed sheet in the page, marked modal, under an aria-hidden layer)
export function aModalIsOpen() {
  return Array.from(document.querySelectorAll<HTMLElement>('[aria-modal="true"]')).some((el) => {
    if (el.closest('[aria-hidden="true"], [inert]') || !el.getClientRects().length) return false;
    const style = getComputedStyle(el);
    return style.visibility !== 'hidden' && style.opacity !== '0';
  });
}

// a key meant for a field being typed in
export function typingIn(target: EventTarget | null) {
  const el = target as HTMLElement | null;
  return Boolean(el && (el.isContentEditable || /^(INPUT|TEXTAREA|SELECT)$/.test(el.tagName)));
}
