export function getVisualViewportEventTarget(): VisualViewport | null {
  if (typeof window === 'undefined') return null;

  const viewport = window.visualViewport;
  if (!viewport) return null;

  return typeof viewport.addEventListener === 'function'
    && typeof viewport.removeEventListener === 'function'
    ? viewport
    : null;
}
