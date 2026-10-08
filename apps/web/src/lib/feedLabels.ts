/* How feeds and feeders are named on screen, the same on every tab. */

export function normalizeHandle(value: string | null | undefined) {
  return String(value || '').trim().replace(/^@+/, '').toLowerCase();
}

export function titleCase(value: string) {
  return value
    .toLowerCase()
    .replace(/(^|\s|[-_/&])\w/g, (character) => character.toUpperCase());
}

// a feed's badge on the story rail: up to three initials ("Creators" → CRE, "Food and Drink" → FAD)
export function feedInitials(value: string) {
  const words = value.replace(/[^a-zA-Z0-9 ]/g, ' ').trim().split(/\s+/).filter(Boolean);
  if (words.length > 1) return words.slice(0, 3).map((word) => word[0]).join('').toUpperCase();
  return (words[0] || 'ALL').slice(0, 3).toUpperCase();
}
