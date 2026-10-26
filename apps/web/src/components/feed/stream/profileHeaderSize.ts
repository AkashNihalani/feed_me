/* ─────────────────────────────────────────────────────────────
   PROFILE HEADER SIZE — the page profile's rows, in px.

   ProfileHeader (variant 'page') draws exactly these rows, each at
   a fixed height, loaded or not, so its height is known from its
   width and scope before it renders: the layout can reserve it and
   nothing below moves when the numbers arrive.

   Narrow (a phone): face + stats · name (with the settings button) ·
   highlights · Read's door (the runs of 10) · Lead's door (its Hall
   of Feed win) · the order tabs, one under another. The doors are
   rows, not tiles, and as short as they read: on a phone's Day view
   the profile is one stop of the page's snap, so every px here is
   a px of the first card pushed down.
   Wide (≥ 720): the face beside the name, stats and the settings;
   then highlights; then the two doors side by side, as tiles; then
   the order tabs. Spread (≥ 860): two sides. Instagram's profile
   on the left (the face, the name, the numbers on one line, the
   bio; the highlights under it); the two doors as cards, one over
   the other, on the right, sharing the left side's height; then
   the order tabs.
   ───────────────────────────────────────────────────────────── */

import type { StreamScope } from '@/lib/feedStream/contract';

export const PROFILE_ROW = {
  pad: 8, // above the face
  gap: 14, // between rows (a phone)
  wideGap: 24, // between the wide layout's bands
  face: 120, // the 108px face in its ring
  stats: 60, // value · label · note
  statsPhone: 62, // a phone's, beside the face (20px numbers)
  title: 48, // the name and its line (a phone)
  titleWide: 56,
  today: 24, // all feeds: "Today · …", with the space above it (a phone)
  todayWide: 30,
  highlights: 92,
  read: 36, // Read's door on a phone: one line, the label beside the boxes
  doorGap: 8, // between the two doors (a phone): a pair
  lead: 48, // Lead's door on a phone: the post, its win in two lines
  doorsWide: 88, // the two doors side by side, as tiles
  profileTop: 160, // a spread top's profile: the face, the name, the numbers (two lines on a narrow one) and the bio
  profileGap: 16, // between it and its highlights
  doorsColumnMin: 380, // the doors' column on a spread top (its two cards share the top's height)
  doorsColumnMax: 460,
  fullCoverFrom: 420, // a doors' column this wide gives Lead's card a cover the card's height (narrower: 80px)
  bigFaceFrom: 1100, // a spread top this wide has the larger face
  tabs: 44,
} as const;

export const PROFILE_WIDE = 720;
export const PROFILE_SPREAD = 860;

export type ProfileFrame = {
  wide: boolean; // the face beside its name
  spread: boolean; // the top across the column: the stats beside the name
  hero: number; // the face row's height
  height: number; // the whole profile, down to the order tabs' hairline
};

export function profileFrame(width: number, scope: StreamScope): ProfileFrame {
  const row = PROFILE_ROW;
  const all = !scope.feedId && !scope.handle;
  if (width < PROFILE_WIDE) {
    const height = row.pad
      + row.face
      + row.gap + row.title + (all ? row.today : 0)
      + (row.gap + 4) + row.highlights
      + row.gap + row.read + row.doorGap + row.lead
      + row.gap + row.tabs;
    return { wide: false, spread: false, hero: row.face, height };
  }
  const spread = width >= PROFILE_SPREAD;
  if (spread) {
    const hero = row.profileTop + row.profileGap + row.highlights;
    const height = row.pad + hero + (row.gap + 6) + row.tabs;
    return { wide: true, spread, hero, height };
  }
  const hero = Math.max(row.face, row.titleWide + row.gap + row.stats + (all ? row.todayWide : 0));
  const height = row.pad + hero + row.wideGap + row.highlights + row.wideGap + row.doorsWide + (row.gap + 6) + row.tabs;
  return { wide: true, spread, hero, height };
}

// the page profile's height at a width, for the layout to reserve
export function profileHeaderHeight(width: number, scope: StreamScope): number {
  return profileFrame(width, scope).height;
}
