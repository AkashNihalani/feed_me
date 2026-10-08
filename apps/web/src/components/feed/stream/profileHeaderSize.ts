/* ─────────────────────────────────────────────────────────────
   PROFILE HEADER SIZE — the page profile's rows, in px.

   ProfileHeader (variant 'page') draws exactly these rows, each at
   a fixed height, loaded or not, so its height is known from its
   width and scope before it renders: the layout can reserve it and
   nothing below moves when the numbers arrive.

   Narrow (a phone): face + stats · name · actions · highlights ·
   week strip · habits · the order tabs, one under another.
   Wide (≥ 720): the face beside the name, stats and actions; then
   highlights; then the strip and the habits (side by side ≥ 1000);
   then the order tabs.
   ───────────────────────────────────────────────────────────── */

import type { StreamScope } from '@/lib/feedStream/contract';

export const PROFILE_ROW = {
  pad: 8, // above the face
  gap: 14, // between rows (a phone)
  wideGap: 24, // between the wide layout's bands
  face: 120, // the 108px face in its ring
  stats: 58, // value · label · note
  title: 48, // the name and its line (a phone)
  titleWide: 56,
  today: 24, // all feeds: "Today · …", with the space above it (a phone)
  todayWide: 30,
  actions: 44,
  highlights: 92,
  strip: 52,
  habits: 84,
  tabs: 44,
} as const;

export const PROFILE_WIDE = 720;
export const PROFILE_WIDEST = 1000;

export type ProfileFrame = {
  wide: boolean; // the face beside its name
  widest: boolean; // the week strip beside the habits
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
      + row.gap + row.actions
      + (row.gap + 4) + row.highlights
      + row.gap + row.strip
      + row.gap + row.habits
      + (row.gap + 4) + row.tabs;
    return { wide: false, widest: false, hero: row.face, height };
  }
  const widest = width >= PROFILE_WIDEST;
  const hero = Math.max(row.face, row.titleWide + row.gap + row.stats + (all ? row.todayWide : 0));
  const lower = widest ? Math.max(row.strip, row.habits) : row.strip + row.gap + row.habits;
  const height = row.pad + hero + row.wideGap + row.highlights + row.wideGap + lower + (row.gap + 6) + row.tabs;
  return { wide: true, widest, hero, height };
}

// the page profile's height at a width, for the layout to reserve
export function profileHeaderHeight(width: number, scope: StreamScope): number {
  return profileFrame(width, scope).height;
}
