/* ─────────────────────────────────────────────────────────────
   PROFILE HEADER SIZE — the profile's rows, in px.

   ProfileHeader draws exactly these rows, each at a fixed height,
   loaded or not, so its height is known from its width and scope
   before it renders: the layout can reserve it and nothing below
   moves when the numbers arrive.

   Narrow (a phone): face + stats · name (with the settings button) ·
   highlights · where the pick stands, one line, one under another,
   with air between the bands; the posts start under it. On a
   phone's Day view the profile is one stop of the page's snap.
   Wide (≥ 720): the face beside the name, stats and the settings;
   then highlights; then the standing's line. Spread (≥ 860): two
   sides. Instagram's profile on the left (the face, the name, the
   numbers on one line, the bio; the highlights under it); the
   board's five places on the right, sharing the left side's height.
   ───────────────────────────────────────────────────────────── */

import type { StreamScope } from '@/lib/feedStream/contract';

export const PROFILE_ROW = {
  pad: 8, // above the face
  gap: 16, // between the face and the name (a phone)
  band: 20, // between a phone's bands: the name, the highlights, the standing
  wideGap: 26, // between the wide layout's bands
  foot: 10, // under the last band
  face: 120, // the 108px face in its ring
  stats: 60, // value · label · note
  statsPhone: 62, // a phone's, beside the face (20px numbers)
  title: 52, // the name and its line (a phone)
  titleWide: 60,
  today: 24, // all feeds: "Today · …", with the space above it (a phone)
  todayWide: 30,
  highlights: 92,
  standing: 60, // the standing's one line: the place, what it is, its move, the first three faces
  profileTop: 160, // a spread top's profile: the face, the name, the numbers (two lines on a narrow one) and the bio
  profileGap: 22, // between it and its highlights
  standingHead: 28, // the board's marker line (a spread top)
  standingRow: 48, // a place on the board (five of them)
  boardMin: 360, // the board's column on a spread top (it shares the top's height)
  boardMax: 440,
  bigFaceFrom: 1100, // a spread top this wide has the larger face
} as const;

export const PROFILE_WIDE = 720;
export const PROFILE_SPREAD = 860;

export type ProfileFrame = {
  wide: boolean; // the face beside its name
  spread: boolean; // the top across the column: the stats beside the name
  hero: number; // the face row's height
  height: number; // the whole profile, down to the foot of its last band
};

export function profileFrame(width: number, scope: StreamScope): ProfileFrame {
  const row = PROFILE_ROW;
  const all = !scope.feedId && !scope.handle;
  if (width < PROFILE_WIDE) {
    const height = row.pad
      + row.face
      + row.gap + row.title + (all ? row.today : 0)
      + row.band + row.highlights
      + row.band + row.standing
      + row.foot;
    return { wide: false, spread: false, hero: row.face, height };
  }
  const spread = width >= PROFILE_SPREAD;
  if (spread) {
    const hero = Math.max(row.profileTop + row.profileGap + row.highlights, row.standingHead + row.standingRow * 5);
    const height = row.pad + hero + row.foot;
    return { wide: true, spread, hero, height };
  }
  const hero = Math.max(row.face, row.titleWide + row.gap + row.stats + (all ? row.todayWide : 0));
  const height = row.pad + hero + row.wideGap + row.highlights + row.wideGap + row.standing + row.foot;
  return { wide: true, spread, hero, height };
}

// the page profile's height at a width, for the layout to reserve
export function profileHeaderHeight(width: number, scope: StreamScope): number {
  return profileFrame(width, scope).height;
}
