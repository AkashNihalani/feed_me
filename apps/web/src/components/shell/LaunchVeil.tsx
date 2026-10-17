'use client';

/* ─────────────────────────────────────────────────────────────
   LAUNCH VEIL — a cold start (the first after signing up or in:
   nothing of the app kept on the device yet, lib/launch) shown as
   the app getting dinner ready: the big number is how far along it
   is, the red Pac-Man eats its way along the pellets to it, and the
   three courses (your feed, Lead, Read) tick off as each is warm.
   It warms all three while it is up (Feed's first screen, Lead's
   board, Read's feeds: components/lead/leadData, read/readFeeds),
   so the first visit to each opens on it, not on a loader.

   The number is honest: each course counts for its share once it is
   in, and creeps towards (never to) its share while it loads, so it
   never sits still and never runs ahead. Once all three are in (or
   LAUNCH_CAP_MS on: a course still out loads on its tab), it reaches
   100, holds a beat, and the veil lifts away as a sheet, up off the
   screen, as Feed's first cards rise in under it (FeedStream waits
   for it: launchHolds). At least LAUNCH_MIN_MS, so it reads as a
   moment, never a flicker.

   A warm start (something kept) never shows it, and loads nothing
   ahead: Lead and Read load when they are opened (nothing runs
   behind a Feed being scrolled).

   Only transform and opacity move (the number's digits roll, the
   Pac-Man glides, the pellets go), on plain boxes. It never paints
   the strip a phone's status bar is coloured from (fm-under-status;
   the sheet lifts inside a box that starts below it).
   ───────────────────────────────────────────────────────────── */

import { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { loadLead } from '@/components/lead/leadData';
import { warmReadFeeds } from '@/components/read/readFeeds';
import { LAUNCH_CLASS, LAUNCH_EVENT, firstScreenShown, setLaunchVeil, startsCold } from '@/lib/launch';
import { ms } from '@/lib/tempo';
import { cn } from '@/lib/utils';

const useIsoLayoutEffect = typeof window !== 'undefined' ? useLayoutEffect : useEffect;

const LAUNCH_MIN_MS = 1600;
const LAUNCH_CAP_MS = 9000;
// 100 held this long before the sheet lifts
const FULL_HOLD_MS = 260;
const LIFT_MS = ms(560);
// how quickly the number chases where it is heading (a time constant), and how far a course still loading creeps
const CHASE_MS = 140;
const CREEP = 0.86;
const PELLETS = 14;

type CourseKey = 'feed' | 'lead' | 'read';
type Course = { key: CourseKey; title: string; doing: string; share: number; pace: number };
const COURSES: Course[] = [
  { key: 'feed', title: 'Your feed', doing: 'Plating the latest posts', share: 0.5, pace: 1800 },
  { key: 'lead', title: 'Lead', doing: 'Warming up the board', share: 0.35, pace: 2600 },
  { key: 'read', title: 'Read', doing: 'Setting out the reads', share: 0.15, pace: 900 },
];

// a digit that rolls (an odometer's wheel: 0–9 stacked, moved by transform)
function Wheel({ digit }: { digit: number }) {
  return (
    <span className="fm-launch-wheel" aria-hidden="true">
      <span className="fm-launch-strip" style={{ transform: `translate3d(0, ${-digit * 10}%, 0)` }}>
        {[0, 1, 2, 3, 4, 5, 6, 7, 8, 9].map((n) => <span key={n}>{n}</span>)}
      </span>
    </span>
  );
}

export default function LaunchVeil() {
  const [phase, setPhase] = useState<'on' | 'lifting' | 'off'>('on');
  const [shownAt, setShownAt] = useState(0);
  const [percent, setPercent] = useState(0);
  const [ready, setReady] = useState<Record<CourseKey, boolean>>({ feed: false, lead: false, read: false });
  const pacRef = useRef<HTMLSpanElement | null>(null);
  const trackRef = useRef<HTMLDivElement | null>(null);

  // a cold start the script didn't see (signing in moves to the app without a reload): up before paint. A warm one: off
  useIsoLayoutEffect(() => {
    const root = document.documentElement;
    if (!root.classList.contains(LAUNCH_CLASS)) {
      if (firstScreenShown() || !startsCold()) {
        setPhase('off');
        return;
      }
      root.classList.add(LAUNCH_CLASS);
      setShownAt(performance.now());
    }
    setLaunchVeil('up');
  }, []);

  // gone before it has lifted (the app moved to a page without the tabs: sign-in, a 401): it takes itself away with it,
  // never left on to come back over the next tab
  useEffect(() => () => {
    if (!document.documentElement.classList.contains(LAUNCH_CLASS)) return;
    document.documentElement.classList.remove(LAUNCH_CLASS);
    setLaunchVeil('none');
  }, []);

  // the courses: each one ready when it is in (or can't be: its tab says so itself)
  useEffect(() => {
    if (phase !== 'on') return undefined;
    let live = true;
    const done = (key: CourseKey) => () => {
      if (live) setReady((current) => (current[key] ? current : { ...current, [key]: true }));
    };
    if (firstScreenShown()) done('feed')();
    else window.addEventListener(LAUNCH_EVENT, done('feed'), { once: true });
    void loadLead().then(done('lead'), done('lead'));
    void warmReadFeeds().then(done('read'), done('read'));
    return () => {
      live = false;
    };
  }, [phase]);

  // the number and the Pac-Man: chasing where it is heading, a frame at a time
  const readyRef = useRef(ready);
  useEffect(() => {
    readyRef.current = ready;
  }, [ready]);
  useEffect(() => {
    if (phase !== 'on') return undefined;
    const started = performance.now();
    let shown = 0;
    let last = started;
    let fullAt = 0;
    let frame = 0;
    const tick = (now: number) => {
      const elapsed = now - started;
      const capped = now - shownAt >= LAUNCH_CAP_MS;
      let target = 0;
      let all = true;
      COURSES.forEach((course) => {
        const isReady = readyRef.current[course.key] || capped;
        if (isReady) target += course.share;
        else {
          all = false;
          target += course.share * CREEP * (1 - Math.exp(-elapsed / course.pace));
        }
      });
      // never faster than the minimum lets it (a quick start still reads as a moment)
      const floor = Math.min(1, (now - shownAt) / LAUNCH_MIN_MS);
      if (all) target = Math.min(1, target) * Math.min(1, floor + 0.0001);
      const step = 1 - Math.exp(-(now - last) / CHASE_MS);
      last = now;
      shown = Math.max(shown, shown + (target - shown) * step);
      if (all && target >= 1 && shown > 0.995) shown = 1;
      const whole = Math.round(shown * 100);
      setPercent((current) => (current === whole ? current : whole));
      const track = trackRef.current;
      const pac = pacRef.current;
      if (track && pac) {
        const room = track.clientWidth - pac.offsetWidth;
        pac.style.transform = `translate3d(${(room * shown).toFixed(1)}px, 0, 0)`;
        track.querySelectorAll<HTMLElement>('[data-pellet]').forEach((pellet, at) => {
          const eaten = shown >= (at + 1) / (PELLETS + 1);
          if (eaten !== (pellet.dataset.eaten !== undefined)) {
            if (eaten) pellet.dataset.eaten = '';
            else delete pellet.dataset.eaten;
          }
        });
      }
      if (shown >= 1) {
        if (!fullAt) fullAt = now;
        if (now - fullAt >= FULL_HOLD_MS) {
          setPhase('lifting');
          return;
        }
      }
      frame = window.requestAnimationFrame(tick);
    };
    frame = window.requestAnimationFrame(tick);
    return () => window.cancelAnimationFrame(frame);
  }, [phase, shownAt]);

  // lifting: the first screen comes in under it as it goes (launchHolds), then it is gone
  useEffect(() => {
    if (phase !== 'lifting') return undefined;
    setLaunchVeil('leaving');
    const reduce = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    const timer = window.setTimeout(() => {
      document.documentElement.classList.remove(LAUNCH_CLASS);
      setLaunchVeil('none');
      setPhase('off');
    }, reduce ? 0 : LIFT_MS + 60);
    return () => window.clearTimeout(timer);
  }, [phase]);

  if (phase === 'off') return null;
  const tens = Math.floor((percent % 100) / 10);
  const units = percent % 10;
  return (
    <div className="fm-launch-veil fm-under-status" data-lifting={phase === 'lifting' ? '' : undefined} aria-hidden={phase === 'lifting'}>
      <div className="fm-launch-sheet">
        <div className="fm-launch-stage" role="status" aria-live="polite" aria-label={`Getting your feeds ready, ${percent}%`}>
          <div className="fm-launch-mark" aria-hidden="true">
            <span>FEED</span>
            <span className="fm-launch-me">ME</span>
          </div>
          <p className="fm-launch-kicker">Setting the table</p>
          <div className="fm-launch-number" aria-hidden="true">
            <span className={cn('fm-launch-hundred', percent >= 100 && 'is-on')}><Wheel digit={percent >= 100 ? 1 : 0} /></span>
            <Wheel digit={percent >= 100 ? 0 : tens} />
            <Wheel digit={percent >= 100 ? 0 : units} />
            <span className="fm-launch-sign">%</span>
          </div>
          <div ref={trackRef} className="fm-launch-track" aria-hidden="true">
            {Array.from({ length: PELLETS }, (_, at) => (
              <span key={at} data-pellet="" className="fm-launch-pellet" style={{ left: `${((at + 1) / (PELLETS + 1)) * 100}%` }} />
            ))}
            <span ref={pacRef} className="fm-launch-pac">
              <span className="fm-launch-jaw fm-launch-jaw--top" />
              <span className="fm-launch-jaw fm-launch-jaw--bottom" />
            </span>
          </div>
          <ul className="fm-launch-courses">
            {COURSES.map((course) => (
              <li key={course.key} className="fm-launch-course" data-ready={ready[course.key] ? '' : undefined}>
                <span className="fm-launch-tick" aria-hidden="true">
                  <svg viewBox="0 0 16 16" width="12" height="12">
                    <path d="M3.5 8.4 6.6 11.4 12.5 4.8" fill="none" stroke="currentColor" strokeWidth="2.4" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                </span>
                <span className="fm-launch-course-text">
                  <span className="fm-launch-course-title">{course.title}</span>
                  <span className="fm-launch-course-doing">{ready[course.key] ? 'Ready' : course.doing}</span>
                </span>
              </li>
            ))}
          </ul>
          <p className="fm-launch-note">Only the first time. After this, Feed Me opens straight away.</p>
        </div>
      </div>
    </div>
  );
}
