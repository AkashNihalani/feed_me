'use client';

/* ─────────────────────────────────────────────────────────────
   ME — your Bitecoins, your plan, and what Feed Me does about the
   feeds you track: what was spent, what it tells you, what it has
   tracked. Not a tab: it opens over the one you were on (the line
   over the phone dock's tabs, the side rail's foot), and the dock
   on it is the way back.

   React mounts the plain-JS engine (components/me/meEngine) on the
   page and on its layer at body level, once, and puts it to sleep
   while the tab is hidden; it hands it the feeds, the Bitecoins
   and the posts, and does what it asks (a pick, the look, Manage).
   ───────────────────────────────────────────────────────────── */

import dynamic from 'next/dynamic';
import { useRouter } from 'next/navigation';
import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { createPortal } from 'react-dom';
import { mountMe } from '@/components/me/meEngine';
import { keptMe, loadMe, meOnHand, staleMe } from '@/components/me/meData';
import { usePageReady } from '@/components/shell/AppShell';
import { useTabHeader } from '@/components/shell/TabHeader';
import { bitecoinsLabel, dayOfWeek, feederCount, getBitecoins, subscribeBitecoins, syncBitecoins, useBitecoins } from '@/lib/bitecoins';
import { guessedIcon, feedIcon, feedIconName, setFeedIcon } from '@/lib/feedIcons';
import { normalizeHandle, titleCase } from '@/lib/feedLabels';
import { clearKept } from '@/lib/feedStream/keep';
import { useFeeds } from '@/lib/feedsStore';
import { appFont } from '@/lib/fonts';
import { announceFirstScreen } from '@/lib/launch';
import { istDayKey } from '@/lib/postMoment';
import { acquireDocumentClass } from '@/lib/rootScrollMode';
import { getSupabase } from '@/lib/supabase';
import { getTabScope, setTabScope, useTabScope, type TabScope } from '@/lib/tabScope';
import { getThemePreference, setThemePreference, useThemePreference, type ThemePreference } from '@/lib/theme';
import { useMobileImmersiveViewport } from '@/lib/useMobileImmersiveViewport';
import '@/components/me/me.css';

const FeedManageSheet = dynamic(() => import('@/components/feed/stream/FeedManageSheet'), { ssr: false });

const SUPPORT = 'support@feedmemore.com';
const TOP_SEEN = 'fm:bitecoins-top:v1';
// the dock's face on this page: a person, on the circles' silver
const PERSON_ART = `data:image/svg+xml,${encodeURIComponent('<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 64 64"><defs><linearGradient id="g" x1="0" y1="0" x2="1" y2="1"><stop offset="0" stop-color="#f7f7f8"/><stop offset="1" stop-color="#cfd3dc"/></linearGradient></defs><rect width="64" height="64" fill="url(#g)"/><g transform="translate(19 18) scale(1.08)" fill="none" stroke="#0b0b0f" stroke-width="2.3" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="8" r="4.6"/><path d="M4.5 20.5a7.5 7.5 0 0 1 15 0"/></g></svg>')}`;

const MONTHS = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December'];
function weekName(start: string) {
  const a = new Date(`${start}T00:00:00Z`);
  const b = new Date(a.getTime() + 6 * 86_400_000);
  return a.getUTCMonth() === b.getUTCMonth()
    ? `${a.getUTCDate()} to ${b.getUTCDate()} ${MONTHS[b.getUTCMonth()]}`
    : `${a.getUTCDate()} ${MONTHS[a.getUTCMonth()].slice(0, 3)} to ${b.getUTCDate()} ${MONTHS[b.getUTCMonth()].slice(0, 3)}`;
}

type Engine = ReturnType<typeof mountMe>;
type MeFeed = { id: string; title: string; feeders: Array<{ handle: string; profilePicUrl: string | null }> };

// the pick every tab shares, as Me shows it: a feeder always inside its feed
function resolvePick(feeds: MeFeed[], scope: TabScope): { feed: string | null; h: string | null } {
  const open = scope.feedId ? feeds.find((feed) => feed.id === scope.feedId) : undefined;
  const handle = scope.handle ? normalizeHandle(scope.handle) : null;
  if (!handle) return { feed: open?.id ?? null, h: null };
  const home = open?.feeders.some((feeder) => feeder.handle === handle) ? open : feeds.find((feed) => feed.feeders.some((feeder) => feeder.handle === handle));
  return home ? { feed: home.id, h: handle } : { feed: open?.id ?? null, h: null };
}

// the shared chrome's config: the rail's feeds, and the dock's face and line
const MeHeader = memo(function MeHeader({ feeds }: { feeds: MeFeed[] }) {
  const pick = resolvePick(feeds, useTabScope());
  const balance = useBitecoins();
  useTabHeader('me', {
    title: 'ME',
    label: 'Me',
    compressed: false,
    feeds: feeds.length ? feeds : null,
    feedId: pick.feed,
    handle: pick.h,
    back: { label: 'Me', note: bitecoinsLabel(balance), art: PERSON_ART, onBack: () => window.history.back() },
    onAll: () => setTabScope({ feedId: null, handle: null }),
    onFeed: (id) => setTabScope(id === pick.feed && !pick.h ? { feedId: null, handle: null } : { feedId: id, handle: null }),
    onFeeder: (handle) => setTabScope({ feedId: pick.feed, handle: pick.h === normalizeHandle(handle) ? null : handle }),
  });
  return null;
});

export default function MeTab() {
  const router = useRouter();
  useMobileImmersiveViewport();
  usePageReady(true);
  const { feeds, reload } = useFeeds();
  const look = useThemePreference();
  const scope = useTabScope();
  const [root, setRoot] = useState<HTMLDivElement | null>(null);
  const [layer, setLayer] = useState<HTMLDivElement | null>(null);
  const [manage, setManage] = useState<{ open: boolean; feedId: string | null } | null>(null);
  const engine = useRef<Engine | null>(null);
  const email = useRef('');

  const shown = useMemo<MeFeed[]>(() => (feeds || []).map((feed) => ({
    id: String(feed.id),
    title: titleCase(feed.title || ''),
    feeders: feed.feeders.map((feeder) => ({ handle: normalizeHandle(feeder.handle), profilePicUrl: feeder.profilePicUrl || null })).filter((feeder) => feeder.handle),
  })), [feeds]);

  useEffect(() => {
    document.title = 'Me | FeedMe';
    announceFirstScreen();
  }, []);

  useEffect(() => {
    void getSupabase().auth.getUser().then(({ data }) => {
      email.current = data.user?.email || '';
    });
  }, []);

  const signOut = useCallback(async () => {
    clearKept();
    await getSupabase().auth.signOut();
    router.push('/login');
  }, [router]);

  /* the page's own class while it is on screen (the body lets go of its scroller: what is stuck sticks to the screen), and
     the engine awake only then. It is mounted once: hidden, the page keeps what it drew */
  const host = useMemo(() => ({
    icon: feedIcon,
    iconName: feedIconName,
    guess: guessedIcon,
    setIcon: setFeedIcon,
    pick: (feed: string | null, h: string | null) => setTabScope({ feedId: feed, handle: h }),
    theme: () => getThemePreference(),
    setTheme: (next: ThemePreference, at: { x: number; y: number }) => setThemePreference(next, at),
    email: () => email.current,
    help: () => {
      window.location.href = `mailto:${SUPPORT}`;
    },
    manage: (feedId: string | null) => setManage({ open: true, feedId }),
    signOut: () => void signOut(),
    toppedSeen: () => {
      const week = getBitecoins()?.weeks[0].start;
      try {
        if (week) window.localStorage.setItem(TOP_SEEN, week);
      } catch {
        // storage blocked
      }
    },
  }), [signOut]);

  useLayoutEffect(() => acquireDocumentClass('fm-me-open'), []);

  useLayoutEffect(() => {
    if (!root || !layer) return undefined;
    const fresh = !engine.current;
    const one = engine.current ?? mountMe({ root, layer, host });
    engine.current = one;
    const count = feederCount(feeds);
    if (count != null) syncBitecoins(count);
    const tell = () => {
      const ledger = getBitecoins();
      if (!ledger) return;
      let seen: string | null = null;
      try {
        seen = window.localStorage.getItem(TOP_SEEN);
      } catch {
        // storage blocked
      }
      const top = ledger.weeks[0].d[0].find((event) => event.t === 'top');
      one.setLedger({
        weekly: ledger.weekly,
        bought: ledger.bought,
        full: ledger.full,
        today: dayOfWeek(ledger.weeks[0].start, istDayKey(Date.now())),
        topped: top && seen !== ledger.weeks[0].start ? top.n : 0,
        weeks: ledger.weeks.map((week) => ({ t: weekName(week.start), d: week.d })),
      });
    };
    if (fresh && feeds) one.setFeeds(shown);
    const at = resolvePick(shown, getTabScope());
    one.setPick(at.feed, at.h);
    tell();
    const posts = meOnHand();
    if (posts) one.setData(posts);
    one.wake();
    const stop = subscribeBitecoins(tell);
    return () => {
      stop();
      one.sleep();
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- waking is for coming on screen; the feeds come in below
  }, [root, layer, host]);

  // the page taken away for good
  useEffect(() => () => {
    if (root?.isConnected === false) {
      engine.current?.unmount();
      engine.current = null;
    }
  }, [root]);

  useLayoutEffect(() => {
    if (!feeds || !engine.current) return;
    const count = feederCount(feeds);
    if (count != null) syncBitecoins(count);
    engine.current.setFeeds(shown);
    const at = resolvePick(shown, getTabScope());
    engine.current.setPick(at.feed, at.h);
  }, [feeds, shown, root, layer]);

  useEffect(() => {
    const at = resolvePick(shown, scope);
    engine.current?.setPick(at.feed, at.h);
  }, [shown, scope]);

  useEffect(() => {
    engine.current?.setLook();
  }, [look]);

  // the posts: what the device kept, then the fresh ones
  useEffect(() => {
    let live = true;
    const give = (data: Awaited<ReturnType<typeof loadMe>> | null) => {
      if (live && data) engine.current?.setData(data);
    };
    void keptMe().then(give);
    loadMe().then(give, () => {
      if (live && !meOnHand()) engine.current?.say('What was tracked could not be loaded');
    });
    return () => {
      live = false;
    };
  }, [feeds]);

  const closeManage = useCallback(() => {
    setManage((current) => (current ? { ...current, open: false } : current));
    staleMe();
    void reload();
  }, [reload]);

  const portalTarget = typeof document === 'undefined' ? null : document.body;
  return (
    <>
      <MeHeader feeds={shown} />
      {/* (its own arrival: the tab host leaves it be on a phone) */}
      <div data-tab-arrival="own" className={`fm-me fm-me-page ${appFont.variable}`}>
        <div ref={setRoot} className="mein" />
      </div>
      {portalTarget ? createPortal(<div ref={setLayer} className={`fm-me fm-me-layer ${appFont.variable}`} />, portalTarget) : null}
      {manage ? <FeedManageSheet open={manage.open} scope={{ feedId: manage.feedId, handle: null }} onClose={closeManage} /> : null}
    </>
  );
}
