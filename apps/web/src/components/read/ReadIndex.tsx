'use client';

/* ─────────────────────────────────────────────────────────────
   READ INDEX — what the page shows before you pick someone with
   a full read: not the rail again, but every feeder's trajectory
   from the posts the app tracks (readBoard), in the read's own
   language: runs of 10, each box its typical post, shaded the
   way the read's trajectory is.

   - All feeds, or one feed: the board. Feeders are grouped by
     which way their latest run moved (on the up, sliding,
     holding, too early), with a one-line summary on top.
   - A feeder without a full read: their trajectory large, their
     latest run post by post, their best post, and who does have
     a full read.

   Laid out on the page's full width (left-aligned), styled in
   readTab.css (.rd .ix-*).
   ───────────────────────────────────────────────────────────── */

import { Fragment, useEffect, useMemo, useState, type CSSProperties } from 'react';
import { AnimatePresence, LayoutGroup, motion, useReducedMotion, type MotionProps } from 'framer-motion';
import FeederStoryAvatar from '@/components/feed/FeederStoryAvatar';
import { SCENES } from '@/components/read/readerEngine';
import { RUN_SIZE, RUN_SLOTS, cachedBoard, loadBoard, type BoardFeeder, type BoardPost, type BoardRun, type Momentum } from '@/components/read/readBoard';
import { READ_POSTS, readOf as READ_INDEX_OF, titleCase, type RailFeed, type RailFeeder } from '@/components/read/readFeeds';
import { RAMP, shadeStyle } from '@/components/read/readShade';
import feedReadsData from '@/data/readerFeedPreview.json';

const SOFT_EASE = [0.16, 0.9, 0.2, 1] as const;

/* One arrival for every trajectory, the read's own too (readerEngine's render): each run's box rises into place,
   growing up from its baseline, and its colour lights as it lands, one box after the next, left to right, on the
   same curve as every other piece of the page. A board row plays it tighter, and each row starts a step after
   the one above, so the board arrives as one wave. The motion is CSS (readTab.css, rd-ix-box); this is when
   each box starts (--rd), in ms. */
const STEP_MS = 60;
const stepDelay = (step: number) => 60 + Math.min(step, 12) * STEP_MS;
const TRAJ = { at: 80, gap: 55 };
const ROW = { at: 60, gap: 40 };

function cascade(base: number, k: number, beat: typeof TRAJ): CSSProperties {
  return { ['--rd' as string]: `${base + beat.at + k * beat.gap}ms` };
}
const WORDS = ['No one', 'One', 'Two', 'Three', 'Four', 'Five', 'Six', 'Seven', 'Eight', 'Nine', 'Ten'];

const GROUPS: { id: Momentum; title: string; line: string }[] = [
  { id: 'up', title: 'On the up', line: 'Their latest run beat the one before it.' },
  { id: 'down', title: 'Sliding', line: 'Their latest run fell short of the one before it.' },
  { id: 'level', title: 'Holding', line: 'Landing about where they were.' },
  { id: 'early', title: 'Too early to call', line: 'Not two runs of posts yet.' },
];

function counted(n: number, lower = false) {
  const word = n <= 10 ? WORDS[n] : String(n);
  return lower ? word.toLowerCase() : word;
}

function plural(n: number, word: string) {
  return `${n} ${word}${n === 1 ? '' : 's'}`;
}

function top(pct: number | null | undefined) {
  return pct == null ? null : Math.max(1, Math.round(pct));
}

function ago(row: BoardFeeder, post: BoardPost) {
  const days = Math.max(0, Math.floor((row.at - post.postedAt) / 86_400_000));
  if (days < 1) return 'today';
  if (days < 14) return `${days}d ago`;
  if (days < 60) return `${Math.round(days / 7)}w ago`;
  return `${Math.round(days / 30)}mo ago`;
}

/* the line under a feeder's handle: the run the move is read from, against the one before it */
function noteFor(row: BoardFeeder) {
  if (row.momentum === 'early' || !row.now || row.now.tp == null) {
    const core = row.total ? `${plural(row.total, 'post')} so far` : 'No posts tracked yet';
    return { pre: '', core, full: core };
  }
  const pre = `${row.now.live ? 'This run' : 'Last run'}: typical post `;
  const core = `top ${row.now.tp}%${row.before?.tp != null ? `, was ${row.before.tp}%` : ''}`;
  return { pre, core, full: pre + core };
}

/* the board's headline and the line under it: how the group splits, who is strongest, who moved most */
function summaryOf(rows: BoardFeeder[], where: string) {
  const count = (m: Momentum) => rows.filter((row) => row.momentum === m).length;
  const up = count('up');
  const down = count('down');
  const level = count('level');
  const head = up && down ? `${counted(up)} on the up, ${counted(down, true)} sliding`
    : up ? `${counted(up)} on the up, nobody sliding`
    : down ? `${counted(down)} sliding, nobody on the up`
    : level ? 'Everyone is holding steady'
    : rows.length ? 'Too early to call anyone' : 'Nobody to read here yet';
  const ranked = rows.filter((row): row is BoardFeeder & { now: BoardRun & { tp: number } } => row.now?.tp != null);
  const strongest = [...ranked].sort((a, b) => a.now.tp - b.now.tp)[0] || null;
  const moved = (row: (typeof ranked)[number]) => (row.before?.tp != null ? Math.abs(row.before.tp - row.now.tp) : 0);
  const mover = [...ranked].sort((a, b) => moved(b) - moved(a))[0] || null;
  const lines: string[] = [];
  // every top % is against the feeder's own posts, so "strongest" means outdoing themselves the most
  if (strongest) lines.push(`@${strongest.handle} is outdoing themselves the most ${where}: the typical post of their ${strongest.now.live ? 'current' : 'last'} run sits in their own top ${strongest.now.tp}%.`);
  if (mover && mover !== strongest && mover.before?.tp != null && moved(mover) >= 8) {
    lines.push(`@${mover.handle} moved the most, from their own top ${mover.before.tp}% to top ${mover.now.tp}%.`);
  }
  return { head, sub: lines.join(' ') };
}

/* a headline arrives word by word, each word sliding up out of its own mask, one after another (it leaves the
   same way, readMotion) */
function Words({ text, at }: { text: string; at: number }) {
  const words = text.split(' ');
  return (
    <>
      {words.map((word, k) => (
        <Fragment key={k}>
          <span className="ix-w"><span style={{ ['--wd' as string]: `${at + k * 34}ms` }}>{word}</span></span>
          {k < words.length - 1 ? ' ' : null}
        </Fragment>
      ))}
    </>
  );
}

// a piece's own start, for the CSS details inside it (a dot, a scale) to follow it in
const at = (step: number): CSSProperties => ({ ['--rd' as string]: `${stepDelay(step)}ms` });

function Chevron() {
  return (
    <svg className="ix-go" viewBox="0 0 16 16" aria-hidden="true">
      <path d="M6 3.5 10.5 8 6 12.5" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" />
    </svg>
  );
}

/* a post's cover: the real thumbnail, or a sample post's scene colours */
function Cover({ post, className }: { post: BoardPost | null; className: string }) {
  const scene = post?.scene ? (SCENES as Record<string, string[]>)[post.scene] : null;
  return (
    <span className={className} style={scene ? { background: `linear-gradient(160deg, ${scene[0]}, ${scene[1]})` } : undefined}>
      {post?.thumb ? (
        // eslint-disable-next-line @next/next/no-img-element -- remote feeder media, same as the feed's avatars
        <img src={post.thumb} alt="" loading="lazy" decoding="async" />
      ) : null}
    </span>
  );
}

function runsLabel(row: BoardFeeder) {
  if (!row.runs.length) return 'No runs yet';
  return `Runs of ${RUN_SIZE}, oldest first: ${row.runs.map((run) => `${run.tp != null ? `top ${run.tp}%` : 'unranked'}${run.live ? ` (this run so far, ${run.count} of ${RUN_SIZE})` : ''}`).join(', ')}`;
}

function slotsOf(row: BoardFeeder) {
  return [...row.runs, ...Array.from({ length: Math.max(0, RUN_SLOTS - row.runs.length) }, () => null)];
}

/* one feeder on the board: who, their runs, their latest post */
function BoardRow({ row, pic, full, feedTitle, delay, chosen, onPick }: { row: BoardFeeder; pic: string | null; full: boolean; feedTitle: string | null; delay: number; chosen: boolean; onPick: OnPick }) {
  const note = noteFor(row);
  const latest = top(row.latest?.pct);
  return (
    <button type="button" className={`ix-row m-${row.momentum}${chosen ? ' chosen' : ''}`} onClick={(event) => onPick(row.handle, event.currentTarget)} aria-label={`@${row.handle}${full ? ', full read' : ''}. ${note.full}`}>
      <span className={`ix-av${full ? ' on' : ''}`}>
        <FeederStoryAvatar feeder={{ handle: row.handle, profilePicUrl: pic }} className="text-[13px]" />
      </span>
      <span className="ix-id">
        <b><span>@{row.handle}</span>{full ? <em>Full read</em> : null}</b>
        <small>
          {feedTitle ? <span className="ix-in">{feedTitle} · </span> : null}
          <span className="ix-pre">{note.pre}</span>
          {note.core}
        </small>
      </span>
      <span className="ix-runs" role="img" aria-label={runsLabel(row)}>
        {slotsOf(row).map((run, k) => (run ? (
          <span key={k} className={`ix-run${run.live ? ' live' : ''}${run === row.now ? ' now' : ''}`} style={{ ...(run.tp != null ? shadeStyle(run.tp) : null), ...cascade(delay, k, ROW) }}>
            <i className="ix-f" />
            <span className="ix-v">{run.tp ?? '–'}{run.tp != null ? <small>%</small> : null}</span>
            {run.live ? <i className="ix-prog" style={{ ['--p' as string]: run.count / RUN_SIZE }} /> : null}
          </span>
        ) : <span key={k} className="ix-run empty" style={cascade(delay, k, ROW)} />))}
      </span>
      <span className="ix-last" style={{ ['--rd' as string]: `${delay + ROW.at + RUN_SLOTS * ROW.gap}ms` }}>
        <Cover post={row.latest} className="ix-th" />
        <span className="ix-lt">
          <b>{row.latest ? (latest != null ? `Top ${latest}%` : 'Unranked') : 'No posts'}</b>
          {row.latest ? <small>{ago(row, row.latest)}{row.quietDays ? ' · quiet' : ''}</small> : null}
        </span>
      </span>
      <Chevron />
    </button>
  );
}

function SkeletonRow({ index }: { index: number }) {
  const delay = stepDelay(index + 1);
  return (
    <div className="ix-row sk" aria-hidden="true">
      <span className="ix-av" />
      <span className="ix-id"><i className="ix-sk" style={{ width: '46%', height: 12 }} /><i className="ix-sk" style={{ width: '72%', height: 10 }} /></span>
      <span className="ix-runs">{Array.from({ length: RUN_SLOTS }, (_, k) => <span key={k} className="ix-run empty" style={cascade(delay, k, ROW)} />)}</span>
      <span className="ix-last"><span className="ix-th" /></span>
    </div>
  );
}

/* a feeder Read has a full read for, as a way out of a page that has none */
function PickRow({ feeder, chosen, onPick }: { feeder: RailFeeder; chosen: boolean; onPick: OnPick }) {
  return (
    <button type="button" className={`ix-pick${chosen ? ' chosen' : ''}`} onClick={(event) => onPick(feeder.handle, event.currentTarget)} aria-label={`Read @${feeder.handle}`}>
      <span className="ix-av on">
        <FeederStoryAvatar feeder={{ handle: feeder.handle, profilePicUrl: feeder.profilePicUrl }} className="text-[13px]" />
      </span>
      <span className="ix-pt">
        <b>@{feeder.handle}</b>
        <small>Full read · {feeder.read != null ? `${READ_POSTS[feeder.read]} posts` : ''}</small>
      </span>
      <Chevron />
    </button>
  );
}

/* the boards of the feeds in view, fetched on first sight and kept for the visit */
function useBoards(list: RailFeed[]) {
  const [, landed] = useState(0);
  const [failed, setFailed] = useState<Record<string, true>>({});
  useEffect(() => {
    let live = true;
    list.forEach((feed) => {
      if (cachedBoard(feed.id)) return;
      loadBoard(feed)
        .then(() => { if (live) landed((n) => n + 1); })
        .catch(() => { if (live) setFailed((current) => ({ ...current, [feed.id]: true })); });
    });
    return () => { live = false; };
  }, [list]);
  return list.map((feed) => ({ feed, rows: cachedBoard(feed.id), failed: Boolean(failed[feed.id]) }));
}

type Rise = (step: number, depth?: boolean) => Pick<MotionProps, 'initial' | 'animate' | 'transition'>;
// a feeder picked on this page, and the row it was picked from
type OnPick = (handle: string, from: HTMLElement) => void;

function Board({ list, all, pics, full, chosen, rise, onFeeder }: { list: RailFeed[]; all: boolean; pics: Map<string, string | null>; full: Set<string>; chosen: string | null; rise: Rise; onFeeder: OnPick }) {
  const boards = useBoards(list);
  const loading = boards.some((board) => !board.rows && !board.failed);
  const failed = boards.filter((board) => board.failed).map((board) => titleCase(board.feed.title));
  const titles = new Map(list.map((feed) => [feed.id, titleCase(feed.title)]));
  // each feeder once, even if two feeds share them
  const rows: BoardFeeder[] = [];
  const seen = new Set<string>();
  boards.forEach((board) => board.rows?.forEach((row) => {
    if (seen.has(row.handle)) return;
    seen.add(row.handle);
    rows.push(row);
  }));
  const feederCount = all ? new Set(list.flatMap((feed) => feed.feeders.map((feeder) => feeder.handle))).size : list[0]?.feeders.length ?? 0;
  const kicker = all ? `All feeds · ${plural(feederCount, 'feeder')}` : `${titleCase(list[0]?.title || 'Feed')} · ${plural(feederCount, 'feeder')}`;
  const { head, sub } = summaryOf(rows, all ? 'across your feeds' : 'in this feed');
  const groups = GROUPS
    .map((group) => ({
      ...group,
      rows: rows.filter((row) => row.momentum === group.id).sort((a, b) => (a.now?.tp ?? 101) - (b.now?.tp ?? 101)),
    }))
    .filter((group) => group.rows.length);
  let step = 0;

  return (
    <div className={`wrap ix${chosen ? ' picking' : ''}`}>
      {/* the header's lines arrive one after another, never as a block */}
      <header className="ix-hd">
        <motion.span className="tj-k" style={at(step)} {...rise(step++)}><i />{kicker}</motion.span>
        {(() => {
          const text = loading && !rows.length ? 'Reading the runs…' : head;
          return <h2 className="ix-h" key={text}><Words text={text} at={stepDelay(step++)} /></h2>;
        })()}
        {sub ? <motion.p className="ix-p" {...rise(step++)}>{sub}</motion.p> : null}
        <motion.div className="ix-key" aria-hidden="true" style={at(step)} {...rise(step++)}>
          <span className="ix-kl">Each box is a run of {RUN_SIZE} posts, oldest to newest, shaded by where its typical post ranks among the feeder&apos;s own</span>
          <span className="ix-ramp">
            <span className="tj-kl">Top 1%</span>
            <span className="tj-ramp" style={{ ['--ramp' as string]: RAMP }}><i className="mid" /></span>
            <span className="tj-kl">Bottom</span>
          </span>
        </motion.div>
      </header>

      {loading && !rows.length ? (
        <div className="ix-rows">
          {Array.from({ length: Math.min(Math.max(feederCount, 3), 6) }, (_, k) => <SkeletonRow key={k} index={k} />)}
        </div>
      ) : null}

      {groups.map((group) => (
        <section key={group.id} className={`ix-grp m-${group.id}`} aria-labelledby={`rd-ix-${group.id}`}>
          <motion.div className="ix-gh" style={at(step)} {...rise(step++)}>
            <h3 id={`rd-ix-${group.id}`}><i aria-hidden="true" />{group.title}<span>{group.rows.length}</span></h3>
            <p>{group.line}</p>
          </motion.div>
          <div className="ix-rows">
            {group.rows.map((row) => {
              const rowStep = step++;
              return (
                <motion.div key={row.handle} {...rise(rowStep, true)}>
                  <BoardRow
                    row={row}
                    pic={pics.get(row.handle) ?? null}
                    full={full.has(row.handle)}
                    feedTitle={all ? titles.get(row.feedId) ?? null : null}
                    delay={stepDelay(rowStep)}
                    chosen={chosen === row.handle}
                    onPick={onFeeder}
                  />
                </motion.div>
              );
            })}
          </div>
        </section>
      ))}

      {!loading && !rows.length && !failed.length ? (
        <p className="ix-p">{all ? 'Add feeders in Feed and their runs show up here.' : 'No feeders in this feed yet. Add some in Feed and their runs show up here.'}</p>
      ) : null}
      {failed.length ? <p className="ix-p ix-err">Couldn&apos;t load the posts for {failed.join(', ')}. Try again in a bit.</p> : null}
    </div>
  );
}

/* ── a feed, read: one row per feeder the reader has read, headed by that feeder's top rule ──────────────
   Lead's board grammar one level up from a feeder's rules: every row is a feeder, its headline is the rule
   that carries it, and its chips are what moved in its latest run (a rule born, sharpened, broken, dropped,
   a call settled). Rows are ranked by how much moved, so the page reads as the feed's "now", not a league
   table. A row opens in place into the feeder's top rules, its latest run and the call on the record; the
   full read is one more tap. Feeders the reader hasn't read yet wait underneath, with their runs. Sample
   content for now (data/readerFeedPreview.json). */
type FeedRule = { law: string; carry: [number, number]; held: [number, number]; move: string };
type FeedMove = { move: string; law: string; line: string };
type FeedCall = { verdict: 'called' | 'missed' | 'open'; text: string; line: string };
type FeedRead = { run: number; posts: number; who: string; head: string; rules: FeedRule[]; moves: FeedMove[]; call: FeedCall; next: string };
const FEED_READS = (feedReadsData as unknown as { feeders: Record<string, FeedRead> }).feeders;
export const hasFeedRead = (feed: RailFeed | null) => Boolean(feed && feed.feeders.some((feeder) => FEED_READS[feeder.handle]));
const MOVES: Record<string, { word: string; tone: '' | 'up' | 'dn'; weight: number }> = {
  born: { word: 'New rule', tone: 'up', weight: 3 },
  broke: { word: 'Broke', tone: 'dn', weight: 3 },
  dropped: { word: 'Dropped', tone: 'dn', weight: 2 },
  sharpened: { word: 'Sharpened', tone: 'up', weight: 1 },
  tweaked: { word: 'Tweaked', tone: '', weight: 1 },
  held: { word: 'Held', tone: '', weight: 0 },
};
const VERDICT: Record<FeedCall['verdict'], string> = { called: 'Called it', missed: 'Missed', open: 'Still open' };
const weightOf = (read: FeedRead) => read.moves.reduce((sum, move) => sum + (MOVES[move.move]?.weight ?? 0), 0) + (read.call.verdict === 'open' ? 0 : 1);
const pad2 = (k: number) => String(k).padStart(2, '0');

function FeedChevron({ open }: { open: boolean }) {
  return <svg viewBox="0 0 16 16" aria-hidden="true" style={{ transform: open ? 'rotate(180deg)' : undefined }}><path d="M4 6l4 4 4-4" fill="none" stroke="currentColor" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" /></svg>;
}
/* a feeder's runs, small: one box per run, shaded by its typical post, lighting left to right */
function MiniRuns({ row, delay }: { row: BoardFeeder | null; delay: number }) {
  const runs = row ? row.runs.slice(-RUN_SLOTS) : [];
  return (
    <span className="fb-runs" role="img" aria-label={row ? runsLabel(row) : 'No runs yet'}>
      {runs.map((run, k) => <i key={run.r} className={run.live ? 'live' : ''} style={{ ...(run.tp != null ? shadeStyle(run.tp) : null), ['--rd' as string]: `${delay + k * 45}ms` }} />)}
    </span>
  );
}
/* the counts that head the page: what moved across every feeder with a read */
function feedHeadline(reads: FeedRead[]) {
  const tally = (move: string) => reads.reduce((sum, read) => sum + read.moves.filter((m) => m.move === move).length, 0);
  const sharp = tally('sharpened'), born = tally('born'), broke = tally('broke'), dropped = tally('dropped');
  const parts: string[] = [];
  if (sharp) parts.push(`${counted(sharp)} ${sharp === 1 ? 'rule' : 'rules'} sharpened`);
  if (born) parts.push(`${parts.length ? counted(born, true) : counted(born)} ${parts.length ? 'born' : born === 1 ? 'new rule' : 'new rules'}`);
  if (broke) parts.push(`${parts.length ? counted(broke, true) : counted(broke)} broke`);
  if (dropped) parts.push(`${parts.length ? counted(dropped, true) : counted(dropped)} dropped`);
  return parts.length ? parts.join(', ') : 'A quiet run everywhere';
}

function FeedRow({ handle, read, row, pic, rank, top, open, delay, onToggle, onOpen }: { handle: string; read: FeedRead; row: BoardFeeder | null; pic: string | null; rank: number; top: number; open: boolean; delay: number; onToggle: () => void; onOpen: (from: HTMLElement) => void }) {
  const reduce = Boolean(useReducedMotion());
  const lead = read.rules[0];
  const throne = rank === 1;
  const strength = Math.max(0.08, weightOf(read) / Math.max(1, top));
  const tp = row?.now?.tp ?? null;
  const counts = read.moves.reduce<Record<string, number>>((acc, move) => ({ ...acc, [move.move]: (acc[move.move] || 0) + 1 }), {});
  const hasFull = readable(handle);
  return (
    <motion.div layout="position" transition={{ layout: { duration: 0.6, ease: SOFT_EASE } }} className={`rbd-row fb-row${throne ? ' throne' : ''}`} data-open={open || undefined}>
      <button type="button" className="rbd-hd fb-hd" onClick={onToggle} aria-expanded={open} aria-label={`@${handle}: ${lead.law} ${open ? 'Close' : 'Show'} the read`}>
        <motion.span className="rbd-fill" aria-hidden="true" initial={reduce ? false : { scaleX: 0 }} animate={{ scaleX: strength }} transition={{ duration: 1.1, delay: (delay + 160) / 1000, ease: SOFT_EASE }} />
        {throne ? <motion.span className="rbd-crown" aria-hidden="true" initial={reduce ? false : { opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.9, delay: (delay + 300) / 1000 }} /> : null}
        <span className="rbd-n" aria-hidden="true">{pad2(rank)}</span>
        <span className="fb-av"><FeederStoryAvatar feeder={{ handle, profilePicUrl: pic }} className="text-[12px]" /></span>
        <span className="rbd-main">
          <span className="fb-who">@{handle} · run {read.run}</span>
          <b className="rbd-law">{lead.law}</b>
          <span className="fb-moves">
            {Object.entries(counts).map(([move, n]) => <span key={move} className={`mv ${MOVES[move]?.tone ?? ''}`}>{MOVES[move]?.word ?? move}{n > 1 ? ` ×${n}` : ''}</span>)}
            <span className={`mv call ${read.call.verdict}`}>{VERDICT[read.call.verdict]}</span>
          </span>
        </span>
        <span className="fb-form">
          <MiniRuns row={row} delay={delay + 260} />
          <em>{tp != null ? <>typical <b>top {tp}%</b></> : 'runs'}</em>
        </span>
        <span className="rbd-cv" aria-hidden="true"><FeedChevron open={open} /></span>
      </button>
      <AnimatePresence initial={false}>
        {open ? (
          <motion.div
            key="x"
            className="fb-x"
            initial={reduce ? { opacity: 0 } : { height: 0, opacity: 0 }}
            animate={reduce ? { opacity: 1 } : { height: 'auto', opacity: 1 }}
            exit={reduce ? { opacity: 0 } : { height: 0, opacity: 0 }}
            transition={{ height: { duration: 0.56, ease: SOFT_EASE }, opacity: { duration: 0.36 } }}
          >
            <div className="fb-in">
              <motion.p className="fb-line" initial={reduce ? false : { opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.6, delay: 0.12, ease: SOFT_EASE }}>{read.who}</motion.p>
              <motion.div className="fb-rules" initial={reduce ? false : { opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.6, delay: 0.2, ease: SOFT_EASE }}>
                <span className="k">Its rules</span>
                <ol>
                  {read.rules.map((rule, k) => (
                    <li key={rule.law}>
                      <span className="fb-rn">{k + 1}</span>
                      <b>{rule.law}</b>
                      <span className="fb-rs"><span><strong>{rule.carry[0]}</strong>/{rule.carry[1]} of top 10</span><span>held <strong>{rule.held[0]}</strong>/{rule.held[1]}</span><span className={`mv ${MOVES[rule.move]?.tone ?? ''}`}>{MOVES[rule.move]?.word ?? rule.move}</span></span>
                    </li>
                  ))}
                </ol>
              </motion.div>
              <motion.div className="fb-run" initial={reduce ? false : { opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.6, delay: 0.28, ease: SOFT_EASE }}>
                <span className="k">Run {read.run} · what moved</span>
                <p className="fb-head">{read.head}</p>
                <ul>
                  {read.moves.map((move) => <li key={move.law + move.move}><span className={`mv ${MOVES[move.move]?.tone ?? ''}`}>{MOVES[move.move]?.word ?? move.move}</span><span><b>{move.law}</b>{move.line}</span></li>)}
                </ul>
              </motion.div>
              <motion.div className={`fb-call ${read.call.verdict}`} initial={reduce ? false : { opacity: 0, y: 10 }} animate={{ opacity: 1, y: 0 }} transition={{ duration: 0.6, delay: 0.36, ease: SOFT_EASE }}>
                <span className="k">On the record</span>
                <p>{read.call.text}</p>
                <span className="fb-v"><b>{VERDICT[read.call.verdict]}</b>{read.call.line}</span>
                <span className="fb-next"><span className="k">Watching next</span>{read.next}</span>
              </motion.div>
              <motion.div className="fb-ft" initial={reduce ? false : { opacity: 0 }} animate={{ opacity: 1 }} transition={{ duration: 0.5, delay: 0.44 }}>
                {hasFull
                  ? <button type="button" className="tj-go" onClick={(event) => onOpen(event.currentTarget)}>Open the full read<Chevron /></button>
                  : <span className="fb-soon">Full read for @{handle} is a placeholder for now</span>}
              </motion.div>
            </div>
          </motion.div>
        ) : null}
      </AnimatePresence>
    </motion.div>
  );
}
const readable = (handle: string) => READ_POSTS[(READ_INDEX_OF(handle) ?? -1)] != null;

function FeedBoard({ feed, pics, chosen, rise, onFeeder }: { feed: RailFeed; pics: Map<string, string | null>; chosen: string | null; rise: Rise; onFeeder: OnPick }) {
  const list = useMemo(() => [feed], [feed]);
  const board = useBoards(list)[0];
  const rowOf = (handle: string) => board?.rows?.find((row) => row.handle === handle) || null;
  const [open, setOpen] = useState<string | null>(null);
  const reads = feed.feeders.filter((feeder) => FEED_READS[feeder.handle]).map((feeder) => ({ handle: feeder.handle, read: FEED_READS[feeder.handle] }))
    .sort((a, b) => weightOf(b.read) - weightOf(a.read));
  const waiting = feed.feeders.filter((feeder) => !FEED_READS[feeder.handle]);
  const top = Math.max(1, ...reads.map((item) => weightOf(item.read)));
  const calls = reads.map((item) => item.read.call.verdict);
  const called = calls.filter((v) => v === 'called').length, missed = calls.filter((v) => v === 'missed').length;
  const head = feedHeadline(reads.map((item) => item.read));
  const sub = `Across the ${plural(reads.length, 'feeder')} with a read. ${called || missed ? `On the record: ${called} called, ${missed} missed. ` : ''}${waiting.length ? `${counted(waiting.length)} still waiting on a first read.` : ''}`;
  let step = 0;
  return (
    <div className={`wrap ix fb${chosen ? ' picking' : ''}`}>
      <header className="ix-hd">
        <motion.span className="tj-k" style={at(step)} {...rise(step++)}><i />{titleCase(feed.title)} · {plural(feed.feeders.length, 'feeder')} · this run</motion.span>
        <h2 className="ix-h fb-h"><Words text={head} at={stepDelay(step++)} /></h2>
        <motion.p className="ix-p" {...rise(step++)}>{sub}</motion.p>
      </header>
      <motion.section className="rbd-board fb-board" aria-labelledby="rd-fb-t" {...rise(step++)}>
        <div className="rbd-bh"><div><h3 className="rbd-t" id="rd-fb-t">The read on each</h3><p>Headed by the rule that carries them · ranked by what moved · tap one to open it</p></div><span className="rbd-ct">{reads.length} read<br />{waiting.length} waiting</span></div>
        <LayoutGroup id={`rd-fb-${feed.id}`}>
          {reads.map((item, k) => {
            const rowStep = step++;
            return (
              <motion.div key={item.handle} {...rise(rowStep, true)}>
                <FeedRow
                  handle={item.handle}
                  read={item.read}
                  row={rowOf(item.handle)}
                  pic={pics.get(item.handle) ?? null}
                  rank={k + 1}
                  top={top}
                  open={open === item.handle}
                  delay={stepDelay(rowStep)}
                  onToggle={() => setOpen((cur) => (cur === item.handle ? null : item.handle))}
                  onOpen={(from) => onFeeder(item.handle, from)}
                />
              </motion.div>
            );
          })}
        </LayoutGroup>
      </motion.section>
      {waiting.length ? (
        <motion.section className="fb-wait" aria-labelledby="rd-fb-w" {...rise(step++)}>
          <div className="ix-gh"><h3 id="rd-fb-w"><i aria-hidden="true" />Waiting on a first read<span>{waiting.length}</span></h3><p>Their runs so far. The reader writes their rules once it has read enough posts.</p></div>
          <div className="fb-wl">
            {waiting.map((feeder, k) => {
              const row = rowOf(feeder.handle);
              const tp = row?.now?.tp ?? null;
              return (
                <button key={feeder.handle} type="button" className={`fb-w${chosen === feeder.handle ? ' chosen' : ''}`} onClick={(event) => onFeeder(feeder.handle, event.currentTarget)} aria-label={`@${feeder.handle}, not read yet`}>
                  <span className="fb-av"><FeederStoryAvatar feeder={{ handle: feeder.handle, profilePicUrl: pics.get(feeder.handle) ?? feeder.profilePicUrl }} className="text-[12px]" /></span>
                  <span className="fb-wn"><b>@{feeder.handle}</b><small>{row ? (row.total ? `${plural(row.total, 'post')} tracked` : 'No posts yet') : 'Loading runs…'}</small></span>
                  <MiniRuns row={row} delay={stepDelay(step) + k * 60} />
                  <span className="fb-wt">{tp != null ? `top ${tp}%` : '–'}</span>
                </button>
              );
            })}
          </div>
        </motion.section>
      ) : null}
      <p className="foot">Sample reads. @anuj.mp4 mirrors his full read; the other reads are hand-written placeholders until the reader runs on them.</p>
    </div>
  );
}

/* a feeder without a full read: what the posts already say */
function Lite({ feed, handle, readable: picks, chosen, rise, onFeeder }: { feed: RailFeed | null; handle: string; readable: RailFeeder[]; chosen: string | null; rise: Rise; onFeeder: OnPick }) {
  const list = useMemo(() => (feed ? [feed] : []), [feed]);
  const boards = useBoards(list);
  const row = boards[0]?.rows?.find((item) => item.handle === handle) || null;
  const loading = Boolean(feed) && !boards[0]?.rows && !boards[0]?.failed;
  const note = row ? noteFor(row) : null;
  const head = !row ? `@${handle}`
    : row.momentum === 'up' ? `@${handle} is on the up`
    : row.momentum === 'down' ? `@${handle} is sliding`
    : row.momentum === 'level' ? `@${handle} is holding steady`
    : `Too early to call @${handle}`;
  const run = row ? row.runs[row.runs.length - 1] || null : null;
  const recent = run ? [...run.items].reverse() : [];
  const firstEmpty = row && row.runs.length ? row.runs[row.runs.length - 1].r + 1 : 1;
  const best = row?.best || null;
  // the order things arrive in: the header's three lines, the trajectory, the run's posts, then the best post
  // and the reads
  const trajAt = 3;
  const postsAt = 4;
  const restAt = 5;

  return (
    <div className={`wrap ix ix-lite${chosen ? ' picking' : ''}`}>
      <header className="ix-hd">
        <motion.span className="tj-k" style={at(0)} {...rise(0)}><i />Not read yet{feed ? ` · ${titleCase(feed.title)}` : ''}</motion.span>
        {(() => {
          const text = loading ? `Reading @${handle}'s runs…` : head;
          return <h2 className="ix-h" key={text}><Words text={text} at={stepDelay(1)} /></h2>;
        })()}
        <motion.p className="ix-p" {...rise(2)}>
          {note ? `${note.full}. ` : ''}
          The full read, what @{handle} repeats, what ranks and why, isn&apos;t ready yet. This is what their tracked posts already show.
        </motion.p>
      </header>

      {row && row.total ? (
        <>
          <motion.section className="traj" aria-labelledby="rd-ix-traj" {...rise(trajAt)}>
            <div className="tj-hd">
              <span className="tj-k" id="rd-ix-traj"><i />The trajectory</span>
              <span className="tj-m">{plural(row.runs.length, 'run')} · {plural(row.total, 'post')}</span>
            </div>
            <div className="tj-runs" role="img" aria-label={runsLabel(row)}>
              {slotsOf(row).map((slot, k) => (slot ? (
                <span key={k} className={`tj-b${slot.live ? ' cur' : ''}`} style={{ ...shadeStyle(slot.tp ?? 100), ...cascade(stepDelay(trajAt), k, TRAJ) }}>
                  <span className="tj-c">
                    <i className="tj-f" />
                    <span className="tj-v">{slot.tp ?? '–'}{slot.tp != null ? <span>%</span> : null}</span>
                    <span className="tj-u">typ</span>
                  </span>
                  <small>{slot.live ? `${slot.count}/${RUN_SIZE}` : `Run ${slot.r}`}</small>
                </span>
              ) : (
                <span key={k} className="tj-b empty" aria-hidden="true" style={cascade(stepDelay(trajAt), k, TRAJ)}><span className="tj-c" /><small>Run {firstEmpty + (k - row.runs.length)}</small></span>
              )))}
            </div>
            <div className="tj-key" aria-hidden="true">
              <span className="tj-kl">Top 1%</span>
              <span className="tj-ramp" style={{ ['--ramp' as string]: RAMP }}><i className="mid" /></span>
              <span className="tj-kl">Bottom</span>
            </div>
          </motion.section>

          {recent.length ? (
            <motion.section className="ix-sec" aria-labelledby="rd-ix-recent" {...rise(postsAt)}>
              <div className="ix-sh">
                <span className="k" id="rd-ix-recent">{run?.live ? `This run so far · ${run.count} of ${RUN_SIZE}` : `Their last run · run ${run?.r}`}</span>
                <span className="k">Newest first</span>
              </div>
              <div className="ix-posts">
                {recent.map((post, k) => {
                  const enter = { ['--rd' as string]: `${stepDelay(postsAt) + 120 + k * STEP_MS}ms` };
                  const body = (
                    <>
                      <Cover post={post} className="ix-pth" />
                      <span className="ix-pc" style={post.pct != null ? shadeStyle(post.pct) : undefined}>{post.pct != null ? `Top ${top(post.pct)}%` : 'Unranked'}</span>
                      <small>{ago(row, post)}{post.checkpoint ? ` · ${post.checkpoint}` : ''}</small>
                    </>
                  );
                  return post.url
                    ? <a key={post.key} className="ix-post" style={enter} href={post.url} target="_blank" rel="noreferrer">{body}</a>
                    : <span key={post.key} className="ix-post" style={enter}>{body}</span>;
                })}
              </div>
            </motion.section>
          ) : null}
        </>
      ) : null}

      <motion.div className="ix-duo" {...rise(restAt)}>
        {best ? (
          <section className="ix-sec" aria-labelledby="rd-ix-best">
            <span className="k" id="rd-ix-best">Best of their last {Math.min(row?.total ?? 0, 100)}</span>
            {(() => {
              const body = (
                <>
                  <Cover post={best} className="ix-bth" />
                  <span className="ix-bt">
                    <b style={best.pct != null ? shadeStyle(best.pct) : undefined}>Top {top(best.pct)}%</b>
                    <small>{row ? ago(row, best) : ''}{best.checkpoint ? ` · ranked at ${best.checkpoint}` : ''}</small>
                  </span>
                </>
              );
              return best.url
                ? <a className="ix-best" href={best.url} target="_blank" rel="noreferrer">{body}</a>
                : <span className="ix-best">{body}</span>;
            })()}
          </section>
        ) : null}
        {picks.length ? (
          <section className="ix-sec" aria-labelledby="rd-ix-ready">
            <span className="k" id="rd-ix-ready">Full reads you can open now</span>
            <div className="ix-picks">
              {picks.map((feeder) => <PickRow key={feeder.handle} feeder={feeder} chosen={chosen === feeder.handle} onPick={onFeeder} />)}
            </div>
          </section>
        ) : null}
      </motion.div>
      {!loading && row && !row.total ? <p className="ix-p">The app hasn&apos;t tracked any posts from @{handle} yet.</p> : null}
    </div>
  );
}

export default function ReadIndex({
  feeds,
  feed,
  handle,
  readable: picks,
  onFeeder,
}: {
  feeds: RailFeed[] | null;
  feed: RailFeed | null;
  handle: string | null;
  // the feeders Read has full reads for, wherever they sit
  readable: RailFeeder[];
  // from: the row on this page the pick came from, which the page leaves around
  onFeeder: (handle: string, from?: HTMLElement | null) => void;
}) {
  const reduce = Boolean(useReducedMotion());
  const [chosen, setChosen] = useState<string | null>(null);
  const choose: OnPick = (pick, from) => {
    setChosen(pick);
    onFeeder(pick, from);
  };
  // everything rises in one after another, slowly; a long board stops adding delay after its first rows
  const rise: Rise = (step, depth = false) => (reduce
    ? { initial: { opacity: 0 }, animate: { opacity: 1 }, transition: { duration: 0.16 } }
    : {
      initial: depth ? { opacity: 0, y: 20, scale: 0.985 } : { opacity: 0, y: 16 },
      animate: depth ? { opacity: 1, y: 0, scale: 1 } : { opacity: 1, y: 0 },
      transition: { duration: depth ? 0.8 : 0.72, delay: stepDelay(step) / 1000, ease: SOFT_EASE },
    });
  const list = useMemo(() => (feed ? [feed] : feeds || []), [feed, feeds]);
  const pics = useMemo(() => {
    const map = new Map<string, string | null>();
    (feeds || []).forEach((item) => item.feeders.forEach((feeder) => {
      if (!map.get(feeder.handle)) map.set(feeder.handle, feeder.profilePicUrl);
    }));
    return map;
  }, [feeds]);
  const full = useMemo(() => new Set(picks.map((feeder) => feeder.handle)), [picks]);

  if (handle) {
    return <Lite feed={feed} handle={handle} readable={picks} chosen={chosen} rise={rise} onFeeder={choose} />;
  }
  if (feed && hasFeedRead(feed)) return <FeedBoard feed={feed} pics={pics} chosen={chosen} rise={rise} onFeeder={choose} />;
  return <Board list={list} all={!feed} pics={pics} full={full} chosen={chosen} rise={rise} onFeeder={choose} />;
}
