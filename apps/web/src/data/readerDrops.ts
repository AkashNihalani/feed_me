/* Build the Reader Drop preview from records that already ship on main.
   New model outputs belong outside the frontend tree. */

import { buildReaderDrops, type ReaderRunOutput, type ReaderRunPost } from '@/lib/readerDropModel';

import week1Output from './terraReaderRuns/anuj-w01-output.json';
import week1Request from './terraReaderRuns/anuj-w01-request.json';
import week2Output from './terraReaderRuns/anuj-w02-output.json';
import week2Request from './terraReaderRuns/anuj-w02-request.json';
import week3Output from './terraReaderRuns/anuj-w03-output.json';
import week3Request from './terraReaderRuns/anuj-w03-request.json';

type ReaderRequest = { current_posts: ReaderRunPost[] };
type PostRef = { post_key: string; post_url?: string };

const POST_REFS: Record<string, PostRef> = {
  'Cut to Ludo': { post_key: 'p/dzkrx9csxqm#f27' },
  'Escalate a complaint': { post_key: 'p/dzuomhbsmqt#f27' },
  'Flip State Punchline': { post_key: 'p/dzmolq6sr0-#f27' },
  'Flip into Dad': { post_key: 'p/dy4jhvtskgr#f27' },
  'Flip whiteboard beats': { post_key: 'p/dyjcvjqmpbo#f27' },
  'Food Tour Cuts': { post_key: 'p/dzrtr7rsprw#f27' },
  'Force a Hug': { post_key: 'p/dx6do9cspq9#f27' },
  'Freeze-Frame Reveal': { post_key: 'p/dznjbxamb0n#f27' },
  'Glass-Review Parody': { post_key: 'p/dzh95wcsja-#f27' },
  'Invent a Driver': { post_key: 'p/dygn2gvs6xg#f27' },
  'Layer room with mashups': { post_key: 'p/dzc7xuwm4au#f27' },
  'Mock Complaint Loop': { post_key: 'p/dy7d3rqm_tg#f27' },
  'Mock-phone rant': { post_key: 'p/dzuaw9kmoa3#f27' },
  'Profile-switch Storyline': { post_key: 'p/dzdnzvpmewh#f27' },
  'Reframe with punchline': { post_key: 'p/dzhyigqbvs0#f27' },
  'Reveal Car, Drive Off': { post_key: 'p/dy6e1m0maze#f27' },
  'Rig a Boom Battle': { post_key: 'p/dyhdg4ogcal#f27' },
  'Roast the Crowd': { post_key: 'p/dywjnk2mrli#f27' },
  'Roll and Cut': { post_key: 'p/dx7gl9pb0fm#f27' },
  'Swap and Style Jerseys': { post_key: 'p/dzfnnejsqsa#f27' },
  'Zoom Through Sorrow': { post_key: 'p/dy-zop6srgl#f27' },
};

function record(output: unknown, request: unknown) {
  return {
    output: output as ReaderRunOutput,
    posts: (request as ReaderRequest).current_posts.map((post) => {
      const ref = POST_REFS[post.title];
      return ref
        ? {
            ...post,
            ...ref,
            thumbnail_url: `/api/media?postKey=${encodeURIComponent(ref.post_key)}&role=thumbnail`,
          }
        : post;
    }),
  };
}

export const READER_FEEDERS = [
  {
    handle: 'anuj.mp4',
    drops: buildReaderDrops('anuj.mp4', [
      record(week1Output, week1Request),
      record(week2Output, week2Request),
      record(week3Output, week3Request),
    ]),
  },
] as const;
