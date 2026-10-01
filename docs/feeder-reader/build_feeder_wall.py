"""Build the Feeder Wall prototype data.

Anuj: 40 real postcards + recent ranks from the SIM-W03 run packet. Order
inside a week is approximated from the card age bucket.
Lakme: 10 posts from the run-bites file (captions + placements, no postcards).

Everything under ANGLES / VIEWPOINTS / RUNS / TWINS is hand-written, standing
in for what the run-of-10 reader would produce from memory.
"""
import json, re, pathlib

HERE = pathlib.Path(__file__).resolve().parent
REPO = HERE.parents[1] if (HERE.parents[1] / 'apps').exists() else pathlib.Path('/home/user/feed_me')
req = json.loads((REPO / 'apps/web/src/data/terraReaderRuns/anuj-w03-request.json').read_text())
AGE = {'2 months ago': 9, '8 weeks ago': 8, '7 weeks ago': 7, '6 weeks ago': 6,
       '5 weeks ago': 5, '4 weeks ago': 4, '3 weeks ago': 3, '2 weeks ago': 2}
ordered = sorted(enumerate(req['current_posts']), key=lambda ip: (-AGE[ip[1]['posted']], -ip[0]))


def parse_card(md):
    sec, out = None, {}
    for raw in md.splitlines():
        line = raw.strip()
        if not line:
            continue
        if line in ('POST NAME', 'HEADER', 'WORK', 'FLOW', 'TEXT & REFERENCES', 'MECHANIC FINGERPRINT'):
            sec = line
            out.setdefault(sec, [])
            continue
        out.setdefault(sec, []).append(line)
    hdr, refs, flow = {}, {}, []
    for l in out.get('HEADER', []):
        if ':' in l:
            k, v = l.split(':', 1); hdr[k.strip()] = v.strip()
    for l in out.get('TEXT & REFERENCES', []):
        if ':' in l:
            k, v = l.split(':', 1); refs[k.strip()] = v.strip()
    for l in out.get('FLOW', []):
        m = re.match(r'^([0-9:.\-– ]+?)\s*[:—–-]?\s+(.*)$', l)
        if m and re.search(r'\d', m.group(1)):
            flow.append({'t': m.group(1).strip(' :—–-'), 'beat': m.group(2).strip()})
        else:
            flow.append({'t': '', 'beat': l})
    length = hdr.get('Length / Slides', '')
    if length.lower() in ('null', 'none', 'unknown', '1 reel') or 'observed' in length or length.startswith('0:00-') or length.startswith('00:00-'):
        length = ''
    named = refs.get('Named', '')
    return {
        'flow': flow,
        'hook': refs.get('Hook', '').strip('“”" '),
        'named': [] if named.lower() == 'none' else [n.strip() for n in named.split(',') if n.strip()],
        'length': length,
    }


# title: (scene, art, speaks_for, pokes, feeling, reader line)
ANGLES = {
    'Alert-Sound Reaction': ('bedroom', 'p1', 'Late sleepers', 'Himself', 'Sleeping through a national emergency.',
        'A bit about his own sleep. Nobody in it to call out but himself, and it sank into the bottom half.'),
    'Split by alarm': ('plain', 'p1', 'Mumbaikars', 'Delhi', 'Delhi panics. Mumbai shrugs.',
        'A Mumbai-vs-Delhi contrast. It points at another city instead of laughing at his own, and it held the middle.'),
    'Escalate Then Clap': ('home', 'p1', 'Flatmates', 'Friendship', 'Moving in together ruins everything.',
        'He plays both flatmates. A general premise with no tribe he belongs to, and it sat low.'),
    'Hard Cut to Notification': ('bedroom', 'p1', 'Creators', 'Him, lectured', '“Reels is not a job.”',
        'Arjun bhai lectures him, and Zomato’s notification answers back. He is the one being judged here, and this angle keeps landing in the bottom third.'),
    'Replace With AI': ('home', 'p3', 'Mums', 'The influencer son', 'Even an AI is a better son than an influencer.',
        'The creator gets replaced by an AI in his own home, and Blinkit saves Mother’s Day. The brand paid for his angle on creators, not for his face, and it is the ceiling of this memory.'),
    'Swap MJ Meaning': ('party', 'p2', 'Friends', 'The friend who gets it wrong', 'That one friend who mixes up every reference.',
        'A general friend type, not a tribe he is inside. District shows up only on the end card. Just below the middle.'),
    'Viralize the clip': ('home', 'p2', 'Creators', 'Creators after one hit', 'One viral video and suddenly it’s an “IP”.',
        'He and Ruhee play the creators he knows: 10 million views, IP talk, can’t pay the cafe bill. He is guilty of it too, which is why it stings. Near the top.'),
    'Force a Hug': ('night', 'p3', 'Anyone who has watched a road fight', 'The self-appointed mediator', 'Every road fight has one guy who takes charge.',
        'He plays the guy who appoints himself mediator, and the Kokilaben-or-“Ambani” line only a Mumbaikar says out loud. Upper third.'),
    'Roll and Cut': ('garage', 'car', 'His followers', 'His own flex', '“We get it bro.” You bought the car.',
        'The BMW arrives already heckled by its own caption. He calls the bluff on his own flex before anyone else can. Upper quarter.'),
    'Skip and React': ('car', 'p1', 'Guys', 'Himself', 'Editing out the cheating part.',
        'A confession about his own night. The camera is on him, and it settled below the middle.'),
    'Rig a Boom Battle': ('street', 'p3', 'The street', 'Wannabe rappers', 'Everyone’s a rapper until the shopkeeper judges.',
        'A shopkeeper judges a diss battle at a cosmetics stall. The crowd carries it into the upper half.'),
    'Split Life Updates': ('home', 'p1', 'Girls vs boys', 'Both', 'Girls get drama. Boys get a shrug.',
        'A gender contrast with no tribe he belongs to. It fell short of seven straight.'),
    'Hands over cash': ('balcony', 'p2', 'Unclear', 'Unclear', 'The card doesn’t show the angle.',
        'Cash handed to a worker on scaffolding. The reader can’t tell who the joke is on, so it stays unexplained.'),
    'Panic-Cut Through Street': ('night', 'p3', 'Mumbaikars outside SoBo', 'SoBo kids', 'SoBo kids think the world ends at Bombay.',
        'A class joke inside Mumbai, told by someone who knows exactly who the SoBo brats are. It beat four straight.'),
    'Flip whiteboard beats': ('luxe', 'p1', 'Marketing people', 'Parle’s marketing team', 'One meme, and suddenly it’s an “Italy expansion proposal”.',
        'He plays a marketing team milking “Melodi”: whiteboard, cash, robe, toast. The industry he works in, called out from inside. #2 in memory.'),
    'Invent a Driver': ('garage', 'p1', 'Anyone dodging blame', 'Himself', 'Blame someone who doesn’t exist.',
        'A self-recorded excuse plan. The joke is on his own scheme, and it held the middle.'),
    'Rapid-fire Answer Check': ('studio', 'p3', 'The sponsor', 'Nobody', 'No angle. A panel answers questions.',
        'A Startup Village panel. The sponsor bought a format, not his viewpoint. Every post before it landed higher.'),
    'Roast the Crowd': ('night', 'p3', 'The street', 'Everyone, then himself', 'Hosting a roast and losing control of it.',
        'Street mic in the Versace robe, ending with him quitting his own show. The middle.'),
    'Flip into Dad': ('gray', 'p1', 'Dads', 'Him', 'Dad thinks your Instagram is a waste.',
        'He plays his own dad calling his videos “waahiyat”. The camera turns on his life, and it drops to the bottom quarter.'),
    'Layer room with mashups': ('ui', 'ui', 'Unclear', 'Too many targets', 'A pile of references, no single target.',
        'Shahid, Kareena, Central Cee, Modi, Hanumankind. Lots of names and no one to call out. Near the floor.'),
    'Zoom Through Sorrow': ('gray', 'p1', 'Madrid fans', 'Mbappé', 'Leave for glory, watch your old club win it.',
        'A fan’s grief, played straight in grayscale with no dialogue. It beat four straight but stopped at the middle.'),
    'Glass-Review Parody': ('counter', 'p1', 'Himself', 'His own habit', '“Reviewing glasses so I drink more water.”',
        'A bit about his own kitchen counter, carried by sad violins and Sarabhai inserts. Just below the middle.'),
    'Swap and Style Jerseys': ('turf', 'p1', 'The brand', 'Nobody', 'No angle. The jerseys pose.',
        'Lotto x H&M jerseys, posed straight. No bluff to call and no feeling named, and it fell short of five straight.'),
    'Mock Complaint Loop': ('gray', 'p1', 'Him', 'Him, via Mom', '“My parents don’t appreciate me.”',
        'Mom teases his self-praise until he says it’s going on Instagram. The camera turns on him again, and it lands in the bottom quarter again.'),
    'Profile-switch Storyline': ('plain', 'p1', 'Middle-class friends', 'The rich friend', 'Rich friends and their “humble beginnings”.',
        'The rich friend’s hardship story collapses into his dad getting into racing. Right tribe, but solo in two profile shots, and it held the middle.'),
    'Reveal Car, Drive Off': ('night', 'car', 'His followers', 'His own flex', '“Get used to these.” Sorry, not sorry.',
        'The BMW again, mocking its own flex. It beat ten straight and ended a run of bits about his own life.'),
    'Say the tattoo': ('home', 'arm', 'Unclear', 'Unclear', 'The card doesn’t show the angle.',
        'One second of a forearm tattoo that says “Mauli”. Nothing to read an angle from. Near the floor.'),
    'Stutter Into Chaos': ('studio', 'p3', 'Writers', 'Actors with “inputs”', 'Hired to act, suddenly a writer.',
        'An industry tribe he knows, but it stays in a white room with no reveal. Below the middle.'),
    'Sundae Taste Tour': ('icecream', 'p2', 'Foodies', 'Nobody', 'No angle. A favourite place, shown.',
        'Baskin Robbins with a friend. It landed well, but there’s no viewpoint in it. The city is just where it happens.'),
    'Open with chord': ('piano', 'p1', 'Unclear', 'Nobody', 'No angle. A piano take.',
        'A 14-second piano take with no joke. It fell short of twelve straight.'),
    'Roast by voiceover': ('car', 'p1', 'Mumbaikars', 'Delhi', 'Delhi’s obsession is loud.',
        'A Delhi voice roasts him. Pointed at another city again, and again the middle.'),
    'Escalate a complaint': ('plain', 'p1', 'Ronaldo fans', 'Ronaldo fans, and FIFA', '“It’s rigged. Rigged for Messi.”',
        'He is the fan who takes losing personally, already rehearsing the next excuse. Solo, and it beat sixteen straight.'),
    'Mock-phone rant': ('gray', 'p1', 'Commuters', 'The civic body', '₹248 crore for 750 metres of flyover.',
        'A real Mumbai grievance, but delivered as a rant. He is amused by his city, not angry at it, and the anger didn’t land.'),
    'Food Tour Cuts': ('restaurant', 'p3', 'Foodies', 'Nobody', 'No angle. A restaurant, shown.',
        'O Pedro with friends. It landed in the top quarter, but the reader can’t credit a viewpoint. It’s hosting, not insight.'),
    'Freeze-Frame Reveal': ('store', 'p3', 'His followers', 'His own status', '“Anuj’s PA.” Correction: not Anuj’s PA.',
        'Broadway’s launch arrives through a staged fight and a PA who isn’t his. The brand bought his self-deflating angle. Upper half.'),
    'Sock Mistake Cut': ('office', 'p3', 'Office friends', 'The friend who used a sock', 'That one disgusting thing your friend did.',
        'His crew being idiots, filmed like an inside joke. No names, no message. It beat twenty straight.'),
    'Reframe with punchline': ('bedroom', 'p1', 'Every Mumbaikar', 'Andheri East', 'Being in a bad place has a postcode.',
        'Every Mumbaikar has been stuck in Andheri East. He laughs at his own city from inside it. It didn’t beat the sock reel before it, but that one was top 10%: strong company.'),
    'Taunt into play': ('turf', 'p3', 'Sunday-league guys', 'The show-off', 'Talk big, slide into the net.',
        'His crew on the turf, the trash talk undone by the slide into the net. Top quarter.'),
    'Cut to Ludo': ('office', 'p3', 'Agency people', 'Corporate seriousness', 'Every “urgent pitch” is a board game.',
        'The serious huddle turns out to be a Ludo game. His own work world, called out by people inside it. Top 20%.'),
    'Flip State Punchline': ('bedroom', 'p1', 'Nobody in particular', 'A whole state, from outside', 'A pun, not a feeling.',
        'The Andheri joke’s twin, pointed at UP. No Mumbaikar feeling behind it, just a double meaning on “state”. It fell short of all nine posts before it.'),
}

TWINS = [
    ('Reframe with punchline', 'Flip State Punchline', 'Same setup, same five-second two-hander, one word apart. Andheri East is a Mumbaikar laughing at his own city. UP is a pun about someone else’s home.'),
    ('Roll and Cut', 'Swap and Style Jerseys', 'Both show off a possession. The BMW heckles itself. The jerseys just pose.'),
    ('Replace With AI', 'Rapid-fire Answer Check', 'Both are paid. Blinkit bought his angle on creators. Startup Village bought a panel format.'),
    ('Flip whiteboard beats', 'Flip into Dad', 'Same solo performer. In one he calls out the industry. In the other, his dad calls him out.'),
]

# id, title, claim, body, core, contrast, exception, core_label, contrast_label, history{run: (movement, status)}
VIEWPOINTS = [
    ('bluff', 'Calls the bluff on his own tribe',
     'He is inside every circle he mocks: SoBo kids, creators after one hit, Parle’s marketing team, the rich friend, the agency pitch, his own BMW. The joke is the pretension, told by someone guilty of it too.',
     ['Panic-Cut Through Street', 'Viralize the clip', 'Flip whiteboard beats', 'Profile-switch Storyline', 'Cut to Ludo', 'Roll and Cut', 'Reveal Car, Drive Off', 'Freeze-Frame Reveal'],
     [], ['Stutter Into Chaos'], 'calling out his tribe', None,
     {1: ('new', 'emerging'), 2: ('strengthened', 'tested'), 3: ('sharpened', 'settled'), 4: ('strengthened', 'settled')}),
    ('local', 'Mumbai’s private jokes, from a local',
     'Andheri East as the bad place, SoBo kids lost outside Bombay, Kokilaben as the hospital you’d pick over “Ambani”. Only someone who lives the city has these feelings. Pointed at someone else’s home (Delhi, UP), it’s a stereotype from outside. Places he just visits (O Pedro, Baskin Robbins) don’t count.',
     ['Reframe with punchline', 'Panic-Cut Through Street', 'Force a Hug'],
     ['Split by alarm', 'Roast by voiceover', 'Flip State Punchline'], ['Mock-phone rant'], 'laughing at his own city', 'pointing at elsewhere',
     {4: ('new', 'emerging')}),
    ('crew', 'His crew, being idiots',
     'A sock used as a tissue, a pitch that is Ludo, trash talk undone by a slide into the net. No message and no target outside the group: the audience sees its own friends.',
     ['Sock Mistake Cut', 'Cut to Ludo', 'Taunt into play', 'Panic-Cut Through Street'],
     [], ['Stutter Into Chaos'], 'the crew’s own moments', None,
     {4: ('new', 'emerging')}),
    ('fan', 'The fan who takes losing personally',
     'Ronaldo’s loss is “rigged for Messi”. Mbappé watches PSG win it twice. Football works when he is the wounded fan, not the model in the jersey.',
     ['Escalate a complaint', 'Zoom Through Sorrow', 'Taunt into play'],
     ['Swap and Style Jerseys'], [], 'the wounded fan', 'football, no feeling',
     {3: ('new', 'emerging'), 4: ('strengthened', 'emerging')}),
    ('quiet', 'Turn the camera on him and the room goes quiet',
     'Dad on his “waahiyat” videos, Mom on his self-praise, Arjun bhai on reels not being a job, sleeping past 2 PM. When Anuj is the subject instead of the observer, the audience loses its seat next to him.',
     ['Flip into Dad', 'Mock Complaint Loop', 'Hard Cut to Notification', 'Alert-Sound Reaction', 'Skip and React', 'Glass-Review Parody'],
     [], ['Replace With AI'], 'Anuj as the subject', None,
     {1: ('new', 'emerging'), 2: ('strengthened', 'tested'), 3: ('strengthened', 'tested'), 4: ('held', 'tested')}),
    ('sponsor', 'Sponsors land when they buy his angle',
     'Blinkit paid for his take on creators (an AI out-sons the influencer). Broadway paid for him deflating himself (“not Anuj’s PA”). Startup Village and Lotto paid for a format with no angle, and they sit at the floor.',
     ['Replace With AI', 'Freeze-Frame Reveal'],
     ['Rapid-fire Answer Check', 'Swap and Style Jerseys', 'Swap MJ Meaning'], [], 'brand buys his angle', 'brand brings a format',
     {2: ('new', 'emerging'), 3: ('strengthened', 'tested'), 4: ('strengthened', 'tested')}),
]

RUNS = {
    1: ('Two ceilings on day one', 'The Blinkit story, where an AI out-sons the influencer, and “creators after one viral video” both land near the top. Both call the bluff on creators. The BMW arrives already heckled. The bits about his own life sink.'),
    2: ('He goes after the tribes', 'SoBo kids panicking outside Bombay and Parle’s “Melodi” whiteboard: two circles he belongs to, both called out, both top 30%. The Startup Village panel, a sponsor’s format with no angle, lands last.'),
    3: ('He turned the camera on himself', 'Mom, Dad in grayscale, a tattoo, a piano, his water-glass habit: seven of ten sink. The BMW reveal breaks the run by mocking his own flex and beats ten straight.'),
    4: ('Back in the room', 'Sock, Ludo, the turf taunt: his crew being idiots, all top 25%. Ronaldo’s “rigged” beats sixteen straight. Andheri East lands top 15% because every Mumbaikar has been in that bad place. UP is the same joke about someone else’s home, and it falls short of nine straight.'),
}

STANCE = ('The insider who calls everyone’s bluff.', 'His own included. He’s strongest inside the room, saying what everyone in it is thinking. He’s weakest when the room turns on him.')

posts = []
for order, (i, p) in enumerate(ordered):
    c = parse_card(p['post_card'])
    sc, art, sp, pk, fe, line = ANGLES[p['title']]
    posts.append({'id': f'a{order}', 'title': p['title'], 'age': p['posted'], 'rank_final': int(p['recent_rank'].split('/')[0]),
                  'scene': sc, 'art': art, 'speaks': sp, 'pokes': pk, 'feeling': fe, 'line': line, **c})
assert {p['title'] for p in posts} == set(ANGLES)
byt = {p['title']: p['id'] for p in posts}
ids = lambda names: [byt[n] for n in names]


def vp_out(vps, ids_):
    return [{'id': v[0], 'title': v[1], 'body': v[2], 'core': ids_(v[3]), 'contrast': ids_(v[4]), 'exception': ids_(v[5]),
             'coreLabel': v[6], 'contrastLabel': v[7], 'history': {str(k): {'movement': a, 'status': b} for k, (a, b) in v[8].items()}} for v in vps]


anuj = {
    'handle': 'anuj.mp4', 'kind': 'Creator', 'runs': 4, 'cap': 100, 'unit': 'postcards',
    'stance': STANCE, 'posts': posts, 'viewpoints': vp_out(VIEWPOINTS, ids),
    'runNotes': {str(k): {'head': a, 'body': b} for k, (a, b) in RUNS.items()},
    'twins': [{'a': byt[a], 'b': byt[b], 'line': l} for a, b, l in TWINS],
    'source': 'Ranks and postcards from the SIM-W03 run packet. Order inside each week is approximate.',
}

# ---- Lakme -----------------------------------------------------------------
lk = json.loads((REPO / 'apps/web/src/data/runSignals/lakmeindia_run_bites.json').read_text())
LK = [  # title, scene, art, speaks, pokes(aims), feeling, line
    ('Council of Makeup Lovers', 'lips', 'p3', 'Makeup lovers', 'The community', 'You’re part of the council.', 'The caption talks to a community, and the product arrives on a gesture. Upper quarter.'),
    ('Downloading in Progress', 'phone', 'ui', 'Unclear', 'Unclear', 'A look, loading.', 'A loading-screen overlay. It kept climbing after day one and finished mid-run.'),
    ('Golden Hour on My Lips', 'gold', 'p1', 'The wearer', 'Her own look', 'Wearing the golden hour.', 'A person wears the claim in first person. Top 18%.'),
    ('Eyeliner Does It All', 'shelf', 'product', 'The product', 'Nobody', 'The eyeliner does it all.', 'The product alone, as a packshot. Bottom half.'),
    ('Lashes Get the Main Role', 'nude', 'p1', 'The product', 'Nobody', 'Your lashes, main character.', 'The product is the subject even with a face in frame. Bottom half.'),
    ('We Celebrate Your Skin', 'phone', 'ui', 'You', 'Unclear', 'Celebrating your skin.', 'A screen-style overlay about skin. Middle of the run.'),
    ('Wand Came to Work', 'shelf', 'product', 'The product', 'Nobody', 'This wand came to work.', 'A packshot that kept climbing after day one but stayed in the bottom half.'),
    ('Her Day’s Packed, Ice D', 'mint', 'p1', 'Busy women', 'A packed day', 'Her day’s packed; this keeps her fresh.', 'A woman’s day leads and the product follows. The best of the run: top 10%, 3.7× usual views.'),
    ('First Job Confusion', 'nude', 'p1', 'First-jobbers', 'Corporate confusion', 'First-job confusion, in Bengali.', 'A person and a real moment, tested on skin. Middle of the run.'),
    ('Eyeconic Instants', 'phone', 'product', 'The product', 'Nobody', 'Instants that look “Eyeconic”.', 'A product pun over a screen overlay. The lowest placement of the run.'),
]
lk_posts = []
for n, pp in enumerate(lk['run_stats']['per_post']):
    t, sc, art, sp, pk, fe, line = LK[n]
    lk_posts.append({'id': f'l{n}', 'title': t, 'age': 'this run', 'pct_fixed': int(pp['placed'].split()[1].rstrip('%')),
                     'scene': sc, 'art': art, 'speaks': sp, 'pokes': pk, 'feeling': fe, 'line': line,
                     'hook': pp['post'].strip() + '…', 'flow': [], 'named': [], 'length': '',
                     'views': pp['views_vs_usual'], 'legs': pp['legs']})
lkb = {p['title']: p['id'] for p in lk_posts}
lids = lambda names: [lkb[n] for n in names]
LK_VP = [
    ('person', 'A woman’s day leads, the product follows',
     'When a person and her moment come first (a packed day, “my lips”, the makeup lovers), the post places in the top quarter. When the product is the subject (the eyeliner does it all, the wand came to work), it places in the bottom half. Ten posts and captions only: a first sighting.',
     ['Her Day’s Packed, Ice D', 'Golden Hour on My Lips', 'Council of Makeup Lovers'],
     ['Eyeliner Does It All', 'Lashes Get the Main Role', 'Wand Came to Work', 'Eyeconic Instants'], ['First Job Confusion'],
     'person leads', 'product leads', {1: ('new', 'emerging')}),
    ('overlay', 'Screen overlays are a habit, not a reason',
     'Four of ten posts wear a phone-screen overlay, and they place anywhere from top 10% to top 61%. It’s how Lakmé dresses a post right now. On its own, it doesn’t say where a post will land.',
     ['Downloading in Progress', 'We Celebrate Your Skin', 'Her Day’s Packed, Ice D', 'Eyeconic Instants'], [], [],
     'overlay posts', None, {1: ('new', 'emerging')}),
]
lakme = {
    'handle': 'lakmeindia', 'kind': 'Brand', 'runs': 1, 'cap': 100, 'unit': 'posts',
    'stance': ('Too early to call.', 'One run and captions only. One split is already visible: Lakmé lands when a woman’s day leads and the product follows.'),
    'posts': lk_posts, 'viewpoints': vp_out(LK_VP, lids),
    'runNotes': {'1': {'head': 'One run in, captions only', 'body': 'All ten beat Lakmé’s usual views, so this run is about which beat them most. Her packed day with Ice D tops it at 3.7×. The four posts where the product is the subject all land in the bottom half.'}},
    'twins': [{'a': lkb['Her Day’s Packed, Ice D'], 'b': lkb['Eyeconic Instants'], 'line': 'Both wear a screen overlay. One puts a woman’s day first, the other a product pun. Top 10% against top 61%.'}],
    'source': 'Ten posts from the run-bites file: captions, placements and the media log. No postcards yet. Order as recorded.',
}

data = json.dumps({'feeders': [anuj, lakme]}, ensure_ascii=False)
for target in [HERE / 'feeder-wall.html']:
    if target.exists():
        html = target.read_text()
        s = html.index('const DATA = ') + len('const DATA = ')
        e = html.index(';\nconst ', s)
        target.write_text(html[:s] + data + html[e:])

print('posts', len(posts), 'lakme', len(lk_posts))
