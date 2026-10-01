"""Build the Memory Skyline prototype data from the repo's sample runs.

Anuj: 40 real postcards + recent ranks from SIM-W03 request. Chronology is the
card age bucket; order inside a week is approximated.
Lakme: 10 posts from the run-bites file (captions + placements, no postcards).
Threads, readings and per-post reader lines are hand-authored here, standing in
for what the run-of-10 reader would write.
"""
import json, re, statistics, pathlib

REPO = pathlib.Path(__file__).resolve().parents[2]
req = json.loads((REPO / 'apps/web/src/data/terraReaderRuns/anuj-w03-request.json').read_text())

AGE = {'2 months ago': 9, '8 weeks ago': 8, '7 weeks ago': 7, '6 weeks ago': 6,
       '5 weeks ago': 5, '4 weeks ago': 4, '3 weeks ago': 3, '2 weeks ago': 2}
posts = sorted(enumerate(req['current_posts']), key=lambda ip: (-AGE[ip[1]['posted']], -ip[0]))


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
    hdr = {}
    for l in out.get('HEADER', []):
        if ':' in l:
            k, v = l.split(':', 1)
            hdr[k.strip()] = v.strip()
    refs = {}
    for l in out.get('TEXT & REFERENCES', []):
        if ':' in l:
            k, v = l.split(':', 1)
            refs[k.strip()] = v.strip()
    flow = []
    for l in out.get('FLOW', []):
        m = re.match(r'^([0-9:.\-– ]+?)\s*[:—–-]?\s+(.*)$', l)
        if m and re.search(r'\d', m.group(1)):
            flow.append({'t': m.group(1).strip(' :—–-'), 'beat': m.group(2).strip()})
        else:
            flow.append({'t': '', 'beat': l})
    length = hdr.get('Length / Slides', '')
    if length.lower() in ('null', 'none', 'unknown', '1 reel'):
        length = ''
    named = refs.get('Named', '')
    return {
        'work': ' '.join(out.get('WORK', [])),
        'flow': flow,
        'hook': refs.get('Hook', '').strip('“”"'),
        'named': [] if named.lower() == 'none' else [n.strip() for n in named.split(',') if n.strip()],
        'mechanic': ' '.join(out.get('MECHANIC FINGERPRINT', [])).replace('->', '→'),
        'surface': hdr.get('Surface', ''),
        'length': length,
        'caption': hdr.get('Caption', ''),
    }


# ---- Threads: what this feed keeps returning to -------------------------
ANUJ_THREADS = [
    ('cast', 'crew', 'With a crew or crowd', ['Replace With AI', 'Swap MJ Meaning', 'Viralize the clip', 'Force a Hug', 'Rig a Boom Battle', 'Hands over cash', 'Panic-Cut Through Street', 'Rapid-fire Answer Check', 'Roast the Crowd', 'Layer room with mashups', 'Stutter Into Chaos', 'Sundae Taste Tour', 'Food Tour Cuts', 'Freeze-Frame Reveal', 'Sock Mistake Cut', 'Taunt into play', 'Cut to Ludo']),
    ('cast', 'solo', 'Anuj alone', ['Alert-Sound Reaction', 'Split by alarm', 'Escalate Then Clap', 'Hard Cut to Notification', 'Skip and React', 'Split Life Updates', 'Flip whiteboard beats', 'Invent a Driver', 'Flip into Dad', 'Zoom Through Sorrow', 'Glass-Review Parody', 'Swap and Style Jerseys', 'Mock Complaint Loop', 'Profile-switch Storyline', 'Reveal Car, Drive Off', 'Roast by voiceover', 'Escalate a complaint', 'Mock-phone rant', 'Reframe with punchline', 'Flip State Punchline']),
    ('cast', 'both', 'He plays both sides', ['Escalate Then Clap', 'Split Life Updates', 'Flip into Dad', 'Profile-switch Storyline', 'Flip State Punchline', 'Reframe with punchline', 'Split by alarm']),
    ('place', 'mumbai', 'Mumbai as home', ['Force a Hug', 'Panic-Cut Through Street', 'Sundae Taste Tour', 'Food Tour Cuts', 'Freeze-Frame Reveal', 'Mock-phone rant', 'Reframe with punchline']),
    ('place', 'elsewhere', 'Pointed at elsewhere', ['Split by alarm', 'Roast by voiceover', 'Flip State Punchline']),
    ('world', 'football', 'Football', ['Escalate a complaint', 'Zoom Through Sorrow', 'Taunt into play', 'Swap and Style Jerseys']),
    ('world', 'industry', 'The content business', ['Viralize the clip', 'Flip whiteboard beats', 'Cut to Ludo', 'Stutter Into Chaos']),
    ('world', 'trial', 'Creator on trial at home', ['Hard Cut to Notification', 'Flip into Dad', 'Mock Complaint Loop', 'Replace With AI']),
    ('world', 'status', 'BMW, robe, cash, PA', ['Roll and Cut', 'Reveal Car, Drive Off', 'Flip whiteboard beats', 'Roast the Crowd', 'Rig a Boom Battle', 'Stutter Into Chaos', 'Hands over cash', 'Freeze-Frame Reveal']),
    ('money', 'sponsor', 'Paid partner', ['Replace With AI', 'Swap MJ Meaning', 'Swap and Style Jerseys', 'Rapid-fire Answer Check', 'Freeze-Frame Reveal']),
    ('craft', 'nameturn', 'One name is the punchline', ['Reframe with punchline', 'Flip State Punchline', 'Escalate a complaint', 'Flip whiteboard beats', 'Swap MJ Meaning', 'Zoom Through Sorrow']),
    ('craft', 'gray', 'Grayscale scenes', ['Skip and React', 'Flip into Dad', 'Zoom Through Sorrow', 'Glass-Review Parody', 'Mock Complaint Loop', 'Mock-phone rant']),
    ('craft', 'short', 'Under 10 seconds', ['Flip State Punchline', 'Reframe with punchline', 'Say the tattoo']),
]

ANUJ_LINES = {
    'Alert-Sound Reaction': 'Waking up after 2 PM to the national emergency alert test. Alone in bed, with a news moment the feed never comes back to, it stayed in the bottom half.',
    'Split by alarm': 'Delhi panics, Mumbai shrugs: the same face under two labels. The city comparison held the middle. This feed does better when Mumbai is home than when it is one side of a contrast.',
    'Escalate Then Clap': 'One actor plays both flatmates, from moving in to a dark confession to a hand-slap routine. A solo two-hander with no thread to hold it, and it sat low.',
    'Hard Cut to Notification': 'Arjun bhai is told reels are not a job, then Zomato’s notification answers back. The creator on trial, alone in a bedroom. This thread keeps landing in the bottom third.',
    'Replace With AI': 'His mother swaps the “macro influencer” son for an AI helper, and Blinkit plus a boAt watch close out Mother’s Day. A full cast, a home story, a sponsor with a job inside the plot: the ceiling of this memory.',
    'Swap MJ Meaning': 'Michael Jordan or Michael Jackson: an outing, a cutout, a party. District arrives only on the end card. A story with a partner, but the partner has no part in it, and it landed just below the middle.',
    'Viralize the clip': '“Creators after that one viral video”: 10 million views, IP talk, a cafe bill they can’t pay. The content business sent up from inside it, with Ruhee Dosani beside him, near the top.',
    'Force a Hug': 'Anuj appoints himself mediator at a roadside crash: breathing drills, Kokilaben vs “Ambani”, a forced three-way hug. Real street, real strangers, a Mumbai hospital as the punchline.',
    'Roll and Cut': 'The matte black BMW, introduced under “we get it bro 🙏”. The car arrives already heckled by its own caption, and it landed in the upper third.',
    'Skip and React': 'A backseat story that drops into grayscale when he skips the cheating part. A solo bit about his own night, and it settled just below the middle.',
    'Rig a Boom Battle': 'A diss battle at a cosmetics stall: three rappers, a shopkeeper who says “Boom” and picks the winner. Street, crowd, and a judge who isn’t Anuj. The crowd carries it.',
    'Split Life Updates': 'Girls’ life updates as drama, boys’ as a shrug, played in wigs. The contrast doesn’t plug into anything the feed returns to, and it fell short of seven straight.',
    'Hands over cash': '“anuj.mp4 hangs out with new friend”: cash handed to a worker on bamboo scaffolding. Money shown without a heckle, and it sat below the middle.',
    'Panic-Cut Through Street': 'SoBo kids discovering a world outside Bombay: five people, a night street, an auto-rickshaw. A Mumbai joke told from inside Mumbai, with a crew. It beat four straight.',
    'Flip whiteboard beats': '“Parle marketing team rn”: MODI and MELONY on a FLAMES board, an Italy expansion plan, cash to the ear, the Versace robe. He plays the industry and mocks the money. It beat nine straight; only the Blinkit story sits above it.',
    'Invent a Driver': 'He blames a parking mess on a driver who doesn’t exist, all the way down to the basement. Solo on camera, but working on real people, and it held the middle.',
    'Rapid-fire Answer Check': 'A Startup Village Mumbai panel: college, Claude Code or Codex, subreddits. A crew, but the format belongs to the sponsor, not to his world. Every post before it in memory landed higher.',
    'Roast the Crowd': 'Street mic in the Versace robe: two roasts, a crowd vote, and Anuj quitting his own show. The crowd carries it to the middle; the robe is his status costume.',
    'Flip into Dad': 'His dad, played by him in grayscale: the ₹3000, the engineering he skipped, the “waahiyat” Instagram videos. The creator on trial at home, in the bottom quarter.',
    'Layer room with mashups': 'A Clubhouse-style room with singing, rap and cutouts of Shahid, Kareena, Modi and Hanumankind. Plenty of names, none of them a thread this feed runs, and it landed near the floor.',
    'Zoom Through Sorrow': 'Mbappé in a Madrid shirt, watching PSG win back to back. Solo, grayscale, no dialogue. Football gave it enough to beat four straight and end a soft patch, but it only reached the middle.',
    'Glass-Review Parody': 'He reviews drinking glasses with sad violins, a Bollywood father and a Sarabhai insert. A solo bit at his own kitchen counter, held up by references, and it sat just below the middle.',
    'Swap and Style Jerseys': 'Lotto x H&M: title cards, jerseys, ball tricks, a rooftop pose. The one post where a product poses and nobody heckles it. It fell short of five straight.',
    'Mock Complaint Loop': '“my parents don’t appreciate me”: his mother, off camera, teasing the self-praise until it ends with “this is going on Instagram”. The creator on trial at home again, and again in the bottom quarter.',
    'Profile-switch Storyline': 'The rich friend’s “humble beginnings” collapse into his dad getting into racing. Status heckled, but solo in two profile shots, and it held the middle.',
    'Reveal Car, Drive Off': 'The BMW again: “yall just have to get used to these”. A reveal that mocks its own flex. It beat ten straight and ended a run of home bits in the bottom half.',
    'Say the tattoo': 'One second: a forearm tattoo that says “Mauli”. Nothing in it connects to what the feed keeps returning to, and it landed near the floor.',
    'Stutter Into Chaos': '“Comics when they get hired to act but suddenly have writing inputs.” An industry joke, but it stays in a white studio with no reveal, and it sat below the middle.',
    'Sundae Taste Tour': '“We’re in Mumbai, and our favourite ice cream place in the whole wide world”: Baskin Robbins, a Dubai chocolate sundae. Mumbai as home turf. It beat both posts before it.',
    'Open with chord': 'A 14-second grand piano take with no joke and no names. The feed has never shown what this is for, and it fell short of twelve straight.',
    'Roast by voiceover': '“watching obsession: Delhi”: a Delhi voice roasts him from off camera. Mumbai against Delhi again, and again the middle.',
    'Escalate a complaint': '“Practicing for when Ronaldo loses yet another WC”: it’s rigged, it’s for Messi, he’s FIFA’s boy. Solo, but plugged straight into football, and it beat sixteen straight.',
    'Mock-phone rant': '₹248 crore for 750 m of unfinished Mrinal Tai Gore flyover, shouted into a plaster bust. A Mumbai grievance, but solo and grayscale, and the city couldn’t lift it. This is the exception to the Mumbai read.',
    'Food Tour Cuts': '“We’re in Mumbai at what people say is one of…”: O Pedro, coconut prawns, burrata. Three people, a Mumbai table, and the top quarter.',
    'Freeze-Frame Reveal': 'Broadway’s Bandra launch arrives through a bleeped store fight and Aysha Sultan, “Not Anuj’s PA”. The sponsor gets a scene and the status gets corrected on screen. Upper half, just under the food tour before it.',
    'Sock Mistake Cut': 'A sock handed over as a tissue, an urgent office warning, a scream. No names at all, just a crew and a physical reveal. It beat twenty straight, back to the Parle whiteboard.',
    'Reframe with punchline': '“In a bad place rn.” “Damn. Andheri East.” Five seconds, solo, one name. It didn’t beat the sock reel just before it, but that one was top 10%: strong company. The name belongs to his city, and the city carried it.',
    'Taunt into play': 'A turf taunt, a slide into the net, a collision. A crew and football, in the top quarter. It only fell short of the two posts before it, both top 15%.',
    'Cut to Ludo': '“Humari pitch mein…”: the serious huddle turns out to be a Ludo game in an office lounge. The work world sent up from inside it, with a crew of collaborators.',
    'Flip State Punchline': 'The Andheri joke’s twin. “In a bad state rn.” “UP.” Same setup, same solo two-hander, one word different. UP is a name this feed has never lived with, and it fell short of all nine posts before it.',
}

ANUJ_READINGS = [
    {
        'id': 'heckle', 'threads': ['status'],
        'title': 'Status Needs a Heckler',
        'read': 'Anything expensive arrives with its own heckle: the matte BMW under “we get it bro”, then “get used to these”; the Versace robe worn to a street roast; cash slapped against a leg; “Anuj’s PA” corrected to “Not Anuj’s PA”. These sit in the upper half. When money or product shows up straight (cash handed over, Lotto x H&M jerseys posed with no joke), it sinks.',
        'evidence': ['Roll and Cut', 'Reveal Car, Drive Off', 'Flip whiteboard beats', 'Freeze-Frame Reveal', 'Roast the Crowd', 'Profile-switch Storyline', 'Hands over cash', 'Swap and Style Jerseys'],
        'boundary': [],
        'history': {
            1: ('new', 'emerging', None, 'The BMW arrives under “we get it bro” and lands in the upper third. One post, one rule to watch.'),
            2: ('strengthened', 'tested', None, 'The Parle whiteboard and the robe at the street roast: the heckle now covers cash and costume, not just the car.'),
            3: ('sharpened', 'settled', None, 'The BMW reveal beats ten straight. The Lotto jerseys, posed with no heckle, sink.'),
            4: ('held', 'settled', None, '“Not Anuj’s PA” keeps the rule. Nothing this run argues with it.'),
        },
        'was': {3: ('Pure jersey styling is the exception: sometimes the product simply gets to pose.', 'The jerseys are the proof. The one time a product posed without a heckle, it fell near the floor.')},
    },
    {
        'id': 'crew', 'threads': ['crew', 'solo'],
        'title': 'Alone, He Needs One of the Feed’s Threads',
        'read': 'With a crew or a crowd, the scene carries itself: the sock on the sofa, Ludo in the office, the roadside hug, the SoBo panic. Alone, a bit lands only when it plugs into something the feed keeps returning to: Ronaldo and FIFA, Parle’s marketing team, Andheri East. Alone with a name the feed has never used (UP, the emergency alert test) or with his own home life (dad, mother, his glasses), it settles in the bottom half.',
        'evidence': ['Sock Mistake Cut', 'Cut to Ludo', 'Taunt into play', 'Force a Hug', 'Panic-Cut Through Street', 'Escalate a complaint', 'Flip whiteboard beats', 'Reframe with punchline', 'Flip State Punchline', 'Alert-Sound Reaction', 'Flip into Dad', 'Mock Complaint Loop'],
        'boundary': ['Rapid-fire Answer Check', 'Zoom Through Sorrow'],
        'history': {
            2: ('new', 'emerging', 'A Crowd Carries the Scene', 'Street mic, boom battle, SoBo panic: crowds land in the upper half. Solo bits keep sinking.'),
            3: ('sharpened', 'tested', None, 'Solo isn’t the problem. The BMW reveal is solo and beat ten straight. Alone, the post needs a thread.'),
            4: ('sharpened', 'tested', None, 'Andheri East and UP split the same joke. Alone, the name has to be one the feed already lives with. Sock, Ludo and the turf taunt confirm the crew side.'),
        },
        'was': {3: ('A crowd carries the scene.', 'A crowd carries the scene. Alone, he needs one of the feed’s threads.')},
    },
    {
        'id': 'industry', 'threads': ['industry', 'trial'],
        'title': 'He Sends Up the Industry From Inside It',
        'read': 'When Anuj plays the business of content (Parle’s marketing team, creators after one viral video, a pitch huddle that turns out to be Ludo), he lands near the very top. When the creator is the one on trial at home (dad calling the videos “waahiyat”, mom teasing the self-praise, “scrolling reels is not a job”), it sinks to the bottom third. The joke works when he is in on the industry, not defending himself to his family.',
        'evidence': ['Flip whiteboard beats', 'Viralize the clip', 'Cut to Ludo', 'Flip into Dad', 'Mock Complaint Loop', 'Hard Cut to Notification'],
        'boundary': ['Replace With AI', 'Stutter Into Chaos'],
        'history': {
            1: ('new', 'emerging', None, '“Creators after that one viral video” lands near the top; the “reels is not a job” lecture sinks. Two posts pointing the same way.'),
            2: ('strengthened', 'tested', None, 'Parle’s whiteboard takes #2. Dad’s grayscale monologue drops to the bottom quarter.'),
            3: ('held', 'tested', None, 'Mom’s “going on Instagram” sinks too. The writers’-room joke stays middling: the industry needs a reveal, not just a premise.'),
            4: ('strengthened', 'tested', None, 'The pitch that turns out to be Ludo lands in the top 20%.'),
        },
    },
    {
        'id': 'mumbai', 'threads': ['mumbai', 'elsewhere'],
        'title': 'Mumbai Is a Cast Member Nobody Introduces',
        'read': 'The city is used by name and never explained: Andheri East as a punchline, SoBo kids lost outside Bombay, Kokilaben as the hospital to beat, O Pedro and Baskin Robbins as “our place”, Broadway in Bandra. These land in the top third. When the joke points away from the city (Delhi’s alarm, Delhi’s obsession, UP), it slides to the middle or below. The city has to be home, not one side of a comparison.',
        'evidence': ['Reframe with punchline', 'Panic-Cut Through Street', 'Force a Hug', 'Food Tour Cuts', 'Sundae Taste Tour', 'Freeze-Frame Reveal', 'Split by alarm', 'Roast by voiceover', 'Flip State Punchline'],
        'boundary': ['Mock-phone rant'],
        'history': {
            4: ('new', 'emerging', None, 'The reader had seen Mumbai for weeks without knowing it mattered. Andheri East and UP split the same joke, and the city became a reading.'),
        },
    },
    {
        'id': 'sponsor', 'threads': ['sponsor'],
        'title': 'Sponsors Need a Part in the Scene',
        'read': 'Brands land when they get a role in his story: Blinkit and an AI helper win back his mother for Mother’s Day (the top post in memory), and Broadway’s launch arrives through a staged fight and a fake PA. When the brand brings its own format (a Startup Village rapid-fire panel, a Lotto x H&M styling reel), it lands at the very bottom. District’s MJ mix-up sits between: a story, but the brand only shows up on the end card.',
        'evidence': ['Replace With AI', 'Freeze-Frame Reveal', 'Swap MJ Meaning', 'Swap and Style Jerseys', 'Rapid-fire Answer Check'],
        'boundary': [],
        'history': {
            3: ('new', 'emerging', None, 'Blinkit’s story sits at #1; the Startup Village panel and Lotto’s jerseys sit at the floor. Same account, opposite ends.'),
            4: ('strengthened', 'emerging', None, 'Broadway’s launch gets a scene and lands in the upper half.'),
        },
    },
]

ANUJ_RUN_NOTES = {
    1: ('Two ceilings on day one', 'First ten posts. Two set the ceiling straight away: the Blinkit Mother’s Day story, where an AI replaces the influencer son, and the “creators after one viral video” duo. The BMW arrived already mocked (“we get it bro”) and landed in the upper third. The solo bedroom bits sank. Too early to call anything but the status joke.'),
    2: ('The crowd run', 'A street mic roast, a cosmetics-stall boom battle and SoBo kids panicking outside Bombay all landed in the upper half. The Parle “Melodi” whiteboard beat nine straight to take #2. The Startup Village rapid-fire panel landed below every post in memory.'),
    3: ('A home run, and it showed', 'Glasses reviewed in grayscale, the rich friend’s humble beginnings, mom and Instagram, a tattoo, a piano take: seven of ten sat in the bottom half. The BMW reveal broke the run, beating ten straight. The Lotto x H&M jerseys, a product left to pose with no heckle, landed near the floor.'),
    4: ('A crew run', 'Sock on the sofa, Ludo in the office and the turf taunt all landed in the top quarter, and Ronaldo’s “rigged” rant beat sixteen straight. Andheri East didn’t beat the post before it, but that post was the sock reel, top 10%: strong company. The same joke pointed at UP fell short of nine straight. Alone, the name has to come from his city.'),
}

anuj_posts = []
for order, (i, p) in enumerate(posts):
    c = parse_card(p['post_card'])
    anuj_posts.append({
        'id': f'a{order}', 'title': p['title'], 'age': p['posted'], 'lane': p['lane'],
        'rank_final': int(p['recent_rank'].split('/')[0]),
        'line': ANUJ_LINES[p['title']],
        **c,
    })
titles = {p['title'] for p in anuj_posts}
assert set(ANUJ_LINES) == titles, set(ANUJ_LINES) ^ titles


def post_ids(posts_, names):
    m = {p['title']: p['id'] for p in posts_}
    missing = [n for n in names if n not in m]
    assert not missing, missing
    return [m[n] for n in names]


def readings_out(posts_, readings):
    out = []
    for r in readings:
        out.append({
            'id': r['id'], 'title': r['title'], 'read': r['read'], 'threads': r['threads'],
            'evidence': post_ids(posts_, r['evidence']), 'boundary': post_ids(posts_, r['boundary']),
            'history': {str(k): {'movement': v[0], 'status': v[1], 'title': v[2], 'line': v[3]} for k, v in r['history'].items()},
            'was': {str(k): {'old': v[0], 'new': v[1]} for k, v in r.get('was', {}).items()},
        })
    return out


def threads_out(posts_, threads):
    return [{'group': g, 'id': tid, 'label': lab, 'posts': post_ids(posts_, names)} for g, tid, lab, names in threads]


anuj = {
    'handle': 'anuj.mp4', 'kind': 'Creator', 'lane': 'reels',
    'source': 'Ranks and postcards from the SIM-W03 run packet. Order inside each week is approximate.',
    'memoryNote': 'postcards',
    'runs': 4, 'posts': anuj_posts,
    'threads': threads_out(anuj_posts, ANUJ_THREADS),
    'readings': readings_out(anuj_posts, ANUJ_READINGS),
    'notes': {str(k): {'head': v[0], 'body': v[1]} for k, v in ANUJ_RUN_NOTES.items()},
    'compareDefault': ['Reframe with punchline', 'Flip State Punchline'],
    'lensDefault': {'type': 'reading', 'id': 'mumbai'},
}

# ---- Lakme ---------------------------------------------------------------
lk = json.loads((REPO / 'apps/web/src/data/runSignals/lakmeindia_run_bites.json').read_text())
LK_TITLES = ['Council of Makeup Lovers', 'Downloading in Progress', 'Golden Hour on My Lips', 'Eyeliner Does It All',
             'Lashes Get the Main Role', 'We Celebrate Your Skin', 'Wand Came to Work', 'Her Day’s Packed, Ice D',
             'First Job Confusion', 'Eyeconic Instants']
LK_LINES = [
    'The caption speaks to “the council of makeup lovers”; the product appears on a gesture. Upper quarter, 2.3× the usual views.',
    'A “downloading in progress” screen overlay. It kept climbing after day one.',
    '“Wearing the golden hour on my lips”: a person wears the claim. Top 18%, 2.8× usual views.',
    '“When your eyeliner does it all”: the product alone, as a packshot. Bottom half.',
    '“TLDR: your lashes finally got the main character…”: the product is the subject, and the claim test didn’t lift it. Bottom half.',
    'A screen-style overlay around “we celebrate your skin”. Middle of the run.',
    '“TLDR: this wand came to work”: a packshot that kept climbing after day one but stayed in the bottom half.',
    '“Her day’s packed, but a refreshing Ice D…”: a person’s day, with the product in it. The best of the run: top 10%, 3.7× usual views.',
    'A Bengali caption about first-job confusion, with the product tested on skin. Middle of the run.',
    '“Only posting instants that look Eyeconic”: a product pun over a screen overlay. The lowest placement of the run.',
]
lk_posts = []
for n, pp in enumerate(lk['run_stats']['per_post']):
    pct = int(pp['placed'].replace('top ', '').replace('%', ''))
    lk_posts.append({
        'id': f'l{n}', 'title': LK_TITLES[n], 'age': 'this run', 'lane': 'reels', 'pct_fixed': pct,
        'line': LK_LINES[n], 'work': '', 'flow': [], 'hook': pp['post'].strip() + '…', 'named': [],
        'mechanic': pp['carried_by'].capitalize() if pp['carried_by'] else '', 'surface': '', 'length': '', 'caption': '',
        'views': pp['views_vs_usual'], 'legs': pp['legs'], 'hour': pp['hour_ist'],
    })
LK_THREADS = [
    ('caption', 'person', 'A person is the subject', ['Council of Makeup Lovers', 'Golden Hour on My Lips', 'Her Day’s Packed, Ice D', 'First Job Confusion']),
    ('caption', 'product', 'The product is the subject', ['Eyeliner Does It All', 'Lashes Get the Main Role', 'Wand Came to Work', 'Eyeconic Instants']),
    ('craft', 'overlay', 'Screen-style overlay', ['Downloading in Progress', 'We Celebrate Your Skin', 'Her Day’s Packed, Ice D', 'Eyeconic Instants']),
    ('craft', 'claim', 'Claim tested on skin', ['Golden Hour on My Lips', 'Lashes Get the Main Role', 'First Job Confusion']),
    ('craft', 'packshot', 'Product alone', ['Eyeliner Does It All', 'Wand Came to Work']),
    ('life', 'legs', 'Kept climbing after day one', ['Downloading in Progress', 'Wand Came to Work', 'Eyeconic Instants']),
]
LK_READINGS = [
    {
        'id': 'person', 'threads': ['person', 'product'],
        'title': 'The Person Leads, Not the Product',
        'read': 'When the caption puts a person in front (her packed day, “my lips”, the council of makeup lovers), the post placed in the top quarter. When the product is the subject (the eyeliner does it all, the wand came to work, the lashes got the main character), it placed in the bottom half. Ten posts and captions only: a first sighting, not a rule.',
        'evidence': ['Her Day’s Packed, Ice D', 'Golden Hour on My Lips', 'Council of Makeup Lovers', 'Eyeliner Does It All', 'Lashes Get the Main Role', 'Wand Came to Work', 'Eyeconic Instants'],
        'boundary': ['First Job Confusion'],
        'history': {1: ('new', 'emerging', None, 'First sighting from captions alone.')},
    },
    {
        'id': 'overlay', 'threads': ['overlay'],
        'title': 'Screen Overlays Are a Habit, Not a Reason',
        'read': 'Four of ten posts use a phone-screen overlay, and they placed anywhere from top 10% to top 61%. The overlay is how Lakmé dresses a post right now. On its own it doesn’t tell you where the post will land, so the reader isn’t crediting it with the Ice D result.',
        'evidence': ['Downloading in Progress', 'We Celebrate Your Skin', 'Her Day’s Packed, Ice D', 'Eyeconic Instants'],
        'boundary': [],
        'history': {1: ('new', 'emerging', None, 'Recorded as a habit. Not yet a reason for anything.')},
    },
]
lakme = {
    'handle': 'lakmeindia', 'kind': 'Brand', 'lane': 'reels',
    'source': 'Ten posts from the run-bites file: captions, placements and the media log’s “carried by”. No postcards yet. Order as recorded.',
    'memoryNote': 'posts, captions only',
    'runs': 1, 'posts': lk_posts,
    'threads': threads_out(lk_posts, LK_THREADS),
    'readings': readings_out(lk_posts, LK_READINGS),
    'notes': {'1': {'head': 'One run in, captions only', 'body': 'All ten posts beat Lakmé’s usual views, so this run is about which beat them most. One split is already visible: when a person is the subject (her packed day, “my lips”, the makeup lovers), the post placed in the top quarter. When the product is the subject (the eyeliner does it all, the wand came to work), it placed in the bottom half. The reader can say what Lakmé does often. It can’t yet say what Lakmé does well.'}},
    'compareDefault': ['Her Day’s Packed, Ice D', 'Eyeconic Instants'],
    'lensDefault': {'type': 'reading', 'id': 'person'},
}

out = {'feeders': [anuj, lakme]}
page = pathlib.Path(__file__).with_name('memory-skyline.html')
html = page.read_text()
start = html.index('const DATA = ') + len('const DATA = ')
end = html.index(';\nconst MOVE')
page.write_text(html[:start] + json.dumps(out, ensure_ascii=False) + html[end:])


# ---- sanity: thread medians at final memory ----------------------------
def med(ranks, n):
    return round(statistics.median(ranks) / n * 100)


rk = {p['id']: p['rank_final'] for p in anuj_posts}
for t in anuj['threads']:
    print(f"{t['label']:28s} n={len(t['posts']):2d} median top {med([rk[i] for i in t['posts']], 40)}%")
print('all', med(list(rk.values()), 40))
for p in anuj_posts[:3]:
    print(json.dumps(p, ensure_ascii=False)[:400])
