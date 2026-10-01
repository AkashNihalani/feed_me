"""Build data for the Feeder Reader app prototype (Now / Series / Grid).

Starts from wall-data.json (postcards, covers, reader lines) and adds:
- a date per post (approximate: card age bucket, anchored so Blinkit's
  Mother's Day film lands on May 8),
- series: the recurring things each account posts,
- concrete run recaps and a three-line "read" for the latest run.
All copy below is hand-written as a stand-in for the run-of-10 reader.
"""
import json, datetime as dt, pathlib
from collections import defaultdict

HERE = pathlib.Path(__file__).resolve().parent
_wall = (HERE / 'feeder-wall.html').read_text()  # run build_feeder_wall.py first; its embedded data is the input here
_s = _wall.index('const DATA = ') + len('const DATA = ')
d = json.loads(_wall[_s:_wall.index(';\nconst ', _s)])
anuj, lakme = d['feeders']

# ---------------------------------------------------------------- Anuj
W = {'2 months ago': 9, '8 weeks ago': 8, '7 weeks ago': 7, '6 weeks ago': 6, '5 weeks ago': 5, '4 weeks ago': 4, '3 weeks ago': 3, '2 weeks ago': 2}
anchor = dt.date(2026, 7, 3)
buckets = defaultdict(list)
for i, p in enumerate(anuj['posts']):
    buckets[p['age']].append(i)
for age, idx in buckets.items():
    base = anchor - dt.timedelta(days=7 * W[age])
    for k, i in enumerate(idx):
        anuj['posts'][i]['date'] = (base + dt.timedelta(days=round(k * 6 / (len(idx) - 1)) if len(idx) > 1 else 3)).isoformat()

A_SERIES = [
    ('biz', 'Content-business satire', 'He plays people from his own industry: Parle’s marketing team milking the “Melodi” meme, creators who think one viral reel is an “IP”, an agency pitch huddle that turns out to be Ludo, comics with “writing inputs”.',
     'His most reliable thing. 3 of 4 landed in the top 20%. The miss (comics with “writing inputs”) stays in one white room and never has a reveal.',
     ['Flip whiteboard beats', 'Viralize the clip', 'Cut to Ludo', 'Stutter Into Chaos']),
    ('football', 'Football grief', 'Football told as a fan’s wound: Ronaldo’s loss is “rigged for Messi”, Mbappé watches PSG win twice, a turf taunt that ends with him sliding into the net.',
     '2 of 3 in the top 25%. It works when he plays the hurt, over-invested fan.',
     ['Escalate a complaint', 'Zoom Through Sorrow', 'Taunt into play']),
    ('crew', 'Crew pranks & chaos', 'Sketches with his friends where something dumb happens: a sock used as a tissue in the office, SoBo kids panicking outside Bombay, a Clubhouse-style room that falls apart.',
     'Two hits and one flop. The sock prank (top 10%) and the SoBo panic (top 28%) each have one clear joke. The Clubhouse mashup piles up references and landed near the bottom.',
     ['Sock Mistake Cut', 'Panic-Cut Through Street', 'Layer room with mashups']),
    ('bmw', 'The BMW', 'His matte black BMW M340i, always posted with a caption that mocks the flex first: “we get it bro”, then “yall just have to get used to these”.',
     'Both landed in the upper third. The second one beat 10 posts in a row.',
     ['Roll and Cut', 'Reveal Car, Drive Off']),
    ('food', 'Mumbai food spots', 'Him and friends at a Mumbai restaurant naming dishes: O Pedro (coconut prawns, burrata), Baskin Robbins (Dubai chocolate sundae).',
     'Both landed well (top 20% and top 32%). There’s no joke in them; this is a separate lifestyle lane from the comedy.',
     ['Food Tour Cuts', 'Sundae Taste Tour']),
    ('place', 'Place-name punchlines', 'New in run 4. A 5–7 second two-hander where a vague line ends on a place name: “I’m in a bad place” → “Andheri East”, “I’m in a bad state” → “UP”.',
     'Tried twice in one week with opposite results: Andheri East top 15%, UP top 88%. Andheri East is a commute every Mumbaikar has suffered. UP is only a word pun.',
     ['Reframe with punchline', 'Flip State Punchline']),
    ('street', 'Anuj vs strangers in public', 'He walks up to real people: mediates a roadside crash with a forced hug, lies to a parking attendant about a driver who doesn’t exist, hosts street rap battles and roasts.',
     'The steadiest thing he posts: all 4 landed between top 35% and top 50%. Never a hit, never a flop.',
     ['Force a Hug', 'Invent a Driver', 'Rig a Boom Battle', 'Roast the Crowd']),
    ('city', 'Delhi vs Mumbai & city gripes', 'Mumbai shrugs while Delhi panics at an alarm, a Delhi voice roasting him, a rant about ₹248 crore spent on 750 m of unfinished flyover.',
     'Middle to low (top 42% to 75%). Comparing cities lands worse than jokes from inside Mumbai.',
     ['Split by alarm', 'Roast by voiceover', 'Mock-phone rant']),
    ('brand', 'Brand deals', 'Paid posts: Blinkit (an AI replaces him as the son, for Mother’s Day), Broadway’s Bandra launch, District (an MJ mix-up), Lotto x H&M jerseys, a Startup Village rapid-fire panel.',
     'The widest spread in the account: #1 and #40 are both brand deals. When the brand is written into one of his sketches (Blinkit, Broadway) it lands. When it brings its own format (a jersey lookbook, a panel Q&A) it lands last.',
     ['Replace With AI', 'Swap MJ Meaning', 'Rapid-fire Answer Check', 'Swap and Style Jerseys', 'Freeze-Frame Reveal']),
    ('both', 'He plays both people', 'He plays both sides of a conversation with jump cuts: flatmates deciding to move in, girls’ vs boys’ life updates, the rich friend’s “humble beginnings”.',
     'Middle to low (top 52% to 80%). The two-character format on its own doesn’t carry a post; it needs a sharp target.',
     ['Escalate Then Clap', 'Split Life Updates', 'Profile-switch Storyline']),
    ('solo', 'Solo bits from his own day', 'Just him and his own life: waking at 2 PM to the emergency alert, a backseat story, handing cash to a worker, reviewing water glasses, a tattoo, a piano take.',
     'His weakest recurring thing by volume: 6 posts, none above top 55%.',
     ['Alert-Sound Reaction', 'Skip and React', 'Hands over cash', 'Glass-Review Parody', 'Say the tattoo', 'Open with chord']),
    ('family', 'Family says reels aren’t a job', 'Monologues where someone at home judges his Instagram life: a friend says scrolling reels isn’t a job, his mom mocks his self-praise, he plays his dad calling his videos “waahiyat”.',
     'All 3 landed in the bottom quarter. When he’s the one being judged, it doesn’t land.',
     ['Hard Cut to Notification', 'Mock Complaint Loop', 'Flip into Dad']),
]
A_RUNS = {
    1: ('Blinkit’s Mother’s Day film sets the ceiling', 'Blinkit’s film, where an AI replaces him as the son, became the #1 post of the whole window. “Creators after one viral video” landed #3. The BMW debuted under “we get it bro”. The solo and family bits sank.'),
    2: ('Parle satire hits #2, a panel Q&A hits the floor', 'Street bits with strangers held the middle all run. The Parle “Melodi” whiteboard beat 9 posts in a row to become #2. The Startup Village rapid-fire panel landed #40, below every post before it.'),
    3: ('Seven of ten sink; the BMW reveal saves the run', 'Mostly solo bits: a glasses review, mom mocking his self-praise, the rich friend’s “humble beginnings”, a tattoo, a piano take. Seven landed in the bottom half. The second BMW post beat 10 in a row. The Lotto x H&M jerseys landed #38.'),
    4: ('Crew sketches carry the run', 'Six of ten landed in the top 25%. The sock prank beat 20 posts in a row and Ronaldo “rigged” beat 16. A new 5-second place-pun format was tried twice: Andheri East landed top 15%, UP top 88%.'),
}
A_READ = [
    ('What he posted', 'Crew sketches, football and a new 5-second place-pun format, tried twice.', {'type': 'run', 'id': 4}),
    ('What landed', 'Content-business satire is his most reliable thing: 4 posts since May, 3 in the top 20%. Family-judges-him skits are the least: 3 posts, all in the bottom quarter.', {'type': 'series', 'id': 'biz'}),
    ('Watch next', 'Place-name punchlines: two tries in one week, opposite results (top 15% vs top 88%).', {'type': 'series', 'id': 'place'}),
]

# ---------------------------------------------------------------- Lakme
start = dt.date(2026, 6, 13)
for i, p in enumerate(lakme['posts']):
    p['date'] = (start + dt.timedelta(days=round(i * 12 / 9))).isoformat()
L_SERIES = [
    ('skin', 'Skin & freshness', 'Skincare posts built around a person’s day: “Her day’s packed, but a refreshing Ice D…”, “We celebrate your skin and skincare…”.',
     'Ice D was the best post of the run (top 10%, 3.7× usual views). The skin-celebration post sat mid (top 39%).',
     ['Her Day’s Packed, Ice D', 'We Celebrate Your Skin']),
    ('people', 'Talking to people', 'Posts that address a community or a life moment: “the council of makeup lovers”, first-job confusion (caption in Bengali).',
     'Top 24% and top 41%: middle of the pack.',
     ['Council of Makeup Lovers', 'First Job Confusion']),
    ('eye', 'Eye makeup: liner & mascara', 'Liner, mascara and kajal reels where the product is the subject: “your eyeliner does it all”, “this wand came to work”, “your lashes finally got the main character”, “Eyeconic” instants.',
     'All 4 landed in the bottom half (top 55% to 61%), even though eye makeup was the biggest lane this run.',
     ['Eyeliner Does It All', 'Lashes Get the Main Role', 'Wand Came to Work', 'Eyeconic Instants']),
    ('oneoff', 'One-offs', 'Posts that didn’t repeat this run: a “golden hour on my lips” look and a “downloading in progress” overlay post.',
     'Golden hour lips landed top 18%; the downloading post top 31%.',
     ['Golden Hour on My Lips', 'Downloading in Progress']),
]
L_RUNS = {1: ('Every post beat usual views; eye makeup sat lowest', 'All ten beat Lakmé’s usual views (2× on average). Ice D’s packed-day post led at 3.7× and top 10%. The four eye-makeup reels, the biggest lane this run, all landed in the bottom half.')}
L_READ = [
    ('What they posted', '4 eye-makeup reels (liner, mascara, kajal), 2 skin posts, 2 posts talking to people, 2 one-offs.', {'type': 'run', 'id': 1}),
    ('What landed', 'Skin & freshness led: Ice D’s packed-day post was top 10%. All 4 eye-makeup reels landed in the bottom half.', {'type': 'series', 'id': 'eye'}),
    ('Watch next', 'Eye makeup is the biggest lane by volume and the weakest by landing. One run and captions only, so treat this as a first sighting.', {'type': 'series', 'id': 'eye'}),
]


def attach(f, series, runs, read, note):
    by = {p['title']: p for p in f['posts']}
    out = []
    for sid, name, what, verdict, titles in series:
        for t in titles:
            by[t]['series'] = sid
        out.append({'id': sid, 'name': name, 'what': what, 'verdict': verdict})
    missing = [p['title'] for p in f['posts'] if 'series' not in p]
    assert not missing, missing
    f['series'] = out
    f['runRecaps'] = {str(k): {'head': a, 'body': b} for k, (a, b) in runs.items()}
    f['read'] = [{'label': a, 'text': b, 'link': c} for a, b, c in read]
    f['dateNote'] = note
    for k in ('viewpoints', 'runNotes', 'stance'):
        f.pop(k, None)


attach(anuj, A_SERIES, A_RUNS, A_READ, 'Dates are approximate (from each card’s age), anchored so the Mother’s Day post lands on May 8.')
attach(lakme, L_SERIES, L_RUNS, L_READ, 'Dates are placeholders spread across the run; the source file has no post dates.')


# ---------------------------------------------------------------- plain-language layer
# Postcard titles ("Reframe with punchline") are internal. Users get a label
# that says what happens, so copy never assumes they've seen the post.
A_LABELS = {
    'Alert-Sound Reaction': 'Sleeps through the national emergency alert',
    'Split by alarm': 'An alarm goes off: Delhi panics, Mumbai shrugs',
    'Escalate Then Clap': 'Two flatmates trade dark confessions',
    'Hard Cut to Notification': 'A friend lectures him that reels aren’t a job',
    'Replace With AI': 'Blinkit ad: his mom swaps him for an AI assistant',
    'Swap MJ Meaning': 'District ad: a Jordan-or-Jackson mix-up',
    'Viralize the clip': 'Creators after one viral video talk “IP”',
    'Force a Hug': 'He forces a hug at a roadside crash',
    'Roll and Cut': 'His new BMW, captioned “we get it bro”',
    'Skip and React': 'Retelling a night, skipping the cheating part',
    'Rig a Boom Battle': 'A rap battle at a street cosmetics stall',
    'Split Life Updates': 'Girls’ life updates vs boys’ life updates',
    'Hands over cash': 'He hands cash to a scaffolding worker',
    'Panic-Cut Through Street': 'South Bombay kids lost outside their bubble',
    'Flip whiteboard beats': 'Parody of Parle’s marketing team milking “Melodi”',
    'Invent a Driver': 'He blames a parking mess on a made-up driver',
    'Rapid-fire Answer Check': 'Sponsored Startup Village rapid-fire panel',
    'Roast the Crowd': 'A street-mic roast hosted in a bathrobe',
    'Flip into Dad': 'He plays his dad mocking his videos',
    'Layer room with mashups': 'A Clubhouse room turns into a song mashup',
    'Zoom Through Sorrow': 'Mbappé watching PSG win without him',
    'Glass-Review Parody': 'He reviews drinking glasses',
    'Swap and Style Jerseys': 'Sponsored Lotto x H&M jersey lookbook',
    'Mock Complaint Loop': 'His mom mocks his self-praise',
    'Profile-switch Storyline': 'A rich friend’s “humble beginnings” story',
    'Reveal Car, Drive Off': 'The BMW again: “get used to these”',
    'Say the tattoo': 'A one-second shot of a tattoo that says “Mauli”',
    'Stutter Into Chaos': 'Comic actors who suddenly have “writing inputs”',
    'Sundae Taste Tour': 'Tasting sundaes at Baskin Robbins',
    'Open with chord': 'A 14-second piano performance',
    'Roast by voiceover': 'A Delhi voice roasts him from off camera',
    'Escalate a complaint': 'A rant that Ronaldo’s losses are “rigged for Messi”',
    'Mock-phone rant': 'A rant about a ₹248 crore unfinished flyover',
    'Food Tour Cuts': 'Eating at O Pedro with friends',
    'Freeze-Frame Reveal': 'Broadway store launch ad: “not Anuj’s PA”',
    'Sock Mistake Cut': 'Office prank: a friend’s sock used as a tissue',
    'Reframe with punchline': '“I’m in a bad place” → “Andheri East”',
    'Taunt into play': 'Football trash talk, then he slides into the net',
    'Cut to Ludo': 'A tense office “pitch” is actually a Ludo game',
    'Flip State Punchline': '“I’m in a bad state” → “UP”',
}
L_LABELS = {
    'Council of Makeup Lovers': '“The council of makeup lovers” community post',
    'Downloading in Progress': 'A “downloading in progress” screen overlay',
    'Golden Hour on My Lips': 'A golden-hour lip look, worn in first person',
    'Eyeliner Does It All': 'Eyeliner packshot: “your eyeliner does it all”',
    'Lashes Get the Main Role': 'Mascara: “your lashes got the main character”',
    'We Celebrate Your Skin': '“We celebrate your skin” skincare post',
    'Wand Came to Work': 'Mascara packshot: “this wand came to work”',
    'Her Day’s Packed, Ice D': 'Ice D: a woman’s packed day, kept fresh',
    'First Job Confusion': 'First-job confusion, captioned in Bengali',
    'Eyeconic Instants': 'A product pun: “instants that look Eyeconic”',
}
TAGLINES = {
    'biz': 'He plays marketers, creators and agency people', 'football': 'Football as a fan’s heartbreak',
    'crew': 'Dumb moments with his friends', 'bmw': 'His BMW, always mocked in the caption',
    'food': 'Eating out in Mumbai with friends', 'place': '5-second jokes that end on a place name',
    'street': 'Unscripted bits with real strangers', 'city': 'Delhi vs Mumbai and city complaints',
    'brand': 'Paid partnerships', 'both': 'He plays both people in a conversation',
    'solo': 'Just him, about his own day', 'family': 'Family judging his Instagram career',
    'skin': 'Skincare built around a woman’s day', 'people': 'Captions that talk to people',
    'eye': 'Liner and mascara as the hero', 'oneoff': 'Posted once this run',
}
A_RUNS2 = {
    1: ('A Blinkit ad became his best post', 'Blinkit’s Mother’s Day ad, where his mom swaps him for an AI assistant, ranked #1 of all 40 posts. A sketch about creators who think one viral video makes them an “IP” ranked #3. His new BMW debuted with the caption “we get it bro” and landed in his top 25%. Solo bits about his own day (sleeping through the emergency alert, a friend’s lecture that reels aren’t a job) landed in his bottom half.'),
    2: ('A Parle parody hit #2; a sponsored panel hit #40', 'His parody of Parle’s marketing team milking the “Melodi” meme beat each of the 9 posts before it and ranked #2. Street bits with strangers (a made-up driver, a rap battle at a cosmetics stall, a bathrobe roast) all landed mid-table. A sponsored Startup Village rapid-fire panel ranked #40, below every post before it.'),
    3: ('Seven of ten sank; the BMW sequel saved it', 'Mostly him alone: reviewing drinking glasses, his mom mocking his self-praise, a tattoo, a piano take. Seven of ten landed in his bottom half. The second BMW post (“get used to these”) beat each of the 10 posts before it. A sponsored Lotto x H&M jersey lookbook ranked #38.'),
    4: ('His friend-group sketches carried the run', 'Six of ten landed in his top 25%. Best: an office prank where a friend’s sock gets used as a tissue, which beat each of the 20 posts before it. A rant that Ronaldo’s losses are “rigged for Messi” beat 16. He tried a new 5-second joke twice: “I’m in a bad place” → “Andheri East” landed in his top 15%; “I’m in a bad state” → “UP” landed in his bottom 15%.'),
}
L_RUNS2 = {1: ('Every reel beat usual views; eye makeup ranked lowest', 'All ten reels beat Lakmé’s usual view count (2× on average). The best: an Ice D post about a woman’s packed day (3.7× usual views, top 10%). The four eye-makeup reels, where the eyeliner or mascara is the hero, all landed in the bottom half.')}
A_READ2 = [
    ('Works best', 'Parodies of his own industry: Parle’s marketers, creators after one viral video, a “pitch” that’s a Ludo game. 3 of 4 landed in his top 20%.', 'biz'),
    ('Works least', 'Family judging his Instagram career: his dad calls his videos “waahiyat”, his mom mocks his self-praise, a friend says reels aren’t a job. All 3 landed in his bottom quarter.', 'family'),
    ('Watch next', 'The new 5-second place-name joke. Andheri East (a commute every Mumbaikar knows) landed top 15%; UP (just a pun) landed bottom 15%.', 'place'),
]
L_READ2 = [
    ('Works best', 'Skincare built around a woman’s day: the Ice D packed-day post was their best of the run (top 10%).', 'skin'),
    ('Works least', 'Eye makeup as the hero: 4 reels (eyeliner, mascara, kajal), all in the bottom half.', 'eye'),
    ('Watch next', 'Posts that talk to people (the makeup-lovers “council”, first-job confusion): top 24% and 41%. Too early to call.', 'people'),
]
for f, labels, runs, read in ((anuj, A_LABELS, A_RUNS2, A_READ2), (lakme, L_LABELS, L_RUNS2, L_READ2)):
    for p in f['posts']:
        p['label'] = labels[p['title']]
    for sr in f['series']:
        sr['sub'] = TAGLINES[sr['id']]
    f['runRecaps'] = {str(k): {'head': a, 'body': b} for k, (a, b) in runs.items()}
    f['read'] = [{'label': a, 'text': b, 'series': c} for a, b, c in read]
    f['who'] = 'his' if f is anuj else 'their'
assert len(A_LABELS) == 40 and len(L_LABELS) == 10

data = json.dumps({'feeders': [anuj, lakme]}, ensure_ascii=False)
page = HERE / 'feeder-reader.html'
if page.exists():
    html = page.read_text()
    s = html.index('const DATA = ') + len('const DATA = ')
    e = html.index(';\nconst ', s)
    page.write_text(html[:s] + data + html[e:])
print('ok', len(anuj['posts']), len(lakme['posts']))
