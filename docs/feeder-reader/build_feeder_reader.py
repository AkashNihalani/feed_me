"""Build data for the Feeder Reader prototype: a pulse hero and one feed wall with Rank / Why / Bits layers.

Starts from the data embedded in feeder-wall.html (postcards, covers, reader
lines) and adds:
- a date per post (approximate: card age bucket, anchored so Blinkit's
  Mother's Day film lands on May 8),
- series: the recurring things each account posts,
- concrete run recaps and a three-line "read" for the latest run.
- the Feeder Reader's voice: a take and a driver (idea / moment / faces /
  craft) per post, a name and take per bit (series), a dispatch per run.
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


# ---------------------------------------------------------------- the Feeder Reader's voice
# Written as someone who has watched every reel: context first, opinion second,
# numbers only where they settle the argument. Drivers say what decided where it landed:
# idea (the joke/premise), moment (something already in the air), faces (who's in it),
# craft (length, pacing, format).
A_TAKE = {
    'a0': ('idea', 'He sleeps through the nationwide emergency-alert test because he “doesn’t wake up before 2 PM”. Everyone got that alert, so the moment was free, but the reel is just him jolting awake in bed. Nothing for anyone to grab.'),
    'a1': ('idea', 'The same guy hears an alarm twice: as a Delhi guy (panic, shouting) and as a Mumbai guy (a shrug). A city stereotype his crowd has seen a hundred times. Middle of the pack.'),
    'a2': ('idea', 'He plays two flatmates who decide to move in, then trade confessions (“I drank your milk”, “I dream about my own funeral”) and break into a hand-slap routine. A dark swerve, but nobody in his crowd knows these two guys.'),
    'a3': ('idea', 'He lectures “Arjun bhai” that scrolling reels isn’t a job, then a Zomato notification cuts in as the punchline. The joke lives in a phone banner you have to read in a second. And it’s Anuj getting preachy, which his crowd never orders.'),
    'a4': ('moment', 'A Blinkit ad, dropped right before Mother’s Day: his mom asks for help with WhatsApp, he’s “too busy being a macro influencer”, so she adopts an AI helper instead, and he has to order her a boAt watch to win her back. A brand deal that is actually a story, on the right weekend. The best-fed reel of the 90 days.'),
    'a5': ('craft', 'District ad: his friend mixes up Michael Jordan and Michael Jackson on the way to a concert. A one-line joke stretched to a minute, and the brand only appears on the end card.'),
    'a6': ('idea', 'He and Ruhee Dosani play creators whose one reel hit 10 million. Suddenly it’s an “IP”, there are brand calls, and they still can’t pay the café bill. Every creator watching has met these two, or been them. Top 10%.'),
    'a7': ('faces', 'A real roadside fight between a scooter rider and a cab driver. Anuj walks in, makes them do breathing exercises, waves Kokilaben hospital at them and forces a three-way hug. Real strangers make it; at over a minute it runs out of road.'),
    'a8': ('idea', 'First look at his new matte-black BMW, captioned “we get it bro 🙏”. He rolls his eyes at his own flex before you can. That pre-emptive roast is the whole trick. Top quarter.'),
    'a9': ('idea', 'Telling the girls about last night while skipping the part where he cheated. The guilty bit flashes to black and white. One expression doing all the work, and it isn’t enough.'),
    'a10': ('faces', 'A diss-track battle at a roadside cosmetics stall. The shopkeeper says “Boom” after every verse and picks the winner. The shopkeeper steals it; the reel is long enough that most people never meet him.'),
    'a11': ('idea', 'Girls sharing life updates (big drama, tears) vs boys (a shrug), all played by him in wigs. A premise the internet has done to death.'),
    'a12': ('idea', 'He leans off his balcony, chats with a worker on the bamboo scaffolding and hands him some cash. Sweet. But there’s no joke and no point of view, so there’s nothing to send to the group chat.'),
    'a13': ('idea', 'South Bombay rich kids find out there’s a world outside Bombay and panic on a night street. A very Mumbai jab, played by the whole crew. Just outside his top quarter.'),
    'a14': ('moment', 'He plays Parle’s marketing team milking the “Melodi” meme (Modi + Meloni): a FLAMES chart on a whiteboard, cash held to his ear like a phone, an “Italy expansion proposal”. The internet was already laughing at that meme; he gave it a boardroom. #2 of 40.'),
    'a15': ('craft', 'He gets a parking complaint and blames “the new driver”, who doesn’t exist, all the way down to the basement. A great lie stretched over 90 seconds of hallway.'),
    'a16': ('idea', 'A sponsored Startup Village panel: is college worth it, Claude Code or Codex, favourite subreddits. It’s an event recap with Anuj in it, not an Anuj reel, and his crowd treated it that way. Dead last.'),
    'a17': ('faces', 'Street mic in a Versace bathrobe: two kids roast each other, the crowd votes, and he quits his own show. The strangers are funny; the reel is long.'),
    'a18': ('idea', 'He plays his own dad, in black and white, ranting about money, the engineering degree he skipped, and his “waahiyat” (rubbish) Instagram videos. Watching Anuj get told off isn’t why anyone follows Anuj.'),
    'a19': ('craft', 'A fake Clubhouse room: one friend sings, another raps over it, and cutouts of Shahid, Kareena, Modi and Hanumankind keep popping up. Six jokes fighting for one reel. Nobody won.'),
    'a20': ('moment', 'A slow zoom on him in a Real Madrid shirt: “Mbappé watching PSG win back-to-back Champions Leagues” after leaving them. Football Twitter would love it; there’s no Anuj in it beyond his face.'),
    'a21': ('craft', 'He reviews drinking glasses “so I drink more water”, with sad violins, a Bollywood dad bit and a Sarabhai vs Sarabhai cutaway. Funny inserts, 88 seconds about glasses.'),
    'a22': ('idea', 'A sponsored Lotto x H&M jersey lookbook: ball tricks, rooftop poses, no joke. It looks great. His crowd came for jokes and scrolled. #38 of 40.'),
    'a23': ('idea', '“My parents don’t appreciate me.” His real mom, off camera, teases his self-praise until he announces it’s going on Instagram. Real banter, but once again Anuj is the one getting roasted.'),
    'a24': ('idea', 'His rich friend explains his “humble beginnings”: no food in the canteen, dad begging colleagues for help… dad got into racing. A good target, played solo in two profile shots. Just missed.'),
    'a25': ('idea', 'The BMW again, posed at a petrol pump: “y’all just have to get used to these for some time”. The eye-rolling caption carries it again; it beat the 10 reels before it and pulled a slow week back up.'),
    'a26': ('craft', 'A one-second clip of a forearm tattoo that says “Mauli”. Blink and it’s over, so people did.'),
    'a27': ('idea', '“Comics when they get hired to act but suddenly have writing inputs”: one actor loses it, the others hold him back. An industry in-joke with no reveal at the end.'),
    'a28': ('faces', 'He and a friend at Baskin Robbins in Mumbai tasting a Dubai chocolate sundae. Zero jokes, still his top third. People like hanging out with him, not just laughing at him.'),
    'a29': ('idea', 'A 14-second piano performance. Lovely. Not what anyone came for. #39 of 40.'),
    'a30': ('idea', '“Watching Obsession: Delhi”: a loud Delhi voice roasts him from the back seat. Delhi-vs-Mumbai again, and again just short.'),
    'a31': ('moment', '“Practicing for when Ronaldo loses yet another World Cup”: it’s rigged, it’s for Messi, he’s FIFA’s boy. Fan grief everyone has heard from that one friend, timed to football season. It beat the 16 reels before it.'),
    'a32': ('moment', '₹248 crore for a 750-metre flyover that’s still unfinished, ranted into a plaster bust like it’s a phone. The anger is real and the issue is real, but he’s angry here, not funny.'),
    'a33': ('faces', 'Three friends at O Pedro, a Goan restaurant in Mumbai: coconut prawns, burrata, a shouted “O Pedro baby!”. No joke, all vibe, and it made his top 20%.'),
    'a34': ('faces', 'A Broadway store-launch teaser: a shouting match in the store, a woman introduced as “Anuj’s PA”, a record scratch, “Not Anuj’s PA”. A brand deal with an actual joke in it, and it held his top half.'),
    'a35': ('faces', 'The office crew: one friend pulls off his sock, another takes it for a tissue and blows her nose in it. Gross, quick, and very “this happened in our office”. Top 10%, and it beat the 20 reels before it.'),
    'a36': ('idea', 'Five seconds. “I’m in a bad place right now.” “Damn. Andheri East.” Every Mumbaikar has been stuck there. Top 15%; only the sock prank right before it did better.'),
    'a37': ('faces', 'Turf football with the boys: big trash talk, then he slides feet-first into the net. The crew does the heavy lifting again.'),
    'a38': ('faces', 'Four people in office clothes huddle over “our pitch”… it’s a Ludo game. He tagged half a creative agency’s worth of friends, and they showed up.'),
    'a39': ('idea', 'The Andheri joke’s twin: “I’m in a bad state” → “UP”. A pun on a state, not a feeling anyone in Mumbai has. All 9 reels before it did better.'),
}
L_TAKE = {
    'l0': ('faces', 'Talks to “the council of makeup lovers” and lets the product show up on a hand gesture. It reads like a group chat, not an ad. Top quarter.'),
    'l1': ('craft', 'A “downloading in progress” screen overlay, the look loading in. A slow burner: it kept climbing after day one.'),
    'l2': ('faces', 'First person: “Wearing the golden hour on my lips.” A face wearing the claim instead of a product making it.'),
    'l3': ('craft', 'Eyeliner packshot: “When your eyeliner does it all.” Just the product on screen, doing a job nobody watched it do.'),
    'l4': ('idea', '“TLDR: your lashes finally got the main character…” The mascara is the hero even with a face in frame. Bottom half, like every eye-makeup reel this run.'),
    'l5': ('idea', '“We celebrate your skin and skincare…” in a phone-screen overlay. Warm, generic, middle of the pack.'),
    'l6': ('craft', 'Mascara wand packshot: “This wand came to work.” It kept climbing after day one and still finished in the bottom half.'),
    'l7': ('faces', '“Her day’s packed, but a refreshing Ice D…” A woman’s busy day first, the product second. 3.7× their usual views. Best of the run.'),
    'l8': ('moment', 'First-job confusion, captioned in Bengali. A real life moment for a regional crowd. Top half.'),
    'l9': ('idea', 'A product pun (“instants that look Eyeconic”) over a screen overlay. The lowest of the run.'),
}
A_BITS = {
    'biz': ('Roasting his own industry', 'Anuj is a creator who works with brands, so he roasts creators and brand people from the inside: Parle’s marketing team milking the Melodi meme, creators who hit 10M once and start saying “IP”, an urgent pitch that’s a Ludo game. Half his crowd works in this world. 3 of 4 made his top quarter; the miss (actors who suddenly have “writing inputs”) never lands a reveal.'),
    'football': ('Football heartbreak', 'Football as personal grief: Ronaldo “robbed” by FIFA, Mbappé watching PSG win without him, trash talk on the turf that ends with him in the net. 2 of 3 made his top quarter. It works when he’s the wounded fan; the sponsored jersey lookbook is football too, and it came #38 of 40.'),
    'crew': ('The crew being idiots', 'His friends doing dumb things: the sock-for-a-tissue office prank, South Bombay kids panicking outside their bubble, a Clubhouse room falling apart. The sock prank beat the 20 reels before it. The Clubhouse one crammed six celebrities and a rap verse into one joke and sank.'),
    'bmw': ('The BMW (pre-roasted)', 'He bought a matte-black BMW M340i and keeps posting it, always with a caption that rolls its eyes first: “we get it bro”, then “y’all just have to get used to these”. That eye-roll is the trick. Both landed in his top third.'),
    'food': ('Eating out with friends', 'Eating out in Mumbai with friends: O Pedro, Baskin Robbins. Not a single joke, and both still landed in his top third. His crowd likes hanging out with him, not just laughing at him.'),
    'place': ('5-second place puns', 'New in run 4: five-second jokes that end on a place name. “Bad place” → Andheri East made his top 15%. “Bad state” → UP sank to #35. Andheri East is a commute every Mumbaikar has suffered; UP is just wordplay. One more try will tell us whether this is a format or a fluke.'),
    'street': ('Loose on the street', 'Anuj with real strangers: forcing a hug at a road fight, lying to a parking attendant about a driver who doesn’t exist, hosting rap battles and roasts. All four in his top half, none in the top quarter. The strangers are gold; the reels run a minute too long.'),
    'city': ('Delhi vs Mumbai', 'City jabs: Mumbai shrugs while Delhi panics at an alarm, a Delhi voice roasting him, a rant about a ₹248 crore flyover. The alarm one held his top half; the other two sank below it. City stereotypes are middle-of-the-pack at best for him.'),
    'brand': ('Brand deals', 'Five sponsored reels and the widest spread in the feed: #1 and #40. Blinkit wrote him into a Mother’s Day story where an AI replaces him as the son: #1 of 40. Broadway gave him a scene (“Not Anuj’s PA”). Lotto x H&M (a lookbook) and Startup Village (a panel Q&A) gave him a format with no joke, and his crowd scrolled. Hire the bit, not the face.'),
    'both': ('Playing both sides', 'He plays both people in a conversation with jump cuts: flatmates moving in, girls’ vs boys’ life updates, the rich friend’s “humble beginnings”. All three in his bottom half. The format doesn’t carry anything on its own.'),
    'solo': ('Just Anuj, about Anuj', 'Just him and his own day: sleeping through the emergency alert, reviewing drinking glasses, a tattoo, a piano take, cash for a scaffolding worker. Six reels, none made his top half. Without someone to roast, there’s nothing to send to the group chat.'),
    'family': ('Getting told off at home', 'Somebody at home tells Anuj off: his dad (played by him, in black and white) calls his videos “waahiyat”, his mom teases his self-praise, a friend says reels aren’t a job. His crowd follows him to watch him roast everyone else. Flip it and they keep scrolling: all three in his bottom quarter.'),
}
L_BITS = {
    'skin': ('Her day, then the product', 'Skincare built around a woman’s day: Ice D’s “her day’s packed” post was their best of the run; “we celebrate your skin” held the top half.'),
    'people': ('Talking to people', 'Captions that talk to people: the “council of makeup lovers” made the top quarter; first-job confusion in Bengali held the top half.'),
    'eye': ('Eyeliner & mascara as the star', 'Liner and mascara as the main character: “your eyeliner does it all”, “this wand came to work”, “your lashes got the main character”, “Eyeconic” instants. Four reels, all in the bottom half. Their biggest lane this run, and their weakest.'),
    'oneoff': ('One-offs', 'Posted once this run: a golden-hour lip look (top quarter) and a “downloading” overlay (top half).'),
}
A_DISPATCH = {
    'all': ('The read so far', 'His crowd comes to watch Anuj roast everyone else: his own industry, his own BMW, his own city. They show up hardest when his friends are in it. They keep scrolling when he’s alone, preachy, or the one getting told off.'),
    1: ('Blinkit carried the run', 'The Mother’s Day Blinkit ad, where his mom swaps him for an AI helper because he’s “too busy being an influencer”, became the best-fed reel of the 90 days. The creators-who-went-viral-once sketch with Ruhee Dosani made his top 10% too. The BMW debuted pre-roasted (“we get it bro”) and made the top quarter. Everything where it’s just him at home (sleeping through the alert, getting lectured about reels) sank to the bottom half.'),
    2: ('The Melodi run', 'His Parle-marketing-team parody, riding the Melodi meme, landed #2 of the 90 days. Street bits with strangers (a rap battle at a cosmetics stall, a made-up driver, a bathrobe roast) all held the top half, none higher. The sponsored Startup Village panel came dead last: #40.'),
    3: ('A quiet week at home', 'Seven of ten in his bottom half: glasses reviewed with sad violins, mom’s teasing, a tattoo, a piano take. Anuj alone, about Anuj. The second BMW reel saved the week, beating the 10 before it. The Lotto x H&M jersey lookbook came #38.'),
    4: ('Back with the boys', 'Six of ten in his top quarter. The office crew’s sock-as-a-tissue prank beat the 20 reels before it; the Ronaldo-was-robbed rant beat 16. He tried a new 5-second format twice: “bad place” → Andheri East made his top 15%, “bad state” → UP sank to #35.'),
}
L_DISPATCH = {
    'all': ('The read so far', 'Ten reels in and the pattern is loud: Lakmé lands when a woman’s day leads and the product follows. When the eyeliner or mascara is the main character, the crowd scrolls. Captions only so far, so treat it as a first sighting.'),
    1: ('Her day beat the packshot', 'Nine of ten beat their usual views (about 2× on average), but not equally. The Ice D post about a woman’s packed day pulled 3.7× and topped the run. All four eye-makeup reels, where the eyeliner or mascara is the hero, finished in the bottom half.'),
}
A_DRIVERS = {
    'idea': 'Most of his feed lives or dies on the idea. Roasting a world he’s part of (creators, his BMW, Andheri East) lands. Being the one roasted (dad, mom, a lecture) or having no joke at all (piano, a lookbook) gets scrolled past.',
    'moment': 'Five reels rode something already in the air: Mother’s Day (Blinkit), the Melodi meme (Parle), Ronaldo vs Messi, Mbappé leaving Madrid, a flyover scandal. Three made his top quarter, including #1 and #2. The moment opens the door; the bit still has to walk through. The flyover rant was angry, not funny, and stalled.',
    'faces': 'Put him with people and the floor rises: the office crew (sock, Ludo), the turf boys, strangers on the street, friends at a restaurant. All nine landed in his top half and four made the top quarter. None of them sank to the bottom half.',
    'craft': 'Five reels lost on execution, not the idea: too long (a 90-second parking lie, 88 seconds about glasses), too short (a one-second tattoo), too busy (the Clubhouse mashup), or the brand hiding on the end card. None made the top quarter.',
}
L_DRIVERS = {
    'faces': 'A person leading the post (the makeup-lovers “council”, “my lips”, her packed day) is where Lakmé landed: all three in the top quarter.',
    'idea': 'Product-as-hero captions (“your lashes got the main character”, “Eyeconic” instants) and a generic skin post: none made the top quarter.',
    'craft': 'Packshots and overlays: the eyeliner and mascara-wand packshots, a “downloading” screen. A slow climb at best.',
    'moment': 'One real-life moment (first-job confusion, in Bengali). Top half.',
}
A_PRIMER = 'Anuj (@anuj.mp4) is a Mumbai comedy creator: crew sketches, street bits with strangers, a BMW he can’t stop roasting himself about, and the odd brand deal. 40 reels since May 1, ranked against each other at day 7.'
L_PRIMER = 'Lakmé (@lakmeindia) is one of India’s oldest beauty brands. Ten reels so far, read from captions only, so the reader is still getting to know them.'

for f, takes, bits, disp, drivers, primer in ((anuj, A_TAKE, A_BITS, A_DISPATCH, A_DRIVERS, A_PRIMER), (lakme, L_TAKE, L_BITS, L_DISPATCH, L_DRIVERS, L_PRIMER)):
    for p in f['posts']:
        p['driver'], p['take'] = takes[p['id']]
    for sr in f['series']:
        sr['name'], sr['take'] = bits[sr['id']]
    f['dispatch'] = {str(k): {'head': a, 'body': b} for k, (a, b) in disp.items()}
    f['drivers'] = drivers
    f['primer'] = primer
assert all('take' in p for f in (anuj, lakme) for p in f['posts'])

data = json.dumps({'feeders': [anuj, lakme]}, ensure_ascii=False)
page = HERE / 'feeder-reader.html'
if page.exists():
    html = page.read_text()
    s = html.index('const DATA = ') + len('const DATA = ')
    e = html.index(';\nconst ', s)
    page.write_text(html[:s] + data + html[e:])
print('ok', len(anuj['posts']), len(lakme['posts']))
