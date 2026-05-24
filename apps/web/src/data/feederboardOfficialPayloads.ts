export type MetricCard = {
  label: string;
  value: string;
  detail?: string;
  accent?: boolean;
};

export type ProofBlock = {
  post_key: string;
  proof_label: string;
  proof_headline: string;
  post_read: string;
  what_clicked: string;
  evidence: string[];
  metrics: MetricCard[];
};

export type FocusOverview = {
  focus_id: string;
  tile_label: string;
  tile_headline: string;
  tile_read: string;
  modal_headline: string;
  the_hook: string;
  the_breakdown: string[];
  why_it_works: string;
  what_to_keep: string[];
  what_kills_it: string[];
};

export type FeederboardFocus = {
  account: '@saniyamirwani' | '@trysugar' | '@anuj.mp4';
  accountLabel: string;
  accountMeta: string;
  focus_id: string;
  focus_overview: FocusOverview;
  proof_rail: ProofBlock[];
  focusMetrics: MetricCard[];
};

export const FEEDERBOARD_FOCUSES: FeederboardFocus[] = [
  {
    "account": "@saniyamirwani",
    "accountLabel": "Saniya Mirwani",
    "accountMeta": "creator · 3 proof posts · last 90 days",
    "focus_id": "pattern_02",
    "focusMetrics": [
      {
        "label": "Strongest",
        "value": "Top 3.7%",
        "detail": "recent reel rank",
        "accent": true
      },
      {
        "label": "Support",
        "value": "3",
        "detail": "posts in this read"
      },
      {
        "label": "Signal",
        "value": "Comments",
        "detail": "led the lift"
      }
    ],
    "focus_overview": {
      "focus_id": "pattern_02",
      "tile_label": "Caught Performing",
      "tile_headline": "The feeling you're performing over the one you actually have",
      "tile_read": "The post lets you see through someone before they let you.",
      "modal_headline": "The feeling you're performing over the one you actually have",
      "the_hook": "The post lets you see through someone before they let you.",
      "the_breakdown": [
        "You walk into something that looks like a normal moment — someone going about their day, holding a mood together, keeping it social. Nothing announces itself as a reveal. But something in the frame, the caption, or the way the camera sits on a face tells you to look closer than the performer wants you to.",
        "From there, the gap is the whole game. The cover story is running, but it's leaking. A face does something small. The camera lingers a beat too long. The status the moment is supposed to project quietly doesn't add up. You're not waiting to be told what's happening — you're already reading it, and the post knows you are.",
        "It ends before the admission comes. You get the recognition without the confession, which is exactly why it lands. The satisfaction is in catching it yourself, not in being handed it."
      ],
      "why_it_works": "People are wired to read faces and situations for what's being managed rather than what's being said — it's how social life actually works. This pattern puts that instinct to use. The viewer isn't a passive audience; they're doing something, and the small reward of being right is what makes the whole thing feel good.",
      "what_to_keep": [
        "The gap between performed feeling and real feeling has to be visible, not just implied.",
        "Let the viewer find the tell — don't caption it into the ground before they get there.",
        "The hidden feeling needs to be instantly recognizable once spotted, no explanation required."
      ],
      "what_kills_it": [
        "Stating the real feeling outright collapses the inspection and turns it into a confession.",
        "If the visual confirm never arrives, the viewer has nothing to catch and the gap just hangs there.",
        "Playing it fully sincere removes the cover, and without a cover there's nothing to see through."
      ]
    },
    "proof_rail": [
      {
        "post_key": "p/dxxb6rrif3e#f16",
        "proof_label": "Eye-Roll Beats the Lyric",
        "proof_headline": "Her face confesses before the text overlay even shows up.",
        "post_read": "The post opens as a video call that reads warm — man in the main frame looking neutral, woman smiling in a small inset window — and the bottom text is the only thing that reframes it as a wound: he is talking about how much he loves his local best friend, and she is just listening. At 00:03 her inset expands to fill the screen, which is the post's first move against her, because the expansion happens before she has time to reset the smile. By 00:05 the close-up is holding on a squint, a hand going to her cheek, and a full eye-roll — the composed look from three seconds ago completely gone — and the viewer has already caught it before the text does anything. \"CAUSE I DON'T CARE\" drops in large white caps at the exact beat the lyric hits and the eye-roll peaks, landing not as a punchline but as a caption for something the face already said out loud. The upbeat pop track underneath makes the whole denial feel even less convincing, because nothing about the sound matches the effort she is putting into not caring.",
        "what_clicked": "The inset expansion is timed to the beat but functions as an ambush — it moves the camera onto her face at the one moment she cannot manage what it shows.",
        "evidence": [
          "Woman holds a smile in the small inset while he talks about his local friend.",
          "Inset expands at 00:03, beat-timed, before her expression has reset.",
          "Close-up at 00:05 holds on squint, cheek-pull, and eye-roll simultaneously.",
          "\"CAUSE I DON'T CARE\" overlays her face as lyric and eye-roll land together."
        ],
        "metrics": [
          {
            "label": "Proof",
            "value": "1/3",
            "detail": "clearest example",
            "accent": true
          },
          {
            "label": "Best rank",
            "value": "Top 6.1%",
            "detail": "recent reel rank"
          },
          {
            "label": "Baseline",
            "value": "7.2×",
            "detail": "comments lift"
          },
          {
            "label": "Views",
            "value": "313.7K",
            "detail": "selected post"
          },
          {
            "label": "Comments",
            "value": "152",
            "detail": "conversation signal"
          }
        ]
      },
      {
        "post_key": "p/dxewewacicl#f16",
        "proof_label": "Videographer as Accidental Narrator",
        "proof_headline": "The caption makes the camera operator the subject, not the gossip he recorded.",
        "post_read": "She drops you into the middle of the story — \"friend X hooked up with friend Y\" is already out before you've placed yourself in the podcast studio — and the three men are already reacting, which means you're catching up to a room that already knows something. The tell arrives in Hindi: someone off-camera mutters \"Somu, kya kar raha hai yaar? Saniya ko pasand aayega baadme\" — a direct instruction to stop recording, which the videographer ignored. That ignored instruction is the whole post. By the time she lands on \"He has all the tea,\" the caption has already done its work: \"understood the assignment a little too well\" isn't praise for the footage, it's a read on the person who chose to keep rolling when someone told him not to. The hookups are the surface; the videographer's editorial judgment is what you're actually watching.",
        "what_clicked": "The off-camera voice telling Somu to stop is the mechanism — it turns passive recording into a decision, and that decision is what the caption is really about.",
        "evidence": [
          "Off-camera voice tells Somu to stop; he keeps rolling anyway.",
          "\"A little too well\" in the caption signals the camera exceeded its brief.",
          "\"He has all the tea\" lands as a verdict on the videographer's choices, not the friends' behavior.",
          "Hindi interjection marks a private moment explicitly not meant to be captured."
        ],
        "metrics": [
          {
            "label": "Proof",
            "value": "2/3",
            "detail": "camera-led example",
            "accent": true
          },
          {
            "label": "Best rank",
            "value": "Top 3.7%",
            "detail": "recent reel rank"
          },
          {
            "label": "Signal",
            "value": "Comments",
            "detail": "discussion-led"
          },
          {
            "label": "Support",
            "value": "Core",
            "detail": "same emotional engine"
          }
        ]
      },
      {
        "post_key": "p/dxtkzzzcfna#f16",
        "proof_label": "Borrowed Crowd, Empty Gate",
        "proof_headline": "The song invents witnesses the airport forgot to provide.",
        "post_read": "This post figured out that the gap between performed feeling and real feeling works even harder when the environment actively refuses to cooperate. She's already smiling in selfie frame before the gate is in sight — the confidence is fully operational before there's anything to be confident about. The text overlay hits immediately: \"How I walk in the airport because I downloaded Digi Yatra,\" which locks in that this is a mood being enacted, not a process being shown. Then the camera passes through the green-lit e-gate in one uncut move while \"All eyes on us / They watching us\" plays directly over three uniformed security personnel who are visibly doing anything but watching her. The queue lines behind the gate are empty the whole time — the fast-track fantasy has no crowd to beat, no line to skip, no audience to impress — and the caption \"The superiority complex is insane\" only names the delusion after the shot has already made it visible.",
        "what_clicked": "The music assigns an audience to people doing their jobs, and the empty lines make the distance between that fantasy and the actual airport legible enough to become the joke.",
        "evidence": [
          "\"All eyes on us\" plays while three uniformed personnel visibly ignore her.",
          "Queue lines behind the e-gate are empty throughout — nothing to bypass.",
          "Selfie frame drops at 0:02; the switch from face to gate is where the performance goes public.",
          "Caption names \"superiority complex\" only after the shot has already exposed it."
        ],
        "metrics": [
          {
            "label": "Proof",
            "value": "3/3",
            "detail": "status-fantasy example",
            "accent": true
          },
          {
            "label": "Signal",
            "value": "Views",
            "detail": "clean visual read"
          },
          {
            "label": "Support",
            "value": "Core",
            "detail": "same gap mechanic"
          },
          {
            "label": "Mode",
            "value": "Inspect",
            "detail": "viewer catches the frame"
          }
        ]
      }
    ]
  },
  {
    "account": "@trysugar",
    "accountLabel": "SUGAR Cosmetics",
    "accountMeta": "brand · 3 proof posts · last 90 days",
    "focus_id": "enumerated-shade-range-completion",
    "focusMetrics": [
      {
        "label": "Strongest",
        "value": "Top 4.8%",
        "detail": "recent reel rank",
        "accent": true
      },
      {
        "label": "Support",
        "value": "3",
        "detail": "range posts"
      },
      {
        "label": "Signal",
        "value": "Views",
        "detail": "choice-led loop"
      }
    ],
    "focus_overview": {
      "focus_id": "enumerated-shade-range-completion",
      "tile_label": "Shade Range Closer",
      "tile_headline": "Name the Full Set, Then Make Them Watch It Close",
      "tile_read": "The moment you tell someone there are N things to see, they can't leave until they've seen all N.",
      "modal_headline": "Name the Full Set, Then Make Them Watch It Close",
      "the_hook": "The moment you tell someone there are N things to see, they can't leave until they've seen all N.",
      "the_breakdown": [
        "You walk in knowing exactly what you're getting. The set is declared upfront — a count, a name, a clear boundary — so the viewer isn't guessing whether more is coming. That declaration is the whole setup. It turns a piece of content into a sequence with a known end, and a sequence with a known end is almost impossible to leave early.",
        "What keeps you watching is a small loop that runs once per unit and resets. Each item appears, lands, resolves, and the next one starts. The count ticking down in your head is doing real work — you're not just watching, you're tracking. You're comparing what you just saw to what came before and holding it against what's still left.",
        "When it ends, you don't feel like you were shown something. You feel like you finished something. The shortlist you leave with feels like yours, because you built it by watching, not because someone handed it to you."
      ],
      "why_it_works": "The count converts a passive viewer into an active one before the first unit even plays. Once someone is tracking a sequence, they're invested in closing it — not because the content is gripping, but because an open count feels unfinished. The pattern borrows the same pull as a checklist.",
      "what_to_keep": [
        "Declare the full count early — the tension only exists if the viewer knows how far they have to go.",
        "Keep each micro-loop identical in structure so the comparison is clean and the rhythm holds.",
        "Let the viewer land on their own answer; the pattern breaks the moment you steer them toward one."
      ],
      "what_kills_it": [
        "Leaving the set open-ended removes the finish line and kills the reason to stay.",
        "Skipping units or breaking the loop structure collapses the sense of a complete sequence.",
        "Telling the viewer which option wins turns a comparison into a recommendation, and they stop deciding for themselves."
      ]
    },
    "proof_rail": [
      {
        "post_key": "p/dye-ckenrme#f10",
        "proof_label": "Abundance Framed as Problem",
        "proof_headline": "The opening text makes indecision the premise, so every shade becomes evidence in a verdict the viewer is already trying to reach.",
        "post_read": "Most posts in this pattern declare a count and let the sequence do the work — this one goes a step further by declaring the count as a problem before shade 01 even appears. \"POV: Shade range is SO good you can't decide\" lands while all seven tubes are still fanned between her fingers, which means the viewer enters the loop already mid-deliberation. Each shade then runs the same four-beat cycle — closed tube, bottom label, open bullet, application and smile — with the text at the bottom swapping from \"01 Santorini Sunset\" through \"07 Tuscany Truffle\" like a scoreboard the viewer is quietly updating. The city-themed names do extra work here: Positano, Cairo, Tuscany each carry a mood that the swatch alone wouldn't, so the comparison isn't just shade versus shade, it's a feeling versus a feeling. When the verbal \"WOOOOOOW... Last one.\" breaks in at 0:45, it confirms what the viewer already suspects — they've watched all seven and still haven't settled — and the caption's \"tell us the number you're stuck between\" converts that unresolved feeling directly into a comment.",
        "what_clicked": "Naming indecision as the premise in the first frame makes the viewer's own uncertainty feel like the point of watching, not a side effect of it.",
        "evidence": [
          "\"POV: Shade range is SO good you can't decide\" appears before any individual shade is shown.",
          "All seven tubes fanned in both hands at 0:00 before the per-shade loop begins.",
          "Bottom text updates per shade from \"01 Santorini Sunset\" through \"07 Tuscany Truffle.\"",
          "Verbal \"WOOOOOOW... Last one.\" breaks the neutral cadence at 0:45–0:48."
        ],
        "metrics": [
          {
            "label": "Proof",
            "value": "1/3",
            "detail": "choice-problem example",
            "accent": true
          },
          {
            "label": "Best rank",
            "value": "Top 4.8%",
            "detail": "recent reel rank"
          },
          {
            "label": "Signal",
            "value": "Views",
            "detail": "range curiosity"
          },
          {
            "label": "Products",
            "value": "7",
            "detail": "countable loop"
          }
        ]
      },
      {
        "post_key": "p/dxrbg0zilbr#f10",
        "proof_label": "Calendar Loop",
        "proof_headline": "Seven days frame seven shades as a set that must complete.",
        "post_read": "The reel opens cold on a lip close-up — \"01 Peach Ping\" and \"MONDAY\" stacked at the bottom of frame, no setup, no introduction, just the first slot already filled. What this post figured out is that the calendar is a count the viewer already owns — once Monday is named, Sunday exists whether the reel earns it or not. From there it runs a strict two-beat cycle: tight lip shot with the shade tube held to the chin, then a medium lifestyle shot where the outfit, background color, and props have all shifted to match that shade's mood — pink blazer on Tuesday, green dress on Friday, tie-dye on Saturday. The voiceover calls each day by name in sequence, which means the viewer isn't tracking an arbitrary number but a structure they already have memorized. When both hands come up holding all seven tubes at once against a purple background, the count closes — not because the reel announced it was over, but because Sunday was always the last slot.",
        "what_clicked": "The calendar borrows a sequence the viewer already carries, so the obligation to finish it was never the reel's to create.",
        "evidence": [
          "\"01 Peach Ping MONDAY\" opens with no context — the slot is simply filled.",
          "Outfit, background color, and props shift to match each shade across all seven days.",
          "Voiceover names each day in order, locking the sequence to a structure the viewer already knows.",
          "Both hands hold all seven tubes simultaneously in the final shot, closing the count visually."
        ],
        "metrics": [
          {
            "label": "Proof",
            "value": "2/3",
            "detail": "calendar example",
            "accent": true
          },
          {
            "label": "Structure",
            "value": "7 days",
            "detail": "owned by viewer"
          },
          {
            "label": "Signal",
            "value": "Completion",
            "detail": "viewer tracks count"
          },
          {
            "label": "Support",
            "value": "Core",
            "detail": "same range engine"
          }
        ]
      },
      {
        "post_key": "p/dxmvkifcctz#f10",
        "proof_label": "Regret-First Swatch Run",
        "proof_headline": "The regret hook arrives before shade one, so every swatch feels like catching up.",
        "post_read": "Most posts in this pattern declare the count and start the loop — this one inserts a layer of performed regret before either happens. The woman opens holding all seven tubes at once and asks \"Tell me why I haven't bought these sooner,\" which means the viewer's first impression of the product is someone already wishing they'd moved faster. At 00:05 she names the exact hesitation — \"you don't know which shade works for you\" — and the numbered loop that follows (01 Peach Ping through 07 Brown Buzz, each with its own overlay) is now answering a question rather than running a demo. The click sound effect at 00:13 when she says \"It's non-retractable\" is the only moment the loop breaks rhythm, and it lands a product detail without slowing the sequence down. By the time she closes on \"suitable for all Indian skin tones,\" the viewer has already built a shortlist — that line removes the last reason to wait rather than introducing a reason to buy.",
        "what_clicked": "The regret frame front-loads a feeling of being behind, so the numbered sequence reads as the viewer catching up to a decision they should have already made.",
        "evidence": [
          "\"Tell me why I haven't bought these sooner\" plays before any shade appears.",
          "\"You don't know which shade works for you\" spoken at 00:05, naming the block exactly.",
          "Numbered overlays 01 through 07 run with each shade name from 00:11 to 00:36.",
          "Click sound effect punctuates \"It's non-retractable\" at 00:13 mid-loop without breaking rhythm."
        ],
        "metrics": [
          {
            "label": "Proof",
            "value": "3/3",
            "detail": "swatch-decision example",
            "accent": true
          },
          {
            "label": "Signal",
            "value": "Saves",
            "detail": "shade reference behavior"
          },
          {
            "label": "Products",
            "value": "7",
            "detail": "bounded set"
          },
          {
            "label": "Support",
            "value": "Core",
            "detail": "same completion loop"
          }
        ]
      }
    ]
  },
  {
    "account": "@anuj.mp4",
    "accountLabel": "Anuj",
    "accountMeta": "creator · 3 core proofs · 1 adjacent · last 90 days",
    "focus_id": "gap_between_claimed_and_visible",
    "focusMetrics": [
      {
        "label": "Strongest",
        "value": "Core 3",
        "detail": "locked read",
        "accent": true
      },
      {
        "label": "Adjacent",
        "value": "1",
        "detail": "related proof"
      },
      {
        "label": "Signal",
        "value": "Recognition",
        "detail": "viewer confirms the gap"
      }
    ],
    "focus_overview": {
      "focus_id": "gap_between_claimed_and_visible",
      "tile_label": "Self-Image Collapse",
      "tile_headline": "The Claim and the Proof Arrive in the Same Frame",
      "tile_read": "Someone tells you who they are while the footage quietly disagrees.",
      "modal_headline": "The Claim and the Proof Arrive in the Same Frame",
      "the_hook": "Someone tells you who they are while the footage quietly disagrees.",
      "the_breakdown": [
        "You come in mid-performance. The subject is already stating their case — confident, certain, fully committed to a version of themselves. There's no setup because none is needed. The self-image is the setup.",
        "What keeps you watching is that the contradiction doesn't arrive later. It's already there. Something in the same shot — another person, their own body, the gap between what they're saying and what's visibly happening — is doing the opposite of confirming them. The subject keeps going. The viewer keeps watching. Nobody names it.",
        "When it ends, you leave holding the receipt. The post never called it out, never resolved it, never asked you to feel a certain way about it. It just let the gap sit there long enough for you to see it clearly, and that's enough."
      ],
      "why_it_works": "The viewer gets to be the one who notices. Nothing is handed to them — no caption explaining the irony, no reaction shot cuing the laugh. That act of noticing, of holding the contradiction yourself, is where all the satisfaction lives. The post trusts you to get it, and that trust is the whole move.",
      "what_to_keep": [
        "The contradiction has to be visible in the same frame as the claim — not after, not cut to, right there.",
        "The subject must stay fully committed; the moment they wink at the camera, the gap closes.",
        "Leave the gap unnamed — the viewer's job is to confirm it, not receive it pre-labeled."
      ],
      "what_kills_it": [
        "Ending with a twist reveals the subject always knew, which turns confirmation into a prank and loses the viewer's role entirely.",
        "Any outside voice explaining the irony removes the one thing that makes this work — the viewer doing it themselves.",
        "If the subject admits the gap or grows out of it, there's nothing left to hold."
      ]
    },
    "proof_rail": [
      {
        "post_key": "p/dxy_ou6kc9k#f27",
        "proof_label": "Celebrity Installed as Fact-Checker",
        "proof_headline": "Sania Mirza doesn't cameo — she audits every claim as it leaves his mouth.",
        "post_read": "The reel opens on a man holding a jar of pickles on a bench, delivering a mock-serious testimonial with total commitment — \"It's almost as if I am the Sania Mirza of Pickleball\" — while wearing the exact same black-over-pink outfit Sania is wearing on the court behind him, so the visual contradiction is already running before she says a word. What this post figured out is that the celebrity doesn't need to be the proof of the product — she can be the proof against the protagonist, which makes the product pitch land somewhere else entirely. Every claim he makes gets answered inside the same cut: \"Rules — I don't take the rules lightly\" is followed immediately by Sania calling \"one tip out\" against him; \"you learn to accept\" is answered by \"I'll see you outside\" before the sentence can settle. The overlay that closes it — \"You don't have to be Sania Mirza to play\" — converts his complete on-court humiliation into the actual reason to download the app, so the viewer leaves holding a discount offer that feels like a punchline they earned.",
        "what_clicked": "Sania's responses are timed to land inside the protagonist's claims rather than after them, so the gap between self-image and reality never gets a moment to close — the viewer watches it widen in real time instead of waiting for a reveal.",
        "evidence": [
          "Matching black-over-pink outfits make the contrast visual before any gameplay begins.",
          "\"Sania Mirza of Pickleball\" claim answered by \"Come, fight me\" at 00:13.",
          "Rules — I don't take the rules lightly\" followed immediately by Sania calling \"one tip out.",
          "\"You learn to accept\" answered by \"I'll see you outside\" before the line can land."
        ],
        "metrics": [
          {
            "label": "Proof",
            "value": "1/3",
            "detail": "athletic self-image",
            "accent": true
          },
          {
            "label": "Fit",
            "value": "Core",
            "detail": "same collapse engine"
          },
          {
            "label": "Mode",
            "value": "Confirm",
            "detail": "viewer tracks the gap"
          },
          {
            "label": "Signal",
            "value": "Quote",
            "detail": "compressed receipt"
          }
        ]
      },
      {
        "post_key": "p/dxoh-undfw1#f27",
        "proof_label": "Lie Told To Camera's Face",
        "proof_headline": "He denies the camera is rolling while the camera is rolling, then smirks to confirm it.",
        "post_read": "The caption does something specific here — it tells you the confession before the son does, so when he opens with \"I'm in love, mom,\" you're not waiting to find out what happens, you're watching how he plays it. He plays it calm, almost bored, which is what makes each new detail land harder than the last — \"her husband is boring,\" \"she's 35,\" \"I can save her\" — not as escalations he's forced into, but as moves he's choosing while his mother's outrage is already at its ceiling. Then at 0:23 she says \"I think you're recording,\" and he looks directly into the lens and denies it, which is the moment the post breaks open: the viewer is the proof that he's lying, and he knows it. The smirk at 0:52 — mid-sentence, mid-claim about saving this woman — closes it without a word. The black and white filter, the static single shot, the chain and tank top framed against a high-rise window — none of it reads as accidental, and that's the point. You leave holding the gap between what he told her and what he showed you.",
        "what_clicked": "He makes the viewer complicit in the lie the moment he denies the camera to the camera — you're not watching the contradiction from outside, you are the contradiction.",
        "evidence": [
          "\"I think you're recording\" denied at 0:23 directly into the rolling lens.",
          "Smirk at 0:52 lands mid-sentence, mid-claim about saving her from her husband.",
          "Each escalation — boring husband, she's 35, I can save her — is unprompted, not defensive.",
          "Single uncut shot means the camera's presence is never hidden, only verbally denied."
        ],
        "metrics": [
          {
            "label": "Proof",
            "value": "2/3",
            "detail": "relational self-image",
            "accent": true
          },
          {
            "label": "Fit",
            "value": "Core",
            "detail": "same collapse engine"
          },
          {
            "label": "Mode",
            "value": "Witness",
            "detail": "viewer holds both frames"
          },
          {
            "label": "Signal",
            "value": "Smirk",
            "detail": "gap stays open"
          }
        ]
      },
      {
        "post_key": "p/dxjldsojbr7#f27",
        "proof_label": "Idealist Folds on Offer",
        "proof_headline": "The collapse is physical before it's verbal — the body sells out half a second ahead of the mouth.",
        "post_read": "The reel opens with someone already mid-conviction — silver chain, animated hands, \"unpadh gundas\" — staking his credibility on the three words \"cool, liberal, woke\" before a single politician has appeared on screen. The claim and the credentials land in the same breath, which means when the mustached man cuts in at 0:07 with one flat question — \"You wanna join us or what?\" — the viewer is already holding the full weight of the self-image that's about to go. The body swings toward the offer before the mouth has caught up, so the betrayal registers twice: once in the pivot, once in the words. What makes this version of the pattern specific is that the subject doesn't just fold — he narrates the fold, landing on \"I have no morals\" with a nervous laugh that isn't a joke so much as a live transcript of the gap the viewer already clocked. The post never names the irony; it just lets the confession sit there, delivered by the same voice that was citing its own integrity eight seconds earlier.",
        "what_clicked": "The physical pivot at 0:08 makes the reversal legible before the admission arrives, so the viewer catches the betrayal in the body first and the words second — two receipts for the same collapse.",
        "evidence": [
          "\"cool, liberal, woke\" — credentials staked before any offer is visible.",
          "\"You wanna join us or what?\" — one flat question triggers the entire reversal.",
          "Body swings toward the second figure before the first character speaks again.",
          "\"I have no morals\" delivered with a nervous laugh, no recovery attempt."
        ],
        "metrics": [
          {
            "label": "Proof",
            "value": "3/3",
            "detail": "ideological self-image",
            "accent": true
          },
          {
            "label": "Fit",
            "value": "Core",
            "detail": "same collapse engine"
          },
          {
            "label": "Mode",
            "value": "Judge",
            "detail": "viewer confirms the fold"
          },
          {
            "label": "Signal",
            "value": "Line",
            "detail": "quotable receipt"
          }
        ]
      }
    ]
  }
];

export function focusForHandle(selectedHandle?: string): FeederboardFocus[] {
  const handle = String(selectedHandle || '').replace(/^@+/, '').toLowerCase();
  if (handle.includes('anuj')) return FEEDERBOARD_FOCUSES.filter((focus) => focus.account === '@anuj.mp4');
  if (handle.includes('sugar')) return FEEDERBOARD_FOCUSES.filter((focus) => focus.account === '@trysugar');
  if (handle.includes('saniya')) return FEEDERBOARD_FOCUSES.filter((focus) => focus.account === '@saniyamirwani');
  return FEEDERBOARD_FOCUSES;
}
