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
      "tile_headline": "Showing the Feeling You're Pretending Not to Have",
      "tile_read": "The viewer isn't waiting for a confession — they're already inspecting the performance for the crack.",
      "modal_headline": "Showing the Feeling You're Pretending Not to Have",
      "the_hook": "The viewer isn't waiting for a confession — they're already inspecting the performance for the crack.",
      "the_breakdown": [
        "You walk into something that looks like normal social behavior. A check-in, a casual moment, someone going about their day. Nothing flags it as confessional. Then a small detail — a caption, a frame choice, a lyric — tells you to stop watching the scene and start watching the person inside it.",
        "From there, the gap does the work. The performed cover holds just long enough for you to feel the distance between it and what's actually showing. A face shifts. A camera lingers somewhere it shouldn't. The fantasy the person is living quietly fails to match the reality in the frame. You keep watching because you're waiting for the slip to complete itself.",
        "When it lands, it lands as recognition. Not surprise — you already saw it coming. That's the point. You caught the real feeling before it was admitted, and that small act of seeing is exactly what the post was built to give you."
      ],
      "why_it_works": "The viewer is cast as a witness, not an audience. Once the reframe tells them what to inspect, every detail becomes evidence — and finding evidence feels like insight. The post doesn't deliver a feeling; it lets the viewer discover one, which makes the payoff feel earned rather than handed over.",
      "what_to_keep": [
        "The reframe has to arrive early enough to prime inspection before the cover slips.",
        "The tell must be visible, not stated — the viewer needs to see it, not be told it.",
        "The hidden feeling has to be instantly recognizable without any explanation."
      ],
      "what_kills_it": [
        "Stating the feeling directly collapses the gap the whole pattern depends on.",
        "If the visual confirm never arrives, the viewer has nothing to catch and the inspection goes nowhere.",
        "A sincere tone throughout removes the cover, so there's nothing to see through."
      ]
    },
    "proof_rail": [
      {
        "post_key": "p/dxxb6rrif3e#f16",
        "proof_label": "Face Cracks the Disclaimer",
        "proof_headline": "The eye-roll arrives exactly when the lyric says she's fine.",
        "post_read": "The post opens on what looks like a warm video call — man in the main frame, woman smiling in a small inset window — until the text at the bottom reframes it: he is talking about his local best friend, and she is just listening. At 00:05 the inset expands to fill the screen and holds on her face, which is no longer smiling — she squints, pulls at her cheek, and rolls her eyes, the composed look from three seconds ago completely gone. \"CAUSE I DON'T CARE\" drops in large white caps over her face at the exact beat the lyric hits and the eye-roll peaks, and the joke lands because her face already gave it away before the text showed up.",
        "what_clicked": "The cut to her face forces the viewer to inspect the very thing she is trying to hide, so the \"I don't care\" overlay reads as a confession caught in the act, not a denial that lands clean.",
        "evidence": [
          "Woman smiles in the small inset window while the man talks, performing ease.",
          "Inset expands at 00:03, shifting all attention to her before she can reset.",
          "Hard cut at 00:05 holds on the squint, cheek-pull, and eye-roll in close-up.",
          "\"CAUSE I DON'T CARE\" appears over her face as lyric and eye-roll land together.",
          "Upbeat pop track plays throughout, making the dismissal feel even less convincing."
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
        "proof_label": "Camera as Editorial Witness",
        "proof_headline": "The caption makes the lens the thing being watched, not the gossip it caught.",
        "post_read": "The reel drops you mid-story — she's already past the setup, already at \"friend X hooked up with friend Y,\" and the three men in the podcast studio are already reacting before you've oriented yourself. Then the Hindi kicks in: someone off-camera mutters \"Somu, kya kar raha hai yaar? Saniya ko pasand aayega baadme\" — a guy telling the videographer to stop recording, which means the videographer kept recording. The whole thing ends on \"He has all the tea,\" and the caption — \"understood the assignment a little too well\" — has already told you that the real subject was never the hookups, it was the person holding the camera.",
        "what_clicked": "The caption reframes the footage as evidence of the videographer's choices, so every cut and angle reads as something he decided to keep rather than something that just happened to be there.",
        "evidence": [
          "\"A little too well\" implies the camera went past its job description.",
          "Off-camera voice tells Somu to stop — he keeps rolling anyway.",
          "\"He has all the tea\" lands as a verdict on the videographer, not the friends.",
          "Hindi interjection at the end signals a private moment that was not meant to be caught.",
          "\"Got everything\" — her word is \"everything,\" not \"it\" or \"that.\""
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
        "proof_label": "Manufactured Audience",
        "proof_headline": "The lyric invents a crowd the airport never provided.",
        "post_read": "The post opens on a woman in a black tank top already swaying in selfie frame, smiling before the gate is even in sight — the performance is running before the product appears. The text overlay lands fast: \"How I walk in the airport because I downloaded Digi Yatra,\" which tells you immediately this is a feeling being acted out, not a feature being shown. The camera then passes through the green-lit e-gate in one uncut shot while \"All eyes on us / They watching us\" plays directly over three uniformed security personnel who are not watching her at all.",
        "what_clicked": "The music assigns an audience to people doing their jobs, and the empty queue lines behind her make the gap between that fantasy and the actual airport visible enough to be the joke.",
        "evidence": [
          "\"All eyes on us\" plays while three uniformed personnel visibly ignore her.",
          "Queue lines behind the e-gate are completely empty throughout the shot.",
          "Green checkmark and face-scan icon appear as the camera moves through, not before.",
          "Caption \"The superiority complex is insane\" names the fantasy only after the shot commits.",
          "Selfie frame drops at 0:02 — the switch from face to gate is where the brag goes public."
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
      "tile_headline": "The Full-Range Loop Viewers Have to Finish",
      "tile_read": "Give the viewer a count and a sequence, and they can't leave until it closes.",
      "modal_headline": "The Full-Range Loop Viewers Have to Finish",
      "the_hook": "Give the viewer a count and a sequence, and they can't leave until it closes.",
      "the_breakdown": [
        "The viewer walks in knowing exactly what they signed up for — a named set, a number, a sequence that has a start and an end. That framing turns passive watching into active tracking before a single unit has played.",
        "Each item in the range runs the same tight loop: it appears, it lands, it resolves, and the next one starts. The count ticking down is what keeps the viewer in — they're not waiting to be told what to pick, they're running their own bracket the whole way through.",
        "By the end, the feeling isn't \"I watched a review.\" It's completion — the whole set has been seen, and the viewer has quietly built their own shortlist without being handed one."
      ],
      "why_it_works": "A declared, countable set turns watching into a task the viewer assigned themselves. The micro-loop structure resets attention at every unit without breaking the rhythm, and the count keeps the viewer aware of what's left. Leaving early means leaving a task unfinished — and most people won't.",
      "what_to_keep": [
        "Declare the count up front so the viewer knows exactly what the sequence owes them.",
        "Run every unit through the same loop — any break in the rhythm breaks the bracket.",
        "Let the viewer reach their own shortlist; the moment you tell them what to pick, the task collapses."
      ],
      "what_kills_it": [
        "Skipping units or leaving the count open-ended removes the closure the whole mechanic runs on.",
        "Turning any unit into a longer feature moment breaks the rhythm and signals the sequence isn't equal.",
        "Telling the viewer which one to choose converts their active comparison into passive receiving."
      ]
    },
    "proof_rail": [
      {
        "post_key": "p/dye-ckenrme#f10",
        "proof_label": "Declared Paradox",
        "proof_headline": "Framing abundance as the problem keeps viewers watching all seven shades to solve it.",
        "post_read": "The reel opens on a woman holding all seven SUGAR lipsticks fanned between her fingers, and the text lands immediately — \"POV: Shade range is SO good you can't decide\" — so the viewer's job is set before a single shade is shown. Each shade then runs the same four-beat loop: closed tube, bottom label, open bullet, application and smile, with the shade name updating at the bottom to match, and the whole thing resets seven times without breaking pace. By the time a yellow \"LAST ONE\" cuts in at 0:46, the viewer has been quietly running their own bracket the entire way through and still hasn't picked one.",
        "what_clicked": "The opening frame hands the viewer a problem they accept as their own, so every subsequent shade feels like evidence in a decision they're already committed to making.",
        "evidence": [
          "\"POV: Shade range is SO good you can't decide\" appears before shade 01 is shown.",
          "All seven tubes are fanned in both hands at 0:00 before any individual cycle begins.",
          "Bottom text overlay updates per shade — \"01 Santorini Sunset\" through \"07 Tuscany Truffle.\"",
          "Yellow interjection \"WOOOOOOW... Last one.\" breaks the neutral cadence at 0:45–0:48.",
          "Caption CTA asks viewers to \"tell us the number you're stuck between,\" matching the numbered sequence on screen."
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
        "post_read": "The reel opens cold on a lip close-up — \"01 Peach Ping\" and \"MONDAY\" stacked at the bottom of frame, no setup, just the first slot already filled. From there it runs a strict two-beat cycle for every day: tight lip shot with the shade tube held to the chin, then a medium lifestyle shot where the outfit, background color, and props have all shifted to match that shade's mood — pink blazer on Tuesday, green dress on Friday, tie-dye on Saturday. At the end, both hands come up holding all seven tubes at once against a purple background, and the count the viewer has been running since Monday closes.",
        "what_clicked": "The calendar structure borrows the viewer's own sense of how a week works — once Monday is named, Sunday becomes an obligation, not a choice.",
        "evidence": [
          "\"01 Peach Ping MONDAY\" opens the reel with no introduction.",
          "Every day opens on a lip close-up before cutting to a lifestyle shot.",
          "Background color and outfit shift with each shade across all seven days.",
          "Voiceover calls each day by name, locking the sequence to a familiar order.",
          "Both hands hold all seven tubes simultaneously in the final shot."
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
        "proof_label": "Regret Frame",
        "proof_headline": "Performed regret turns a seven-shade range into a decision the viewer feels they're already behind on.",
        "post_read": "The post opens on a woman cupping all seven Glide Peptide Plumping Gloss Stick tubes at once, asking \"Tell me why I haven't bought these sooner\" before a single shade has been named — the regret lands before any product information does. From 00:11 onward, the same four-beat loop runs seven times: hold the tube, call the name, apply it, smile, with numbered overlays (\"01 Peach Ping,\" \"02 Pink Pop,\" all the way to \"07 Brown Buzz\") so you always know exactly where you are in the set. The closing line — \"suitable for all Indian skin tones\" — arrives at the exact moment you've already narrowed to a shortlist, and it removes the last reason to wait.",
        "what_clicked": "She names the viewer's hesitation out loud at 00:05 — \"you don't know which shade works for you\" — so every swatch that follows is answering a question the viewer already had, not pitching one they didn't.",
        "evidence": [
          "\"Tell me why I haven''t bought these sooner\" plays before any shade is shown.",
          "Numbered overlays 01 through 07 run with each shade name from 00:11 to 00:36.",
          "\"You don''t know which shade works for you\" spoken at 00:05, naming the exact block.",
          "A click sound effect punctuates \"It''s non-retractable\" at 00:13, marking a product detail mid-loop.",
          "Woman returns to holding all seven tubes at 00:39, closing the frame opened in the first shot."
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
      "tile_headline": "The Claim That Disproves Itself While Being Made",
      "tile_read": "The subject tells you who they are while the same frame shows you they aren't.",
      "modal_headline": "The Claim That Disproves Itself While Being Made",
      "the_hook": "The subject tells you who they are while the same frame shows you they aren't.",
      "the_breakdown": [
        "You walk in mid-performance. The subject is already declaring something — their character, their values, their version of events. You get a clean, confident picture of how they see themselves before anything pushes back.",
        "Then the contradiction appears in the same breath. Another person, the camera, their own body — something in the frame quietly disagrees. The subject keeps going anyway, which is exactly what holds you. Every new claim widens the gap instead of closing it.",
        "You leave holding the receipt. The subject never names the gap, never flinches, never resolves it. That's your job, and the post hands it to you without asking."
      ],
      "why_it_works": "The viewer gets to be the smartest person in the room without being told they are. No twist is needed because the gap is visible the whole time — the satisfaction is confirmation, not surprise. That's a stronger pull than a reveal, because the viewer earns the conclusion themselves.",
      "what_to_keep": [
        "The contradiction has to live inside the same frame as the claim — cut away and you lose everything.",
        "The subject must stay committed; the moment they wink at the camera, the gap collapses into a bit.",
        "Let the viewer hold the judgment — the second you name the gap out loud, you've stolen their moment."
      ],
      "what_kills_it": [
        "If the subject acknowledges the gap, admits fault, or grows, the tension resolves and there's nothing left to confirm.",
        "Withholding the contradiction as a final twist turns confirmation into surprise, which is a different pattern entirely.",
        "An outside voice explaining the irony does the viewer's work for them and kills the satisfaction."
      ]
    },
    "proof_rail": [
      {
        "post_key": "p/dxy_ou6kc9k#f27",
        "proof_label": "Live Deflation By Subject",
        "proof_headline": "Sania Mirza dismantles each claim in the same breath it's made, so the gap never gets a chance to close.",
        "post_read": "The reel opens on a man on a bench, holding a jar of pickles, delivering a mock-serious testimonial — \"It's almost as if I am the Sania Mirza of Pickleball\" — with complete conviction. Every claim he makes is answered immediately by Sania Mirza herself, on the same court, in the same cut: he says he respects the rules, she calls \"one tip out\" against him; he says you learn to accept the result, she tells him she'll see him outside. The whole collapse gets absorbed by a single overlay at the end — \"You don't have to be Sania Mirza to play\" — which turns his humiliation into the product's actual pitch.",
        "what_clicked": "The deflation never pauses long enough to become a punchline — Sania's response lands inside the same moment as the claim, so the viewer is always watching the gap widen in real time rather than waiting for a reveal.",
        "evidence": [
          "\"Sania Mirza of Pickleball\" claim answered by \"Come, fight me\" at 00:13.",
          "Narrator and Sania wear matching black-over-pink outfits, making the contrast visual before it's verbal.",
          "\"Rules — I don't take the rules lightly\" followed immediately by Sania calling \"one tip out\" at 00:36.",
          "\"You learn to accept\" answered by \"I'll see you outside\" before the sentence can settle.",
          "Pickle jar on the bench ties the pickleball pun to the character before a word is spoken."
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
        "proof_label": "Dual-Frame Collapse In Progress",
        "proof_headline": "Son performs sincerity while smirking at the camera he just denied was rolling.",
        "post_read": "The caption — \"telling my mom i'm in love with a married woman\" — sits at the top of the frame before anyone speaks, so you already know where this is going before the son opens his mouth. What keeps you watching is the escalation ladder: each new detail he adds (\"her husband is boring,\" \"she's 35,\" \"I can save her\") lands while his mother's outrage is already maxed out, so every rung feels deliberate rather than defensive. Then at 0:23 she says \"I think you're recording,\" he denies it directly to the camera that is visibly rolling, and at 0:52 he turns and smirks straight into the lens — confirming the whole thing was a performance for you, not a confession to her.",
        "what_clicked": "The mother's suspicion collapses his cover story mid-scene — he lies to her face while the camera proves the lie to you in real time, and the smirk at 0:52 closes the loop.",
        "evidence": [
          "\"I think you're recording\" — denied at 0:23 while the camera rolls uncut.",
          "Static single shot, no cuts — the camera's presence is never hidden from the viewer.",
          "Each escalation is unprompted: boring husband, can't satisfy her, I can save her.",
          "Direct smirk into the lens at 0:52 while mid-sentence about saving her.",
          "Black and white filter frames the scene as composed, not candid."
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
        "proof_headline": "Self-declared liberal collapses the instant power is offered — and says so out loud.",
        "post_read": "The post opens mid-rant: curly hair, silver chain, animated hands, someone already deep into calling politicians \"anpadh gundas\" and citing his own credentials — \"cool, liberal, woke\" — as the reason you should trust him. At 0:07, a flat-faced man with a mustache cuts in with one question — \"You wanna join us or what?\" — and the first guy's body swings toward the offer before he's finished a thought. The close-out is \"I have no morals,\" delivered with a nervous laugh, which isn't a punchline so much as a signed confession.",
        "what_clicked": "The body turn at 0:08 lands before the words do — the physical pivot makes the collapse visible a half-second before the mouth catches up, so the viewer clocks the betrayal twice.",
        "evidence": [
          "\"cool, liberal, woke\" — self-image staked before any offer appears.",
          "\"You wanna join us or what?\" — the entire reversal triggered by one flat question.",
          "Body swings toward the second figure before the first character speaks again.",
          "\"I have no morals\" delivered with a nervous laugh, no recovery attempt.",
          "Mustached man's stern silence makes the offer feel like a test already passed."
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
