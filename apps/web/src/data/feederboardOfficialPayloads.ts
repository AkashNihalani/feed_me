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
  pattern_read: string[];
  why_it_matters: string;
  match_read: string;
  avoid_read: string;
  how_to_repeat_it: string;
  watch_out: string;
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
    account: '@saniyamirwani',
    accountLabel: 'Saniya Mirwani',
    accountMeta: 'creator · 3 proof posts · last 90 days',
    focus_id: 'pattern_02',
    focusMetrics: [
      { label: 'Strongest', value: 'Top 3.7%', detail: 'recent reel rank', accent: true },
      { label: 'Support', value: '3', detail: 'posts in this read' },
      { label: 'Signal', value: 'Comments', detail: 'led the lift' },
    ],
    focus_overview: {
      focus_id: 'pattern_02',
      tile_label: 'Caught Performing',
      tile_headline: "Showing the feeling you're pretending not to have",
      tile_read:
        'The viewer walks into a scene that reads as normal, then a reframe arrives early enough to prime inspection instead of passive watching. Attention holds in the gap between the performance and what the frame quietly gives away.',
      modal_headline: 'The camera sees more than the performer admits, and the viewer already knows it',
      pattern_read: [
        'This read puts the viewer in a specific mode: witnessing and inspecting at the same time. They are not waiting to be surprised; they are primed from the first reframe to track a performance they already know is a cover. The emotional state being hidden is universally recognizable: caring while pretending not to, feeling the sting of envy while disclaiming it, holding a social role together while something underneath is quietly coming apart.',
        'What gives this structure its force is the visual confirm — a face that cracks, a camera angle that editorializes, a POV that manufactures an audience for a moment that was never supposed to have one. The self-aware signal functions as deflection rather than confession. It names the feeling sideways, which is exactly how the feeling gets named in real life.',
      ],
      why_it_matters:
        'This is durable because it does not require a setup the viewer has to be taught. The recognition arrives before the punchline does. For an account, that means lower friction, faster payoff, and a viewer who feels seen rather than informed — a meaningfully different relationship to build over time.',
      match_read:
        'A future post belongs when a visible gap exists between what someone performs and what they actually feel, when a reframe primes inspection rather than passive watching, and when a visual confirm closes that gap. The emotional truth underneath must be immediately recognizable without explanation.',
      avoid_read:
        'A post that states the emotional gap outright rather than performing against it does not belong here, even if the feeling is relatable. Neither does a post where the visual confirm never arrives, the tone stays sincere throughout, or the structure reads as direct confession.',
      how_to_repeat_it:
        'Start inside a scene that reads as ordinary. Introduce the reframe early — not as the punchline, but as the thing that tells the viewer what to look for. Let the performance stay intact long enough to feel real, then give the viewer the visual moment that confirms what they already suspected.',
      watch_out:
        'This goes flat when the gap is explained instead of shown, or when the self-aware signal becomes the entire premise rather than the deflection. It drifts toward generic relatability the moment the visual confirm disappears and the post becomes a caption with a face behind it.',
    },
    proof_rail: [
      {
        post_key: 'p/dxxb6rrif3e#f16',
        proof_label: 'Face Cracks the Disclaimer',
        proof_headline: "The eye-roll arrives exactly when the lyric says she's fine",
        post_read:
          'The post opens on a simulated video call — man in the main frame, woman smiling in a small inset window, the whole thing reading as a warm long-distance check-in until the center-bottom text reframes it: he is talking about his local best friend, and she is just listening. The camera cuts to her face at 00:05 and holds there while she squints, grimaces, pulls her cheek, and rolls her eyes — the composed smile from seconds ago completely gone. “CAUSE I DON’T CARE” lands in large white caps over her face at the exact moment the lyric hits and the eye-roll peaks, and the joke works because the face already told the truth before the text arrived.',
        what_clicked:
          'The cut to her face at 00:05 forces the viewer to inspect the very thing she is trying to hide, so the “I don’t care” disclaimer lands as a confession rather than a denial.',
        evidence: [
          'Woman smiles in the small inset window while the man talks, performing ease before the camera moves.',
          'Hard cut at 00:05 centers her face, forcing inspection of the squint, grimace, and eye-roll.',
          '“CAUSE I DON’T CARE” appears over her face as the lyric and eye-roll coincide.',
        ],
        metrics: [
          { label: 'Proof', value: '1/3', detail: 'clearest example', accent: true },
          { label: 'Best rank', value: 'Top 6.1%', detail: 'recent reel rank' },
          { label: 'Baseline', value: '7.2×', detail: 'comments lift' },
          { label: 'Views', value: '313.7K', detail: 'selected post' },
          { label: 'Comments', value: '152', detail: 'conversation signal' },
        ],
      },
      {
        post_key: 'p/dxewewacicl#f16',
        proof_label: 'Camera as Editorial Witness',
        proof_headline: 'The caption makes the lens the thing being watched',
        post_read:
          'The post opens mid-gossip, no preamble — a woman already delivering the line about friend X and friend Y, the viewer dropped into social heat that started before the record button. The caption reframes everything before the second sentence lands: “understood the assignment a little too well” tells the viewer to stop watching the story and start watching whoever is holding the camera. What lands is the feeling of being handed a small piece of evidence — not the gossip itself, but the frame around it.',
        what_clicked:
          'The caption turns a passive viewer into an inspector, so every cut and framing choice reads as a confession the videographer did not quite mean to make.',
        evidence: [
          'Caption phrase “a little too well” implies excess — the camera did more than its job.',
          'The single-word “Why?” leaves a gap the framing has to fill.',
          'Hindi/Hinglish switch at 00:28–00:31 signals a register drop.',
        ],
        metrics: [
          { label: 'Proof', value: '2/3', detail: 'camera-led example', accent: true },
          { label: 'Best rank', value: 'Top 3.7%', detail: 'recent reel rank' },
          { label: 'Signal', value: 'Comments', detail: 'discussion-led' },
          { label: 'Support', value: 'Core', detail: 'same emotional engine' },
        ],
      },
      {
        post_key: 'p/dxtkzzzcfna#f16',
        proof_label: 'Manufactured Audience',
        proof_headline: 'The lyric invents a crowd the airport never provided',
        post_read:
          'The post opens on a woman in a black tank top and red-striped bag strap walking toward the camera in a selfie frame, already swaying and smiling before the gate is even in sight. The text overlay — “How I walk in the airport because I downloaded Digi Yatra” — lands immediately, so the viewer knows they are watching a performance of a feeling, not a demonstration of an app. What closes it is the camera passing through the green-lit e-gate in a single uncut shot while “All eyes on us / They watching us” plays directly over three uniformed security personnel who are not watching her at all.',
        what_clicked:
          'The music manufactures an audience out of people doing their jobs, and the gap between that fantasy and the empty queue lines behind her is where the whole joke lives.',
        evidence: [
          '“All eyes on us” plays while three uniformed personnel are visibly not watching.',
          'The queue lines leading to the e-gates are completely empty.',
          'Caption “The superiority complex is insane” names the fantasy after the shot commits to it.',
        ],
        metrics: [
          { label: 'Proof', value: '3/3', detail: 'status-fantasy example', accent: true },
          { label: 'Signal', value: 'Views', detail: 'clean visual read' },
          { label: 'Support', value: 'Core', detail: 'same gap mechanic' },
          { label: 'Mode', value: 'Inspect', detail: 'viewer catches the frame' },
        ],
      },
    ],
  },
  {
    account: '@trysugar',
    accountLabel: 'SUGAR Cosmetics',
    accountMeta: 'brand · 3 proof posts · last 90 days',
    focus_id: 'enumerated-shade-range-completion',
    focusMetrics: [
      { label: 'Strongest', value: 'Top 4.8%', detail: 'recent reel rank', accent: true },
      { label: 'Support', value: '3', detail: 'range posts' },
      { label: 'Signal', value: 'Views', detail: 'choice-led loop' },
    ],
    focus_overview: {
      focus_id: 'enumerated-shade-range-completion',
      tile_label: 'Shade Range Closer',
      tile_headline: 'Turn a full range into a loop viewers must finish',
      tile_read:
        'The viewer is handed a countable set and immediately understands the sequence has to close. Each shade runs its own small arc — reveal, apply, react — and the viewer stays to finish the comparison they started.',
      modal_headline: 'A structured shade sequence turns viewers into active comparators',
      pattern_read: [
        'The behavioral engine here is completion pressure. The moment a viewer understands that a countable set is in motion, they are no longer passive — they are tracking. Each shade functions as a discrete unit with its own micro-resolution: it appears, it lands on skin, it gets a reaction, and it closes before the next one opens.',
        'The structure works because it converts a product lineup into a viewer-driven exercise rather than a brand presentation. The tension is internal: the viewer has an open question and the content is the only thing that can close it. The consistent micro-loop keeps the rhythm satisfying without becoming predictable, because the variable changes every cycle.',
      ],
      why_it_matters:
        'This earns watch time through structure, not spectacle. It works because the viewer has a job to do and the post gives them the tools to do it. For any account selling range breadth, this builds genuine purchase readiness: the viewer exits with a narrowed decision they constructed themselves.',
      match_read:
        'A new post belongs if it presents multiple shades as a named, countable sequence with a declared total, runs each shade through an identical micro-loop, positions the viewer as a comparator rather than a recipient, and closes with a completion signal that confirms the full set has been seen.',
      avoid_read:
        'A post that features only one or two shades has no sequence and no completion pressure. Posts where shades appear as background texture, where the viewer is told which shade to choose, or where the structure is open-ended with no count will look adjacent but will not create the same behavior.',
      how_to_repeat_it:
        'Start by showing the full count — physically, in text, or through a structure the viewer already owns like a calendar. Run every shade through the same loop in the same order. Let the numbered overlay do the tracking so the viewer always knows where they are. End by returning to all units together.',
      watch_out:
        'This goes flat when the micro-loop becomes mechanical and the shades stop feeling meaningfully different from each other. It drifts when the count is implied rather than declared, removing completion pressure entirely. It becomes generic when reaction beats feel performed rather than useful.',
    },
    proof_rail: [
      {
        post_key: 'p/dye-ckenrme#f10',
        proof_label: 'Declared Paradox',
        proof_headline: 'Framing abundance as the problem keeps viewers watching all seven',
        post_read:
          'The reel opens on a woman holding all seven SUGAR lipsticks fanned out in both hands, and the text already tells you the problem: “Shade range is SO good you can’t decide.” Each shade then runs its own tight loop — tube held up, bottom label confirmed, bullet applied, smile delivered — resetting the viewer’s attention seven times without breaking the rhythm. By the time “LAST ONE” appears in yellow at 00:46, the viewer has been running their own elimination bracket the entire way through and still has not landed on a single answer.',
        what_clicked:
          'The opening frame makes the viewer’s indecision the premise, so finishing the reel feels like attempting to solve a problem they already accepted as theirs.',
        evidence: [
          'All seven tubes are fanned out at 00:00 before any individual shade appears.',
          'Yellow interjection text “WOOOOOOW...” and “LAST ONE” breaks neutral cadence mid-sequence.',
          'Caption CTA asks which number the viewer is stuck between.',
        ],
        metrics: [
          { label: 'Proof', value: '1/3', detail: 'choice-problem example', accent: true },
          { label: 'Best rank', value: 'Top 4.8%', detail: 'recent reel rank' },
          { label: 'Signal', value: 'Views', detail: 'range curiosity' },
          { label: 'Products', value: '7', detail: 'countable loop' },
        ],
      },
      {
        post_key: 'p/dxrbg0zilbr#f10',
        proof_label: 'Calendar Loop',
        proof_headline: 'Seven days frame seven shades as a set that must complete',
        post_read:
          'The reel opens on a lip close-up with “01 Peach Ping” and “MONDAY” stacked at the bottom of frame — no introduction, just the first slot already filled. Each of the next six days follows the same two-beat structure: a tight lip shot with the shade tube held to the chin, then a medium lifestyle shot where the outfit and background color have shifted to match the mood of that shade. At 00:21, all seven tubes come up in both hands at once, and the count the viewer has been running since Monday closes.',
        what_clicked:
          'The viewer is not watching a shade range — they are watching a week fill up, and their own sense of calendar logic will not let them leave before Sunday lands.',
        evidence: [
          'MONDAY through SUNDAY appear as overlays, each paired with a numbered shade name.',
          'Every day segment opens on a lip close-up before cutting to a lifestyle shot.',
          'At 00:21, both hands hold all seven tubes simultaneously.',
        ],
        metrics: [
          { label: 'Proof', value: '2/3', detail: 'calendar example', accent: true },
          { label: 'Structure', value: '7 days', detail: 'owned by viewer' },
          { label: 'Signal', value: 'Completion', detail: 'viewer tracks count' },
          { label: 'Support', value: 'Core', detail: 'same range engine' },
        ],
      },
      {
        post_key: 'p/dxmvkifcctz#f10',
        proof_label: 'Regret Frame',
        proof_headline: 'Performed regret converts a shade range into a decision tool',
        post_read:
          'The post opens on a woman cupping all seven Glide Peptide Plumping Gloss Stick tubes in her hands, asking “Tell me why I haven’t bought these sooner” before a single shade has been named. From 00:11 to 00:38, the same four-beat loop runs seven times — hold tube, name it, apply it, smile — with numbered overlays marking each unit so the viewer always knows where they are in the set. The closing line, “suitable for all Indian skin tones,” lands at exactly the moment the viewer has narrowed to a shortlist, collapsing the last reason not to buy.',
        what_clicked:
          'The viewer’s hesitation is named out loud at 00:05 — “you don’t know which shade works for you” — which makes every swatch answer a question they already had.',
        evidence: [
          'Numbered overlays 01 through 07 appear with each shade name from 00:11 to 00:36.',
          'The woman returns to holding all seven tubes at 00:39, mirroring the opening frame.',
          '“Tell me why I haven’t bought these sooner” frames the post as decision confirmation.',
        ],
        metrics: [
          { label: 'Proof', value: '3/3', detail: 'swatch-decision example', accent: true },
          { label: 'Signal', value: 'Saves', detail: 'shade reference behavior' },
          { label: 'Products', value: '7', detail: 'bounded set' },
          { label: 'Support', value: 'Core', detail: 'same completion loop' },
        ],
      },
    ],
  },
  {
    account: '@anuj.mp4',
    accountLabel: 'Anuj',
    accountMeta: 'creator · 3 core proofs · 1 adjacent · last 90 days',
    focus_id: 'gap_between_claimed_and_visible',
    focusMetrics: [
      { label: 'Strongest', value: 'Core 3', detail: 'locked read', accent: true },
      { label: 'Adjacent', value: '1', detail: 'related proof' },
      { label: 'Signal', value: 'Recognition', detail: 'viewer confirms the gap' },
    ],
    focus_overview: {
      focus_id: 'gap_between_claimed_and_visible',
      tile_label: 'Self-Image Collapse',
      tile_headline: 'The claim and the proof arrive together',
      tile_read:
        'The viewer watches someone perform a confident version of themselves while the evidence against that version accumulates in the same frame. No twist, no resolution — just the gap held open until the weight of it lands.',
      modal_headline: 'When the frame quietly disproves everything the subject just said',
      pattern_read: [
        'The viewer enters mid-performance. Someone is already delivering a claim with full conviction — moral, athletic, relational, ideological — and the viewer is given just enough context to understand what that person believes about themselves before the contradicting evidence appears. What makes this structure hold is that the contradiction never arrives as a reveal. It accumulates in the same frame, visible the whole time, while the subject keeps escalating.',
        'The force here is confirmation, not discovery. The viewer already suspects the collapse is coming, and the satisfaction arrives when it lands faster and more completely than the subject intended. That compression is what makes it replayable: the viewer walks away holding a receipt, a single image of the contradiction they can quote or return to.',
      ],
      why_it_matters:
        'Most humor or critique content resolves the tension it creates. This structure refuses to. The gap stays open, the subject stays unaware, and the viewer is left holding the contradiction without anyone naming it. That refusal to resolve is what gives the moment its staying power — it feels like evidence, not entertainment.',
      match_read:
        'A subject performs a clear self-image before any contradiction appears. The contradicting evidence is visible in the same frame, not withheld as a twist. The subject never acknowledges or closes the gap. The viewer satisfaction comes from confirmation rather than learning something new.',
      avoid_read:
        'Skip anything where the subject is deliberately performing the gap for the camera from the start. Avoid twist-ending structures where the contradiction is withheld until the final moment. If the subject admits fault, grows, or resolves the tension, the engine closes.',
      how_to_repeat_it:
        'Open with the subject already mid-claim, confident and unguarded. Let the contradicting evidence sit in the frame without announcing itself. Resist the urge to editorialize or signal the gap; the viewer should arrive at it alone. The subject escalation is the pacing mechanism.',
      watch_out:
        'This goes flat when the contradiction is too obvious too early, leaving nothing to track. It drifts when the subject becomes sympathetic enough that the viewer roots for them instead of watching them fall. It becomes generic when an outside voice names the gap.',
    },
    proof_rail: [
      {
        post_key: 'p/dxy_ou6kc9k#f27',
        proof_label: 'Live Deflation By Subject',
        proof_headline: 'Sania Mirza dismantles each claim as it leaves his mouth',
        post_read:
          "The post opens on a man sitting on a bench holding a jar of pickles, delivering a mock-serious testimonial about becoming a champion athlete since booking courts on District. Each claim he makes — serenity, respect for rules, graceful acceptance — is immediately answered by Sania Mirza in the same frame, calling him out, overruling him, and threatening to meet him outside. What lands is the overlay at 00:44 flipping the entire collapse into a product invitation: you don't have to be Sania Mirza to play.",
        what_clicked:
          'Sania Mirza responses arrive inside the same breath as each claim, so the gap never closes — it just keeps widening until the product line absorbs it.',
        evidence: [
          'Narrator claims "Sania Mirza of Pickleball"; she responds "Come, fight me" at 00:13.',
          'Narrator declares rules are sacred at 00:31; Sania calls "one tip out" against him at 00:36.',
          'Narrator says "you learn to accept" at 00:37; Sania threatens "I will see you outside" at 00:40.',
        ],
        metrics: [
          { label: 'Proof', value: '1/3', detail: 'athletic self-image', accent: true },
          { label: 'Fit', value: 'Core', detail: 'same collapse engine' },
          { label: 'Mode', value: 'Confirm', detail: 'viewer tracks the gap' },
          { label: 'Signal', value: 'Quote', detail: 'compressed receipt' },
        ],
      },
      {
        post_key: 'p/dxoh-undfw1#f27',
        proof_label: 'Dual-Frame Collapse In Progress',
        proof_headline: 'Son performs sincerity while smirking at the camera behind it',
        post_read:
          'The post opens on a title card that gives the entire game away — "telling my mom i\'m in love with a married woman" sits in white text at the top of the frame before a single word is spoken, so the viewer enters already holding the punchline. What holds attention is the son escalation strategy: each new detail ("her husband can\'t satisfy her," "she\'s 35," "I can save her") is a deliberate provocation added while the mother disapproval is already at full volume. What lands is the smirk directly into the lens at 00:51 — the son confirming, without words, that the viewer was always the real audience and the mother never was.',
        what_clicked:
          'The mother catches him at 00:23 — "I think you are recording" — and he denies it while the static camera is visibly rolling, collapsing his performed sincerity in a single exchange.',
        evidence: [
          'Son denies recording at 00:24 while a static camera rolls the entire scene.',
          'Each confession escalates unprompted: boring husband, cannot satisfy her, I can save her.',
          'Direct-camera smirk at 00:51 confirms the viewer was always the target.',
        ],
        metrics: [
          { label: 'Proof', value: '2/3', detail: 'relational self-image', accent: true },
          { label: 'Fit', value: 'Core', detail: 'same collapse engine' },
          { label: 'Mode', value: 'Witness', detail: 'viewer holds both frames' },
          { label: 'Signal', value: 'Smirk', detail: 'gap stays open' },
        ],
      },
      {
        post_key: 'p/dxjldsojbr7#f27',
        proof_label: 'Idealist Folds on Offer',
        proof_headline: 'Self-declared liberal collapses the instant power is offered',
        post_read:
          'The post opens on a close-up rant — curly hair, silver chain, animated hands — someone already mid-conviction calling politicians "anpadh gundas" and tagging himself "cool, liberal, woke" before any challenge appears. The flat-toned second figure cuts in at 00:07 with a single question — "You wanna join us or what?" — and the first character body turns toward the offer before his mouth catches up. What lands is the explicit receipt: "I have no morals," delivered with a nervous laugh, closing the gap the viewer had already measured.',
        what_clicked:
          'The body turns before the words do — the physical pivot at 00:08 confirms the collapse a half-second before the confession arrives.',
        evidence: [
          '"I am cool, liberal, woke" establishes the self-image before any offer appears.',
          'The first character physically turns toward the second figure before speaking.',
          '"I have no morals" lands as an explicit self-indictment with no recovery.',
        ],
        metrics: [
          { label: 'Proof', value: '3/3', detail: 'ideological self-image', accent: true },
          { label: 'Fit', value: 'Core', detail: 'same collapse engine' },
          { label: 'Mode', value: 'Judge', detail: 'viewer confirms the fold' },
          { label: 'Signal', value: 'Line', detail: 'quotable receipt' },
        ],
      },
    ],
  },
];

export function focusForHandle(selectedHandle?: string): FeederboardFocus[] {
  const handle = String(selectedHandle || '').replace(/^@+/, '').toLowerCase();
  if (handle.includes('anuj')) return FEEDERBOARD_FOCUSES.filter((focus) => focus.account === '@anuj.mp4');
  if (handle.includes('sugar')) return FEEDERBOARD_FOCUSES.filter((focus) => focus.account === '@trysugar');
  if (handle.includes('saniya')) return FEEDERBOARD_FOCUSES.filter((focus) => focus.account === '@saniyamirwani');
  return FEEDERBOARD_FOCUSES;
}

