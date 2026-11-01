# Read data exported for the cloud (Oct 30 2026)

Exported from the production database (read-only) and the Apify test runs. The cloud session has no database access.

- `checkpoints-<acct>.json`: maps shortcode → `{d1|d3|d7|d21: {likes, comments, views, at}}`.
  - Views stopped coming through Bright Data in July, so many are null. Apify fills them for reels from now on.
  - Shortcodes match the keys in `apps/web/public/read17/<acct>.json` posts.
- `comments-<acct>.json`: maps shortcode → `{at, plays, texts[]}`, the 15 newest comments from the Apify test runs.
  - These come only from the ~60 test posts; no comment text is stored anywhere else yet.
  - Commenter names were never kept, and @handles inside comments are masked as `@someone`.

Tools used for the post reads are in `../tools`:
- `check_post_reads.py`: grounding, bans, lengths and hero repeats.
- the writer briefs, one per account.
