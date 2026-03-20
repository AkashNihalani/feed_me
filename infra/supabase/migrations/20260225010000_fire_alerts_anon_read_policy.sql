-- Allow the anon (unauthenticated) role to read fire_alerts.
-- The Fire page uses createBrowserClient with the anon key and has no
-- authenticated user session, so auth.uid() is NULL and the existing
-- "Users can read own fire alerts" policy blocks every row.

-- Also grant read on the upstream tables the page may join through
-- (feeds, feeders, posts) so the foreign-key-based policies don't
-- silently block related lookups.

create policy "Anon can read all fire alerts"
  on public.fire_alerts for select to anon
  using (true);

create policy "Anon can read all feeds"
  on public.feeds for select to anon
  using (true);

create policy "Anon can read all feeders"
  on public.feeders for select to anon
  using (true);

create policy "Anon can read all posts"
  on public.posts for select to anon
  using (true);

create policy "Anon can read all post metrics"
  on public.post_metrics for select to anon
  using (true);
