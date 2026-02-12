create table if not exists public.scheduled_scrapes (
  id uuid default gen_random_uuid() primary key,
  user_id uuid references public.users(id) not null,
  platform text not null,
  target_url text not null,
  frequency text check (frequency in ('daily', 'weekly')) not null,
  status text check (status in ('active', 'paused')) default 'active',
  created_at timestamp with time zone default timezone('utc'::text, now()) not null,
  next_run_at timestamp with time zone default timezone('utc'::text, now() + interval '1 day'), -- Default next run tomorrow
  last_run_at timestamp with time zone
);

-- Enable RLS
alter table public.scheduled_scrapes enable row level security;

-- Policies
create policy "Users can view their own schedules"
  on public.scheduled_scrapes for select
  using (auth.uid() = user_id);

create policy "Users can insert their own schedules"
  on public.scheduled_scrapes for insert
  with check (auth.uid() = user_id);

create policy "Users can update their own schedules"
  on public.scheduled_scrapes for update
  using (auth.uid() = user_id);

create policy "Users can delete their own schedules"
  on public.scheduled_scrapes for delete
  using (auth.uid() = user_id);
