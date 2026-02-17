update public.feeders
set profile_pic_url = null,
    updated_at = now()
where profile_pic_url like 'https://unavatar.io/instagram/%';
