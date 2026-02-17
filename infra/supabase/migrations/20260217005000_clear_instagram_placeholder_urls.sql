update public.feeders
set profile_pic_url = null,
    profile_pic_fetched_at = now(),
    updated_at = now()
where profile_pic_url ilike '%static.cdninstagram.com/rsrc.php%'
   or profile_pic_url ilike '%/rsrc.php/%';
